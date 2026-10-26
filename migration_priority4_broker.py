# Independent P4 broker; launch only within its explicitly registered scope.
#!/usr/bin/env python3
"""Compatible broker with local lease exclusion and independent NFS work.

SQLite remains the assignment authority. No shared-filesystem operation is
performed under the database mutex. Lock objects are retained for the run so
waiters can never acquire different locks for the same owner or task.
"""
import collections
import hmac
import json
import pathlib
import re
import threading
import time
import uuid

from migration_broker import Broker as OriginalBroker, validate_draining_hosts
from migration_io import read_json, write_json


def atomic_json(path, value):
    return write_json(path, value)


class Broker(OriginalBroker):
    def __init__(self, control):
        cfg = read_json(pathlib.Path(control) / 'config.json')
        if (cfg.get('source'), cfg.get('destination'), cfg.get('migration_scope'), cfg.get('exclude')) != (
                '/share/project', '/share-mixed/project', 'project_priority4', ['eai_pwm']):
            raise ValueError('priority4 broker requires the exact registered scope')
        super().__init__(control)
        self.locks_lock = threading.Lock()
        self.owner_locks = {}
        self.task_locks = {}
        self.state_lock = threading.Lock()
        self.ready_lock = threading.Lock()
        self.control_lock = threading.Lock()
        self.fs_slots = threading.BoundedSemaphore(max(1, int(self.cfg.get('broker_fs_concurrency', 64))))

    def _owner_lock(self, stage, owner):
        with self.locks_lock:
            return self.owner_locks.setdefault((stage, owner), threading.Lock())

    def _task_lock(self, stage, task_id):
        with self.locks_lock:
            return self.task_locks.setdefault((stage, task_id), threading.Lock())

    def running_path(self, stage, owner, task_id):
        return self.control / 'stages' / stage / 'running' / (
            'broker--' + owner + '--' + str(task_id) + '.json')

    def publish_json(self, path, value):
        return atomic_json(path, value)

    def _state(self):
        with self.state_lock:
            if self.state_cache is None or time.monotonic() >= self.state_cache_until:
                with self.fs_slots:
                    self.state_cache = read_json(self.control / 'state.json')
                self.state_cache_until = time.monotonic() + 0.5
            return self.state_cache

    @staticmethod
    def _active(state, stage):
        return (state.get('stage') == stage and state.get('status') not in
                ('failed', 'stopped', 'complete', 'complete_with_changes'))

    def _outstanding(self, stage, owner):
        with self.lock:
            return self.db.execute(
                "SELECT id,lease,payload,running FROM claims "
                "WHERE stage=? AND owner=? AND status='leased'", (stage, owner)).fetchone()

    def _candidate(self, stage):
        # This mutex only serializes ready-cache enumeration. In particular,
        # slow getdents/stat calls never hold the SQLite or completion locks.
        with self.ready_lock:
            ready = self.ready.setdefault(stage, collections.deque())
            if not ready and time.monotonic() >= self.next_scan.get(stage, 0):
                root = self.control / 'stages' / stage / 'pending'
                with self.fs_slots:
                    for shard in range(64):
                        parent = root / str(shard)
                        if parent.is_dir():
                            ready.extend(p for p in parent.iterdir() if p.name.endswith('.json'))
                self.next_scan[stage] = time.monotonic() + 0.5
            return ready.popleft() if ready else None

    def _publish_claim(self, running, task, pending, recover=False):
        with self.fs_slots:
            if not recover or not running.exists():
                self.publish_json(running, task)
            try:
                pending.unlink()
            except FileNotFoundError:
                pass

    def _recover_claim(self, stage, owner, previous):
        task_id = previous[0]
        with self._task_lock(stage, task_id):
            # A completion may have won the task lock after the first lookup.
            # Never recreate a running marker for an already completed lease.
            with self.lock:
                row = self.db.execute(
                    'SELECT lease,status,payload,running,owner FROM claims WHERE stage=? AND id=?',
                    (stage, task_id)).fetchone()
            if (not row or row[1] != 'leased' or row[4] != owner or
                    not hmac.compare_digest(row[0], previous[1])):
                return {'task': None}
            task = json.loads(row[2])
            running = pathlib.Path(row[3])
            pending = self.control / 'stages' / stage / 'pending' / str(task_id % 64) / (str(task_id) + '.json')
            self._publish_claim(running, task, pending, recover=True)
            return {'task': task, 'lease': row[0]}

    def claim(self, stage, owner):
        if (not re.fullmatch(r'p4-r[123]', stage) or
                not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', owner)):
            raise ValueError('invalid stage or owner')
        # Concurrent retries for this owner cannot allocate two tasks.
        with self._owner_lock(stage, owner):
            previous = self._outstanding(stage, owner)
            if previous:
                return self._recover_claim(stage, owner, previous)
            with self.lock:
                if owner.rsplit('-', 1)[0] in self.draining_hosts:
                    return {'task': None, 'draining': True}
            if not self._active(self._state(), stage):
                return {'task': None}
            while True:
                path = self._candidate(stage)
                if path is None:
                    return {'task': None}
                task_id = int(path.stem)
                with self._task_lock(stage, task_id):
                    with self.lock:
                        assigned = self.db.execute(
                            'SELECT status FROM claims WHERE stage=? AND id=?',
                            (stage, task_id)).fetchone()
                    if assigned:
                        continue
                    try:
                        with self.fs_slots:
                            task = read_json(path)
                    except FileNotFoundError:
                        continue
                    if type(task.get('id')) is not int or task['id'] != task_id:
                        raise RuntimeError('queue payload id mismatch')
                    if (type(task.get('phase')) is not int or type(task.get('round')) is not int or
                            (task['phase'], task['round']) != (4, int(stage[-1])) or
                            task.get('op') not in ('copy', 'sync')):
                        raise ValueError('task payload does not match its priority4 stage')
                    payload = json.dumps(task, ensure_ascii=True)
                    lease = uuid.uuid4().hex
                    running = self.running_path(stage, owner, task_id)
                    state = self._state()
                    with self.lock:
                        if owner.rsplit('-', 1)[0] in self.draining_hosts:
                            return {'task': None, 'draining': True}
                        if not self._active(state, stage):
                            return {'task': None}
                        # Recheck after the pending payload's NFS read. The
                        # owner mutex normally makes this empty; keep the DB
                        # invariant explicit at the assignment transaction.
                        previous = self.db.execute(
                            "SELECT id,lease,payload,running FROM claims "
                            "WHERE stage=? AND owner=? AND status='leased'",
                            (stage, owner)).fetchone()
                        assigned = self.db.execute(
                            'SELECT status FROM claims WHERE stage=? AND id=?',
                            (stage, task_id)).fetchone()
                        if previous:
                            # Do not acquire a second task lock while holding
                            # this one. A retry resolves the existing lease.
                            return {'task': None}
                        if assigned:
                            continue
                        self.db.execute('INSERT INTO claims VALUES (?,?,?,?,?,?,?)',
                                        (stage, task_id, owner, lease, 'leased', payload, str(running)))
                        self.db.commit()
                    # Pending stays visible through the full publication. A
                    # crash/failure here leaves a durable recoverable lease.
                    self._publish_claim(running, task, path)
                    return {'task': task, 'lease': lease}

    def done(self, data):
        stage, task_id, lease = data['stage'], int(data['id']), data['lease']
        with self._task_lock(stage, task_id):
            with self.lock:
                row = self.db.execute(
                    'SELECT lease,status,running,payload FROM claims WHERE stage=? AND id=?',
                    (stage, task_id)).fetchone()
            if not row or not hmac.compare_digest(row[0], lease):
                raise ValueError('invalid lease')
            if row[1] == 'done':
                return {'ok': True, 'already_done': True}
            receipt = data['receipt']
            task = json.loads(row[3])
            if (receipt['id'] != task_id or [r['rel'] for r in receipt['results']] !=
                    [e['rel'] for e in task['entries']]):
                raise ValueError('receipt does not cover assigned entries')
            target = self.control / 'stages' / stage / 'done' / str(task_id % 64) / (str(task_id) + '.json')
            with self.fs_slots:
                snapshots = target.parent / '.snapshots' / target.name
                if target.exists() or snapshots.exists():
                    try:
                        prior = {r['rel']: r for r in read_json(target)['results']}
                    except FileNotFoundError:
                        prior = {}
                else:
                    prior = {}
                for result in receipt['results']:
                    previous = prior.get(result['rel'])
                    if result['status'] == 'skipped' and previous and previous['status'] == 'copied':
                        result['status'] = 'copied'
                        result['bytes'] = previous.get('bytes', 0)
                        result['replay_reused_copy'] = True
                self.publish_json(target, receipt)
                try:
                    pathlib.Path(row[2]).unlink()
                except FileNotFoundError:
                    pass
            with self.lock:
                self.db.execute("UPDATE claims SET status='done' WHERE stage=? AND id=?",
                                (stage, task_id))
                self.db.commit()
            return {'ok': True}

    def update_control(self, data):
        if not isinstance(data, dict) or 'draining_hosts' not in data:
            raise ValueError('control requires draining_hosts')
        hosts = validate_draining_hosts(data['draining_hosts'])
        # Serialize updates, publish without the DB lock, then install the
        # in-memory barrier under that lock before acknowledging the request.
        with self.control_lock:
            with self.fs_slots:
                atomic_json(self.control / 'drain-hosts.json',
                            {'draining_hosts': sorted(hosts), 'updated': time.time()})
            with self.lock:
                self.draining_hosts = hosts
            return {'ok': True, 'draining_hosts': sorted(hosts)}


def main():
    # Retain the exact CLI, authentication, HTTP routes/backlog, local flock,
    # readonly WAL control observation, and broker.json publication contract.
    import migration_broker
    migration_broker.Broker = Broker
    migration_broker.main()


if __name__ == '__main__':
    main()
