#!/usr/bin/env python3
"""One-shot, audited handover of ONLY the warm recovery foreground producer.

Default mode is a read-only SSH inspection. --prepare-only creates an immutable
code bundle and ledger while the existing producer continues. --apply prepares
and then performs the handover in one foreground SSH invocation. Copy workers,
broker, training, leases and in-flight file descriptors are never signalled.

An interrupted/ambiguous attempt must be inspected, not retried automatically.
The final status is starting_replay, NOT ready: observe the adopted manifest and
task_publication counters before declaring this rollout healthy.
"""

import argparse
import base64
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import traceback
import urllib.request
import uuid


DEFAULT_POINTER = Path('/share-mixed/project/.hcr-copy-monitor/active.json')
BUNDLE_FILES = (
    'migration_pipelined_target_filter.py', 'migration_task_publisher.py',
    'migration_distributed_target_filter.py', 'migration_overlap_parallel_filter.py',
    'migration_parallel_scan.py', 'migration_parallel_pipeline.py', 'migration_io.py',
)
IDENTITY_KEYS = ('pid', 'starttime_ticks', 'argv')
WARM_STAGE = 'p3-r3'
MAX_RECORD = 1024 * 1024


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def process_identity(pid):
    try:
        root = Path('/proc') / str(pid)
        fields = (root / 'stat').read_text().rsplit(') ', 1)[1].split()
        return {'pid': int(pid), 'state': fields[0], 'starttime_ticks': fields[19],
                'argv': (root / 'cmdline').read_bytes().decode(
                    errors='surrogateescape').rstrip('\0').split('\0')}
    except (FileNotFoundError, ProcessLookupError):
        return None


def same_identity(actual, expected):
    return (isinstance(actual, dict) and isinstance(expected, dict)
            and actual.get('state') not in ('Z', 'X')
            and all(actual.get(key) == expected.get(key) for key in IDENTITY_KEYS))


def role_identity(role):
    result = {key: role.get(key) for key in IDENTITY_KEYS}
    result['starttime_ticks'] = str(role.get('starttime_ticks', role.get('startticks', '')))
    if (type(result['pid']) is not int or result['pid'] <= 0
            or not result['starttime_ticks'].isdigit()
            or not isinstance(result['argv'], list) or not result['argv']
            or not all(isinstance(arg, str) for arg in result['argv'])):
        raise ValueError('complete PID/starttime/full argv identity is required')
    return result


def require_identity(expected, *, states=None):
    actual = process_identity(expected['pid'])
    if not same_identity(actual, expected) or (states and actual['state'] not in states):
        raise RuntimeError('process identity/state changed: PID ' + str(expected['pid']))
    return actual


def task_bytes(task):
    if (not isinstance(task, dict) or type(task.get('id')) is not int
            or task['id'] < 0 or (task.get('phase'), task.get('round')) != (3, 3)
            or task.get('op') != 'sync' or not isinstance(task.get('entries'), list)):
        raise ValueError('invalid warm-recovery task payload')
    for entry in task['entries']:
        if not isinstance(entry, dict) or not isinstance(entry.get('rel'), str):
            raise ValueError('invalid task entry')
        rel = entry['rel']
        if (not rel or '\0' in rel or rel.startswith('/')
                or any(p in ('', '.', '..') for p in rel.split('/'))):
            raise ValueError('unsafe task relative path')
    return json.dumps(task, ensure_ascii=True, sort_keys=True, separators=(',', ':'),
                      allow_nan=False).encode('ascii')


class AssignmentLedger:
    """Streaming complete payload ledger; memory holds IDs and hashes only."""

    def __init__(self, path):
        self.path = Path(path)
        self.handle = open(self.path, 'xb')
        self.hashes = {}
        self.digest = hashlib.sha256()
        self.entries = 0
        self.bytes = 0
        self.closed = False

    def add(self, task, *, expected_id=None):
        payload = task_bytes(task)
        task_id = task['id']
        if expected_id is not None and task_id != expected_id:
            raise ValueError('queue/claim ID differs from payload')
        digest = hashlib.sha256(payload).digest()
        prior = self.hashes.get(task_id)
        if prior is not None:
            if prior != digest:
                raise RuntimeError('immutable task payload conflict: ' + str(task_id))
            return False
        if self.closed:
            raise RuntimeError('assignment ledger is closed')
        line = payload + b'\n'
        self.handle.write(line)
        self.digest.update(line)
        self.hashes[task_id] = digest
        self.entries += len(task['entries'])
        self.bytes += len(line)
        return True

    def close(self):
        if not self.closed:
            self.handle.flush()
            os.fsync(self.handle.fileno())
            self.handle.close()
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self.closed = True
        return {'tasks': len(self.hashes), 'entries': self.entries, 'bytes': self.bytes,
                'sha256': self.digest.hexdigest(), 'maximum_id': max(self.hashes, default=-1)}


def claims_reader(path):
    if not Path(path).is_file():
        raise FileNotFoundError('existing broker SQLite database is required')
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True, timeout=60)
    db.execute('PRAGMA busy_timeout=60000')
    db.execute('PRAGMA query_only=ON')
    return db


def collect_pending(control, db, ledger, read_json):
    """Read pending first; vanished entries MUST already have durable claims."""
    root = Path(control) / 'stages' / WARM_STAGE / 'pending'
    count = 0
    for shard in range(64):
        parent = root / str(shard)
        if not parent.is_dir():
            raise RuntimeError('missing queue shard: ' + str(parent))
        with os.scandir(parent) as paths:
            for item in paths:
                if not re.fullmatch(r'[0-9]+\.json', item.name):
                    continue
                task_id = int(item.name[:-5])
                if task_id % 64 != shard:
                    raise RuntimeError('pending task stored under wrong shard')
                try:
                    task = read_json(Path(item.path))
                except FileNotFoundError:
                    row = db.execute('SELECT payload FROM claims WHERE stage=? AND id=?',
                                     (WARM_STAGE, task_id)).fetchone()
                    if row is None:
                        raise RuntimeError('pending task vanished without a durable claim: ' + str(task_id))
                    task = json.loads(row[0])
                ledger.add(task, expected_id=task_id)
                count += 1
    return count


def collect_claims(db, ledger, *, missing_only=False):
    """Frozen reconciliation uses ALL claim IDs, not MAX(id) as a watermark."""
    stages = [row[0] for row in db.execute('SELECT DISTINCT stage FROM claims')]
    if any(stage != WARM_STAGE for stage in stages):
        raise RuntimeError('broker contains unexpected formal-stage claims')
    if not missing_only:
        count = 0
        for task_id, payload in db.execute('SELECT id,payload FROM claims WHERE stage=?',
                                          (WARM_STAGE,)):
            ledger.add(json.loads(payload), expected_id=task_id)
            count += 1
        return count
    # Enumerate complete IDs first, close the read cursor before fetching rows.
    # Existing payloads are immutable by the broker contract. Every absent ID,
    # including a lower-ID hole behind a higher pending/claimed ID, is fetched.
    missing = [row[0] for row in db.execute('SELECT id FROM claims WHERE stage=?',
                                          (WARM_STAGE,)) if row[0] not in ledger.hashes]
    for task_id in missing:
        row = db.execute('SELECT payload FROM claims WHERE stage=? AND id=?',
                         (WARM_STAGE, task_id)).fetchone()
        if row is None:
            raise RuntimeError('durable claim disappeared during reconciliation')
        ledger.add(json.loads(row[0]), expected_id=task_id)
    return len(missing)


def broker_control(cfg, timeout):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(cfg['broker_url'].rstrip('/') + '/control',
                                    headers={'Authorization': 'Bearer ' + cfg['broker_token']})
    with opener.open(request, timeout=timeout) as response:
        data = response.read(MAX_RECORD + 1)
    if len(data) > MAX_RECORD:
        raise RuntimeError('broker control reply too large')
    value = json.loads(data)
    if value.get('draining_hosts') != []:
        raise RuntimeError('broker has active drains; do not overlap deployments')
    return {'draining_hosts': [], 'observed': now()}


def guarded_state(control, read_json):
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present')
    state = read_json(control / 'state.json')
    if (state.get('stage') != WARM_STAGE or state.get('recovery_warm') is not True
            or state.get('status') != 'scanning' or state.get('scan_complete')):
        raise RuntimeError('handover requires the unfinished scanning warm recovery queue')
    if read_json(control / 'pipeline.json').get('stages'):
        raise RuntimeError('formal pipeline stages have started')
    return state


def within_control(path, control):
    result = Path(path).resolve()
    if result == control or control not in result.parents or not result.is_file():
        raise ValueError('preserved input must be an existing file inside control')
    return result


def preflight(control, read_json, timeout):
    cfg = read_json(control / 'config.json')
    if (cfg.get('source'), cfg.get('destination')) != (
            '/share/project/eai_pwm', '/share-mixed/project/eai_pwm'):
        raise RuntimeError('only the existing eai_pwm recovery producer may be handed over')
    if cfg.get('control') != str(control) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', cfg['run_id']):
        raise RuntimeError('control/run ID mismatch')
    state = guarded_state(control, read_json)
    role = read_json(control / 'pipeline-deployment.json')
    if (role.get('host') != socket.gethostname()
            or socket.gethostname() != 'job-' + cfg['job'] + '-master-0'):
        raise RuntimeError('SSH endpoint is not the saved foreground master host')
    old = role_identity(role)
    actual = require_identity(old)
    if actual['state'] in ('T', 't'):
        raise RuntimeError('old producer was already stopped before this attempt')
    if not any(arg.endswith('/migration_distributed_target_filter.py') for arg in old['argv']):
        raise RuntimeError('unexpected current foreground producer entrypoint')
    if role.get('control') != str(control):
        raise RuntimeError('saved foreground role belongs to a different control')
    seed = read_json(control / 'overlap-seed-helper-identity.json')
    seed_id = role_identity(seed)
    result = Path(seed.get('result_path', '')).resolve()
    if result == control or control not in result.parents:
        raise RuntimeError('seed result must remain inside control')
    if not same_identity(process_identity(seed_id['pid']), seed_id) and not result.is_file():
        raise RuntimeError('original seed has neither exact live identity nor a result')
    broker = read_json(control / 'broker.json')
    broker_actual = process_identity(broker['pid'])
    if not broker_actual or broker_actual['state'] in ('Z', 'X'):
        raise RuntimeError('broker is not running')
    if not any(arg.endswith('/migration_broker_parallel.py') for arg in broker_actual['argv']):
        raise RuntimeError('unexpected broker entrypoint')
    training = read_json(control / 'training-health.json')
    if training.get('state') != 'watching' or not training.get('training_pids'):
        raise RuntimeError('training watchdog is not watching a baseline launcher')
    launchers = []
    for pid in training['training_pids']:
        actual_training = process_identity(int(pid))
        if (not actual_training or actual_training['state'] in ('Z', 'X')
                or (Path('/proc') / str(pid) / 'comm').read_text().strip() != 'pt_elastic'):
            raise RuntimeError('baseline training launcher is not alive')
        launchers.append(actual_training)
    warm = within_control(control / 'manifests/recovery-warm.jsonl', control)
    missing = within_control(state['active_manifest'], control)
    return {'captured': now(), 'control': str(control), 'old_producer': old,
            'old_role': role, 'seed_helper': seed, 'broker': broker_actual,
            'training': launchers, 'broker_control': broker_control(cfg, timeout),
            'warm_manifest': str(warm), 'missing_manifest': str(missing),
            'state': state}, cfg


def assert_protected_identities(evidence):
    require_identity(evidence['broker'])
    for launcher in evidence['training']:
        require_identity(launcher)


def immutable_bundle(control, payload, write_json):
    operation = payload['operation']
    if not re.fullmatch(r'[0-9a-f]{32}', operation):
        raise ValueError('invalid operation ID')
    bundle = control / ('pipelined-producer-' + operation)
    bundle.mkdir(mode=0o700)
    hashes = {}
    files = payload['files']
    if set(files) != set(BUNDLE_FILES) | {'deploy_pipelined_producer.py'}:
        raise ValueError('bundle file set differs from the fixed allowlist')
    for name, record in files.items():
        raw = base64.b64decode(record['base64'], validate=True)
        if len(raw) > 4 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != record['sha256']:
            raise RuntimeError('bundle SHA mismatch: ' + name)
        destination = bundle / name
        with open(destination, 'xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        destination.chmod(0o444)
        if hashlib.sha256(destination.read_bytes()).hexdigest() != record['sha256']:
            raise RuntimeError('deployed SHA mismatch: ' + name)
        hashes[name] = record['sha256']
    write_json(bundle / 'source-sha256.json', {'operation': operation, 'files': hashes})
    return bundle, hashes


def durable_json(path, value, writer):
    """fsync the actual immutable record, not a possibly stale canonical file."""
    actual = Path(writer(path, value))
    with open(actual, 'rb') as handle:
        os.fsync(handle.fileno())
    directory = os.open(actual.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return actual


def wait_state(expected, states, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        actual = require_identity(expected)
        if actual['state'] in states:
            return actual
        time.sleep(0.05)
    raise RuntimeError('process did not reach required state: ' + str(expected['pid']))


def wait_exited(expected, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        actual = process_identity(expected['pid'])
        if (actual is None or actual['state'] in ('Z', 'X')
                or actual['starttime_ticks'] != expected['starttime_ticks']):
            return
        if actual['argv'] in ([], ['']):
            # /proc/stat and /proc/cmdline are separate reads. During expected
            # process teardown cmdline can become empty after an initial live
            # stat read, before the next observation reports Z/disappearance.
            # Keep waiting for positive exit evidence; do not infer exit or
            # misclassify this bounded transition as a different executable.
            time.sleep(.1)
            continue
        if not same_identity(actual, expected):
            raise RuntimeError('old producer unexpectedly changed argv before exit')
        time.sleep(0.1)
    raise RuntimeError('old producer did not exit; no SIGKILL will be sent')


def wait_lock_released(path, seconds):
    with open(path, 'a') as lock:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(lock, fcntl.LOCK_UN)
                return
            except BlockingIOError:
                time.sleep(0.1)
    raise RuntimeError('coordinator lock has not released; no second producer started')


def remote_operation(payload):
    control = Path(payload['control']).resolve()
    # Import only the deployed run's JSON protocol, not arbitrary user PYTHONPATH.
    sys.path.insert(0, str(control))
    from migration_io import read_json, write_json
    timeout = payload['control_timeout']
    evidence, cfg = preflight(control, read_json, timeout)
    if payload['mode'] == 'inspect':
        return {'status': 'inspected', 'evidence': evidence}
    operation = payload['operation']
    # The older target-filter deployer used this lock. Also coordinate with the
    # cache rollout dispatch lock to exclude concurrent host-drain operations.
    dispatches = []
    for suffix in ('-parallel-target-dispatch.lock', '-cache-age-dispatch.lock'):
        lock = open('/tmp/' + cfg['run_id'] + suffix, 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        dispatches.append(lock)
    bundle, hashes = immutable_bundle(control, payload, write_json)
    record = {'version': 1, 'operation': operation, 'status': 'preparing',
              'created': now(), 'control': str(control), 'bundle': str(bundle),
              'code_sha256': hashes, 'preflight': evidence,
              'mutation_attempted': False, 'termination_attempted': False,
              'old_producer_terminated': False,
              'worker_broker_training_signalled': False, 'drains_modified': False}
    deployment = bundle / 'deployment.json'
    write_json(deployment, record)
    frozen = False
    terminated = False
    old_exited = False
    ledger = None
    db = None
    old = evidence['old_producer']
    previous_handlers = {}

    def interrupted(signum, frame):
        raise RuntimeError('deployment controller interrupted by signal ' + str(signum))

    for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, interrupted)
    try:
        entry = bundle / 'migration_pipelined_target_filter.py'
        environment = dict(os.environ, PYTHONPATH=str(bundle) + os.pathsep + str(control),
                           PYTHONPYCACHEPREFIX='/tmp/' + cfg['run_id'] + '-pipelined-' + operation)
        help_log = bundle / 'entry-help.log'
        with open(help_log, 'xb') as handle:
            subprocess.run([sys.executable, str(entry), '--help'], env=environment,
                           stdout=handle, stderr=subprocess.STDOUT, check=True, timeout=120)
        # Preload the expensive durable assignment history while the original
        # producer and all workers continue. No source/destination data is read.
        ledger = AssignmentLedger(bundle / 'assigned-tasks.jsonl')
        db = claims_reader(Path('/tmp') / (cfg['run_id'] + '-broker.sqlite'))
        pending_before = collect_pending(control, db, ledger, read_json)
        claims_before = collect_claims(db, ledger)
        record.update(status='prepared', prepared=now(),
                      preloaded={'pending_seen': pending_before, 'claims_seen': claims_before,
                                 'unique_tasks': len(ledger.hashes)})
        write_json(deployment, record)
        if payload['mode'] == 'prepare-only':
            record.update(status='prepared_only_not_reusable', ledger=ledger.close(),
                          note='No producer signal sent. Apply requires a fresh one-shot attempt.')
            write_json(deployment, record)
            return {key: record[key] for key in ('status', 'operation', 'bundle', 'ledger')}
        # Repeat the full mutable preflight immediately before the first signal.
        latest, _ = preflight(control, read_json, timeout)
        if not same_identity(latest['old_producer'], old):
            raise RuntimeError('foreground identity changed during preparation')
        assert_protected_identities(evidence)
        record.update(status='freezing', mutation_attempted=True, freeze_requested=now())
        write_json(deployment, record)
        require_identity(old)
        frozen = True
        os.kill(old['pid'], signal.SIGSTOP)
        wait_state(old, {'T', 't'}, 30)
        state = guarded_state(control, read_json)
        broker_control(cfg, timeout)
        assert_protected_identities(evidence)
        frozen_pending = collect_pending(control, db, ledger, read_json)
        missing_claims = collect_claims(db, ledger, missing_only=True)
        info = ledger.close()
        # The old state may reserve an ID beyond publication; never reuse it.
        state_next = state.get('next_task_id')
        if type(state_next) is not int or state_next < 0:
            raise RuntimeError('old state has no valid next_task_id reservation')
        next_id = max(info['maximum_id'] + 1, state_next)
        handover = {'control': str(control), 'stage': WARM_STAGE,
                    'old_producer': old, 'seed_helper': evidence['seed_helper'],
                    'assigned_tasks_path': str(ledger.path),
                    'warm_manifest': str(within_control(evidence['warm_manifest'], control)),
                    'missing_manifest': str(within_control(state['active_manifest'], control)),
                    'next_task_id': next_id, 'created': now(),
                    'preserved_task_count': info['tasks'], 'ledger_sha256': info['sha256'],
                    'operation': operation, 'publication_workers': payload['publish_workers'],
                    'publication_limit': payload['publish_limit'],
                    'source_code_sha256': hashes[entry.name]}
        handover_path = bundle / 'handover.json'
        durable_json(handover_path, handover, write_json)
        record.update(status='frozen_checkpoint', frozen=now(), handover=handover,
                      ledger=info, reconciliation={'pending_seen': frozen_pending,
                                                   'missing_claims_added': missing_claims},
                      frozen_state=state)
        durable_json(bundle / 'frozen-checkpoint.json', record, write_json)
        write_json(deployment, record)
        # No broker drain or node pause: existing leases can finish/claim while
        # only the foreground producer is stopped. Its complete manifests stay.
        guarded_state(control, read_json)
        broker_control(cfg, timeout)
        assert_protected_identities(evidence)
        require_identity(old, states={'T', 't'})
        # Record intent BEFORE signalling. From this point uncertainty must never
        # trigger a second handover or an automatic old-producer restart.
        record.update(status='terminating_old', termination_requested=now(),
                      termination_attempted=True)
        durable_json(deployment, record, write_json)
        # Mark the uncertain boundary before the syscall (a signal to this
        # controller may arrive immediately after the old producer is signalled).
        terminated = True
        os.kill(old['pid'], signal.SIGTERM)
        os.kill(old['pid'], signal.SIGCONT)
        frozen = False
        wait_exited(old, 60)
        old_exited = True
        wait_lock_released('/tmp/' + cfg['run_id'] + '-coordinator.lock', 60)
        guarded_state(control, read_json)
        assert_protected_identities(evidence)
        argv = [sys.executable, '-u', str(entry), 'run', '--control', str(control),
                '--handover', str(handover_path), '--publish-workers',
                str(payload['publish_workers']), '--publish-limit', str(payload['publish_limit'])]
        log = bundle / 'producer.log'
        record.update(status='launching', launch_requested=now(), launch_argv=argv)
        durable_json(deployment, record, write_json)
        with open(log, 'ab', buffering=0) as handle:
            proc = subprocess.Popen(['nice', '-n', '15', 'ionice', '-c', '3', *argv],
                                    stdin=subprocess.DEVNULL, stdout=handle,
                                    stderr=subprocess.STDOUT, env=environment,
                                    start_new_session=True, close_fds=True)
        candidate = None
        for _ in range(200):
            candidate = process_identity(proc.pid)
            if candidate and candidate['state'] not in ('Z', 'X') and candidate['argv'] == argv:
                break
            if proc.poll() is not None:
                raise RuntimeError('new producer exited; inspect log without retrying')
            time.sleep(0.05)
        else:
            raise RuntimeError('new producer identity is uncertain; inspect without retrying')
        role = dict(evidence['old_role'])
        role.update(pid=candidate['pid'], startticks=candidate['starttime_ticks'],
                    argv=argv, bundle=str(bundle), log=str(log), entrypoint=entry.name,
                    handover=str(handover_path), publication_workers=payload['publish_workers'],
                    publication_limit=payload['publish_limit'], deployment_operation=operation)
        write_json(control / 'pipeline-deployment.json', role)
        write_json(control / 'recovery-roles' / (role['host'] + '-producer.json'), role)
        assert_protected_identities(evidence)
        record.update(status='starting_replay', started=now(), producer=role,
                      old_producer_terminated=True,
                      readiness='Not yet ready: require new active_manifest and publication counters')
        write_json(deployment, record)
        return {'status': record['status'], 'operation': operation, 'bundle': str(bundle),
                'producer': role, 'preserved_tasks': info['tasks'], 'next_task_id': next_id}
    except BaseException as error:
        # Resume a pre-TERM frozen producer BEFORE potentially slow NFS audit
        # publication. The final finally block repeats the identity-safe check.
        if frozen and not terminated:
            actual = process_identity(old['pid'])
            if same_identity(actual, old) and actual['state'] in ('T', 't'):
                os.kill(old['pid'], signal.SIGCONT)
                frozen = False
        record.update(status='needs_inspection' if terminated else 'aborted_old_preserved',
                      error=type(error).__name__ + ': ' + str(error),
                      failed=now(), termination_attempted=terminated,
                      old_producer_terminated=old_exited,
                      traceback=traceback.format_exc()[-32000:])
        try:
            write_json(deployment, record)
        except BaseException:
            pass
        raise
    finally:
        if frozen:
            # Resume only the exact producer we stopped, even after pre-TERM
            # exception, SSH hangup or SIGTERM to this controller. Never resume
            # an unrelated process with a reused PID.
            actual = process_identity(old['pid'])
            if same_identity(actual, old) and actual['state'] in ('T', 't'):
                os.kill(old['pid'], signal.SIGCONT)
        if db is not None:
            db.close()
        if ledger is not None and not ledger.closed:
            ledger.close()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        for lock in dispatches:
            lock.close()


def source_payload(directory):
    result = {}
    for name in (*BUNDLE_FILES, 'deploy_pipelined_producer.py'):
        raw = (directory / name).read_bytes()
        result[name] = {'base64': base64.b64encode(raw).decode('ascii'),
                        'sha256': hashlib.sha256(raw).hexdigest()}
    return result


def run_ssh(payload, *, alias, output, timeout):
    """Persist bounded remote output to disk, never accumulate it in memory."""
    source = Path(__file__).read_bytes()
    bootstrap = ('import base64; namespace={"__name__":"deploy_pipelined_remote"};'
                 'exec(compile(base64.b64decode(' + repr(base64.b64encode(source).decode('ascii'))
                 + '),"deploy_pipelined_producer.py","exec"),namespace);'
                 'namespace["remote_main"]()')
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
               '-o', 'ServerAliveInterval=15', alias,
               shlex.join(['python3', '-u', '-c', bootstrap])]
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, 'xb') as log:
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT)
        try:
            proc.communicate(json.dumps(payload).encode('ascii'), timeout=timeout)
        except subprocess.TimeoutExpired:
            # This only tears down SSH; the remote attempt may have progressed.
            # No second mutation is sent and a fresh --apply is NOT a retry.
            proc.terminate()
            proc.wait(timeout=15)
            raise RuntimeError('SSH operation timed out: inspect remote bundle; do not retry')
    with open(output, 'rb') as log:
        log.seek(max(0, output.stat().st_size - MAX_RECORD))
        tail = log.read(MAX_RECORD)
    records = [json.loads(line[7:]) for line in tail.decode(errors='replace').splitlines()
               if line.startswith('RESULT ')]
    if proc.returncode or len(records) != 1:
        raise RuntimeError('remote operation failed/uncertain; inspect ' + str(output))
    return records[0]


def remote_main():
    # The input contains code only, never broker authentication tokens.
    raw = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError('deployment request too large')
    payload = json.loads(raw)
    print('RESULT ' + json.dumps(remote_operation(payload)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--prepare-only', action='store_true')
    modes.add_argument('--apply', action='store_true')
    parser.add_argument('--control')
    parser.add_argument('--active-pointer', type=Path, default=DEFAULT_POINTER)
    parser.add_argument('--ssh-alias', default='copy-master-0')
    parser.add_argument('--publish-workers', type=int, default=8)
    parser.add_argument('--publish-limit', type=int, default=64)
    parser.add_argument('--control-timeout', type=float, default=30)
    parser.add_argument('--ssh-timeout', type=float, default=7200)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not 1 <= args.publish_workers <= 64 or not args.publish_workers <= args.publish_limit <= 4096:
        parser.error('require 1 <= publish-workers <= 64 and workers <= publish-limit <= 4096')
    if not 30 <= args.control_timeout <= 120 or not 300 <= args.ssh_timeout <= 21600:
        parser.error('control timeout must be 30..120s; SSH timeout must be 300..21600s')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.ssh_alias):
        parser.error('SSH alias must be a plain configured host alias')
    from migration_io import read_json, write_json
    control = Path(args.control or read_json(args.active_pointer)['control']).resolve()
    operation = uuid.uuid4().hex
    mode = 'apply' if args.apply else 'prepare-only' if args.prepare_only else 'inspect'
    output = args.output or Path.cwd() / ('pipelined-producer-controller-' + operation + '.json')
    if output.exists() or output.with_name(output.name + '.ssh.log').exists():
        raise FileExistsError('controller output must be a fresh path')
    payload = {'control': str(control), 'operation': operation, 'mode': mode,
               'control_timeout': args.control_timeout, 'publish_workers': args.publish_workers,
               'publish_limit': args.publish_limit}
    if mode != 'inspect':
        payload['files'] = source_payload(Path(__file__).resolve().parent)
    record = {'operation': operation, 'status': 'attempting_once', 'mode': mode,
              'control': str(control), 'created': now(), 'ssh_alias': args.ssh_alias,
              'bundle': str(control / ('pipelined-producer-' + operation)) if mode != 'inspect' else None}
    write_json(output, record)
    try:
        result = run_ssh(payload, alias=args.ssh_alias,
                         output=output.with_name(output.name + '.ssh.log'), timeout=args.ssh_timeout)
        record.update(status=result['status'], result=result, completed=now())
        write_json(output, record)
        print(json.dumps(result))
    except BaseException as error:
        record.update(status='needs_inspection' if mode == 'apply' else 'inspection_or_prepare_failed',
                      error=type(error).__name__ + ': ' + str(error), failed=now())
        write_json(output, record)
        raise


if __name__ == '__main__':
    main()
