#!/usr/bin/env python3
"""One explicit node/lane canary; replace only a proven idle copy process.

Deploy this immutable helper beside migration_io.py first, then invoke locally:
  rollout_cache_age.py --node worker-0 --lane supplement --remote-script PATH
The same file serves remote actions. It never alters copy/SHA code, budgets,
training processes, other lanes, or workers carrying payload file descriptors.
"""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import resource
import select
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
import uuid

from migration_io import read_json, write_json

BASE = Path(__file__).resolve().parent
TERMINAL = {'complete', 'complete_with_changes', 'failed', 'stopped'}
COUNTERS = ('bytes', 'streamed_bytes', 'verified_bytes', 'verified_files', 'completed_batches', 'broker_retries')


def lane_owners(host, lane):
    if lane not in ('primary', 'supplement'):
        raise ValueError('one primary or supplement lane is required')
    return [host + '-' + str(i) for i in range(0 if lane == 'primary' else 64,
                                               64 if lane == 'primary' else 128)]


def lane_leases(db, host, lane):
    owners = lane_owners(host, lane)
    query = "SELECT stage,COUNT(*) FROM claims WHERE status='leased' AND owner IN (" + ','.join('?' for _ in owners) + ') GROUP BY stage'
    return dict(db.execute(query, owners))


def epoch(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def same_identity(first, second):
    return (first.get('pid'), str(first.get('starttime_ticks')), first.get('argv')) == (
            second.get('pid'), str(second.get('starttime_ticks')), second.get('argv'))


def counter_check(before, after):
    def compare(old, new, prefix=''):
        for key, value in old.items():
            if isinstance(value, dict):
                if not isinstance(new.get(key), dict):
                    raise RuntimeError('counter mapping disappeared: ' + prefix + key)
                compare(value, new[key], prefix + key + '.')
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                if new.get(key, 0) < value:
                    raise RuntimeError('counter regressed: ' + prefix + key)
    compare({key: before.get(key, 0) for key in COUNTERS}, after)
    for key in ('counts', 'stages'):
        compare(before.get(key, {}), after.get(key, {}), key + '.')
    if after.get('run_started') != before.get('run_started', before.get('started')):
        raise RuntimeError('run_started changed')


def idle_sample(snapshot, expected, barrier, now=None):
    now = time.time() if now is None else now
    return (snapshot.get('pid') == expected['pid'] and not snapshot.get('active') and
            not snapshot.get('ended') and not snapshot.get('thread_error') and
            epoch(snapshot['updated']) >= barrier and 0 <= now - epoch(snapshot['updated']) <= 10)


def proc_identity(pid):
    root = Path('/proc') / str(pid)
    fields = (root / 'stat').read_text().rsplit(') ', 1)[1].split()
    return {'pid': pid, 'state': fields[0], 'starttime_ticks': fields[19],
            'argv': (root / 'cmdline').read_bytes().decode(errors='surrogateescape').strip('\0').split('\0')}


def payload_fds(pid, roots):
    found = []
    for fd in (Path('/proc') / str(pid) / 'fd').iterdir():
        try:
            target = os.readlink(fd).removesuffix(' (deleted)')
        except OSError:
            continue
        if any(target == root or target.startswith(root + '/') for root in roots):
            found.append({'fd': fd.name, 'path': target})
    return found


def remote(a):
    control = Path(a.control).resolve()
    cfg = read_json(control / 'config.json')
    if (cfg.get('source'), cfg.get('destination')) != ('/share/project/eai_pwm', '/share-mixed/project/eai_pwm'):
        raise ValueError('cache-age canary is restricted to the current eai_pwm copy')
    host, lane = a.host, a.lane
    os.umask(0o077)
    record_path = Path(__file__).resolve().parent / 'cache-age-rollout' / ((host or 'master') + '-' + (lane or 'all') + '.json')
    node_path = control / ('nodes' if lane == 'primary' else 'nodes-extra') / ((host or '-') + '.json')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def result(value):
        print('RESULT ' + json.dumps(value), flush=True)
    def active():
        return not (control / 'STOP').exists() and read_json(control / 'state.json').get('status') not in TERMINAL
    def request(value=None):
        data = None if value is None else json.dumps(value).encode()
        req = urllib.request.Request(cfg['broker_url'] + '/control', data=data,
            method='GET' if data is None else 'POST', headers={
                'Authorization': 'Bearer ' + cfg['broker_token'], 'Content-Type': 'application/json'})
        with opener.open(req, timeout=10) as response:
            return json.load(response)
    def snapshot():
        return read_json(node_path)
    def save(record):
        record['updated'] = time.time()
        write_json(record_path, record)
    def own_record():
        record = read_json(record_path)
        if record.get('operation') != a.operation:
            raise RuntimeError('operation identity differs; do not mutate another rollout')
        return record
    def guard_worker(identity):
        current = proc_identity(identity['pid'])
        if current['state'] == 'Z' or not same_identity(identity, current):
            raise RuntimeError('copy worker identity changed')
        argv = current['argv']
        if (str(control) not in argv or '--control' not in argv or lane not in argv or
                not any(Path(value).name == 'migration_cached_worker.py' for value in argv)):
            raise RuntimeError('expected exactly the cached copy lane, never another process')
        return current
    if a.action == 'hold':
        lock = open('/tmp/' + cfg['run_id'] + '-cache-age-dispatch.lock', 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result({'locked': True, 'identity': proc_identity(os.getpid())})
        sys.stdin.read()
        return
    if a.action == 'inspect':
        if socket.gethostname() != host:
            raise RuntimeError('host mismatch')
        n = snapshot()
        identity = proc_identity(n['pid'])
        guard_worker(identity)
        try:
            record = read_json(record_path)
        except FileNotFoundError:
            record = {}
        new = record.get('replacement')
        adopted_ready = False
        if (record.get('operation') == a.operation and new and same_identity(new, identity)
                and not n.get('ended') and not n.get('thread_error')):
            counter_check(record['idle'], n)
            adopted_ready = (n.get('threads') == 64 and n.get('io_bytes_per_second') == record['idle']['io_bytes_per_second']
                             and '--cache-max-age' in identity['argv']
                             and identity['argv'][identity['argv'].index('--cache-max-age') + 1] == str(float(a.max_age)))
        result({'identity': identity, 'active_count': len(n.get('active', {})),
                'payload_fd_count': len(payload_fds(n['pid'], (cfg['source'], cfg['destination']))),
                'record_status': record.get('status'), 'replacement_ready': adopted_ready})
        return
    if a.action == 'observe':
        try:
            record = read_json(record_path)
        except FileNotFoundError:
            record = {}
        result({'status': record.get('status'),
                'drain_owned_current': record.get('operation') == a.operation and record.get('added_by_this_rollout') is True,
                'draining': host in request()['draining_hosts']})
        return
    if a.action == 'drain':
        if not active():
            raise RuntimeError('STOP or terminal state')
        ctl = request()
        hosts = set(ctl['draining_hosts'])
        try:
            prior = read_json(record_path)
        except FileNotFoundError:
            prior = {}
        if prior.get('status') == 'complete' and prior.get('max_age') == a.max_age:
            result({'skipped': True, 'already_complete': True})
            return
        if prior.get('status') not in (None, 'waiting_payload', 'skipped_preexisting_drain'):
            raise RuntimeError('unfinished rollout requires identity inspection before retry')
        if host in hosts:
            result({'skipped': True, 'reason': 'preexisting host drain'})
            return
        identity = json.loads(a.identity_json)
        before = snapshot()
        if before['pid'] != identity['pid']:
            raise RuntimeError('lane changed before drain')
        record = {'host': host, 'lane': lane, 'expected': identity, 'before': before,
                  'max_age': a.max_age, 'status': 'drain_requested', 'added_by_this_rollout': True,
                  'prior_drains': sorted(hosts), 'started': time.time(), 'operation': a.operation}
        save(record)  # Restoration intent must precede even a lost POST reply.
        hosts.add(host)
        request({'draining_hosts': sorted(hosts)})
        record.update(status='draining', barrier=time.time())
        save(record)
        result({'draining': True})
        return
    if a.action == 'release':
        record = own_record()
        hosts = set(request()['draining_hosts'])
        if record.get('added_by_this_rollout'):
            hosts.discard(host)
            request({'draining_hosts': sorted(hosts)})
        record['drain_released'] = time.time()
        record['status'] = 'complete' if record.get('status') == 'workers_ready' else 'waiting_payload'
        save(record)
        result({'released': True, 'draining_hosts': sorted(hosts), 'status': record['status']})
        return
    if a.action == 'gate':
        record = own_record()
        started = time.monotonic()
        previous_updated = None
        good = 0
        db_path = Path('/tmp') / (cfg['run_id'] + '-broker.sqlite')
        db = sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True, timeout=2)
        try:
            while time.monotonic() - started < a.gate_timeout:
                if not active():
                    result({'ready': False, 'terminal_or_stop': True})
                    return
                if host not in request()['draining_hosts']:
                    raise RuntimeError('host drain barrier disappeared')
                leases = lane_leases(db, host, lane)
                n = snapshot()
                if n['pid'] != record['expected']['pid']:
                    raise RuntimeError('lane PID changed during gate')
                if not leases and idle_sample(n, record['expected'], record['barrier']):
                    if n['updated'] != previous_updated:
                        good += 1
                        previous_updated = n['updated']
                    if good >= 2:
                        record.update(status='idle_verified', idle=n, idle_verified=time.time(),
                                      lane_leases={}, samples=good)
                        save(record)
                        result({'ready': True, 'wait_seconds': time.monotonic() - started})
                        return
                else:
                    good = 0
                    previous_updated = None
                time.sleep(.25)
        finally:
            db.close()
        result({'ready': False, 'skipped': True, 'reason': 'lane not idle within gate; payload retained'})
        return
    if a.action != 'replace':
        raise ValueError('unknown action')
    if socket.gethostname() != host:
        raise RuntimeError('host mismatch')
    lock = open('/tmp/' + cfg['run_id'] + '-' + lane + '-cache-age-replace.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    record = own_record()
    if record.get('status') != 'idle_verified' or record.get('lane_leases') != {}:
        raise RuntimeError('master has not proved this lane lease-free')
    if not active() or host not in request()['draining_hosts']:
        raise RuntimeError('STOP, terminal, or lost host drain')
    current = guard_worker(record['expected'])
    n = snapshot()
    if not idle_sample(n, record['expected'], record['barrier']) or payload_fds(n['pid'], (cfg['source'], cfg['destination'])):
        raise RuntimeError('lane gained work or has payload descriptors; never interrupt')
    wrapper = next(value for value in current['argv'] if Path(value).name == 'migration_cached_worker.py')
    if a.wrapper:
        from hashlib import sha256
        previous = Path(wrapper).parent
        proposed = Path(a.wrapper).resolve().parent
        for name in ('migration_run.py', 'migration_worker.py', 'migration_scan.py',
                     'migration_worker128_run.py', 'migration_io.py', 'migration_nodes.py'):
            if sha256((previous / name).read_bytes()).digest() != sha256((proposed / name).read_bytes()).digest():
                raise RuntimeError('replacement data engine differs: ' + name)
        wrapper = a.wrapper
    if not Path(wrapper).is_file() or os.path.commonpath((str(control), str(Path(wrapper).resolve()))) != str(control):
        raise RuntimeError('immutable copy wrapper must remain within this control')
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('NOFILE hard limit cannot support the required65536')
    # The maintained host drain forbids new claims after the master's exact
    # per-owner lease-zero proof; the other lane may keep existing large I/O.
    record.update(status='replacing', helper=proc_identity(os.getpid()))
    save(record)
    os.kill(current['pid'], signal.SIGTERM)
    record['stop_sent'] = True
    save(record)
    def alive(identity):
        try:
            value = proc_identity(identity['pid'])
            return value['state'] != 'Z' and same_identity(value, identity)
        except (FileNotFoundError, ProcessLookupError):
            return False
    end = time.monotonic() + 15
    while alive(current) and time.monotonic() < end:
        time.sleep(.1)
    if alive(current):
        raise RuntimeError('idle worker did not exit; no SIGKILL and no duplicate launch')
    suffix = '-worker.lock' if lane == 'primary' else '-extra64-worker.lock'
    end = time.monotonic() + 15
    while True:
        try:
            with open('/tmp/' + cfg['run_id'] + suffix, 'a') as worker_lock:
                fcntl.flock(worker_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(worker_lock, fcntl.LOCK_UN)
            break
        except BlockingIOError:
            if time.monotonic() >= end:
                raise RuntimeError('old lane flock remains held')
            time.sleep(.1)
    if not active() or host not in request()['draining_hosts']:
        raise RuntimeError('STOP or lost drain before new launch')
    wanted = 65536 if hard == resource.RLIM_INFINITY else min(65536, hard)
    if soft < wanted:
        resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
    command = [sys.executable, '-u', wrapper, lane, '--control', str(control), '--threads', '64',
               '--cache-max-age', str(float(a.max_age))]
    if lane == 'supplement':
        command.extend(['--nice', '0'])
    command = ['nice', '-n', '15'] + command
    if shutil.which('ionice'):
        command = ['ionice', '-c', '3'] + command
    environment = dict(os.environ, PYTHONPYCACHEPREFIX='/tmp/' + cfg['run_id'] + '-pycache-' + lane)
    log = record_path.parent / (host + '-' + lane + '.log')
    with open(log, 'ab', buffering=0) as output:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output,
            stderr=subprocess.STDOUT, start_new_session=True, close_fds=True, env=environment)
    record.update(status='launched', replacement_pid=child.pid, launched=time.time())
    save(record)
    end = time.monotonic() + 45
    while time.monotonic() < end:
        if child.poll() is not None:
            raise RuntimeError('replacement exited; inspect checkpoint and actual identity before recovery')
        n = snapshot()
        if n.get('pid') == child.pid and not n.get('ended') and not n.get('thread_error'):
            if n.get('threads') != 64 or n.get('io_bytes_per_second') != record['idle']['io_bytes_per_second'] or n.get('active'):
                raise RuntimeError('replacement resource budget or drain invariant changed')
            counter_check(record['idle'], n)
            identity = proc_identity(child.pid)
            if identity['state'] == 'Z':
                raise RuntimeError('replacement exited after heartbeat')
            record.update(status='workers_ready', replacement=identity, ready=time.time())
            save(record)
            result({'ready': True, 'replacement': identity, 'threads': 64,
                    'io_bytes_per_second': n['io_bytes_per_second']})
            return
        time.sleep(.25)
    raise RuntimeError('replacement heartbeat timeout; do not relaunch blindly')


def roll_one(call, node, lane, *, gate_timeout=5, max_age=30):
    """Single-lane transaction; transport failures never repeat replacement."""
    before = call(node, 'inspect', lane=lane)
    try:
        drain = call('master-0', 'drain', lane=lane, identity=before['identity'])
    except BaseException:
        # A POST may have succeeded before its SSH response disappeared. Only
        # the exact operation's persisted intent permits restoring its drain.
        observed = call('master-0', 'observe', lane=lane)
        if observed.get('drain_owned_current'):
            call('master-0', 'release', lane=lane)
        raise
    if drain.get('skipped'):
        return {'status': 'skipped', 'detail': drain}
    attempted = False
    released = False
    try:
        gate = call('master-0', 'gate', lane=lane)
        if not gate.get('ready'):
            return {'status': 'waiting_payload', 'gate': gate}
        attempted = True
        try:
            replacement = call(node, 'replace', lane=lane)
        except BaseException:
            # A lost RPC response is not proof of failure. Observe the exact
            # replacement checkpoint/identity once; never send replace twice.
            observed = call(node, 'inspect', lane=lane)
            if not observed.get('replacement_ready'):
                raise RuntimeError('replacement outcome needs inspection; host drain retained')
            replacement = {'adopted_after_rpc_loss': True, **observed}
        try:
            release = call('master-0', 'release', lane=lane)
        except BaseException:
            observed = call('master-0', 'observe', lane=lane)
            if observed.get('draining') or observed.get('status') != 'complete':
                raise
            release = {'adopted_after_rpc_loss': True, **observed}
        released = True
        return {'status': 'complete', 'replacement': replacement, 'release': release}
    finally:
        if not attempted and not released:
            call('master-0', 'release', lane=lane)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--node', choices=['master-0'] + ['worker-' + str(i) for i in range(11)])
    ap.add_argument('--lane', choices=('primary', 'supplement'))
    ap.add_argument('--max-age', type=float, default=30)
    ap.add_argument('--gate-timeout', type=float, default=5)
    ap.add_argument('--remote-script')
    ap.add_argument('--wrapper')
    ap.add_argument('--action', choices=('hold', 'inspect', 'observe', 'drain', 'gate', 'replace', 'release'))
    ap.add_argument('--control')
    ap.add_argument('--host')
    ap.add_argument('--identity-json')
    ap.add_argument('--operation')
    a = ap.parse_args()
    if a.max_age != 30 or not 0 < a.gate_timeout <= 5:
        ap.error('this canary fixes max-age=30 and idle gate at most5s')
    if a.action:
        if not a.control or (a.action != 'hold' and (not a.host or not a.lane)):
            ap.error('remote actions require control and exact host/lane')
        if a.action not in ('hold', 'inspect') and not a.operation:
            ap.error('remote mutations and observations require exact operation identity')
        return remote(a)
    if not a.node or not a.lane or not a.remote_script:
        ap.error('select exactly --node, --lane and deployed --remote-script')
    import migration_launch as transport
    active = read_json(BASE / 'active_migration.json')
    control = active['control']
    host = 'job-' + active['job'] + '-' + a.node
    operation = uuid.uuid4().hex
    local_lock = open(BASE / 'rollout_cache_age.lock', 'a')
    fcntl.flock(local_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    def command(node, action, identity=None):
        destination = transport.ssh_destination(node, job=active['job'], owner=active.get('ssh_owner'))
        args = ['python3', '-u', a.remote_script, '--action', action, '--control', control,
                '--max-age', str(a.max_age), '--gate-timeout', str(a.gate_timeout), '--operation', operation]
        if action != 'hold':
            args.extend(['--host', host, '--lane', a.lane])
        if identity is not None:
            args.extend(['--identity-json', json.dumps(identity)])
        if a.wrapper:
            args.extend(['--wrapper', a.wrapper])
        return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15',
                '-p', '2222', destination, shlex.join(args)]
    def call(node, action, lane=None, identity=None):
        result = subprocess.run(command(node, action, identity), stdin=subprocess.DEVNULL,
            text=True, capture_output=True, timeout=80 if action == 'replace' else 30)
        values = [json.loads(line[7:]) for line in result.stdout.splitlines() if line.startswith('RESULT ')]
        if result.returncode or not values:
            (BASE / 'rollout_cache_age_error.txt').write_text(result.stdout + result.stderr)
            raise RuntimeError(action + ' RPC failed; inspect persistent helper checkpoint')
        return values[-1]
    hold = subprocess.Popen(command('master-0', 'hold'), stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    record = {'control': control, 'job': active['job'], 'node': a.node, 'host': host,
              'lane': a.lane, 'max_age': a.max_age, 'operation': operation,
              'started': time.time(), 'status': 'acquiring_dispatch'}
    path = BASE / 'rollout_cache_age.json'
    try:
        readable, _, _ = select.select([hold.stdout], [], [], 30)
        if not readable:
            raise RuntimeError('master dispatch lock observation timed out')
        line = hold.stdout.readline()
        if not line.startswith('RESULT ') or not json.loads(line[7:]).get('locked'):
            raise RuntimeError('master dispatch lock not acquired')
        record['status'] = 'rolling'
        write_json(path, record)
        outcome = roll_one(call, a.node, a.lane, gate_timeout=a.gate_timeout, max_age=a.max_age)
        record.update(outcome, updated=time.time())
        write_json(path, record)
        print(json.dumps(record, ensure_ascii=False), flush=True)
    except BaseException as exc:
        record.update(status='attention', error=type(exc).__name__ + ': ' + str(exc), updated=time.time())
        write_json(path, record)
        raise
    finally:
        if hold.stdin:
            hold.stdin.close()
        try:
            hold.wait(timeout=10)
        except subprocess.TimeoutExpired:
            hold.terminate()


if __name__ == '__main__':
    main()
