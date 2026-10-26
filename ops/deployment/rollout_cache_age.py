#!/usr/bin/env python3
"""One explicit node/lane canary; replace only a proven idle copy process.

Checkpoint-visibility protocol 2 is a local candidate until separately deployed.
Older deployed helpers fail closed at the controller's visibility check.

Deploy this immutable helper beside migration_io.py first, then invoke locally:
  rollout_cache_age.py --node worker-0 --lane supplement --remote-script PATH
The same file serves remote actions. It never alters copy/SHA code, budgets,
training processes, other lanes, or workers carrying payload file descriptors.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import math
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
ENGINE_FILES = ('migration_run.py', 'migration_worker.py', 'migration_scan.py',
                'migration_worker128_run.py', 'migration_io.py', 'migration_nodes.py')


def validate_engine_transition(previous, proposed, manifest_path, control):
    """Permit one explicitly hash-bound worker change, never a generic upgrade.

    Without a manifest this retains the original six-file equality gate. The
    optional static manifest must be inside the new immutable bundle and bind
    its exact control, bundle, scope and the complete old/new worker sources.
    All five other engine files must still match byte for byte.
    """
    previous, proposed, control = (Path(value).resolve() for value in
                                   (previous, proposed, control))
    proof = None
    if manifest_path is not None:
        manifest = Path(manifest_path).resolve()
        if (not previous.is_relative_to(control) or
                not proposed.is_relative_to(control) or proposed == control or
                not manifest.is_relative_to(proposed) or manifest == proposed):
            raise RuntimeError('engine manifest must remain inside the proposed control bundle')
        payload = manifest.read_bytes()
        if len(payload) > 65536:
            raise RuntimeError('engine transition manifest is oversized')
        data = json.loads(payload)
        if (not isinstance(data, dict) or data.get('scope') not in
                ('directory-open-first-v1', 'destination-opath-checkpoint-v1') or
                data.get('control') != str(control) or data.get('bundle') != str(proposed)):
            raise RuntimeError('engine transition manifest scope/control/bundle differs')
        for key in ('old_sha256', 'new_sha256'):
            value = data.get(key)
            if (not isinstance(value, str) or len(value) != 64 or
                    any(character not in '0123456789abcdef' for character in value)):
                raise RuntimeError('engine transition requires full source SHA256: ' + key)
        path_only = data['scope'] == 'destination-opath-checkpoint-v1'
        if not path_only and data['old_sha256'] == data['new_sha256']:
            raise RuntimeError('engine transition must bind different old/new worker sources')
        if path_only and data['old_sha256'] != data['new_sha256']:
            raise RuntimeError('O_PATH wrapper transition must keep the complete data engine unchanged')
        proof = {'scope': data['scope'], 'control': str(control), 'bundle': str(proposed),
                 'manifest_path': str(manifest),
                 'manifest_sha256': hashlib.sha256(payload).hexdigest(),
                 'old_sha256': data['old_sha256'], 'new_sha256': data['new_sha256'],
                 'engine_sha256': {}}
        if path_only:
            proof['launcher_sha256'] = {}
            names = ('migration_cached_worker.py', 'migration_linux_open.py',
                     'worker-directory-policy.json')
            if (not isinstance(data.get('launcher_sha256'), dict) or
                    set(data['launcher_sha256']) != set(names)):
                raise RuntimeError('O_PATH launcher digest manifest is incomplete')
            for name in names:
                expected = data['launcher_sha256'][name]
                source = proposed / name
                if (source.is_symlink() or source.resolve().parent != proposed
                        or expected != hashlib.sha256(source.read_bytes()).hexdigest()):
                    raise RuntimeError('O_PATH launcher source digest differs: ' + name)
                proof['launcher_sha256'][name] = expected
            if json.loads((proposed / 'worker-directory-policy.json').read_bytes()) != {
                    'scope': 'destination-opath-checkpoint-v1'}:
                raise RuntimeError('O_PATH launcher policy differs')
    for name in ENGINE_FILES:
        old_path, new_path = previous / name, proposed / name
        if proof is not None and (old_path.resolve().parent != previous or
                                  new_path.resolve().parent != proposed):
            raise RuntimeError('engine source escapes its immutable bundle: ' + name)
        old_hash = hashlib.sha256(old_path.read_bytes()).hexdigest()
        new_hash = hashlib.sha256(new_path.read_bytes()).hexdigest()
        if proof is not None:
            proof['engine_sha256'][name] = {'old': old_hash, 'new': new_hash}
        if name == 'migration_worker.py' and proof is not None:
            if (old_hash, new_hash) != (proof['old_sha256'], proof['new_sha256']):
                raise RuntimeError('worker transition source SHA256 differs')
        elif old_hash != new_hash:
            raise RuntimeError('replacement data engine differs: ' + name)
    return proof


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


def visible_idle_checkpoint(observed, gate, expected):
    """A master-written idle record must also be visible on the worker node."""
    if not all(isinstance(value, dict) for value in (observed, gate, expected)):
        return False
    identity = expected.get('identity', {})
    if not all(isinstance(value, dict) for value in
               (identity, observed.get('record_expected'), observed.get('identity'))):
        return False
    complete_identity = (type(identity.get('pid')) is int and identity['pid'] > 0 and
                         str(identity.get('starttime_ticks', '')).isdigit() and
                         isinstance(identity.get('argv'), list) and bool(identity['argv']))
    return bool(complete_identity and gate.get('checkpoint_protocol') == 2 and
                isinstance(gate.get('operation'), str) and gate['operation'] and
                observed.get('checkpoint_protocol') == 2 and
                observed.get('record_operation') == gate['operation'] and
                observed.get('record_status') == 'idle_verified' and
                same_identity(observed.get('record_expected', {}), identity) and
                same_identity(observed.get('identity', {}), identity) and
                observed.get('active_count') == 0 and observed.get('payload_fd_count') == 0)


def checkpoint_can_propagate(observed, gate, expected):
    """Wait only for an old/missing record while every live idle guard holds.

    A different actual process, active work, payload FD, or protocol mismatch
    ends observation immediately. A malformed current-operation idle proof is
    not treated as an NFS propagation delay.
    """
    if not all(isinstance(value, dict) for value in (observed, gate, expected)):
        return False
    identity = expected.get('identity')
    if not isinstance(identity, dict):
        return False
    candidate = dict(observed, record_operation=gate.get('operation'),
                     record_expected=identity, record_status='idle_verified')
    if not visible_idle_checkpoint(candidate, gate, expected):
        return False
    return (observed.get('record_operation') != gate.get('operation') or
            observed.get('record_status') in (None, 'drain_requested', 'draining'))


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
        with opener.open(req, timeout=getattr(a, 'control_timeout', 10)) as response:
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
                'record_status': record.get('status'), 'replacement_ready': adopted_ready,
                'record_operation': record.get('operation'),
                'record_expected': record.get('expected'), 'checkpoint_protocol': 2})
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
                        result({'ready': True, 'wait_seconds': time.monotonic() - started,
                                'operation': record['operation'], 'checkpoint_protocol': 2})
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
    engine_manifest = getattr(a, 'engine_manifest', None)
    engine_transition = None
    previous = proposed = None
    if engine_manifest and not a.wrapper:
        raise RuntimeError('engine transition manifest requires an explicit replacement wrapper')
    if a.wrapper:
        previous = Path(wrapper).parent
        proposed = Path(a.wrapper).resolve().parent
        engine_transition = validate_engine_transition(previous, proposed, engine_manifest, control)
        wrapper = a.wrapper
    if not Path(wrapper).is_file() or os.path.commonpath((str(control), str(Path(wrapper).resolve()))) != str(control):
        raise RuntimeError('immutable copy wrapper must remain within this control')
    guarded_startup = bool(engine_transition and engine_transition['scope'] ==
                           'destination-opath-checkpoint-v1')
    if guarded_startup:
        from migration_cached_worker import StartupProgressGuard
        # This helper and the new wrapper live in the same immutable bundle.
        # A bad checkpoint must fail before the first signal, and the wrapper
        # rechecks the actual node read under the worker's singleton flock.
        StartupProgressGuard(control, lane, record_path, a.operation,
                             read_json, proposed).preflight()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('NOFILE hard limit cannot support the required65536')
    # The maintained host drain forbids new claims after the master's exact
    # per-owner lease-zero proof; the other lane may keep existing large I/O.
    record.update(status='replacing', helper=proc_identity(os.getpid()))
    if engine_transition is not None:
        record['engine_transition'] = engine_transition
    save(record)
    if a.wrapper:
        # Check again after the checkpoint's shared-filesystem publication and
        # immediately before the first signal. Changed inputs never stop a lane.
        if validate_engine_transition(previous, proposed, engine_manifest, control) != engine_transition:
            raise RuntimeError('engine transition manifest changed before signal')
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
    if guarded_startup:
        command.extend(['--progress-checkpoint', str(record_path),
                        '--progress-operation', a.operation])
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


def roll_one(call, node, lane, *, gate_timeout=5, max_age=30, visibility_timeout=0):
    """Single-lane transaction; transport failures never repeat replacement."""
    if not math.isfinite(visibility_timeout) or visibility_timeout < 0:
        raise ValueError('visibility timeout must be finite and nonnegative')
    before = call(node, 'inspect', lane=lane)
    if before.get('checkpoint_protocol') != 2:
        return {'status': 'skipped', 'reason': 'deployed helper lacks checkpoint visibility protocol 2'}
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
        # NFS may still hide this newly published record from the worker.
        # This RPC only observes; no replacement has been dispatched yet, so
        # a missing/stale record or transport failure can release our drain.
        visibility_deadline = time.monotonic() + visibility_timeout if visibility_timeout else None
        while True:
            try:
                visible = call(node, 'inspect', lane=lane)
            except Exception as exc:
                return {'status': 'waiting_payload', 'gate': gate,
                        'reason': 'worker checkpoint visibility was not confirmed',
                        'visibility_error': type(exc).__name__ + ': ' + str(exc)}
            if visible_idle_checkpoint(visible, gate, before):
                break
            if visibility_deadline is not None and checkpoint_can_propagate(visible, gate, before):
                remaining = visibility_deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(.25, remaining))
                    continue
            return {'status': 'waiting_payload', 'gate': gate,
                    'reason': 'worker checkpoint visibility or idle identity differs',
                    'checkpoint_visibility': visible}
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
        return {'status': 'complete', 'replacement': replacement, 'release': release,
                'checkpoint_visibility': visible}
    finally:
        if not attempted and not released:
            call('master-0', 'release', lane=lane)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--node', choices=['master-0'] + ['worker-' + str(i) for i in range(11)])
    ap.add_argument('--lane', choices=('primary', 'supplement'))
    ap.add_argument('--max-age', type=float, default=30)
    ap.add_argument('--gate-timeout', type=float, default=5)
    ap.add_argument('--visibility-timeout', type=float, default=0,
                    help='seconds of read-only observation for a delayed idle checkpoint (default: 0)')
    ap.add_argument('--control-timeout', type=float, default=10,
                    help='timeout for each broker control request, without retries (default: 10)')
    ap.add_argument('--remote-script')
    ap.add_argument('--wrapper')
    ap.add_argument('--engine-manifest', help='explicit hash-bound directory-open-first worker transition')
    ap.add_argument('--action', choices=('hold', 'inspect', 'observe', 'drain', 'gate', 'replace', 'release'))
    ap.add_argument('--control')
    ap.add_argument('--host')
    ap.add_argument('--identity-json')
    ap.add_argument('--operation')
    a = ap.parse_args()
    if a.engine_manifest and not a.wrapper:
        ap.error('--engine-manifest requires an explicit --wrapper')
    if a.max_age != 30 or not 0 < a.gate_timeout <= 30:
        ap.error('this canary fixes max-age=30 and idle gate at most30s')
    if not math.isfinite(a.visibility_timeout) or a.visibility_timeout < 0:
        ap.error('visibility timeout must be finite and nonnegative')
    if not math.isfinite(a.control_timeout) or not 0 < a.control_timeout <= 60:
        ap.error('control timeout must be finite and 0<control-timeout<=60')
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
                '--max-age', str(a.max_age), '--gate-timeout', str(a.gate_timeout),
                '--control-timeout', str(a.control_timeout), '--operation', operation]
        if action != 'hold':
            args.extend(['--host', host, '--lane', a.lane])
        if identity is not None:
            args.extend(['--identity-json', json.dumps(identity)])
        if a.wrapper:
            args.extend(['--wrapper', a.wrapper])
        if a.engine_manifest:
            args.extend(['--engine-manifest', a.engine_manifest])
        return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15',
                '-p', '2222', destination, shlex.join(args)]
    def call(node, action, lane=None, identity=None):
        result = subprocess.run(command(node, action, identity), stdin=subprocess.DEVNULL,
            text=True, capture_output=True, timeout=(a.control_timeout + 90 if action == 'replace' else
                a.gate_timeout + a.control_timeout + 20 if action == 'gate' else a.control_timeout + 20))
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
        outcome = roll_one(call, a.node, a.lane, gate_timeout=a.gate_timeout, max_age=a.max_age,
                           visibility_timeout=a.visibility_timeout)
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
