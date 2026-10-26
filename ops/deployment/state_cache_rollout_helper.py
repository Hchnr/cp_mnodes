"""Drain and replace only idle copy workers; never interrupt payload I/O."""
import argparse
import datetime
import fcntl
import json
import os
import pathlib
import resource
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request

from migration_io import read_json, write_json

TERMINAL = {'complete', 'complete_with_changes', 'failed', 'stopped'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--control', required=True)
    ap.add_argument('--action', choices=('hold', 'drain', 'gate', 'replace', 'release'), required=True)
    ap.add_argument('--host')
    ap.add_argument('--timeout', type=float, default=600)
    a = ap.parse_args()
    c = pathlib.Path(a.control)
    bundle = pathlib.Path(__file__).absolute().parent
    cfg = read_json(c / 'config.json')
    os.umask(0o077)
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(method='GET', value=None):
        body = None if value is None else json.dumps(value).encode()
        req = urllib.request.Request(cfg['broker_url'] + '/control', data=body, method=method,
            headers={'Authorization': 'Bearer ' + cfg['broker_token'], 'Content-Type': 'application/json'})
        with op.open(req, timeout=10) as response:
            return json.load(response)

    def active():
        s = read_json(c / 'state.json')
        return not (c / 'STOP').exists() and s.get('status') not in TERMINAL

    def snapshots():
        return {lane: read_json(c / folder / (a.host + '.json')) for lane, folder in
                (('primary', 'nodes'), ('supplement', 'nodes-extra'))}

    def epoch(value):
        return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()

    def result(value):
        print('RESULT ' + json.dumps(value), flush=True)

    record_path = bundle / 'rollout' / ((a.host or 'master') + '.json')
    if a.action == 'hold':
        with open('/tmp/' + cfg['run_id'] + '-state-cache-rollout.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result({'locked': True, 'pid': os.getpid()})
            sys.stdin.read()
        return
    if a.action == 'drain':
        assert active(), 'STOP or terminal state protects training'
        ctl = request()
        hosts = set(ctl['draining_hosts'])
        if a.host in hosts:
            result({'skipped': True, 'reason': 'preexisting drain'}); return
        record = {'host': a.host, 'status': 'drain_requested', 'added_by_this_rollout': True,
                  'prior_drains': sorted(hosts), 'before': snapshots(), 'started': time.time()}
        # Persist restoration intent BEFORE the POST, including lost replies.
        write_json(record_path, record)
        hosts.add(a.host)
        request('POST', {'draining_hosts': sorted(hosts)})
        record.update(status='draining', barrier_time=time.time())
        write_json(record_path, record)
        result({'draining': True, 'host': a.host}); return
    if a.action == 'release':
        record = read_json(record_path)
        ctl = request()
        hosts = set(ctl['draining_hosts'])
        if record.get('added_by_this_rollout'):
            hosts.discard(a.host)
            request('POST', {'draining_hosts': sorted(hosts)})
        record.update(drain_released=time.time())
        write_json(record_path, record)
        result({'released': True, 'draining_hosts': sorted(hosts)}); return
    if a.action == 'gate':
        record = read_json(record_path)
        began = time.monotonic(); zero_since = None
        while time.monotonic() - began < a.timeout:
            if not active():
                result({'ready': False, 'terminal_or_stop': True}); return
            ctl = request()
            assert a.host in ctl['draining_hosts'], 'drain barrier disappeared'
            leased = ctl['leased_by_host'].get(a.host, 0)
            if leased:
                zero_since = None
            elif zero_since is None:
                zero_since = time.time()
            ns = snapshots()
            same = all(n['pid'] == record['before'][lane]['pid'] for lane, n in ns.items())
            assert same, 'worker identity changed during drain'
            if any(n.get('thread_error') or n.get('last_error') for n in ns.values()):
                raise RuntimeError('worker error during drain')
            fresh = zero_since and all(epoch(n['updated']) >= zero_since and
                                      not n.get('active') and not n.get('ended') for n in ns.values())
            if not leased and fresh and time.time() - zero_since >= 3:
                record.update(status='idle_verified', zero_since=zero_since, idle=ns,
                              idle_verified=time.time())
                write_json(record_path, record)
                result({'ready': True, 'host': a.host, 'wait_seconds': time.monotonic() - began}); return
            time.sleep(2)
        result({'ready': False, 'skipped': True, 'reason': 'waiting for existing payloads; never force kill'}); return

    assert a.action == 'replace'
    assert socket.gethostname() == a.host
    node_lock = open('/tmp/' + cfg['run_id'] + '-state-cache-node-replace.lock', 'a')
    fcntl.flock(node_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    record = read_json(record_path)
    assert record['status'] == 'idle_verified'
    assert active(), 'STOP or terminal state protects training'
    ctl = request()
    assert a.host in ctl['draining_hosts'] and not ctl['leased_by_host'].get(a.host, 0)
    before = snapshots()
    for lane, n in before.items():
        assert n['pid'] == record['idle'][lane]['pid'] and not n.get('active')
        assert epoch(n['updated']) >= record['zero_since'] and not n.get('ended')
        assert not n.get('thread_error')

    def proc(pid):
        p = pathlib.Path('/proc') / str(pid)
        fields = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
        return {'pid': pid, 'state': fields[0], 'start_ticks': fields[19],
                'argv': (p / 'cmdline').read_bytes().decode().strip('\0').split('\0')}

    originals = {}
    for lane, n in before.items():
        info = proc(n['pid'])
        assert info['state'] != 'Z' and str(c) in info['argv'] and '--control' in info['argv']
        scripts = [x for x in info['argv'] if x.endswith('.py')]
        assert any(x.startswith(str(c) + '/') for x in scripts), 'not a copy worker'
        assert any(pathlib.Path(x).name in ('migration_run.py', 'migration_worker128_run.py') for x in scripts)
        for fd in (pathlib.Path('/proc') / str(n['pid']) / 'fd').iterdir():
            try: target = os.readlink(fd)
            except OSError: continue
            for root in (cfg['source'], cfg['destination']):
                assert not (target == root or target.startswith(root + '/')), 'payload FD remains open'
        originals[lane] = info
    helper_identity = proc(os.getpid())
    print('HELPER_IDENTITY ' + json.dumps(helper_identity), flush=True)
    record.update(status='replacing', originals=originals, replacement_helper_pid=os.getpid(),
                  replacement_helper=helper_identity)
    write_json(record_path, record)

    def alive(info):
        try:
            p = proc(info['pid'])
            return p['state'] != 'Z' and p['start_ticks'] == info['start_ticks']
        except (FileNotFoundError, ProcessLookupError): return False

    def wait_dead(info):
        for _ in range(150):
            if not alive(info): return
            time.sleep(.1)
        raise RuntimeError('idle old worker has not exited; do not SIGKILL')

    def wait_lock(lane):
        suffix = '-worker.lock' if lane == 'primary' else '-extra64-worker.lock'
        for _ in range(150):
            try:
                with open('/tmp/' + cfg['run_id'] + suffix, 'a') as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(lock, fcntl.LOCK_UN)
                return
            except BlockingIOError: time.sleep(.1)
        raise RuntimeError('old worker lock still held')

    def launch(lane, rollback=False):
        assert active(), 'STOP/terminal appeared before launch'
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        wanted = 65536 if hard == resource.RLIM_INFINITY else min(65536, hard)
        if soft < wanted: resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
        if rollback:
            command = list(originals[lane]['argv'])
        else:
            command = [sys.executable, '-u', str(bundle / 'migration_cached_worker.py'), lane,
                       '--control', str(c), '--threads', '64']
            if lane == 'supplement': command.extend(['--nice', '0'])
        command = ['nice', '-n', '15'] + command
        if shutil.which('ionice'): command = ['ionice', '-c', '3'] + command
        log = bundle / ('worker-' + a.host + '-' + lane + (('-rollback') if rollback else '') + '.log')
        with open(log, 'ab', buffering=0) as out:
            return subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=out,
                                    stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)

    def healthy(lane, process):
        folder = 'nodes' if lane == 'primary' else 'nodes-extra'
        for _ in range(100):
            if process.poll() is not None: raise RuntimeError('replacement exited')
            n = read_json(c / folder / (a.host + '.json'))
            if n.get('pid') == process.pid and not n.get('ended') and not n.get('thread_error'):
                expected = cfg['io_bytes_per_second'] if lane == 'primary' else cfg['supplement_io_bytes_per_second']
                if lane == 'primary' and a.host.endswith('master-0'): expected -= 32 * 1024**2
                assert n.get('threads') == 64 and n.get('io_bytes_per_second') == expected
                assert not n.get('active'), 'drain must prevent new work'
                for key in ('bytes', 'streamed_bytes', 'verified_bytes', 'verified_files', 'completed_batches'):
                    assert n.get(key, 0) >= before[lane].get(key, 0), 'counter regressed: ' + key
                assert n.get('broker_retries', 0) >= before[lane].get('broker_retries', 0), 'broker retries regressed'
                def counters_not_less(previous, current, prefix):
                    for key, value in previous.items():
                        if isinstance(value, dict):
                            child = current.get(key, {})
                            assert isinstance(child, dict), 'counter mapping missing: ' + prefix + key
                            counters_not_less(value, child, prefix + key + '.')
                        elif isinstance(value, (int, float)) and not isinstance(value, bool):
                            assert current.get(key, 0) >= value, 'counter regressed: ' + prefix + key
                for key in ('counts', 'stages'):
                    counters_not_less(before[lane].get(key, {}), n.get(key, {}), key + '.')
                expected_start = before[lane].get('run_started', before[lane].get('started'))
                assert n.get('run_started') == expected_start, 'run_started changed'
                assert epoch(n['updated']) >= record['idle_verified']
                return n
            time.sleep(.25)
        raise RuntimeError('replacement heartbeat timed out')

    record['replacements'] = {}
    for lane in ('primary', 'supplement'):
        info = originals[lane]
        process = None
        stop_sent = False
        try:
            assert active(), 'STOP/terminal appeared before stop'
            ctl = request()
            assert a.host in ctl['draining_hosts'], 'drain barrier disappeared before stop'
            assert not ctl['leased_by_host'].get(a.host, 0)
            current = proc(info['pid'])
            assert current['start_ticks'] == info['start_ticks'] and current['argv'] == info['argv']
            os.kill(info['pid'], signal.SIGTERM)
            stop_sent = True
            wait_dead(info)
            wait_lock(lane)
            process = launch(lane)
            n = healthy(lane, process)
            identity = proc(n['pid'])
            assert identity['state'] != 'Z', 'replacement exited after its first heartbeat'
            record['replacements'][lane] = {'pid': n['pid'], 'before_pid': before[lane]['pid'],
                'threads': n['threads'], 'io_bytes_per_second': n['io_bytes_per_second'],
                'snapshot': n, 'proc': identity}
            write_json(record_path, record)
        except BaseException as failure:
            record.update(status='recovery_required', failed_lane=lane,
                          replacement_error=type(failure).__name__ + ': ' + str(failure),
                          stop_sent=stop_sent, updated=time.time())
            try:
                # A partially started replacement owns the same lane lock.
                # Prove its exit before even considering an old-code restart.
                if process is not None:
                    record['replacement_pid'] = process.pid
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=15)
                if not active():
                    record['status'] = 'terminal_aborted'
                elif alive(info):
                    # TERM may be pending on uninterruptible NFS I/O. Keep the
                    # host gated; never start a second process beside it.
                    record['status'] = 'recovery_required' if stop_sent else 'old_worker_retained'
                    record['old_worker_alive'] = True
                else:
                    wait_lock(lane)
                    fallback = launch(lane, rollback=True)
                    record['rollback_pid'] = fallback.pid
                    healthy(lane, fallback)
                    record['status'] = 'rolled_back_partial'
            except BaseException as recovery_failure:
                record['recovery_error'] = type(recovery_failure).__name__ + ': ' + str(recovery_failure)
                record['status'] = 'recovery_required'
            write_json(record_path, record)
            raise
    record.update(status='workers_ready', updated=time.time())
    write_json(record_path, record)
    result({'success': True, 'host': a.host, 'replacements': {
        k: {z: v[z] for z in ('pid', 'before_pid', 'threads', 'io_bytes_per_second')}
        for k, v in record['replacements'].items()}})


if __name__ == '__main__':
    main()
