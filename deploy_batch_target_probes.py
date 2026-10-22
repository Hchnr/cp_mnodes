#!/usr/bin/env python3
"""One-shot, identity-checked replacement of one read-only probe service.

Run only from a newly staged immutable bundle, once per node. Copy workers,
producer and broker are never signalled. A failed/ambiguous attempt is inspected
through its durable operation record before any subsequent operator action.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

from migration_io import read_json, write_json


def identity(pid):
    proc = Path('/proc') / str(pid)
    try:
        fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        argv = (proc / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
    except (OSError, ValueError):
        return None
    return {'pid': pid, 'starttime_ticks': fields[19], 'state': fields[0], 'argv': argv}


def same_process(actual, expected):
    return (actual is not None and actual['state'] != 'Z'
            and actual['pid'] == expected['pid']
            and str(actual['starttime_ticks']) == str(expected['starttime_ticks'])
            and actual['argv'] == expected['argv'])


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run_node(control, bundle, node):
    control, bundle = Path(control).resolve(), Path(bundle).resolve()
    if not bundle.is_relative_to(control) or bundle.parent != control:
        raise ValueError('probe rollout bundle must be directly within this control')
    cfg = read_json(control / 'config.json')
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present')
    manifest = read_json(bundle / 'source-sha256.json')
    for name, digest in manifest.items():
        path = bundle / name
        if path.parent != bundle or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise RuntimeError('staged source digest mismatch')
    predecessor = control / 'pipelined-producer-c168b1e89e6d4e9d88025fb847c2794b'
    old_record = predecessor / 'node-restarts' / node / 'probe.json'
    old = read_json(old_record)
    if (old.get('control') != str(control) or old.get('run_id') != cfg['run_id']
            or old.get('host') != socket.gethostname() or not old['host'].endswith('-' + node)
            or old.get('slots') != 64 or old.get('stop_cache_seconds') != .1):
        raise RuntimeError('predecessor role does not match this node/run/limits')
    argv = old['argv']
    expected_old_argv = [
        '/usr/bin/python3', '-u', str(predecessor / 'probe-capacity64.py'),
        '--control', str(control), '--port', str(old['port']),
        '--advertise-address', old['endpoint'].split('://', 1)[1].rsplit(':', 1)[0],
        '--slots', '64', '--record', str(old_record), '--stop-cache-seconds', '0.1']
    if argv != expected_old_argv or not same_process(identity(old['pid']), old):
        raise RuntimeError('live predecessor PID/startticks/full argv do not match')
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('probe NOFILE hard limit is too low')
    if soft < 65536:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, hard))
    role_dir = bundle / 'roles' / node
    role_dir.mkdir(parents=True, exist_ok=True)
    operation_path = role_dir / 'operation.json'
    new_record = role_dir / 'probe.json'
    new_argv = list(expected_old_argv)
    new_argv[2] = str(bundle / 'migration_target_probe.py')
    new_argv[new_argv.index('--record') + 1] = str(new_record)
    operation = {'node': node, 'control': str(control), 'bundle': str(bundle),
                 'started': now(), 'state': 'validated', 'old_identity': old,
                 'new_argv': new_argv}
    # This exclusive file is the durable one-shot boundary before signalling.
    with open(operation_path, 'x') as handle:
        json.dump(operation, handle, ensure_ascii=True)
        handle.flush()
        os.fsync(handle.fileno())

    def save(**values):
        operation.update(values, updated=now())
        write_json(operation_path, operation)

    try:
        if (control / 'STOP').exists() or not same_process(identity(old['pid']), old):
            raise RuntimeError('predecessor/STOP changed before scoped signal')
        os.kill(old['pid'], signal.SIGTERM)
        save(state='old_probe_signalled')
        until = time.monotonic() + 20
        while True:
            exiting = identity(old['pid'])
            if (exiting is None or exiting['state'] == 'Z'
                    or exiting['starttime_ticks'] != str(old['starttime_ticks'])):
                break
            if time.monotonic() > until:
                raise TimeoutError('predecessor probe did not exit after SIGTERM')
            time.sleep(.1)
        with open('/tmp/' + cfg['run_id'] + '-targetprobe.lock', 'a') as lock:
            # Exit notification and final file-lock teardown can briefly race.
            # Wait for this already-identified predecessor's lock to release;
            # never signal an unknown lock holder or launch a competing probe.
            until = time.monotonic() + 20
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() > until:
                        raise TimeoutError('probe lock remains held after predecessor exit')
                    time.sleep(.1)
        if (control / 'STOP').exists():
            raise RuntimeError('STOP changed before probe launch')
        with open(role_dir / 'probe.log', 'xb') as log:
            child = subprocess.Popen(['nice', '-n', '15', 'ionice', '-c', '3', *new_argv],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True, close_fds=True)
        save(state='new_probe_launched', new_pid=child.pid)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        url = 'http://127.0.0.1:' + str(old['port'])

        def request(route, body=None, authenticated=True):
            headers = ({'Authorization': 'Bearer ' + cfg['broker_token']}
                       if authenticated else {})
            if body is not None:
                headers['Content-Type'] = 'application/json'
            data = None if body is None else json.dumps(body).encode()
            query = urllib.request.Request(url + route, data=data, headers=headers)
            try:
                with opener.open(query, timeout=10) as response:
                    return response.status, json.load(response)
            except urllib.error.HTTPError as exc:
                return exc.code, json.load(exc)

        until = time.monotonic() + 45
        while True:
            if child.poll() is not None:
                raise RuntimeError('new probe exited; inspect its one-shot log')
            try:
                status, health = request('/health')
                if (status == 200 and health.get('pid') == child.pid
                        and health.get('batch_limit') == 64 and health.get('slots') == 64
                        and health.get('stop_cache_seconds') == .1
                        and same_process(identity(child.pid), health)
                        and health['argv'] == new_argv):
                    break
            except (OSError, ValueError):
                pass
            if time.monotonic() > until:
                raise TimeoutError('new probe did not validate ready within45 seconds')
            time.sleep(.2)
        status, unauthorized = request('/health', authenticated=False)
        if status != 401 or cfg['broker_token'] in json.dumps(unauthorized):
            raise RuntimeError('probe authentication validation failed')
        # These requests perform actual target metadata checks without writing
        # payloads or inventing any tasks in the coordinator's namespace.
        entry = {'rel': 'datasets/__batch_probe_validation_' + bundle.name, 'kind': 'file', 'size': 0}
        status, single = request('/probe', {'entry': entry})
        status2, batch = request('/probe-batch', {'entries': [entry, entry]})
        if (status != 200 or not single.get('missing') or status2 != 200
                or not batch.get('ok') or len(batch.get('results', [])) != 2
                or any(row.get('status') != 200 or row.get('result') != single
                       for row in batch['results'])):
            raise RuntimeError('single/batch compatibility check failed')
        save(state='ready', health=health, protocol_checks={
            'auth_401': True, 'single_probe_200': True, 'batch_probe_200': True,
            'ordered_compatible_results': True})
        return {'node': node, 'state': 'ready', 'pid': child.pid,
                'starttime_ticks': health['starttime_ticks'], 'endpoint': old['endpoint'],
                'slots': health['slots'], 'batch_limit': health['batch_limit'],
                'operation': str(operation_path), 'record': str(new_record)}
    except BaseException as exc:
        save(state='failed_inspect_before_retry', error=type(exc).__name__ + ': ' + str(exc))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--bundle', required=True)
    parser.add_argument('--node', required=True)
    args = parser.parse_args()
    print(json.dumps(run_node(args.control, args.bundle, args.node)), flush=True)
