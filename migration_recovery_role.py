#!/usr/bin/env python3
"""Idempotent, host-local launcher for one role in a fresh recovery control.

Run this file from the immutable bundle containing the original role scripts.
This helper never kills processes. A dead producer requires manual recovery;
other dead roles require --restart. Printed results contain no configuration.
"""

import argparse
import datetime
import fcntl
import json
import os
import pathlib
import re
import resource
import shutil
import socket
import subprocess
import sys
import time

from migration_io import read_json, write_json


ROLES = ('broker', 'metrics', 'watchdog', 'primary', 'supplement', 'inventory', 'producer')
TERMINAL = {'complete', 'complete_with_changes', 'failed', 'stopped'}
BUNDLE = pathlib.Path(os.path.abspath(__file__)).parent
LOCK_ROOT = pathlib.Path('/tmp')


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def process_info(pid):
    """Read only /proc; missing processes are not confused with PID reuse."""
    root = pathlib.Path('/proc') / str(int(pid))
    try:
        fields = (root / 'stat').read_text().rsplit(') ', 1)[1].split()
        argv = (root / 'cmdline').read_bytes().decode(errors='surrogateescape').rstrip('\0').split('\0')
        return {'pid': int(pid), 'start_ticks': fields[19], 'state': fields[0],
                'nice': int(fields[16]), 'argv': argv}
    except (FileNotFoundError, ProcessLookupError):
        return None


def matching_processes(expected_argv):
    result = []
    with os.scandir('/proc') as entries:
        for entry in entries:
            if not entry.name.isdigit():
                continue
            try:
                info = process_info(int(entry.name))
            except (PermissionError, ValueError, IndexError):
                continue
            if info and info['state'] != 'Z' and info['argv'] == expected_argv:
                result.append(info)
    return sorted(result, key=lambda info: info['pid'])


def role_argv(control, role, node_index=None):
    control = str(control)
    scripts = {'broker': 'migration_broker_parallel.py', 'metrics': 'migration_metrics.py',
               'watchdog': 'migration_watchdog.py', 'primary': 'migration_cached_worker.py',
               'supplement': 'migration_cached_worker.py', 'inventory': 'migration_inventory.py',
               'producer': 'migration_fresh_resume.py'}
    if role not in scripts:
        raise ValueError('unknown recovery role')
    if role == 'inventory':
        if not isinstance(node_index, int) or isinstance(node_index, bool) or not 0 <= node_index < 12:
            raise ValueError('inventory requires --node-index within 0..11')
    elif node_index is not None:
        raise ValueError('--node-index is only valid for inventory')
    command = [sys.executable, '-u', str(BUNDLE / scripts[role])]
    if role in ('primary', 'supplement'):
        command.append(role)
    elif role == 'producer':
        command.append('run')
    command.extend(['--control', control])
    if role == 'broker':
        command.extend(['--port', '37651'])
    elif role == 'metrics':
        command.extend(['--interval', '5'])
    elif role in ('primary', 'supplement'):
        command.extend(['--threads', '64'])
        if role == 'supplement':
            command.extend(['--nice', '0'])
    elif role == 'inventory':
        command.extend(['--node-index', str(node_index), '--rate', '100'])
    return command


def guard(control):
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present; role launch refused')
    state = read_json(control / 'state.json')
    if not isinstance(state, dict) or not isinstance(state.get('status'), str):
        raise ValueError('current state is unavailable or invalid')
    if state['status'] in TERMINAL:
        raise RuntimeError('terminal state; role launch refused: ' + state['status'])


def raise_nofile():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('NOFILE hard limit is below required 65536')
    if soft != resource.RLIM_INFINITY and soft < 65536:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, hard))
    return resource.getrlimit(resource.RLIMIT_NOFILE)


def wait_for_exec(process, expected_argv, timeout=3.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        code = process.poll()
        if code is not None:
            raise RuntimeError('role exited before identity confirmation; exit=' + str(code))
        info = process_info(process.pid)
        if info and info['state'] != 'Z' and info['argv'] == expected_argv:
            if last and last['start_ticks'] == info['start_ticks'] and last['argv'] == info['argv']:
                return info
            last = info
        else:
            last = None
        time.sleep(0.05)
    raise RuntimeError('role did not reach the exact expected argv before timeout')


def launch_role(control, role, node_index=None, restart=False):
    control = pathlib.Path(os.path.abspath(os.fspath(control)))
    cfg = read_json(control / 'config.json')
    run_id = cfg['run_id']
    host = socket.gethostname()
    if any(not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', value)
           for value in (run_id, host)):
        raise ValueError('invalid run_id or host')
    expected = role_argv(control, role, node_index)
    evidence = control / 'recovery-roles' / (host + '-' + role + '.json')
    lock_path = LOCK_ROOT / (run_id + '-recovery-launch-' + host + '-' + role + '.lock')
    with open(lock_path, 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record = {'run_id': run_id, 'control': str(control), 'host': host, 'role': role,
                  'node_index': node_index, 'expected_argv': expected, 'updated': now()}
        try:
            try:
                previous = read_json(evidence)
            except FileNotFoundError:
                previous = None
            if previous is not None:
                requested = record
                if isinstance(previous, dict):
                    record = dict(previous)
                if (not isinstance(previous, dict) or any(previous.get(k) != requested[k]
                    for k in ('run_id', 'control', 'host', 'role', 'node_index', 'expected_argv'))):
                    record['refused_requested_argv'] = expected
                    raise RuntimeError('existing role evidence differs; explicit manual inspection required')
            guard(control)
            if previous is not None:
                current = process_info(previous['pid']) if previous.get('pid') is not None else None
                same_start = current and current['state'] != 'Z' and str(current['start_ticks']) == str(previous.get('start_ticks'))
                if same_start:
                    if current['argv'] != expected or previous.get('argv') != expected:
                        raise RuntimeError('recorded live process argv differs; refusing any replacement')
                    record.update(status='reused', updated=now(), **current)
                    write_json(evidence, record)
                    return record
                if role == 'producer' or not restart:
                    raise RuntimeError('recorded process is dead or replaced; ' +
                                       ('producer requires manual recovery' if role == 'producer' else 'use --restart after inspection'))

            existing = matching_processes(expected)
            if len(existing) > 1:
                record['matching_processes'] = existing
                raise RuntimeError('multiple exact role processes exist; refusing launch')
            if existing:
                record.update(status='adopted', updated=now(), **existing[0])
                write_json(evidence, record)
                return record
            if restart and role == 'producer':
                raise RuntimeError('producer cannot be automatically restarted')
            script = pathlib.Path(expected[2])
            if not script.is_file():
                raise FileNotFoundError('role script is missing: ' + str(script))
            nice, ionice = shutil.which('nice'), shutil.which('ionice')
            if not nice or not ionice:
                raise RuntimeError('both nice and ionice are required')
            soft, hard = raise_nofile()
            command = [ionice, '-c', '3', nice, '-n', '15'] + expected
            log = control / 'recovery-logs' / (host + '-' + role + '.log')
            log.parent.mkdir(parents=True, exist_ok=True)
            if previous is not None:
                record['previous_identity'] = {k: previous.get(k) for k in
                                               ('pid', 'start_ticks', 'argv', 'status', 'started', 'updated', 'error')}
                for key in ('pid', 'start_ticks', 'argv', 'state', 'nice'):
                    record.pop(key, None)
            record.update(status='launching', command=command, log=str(log),
                          nofile_soft=soft, nofile_hard=hard, started=now(), updated=now())
            record.pop('error', None)
            write_json(evidence, record)
            guard(control)
            fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0), 0o600)
            with os.fdopen(fd, 'ab', buffering=0) as output:
                process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output,
                                           stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
            record.update(pid=process.pid, updated=now())
            # Persist the child PID even if exec/validation subsequently fails.
            write_json(evidence, record)
            current = wait_for_exec(process, expected)
            guard(control)
            record.update(status='launched', updated=now(), health_verified=False, **current)
            write_json(evidence, record)
            return record
        except BaseException as exc:
            record.update(status='launch_failed', updated=now(),
                          error={'type': type(exc).__name__, 'detail': str(exc)})
            try:
                write_json(evidence, record)
            except Exception:
                pass
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--role', required=True, choices=ROLES)
    parser.add_argument('--node-index', type=int)
    parser.add_argument('--restart', action='store_true', help='allow inspected dead non-producer role to restart')
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        record = launch_role(args.control, args.role, args.node_index, args.restart)
    except Exception as exc:
        print(json.dumps({'ok': False, 'role': args.role, 'host': socket.gethostname(),
                          'error': {'type': type(exc).__name__, 'detail': str(exc)}}))
        return 1
    public = ('run_id', 'control', 'host', 'role', 'node_index', 'status', 'pid', 'start_ticks',
              'argv', 'expected_argv', 'nice', 'log', 'nofile_soft', 'nofile_hard', 'updated',
              'health_verified')
    print(json.dumps({'ok': True, **{k: record[k] for k in public if k in record}}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
