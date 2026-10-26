#!/usr/bin/env python3
"""One bounded P3 main-thread nice=10 -> 0 -> 10 trial, with EOF watchdog.

No signals to copy, producer or broker processes; no CPU quota, affinity,
I/O priority, thread count, source dispatch, manifest or task changes.
"""
import argparse
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import sys
import time

from migration_io import read_json, write_json
from migration_scan_boundary import identity, require_identity
from migration_warm_scan_checkpoint import validate_runtime


def restoration_value(current, original=10, trial=0):
    return original if current in (original, trial) else None


def restore(expected):
    try:
        require_identity(expected)
    except (FileNotFoundError, RuntimeError):
        return {'status': 'target_exited_or_changed_no_write'}
    current = os.getpriority(os.PRIO_PROCESS, expected['pid'])
    value = restoration_value(current)
    if value is None:
        return {'status': 'external_priority_change_no_write', 'current': current}
    require_identity(expected)
    os.setpriority(os.PRIO_PROCESS, expected['pid'], value)
    return {'status': 'restored', 'nice': os.getpriority(os.PRIO_PROCESS, expected['pid'])}


def watchdog(expected, read_fd, output, deadline):
    for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)
    ready, _, _ = select.select([read_fd], [], [], max(0, deadline-time.monotonic()))
    value = ({'status': 'controller_restored'} if ready and os.read(read_fd, 1) == b'd'
             else restore(expected))
    write_json(output, dict(value, at=time.time()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    intent = read_json(directory/'intent.json')
    control = Path(intent['control'])
    expected = intent['producer']
    if (directory.parent != control or not directory.name.startswith('p3-priority-')
            or control.parent != Path('/share-mixed/project')
            or expected['pid'] != 281673 or expected['start_ticks'] != '78319084'
            or expected['argv'][:3] != ['/usr/bin/python3', '-u',
                str(control/'fast-p3-scan-466d08d7ea874d91a73534fb5caabac5/migration_fast_cold_resume.py')]):
        raise ValueError('exact live P3 main thread and private trial required')
    validate_runtime(control, intent)
    if os.getpriority(os.PRIO_PROCESS, expected['pid']) != 10:
        raise ValueError('expected unmodified nice=10 baseline')
    result = dict(status='baseline', points=[], copy_restarts=0, signals_to_data_roles=0,
                  original_nice=10, trial_nice=0, main_thread_only=True, started=time.time())
    path = directory/'result.json'
    def sample(label):
        require_identity(expected)
        state = read_json(control/'state.json')
        if (state.get('stage') != 'p3-r3' or state.get('status') != 'scanning'
                or state.get('recovery_warm') is not True or (control/'STOP').exists()):
            raise RuntimeError('P3 phase changed')
        fields = (Path('/proc')/str(expected['pid'])/'stat').read_text().rsplit(') ', 1)[1].split()
        result['points'].append(dict(label=label, observed=time.time(), monotonic=time.monotonic(),
            state_updated=state['updated'], source=state['source_reader_progress'],
            target=state['target_filter'], next_task_id=state['next_task_id'],
            cpu_seconds=(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK'),
            nice=os.getpriority(os.PRIO_PROCESS, expected['pid'])))
        write_json(path, result)
    def observe(label, seconds):
        until = time.monotonic()+seconds
        sample(label)
        while time.monotonic()<until:
            time.sleep(5)
            sample(label)
    with open('/tmp/'+intent['run_id']+'-p3-priority-trial.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        observe('baseline',45)
        read_fd, write_fd = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(write_fd)
            watchdog(expected, read_fd, directory/'watchdog-result.json', time.monotonic()+95)
            os._exit(0)
        os.close(read_fd)
        result['watchdog_pid'] = child
        def interrupted(*_): raise InterruptedError('trial interrupted')
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM): signal.signal(sig, interrupted)
        try:
            sample('before_apply')
            if os.getpriority(os.PRIO_PROCESS, expected['pid']) != 10:
                raise RuntimeError('priority ownership changed before trial')
            require_identity(expected)
            os.setpriority(os.PRIO_PROCESS, expected['pid'], 0)
            result['status'] = 'trial'
            observe('trial',90)
        except BaseException as exc:
            result.update(status='interrupted', error=repr(exc))
        finally:
            for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM): signal.signal(sig, signal.SIG_IGN)
            result['restoration'] = restore(expected)
            if result['restoration']['status'] == 'restored': os.write(write_fd,b'd')
            os.close(write_fd);os.waitpid(child,0)
            result['status'] = 'restored'
            write_json(path,result)
        observe('restored',45)
        result['finished'] = time.time()
        write_json(path,result)
        print(json.dumps({k:v for k,v in result.items() if k!='points'}),flush=True)


if __name__ == '__main__': main()
