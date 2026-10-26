#!/usr/bin/env python3
"""P4-producer-only CPU-placement trial with an independent restore watchdog.

Keeps the same producer, scanner, task IDs and all copy processes. This tests
placement/lock handoff, not additional Python CPU parallelism. Never treats a
short byte-rate change as an isolated performance proof.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import time

from experiment_idle_affinity import cpu, identity, restore, same, watchdog
from migration_io import read_json, write_json


def validate_scope(control, expected, config):
    if (control.parent != Path('/share-mixed/project') or control.is_symlink()
            or not control.name.startswith('.hcr-copy-p4-')
            or (config.get('source'), config.get('destination'), config.get('migration_scope'),
                config.get('exclude'), config.get('control')) !=
               ('/share/project', '/share-mixed/project', 'project_priority4', ['eai_pwm'], str(control))
            or expected.get('argv') != ['/usr/bin/python3', '-u',
                str(control / 'migration_priority4_run.py'), '--control', str(control)]):
        raise ValueError('exact P4 producer/control/source/exclusion binding required')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=45)
    parser.add_argument('--cpus', type=int, choices=(1, 2, 4, 8), default=4)
    args = parser.parse_args()
    if not 30 <= args.seconds <= 90:
        raise ValueError('require a bounded 30–90 second trial')
    directory = args.directory.resolve()
    intent = read_json(directory / 'intent.json')
    control, expected = Path(intent['control']), intent['expected']
    if directory.parent != control or not directory.name.startswith('producer-affinity-'):
        raise ValueError('require a private producer trial directory')
    config = read_json(control / 'config.json')
    validate_scope(control, expected, config)
    initial = read_json(control / 'state.json')
    manifest = Path(initial['active_manifest'])
    initial_stat = manifest.stat()

    def guard():
        if not same(identity(expected['pid']), expected):
            raise RuntimeError('producer PID/start/full argv changed')
        state = read_json(control / 'state.json')
        if (state.get('status') != 'scanning' or state.get('stage') != 'p4-r1'
                or state.get('active_manifest') != str(manifest) or (control / 'STOP').exists()
                or state.get('next_task_id', -1) < initial['next_task_id']):
            raise RuntimeError('producer phase/manifest/task prefix changed; restore')
        info = manifest.stat()
        if (info.st_dev, info.st_ino) != (initial_stat.st_dev, initial_stat.st_ino) or info.st_size < initial_stat.st_size:
            raise RuntimeError('source manifest identity or prefix changed')
        return state, info

    guard()
    threads = list((Path('/proc') / str(expected['pid']) / 'task').iterdir())
    masks = [os.sched_getaffinity(int(t.name)) for t in threads]
    original = masks[0]
    if len(threads) != 97 or len(original) < 16 or any(m != original for m in masks):
        raise RuntimeError('require the original 64-reader/32-publisher process and common broad mask')
    trial = set(sorted(original)[:args.cpus])
    plan = dict(expected=expected, original=sorted(original), trial=sorted(trial))
    record = dict(started=time.time(), plan=plan, points=[], state='baseline',
                  copy_threads_changed=0, copy_processes_signalled=0, producer_restarted=False)

    def save():
        write_json(directory / 'result.json', record)

    def sample(label):
        state, info = guard()
        metric = read_json(control / 'metrics.json')
        record['points'].append(dict(label=label, time=time.time(), cpu=cpu(expected['pid']),
            next_task_id=state['next_task_id'], state_updated=state['updated'],
            manifest_size=info.st_size, metric_time=metric['sample_time'],
            copied=metric['copied_bytes'], streamed=metric['streamed_bytes'],
            verified_files=metric['verified_files']))
        save()

    lock_path = '/tmp/' + config['run_id'] + '-producer-affinity.lock'
    with open(lock_path, 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sample('baseline')
        for _ in range(6):
            time.sleep(5)
            sample('baseline')
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(write_fd)
            os._exit(watchdog(plan, read_fd, directory / 'watchdog-result.json',
                             time.monotonic() + args.seconds + 20))
        os.close(read_fd)
        record['watchdog_pid'] = pid
        def interrupted(*_):
            raise InterruptedError('producer trial interrupted')
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, interrupted)
        try:
            record['state'] = 'applying'
            save()
            guard()
            for thread in (Path('/proc') / str(expected['pid']) / 'task').iterdir():
                tid = int(thread.name)
                if os.sched_getaffinity(tid) != original or not same(identity(expected['pid']), expected):
                    raise RuntimeError('producer identity or mask changed before trial')
                os.sched_setaffinity(tid, trial)
            sample('trial')
            until = time.monotonic() + args.seconds
            while time.monotonic() < until:
                time.sleep(2)
                sample('trial')
            record['state'] = 'sampled'
        except BaseException as exc:
            record.update(state='interrupted', error=repr(exc))
        finally:
            for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, signal.SIG_IGN)
            record['restoration'] = restore(plan)
            save()
            if record['restoration']['status'] == 'restored':
                os.write(write_fd, b'd')
            os.close(write_fd)
            os.waitpid(pid, 0)
        if record['restoration']['status'] == 'restored':
            try:
                sample('restored')
                for _ in range(6):
                    time.sleep(5)
                    sample('restored')
            except Exception as exc:
                record['post_restore_observation_error'] = repr(exc)
        record['finished'] = time.time()
        save()
        print(json.dumps({k: v for k, v in record.items() if k != 'points'}), flush=True)


if __name__ == '__main__':
    main()
