#!/usr/bin/env python3
"""Bounded idle-P3 CPU-affinity trial; no signals, restarts, or thread changes.

The watchdog is an independent child. Controller exit/kill, deadline, a new
lease, stale heartbeat, or phase change restores owned masks. An unrelated
operator's mask is never overwritten. This is not a persistent CPU limiter.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import sqlite3
import ssl
import sys
import time
import urllib.parse

from migration_io import read_json, write_json


def identity(pid):
    p = Path('/proc') / str(pid)
    fields = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
    return dict(pid=pid, start_ticks=fields[19], state=fields[0],
                argv=(p / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0'))


def same(actual, expected):
    return actual['state'] not in ('Z', 'X') and all(
        actual[k] == expected[k] for k in ('pid', 'start_ticks', 'argv'))


def restore_mask(current, original, trial):
    """None means another owner changed it; never overwrite that change."""
    if set(current) == set(original):
        return set(original)
    if set(current) == set(trial):
        return set(original)
    return None


def restore(plan):
    try:
        actual = identity(plan['expected']['pid'])
    except FileNotFoundError:
        return dict(status='target_exited', conflicts=[])
    if not same(actual, plan['expected']):
        return dict(status='target_changed_no_write', conflicts=[])
    conflicts, restored = [], []
    # New threads inherit this process's temporary common mask. Restore those
    # too, but only while their current mask is still demonstrably ours.
    for thread in (Path('/proc') / str(actual['pid']) / 'task').iterdir():
        tid = int(thread.name)
        try:
            current = os.sched_getaffinity(tid)
            desired = restore_mask(current, plan['original'], plan['trial'])
            if desired is None:
                conflicts.append(tid)
            elif current != desired:
                if not same(identity(actual['pid']), plan['expected']):
                    raise RuntimeError('target identity changed during restore')
                os.sched_setaffinity(tid, desired)
                if os.sched_getaffinity(tid) == desired:
                    restored.append(tid)
                else:
                    conflicts.append(tid)
        except ProcessLookupError:
            continue
    return dict(status='restored' if not conflicts else 'external_affinity_conflict',
                conflicts=conflicts, threads_restored=restored)


def watchdog(plan, read_fd, output, deadline, guard=None):
    """Parent pipe closure also covers SIGKILL; no worker receives a signal."""
    for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)
    try:
        while True:
            remaining = max(0, deadline - time.monotonic())
            ready, _, _ = select.select([read_fd], [], [], min(0.5, remaining))
            if ready:
                result = (dict(status='controller_restored') if os.read(read_fd, 1) == b'd'
                          else restore(plan))
                break
            if not remaining or time.monotonic() >= deadline:
                result = restore(plan)
                result['reason'] = 'deadline'
                break
            if guard is not None:
                try:
                    guard()
                except BaseException as exc:
                    result = restore(plan)
                    result.update(reason='idle_guard_failed', guard_error=repr(exc))
                    break
        write_json(output, dict(result, at=time.time()))
    except BaseException as exc:
        write_json(output, dict(status='restore_failed', error=repr(exc), at=time.time()))
        return 1
    return 0


def require_no_data_fds(pid, control, run_id):
    """Fail closed on payload/unknown file descriptors, not only heartbeats."""
    prefix = str(control) + '/'
    lock_paths = {'/tmp/' + run_id + suffix for suffix in
                  ('-worker.lock', '-worker128.lock', '-extra64-worker.lock')}
    # urllib builds HTTPS handlers even for the private HTTP broker. Its
    # standard system CA file is not a source/destination payload descriptor.
    default_ca = ssl.get_default_verify_paths().cafile
    ca_path = os.path.realpath(default_ca) if default_ca else None
    if ca_path is not None and not ca_path.startswith('/etc/ssl/certs/'):
        ca_path = None  # An operator-selected payload path is not a system CA exception.
    for fd in (Path('/proc') / str(pid) / 'fd').iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        # Only control metadata/logs and known local singleton locks are files
        # expected in an idle worker. Anonymous pipes/sockets carry no payload FD.
        if (not target.startswith('/') or target == '/dev/null'
                or target.startswith(prefix) or target in lock_paths or target == ca_path):
            continue
        raise RuntimeError('unexpected/data file descriptor: ' + fd.name + ': ' + target)


def idle_guard(control, expected, node_path):
    if not same(identity(expected['pid']), expected):
        raise RuntimeError('bound worker identity changed')
    state, node = read_json(control / 'state.json'), read_json(node_path)
    from datetime import datetime
    age = time.time() - datetime.fromisoformat(node['updated']).timestamp()
    if (state.get('status') != 'scanning' or state.get('stage') != 'p3-r3'
            or state.get('recovery_warm') is not True or (control / 'STOP').exists()
            or node.get('pid') != expected['pid'] or node.get('active')
            or node.get('ended') or node.get('thread_error') or node.get('threads') != 64
            or not 0 <= age < 15):
        raise RuntimeError('worker is no longer freshly idle in the bound warm phase')
    cfg = read_json(control / 'config.json')
    require_no_data_fds(expected['pid'], control, cfg['run_id'])
    path = '/tmp/' + cfg['run_id'] + '-broker.sqlite'
    owners = [node['host'] + '-' + str(i) for i in range(128)]
    uri = 'file:' + urllib.parse.quote(path, safe='/') + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=2) as db:
        count = db.execute("SELECT COUNT(*) FROM claims WHERE status='leased' AND owner IN (" +
                           ','.join('?' for _ in owners) + ')', owners).fetchone()[0]
    if count:
        raise RuntimeError('new lease observed; restore immediately')
    if not same(identity(expected['pid']), expected):
        raise RuntimeError('bound worker identity changed during idle validation')
    return state


def cpu(pid):
    fields = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(') ', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=40)
    args = parser.parse_args()
    if not 20 <= args.seconds <= 60:
        raise ValueError('require a bounded 20–60 second trial')
    directory = args.directory.resolve()
    intent = read_json(directory / 'intent.json')
    control, expected = Path(intent['control']), intent['expected']
    if (directory.parent != control or not directory.name.startswith('idle-affinity-')
            or control.parent != Path('/share-mixed/project') or control.is_symlink()
            or expected['argv'][2] != str(control / 'migration_cached_worker.py')
            or expected['argv'][3] not in ('primary', 'supplement')
            or expected['argv'][4:6] != ['--control', str(control)]):
        raise ValueError('require the exact original P3 cached worker scope')
    node_path = control / ('nodes' if expected['argv'][3] == 'primary' else 'nodes-extra') / (
        os.uname().nodename + '.json')
    idle_guard(control, expected, node_path)
    masks = [os.sched_getaffinity(int(t.name)) for t in
             (Path('/proc') / str(expected['pid']) / 'task').iterdir()]
    original = masks[0]
    if len(masks) != 67 or len(original) < 8 or any(m != original for m in masks):
        raise ValueError('require the unchanged 64-thread lane with a common broad CPU mask')
    trial = set(sorted(original)[:4])
    plan = dict(expected=expected, original=sorted(original), trial=sorted(trial))
    result = dict(plan=plan, points=[], started=time.time(), copy_signals_sent=0,
                  copy_threads_changed=0, status='baseline')
    result_path = directory / 'result.json'

    def sample(label):
        state = idle_guard(control, expected, node_path)
        p4 = Path(read_json(control / 'extension-plan.json')['parallel_execution']['control'])
        metric = read_json(p4 / 'metrics.json')
        reader = state.get('source_reader_progress', {})
        cg = Path('/sys/fs/cgroup')
        cgroup_cpu = dict(line.split() for line in (cg / 'cpu.stat').read_text().splitlines())
        result['points'].append(dict(label=label, time=time.time(), monotonic=time.monotonic(),
            cpu=cpu(expected['pid']), producer_cpu=cpu(281673),
            master_copy_cpu={str(p): cpu(p) for p in (5873, 5879, 3737241, 3741388)},
            cgroup_cpu=cgroup_cpu, cpu_pressure=(cg / 'cpu.pressure').read_text(),
            state_updated=state['updated'], source=reader, target=state['target_filter'],
            next_task_id=state['next_task_id'],
            p4_sample_time=metric.get('sample_time'), p4_copied=metric.get('nodebytes'),
            p4_streamed=metric.get('streamed_bytes')))
        write_json(result_path, result)

    with open('/tmp/' + read_json(control / 'config.json')['run_id'] + '-idle-affinity.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sample('baseline')
        for _ in range(9):
            time.sleep(5)
            sample('baseline')
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(write_fd)
            code = watchdog(plan, read_fd, directory / 'watchdog-result.json',
                            time.monotonic() + args.seconds + 5,
                            guard=lambda: idle_guard(control, expected, node_path))
            os._exit(code)
        os.close(read_fd)
        result['watchdog_pid'] = pid
        def interrupted(*_):
            raise InterruptedError('affinity controller interrupted')
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, interrupted)
        try:
            result['status'] = 'applying'
            write_json(result_path, result)
            idle_guard(control, expected, node_path)
            for thread in (Path('/proc') / str(expected['pid']) / 'task').iterdir():
                tid = int(thread.name)
                if os.sched_getaffinity(tid) != original or not same(identity(expected['pid']), expected):
                    raise RuntimeError('identity or affinity changed before applying trial')
                os.sched_setaffinity(tid, trial)
            sample('trial')
            until = time.monotonic() + args.seconds
            while time.monotonic() < until:
                time.sleep(1)
                sample('trial')
            result['status'] = 'sampled'
        except BaseException as exc:
            result.update(status='interrupted', error=repr(exc))
        finally:
            for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, signal.SIG_IGN)
            result['restoration'] = restore(plan)
            result['finished'] = time.time()
            write_json(result_path, result)
            if result['restoration']['status'] == 'restored':
                os.write(write_fd, b'd')
            os.close(write_fd)
            os.waitpid(pid, 0)
        if result['restoration']['status'] == 'restored':
            try:
                sample('restored')
                for _ in range(9):
                    time.sleep(5)
                    sample('restored')
            except Exception as exc:
                result['post_restore_observation_error'] = repr(exc)
                write_json(result_path, result)
        print(json.dumps({k: v for k, v in result.items() if k != 'points'}), flush=True)


if __name__ == '__main__':
    main()
