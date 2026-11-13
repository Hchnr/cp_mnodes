#!/usr/bin/env python3
"""Paced read-only experiment against one stopped synthetic NFS queue.

The paths and limits are fixed deliberately. The result is evidence about this
small synthetic sample, not permission to read or modify the production queue.
"""
import collections
import importlib.util
import json
import os
from pathlib import Path
import resource
import signal
import sqlite3
import subprocess
import sys
import time

import projset_canary_health_guard as health_guard


AUDIT = Path('/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294')
PROTO = AUDIT / 'monitor20/diagnostics/handoff-prototype-20261112T2057Z'
SYNTHETIC = PROTO / 'full-shape-canary-20261113T0200Z'
SOURCE = SYNTHETIC / 'paused-partial.sqlite3'
WORK = PROTO / 'paced-read-canary-20261113T0607Z'
CODE = Path(__file__).resolve()
RATE = 128 * 1024
ROWS = 16384
BATCH = 32
MAX_BATCH = 512 * 1024
MAX_TOTAL = 32 * 1024 * 1024
MAX_SECONDS = 150


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pacing = load('projset_paced_read_canary', CODE.with_name('projset_paced_read.py'))


def validate_source():
    if (PROTO.name != 'handoff-prototype-20261112T2057Z' or
            SYNTHETIC.name != 'full-shape-canary-20261113T0200Z' or
            SOURCE.is_symlink() or SOURCE.resolve().parent != SYNTHETIC.resolve()):
        raise ValueError('not the fixed synthetic source')
    previous = json.loads((SYNTHETIC / 'result.json').read_text())
    stopped = json.loads((SYNTHETIC / 'stop-observation.json').read_text())
    verifier = json.loads((SYNTHETIC / 'stop-verifier-observation.json').read_text())
    observed = stopped.get('observation', {})
    source = SOURCE.stat()
    if (previous.get('state') != 'stopped' or not observed.get('stopped') or
            source.st_size != observed.get('partial_bytes') or
            not isinstance(previous.get('rows_inserted'), int) or previous['rows_inserted'] < ROWS or
            not verifier.get('zombie_observed_at')):
        raise ValueError('synthetic writer or prior verifier was not confirmed stopped')
    return (source.st_dev, source.st_ino, source.st_size, source.st_mtime_ns)


def live_states(expected=None):
    states = [json.loads((AUDIT / name).read_text()) for name in ('status.json', 'repair/status.json')]
    if any(s.get('state') != 'running' or s.get('nodes_ready') != 16 or
           time.time() - s.get('updated', 0) > 90 for s in states):
        raise RuntimeError('live task heartbeat or readiness changed')
    identities = [(s.get('controller_host'), s.get('controller_pid')) for s in states]
    if expected is not None and identities != expected:
        raise RuntimeError('live controller identity changed')
    return states, identities


def memory_events():
    data = dict(line.split() for line in Path('/sys/fs/cgroup/memory.events').read_text().splitlines())
    return {name: int(data[name]) for name in ('oom', 'oom_kill')}


def mount_read():
    """Aggregate node/mount counters; other processes may contribute."""
    current = False
    for line in Path('/proc/self/mountstats').read_text().splitlines():
        if line.startswith('device '):
            current = ' mounted on /share-mixed/project with fstype nfs' in line
        elif current and line.strip().startswith('READ:'):
            values = [int(x) for x in line.split()[1:9]]
            return {'requests': values[0], 'bytes_received': values[4], 'rtt_ms': values[6]}
    raise ValueError('project NFS mount statistics unavailable')


def process_identity(pid):
    proc = Path('/proc') / str(pid)
    args = [os.fsdecode(x) for x in (proc / 'cmdline').read_bytes().split(b'\0') if x]
    fields = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()
    return args, fields[19], fields[0]


def stop_child(proc, start):
    if proc.poll() is not None:
        return False
    try:
        args, current, _ = process_identity(proc.pid)
        if current != start or str(CODE) not in args or '--child' not in args:
            raise ValueError('synthetic child identity changed')
        os.kill(proc.pid, signal.SIGTERM)
        return True
    except ProcessLookupError:
        return False


def child():
    os.nice(19)
    resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
    source_identity = validate_source()
    _, identities = live_states()
    initial_memory = memory_events()
    last_health = 0.0
    deadline = time.monotonic() + MAX_SECONDS

    def health(force=False):
        nonlocal last_health
        now = time.monotonic()
        if now >= deadline:
            raise TimeoutError('synthetic read deadline reached')
        if not force and now - last_health < 5:
            return
        live_states(identities)
        if memory_events() != initial_memory:
            raise RuntimeError('test node OOM counter changed')
        last_health = now

    def stop(_sig, _frame):
        raise RuntimeError('independent synthetic guard requested stop')

    signal.signal(signal.SIGTERM, stop)
    health(force=True)
    mount_before = mount_read()
    io_before = pacing.read_process_io()
    pacer = pacing.ReadPacer(RATE, health=health, max_batch=MAX_BATCH)
    db = sqlite3.connect('file:' + str(SOURCE) + '?mode=ro', uri=True, timeout=2)
    try:
        advised = pacing.disable_sqlite_readahead(SOURCE)
        db.execute('pragma query_only=on')
        db.execute('pragma mmap_size=0')
        db.execute('pragma cache_size=-256')
        plan = db.execute('explain query plan select id,rel from dirs where id>? order by id limit ?',
                          (0, BATCH)).fetchall()
        if len(plan) != 1 or 'INTEGER PRIMARY KEY (rowid>?)' not in plan[0][3]:
            raise ValueError('synthetic scan is not a bounded primary-key lookup')
        first_charge = pacer.throttle()
        max_logical = first_charge['logical_bytes']
        max_physical = first_charge['process_read_bytes']
        done = 0
        last = 0
        while done < ROWS:
            health()
            rows = db.execute('select id,rel from dirs where id>? order by id limit ?',
                              (last, min(BATCH, ROWS - done))).fetchall()
            charged = pacer.throttle()
            max_logical = max(max_logical, charged['logical_bytes'])
            max_physical = max(max_physical, charged['process_read_bytes'])
            if not rows:
                raise ValueError('synthetic partial queue ended before target sample')
            for ident, raw in rows:
                if ident != done + 1:
                    raise ValueError('synthetic row ID differs')
                if ident == 1:
                    expected = ''
                else:
                    prefix = 'synthetic/%07d/' % (ident - 2)
                    expected = prefix + 'x' * (110 - len(prefix))
                if json.loads(raw) != expected:
                    raise ValueError('synthetic path differs')
                done += 1
            last = rows[-1][0]
            current = pacing.read_process_io()
            if done % 4096 == 0:
                print(json.dumps({'state': 'progress', 'rows_checked': done,
                                  'logical_read_bytes': current[0] - io_before[0],
                                  'process_read_bytes': current[1] - io_before[1],
                                  'max_batch_logical_bytes': max_logical,
                                  'max_batch_process_read_bytes': max_physical}), flush=True)
            if any(a - b > MAX_TOTAL for a, b in zip(current, io_before)):
                raise RuntimeError('synthetic read exceeded total budget')
        health(force=True)
        if validate_source() != source_identity:
            raise RuntimeError('synthetic source changed during read')
        io_after = pacing.read_process_io()
        mount_after = mount_read()
        return {'state': 'passed', 'rows_checked': done, 'page_rows': BATCH, 'rate_bytes_per_second': RATE,
                'matching_sqlite_descriptors': advised, 'logical_read_bytes': io_after[0] - io_before[0],
                'process_read_bytes': io_after[1] - io_before[1],
                'max_batch_logical_bytes': max_logical, 'max_batch_process_read_bytes': max_physical,
                'mount_read_requests_delta': mount_after['requests'] - mount_before['requests'],
                'mount_read_bytes_received_delta': mount_after['bytes_received'] - mount_before['bytes_received'],
                'mount_counters_are_shared': True, 'production_controllers': identities,
                'note': 'read-only synthetic sample; fadvise and process counters do not guarantee wire bandwidth'}
    finally:
        db.close()


def parent():
    validate_source()
    states, identities = live_states()
    before_memory = memory_events()
    monitor = json.loads((AUDIT / 'monitor20/status.json').read_text())
    if (monitor.get('state') != 'running' or monitor.get('audit_state') != 'running' or
            time.time() - monitor.get('updated', 0) > 1800):
        raise RuntimeError('fresh formal throughput baseline unavailable')
    guard = health_guard.Guard(monitor['rates'], window_seconds=60, ratio=.7)
    guard.observe(states[0])
    if WORK.exists() or WORK.is_symlink() or WORK.parent.resolve() != PROTO.resolve():
        raise ValueError('synthetic result directory already exists or escaped')
    WORK.mkdir(mode=0o700)
    proc = subprocess.Popen([sys.executable, '-S', '-B', str(CODE), '--child'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True)
    started = time.monotonic()
    try:
        _, ticks, _ = process_identity(proc.pid)
    except OSError:
        ticks = None
    reason = None
    stopped = False
    try:
        while proc.poll() is None:
            if time.monotonic() - started > MAX_SECONDS + 5:
                reason = 'synthetic read wall-time limit'; break
            try:
                states, _ = live_states(identities)
                latest = guard.observe(states[0])
                if latest and latest['all_below_threshold']:
                    raise RuntimeError('live throughput guard')
                if memory_events() != before_memory:
                    raise RuntimeError('test node OOM counter changed')
            except (OSError, ValueError, KeyError, RuntimeError) as error:
                reason = str(error); break
            time.sleep(3)
        if reason and ticks is not None:
            stopped = stop_child(proc, ticks)
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            stdout, stderr = '', ''
        result = {'at': time.time(), 'elapsed_seconds': time.monotonic() - started,
                  'guard_reason': reason, 'guard_sent_stop': stopped, 'child_exit': proc.poll(),
                  'guard_baseline': guard.baseline, 'guard_latest': guard.latest,
                  'child_still_running': proc.poll() is None}
        if stdout:
            try:
                observations = [json.loads(line) for line in stdout.strip().splitlines()]
                result['progress'] = [item for item in observations if item.get('state') == 'progress']
                result['child'] = observations[-1]
            except (ValueError, AttributeError):
                result['child_output_invalid'] = True
        if stderr:
            result['stderr_tail'] = stderr[-600:]
        result['passed'] = (reason is None and proc.returncode == 0 and
                            result.get('child', {}).get('state') == 'passed' and
                            result['child']['rows_checked'] == ROWS)
        (WORK / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0 if result['passed'] else 1
    finally:
        if proc.poll() is None and ticks is not None:
            try: stop_child(proc, ticks)
            except (OSError, ValueError): pass


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--child']:
            print(json.dumps(child(), ensure_ascii=False), flush=True)
        elif not sys.argv[1:]:
            raise SystemExit(parent())
        else:
            raise ValueError('unexpected canary argument')
    except Exception as error:
        print(json.dumps({'state': 'stopped', 'type': type(error).__name__, 'detail': str(error)}), flush=True)
        raise SystemExit(1)
