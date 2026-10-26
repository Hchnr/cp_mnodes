#!/usr/bin/env python3
"""Add 64 workers beside an existing 64-thread daemon, preserving its leases.

Launch only after all primary daemons acknowledge the half-budget config.
Each process has a fixed <=10 GB/s allocation; together they stay <=20 GB/s per
physical node. Master coordinator reserve is already subtracted by its primary.
This module does not change or attach to the original running Python process.
"""
import argparse
import concurrent.futures
import copy
import json
import os
import pathlib
import socket
import threading
import time
import traceback

from migration_io import read_json, write_json as atomic_json
from migration_run import validate_mounts, process_entry, GOOD, TERMINAL, now, BrokerClient
from migration_worker import (configure_rate_limit, configure_cancel_event,
                              configure_progress_callback)

NODE_IO_CEILING = 20_000_000_000
HALF_IO_CEILING = NODE_IO_CEILING // 2


def supplement_config(control, require_primary=False):
    control = pathlib.Path(control)
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present; supplement must not start or resume')
    cfg = read_json(control / 'config.json')
    primary_threads = int(cfg.get('threads', 0))
    total_threads = int(cfg.get('copy_threads_per_node', 128))
    primary_rate = int(cfg.get('io_bytes_per_second', 0))
    extra_rate = int(cfg.get('supplement_io_bytes_per_second', 0))
    if primary_threads != 64 or not 64 <= total_threads <= 128:
        raise ValueError('keep primary threads=64 and total copy_threads_per_node within64..128')
    if not (0 < primary_rate <= HALF_IO_CEILING and 0 < extra_rate <= HALF_IO_CEILING):
        raise ValueError('primary and supplement each require a positive <=10 GB/s budget')
    if primary_rate + extra_rate > NODE_IO_CEILING:
        raise ValueError('combined primary/supplement budget exceeds20 GB/s')
    desired = total_threads - primary_threads
    if require_primary:
        if not desired:
            raise ValueError('no supplemental threads requested')
        host = socket.gethostname()
        primary = read_json(control / 'nodes' / (host + '.json'))
        try:
            updated = primary['updated'].replace('Z', '+00:00')
            import datetime
            age = time.time() - datetime.datetime.fromisoformat(updated).timestamp()
            acknowledged = int(primary['io_bytes_per_second'])
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError('primary I/O budget acknowledgement missing') from exc
        if age < -5 or age > 180 or primary.get('ended') or primary.get('thread_error'):
            raise ValueError('primary acknowledgement is stale, ended, or unhealthy')
        if not 0 < acknowledged <= primary_rate or int(primary.get('threads', 0)) != 64:
            raise ValueError('primary has not acknowledged its <=10 GB/s/64-thread allocation')
    return cfg, extra_rate, desired


def worker(control, threads=64):
    # All queue ownership and completion writes belong to the broker. Shared
    # filesystem rename/O_EXCL did not provide exclusive claims on this mount.
    import urllib.parse

    control = pathlib.Path(control)
    cfg, worker_rate, desired = supplement_config(control, require_primary=True)
    validate_mounts(cfg)
    threads = min(int(threads), desired)
    if not 1 <= threads <= 64:
        raise ValueError('supplement initial threads must be between 1 and 64')
    configure_rate_limit(worker_rate)
    host = socket.gethostname()
    # One supplement per physical node; primary daemon keeps its own lock.
    import fcntl
    lock = open('/tmp/' + cfg['run_id'] + '-extra64-worker.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    node_path = control / 'nodes-extra' / (host + '.json')
    try:
        previous = read_json(node_path)
    except (OSError, ValueError):
        previous = {}
    started = now()
    progress = {'host': host, 'physical_host': host, 'instance': 'extra64',
                'thread_offset': 64, 'pid': os.getpid(), 'started': started,
                'run_started': previous.get('run_started', previous.get('started', started)),
                'threads': threads, 'io_bytes_per_second': worker_rate, 'counts': dict(previous.get('counts', {})),
                'bytes': previous.get('bytes', 0), 'streamed_bytes': previous.get('streamed_bytes', 0), 'active': {},
                'completed_batches': previous.get('completed_batches', 0),
                'verified_bytes': previous.get('verified_bytes', 0),
                'verified_files': previous.get('verified_files', 0),
                'stages': dict(previous.get('stages', {})),
                'broker_retries': previous.get('broker_retries', 0)}
    broker_request = BrokerClient(cfg['broker_url'], cfg['broker_token']).request
    mutex = threading.Lock()
    stop = threading.Event()
    configure_cancel_event(stop)
    errors = []
    tuning = {'desired': threads, 'launched': 0, 'pool': None, 'rate': worker_rate}

    def streamed(amount):
        with mutex:
            progress['streamed_bytes'] += amount
    configure_progress_callback(streamed)

    def record_broker_error(operation, exc):
        with mutex:
            progress['broker_retries'] += 1
            progress['broker_error'] = {'operation': operation, 'at': now(),
                                        'detail': type(exc).__name__ + ': ' + str(exc)}

    def heartbeat():
        while not stop.wait(3):
            if (control / 'STOP').exists():
                stop.set()
            if not stop.is_set():
                try:
                    current_cfg, rate, desired = supplement_config(control)
                    if rate != tuning['rate']:
                        configure_rate_limit(rate)
                        tuning['rate'] = rate
                    tuning['desired'] = desired
                    pool = tuning['pool']
                    if pool is not None:
                        while tuning['launched'] < desired:
                            index = tuning['launched']
                            pool.submit(thread_main, index)
                            tuning['launched'] += 1
                    with mutex:
                        progress.update(threads=desired, io_bytes_per_second=rate)
                        progress.pop('tuning_error', None)
                except (OSError, ValueError, RuntimeError) as exc:
                    with mutex:
                        progress['tuning_error'] = str(exc)
                    # Fail closed on a broken partition without throwing away
                    # a partially copied large file. A valid config resumes it.
                    configure_rate_limit(1)
                    tuning['rate'] = 1
                    tuning['desired'] = 0
                    with mutex:
                        progress.update(threads=0, io_bytes_per_second=1)
            with mutex:
                progress['updated'] = now()
                snapshot = copy.deepcopy(progress)
            # Slow shared-filesystem publication must not block the lock used
            # by all payload progress callbacks and copying threads.
            atomic_json(node_path, snapshot)

    def thread_main(index):
        logical_index = 64 + index
        owner = host + '-' + str(logical_index)
        while not stop.is_set():
            if index >= tuning['desired']:
                stop.wait(1)
                continue
            if (control / 'STOP').exists():
                return
            try:
                state = read_json(control / 'state.json')
            except (OSError, ValueError) as exc:
                record_broker_error('read_state', exc)
                stop.wait(3)
                continue
            if state['status'] in TERMINAL:
                return
            stage = state.get('stage')
            if not stage:
                stop.wait(1)
                continue
            try:
                claim = broker_request('GET', '/claim?' + urllib.parse.urlencode(
                    {'stage': stage, 'owner': owner}))
                task = claim.get('task')
                if task is not None and (not isinstance(task, dict)
                                         or not isinstance(claim.get('lease'), str)
                                         or not claim['lease']):
                    raise ValueError('broker claim lacks a task or lease')
            except Exception as exc:
                record_broker_error('claim', exc)
                stop.wait(3)
                continue
            if task is None:
                stop.wait(1.5)
                continue
            lease = claim['lease']
            try:
                receipt = {'id': task['id'], 'host': host, 'thread': logical_index,
                           'started': now(), 'op': task['op'], 'results': []}
                for entry in task['entries']:
                    with mutex:
                        progress['active'][str(logical_index)] = {'stage': stage, 'task': task['id'],
                                                         'rel': entry['rel'], 'size': entry.get('size', 0)}
                    if stop.is_set() or (control / 'STOP').exists():
                        stop.set()
                        result = {'rel': entry['rel'], 'kind': entry['kind'], 'status': 'paused',
                                  'bytes': 0, 'detail': 'copy paused by STOP marker'}
                    else:
                        result = process_entry(cfg, task, entry)
                    receipt['results'].append(result)
                    with mutex:
                        status = result['status']
                        progress['counts'][status] = progress['counts'].get(status, 0) + 1
                        progress['bytes'] += result.get('bytes', 0)
                        stage_progress = progress['stages'].setdefault(stage, {
                            'verified_files': 0, 'verified_bytes': 0, 'copied_bytes': 0, 'counts': {}})
                        stage_progress['counts'][status] = stage_progress['counts'].get(status, 0) + 1
                        stage_progress['copied_bytes'] += result.get('bytes', 0)
                        if status in GOOD:
                            verified_bytes = entry.get('size', 0) if entry['kind'] == 'file' else 0
                            progress['verified_files'] += 1
                            progress['verified_bytes'] += verified_bytes
                            stage_progress['verified_files'] += 1
                            stage_progress['verified_bytes'] += verified_bytes
                receipt['finished'] = now()
                payload = {'stage': stage, 'id': task['id'], 'lease': lease,
                           'receipt': receipt}
                with mutex:
                    progress['active'].setdefault(str(logical_index), {}).update(
                        stage=stage, task=task['id'], awaiting_receipt=True)
                delay = 1
                while True:
                    try:
                        broker_request('POST', '/done', payload)
                        break
                    except Exception as exc:
                        # A lost reply may mean the broker already committed.
                        # Retry the same receipt/lease forever; never recopy or
                        # claim new work after an uncertain completion.
                        record_broker_error('done', exc)
                        time.sleep(delay)
                        delay = min(30, delay * 2)
                with mutex:
                    progress['completed_batches'] += 1
                    progress['active'].pop(str(logical_index), None)
            except Exception:
                errors.append(traceback.format_exc())
                with mutex:
                    progress['last_error'] = errors[-1]
                    progress['thread_error'] = True
                # Broker retains this lease for explicit operator recovery.
                return

    progress['updated'] = now()
    atomic_json(node_path, progress)
    hb = threading.Thread(target=heartbeat, daemon=True)
    hb.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
        initial = [pool.submit(thread_main, index) for index in range(threads)]
        tuning.update(pool=pool, launched=threads)
        for future in initial:
            future.result()
        # Additional threads submitted by heartbeat also stop on the terminal
        # state/STOP before executor shutdown waits for them.
        tuning['pool'] = None
    stop.set()
    hb.join()
    with mutex:
        progress['updated'] = now()
        progress['ended'] = now()
        snapshot = copy.deepcopy(progress)
    atomic_json(node_path, snapshot)
    if errors:
        raise RuntimeError(errors[-1])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--threads', type=int, default=64)
    parser.add_argument('--nice', type=int, default=10)
    args = parser.parse_args(argv)
    if args.nice:
        os.nice(args.nice)
    worker(args.control, args.threads)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
