#!/usr/bin/env python3
"""Bounded read-only receipt cache warming; never changes an index or a worker."""
import argparse
import bisect
import concurrent.futures as futures
import datetime
import fcntl
import json
import os
import pathlib
import socket
import stat
import threading
import time

from migration_io import read_json, write_json
from migration_resume_index import _resume_controls


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def process_identity(pid):
    try:
        root = pathlib.Path('/proc') / str(pid)
        fields = (root / 'stat').read_text().rsplit(')', 1)[1].split()
        return {'pid': pid, 'startticks': fields[19], 'state': fields[0],
                'argv': (root / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
    except (OSError, ValueError):
        return None


def scan_directory(path):
    """Enumerate canonical names only, never descend a symlink/snapshot tree."""
    children, files = [], []
    try:
        if not stat.S_ISDIR(path.lstat().st_mode):
            return children, files
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.name == '.snapshots':
                    continue
                if entry.is_dir(follow_symlinks=False):
                    children.append(pathlib.Path(entry.path))
                elif entry.name.endswith('.json') and entry.is_file(follow_symlinks=False):
                    files.append(str(pathlib.Path(entry.path)))
    except OSError:
        pass
    return children, files


def receipt_names(ancestor, workers=16, check=lambda: None, report=lambda _: None):
    stages = ancestor / 'stages'
    if not stat.S_ISDIR(stages.lstat().st_mode):
        return []
    roots = []
    for stage in stages.iterdir():
        if stage.name == '.snapshots' or not stat.S_ISDIR(stage.lstat().st_mode):
            continue
        done = stage / 'done'
        try:
            if stat.S_ISDIR(done.lstat().st_mode):
                roots.append(done)
        except OSError:
            continue
    result = []
    pool = futures.ThreadPoolExecutor(max_workers=workers)
    pending = set()
    try:
        pending = {pool.submit(scan_directory, p) for p in roots}
        while pending:
            check()
            completed, pending = futures.wait(pending, timeout=1, return_when=futures.FIRST_COMPLETED)
            for task in completed:
                directories, files = task.result()
                result.extend(files)
                pending.update(pool.submit(scan_directory, p) for p in directories)
            report(len(result))
    finally:
        for task in pending:
            task.cancel()
        # Cooperative cancellation: only in-flight NFS reads must finish.
        pool.shutdown(wait=True, cancel_futures=True)
    return sorted(result)


def prefetch_one(path, cancel):
    if cancel.is_set():
        return 'cancelled'
    try:
        if not stat.S_ISREG(pathlib.Path(path).lstat().st_mode):
            return 'nonregular'
        # The original reader selects the newest immutable receipt version.
        # Its result is deliberately discarded: it is never checkpoint proof.
        read_json(path)
        return 'read'
    except Exception as exc:
        return type(exc).__name__


class Finished(Exception):
    pass


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--control', required=True)
    p.add_argument('--evidence', required=True)
    p.add_argument('--workers', type=int, default=16)
    p.add_argument('--lookahead', type=int, default=1024)
    p.add_argument('--max-seconds', type=int, default=900)
    args = p.parse_args()
    if not 1 <= args.workers <= 64 or not 128 <= args.lookahead <= 8192:
        p.error('workers must be1..64 and lookahead128..8192')
    c = pathlib.Path(args.control).resolve()
    evidence = pathlib.Path(args.evidence).resolve()
    if not evidence.is_relative_to(c / 'optimizations'):
        p.error('evidence must be a fresh optimization directory within control')
    cfg = read_json(c / 'config.json')
    lock = open('/tmp/' + cfg['run_id'] + '-receipt-prefetch.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    producer = read_json(c / 'recovery-roles' / (socket.gethostname() + '-producer.json'))
    initial_seed = read_json(c / 'seed-progress.json')
    if initial_seed.get('status') != 'reading_receipts':
        raise RuntimeError('prefetch is limited to an actual receipt-reading cursor')
    ancestor = pathlib.Path(initial_seed['current_control']).resolve()
    if ancestor not in _resume_controls(cfg['resume_from']):
        raise RuntimeError('receipt ancestor is outside the recorded ancestry')
    cancel = threading.Event()
    started = time.monotonic()
    record = {'status': 'enumerating', 'started': now(), 'pid': os.getpid(),
              'control': str(c), 'ancestor': str(ancestor), 'workers': args.workers,
              'lookahead': args.lookahead, 'producer': producer,
              'initial_seed': initial_seed, 'discovered': 0, 'counts': {},
              'read_only_cache_warming': True, 'checkpoint_authority': False}
    last_publish = 0
    last_guard = 0
    current_seed = initial_seed
    own_stop = evidence / 'PREFETCH_STOP'

    def publish(force=False):
        nonlocal last_publish
        if not force and time.monotonic() - last_publish < 5:
            return
        record.update(updated=now(), elapsed_seconds=time.monotonic() - started)
        write_json(evidence / 'prefetch.json', record)
        last_publish = time.monotonic()

    def guard():
        nonlocal last_guard, current_seed
        def finish(reason):
            record.update(status='stopping', reason=reason,
                          cancellation='cancel queued reads; wait for in-flight NFS reads')
            cancel.set()
            publish(True)
            raise Finished(reason)
        if (c / 'STOP').exists() or own_stop.exists():
            finish('stop marker; only the prefetcher exits')
        if time.monotonic() - started >= args.max_seconds:
            finish('time budget reached')
        if time.monotonic() - last_guard < 1:
            return
        live = process_identity(producer['pid'])
        if (not live or live['state'] == 'Z' or live['startticks'] != producer['startticks']
                or live['argv'] != producer['argv']):
            finish('original producer identity no longer matches')
        state = read_json(c / 'state.json')
        current_seed = read_json(c / 'seed-progress.json')
        if state.get('status') != 'preparing_resume' or current_seed.get('status') != 'reading_receipts':
            finish('receipt-reading phase ended')
        if current_seed.get('current_control') != str(ancestor):
            finish('ancestor changed')
        record['latest_producer_seed'] = current_seed
        last_guard = time.monotonic()

    def discovery(count):
        record['discovered'] = count
        publish()

    publish(True)
    try:
        guard()
        names = receipt_names(ancestor, args.workers, guard, discovery)
        record.update(status='prefetching', discovered=len(names))
        # Skip receipts already consumed; ordering is full path lexical order,
        # exactly as in the original builder, never numeric task-ID order.
        next_index = bisect.bisect_right(names, current_seed.get('current_receipt') or '')
        publish(True)
        with futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            pending = set()
            try:
                while next_index < len(names) or pending:
                    guard()
                    cursor = bisect.bisect_right(names, current_seed.get('current_receipt') or '')
                    next_index = max(next_index, cursor)
                    boundary = min(len(names), cursor + args.lookahead)
                    while next_index < boundary and len(pending) < args.workers * 2:
                        pending.add(pool.submit(prefetch_one, names[next_index], cancel))
                        next_index += 1
                    if pending:
                        completed, pending = futures.wait(pending, timeout=0.5,
                                                         return_when=futures.FIRST_COMPLETED)
                        for task in completed:
                            result = task.result()
                            record['counts'][result] = record['counts'].get(result, 0) + 1
                    else:
                        cancel.wait(0.5)
                    record.update(next_index=next_index, producer_cursor=cursor,
                                  queued_reads=len(pending), bounded_ahead=next_index-cursor)
                    publish()
            finally:
                cancel.set()
                for task in pending:
                    task.cancel()
        record['status'] = 'finished'
        record['reason'] = 'all remaining canonical receipts warmed'
    except Finished as exc:
        record.update(status='finished', reason=str(exc))
    except BaseException as exc:
        record.update(status='failed', error=type(exc).__name__ + ': ' + str(exc))
        raise
    finally:
        cancel.set()
        record['finished'] = now()
        publish(True)


if __name__ == '__main__':
    main()
