#!/usr/bin/env python3
"""Bounded, read-only directory/stat parallelism comparison. No task writer."""
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
import argparse
import itertools
import json
import multiprocessing
import os
from pathlib import Path
import socket
import sqlite3
import time

from migration_io import read_json, write_json
from migration_linux_open import open_directory_beneath
from migration_priority4_worker import ScopeGuard
from migration_scan_boundary import require_identity
from migration_worker import _open_root, _components
from migration_process_scan import _leaf_record


ROOT_FD = None
STAT_POOL = None


def initialize(source, expected):
    global ROOT_FD, STAT_POOL
    ROOT_FD = _open_root(source)
    info = os.fstat(ROOT_FD)
    if (info.st_dev, info.st_ino) != tuple(expected):
        os.close(ROOT_FD)
        ROOT_FD = None
        raise RuntimeError('readonly source benchmark root changed')
    STAT_POOL = ThreadPoolExecutor(max_workers=8)


def warm(_):
    time.sleep(.1)
    return os.getpid()


def visit(rel):
    _components(rel)
    if rel == 'eai_pwm' or rel.startswith('eai_pwm/'):
        raise ValueError('P4 benchmark cannot enter covered eai_pwm')
    started, cpu = time.monotonic(), time.process_time()
    fd = open_directory_beneath(ROOT_FD, rel)
    try:
        with os.scandir(fd) as entries:
            selected = list(itertools.islice(entries, 65))
        if len(selected) > 64:
            return dict(skipped_large_directory=1, files=0, symlinks=0, errors=0,
                        cpu=time.process_time() - cpu, elapsed=time.monotonic() - started)
        leaves = [e.name for e in selected if not e.is_dir(follow_symlinks=False)]
        records = list(STAT_POOL.map(lambda name: _leaf_record(fd, name, rel + '/' + name), leaves))
        return dict(files=sum(e['kind'] == 'file' for e in records),
            symlinks=sum(e['kind'] == 'symlink' for e in records),
            errors=sum(e['kind'] == 'error' for e in records),
            cpu=time.process_time() - cpu, elapsed=time.monotonic() - started)
    finally:
        os.close(fd)


def bounded_map(pool, paths, concurrency):
    remaining, pending, result = iter(paths), set(), []
    def feed():
        while len(pending) < concurrency:
            try:
                rel = next(remaining)
            except StopIteration:
                break
            pending.add(pool.submit(visit, rel))
    feed()
    while pending:
        ready, pending = wait(pending, return_when=FIRST_COMPLETED)
        result.extend(f.result() for f in ready)
        feed()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    args = parser.parse_args()
    bundle = args.bundle.resolve()
    control = bundle.parent
    if not bundle.name.startswith('fast-p4-source-') or control.parent != Path('/share-mixed/project'):
        raise ValueError('actual active P4 bundle required')
    cfg = read_json(control / 'config.json')
    guard = ScopeGuard(str(control), cfg)
    if socket.gethostname() != 'job-' + cfg['job'] + '-master-0':
        raise ValueError('actual P4 master required')
    producer = read_json(bundle / 'producer-role.json')
    require_identity(producer)
    with sqlite3.connect(Path('/tmp/' + cfg['run_id'] + '-broker.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        tasks = [json.loads(row[0]) for row in db.execute(
            'SELECT payload FROM claims WHERE stage=? ORDER BY id DESC LIMIT 128', ('p4-r1',))]
    parents = list(dict.fromkeys(e['rel'].rsplit('/', 2)[0] for task in tasks for e in task['entries']))[:16]
    root = _open_root(cfg['source'])
    info = os.fstat(root)
    paths = []
    try:
        for rel in parents:
            guard.assert_path(rel)
            fd = open_directory_beneath(root, rel)
            try:
                with os.scandir(fd) as entries:
                    for entry in itertools.islice(entries, 2048):
                        path = rel + '/' + entry.name
                        if entry.is_dir(follow_symlinks=False) and path not in paths:
                            guard.assert_path(path)
                            paths.append(path)
                        if len(paths) >= 4096:
                            break
            finally:
                os.close(fd)
            if len(paths) >= 4096:
                break
    finally:
        os.close(root)
    if len(paths) < 512:
        raise RuntimeError('insufficient disjoint bounded source directories; no live changes made')
    result = dict(control=str(control), producer=producer, source=cfg['source'], selected_directories=len(paths),
                  samples=[], runtime_changes=0, network_job_requests=0, started=time.time(),
                  caveat='recent parent caches; excludes manifest/publication and cannot predict end-to-end GB/s')
    with ProcessPoolExecutor(max_workers=64, mp_context=multiprocessing.get_context('spawn'),
            initializer=initialize, initargs=(cfg['source'], (info.st_dev, info.st_ino))) as pool:
        warm_pids = set(pool.map(warm, range(128)))
        result['warm_processes_observed'] = len(warm_pids)
        chunk = len(paths) // 4
        for index, concurrency in enumerate((16, 64, 64, 16)):
            require_identity(producer)
            selected = paths[index * chunk:(index + 1) * chunk]
            began = time.monotonic()
            rows = bounded_map(pool, selected, concurrency)
            elapsed = time.monotonic() - began
            value = dict(concurrency=concurrency, directories=len(rows), seconds=elapsed,
                files=sum(x['files'] for x in rows), errors=sum(x['errors'] for x in rows),
                skipped_large=sum(x.get('skipped_large_directory', 0) for x in rows),
                cpu_seconds=sum(x['cpu'] for x in rows))
            value['files_per_second'] = value['files'] / elapsed
            result['samples'].append(value)
            write_json(bundle / 'readonly-source-parallelism.json', result)
            print(json.dumps(value), flush=True)
    result['finished'] = time.time()
    write_json(bundle / 'readonly-source-parallelism.json', result)


if __name__ == '__main__':
    main()
