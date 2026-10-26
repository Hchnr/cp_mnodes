#!/usr/bin/env python3
"""Read-only directory name joins; publish missing candidates, never copy data."""
import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import collections
import json
import os
from pathlib import Path
import socket
import time
import uuid

from migration_io import read_json, write_json
from migration_run import validate_mounts
from migration_scan import PRIORITY_SUBTREES
from migration_worker import _open_parent, _components


def children(source, rel):
    fd, _ = _open_parent(source, rel + '/__discovery__' if rel else '__discovery__')
    try:
        with os.scandir(fd) as stream:
            return [rel + '/' + e.name if rel else e.name for e in stream
                    if e.is_dir(follow_symlinks=False)
                    and (rel + '/' + e.name if rel else e.name) not in PRIORITY_SUBTREES]
    finally:
        os.close(fd)


def prepare(control):
    cfg = read_json(control / 'config.json')
    if socket.gethostname() != cfg['master_host']:
        raise RuntimeError('discovery plan must be prepared on the real master')
    validate_mounts(cfg)
    roots = [{'rel': '', 'leaves_only': True}]
    for top in children(cfg['source'], ''):
        roots.append({'rel': top, 'leaves_only': True})
        for second in children(cfg['source'], top):
            # Wide dataset shard directories are independent discovery roots.
            if second.startswith('prepared_datasets/'):
                third = children(cfg['source'], second)
                roots.append({'rel': second, 'leaves_only': True})
                for rel in third:
                    if rel.endswith('/shards'):
                        roots.append({'rel': rel, 'leaves_only': True})
                        roots.extend({'rel': p, 'leaves_only': False} for p in children(cfg['source'], rel))
                    else:
                        roots.append({'rel': rel, 'leaves_only': False})
            else:
                roots.append({'rel': second, 'leaves_only': False})
    hosts = cfg['copy_hosts']
    plan = {'control': str(control), 'run_id': cfg['run_id'], 'operation': uuid.uuid4().hex,
            'roots': {h: [] for h in hosts}, 'created': time.time()}
    for index, row in enumerate(sorted(roots, key=lambda r: r['rel'])):
        plan['roots'][hosts[index % len(hosts)]].append(row)
    path = control / 'frontier-input' / 'plan.json'
    if path.exists():
        raise RuntimeError('discovery plan already exists')
    for host in hosts:
        for name in ('ready', 'done'):
            (path.parent / host / name).mkdir(parents=True, exist_ok=True)
    write_json(path, plan)
    return {'plan': str(path), 'roots': len(roots),
            'roots_by_host': {h: len(r) for h, r in plan['roots'].items()}}


def scan_directory(cfg, control, host, row):
    if (control / 'STOP').exists():
        raise RuntimeError('STOP requested')
    rel = row['rel']
    if rel:
        _components(rel)
    probe_rel = rel + '/__discovery__' if rel else '__discovery__'
    source, _ = _open_parent(cfg['source'], probe_rel)
    target = None
    try:
        try:
            target, _ = _open_parent(cfg['destination'], probe_rel)
            with os.scandir(target) as stream:
                present = {e.name for e in stream}
        except FileNotFoundError:
            present = set()
        except NotADirectoryError:
            return {'directories': [], 'files_seen': 0, 'missing': 0, 'conflicts': 1}
        dirs, batch = [], []
        seen = missing = 0
        def flush():
            if batch:
                path = control / 'frontier-input' / host / 'ready' / (uuid.uuid4().hex + '.json')
                write_json(path, {'control': str(control), 'run_id': cfg['run_id'],
                    'host': host, 'entries': list(batch)})
                batch.clear()
        with os.scandir(source) as stream:
            for entry in stream:
                path = rel + '/' + entry.name if rel else entry.name
                if path in PRIORITY_SUBTREES:
                    continue
                if entry.is_dir(follow_symlinks=False):
                    if not row.get('leaves_only'):
                        dirs.append({'rel': path, 'leaves_only': False})
                    continue
                seen += 1
                if seen % 4096 == 0 and (control / 'STOP').exists():
                    raise RuntimeError('STOP requested')
                if entry.name in present:
                    continue
                missing += 1
                # Probe service obtains actual kind and identity. A name join
                # is only a candidate hint, never authority to overwrite.
                batch.append({'rel': path, 'kind': 'file', '_refresh_source': True})
                if len(batch) == 512:
                    flush()
        flush()
        return {'directories': dirs, 'files_seen': seen, 'missing': missing, 'conflicts': 0}
    finally:
        os.close(source)
        if target is not None:
            os.close(target)


def run(control, threads):
    cfg = read_json(control / 'config.json')
    host = socket.gethostname()
    if host not in cfg['copy_hosts']:
        raise RuntimeError('host is not on the explicit recovery allowlist')
    validate_mounts(cfg)
    import fcntl
    lock = open('/tmp/' + cfg['run_id'] + '-missing-discovery.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    plan = read_json(control / 'frontier-input/plan.json')
    if plan.get('control') != str(control) or plan.get('run_id') != cfg['run_id']:
        raise ValueError('discovery plan identity mismatch')
    pending = collections.deque(plan['roots'][host])
    counts = {'directories': 0, 'files_seen': 0, 'missing': 0, 'conflicts': 0}
    progress = control / 'frontier-input' / host / 'progress.json'
    began = time.monotonic(); last = 0
    def save(status, error=None):
        write_json(progress, dict(counts, status=status, host=host, pid=os.getpid(),
            run_id=cfg['run_id'], updated=time.time(), elapsed=time.monotonic()-began,
            pending_directories=len(pending), error=error))
    try:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            jobs = {}
            while pending or jobs:
                while pending and len(jobs) < threads * 2:
                    row = pending.popleft()
                    jobs[pool.submit(scan_directory, cfg, control, host, row)] = row
                done, _ = wait(jobs, timeout=.2, return_when=FIRST_COMPLETED)
                for job in done:
                    row = jobs.pop(job)
                    result = job.result()
                    pending.extend(result.pop('directories'))
                    counts['directories'] += 1
                    for key, value in result.items(): counts[key] += value
                if time.monotonic() - last >= 3:
                    save('scanning'); last = time.monotonic()
        save('complete')
    except BaseException as exc:
        save('failed', repr(exc))
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=('prepare', 'run'))
    p.add_argument('--control', type=Path, required=True)
    p.add_argument('--threads', type=int, default=16)
    args = p.parse_args()
    if not 1 <= args.threads <= 64:
        p.error('threads must be within 1..64')
    if args.mode == 'prepare': print(json.dumps(prepare(args.control.resolve())), flush=True)
    else: run(args.control.resolve(), args.threads)


if __name__ == '__main__':
    main()
