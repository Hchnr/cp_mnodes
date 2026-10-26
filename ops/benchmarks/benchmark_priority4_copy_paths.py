#!/usr/bin/env python3
"""Bounded full-copy comparison in a fresh, owner-marked P4 scratch tree.

No claims, leases, runtime configuration, roles or process identities change.
The frozen live copy engine is compared with only its directory opener changed.
All variants retain the original source checks, SHA, metadata and fsync behavior.
Scratch outputs are retained for independent verification; none are migration
outputs or progress. Recent source/directory caches limit generalization.
"""
import sys
sys.dont_write_bytecode = True

from concurrent.futures import ThreadPoolExecutor
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat
import threading
import time
import uuid

from migration_io import read_json, write_json
from migration_priority4_run import validate_authorization
from migration_scan_boundary import require_identity


CONTROL = Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053')
MODES = ('legacy', 'native_readonly', 'destination_opath',
         'destination_opath', 'native_readonly', 'legacy')
SCAFFOLDING_NOTE = 'Parent scaffolding and source SHA baseline are outside the timed interval.'


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def directory_opener(mode, destination, helper):
    if mode == 'legacy':
        return None
    if mode not in ('native_readonly', 'destination_opath'):
        raise ValueError('unknown directory strategy')
    destination = os.path.abspath(destination)
    def open_parent(root, rel):
        fn = (helper.open_parent_path if mode == 'destination_opath'
              and os.path.abspath(os.fsdecode(root)) == destination
              and rel != '__directory_open_root__'
              else helper.open_parent_readonly)
        return fn(root, rel)
    return open_parent


def stable_source(engine, source, entry):
    fd, name = engine._open_parent(source, entry['rel'])
    try:
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or any(getattr(info, 'st_' + k) != entry[k]
                       for k in ('ino', 'size', 'mtime_ns', 'ctime_ns'))):
            raise ValueError('benchmark source differs from its current P4 task')
        return info
    finally:
        os.close(fd)


def run_trial(engine, helper, source, destination, entries, hashes, mode,
              run_id, check, *, threads=64):
    """Destination must be exclusively created by the caller, initially empty."""
    if not entries or destination.is_symlink() or any(destination.iterdir()):
        raise ValueError('an empty exclusive trial directory is required')
    engine.configure_directory_opener(None)
    for rel in sorted({e['rel'].rsplit('/', 1)[0] if '/' in e['rel'] else '' for e in entries}):
        engine.ensure_destination_directory(str(destination), rel)
    for entry in entries:
        stable_source(engine, source, entry)
    engine.configure_directory_opener(directory_opener(mode, destination, helper))
    began, cpu = time.monotonic(), time.process_time()
    def copy(entry):
        check()
        result = engine.copy_entry(source, str(destination), entry, run_id, verify=True)
        if (result.get('status') != 'copied' or result.get('metadata_warnings')
                or result.get('sha256') != hashes[entry['rel']]):
            raise RuntimeError('full-copy benchmark failed: ' + str(result))
        info = result['dst_stat']
        for key in ('size', 'uid', 'gid', 'mtime_ns'):
            if info[key] != entry[key]:
                raise RuntimeError('scratch target metadata differs: ' + key)
        if stat.S_IMODE(info['mode']) != stat.S_IMODE(entry['mode']):
            raise RuntimeError('scratch target mode differs')
        return result['bytes']
    try:
        with ThreadPoolExecutor(max_workers=threads) as pool:
            sizes = list(pool.map(copy, entries))
    finally:
        engine.configure_directory_opener(None)
    elapsed = time.monotonic() - began
    check()
    return dict(mode=mode, seconds=elapsed, cpu_seconds=time.process_time() - cpu,
                files=len(entries), bytes=sum(sizes), files_s=len(entries) / elapsed,
                logical_GB_s=sum(sizes) / elapsed / 1e9, threads=threads,
                source_identity_and_destination_sha_verified=True,
                destination=str(destination))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, required=True)
    args = parser.parse_args()
    control = args.control
    if control != CONTROL or control.is_symlink():
        raise ValueError('the exact live independent P4 control is required')
    cfg = read_json(control / 'config.json')
    guard = validate_authorization(control, cfg)
    host = socket.gethostname()
    if host != 'job-' + cfg['job'] + '-master-0':
        raise ValueError('actual P4 master required')
    role = read_json(control / 'roles' / (host + '-producer.json'))
    producer = require_identity(role)
    if (read_json(control / 'state.json').get('status') != 'scanning'
            or (control / 'STOP').exists()):
        raise ValueError('P4 must be scanning without STOP')
    manifest = read_json(control / 'source-sha256.json')
    engine_path = control / 'migration_worker.py'
    if engine_path.is_symlink() or sha(engine_path) != manifest['migration_worker.py']:
        raise ValueError('frozen original P4 data engine differs')
    engine = load_module('_p4_benchmark_frozen_engine', engine_path)
    helper_path = Path(__file__).parent / 'migration_linux_open.py'
    helper = load_module('_p4_benchmark_directory_helper', helper_path)
    engine.configure_rate_limit(cfg['io_bytes_per_second'])
    cancelled = threading.Event()
    engine.configure_cancel_event(cancelled)
    deadline = time.monotonic() + 240
    def check():
        if (control / 'STOP').exists() or time.monotonic() >= deadline:
            cancelled.set()
            raise RuntimeError('benchmark bounded deadline or migration STOP')
        require_identity(producer)
    def watchdog():
        while not cancelled.wait(.5):
            if time.monotonic() >= deadline or (control / 'STOP').exists():
                cancelled.set()
                return
    thread = threading.Thread(target=watchdog, daemon=True)
    thread.start()
    root, result = None, None
    try:
        broker = Path('/tmp/' + cfg['run_id'] + '-broker.sqlite')
        if broker.is_symlink() or not broker.is_file():
            raise ValueError('original local P4 broker database required')
        with sqlite3.connect(broker.as_uri() + '?mode=ro', uri=True) as db:
            tasks = [json.loads(row[0]) for row in db.execute(
                'SELECT payload FROM claims WHERE stage=? ORDER BY id DESC LIMIT 8', ('p4-r1',))]
        entries, seen, total = [], set(), 0
        for task in tasks:
            for entry in task['entries']:
                guard.assert_task(task, entry)
                if (entry['kind'] != 'file' or entry.get('nlink') != 1
                        or not 0 < entry.get('size', 0) <= 1024**2
                        or entry['rel'] in seen or total + entry['size'] > 64 * 1024**2):
                    continue
                stable_source(engine, cfg['source'], entry)
                entries.append(entry)
                seen.add(entry['rel'])
                total += entry['size']
                if len(entries) >= 512:
                    break
            if len(entries) >= 512:
                break
        if len(entries) < 128:
            raise RuntimeError('insufficient bounded stable regular-file sample')
        check()
        root = control.parent / ('.hcr-copy-benchmark-p4-' + uuid.uuid4().hex)
        root.mkdir(mode=0o700)
        owner = uuid.uuid4().hex
        write_json(root / 'owner.json', dict(owner=owner, control=str(control), host=host,
            producer=producer, created=time.time(), scratch_only=True))
        result = dict(status='running', control=str(control), source=cfg['source'],
            destination_scratch=str(root), started=time.time(), owner=owner,
            data_engine_sha256=sha(engine_path), helper_sha256=sha(helper_path),
            benchmark_sha256=sha(__file__), selected_files=len(entries), selected_bytes=total,
            source_mean_depth=sum(len(e['rel'].split('/')) for e in entries)/len(entries),
            broker_requests=0, copy_processes_signalled=0, broker_processes_signalled=0,
            runtime_changes=0, samples=[],
            caveats=['Recent source and directory caches; no cold-cache claim.',
                     'Same files in forward and reverse mode order; independent empty target per trial.',
                     SCAFFOLDING_NOTE,
                     'All copy variants include original SHA, metadata, file fsync and exclusive publication.',
                     'No production HTTP/lease/receipt cost is included; not live-worker or cluster throughput.',
                     'Shared production storage stays active; scratch I/O is bounded, not zero impact.'])
        write_json(root / 'results.json', result)
        hashes = {}
        for entry in entries:
            check()
            parent, name = engine._open_parent(cfg['source'], entry['rel'])
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                try:
                    before = os.fstat(fd)
                    engine._check_scanned(before, entry)
                    hashes[entry['rel']] = engine._hash_fd(fd)
                    if engine._identity(os.fstat(fd)) != engine._identity(before):
                        raise ValueError('source changed during SHA baseline')
                finally:
                    os.close(fd)
            finally:
                os.close(parent)
        write_json(root / 'source-sample.json', dict(entries=entries, sha256=hashes))
        for index, mode in enumerate(MODES):
            check()
            if read_json(root / 'owner.json')['owner'] != owner or root.is_symlink():
                raise RuntimeError('scratch ownership changed')
            destination = root / ('trial-' + str(index) + '-' + mode)
            destination.mkdir(mode=0o700)
            sample = run_trial(engine, helper, cfg['source'], destination, entries, hashes,
                               mode, 'p4-benchmark-' + owner, check)
            result['samples'].append(sample)
            write_json(root / 'results.json', result)
            print(json.dumps(sample), flush=True)
        check()
        result.update(status='complete', finished=time.time(), actual_backend=helper.safe_open_backend())
        write_json(root / 'results.json', result)
        print(json.dumps(dict(status='complete', evidence=str(root / 'results.json'))), flush=True)
    except BaseException as exc:
        if result is not None:
            result.update(status='failed', error=repr(exc), finished=time.time())
            write_json(root / 'results.json', result)
        raise
    finally:
        cancelled.set()
        thread.join(timeout=1)


if __name__ == '__main__':
    main()
