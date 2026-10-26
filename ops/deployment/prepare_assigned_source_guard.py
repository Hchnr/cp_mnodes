#!/usr/bin/env python3
"""Read-only preparation/audit; no producer signals or automatic cutover.

The cache alone does NOT authorize skipping stats. The future warm-only caller
must prove assignment lineage and enforce full formal scans independently.
"""

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import pickle
import sqlite3
import tempfile
import time

from migration_assigned_source_guard import AssignedSourceGuard, sample_manifest
from migration_io import read_json, write_json
from migration_warm_scan_checkpoint import validate_runtime, resource_budget


def build(stream, prefix_tasks, descriptor, report=None):
    guard = AssignedSourceGuard()
    digest = hashlib.sha256()
    tasks = entries = 0
    last = time.monotonic()
    for raw in iter(lambda: stream.readline(16 * 1024**2 + 1), b''):
        if len(raw) > 16 * 1024**2 or not raw.endswith(b'\n'):
            raise ValueError('assignment prefix has an incomplete record')
        digest.update(raw)
        task = json.loads(raw)
        if (type(task.get('id')) is not int or task['id'] != tasks
                or task.get('phase') != 3 or task.get('round') != 3
                or not isinstance(task.get('entries'), list) or not task['entries']):
            raise ValueError('assignment task sequence/scope differs')
        for entry in task['entries']:
            guard.add(entry)
            entries += 1
        tasks += 1
        if report is not None and time.monotonic() - last >= 5:
            report(tasks=tasks, entries=entries, eligible_inodes=len(guard.inode_paths),
                   ambiguous_or_multilink_inodes=len(guard.excluded))
            last = time.monotonic()
    if (tasks != prefix_tasks or tasks != descriptor['prefix_tasks']
            or digest.hexdigest() != descriptor['prefix_sha256']):
        raise ValueError('assignment prefix count or digest differs')
    return guard, dict(tasks=tasks, entries=entries, prefix_sha256=digest.hexdigest())


def prepare(control, directory, expected, assignment_checkpoint):
    control, directory = Path(control).resolve(), Path(directory).resolve()
    if directory.parent != control or not directory.name.startswith('warm-assigned-source-guard-'):
        raise ValueError('preparation needs its own direct control child')
    cfg, state, producer = validate_runtime(control, expected)
    descriptor_path = Path(assignment_checkpoint).resolve()
    if not descriptor_path.is_relative_to(control):
        raise ValueError('assignment descriptor escapes control')
    descriptor = read_json(descriptor_path)
    floor = state['next_task_id']
    ledger = Path(descriptor['base_ledger'])
    if (descriptor.get('version') != 2 or descriptor.get('control') != str(control)
            or descriptor.get('run_id') != cfg['run_id'] or descriptor.get('prefix_tasks') != floor
            or descriptor.get('maximum_id') != floor - 1
            or not ledger.is_relative_to(control) or ledger.is_symlink()):
        raise ValueError('assignment prefix identity/floor differs')
    resource_budget(ledger.stat().st_size)
    uri = (Path('/tmp') / (cfg['run_id'] + '-broker.sqlite')).as_uri() + '?mode=ro'

    def completed_prefix():
        with sqlite3.connect(uri, uri=True, timeout=2) as db:
            row = db.execute('SELECT count(*),min(id),max(id),sum(status=\'done\') '
                             'FROM claims WHERE stage=? AND id<?', ('p3-r3', floor)).fetchone()
        if row != (floor, 0, floor - 1, floor):
            raise ValueError('assignment prefix is not wholly completed and gapless')

    completed_prefix()
    directory.mkdir(mode=0o700)
    record = dict(status='reading_assignment_metadata_only', control=str(control), run_id=cfg['run_id'],
                  producer=producer, assignment_checkpoint=str(descriptor_path),
                  assignment_descriptor_sha256=hashlib.sha256(descriptor_path.read_bytes()).hexdigest(),
                  base_ledger=str(ledger), prefix_floor=floor, started=time.time(),
                  authorizes_cutover=False, authorizes_stat_skip=False, data_processes_signalled=0)

    def report(**values):
        record.update(values, updated=time.time())
        write_json(directory / 'preparation.json', record)
        print(json.dumps({k: v for k, v in record.items() if k != 'producer'}), flush=True)

    enabled = gc.isenabled()
    try:
        gc.disable()
        report()
        with ledger.open('rb', buffering=8 * 1024**2) as stream:
            guard, counts = build(stream, floor, descriptor, report)
        _, current_state, actual = validate_runtime(control, expected)
        if current_state['next_task_id'] != floor or any(actual[k] != producer[k]
                for k in ('pid', 'start_ticks', 'argv')):
            raise ValueError('producer or assignment floor changed while preparing')
        completed_prefix()
        # Any new inode/path omission is observable before a runtime rollout.
        sample = sample_manifest(control / 'manifests/recovery-missing.jsonl', guard)
        report(status='saving_private_guard', sample=sample, **counts)
        scratch = Path(tempfile.mkdtemp(prefix=cfg['run_id'] + '-assigned-source-', dir='/tmp'))
        cache = scratch / 'single-link-inodes.pickle'
        with cache.open('xb') as stream:
            os.chmod(cache, 0o600)
            pickle.dump({'inode_paths': guard.inode_paths, 'excluded': guard.excluded}, stream, protocol=5)
            stream.flush()
            os.fsync(stream.fileno())
        digest = hashlib.sha256()
        with cache.open('rb') as stream:
            for chunk in iter(lambda: stream.read(8 * 1024**2), b''):
                digest.update(chunk)
        report(status='prepared_assignment_metadata_only', cache=str(cache), cache_sha256=digest.hexdigest(),
               cache_bytes=cache.stat().st_size, eligible_inodes=len(guard.inode_paths),
               ambiguous_or_multilink_inodes=len(guard.excluded), **counts)
        return record
    except BaseException as exc:
        report(status='failed_readonly_preparation', error=type(exc).__name__ + ': ' + str(exc))
        raise
    finally:
        if enabled:
            gc.enable()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--expected-runtime', required=True)
    parser.add_argument('--assignment-checkpoint', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    prepare(args.control, args.directory, read_json(Path(args.expected_runtime)), args.assignment_checkpoint)


if __name__ == '__main__':
    main()
