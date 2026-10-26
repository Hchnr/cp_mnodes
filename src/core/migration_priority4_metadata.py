"""UNDEPLOYED P4 directory component; not a complete migration engine.

Parallel destination-directory metadata finalization, independent of queues.

All directory creation finishes before metadata is copied. Depth barriers keep
restrictive parent permissions from blocking child finalization. The mount
root's metadata is preserved. home and repos are ordinary P4 children.
"""
import argparse
import collections
import concurrent.futures
import contextlib
import datetime
import json
import os
import pathlib
import re
import stat
import threading
import time
import uuid

from migration_io import read_json, write_json
from migration_worker import (_open_root, _record, _metadata_fd, _identity,
                              ensure_destination_directory, SourceChanged)

from migration_priority4_worker import ScopeGuard
from migration_metadata import _mkdir as original_mkdir, _metadata as original_metadata

# Only the mount root is shared in P4; home/repos are ordinary copied children.
SHARED_ANCESTORS = frozenset(('',))
_DIRECTORY = re.compile(r'"kind"\s*:\s*"directory"')


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _directories(manifest, stop, only_shared=False):
    result = {}
    with open(manifest, encoding='utf-8') as handle:
        for line in handle:
            if stop.is_set():
                break
            if not _DIRECTORY.search(line):
                continue
            entry = json.loads(line)
            if entry.get('kind') != 'directory':
                continue
            rel = entry['rel']
            if not isinstance(rel, str) or os.path.isabs(rel) or '\x00' in rel or any(
                    p in ('', '.', '..') for p in rel.split('/') if rel):
                raise ValueError('unsafe directory relative path: ' + repr(rel))
            if not only_shared or rel in SHARED_ANCESTORS:
                result[rel] = entry
    return result


def _guarded_directory(cfg, entry, stop, guard, operation):
    rel = entry.get('rel')
    if stop.is_set():
        return {'rel': rel, 'status': 'stopped', 'operation': operation}
    try:
        guard.assert_config(cfg)
        if entry.get('kind') != 'directory':
            raise ValueError('P4 directory finalizer requires directory entries')
        policy = guard.destination_metadata_policy(rel)
        if policy == 'preserve_mount_root':
            return {'rel': rel, 'status': 'preserved_mount_root', 'operation': operation,
                    'policy': 'do not copy mode, owner, ACL, timestamps or xattrs onto mount root'}
    except (ValueError, TypeError, KeyError) as exc:
        return {'rel': rel, 'status': 'error', 'operation': operation,
                'scope_violation': True, 'detail': 'Priority4Scope: ' + str(exc)}
    delegate = original_mkdir if operation == 'mkdir' else original_metadata
    return delegate(cfg, entry, stop)


def _mkdir(cfg, entry, stop, guard):
    return _guarded_directory(cfg, entry, stop, guard, 'mkdir')


def _metadata(cfg, entry, stop, guard):
    return _guarded_directory(cfg, entry, stop, guard, 'metadata')


def _manifest_path(control, manifest):
    # Lexical validation: no per-entry resolve/stat added to the NFS workload.
    path = pathlib.Path(os.path.abspath(manifest))
    if (path.parent != control / 'manifests' or path.suffix != '.jsonl' or
            not re.fullmatch(r'p4-r[123](?:-[A-Za-z0-9_-]+)?', path.stem)):
        raise ValueError('P4 requires its own p4-r1..3 manifest under control/manifests')
    return path


def _result_path(control, label, result_path):
    final = pathlib.Path(os.path.abspath(result_path)) if result_path else control / 'metadata' / label / 'result.json'
    if (final.suffix != '.json' or not any(final.is_relative_to(parent) for parent in (
            control / 'pipeline-results', control / 'metadata' / label))):
        raise ValueError('P4 directory result must remain in its metadata job or pipeline-results')
    return final


@contextlib.contextmanager
def _exclusive(control):
    """One directory finalizer on the coordinator node, never an NFS lock."""
    import fcntl
    control = pathlib.Path(os.path.abspath(control))
    cfg = read_json(control / 'config.json')
    guard = ScopeGuard(str(control), cfg)
    with open('/tmp/' + guard.run_id + '-p4-directories.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _parallel(entries, operation, threads, stop, receive):
    """Bound queued futures so STOP does not leave hundreds of thousands queued."""
    iterator = iter(entries)
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
        pending = set()
        exhausted = False
        while not stop.is_set() and (pending or not exhausted):
            while not exhausted and not stop.is_set() and len(pending) < threads * 2:
                try:
                    entry = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending.add(pool.submit(operation, entry))
            if not pending:
                break
            ready, pending = concurrent.futures.wait(pending, timeout=0.25,
                                                       return_when=concurrent.futures.FIRST_COMPLETED)
            for future in ready:
                receive(future.result())
        for future in pending:
            if not future.cancel():
                receive(future.result())


def _run(control, entries_loader, label, threads=32, defer_roots=True, result_path=None):
    control = pathlib.Path(os.path.abspath(control))
    cfg = read_json(control / 'config.json')
    guard = ScopeGuard(str(control), cfg)
    if label != 'shared-ancestors' and not re.fullmatch(r'p4-r[123](?:-[A-Za-z0-9_-]+)?', label):
        raise ValueError('P4 metadata job label is invalid')
    final_path = _result_path(control, label, result_path)
    if 'source_mount' in cfg and 'destination_mount' in cfg:
        from migration_run import validate_mounts
        validate_mounts(cfg)
    threads = max(1, min(32, int(threads)))
    job = control / 'metadata' / label
    job.mkdir(parents=True, exist_ok=True)
    progress_path = job / 'progress.json'
    stop = threading.Event()
    finished = threading.Event()
    mutex = threading.Lock()
    summary = {'job': label, 'host': __import__('socket').gethostname(), 'pid': os.getpid(),
               'started': now(), 'updated': now(), 'status': 'reading_manifest', 'threads': threads,
               'scope': 'project_priority4', 'mount_root_metadata_policy': 'preserve_existing',
               'preserved_mount_root_operations': 0,
               'directories': 0, 'created_checked': 0, 'errors': 0, 'source_changed': 0,
               'metadata_warnings': 0, 'issues': 0, 'stopped': False,
               'progress_path': str(progress_path), 'result_path': str(final_path),
               'receipt_directory': str(job / 'receipts')}
    write_json(progress_path, summary)

    def watch():
        last_publish = 0
        while not finished.wait(0.25):
            if (control / 'STOP').exists():
                stop.set()
            if time.monotonic() - last_publish >= 3:
                with mutex:
                    summary['updated'] = now()
                    value = dict(summary)
                write_json(progress_path, value)
                last_publish = time.monotonic()
    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    receipts = []
    issues = []
    failed_create = set()

    def flush():
        if receipts:
            write_json(job / 'receipts' / (uuid.uuid4().hex + '.json'),
                       {'written': now(), 'results': list(receipts)})
            receipts.clear()
        if issues:
            write_json(job / 'issues' / (uuid.uuid4().hex + '.json'),
                       {'written': now(), 'issues': list(issues)})
            issues.clear()

    def receive(result):
        receipts.append(result)
        status = result['status']
        with mutex:
            if status == 'preserved_mount_root':
                summary['preserved_mount_root_operations'] += 1
            elif status == 'created_checked':
                summary['created_checked'] += 1
            elif status == 'applied':
                summary['directories'] += 1
            if status in ('error', 'changed') or result.get('warnings'):
                summary['errors'] += 1
                summary['issues'] += 1
                if status == 'changed':
                    summary['source_changed'] += 1
                if result.get('warnings'):
                    summary['metadata_warnings'] += 1
                issues.append(result)
            if result['operation'] == 'mkdir' and status not in ('created_checked', 'preserved_mount_root', 'stopped'):
                failed_create.add(result['rel'])
        if len(receipts) >= 512:
            flush()

    try:
        if (control / 'STOP').exists():
            stop.set()
        entries = entries_loader(stop)
        deferred = [entry for rel, entry in entries.items() if defer_roots and rel in SHARED_ANCESTORS]
        selected = [entry for rel, entry in entries.items() if not defer_roots or rel not in SHARED_ANCESTORS]
        with mutex:
            summary.update(total_directories=len(selected), deferred=len(deferred),
                           deferred_entries=deferred, status='creating_directories')
        # Parent-first scheduling plus secure mkdir-p walks; no metadata yet.
        selected.sort(key=lambda entry: entry['rel'].count('/') + bool(entry['rel']))
        _parallel(selected, lambda entry: _mkdir(cfg, entry, stop, guard), threads, stop, receive)
        with mutex:
            summary['status'] = 'applying_metadata'
        # Barrier above ensures mkdir cannot invalidate restored parent mtimes.
        # Depth barriers additionally preserve traversal until children finish.
        depths = collections.defaultdict(list)
        for entry in selected:
            if entry['rel'] not in failed_create:
                depths[entry['rel'].count('/') + bool(entry['rel'])].append(entry)
        for depth in sorted(depths, reverse=True):
            if stop.is_set():
                break
            _parallel(depths[depth], lambda entry: _metadata(cfg, entry, stop, guard), threads, stop, receive)
        with mutex:
            summary.update(status='stopped' if stop.is_set() else 'complete', stopped=stop.is_set())
    except BaseException as exc:
        with mutex:
            summary.update(status='failed', errors=summary['errors'] + 1,
                           detail=type(exc).__name__ + ': ' + str(exc))
        issues.append({'operation': 'job', 'status': 'error', 'detail': summary['detail']})
        raise
    finally:
        finished.set()
        watcher.join(timeout=5)
        flush()
        summary.update(updated=now(), finished=now())
        write_json(progress_path, summary)
        canonical_result = job / 'result.json'
        write_json(canonical_result, summary)
        if final_path != canonical_result:
            write_json(final_path, summary)
    return summary


def finalize_directories(control, manifest, threads=32, defer_roots=True, result_path=None):
    control = pathlib.Path(os.path.abspath(control))
    manifest = _manifest_path(control, manifest)
    with _exclusive(control):
        return _run(control, lambda stop: _directories(manifest, stop), manifest.stem,
                    threads, defer_roots, result_path)


def finalize_shared_ancestors(control, manifests, threads=32, result_path=None):
    control = pathlib.Path(os.path.abspath(control))
    def load(stop):
        result = {}
        for manifest in manifests:
            if stop.is_set():
                break
            manifest = _manifest_path(control, manifest)
            try:
                previous = read_json(control / 'metadata' / manifest.stem / 'result.json')
                deferred = previous['deferred_entries']
                if (not isinstance(deferred, list) or any(
                        not isinstance(entry, dict) or entry.get('kind') != 'directory' or
                        entry.get('rel') != '' for entry in deferred)):
                    raise RuntimeError('P4 deferred metadata contains non-mount-root entries')
            except (OSError, KeyError, ValueError):
                deferred = list(_directories(manifest, stop, only_shared=True).values())
            result.update((entry['rel'], entry) for entry in deferred)
        return result
    with _exclusive(control):
        return _run(control, load, 'shared-ancestors', threads, False, result_path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--manifest', action='append', required=True)
    parser.add_argument('--threads', type=int, default=32)
    parser.add_argument('--result')
    parser.add_argument('--shared-ancestors', action='store_true')
    args = parser.parse_args(argv)
    if args.shared_ancestors:
        result = finalize_shared_ancestors(args.control, args.manifest, args.threads, args.result)
    elif len(args.manifest) != 1:
        parser.error('one --manifest required unless --shared-ancestors is set')
    else:
        result = finalize_directories(args.control, args.manifest[0], args.threads, True, args.result)
    print(json.dumps(result, ensure_ascii=True))
    return 0 if result['status'] == 'complete' else 2


if __name__ == '__main__':
    raise SystemExit(main())
