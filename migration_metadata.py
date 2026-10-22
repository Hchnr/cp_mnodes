"""Parallel destination-directory metadata finalization, independent of queues.

All directory creation finishes before metadata is copied. Depth barriers keep
restrictive parent permissions from blocking child finalization. Shared root,
home, and repos metadata is deferred until every phase's last writes finish.
"""
import argparse
import collections
import concurrent.futures
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

SHARED_ANCESTORS = frozenset(('', 'home', 'repos'))
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


def _source_stat(cfg, entry):
    fd = _open_root(os.path.join(cfg['source'], entry['rel']))
    try:
        current = os.fstat(fd)
        if not stat.S_ISDIR(current.st_mode) or ('ino' in entry and current.st_ino != entry['ino']):
            raise SourceChanged('source directory path changed since manifest')
        return current
    finally:
        os.close(fd)


def _mkdir(cfg, entry, stop):
    if stop.is_set():
        return {'rel': entry['rel'], 'status': 'stopped', 'operation': 'mkdir'}
    try:
        _source_stat(cfg, entry)
        ensure_destination_directory(cfg['destination'], entry['rel'])
        return {'rel': entry['rel'], 'status': 'created_checked', 'operation': 'mkdir'}
    except Exception as exc:
        return {'rel': entry['rel'], 'status': 'changed' if isinstance(exc, SourceChanged) else 'error',
                'operation': 'mkdir', 'detail': type(exc).__name__ + ': ' + str(exc)}


def _metadata(cfg, entry, stop):
    rel = entry['rel']
    if stop.is_set():
        return {'rel': rel, 'status': 'stopped', 'operation': 'metadata'}
    src = dst = check = None
    try:
        src = _open_root(os.path.join(cfg['source'], rel))
        before = os.fstat(src)
        if not stat.S_ISDIR(before.st_mode) or ('ino' in entry and before.st_ino != entry['ino']):
            raise SourceChanged('source directory path changed since manifest')
        dst = _open_root(os.path.join(cfg['destination'], rel))
        warnings = []
        drift = [k for k in ('mtime_ns', 'ctime_ns', 'mode', 'uid', 'gid')
                 if k in entry and getattr(before, 'st_' + k) != entry[k]]
        if drift:
            warnings.append('source directory metadata changed since manifest: ' + ','.join(drift))
        if stop.is_set():
            return {'rel': rel, 'status': 'stopped', 'operation': 'metadata'}
        _metadata_fd(src, dst, before, warnings)
        check = _open_root(os.path.join(cfg['source'], rel))
        if _identity(os.fstat(src)) != _identity(before) or _identity(os.fstat(check)) != _identity(before):
            raise SourceChanged('source directory changed during metadata synchronization')
        current = os.fstat(dst)
        result = {'rel': rel, 'operation': 'metadata', 'status': 'applied',
                  'source_identity': _record(before), 'dst_stat': _record(current)}
        if warnings:
            result['warnings'] = warnings
        return result
    except Exception as exc:
        return {'rel': rel, 'operation': 'metadata',
                'status': 'changed' if isinstance(exc, SourceChanged) else 'error',
                'detail': type(exc).__name__ + ': ' + str(exc)}
    finally:
        for fd in (src, dst, check):
            if fd is not None:
                os.close(fd)


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
    control = pathlib.Path(control)
    cfg = read_json(control / 'config.json')
    if 'source_mount' in cfg and 'destination_mount' in cfg:
        from migration_run import validate_mounts
        validate_mounts(cfg)
    threads = max(1, min(32, int(threads)))
    job = control / 'metadata' / label
    job.mkdir(parents=True, exist_ok=True)
    progress_path = job / 'progress.json'
    final_path = pathlib.Path(result_path) if result_path else job / 'result.json'
    stop = threading.Event()
    finished = threading.Event()
    mutex = threading.Lock()
    summary = {'job': label, 'host': __import__('socket').gethostname(), 'pid': os.getpid(),
               'started': now(), 'updated': now(), 'status': 'reading_manifest', 'threads': threads,
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
            if status == 'created_checked':
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
            if result['operation'] == 'mkdir' and status not in ('created_checked', 'stopped'):
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
        _parallel(selected, lambda entry: _mkdir(cfg, entry, stop), threads, stop, receive)
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
            _parallel(depths[depth], lambda entry: _metadata(cfg, entry, stop), threads, stop, receive)
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
    manifest = pathlib.Path(manifest)
    return _run(control, lambda stop: _directories(manifest, stop), manifest.stem,
                threads, defer_roots, result_path)


def finalize_shared_ancestors(control, manifests, threads=32, result_path=None):
    control = pathlib.Path(control)
    def load(stop):
        result = {}
        for manifest in manifests:
            if stop.is_set():
                break
            manifest = pathlib.Path(manifest)
            try:
                previous = read_json(control / 'metadata' / manifest.stem / 'result.json')
                deferred = previous['deferred_entries']
            except (OSError, KeyError, ValueError):
                deferred = list(_directories(manifest, stop, only_shared=True).values())
            result.update((entry['rel'], entry) for entry in deferred)
        return result
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
