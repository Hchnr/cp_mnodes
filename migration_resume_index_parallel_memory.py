"""Rebuild a local coordinator index from immutable old copy receipts.

Memory TEMP variant: only the new local SQLite file is written. Source data, destination data, and
the old control directory are never opened for mutation. Current path identity
must still be checked by the fresh scanner/worker before any overwrite.
"""

import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
import pathlib
import re
import sqlite3
import stat
import time
import uuid

from migration_io import read_json


GOOD = frozenset(('copied', 'skipped', 'verified', 'linked'))
REQUIRED = ('dev', 'ino', 'size', 'mode', 'uid', 'gid', 'mtime_ns', 'ctime_ns')


def _resume_controls(control):
    """Resolve newest-to-oldest immutable controls; refuse broken chains."""
    controls = []
    seen = set()
    current = pathlib.Path(control).resolve()
    while True:
        if current in seen:
            raise ValueError('resume_from cycle at ' + str(current))
        if len(controls) >= 16:
            raise ValueError('resume_from chain exceeds 16 control directories')
        if not current.is_dir():
            raise NotADirectoryError(str(current))
        seen.add(current)
        controls.append(current)
        config_path = current / 'config.json'
        if not config_path.exists():
            break
        config = read_json(config_path)
        if not isinstance(config, dict):
            raise ValueError('invalid control configuration: ' + str(config_path))
        previous = config.get('resume_from')
        if previous is None or previous == '':
            break
        if not isinstance(previous, str):
            raise ValueError('resume_from must be a path: ' + str(config_path))
        previous = pathlib.Path(previous)
        current = (previous if previous.is_absolute() else current / previous).resolve()
    return controls


def _receipt_paths(directory, on_discover=None, strict=False):
    """Collect canonical receipts without following symlinks or snapshots.

    Match the reference builder's full-path lexical consumption order. Directory
    entries are classified once during enumeration; each receipt is independently
    lstat-validated by its reader before opening any JSON.
    """
    stages = directory / 'stages'
    try:
        if not stat.S_ISDIR(stages.lstat().st_mode):
            return []
        with os.scandir(stages) as entries:
            names = []
            for entry in entries:
                if entry.name == '.snapshots':
                    continue
                try:
                    if entry.is_dir(follow_symlinks=False):
                        names.append(entry.name)
                except OSError:
                    if strict:
                        raise
                    continue
    except OSError as exc:
        if strict and not isinstance(exc, FileNotFoundError):
            raise
        return []
    paths = []
    for name in sorted(names):
        done = stages / name / 'done'
        try:
            if not stat.S_ISDIR(done.lstat().st_mode):
                continue
        except OSError as exc:
            if strict and not isinstance(exc, FileNotFoundError):
                raise
            continue
        pending = [done]
        while pending:
            parent = pending.pop()
            try:
                with os.scandir(parent) as entries:
                    children = sorted(entries, key=lambda entry: entry.name)
            except OSError:
                if strict:
                    raise
                # os.walk's default onerror behavior is to skip this directory.
                continue
            directories = []
            for entry in children:
                if entry.name == '.snapshots':
                    continue
                path = pathlib.Path(entry.path)
                # Include non-regular .json entries for the same explicit issue
                # validation as the reference builder; never traverse symlinks.
                if entry.name.endswith('.json'):
                    paths.append(path)
                    if on_discover is not None:
                        on_discover(path)
                try:
                    if entry.is_dir(follow_symlinks=False):
                        directories.append(path)
                except OSError:
                    if strict:
                        raise
                    continue
            pending.extend(reversed(directories))
    return sorted(paths)


def _read_receipt(path):
    """Perform only independent filesystem reads, leaving SQL to one thread."""
    try:
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError('receipt is not a regular file')
        receipt = read_json(path)
        results = receipt['results']
        if not isinstance(results, list):
            raise ValueError('receipt results is not a list')
        try:
            event = datetime.datetime.fromisoformat(receipt['finished'].replace('Z', '+00:00')).timestamp()
        except (KeyError, ValueError, TypeError):
            event = path.stat().st_mtime
        return path, results, event, None
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return path, None, None, str(exc)


def _ordered_reads(paths, pool, max_inflight):
    """Bound prefetch and yield strictly in input order, independent of timing."""
    pending = deque()
    source = iter(paths)
    for _ in range(max_inflight):
        try:
            path = next(source)
        except StopIteration:
            break
        pending.append(pool.submit(_read_receipt, path))
    while pending:
        future = pending.popleft()
        value = future.result()
        try:
            path = next(source)
        except StopIteration:
            pass
        else:
            pending.append(pool.submit(_read_receipt, path))
        yield value


def build_resume_index(control, db_path, progress_callback=None, progress_interval=5.0, read_workers=64,
                       strict_enumeration=False):
    """Build the reference index with bounded, ordered parallel receipt reads.

    Only lstat/JSON/event-time reads run concurrently. Receipt/result validation,
    proof and version selection, SQLite writes, and publication retain reference
    order and semantics. Non-phase callbacks are time-throttled; an exception
    cancels queued reads, joins every reader, and removes the unpublished DB.
    """
    if type(read_workers) is not int or not 1 <= read_workers <= 128:
        raise ValueError('read_workers must be an integer between 1 and 128')
    if progress_callback is not None and (not isinstance(progress_interval, (int, float))
                                          or not 0 < progress_interval < float('inf')):
        raise ValueError('progress_interval must be positive and finite')
    started = time.monotonic()
    controls = _resume_controls(control)
    target = pathlib.Path(db_path).absolute()
    if target.exists() or target.is_symlink():
        raise FileExistsError(str(target))
    for directory in controls:
        if os.path.commonpath((str(directory), os.path.realpath(target))) == str(directory):
            raise ValueError('new index must be outside every immutable control directory')
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name('.' + target.name + '.' + uuid.uuid4().hex + '.tmp')
    summary = {'receipts': 0, 'results': 0, 'issues': 0, 'issue_examples': [],
               'metadata_warnings': 0, 'verified_files': 0, 'verified_bytes': 0,
               'owned_files': 0, 'verified_symlinks': 0,
               'control_count': len(controls),
               'controls': [str(directory) for directory in reversed(controls)]}
    progress = {'status': 'enumerating', 'read_workers': read_workers,
                'read_max_inflight': read_workers * 2, 'receipt_paths_discovered': 0,
                'processed_receipts': 0, 'current_control': None, 'current_receipt': None}
    last_report = {'time': started, 'discovered': 0, 'processed': 0}

    def notify(force=False, **updates):
        progress.update(updates)
        if progress_callback is None:
            return
        tick = time.monotonic()
        if not force and tick - last_report['time'] < progress_interval:
            return
        payload = copy.deepcopy({**summary, **progress, 'elapsed_seconds': tick - started})
        progress_callback(payload)
        last_report.update(time=tick, discovered=progress['receipt_paths_discovered'],
                           processed=progress['processed_receipts'])

    def discovered(path):
        progress['receipt_paths_discovered'] += 1
        notify(current_receipt=str(path))

    def issue(path, detail, rel=None):
        summary['issues'] += 1
        if len(summary['issue_examples']) < 30:
            summary['issue_examples'].append({'receipt': str(path), 'rel': rel, 'detail': detail})

    db = sqlite3.connect(temporary)
    # Scratch versions/proofs fit within the explicitly checked memory budget.
    # They remain private and unpublished; the final index is still durable.
    db.execute('PRAGMA temp_store=MEMORY')
    db.execute('PRAGMA cache_size=-262144')
    pool = None
    try:
        db.execute('CREATE TABLE files (rel TEXT PRIMARY KEY, source TEXT, destination TEXT, owned INTEGER, sha256 TEXT)')
        db.execute('CREATE TEMP TABLE versions (rel TEXT PRIMARY KEY, event REAL, ordering TEXT, source TEXT, '
                   'destination TEXT, sha TEXT, kind TEXT, target TEXT)')
        db.execute('CREATE TEMP TABLE proofs (rel TEXT, ino TEXT, sha TEXT, kind TEXT, target TEXT, '
                   'PRIMARY KEY(rel,ino,sha,kind,target))')
        pool = ThreadPoolExecutor(max_workers=read_workers, thread_name_prefix='resume-receipt')
        def canonical_receipts():
            for directory in reversed(controls):
                notify(force=True, status='enumerating', current_control=str(directory), current_receipt=None)
                paths = _receipt_paths(directory, discovered if progress_callback is not None else None,
                                       strict=strict_enumeration)
                notify(force=True, status='reading_receipts', current_control=str(directory), current_receipt=None)
                yield from _ordered_reads(paths, pool, read_workers * 2)

        receipt_paths = canonical_receipts()
        for path, results, event, error in receipt_paths:
            progress['current_receipt'] = str(path)
            if error is not None:
                issue(path, error)
                progress['processed_receipts'] += 1
                notify()
                continue
            summary['receipts'] += 1
            for number, result in enumerate(results):
                summary['results'] += 1
                if not isinstance(result, dict):
                    issue(path, 'invalid result object')
                    continue
                if result.get('status') not in GOOD:
                    continue
                if result.get('metadata_warnings'):
                    summary['metadata_warnings'] += 1
                    continue
                rel = result.get('rel')
                try:
                    if (not isinstance(rel, str) or not rel or '\x00' in rel or
                            os.path.isabs(rel) or any(p in ('', '.', '..') for p in rel.split('/'))):
                        raise ValueError('unsafe relative path')
                    source, destination = result['source_identity'], result['dst_stat']
                    if any(not isinstance(rec, dict) or any(not isinstance(rec.get(k), int) for k in REQUIRED)
                           for rec in (source, destination)):
                        raise ValueError('incomplete identity or metadata')
                    kind = result.get('kind', 'file')
                    digest = result.get('sha256')
                    link_target = result.get('target', result.get('link_target', ''))
                    if kind == 'file':
                        if (not stat.S_ISREG(source['mode']) or not stat.S_ISREG(destination['mode']) or
                                source['size'] != destination['size'] or
                                not isinstance(digest, str) or not re.fullmatch(r'[0-9a-fA-F]{64}', digest)):
                            raise ValueError('regular file lacks valid checksum/type/size')
                        digest = digest.lower()
                        link_target = ''
                    elif kind == 'symlink':
                        if (not stat.S_ISLNK(source['mode']) or not stat.S_ISLNK(destination['mode']) or
                                not isinstance(link_target, str)):
                            raise ValueError('invalid symlink proof')
                        digest = ''
                    else:
                        raise ValueError('unsupported verified kind')
                    # A broker replay marker is not independent proof of creation.
                    if result['status'] == 'copied' and not result.get('replay_reused_copy'):
                        db.execute('INSERT OR IGNORE INTO proofs VALUES (?,?,?,?,?)',
                                   (rel, str(destination['ino']), digest, kind, link_target))
                    ordering = str(path) + ':' + str(number).zfill(8)
                    prior = db.execute('SELECT event,ordering FROM versions WHERE rel=?', (rel,)).fetchone()
                    if prior is None or (event, ordering) >= prior:
                        db.execute('INSERT OR REPLACE INTO versions VALUES (?,?,?,?,?,?,?,?)',
                                   (rel, event, ordering, json.dumps(source), json.dumps(destination),
                                    digest, kind, link_target))
                except (ValueError, KeyError, TypeError) as exc:
                    issue(path, str(exc), rel)
            if summary['receipts'] % 100 == 0:
                db.commit()
            progress['processed_receipts'] += 1
            notify()
        notify(force=True, status='finalizing', current_receipt=None)
        for rel, source, destination, digest, kind, link_target in db.execute(
                'SELECT rel,source,destination,sha,kind,target FROM versions'):
            dst = json.loads(destination)
            # Device IDs vary between node mounts. Ownership never transfers to
            # a different inode solely because its bytes happen to match.
            owned = db.execute('SELECT 1 FROM proofs WHERE rel=? AND ino=? AND sha=? AND kind=? AND target=?',
                               (rel, str(dst['ino']), digest, kind, link_target)).fetchone() is not None
            db.execute('INSERT INTO files VALUES (?,?,?,?,?)',
                       (rel, source, destination, int(owned), digest or None))
            if kind == 'file':
                summary['verified_files'] += 1
                summary['verified_bytes'] += json.loads(source)['size']
            else:
                summary['verified_symlinks'] += 1
            summary['owned_files'] += int(owned)
            notify()
        db.commit()
        db.close()
        notify(force=True, status='publishing')
        # Local filesystem hard-link publication refuses an existing target.
        os.link(temporary, target)
        temporary.unlink()
        summary['db_path'] = str(target)
        return summary
    except BaseException:
        db.close()
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        if pool is not None:
            # No reader is allowed to survive a STOP/callback failure. Pending
            # work is cancelled; already-running filesystem reads finish before
            # the builder returns or propagates the exception.
            pool.shutdown(wait=True, cancel_futures=True)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('control')
    parser.add_argument('db_path')
    parser.add_argument('--read-workers', type=int, default=64)
    args = parser.parse_args()
    print(json.dumps(build_resume_index(args.control, args.db_path, read_workers=args.read_workers), ensure_ascii=True))
