"""Rebuild a local coordinator index from immutable old copy receipts.

Only the new local SQLite file is written. Source data, destination data, and
the old control directory are never opened for mutation. Current path identity
must still be checked by the fresh scanner/worker before any overwrite.
"""

import datetime
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


def _receipt_paths(directory, on_discover=None):
    """Enumerate canonical receipts without entering version/symlink trees.

    Retain the former full-path lexical order within each ancestor control.
    Only canonical names are collected; read_json still selects their latest
    immutable version when the receipt is consumed.
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
                    continue
    except OSError:
        return []
    paths = []
    for name in sorted(names):
        done = stages / name / 'done'
        try:
            if not stat.S_ISDIR(done.lstat().st_mode):
                continue
        except OSError:
            continue
        for parent, directories, files in os.walk(done, topdown=True, followlinks=False):
            directories[:] = sorted(n for n in directories if n != '.snapshots')
            # glob('**/*.json') also reported a directory whose name ends in
            # .json. Preserve its later non-regular-file validation/issue.
            for filename in sorted(files + [n for n in directories if n.endswith('.json')]):
                if filename.endswith('.json'):
                    path = pathlib.Path(parent) / filename
                    paths.append(path)
                    if on_discover is not None:
                        on_discover(path)
    return sorted(paths)


def build_resume_index(control, db_path, progress_callback=None, progress_interval=5.0):
    """Build the unchanged resume index, optionally reporting cooperative progress.

    Callback payloads are independent dictionaries. They are emitted at phase
    changes, each 100 discovered/processed receipts, or an elapsed interval at
    iteration boundaries. An exception aborts before publication and removes
    the temporary database. A normal return means publication succeeded.
    """
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
    progress = {'status': 'enumerating', 'receipt_paths_discovered': 0,
                'processed_receipts': 0, 'current_control': None, 'current_receipt': None}
    last_report = {'time': started, 'discovered': 0, 'processed': 0}

    def notify(force=False, **updates):
        progress.update(updates)
        if progress_callback is None:
            return
        tick = time.monotonic()
        if (not force and tick - last_report['time'] < progress_interval
                and progress['receipt_paths_discovered'] - last_report['discovered'] < 100
                and progress['processed_receipts'] - last_report['processed'] < 100):
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
    try:
        db.execute('CREATE TABLE files (rel TEXT PRIMARY KEY, source TEXT, destination TEXT, owned INTEGER, sha256 TEXT)')
        db.execute('CREATE TEMP TABLE versions (rel TEXT PRIMARY KEY, event REAL, ordering TEXT, source TEXT, '
                   'destination TEXT, sha TEXT, kind TEXT, target TEXT)')
        db.execute('CREATE TEMP TABLE proofs (rel TEXT, ino TEXT, sha TEXT, kind TEXT, target TEXT, '
                   'PRIMARY KEY(rel,ino,sha,kind,target))')
        def canonical_receipts():
            for directory in reversed(controls):
                notify(force=True, status='enumerating', current_control=str(directory), current_receipt=None)
                paths = _receipt_paths(directory, discovered if progress_callback is not None else None)
                notify(force=True, status='reading_receipts', current_control=str(directory), current_receipt=None)
                yield from paths

        receipt_paths = canonical_receipts()
        for path in receipt_paths:
            progress['current_receipt'] = str(path)
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
                summary['receipts'] += 1
            except (OSError, ValueError, KeyError, TypeError) as exc:
                issue(path, str(exc))
                progress['processed_receipts'] += 1
                notify()
                continue
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


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('control')
    parser.add_argument('db_path')
    args = parser.parse_args()
    print(json.dumps(build_resume_index(args.control, args.db_path), ensure_ascii=True))
