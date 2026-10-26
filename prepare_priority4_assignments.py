#!/usr/bin/env python3
"""Read-only, contiguous P4 broker payload cache; never signals any actor."""
import argparse
from contextlib import closing
import gc
import hashlib
import json
import os
from pathlib import Path
import pickle
import sqlite3
import tempfile
import time

from migration_io import read_json, write_json
from migration_priority4_worker import ScopeGuard
from migration_scan_boundary import require_identity


def task_digest(task, guard, expected_id, round_no=1):
    if (type(task) is not dict or task.get('id') != expected_id
            or type(task.get('id')) is not int
            or task.get('round') != round_no or task.get('phase') != 4
            or task.get('op') != ('copy' if round_no == 1 else 'sync')
            or type(task.get('entries')) is not list or not task['entries']):
        raise ValueError('P4 contiguous task payload differs')
    for entry in task['entries']:
        guard.assert_task(task, entry)
        if entry.get('hardlink_to'):
            raise ValueError('P4 hardlink aliases cannot be payload assignments')
    return hashlib.sha256(json.dumps(task, ensure_ascii=True, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode('ascii')).digest()


def consume_prefix(rows, guard, *, limit, report=None):
    """A gap ends the cached prefix; a conflict is never ignored."""
    paths, hashes, hardlinks = set(), [], {}
    for task_id, payload in rows:
        if task_id >= limit or task_id != len(hashes):
            break
        task = json.loads(payload)
        digest = task_digest(task, guard, task_id)
        for entry in task['entries']:
            rel = entry['rel']
            if rel in paths:
                raise ValueError('duplicate P4 assignment path')
            paths.add(rel)
            if entry['kind'] == 'file' and entry.get('nlink', 1) > 1:
                key = (entry['dev'], entry['ino'])
                if key in hardlinks and hardlinks[key] != rel:
                    raise ValueError('P4 canonical hardlink assignment conflicts')
                hardlinks[key] = rel
        hashes.append(digest)
        if report and len(hashes) % 2048 == 0:
            report(tasks=len(hashes), paths=len(paths))
    return dict(paths=paths, hashes=hashes, hardlinks=hardlinks)


def consume_sqlite_prefix(db, guard, *, limit, report=None):
    """JSON1 extracts only required fields, not 20M complete Python dicts.

    The read transaction is unchanged. Validate every root, ID, entry count,
    path, kind and canonical, and retain raw broker payload digests. The full
    immutable original payloads remain in the broker and shared archives.
    """
    db.create_function('payload_sha256', 1, lambda value: hashlib.sha256(value.encode()).digest())
    hashes, counts = [], []
    for row in db.execute('SELECT id,json_type(payload,\'$.id\'),json_extract(payload,\'$.id\'),'
            "json_extract(payload,'$.phase'),json_extract(payload,'$.round'),"
            "json_extract(payload,'$.op'),json_type(payload,'$.entries'),"
            "json_array_length(payload,'$.entries'),payload_sha256(payload) "
            'FROM claims WHERE stage=? AND id<? ORDER BY id', ('p4-r1', limit)):
        number, id_type, task_id, phase, round_no, op, entries_type, count, digest = row
        if number != len(hashes):
            break
        if ((id_type, task_id, phase, round_no, op, entries_type) !=
                ('integer', number, 4, 1, 'copy', 'array') or type(count) is not int or count <= 0):
            raise ValueError('P4 SQL task root identity or entries differ')
        hashes.append(digest)
        counts.append(count)
        if report and len(hashes) % 4096 == 0:
            report(status='validating_task_roots', tasks=len(hashes), paths=0)
    paths, hardlinks, observed, current, current_count = set(), {}, 0, 0, 0
    query = ("SELECT c.id,e.type,json_extract(e.value,'$.rel'),json_extract(e.value,'$.kind'),"
             "json_extract(e.value,'$.hardlink_to'),CASE WHEN json_extract(e.value,'$.kind')='file' "
             "AND json_extract(e.value,'$.nlink')>1 THEN e.value ELSE NULL END "
             "FROM claims c,json_each(c.payload,'$.entries') e "
             'WHERE c.stage=? AND c.id<? ORDER BY c.id')
    plan = db.execute('EXPLAIN QUERY PLAN ' + query, ('p4-r1', len(hashes))).fetchall()
    if any('USE TEMP B-TREE' in str(row) for row in plan):
        raise RuntimeError('P4 assignment preparation would perform an unbounded global sort')
    for number, entry_type, rel, kind, alias, linked in db.execute(query, ('p4-r1', len(hashes))):
        if number != current:
            if number != current + 1 or current_count != counts[current]:
                raise ValueError('P4 SQL task entry count/order differs')
            current, current_count = number, 0
        if entry_type != 'object' or kind not in ('file', 'symlink') or alias:
            raise ValueError('P4 SQL task entry kind/canonical differs')
        guard.assert_path(rel)
        if rel in paths:
            raise ValueError('duplicate P4 assignment path')
        paths.add(rel)
        current_count += 1
        observed += 1
        if linked is not None:
            entry = json.loads(linked)
            key = entry['dev'], entry['ino']
            if key in hardlinks and hardlinks[key] != rel:
                raise ValueError('P4 regular-file canonical hardlink assignment conflicts')
            hardlinks[key] = rel
        if report and observed % 131072 == 0:
            report(status='reading_assignment_paths', tasks=len(hashes), paths=len(paths))
    if hashes and (current != len(hashes) - 1 or current_count != counts[-1] or observed != sum(counts)):
        raise ValueError('P4 SQL assignment prefix is incomplete')
    return dict(paths=paths, hashes=hashes, hardlinks=hardlinks)


def digest_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    directory = parser.parse_args().directory.resolve()
    intent = read_json(directory / 'intent.json')
    control = Path(intent['control'])
    if (directory.parent != control or not directory.name.startswith('assignment-preparation-')
            or control.parent != Path('/share-mixed/project') or control.is_symlink()):
        raise ValueError('dedicated P4 assignment preparation required')
    cfg = read_json(control / 'config.json')
    guard = ScopeGuard(str(control), cfg)
    if cfg['control'] != str(control):
        raise ValueError('P4 control binding differs')
    producer = intent['producer']
    if producer['argv'] != ['/usr/bin/python3', '-u', str(control / 'migration_priority4_run.py'),
                           '--control', str(control)]:
        raise ValueError('exact original P4 producer required')
    root = control / 'source-sha256.json'
    if digest_file(root) != intent['runtime_manifest_sha256']:
        raise ValueError('original frozen runtime binding changed')
    for name, digest in read_json(root).items():
        path = control / name
        if path.parent != control or path.is_symlink() or digest_file(path) != digest:
            raise ValueError('original P4 runtime source changed')
    def validate():
        require_identity(producer)
        state = read_json(control / 'state.json')
        if (state.get('stage') != 'p4-r1' or state.get('status') != 'scanning'
                or (control / 'STOP').exists()):
            raise RuntimeError('original P4 scanning stage changed')
        return state
    state = validate()
    floor = state['next_task_id']
    if type(floor) is not int or floor <= 0:
        raise ValueError('published assignment prefix required')
    broker = Path('/tmp/' + cfg['run_id'] + '-broker.sqlite')
    if not broker.is_file() or broker.is_symlink():
        raise ValueError('existing P4 broker database required')
    info = broker.stat()
    record = dict(status='reading_assignment_prefix', started=time.time(), control=str(control),
                  producer=producer, floor=floor, authorizes_handover=False, process_signals_sent=0)
    last = 0
    def report(**updates):
        nonlocal last
        if time.monotonic() - last > 2:
            validate()
            record.update(updates, updated=time.time())
            write_json(directory / 'progress.json', record)
            last = time.monotonic()
    enabled = gc.isenabled()
    gc.disable()
    try:
        with closing(sqlite3.connect(broker.as_uri() + '?mode=ro', uri=True, timeout=10)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            values = consume_sqlite_prefix(db, guard, limit=floor, report=report)
        validate()
        if not values['hashes']:
            raise ValueError('empty contiguous assignment prefix')
        private = Path(tempfile.mkdtemp(prefix=cfg['run_id'] + '-p4-assignments-'))
        cache = private / 'assignments.pickle'
        gc.collect()
        gc.freeze()
        with open(cache, 'xb', buffering=8 * 1024**2) as stream:
            os.chmod(cache, 0o600)
            pickle.dump(values, stream, protocol=pickle.HIGHEST_PROTOCOL)
            stream.flush()
            os.fsync(stream.fileno())
        meta = dict(status='prepared_assignment_prefix_only', scope='priority4-assignment-prefix-v1',
            control=str(control), run_id=cfg['run_id'], job=cfg['job'], producer=producer,
            broker=str(broker), broker_dev=info.st_dev, broker_ino=info.st_ino,
            cache=str(cache), cache_sha256=digest_file(cache), cache_bytes=cache.stat().st_size,
            payload_digest_kind='sha256-raw-broker-json',
            tasks=len(values['hashes']), assigned_paths=len(values['paths']),
            runtime_manifest_sha256=intent['runtime_manifest_sha256'],
            authorizes_handover=False, process_signals_sent=0, finished=time.time())
        write_json(directory / 'preparation.json', meta)
        record.update(status=meta['status'], tasks=meta['tasks'], paths=meta['assigned_paths'],
                      preparation=str(directory / 'preparation.json'), finished=time.time())
        write_json(directory / 'progress.json', record)
        print(json.dumps(meta), flush=True)
    except BaseException as exc:
        record.update(status='preparation_failed_original_untouched', error=repr(exc), finished=time.time())
        write_json(directory / 'progress.json', record)
        raise
    finally:
        if enabled:
            gc.enable()


if __name__ == '__main__':
    main()
