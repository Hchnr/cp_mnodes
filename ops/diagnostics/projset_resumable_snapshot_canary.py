#!/usr/bin/env python3
"""Resumable fuzzy snapshot prototype restricted to synthetic queues.

The source captures mutations in its own transactions. Each copied page and
its resume position commit together to the local test snapshot; replay also
commits its cursor with the applied changes. This is not a production handoff,
distributed fence, or a guarantee of bounded kernel/NFS I/O.
"""
import fcntl
import json
import os
from pathlib import Path
import sqlite3

import projset_snapshot_capture_canary as capture


KEYS = {'dirs': ('id',), 'issues': ('parent', 'rel', 'issue'),
        'failures': ('dir',), 'meta': ('key',), 'repair_outbox': ('id',)}
SYSTEM = {'handoff_meta', 'handoff_changes'}
OPTIONAL = {'handoff_fence'}
SESSION = 'snapshot_resume'


def _encoded(value):
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'))


def _local_destination_parent(path):
    """Reject shared or memory-backed destination mounts for this experiment."""
    parent = Path(path).resolve().parent
    if not any(parent == root or root in parent.parents for root in (Path('/tmp'), Path('/var/tmp'))):
        raise ValueError('synthetic snapshot destination must be in local temporary storage')
    mounts = []
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left, sep, right = line.partition(' - ')
        if not sep:
            continue
        fields, details = left.split(), right.split()
        if len(fields) < 5 or not details:
            continue
        mount = fields[4].replace('\\040', ' ').replace('\\011', '\t').replace('\\012', '\n').replace('\\134', '\\')
        root = Path(mount)
        if parent == root or root in parent.parents:
            mounts.append((len(str(root)), details[0]))
    if not mounts or max(mounts)[1] not in {'overlay', 'ext4', 'xfs', 'btrfs', 'f2fs'}:
        raise ValueError('synthetic snapshot destination is not on an allowed local filesystem')


def _decode(table, raw):
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != set(capture.TABLES[table]):
        raise ValueError('invalid captured row')
    for column in capture.REAL_COLUMNS.get(table, ()):
        if value[column] is not None:
            value[column] = float(value[column])
    return value


def _apply(db, table, operation, before, after):
    if table not in KEYS or operation not in ('insert', 'update', 'delete'):
        raise ValueError('unexpected captured mutation')
    before, after = _decode(table, before), _decode(table, after)
    keys = KEYS[table]
    if operation == 'update':
        if before is None or after is None or any(before[k] != after[k] for k in keys):
            raise ValueError('captured key change')
        if table == 'dirs' and before['rel'] != after['rel']:
            raise ValueError('directory identity changed')
    if operation == 'delete':
        if before is None or after is not None:
            raise ValueError('invalid delete')
        db.execute('delete from ' + table + ' where ' + ' and '.join(k + '=?' for k in keys),
                   tuple(before[k] for k in keys))
        return
    if after is None or (operation == 'insert' and before is not None):
        raise ValueError('invalid insertion')
    columns = capture.TABLES[table]
    updates = [column + '=excluded.' + column for column in columns if column not in keys]
    sql = ('insert into ' + table + '(' + ','.join(columns) + ') values(' +
           ','.join('?' for _ in columns) + ') on conflict(' + ','.join(keys) + ') ' +
           ('do update set ' + ','.join(updates) if updates else 'do nothing'))
    db.execute(sql, tuple(after[column] for column in columns))


def _inspect(source):
    """Read schema and starting boundary within one short source transaction."""
    source.execute('begin')
    try:
        identity = capture.verify(source)
        version = source.execute('pragma schema_version').fetchone()[0]
        sequence = source.execute('select coalesce(max(seq),0) from handoff_changes').fetchone()[0]
        objects = source.execute("select type,name,tbl_name,sql from sqlite_master "
                                 "where sql is not null and name not like 'sqlite_%' order by rowid").fetchall()
        tables = {name for kind, name, _, _ in objects if kind == 'table'}
        if not set(KEYS) | SYSTEM <= tables or tables - (set(KEYS) | SYSTEM | OPTIONAL):
            raise ValueError('unexpected source tables')
        for table, expected in capture.TABLES.items():
            actual = tuple(row[1] for row in source.execute('pragma table_info(' + table + ')'))
            if actual != expected:
                raise ValueError('unexpected source columns')
        triggers = {name for kind, name, _, _ in objects if kind == 'trigger'}
        required = set(capture.trigger_statements()) | {'repair_missing_outbox'}
        if not required <= triggers or triggers - (required | {'handoff_fence_ack'}):
            raise ValueError('unexpected source triggers')
        source.execute('commit')
        return {'capture_id': identity, 'schema_version': version, 'base_seq': sequence,
                'objects': objects, 'has_fence': 'handoff_fence' in tables}
    except BaseException:
        source.execute('rollback')
        raise


def _check_source(source, static):
    source.execute('begin')
    try:
        if (capture.verify(source) != static['capture_id'] or
                source.execute('pragma schema_version').fetchone()[0] != static['schema_version']):
            raise ValueError('source capture identity or schema changed')
        if static['base_seq'] and source.execute('select 1 from handoff_changes where seq=?',
                                                  (static['base_seq'],)).fetchone() is None:
            raise ValueError('initial capture boundary disappeared')
        source.execute('commit')
    except BaseException:
        source.execute('rollback')
        raise


def _record(target, state):
    target.execute('update ' + SESSION + ' set progress=? where id=1', (_encoded(state),))


def _metadata(source, static):
    source.execute('begin')
    try:
        if (capture.verify(source) != static['capture_id'] or
                source.execute('pragma schema_version').fetchone()[0] != static['schema_version']):
            raise ValueError('source capture identity or schema changed')
        sequence = source.execute('select coalesce(max(seq),0) from handoff_changes').fetchone()[0]
        metadata = source.execute('select id,capture_id from handoff_meta').fetchall()
        fence = source.execute('select * from handoff_fence').fetchall() if static['has_fence'] else []
        sequences = source.execute('select name,seq from sqlite_sequence').fetchall()
        source.execute('commit')
    except BaseException:
        source.execute('rollback')
        raise
    if sequence < static['base_seq'] or metadata != [(1, static['capture_id'])] or len(fence) > 1:
        raise ValueError('invalid source capture boundary')
    return {'seq': sequence, 'metadata': metadata, 'fence': fence, 'sequences': sequences}


def _validate_progress(static, state):
    def nonnegative(value):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if (not isinstance(state, dict) or state.get('stage') not in ('copy', 'replay') or
            not nonnegative(static.get('base_seq')) or not nonnegative(state.get('table_index')) or
            state['table_index'] > len(KEYS) or not nonnegative(state.get('cursor')) or
            not isinstance(state.get('copied'), dict) or set(state['copied']) != set(KEYS) or
            not all(nonnegative(value) for value in state['copied'].values())):
        raise ValueError('invalid snapshot progress')
    last = state.get('last_key')
    if state['table_index'] == len(KEYS):
        if last is not None:
            raise ValueError('invalid completed copy key')
    elif last is not None:
        if not isinstance(last, list) or len(last) != len(KEYS[list(KEYS)[state['table_index']]]):
            raise ValueError('invalid snapshot resume key')
    if state['stage'] == 'copy':
        if state['cursor'] != 0 or 'boundary' in state:
            raise ValueError('invalid copy-stage checkpoint')
    else:
        boundary = state.get('boundary')
        if (state['table_index'] != len(KEYS) or not isinstance(boundary, dict) or
                not nonnegative(boundary.get('seq')) or boundary['seq'] < static['base_seq'] or
                state['cursor'] > boundary['seq'] or
                not all(isinstance(boundary.get(key), list) for key in ('metadata', 'fence', 'sequences'))):
            raise ValueError('invalid replay-stage checkpoint')


def snapshot(source_path, destination_path, page_size=32, hook=None, pacer=None, advise=None):
    """Continue a bounded synthetic snapshot, or create it on the first call.

    `hook` may stop between committed pages. A resumed call revalidates source
    identity, the capture schema, and the existing progress before continuing.
    A caller-provided pacer is charged after bounded operations; it must not
    sleep while this function holds a source transaction.
    """
    if Path(source_path).is_symlink() or Path(destination_path).is_symlink():
        raise ValueError('synthetic snapshot paths must not be symlinks')
    source_path = capture.guard(source_path, capture.LEGACY)
    destination_path = capture.guard(destination_path, capture.SNAPSHOT)
    if (not isinstance(page_size, int) or isinstance(page_size, bool) or
            not 1 <= page_size <= 256 or source_path == destination_path):
        raise ValueError('invalid synthetic snapshot settings')
    _local_destination_parent(destination_path)
    if source_path.is_symlink() or destination_path.is_symlink():
        raise ValueError('synthetic snapshot paths must not be symlinks')
    source_stat = source_path.stat()
    source_identity = [source_stat.st_dev, source_stat.st_ino]

    def charge(operation):
        if pacer is not None:
            try:
                pacer.throttle()
            except ValueError as error:
                raise ValueError(operation + ': ' + str(error)) from error

    def hit(stage, detail=None):
        if hook is not None:
            hook(stage, detail or {})

    def check_integrity(target):
        # integrity_check scans the local destination. Charge at SQLite's
        # progress callbacks while holding no source transaction; a single
        # SQLite operation can still overshoot before the next callback.
        # SQLite callbacks cannot propagate exceptions directly, so interrupt
        # the query and restore the original guard error outside the callback.
        interrupted = []

        def progress():
            try:
                charge('check local snapshot integrity')
                return 0
            except BaseException as error:
                interrupted.append(error)
                return 1

        hit('before_integrity_check')
        if pacer is not None:
            target.set_progress_handler(progress, 128)
        try:
            try:
                result = target.execute('pragma integrity_check').fetchone()
            except sqlite3.OperationalError:
                if interrupted:
                    raise interrupted[0]
                raise
        finally:
            if pacer is not None:
                target.set_progress_handler(None, 0)
        if result is None or result[0] != 'ok':
            raise ValueError('snapshot integrity failed')
        charge('finish local snapshot integrity check')

    lock_path = destination_path.with_name(destination_path.name + '.resume.lock')
    with lock_path.open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source = sqlite3.connect('file:' + str(source_path) + '?mode=ro', uri=True,
                                 timeout=3, isolation_level=None)
        target = None
        try:
            if advise is not None:
                advise(source_path)
            opened_stat = source_path.stat()
            if [opened_stat.st_dev, opened_stat.st_ino] != source_identity:
                raise ValueError('synthetic source changed during open')
            source.execute('pragma query_only=on')
            source.execute('pragma mmap_size=0')
            source.execute('pragma cache_size=-256')
            observed = _inspect(source)
            charge('inspect source')
            if not destination_path.exists():
                with destination_path.open('xb'):
                    pass
                target = sqlite3.connect(str(destination_path), timeout=3)
                target.execute('pragma journal_mode=wal')
                target.execute('pragma synchronous=full')
                # Keep the local WAL small between committed pages. This is
                # best effort: SQLite may still exceed a batch in one call.
                target.execute('pragma wal_autocheckpoint=32')
                static = dict(observed, source_identity=source_identity, version=1)
                state = {'stage': 'copy', 'table_index': 0, 'last_key': None,
                         'copied': {name: 0 for name in KEYS}, 'cursor': 0}
                with target:
                    target.execute('begin immediate')
                    for kind in ('table', 'index'):
                        for obj_kind, _, _, sql in static['objects']:
                            if obj_kind == kind:
                                target.execute(sql)
                    target.execute('create table ' + SESSION +
                                   '(id integer primary key check(id=1),static text not null,progress text not null)')
                    target.execute('insert into ' + SESSION + ' values(1,?,?)', (_encoded(static), _encoded(state)))
                hit('initialized', {'base_seq': static['base_seq']})
            else:
                dest_stat = destination_path.stat()
                if (dest_stat.st_dev, dest_stat.st_ino) == tuple(source_identity):
                    raise ValueError('synthetic source and destination are the same file')
                target = sqlite3.connect(str(destination_path), timeout=3)
                target.execute('pragma synchronous=full')
                target.execute('pragma wal_autocheckpoint=32')
                exists = target.execute("select 1 from sqlite_master where type='table' and name=?", (SESSION,)).fetchone()
                if not exists:
                    if capture.verify(target) != observed['capture_id']:
                        raise ValueError('completed snapshot belongs to another source')
                    check_integrity(target)
                    seq = target.execute('select coalesce(max(seq),0) from handoff_changes').fetchone()[0]
                    if seq > observed['base_seq']:
                        raise ValueError('completed snapshot boundary is ahead of source')
                    target.execute('pragma wal_checkpoint(truncate)')
                    charge('checkpoint completed local snapshot')
                    target.execute('pragma journal_mode=delete')
                    charge('finish completed snapshot validation')
                    return {'capture_id': observed['capture_id'], 'seq': seq, 'already_completed': True}
                row = target.execute('select static,progress from ' + SESSION + ' where id=1').fetchone()
                if row is None:
                    raise ValueError('snapshot checkpoint missing')
                static, state = json.loads(row[0]), json.loads(row[1])
                if not isinstance(static, dict):
                    raise ValueError('invalid snapshot source checkpoint')
                if (static.get('version') != 1 or static.get('source_identity') != source_identity or
                        static.get('capture_id') != observed['capture_id'] or
                        static.get('schema_version') != observed['schema_version'] or
                        static.get('objects') != [list(x) for x in observed['objects']]):
                    raise ValueError('snapshot checkpoint source identity or schema changed')
                _validate_progress(static, state)
                _check_source(source, static)
                charge('validate resumed snapshot')
                hit('resumed', {'stage': state['stage'], 'cursor': state['cursor']})

            tables = list(KEYS)
            if state['stage'] == 'copy':
                while state['table_index'] < len(tables):
                    table = tables[state['table_index']]
                    keys, columns = KEYS[table], capture.TABLES[table]
                    last = state['last_key']
                    where = ''
                    if last is not None:
                        where = (' where ' + keys[0] + ' > ?' if len(keys) == 1 else
                                 ' where (' + ','.join(keys) + ') > (' + ','.join('?' for _ in keys) + ')')
                    rows = source.execute('select ' + ','.join(columns) + ' from ' + table + where +
                                          ' order by ' + ','.join(keys) + ' limit ?',
                                          (() if last is None else tuple(last)) + (page_size,)).fetchall()
                    charge('copy ' + table)
                    if rows:
                        sql = ('insert into ' + table + '(' + ','.join(columns) + ') values(' +
                               ','.join('?' for _ in columns) + ')')
                        state['copied'][table] += len(rows)
                        state['last_key'] = [rows[-1][columns.index(key)] for key in keys]
                        with target:
                            target.executemany(sql, rows)
                            _record(target, state)
                            hit('copy_before_commit', {'table': table, 'rows': len(rows)})
                        hit('copy_committed', {'table': table, 'rows': len(rows),
                                               'last_key': state['last_key'], 'copied': dict(state['copied'])})
                    else:
                        state['table_index'] += 1
                        state['last_key'] = None
                        with target:
                            _record(target, state)
                        hit('table_completed', {'table': table})
                hit('before_boundary', {'copied': dict(state['copied'])})
                boundary = _metadata(source, static)
                charge('capture boundary')
                state.update(stage='replay', boundary=boundary, cursor=0)
                with target:
                    _record(target, state)
                hit('boundary_committed', {'base_seq': static['base_seq'], 'seq': boundary['seq']})

            if state['stage'] == 'replay':
                sequence = state['boundary']['seq']
                while state['cursor'] < sequence:
                    changes = source.execute('select seq,kind,operation,before_json,after_json from handoff_changes '
                                             'where seq>? and seq<=? order by seq limit ?',
                                             (state['cursor'], sequence, page_size)).fetchall()
                    charge('replay changes')
                    if not changes:
                        raise ValueError('capture log has a gap')
                    with target:
                        for seq, table, operation, before, after in changes:
                            if seq != state['cursor'] + 1:
                                raise ValueError('capture log sequence is not contiguous')
                            target.execute('insert into handoff_changes values(?,?,?,?,?)',
                                           (seq, table, operation, before, after))
                            if seq > static['base_seq']:
                                _apply(target, table, operation, before, after)
                            state['cursor'] = seq
                        _record(target, state)
                        hit('replay_before_commit', {'cursor': state['cursor'], 'seq': sequence})
                    hit('replay_committed', {'cursor': state['cursor'], 'seq': sequence})
                _check_source(source, static)
                charge('validate source before finalize')
                hit('before_finalize', {'seq': sequence})
                current_stat = source_path.stat()
                if [current_stat.st_dev, current_stat.st_ino] != source_identity:
                    raise ValueError('synthetic source changed before finalization')
                boundary = state['boundary']
                with target:
                    target.executemany('insert into handoff_meta values(?,?)', boundary['metadata'])
                    if static['has_fence'] and boundary['fence']:
                        count = len(boundary['fence'][0])
                        target.executemany('insert into handoff_fence values(' + ','.join('?' for _ in range(count)) + ')',
                                           boundary['fence'])
                    target.execute('delete from sqlite_sequence')
                    target.executemany('insert into sqlite_sequence values(?,?)', boundary['sequences'])
                    for kind, _, _, sql in static['objects']:
                        if kind == 'trigger':
                            target.execute(sql)
                    target.execute('drop table ' + SESSION)
                    hit('finalize_before_commit', {'seq': sequence})
                hit('finalize_committed', {'seq': sequence})
                if capture.verify(target) != static['capture_id']:
                    raise ValueError('snapshot capture identity differs')
                check_integrity(target)
                if target.execute('select coalesce(max(seq),0) from handoff_changes').fetchone()[0] != sequence:
                    raise ValueError('snapshot capture boundary differs')
                target.execute('pragma wal_checkpoint(truncate)')
                charge('checkpoint local snapshot')
                target.execute('pragma journal_mode=delete')
                charge('finish snapshot validation')
                return {'capture_id': static['capture_id'], 'base_seq': static['base_seq'],
                        'seq': sequence, 'copied': state['copied'], 'already_completed': False}
            raise ValueError('invalid snapshot checkpoint stage')
        finally:
            if target is not None:
                target.close()
            source.close()
