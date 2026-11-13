#!/usr/bin/env python3
"""Change capture for isolated handoff tests; rejects production paths."""
from pathlib import Path
import sqlite3
import uuid

MARKER = 'handoff-prototype-20261112T2057Z'
LEGACY = 'legacy-canary.sqlite3'
SNAPSHOT = 'snapshot-canary.sqlite3'
TABLES = {
    'dirs': ('id', 'rel', 'state', 'node', 'attempt', 'next', 'updated'),
    'issues': ('parent', 'rel', 'issue', 'attempt', 'data'),
    'failures': ('dir', 'attempts', 'detail', 'last_time', 'resolved'),
    'meta': ('key', 'value'),
    'repair_outbox': ('id', 'rel', 'data'),
}
REAL_COLUMNS = {'dirs': ('next', 'updated'), 'failures': ('last_time',)}


def guard(path, name):
    path = Path(path).resolve()
    allowed = (name, 'audit.sqlite3') if name == LEGACY else (name,)
    if path.name not in allowed or not any(part.startswith(MARKER) for part in path.parts):
        raise ValueError('refuse non-canary database')
    return path


def trigger_statements():
    result = {}
    for table, columns in TABLES.items():
        def row(prefix):
            def value(column):
                expression = prefix + '.' + column
                if column in REAL_COLUMNS.get(table, ()):
                    return "case when %s is null then null else printf('%%!.26g',%s) end" % (expression, expression)
                return expression
            fields = ','.join("'%s',%s" % (column, value(column)) for column in columns)
            return 'json_object(' + fields + ')'
        for operation, before, after in (
                ('insert', 'null', row('new')),
                ('update', row('old'), row('new')),
                ('delete', row('old'), 'null')):
            name = 'handoff_capture_' + table + '_' + operation
            result[name] = (
                'CREATE TRIGGER ' + name + ' after ' + operation + ' on ' + table +
                " begin insert into handoff_changes(kind,operation,before_json,after_json) values('" +
                table + "','" + operation + "'," + before + ',' + after + '); end')
    for operation in ('update', 'delete'):
        name = 'handoff_changes_guard_' + operation
        result[name] = ('CREATE TRIGGER ' + name + ' before ' + operation + ' on handoff_changes '
                        "begin select raise(abort,'handoff changes are append-only'); end")
    return result


def verify(db):
    row = db.execute('select capture_id from handoff_meta where id=1').fetchone()
    if row is None or not isinstance(row[0], str) or len(row[0]) != 32:
        raise ValueError('capture identity missing')
    for name, expected in trigger_statements().items():
        actual = db.execute("select sql from sqlite_master where type='trigger' and name=?", (name,)).fetchone()
        if not actual or actual[0] != expected:
            raise ValueError('capture trigger missing or changed: ' + name)
    return row[0]


def install(path):
    path = guard(path, LEGACY)
    db = sqlite3.connect(str(path), timeout=3)
    try:
        db.execute('begin immediate')
        installed = db.execute("select 1 from sqlite_master where type='table' and name='handoff_meta'").fetchone()
        if installed:
            identity = verify(db)
        else:
            if db.execute("select 1 from sqlite_master where name like 'handoff_%' limit 1").fetchone():
                raise ValueError('partial or foreign capture schema')
            for table, expected in TABLES.items():
                actual = tuple(row[1] for row in db.execute('pragma table_info(' + table + ')'))
                if actual != expected:
                    raise ValueError('unexpected legacy schema: ' + table)
            db.execute('create table handoff_meta(id integer primary key check(id=1),capture_id text not null)')
            db.execute('create table handoff_changes(seq integer primary key autoincrement,kind text not null,'
                       'operation text not null,before_json text,after_json text)')
            identity = uuid.uuid4().hex
            db.execute('insert into handoff_meta values(1,?)', (identity,))
            for sql in trigger_statements().values():
                db.execute(sql)
            verify(db)
        db.commit()
        return identity
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()
