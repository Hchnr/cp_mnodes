"""UNDEPLOYED P4 index merge primitive; no paths, launcher or live connection.

The caller supplies two dedicated local SQLite connections, the exact ScopeGuard
and a callable which raises on STOP/configuration change. It must separately
bind the connections to the intended local files and hold the coordinator lock.
This module cannot establish those runtime conditions or authorize launch.

Only INSERT OR IGNORE reaches the live files table. Existing rows, including
their ownership flags, are never updated. Each bounded batch is committed
separately so a fresh-result writer can progress between batches. A failed
batch rolls back; already committed batches are retained for idempotent retry.
There is no payload access, receipt reread, filesystem publication or CLI.
"""
import json
import re
import sqlite3
import stat

from migration_priority4_worker import ScopeGuard


FILES_COLUMNS = [('rel', 'TEXT', 1), ('source', 'TEXT', 0),
                 ('destination', 'TEXT', 0), ('owned', 'INTEGER', 0),
                 ('sha256', 'TEXT', 0)]
SCOPE_COLUMNS = [('version', 'INTEGER', 1), ('kind', 'TEXT', 0),
                 ('binding', 'TEXT', 0), ('issues', 'INTEGER', 0),
                 ('metadata_warnings', 'INTEGER', 0)]
IDENTITY_KEYS = ('dev', 'ino', 'size', 'mode', 'uid', 'gid', 'mtime_ns', 'ctime_ns')
FILES_SQL = 'CREATE TABLE files (rel TEXT PRIMARY KEY, source TEXT, destination TEXT, owned INTEGER, sha256 TEXT)'
SCOPE_SQL = ('CREATE TABLE p4_index_scope (version INTEGER PRIMARY KEY, kind TEXT, '
             'binding TEXT, issues INTEGER, metadata_warnings INTEGER)')


def _binding(guard):
    if not isinstance(guard, ScopeGuard):
        raise ValueError('an exact P4 ScopeGuard is required')
    return json.dumps({'control': guard.control, 'run_id': guard.run_id,
                       'source': '/share/project', 'destination': '/share-mixed/project',
                       'scope': 'project_priority4', 'exclude': ['eai_pwm'],
                       'covered_by': list(guard.covered_by),
                       'protected': sorted(guard.protected)}, sort_keys=True)


def _schema(db, sealed):
    if db.execute('PRAGMA writable_schema').fetchone() != (0,):
        raise ValueError('writable_schema is not permitted on a merge connection')
    databases = {row[1] for row in db.execute('PRAGMA database_list')}
    if databases - {'main', 'temp'} or db.execute('SELECT 1 FROM sqlite_temp_master LIMIT 1').fetchone():
        raise ValueError('attached databases or temporary schema objects are not permitted')
    # Only the files TEXT PRIMARY KEY creates an internal index. A prefix
    # exemption would also hide a trigger renamed with writable_schema while
    # SQLite still has its executable trigger cached on this connection.
    objects = db.execute('SELECT type,name,tbl_name FROM sqlite_master').fetchall()
    expected = {'files', 'p4_index_scope'} if sealed else {'files'}
    allowed = {('table', name, name) for name in expected}
    allowed.add(('index', 'sqlite_autoindex_files_1', 'files'))
    if len(objects) != len(allowed) or set(objects) != allowed:
        raise ValueError('unexpected index schema, trigger, view or explicit index')
    if db.execute('SELECT sql FROM sqlite_master WHERE name=?',
                  ('sqlite_autoindex_files_1',)).fetchall() != [(None,)]:
        raise ValueError('unexpected files primary-key index definition')
    for table, columns in [('files', FILES_COLUMNS)] + ([('p4_index_scope', SCOPE_COLUMNS)] if sealed else []):
        sql = db.execute('SELECT sql FROM sqlite_master WHERE type=? AND name=?', ('table', table)).fetchone()[0]
        expected_sql = FILES_SQL if table == 'files' else SCOPE_SQL
        if re.sub(r'\s+', '', sql).lower() != re.sub(r'\s+', '', expected_sql).lower():
            raise ValueError('unexpected index table constraints or definition')
        observed = db.execute('PRAGMA table_xinfo(' + table + ')').fetchall()
        if ([(r[1], r[2].upper(), r[5]) for r in observed] != columns
                or any(r[6] != 0 for r in observed)
                or db.execute('PRAGMA foreign_key_list(' + table + ')').fetchall()):
            raise ValueError('unexpected index columns or constraints')


def _idle(db):
    if not isinstance(db, sqlite3.Connection) or db.in_transaction:
        raise ValueError('dedicated SQLite connection with no caller transaction required')


def _current(check):
    # Match the existing raising STOP-check convention. A boolean-returning
    # checker must not accidentally turn False into permission to keep writing.
    if check() is not None:
        raise ValueError('current STOP/scope checker must raise on failure and return None on success')


def _stamp(db, guard, kind, issues, warnings):
    _idle(db)
    for value in (issues, warnings):
        if type(value) is not int or value < 0:
            raise ValueError('explicit nonnegative candidate issue counters required')
    binding = _binding(guard)
    db.execute('BEGIN IMMEDIATE')
    try:
        _schema(db, False)
        if kind == 'live_index' and db.execute('SELECT 1 FROM files LIMIT 1').fetchone():
            raise ValueError('an existing populated index cannot acquire P4 scope by adoption')
        db.execute(SCOPE_SQL)
        db.execute('INSERT INTO p4_index_scope VALUES (1,?,?,?,?)', (kind, binding, issues, warnings))
        db.commit()
    except BaseException:
        db.rollback()
        raise


def initialize_empty_live_index(db, guard):
    """Bind a newly created empty files table; never adopt a populated index."""
    _stamp(db, guard, 'live_index', 0, 0)


def seal_receipt_candidate(db, guard, *, issues, metadata_warnings):
    """Called by the scoped receipt builder after its independent validation.

    The scope marker records provenance, not a new payload or ownership proof.
    Nonzero issue counts remain visible and prevent this candidate from merging.
    """
    _stamp(db, guard, 'receipt_candidate', issues, metadata_warnings)


def _check_stamp(db, guard, kind):
    _schema(db, True)
    rows = db.execute('SELECT version,kind,binding,issues,metadata_warnings FROM p4_index_scope').fetchall()
    if rows != [(1, kind, _binding(guard), 0, 0)]:
        raise ValueError('P4 index scope/provenance mismatch or unresolved candidate issue')


def _row(row, guard):
    if len(row) != 5:
        raise ValueError('invalid receipt-index row')
    rel, source, destination, owned, digest = row
    guard.assert_path(rel)
    if type(owned) is not int or owned not in (0, 1):
        raise ValueError('invalid ownership flag')
    try:
        src, dst = json.loads(source), json.loads(destination)
    except (TypeError, ValueError) as exc:
        raise ValueError('invalid identity JSON') from exc
    for identity in (src, dst):
        if (not isinstance(identity, dict)
                or any(type(identity.get(key)) is not int for key in IDENTITY_KEYS)
                or any(identity[key] < 0 for key in ('dev', 'size', 'uid', 'gid'))
                or identity['ino'] <= 0):
            raise ValueError('incomplete receipt identity')
    if stat.S_ISREG(src['mode']) and stat.S_ISREG(dst['mode']):
        if (src['size'] != dst['size'] or not isinstance(digest, str)
                or not re.fullmatch('[0-9a-f]{64}', digest)):
            raise ValueError('invalid regular-file checksum/type/size proof')
    elif stat.S_ISLNK(src['mode']) and stat.S_ISLNK(dst['mode']):
        if digest is not None:
            raise ValueError('invalid symlink index proof')
    else:
        raise ValueError('unsupported or mismatched receipt types')


def merge_candidate(candidate, live, guard, *, check_current, batch_size=1000, progress=None):
    """Merge sealed same-run P4 records without altering any existing row.

    check_current must raise on STOP or changed scope/configuration. Checks run
    before any transaction and again before each batch commit. Runtime file
    identity, exclusive coordinator ownership and complete dependency acceptance
    remain the eventual caller's responsibility; launch_ready stays false.
    """
    if (type(batch_size) is not int or not 1 <= batch_size <= 10000
            or not callable(check_current)):
        raise ValueError('bounded batch size and a current STOP/scope checker required')
    _idle(candidate)
    _idle(live)
    if candidate is live:
        raise ValueError('candidate and live connections must be independent')
    paths = [dict((r[1], r[2]) for r in db.execute('PRAGMA database_list')).get('main')
             for db in (candidate, live)]
    if paths[0] and paths[0] == paths[1]:
        raise ValueError('candidate and live SQLite path must differ')
    _current(check_current)
    report = {'rows_processed': 0, 'rows_inserted': 0, 'rows_ignored': 0,
              'committed_batches': 0, 'existing_rows_updated': 0, 'launch_ready': False}
    candidate.execute('BEGIN')
    try:
        _check_stamp(candidate, guard, 'receipt_candidate')
        _check_stamp(live, guard, 'live_index')
        cursor = candidate.execute('SELECT rel,source,destination,owned,sha256 FROM files ORDER BY rel')
        while True:
            _current(check_current)
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                _row(row, guard)
            live.execute('BEGIN IMMEDIATE')
            try:
                _check_stamp(live, guard, 'live_index')
                _current(check_current)
                before = live.total_changes
                live.executemany('INSERT OR IGNORE INTO files (rel,source,destination,owned,sha256) '
                                 'VALUES (?,?,?,?,?)', rows)
                inserted = live.total_changes - before
                _current(check_current)
                _check_stamp(live, guard, 'live_index')
                live.commit()
            except BaseException:
                live.rollback()
                raise
            report['rows_processed'] += len(rows)
            report['rows_inserted'] += inserted
            report['rows_ignored'] += len(rows) - inserted
            report['committed_batches'] += 1
            if progress is not None:
                progress(dict(report))
        return report
    finally:
        # Release this function's read snapshot; no candidate mutation occurs.
        candidate.rollback()
