import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


OPS = Path(__file__).resolve().parents[2] / 'ops/diagnostics'
sys.path.insert(0, str(OPS))
import projset_resumable_snapshot_canary as snapshot
import projset_snapshot_capture_canary as capture
import projset_paced_read as pacing


SCHEMA = '''
create table dirs(id integer primary key, rel text not null unique, state integer not null default 0,
                  node text, attempt integer not null default 0, next real not null default 0,
                  updated real not null default 0);
create index dir_queue on dirs(state,next,id);
create table issues(parent integer not null, rel text not null, issue text not null,
                    attempt integer not null, data text not null, primary key(parent,rel,issue));
create table failures(dir integer primary key, attempts integer not null, detail text not null,
                      last_time real not null, resolved integer not null default 0);
create table meta(key text primary key, value text not null);
create table repair_outbox(id integer primary key autoincrement, rel text not null, data text not null);
create trigger repair_missing_outbox after insert on issues when new.issue='missing_target'
begin insert into repair_outbox(rel,data) values(new.rel,new.data); end;
'''


class Stopped(Exception):
    pass


class ResumableSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix=capture.MARKER + '-resume-')
        self.root = Path(self.tmp.name)
        self.source = self.root / capture.LEGACY
        self.dest = self.root / capture.SNAPSHOT
        db = sqlite3.connect(self.source)
        db.executescript(SCHEMA)
        with db:
            db.execute('insert into meta values(?,?)', ('stats', '{"initial":true}'))
            db.executemany('insert into dirs(rel) values(?)',
                           [(json.dumps('dir-%04d' % number),) for number in range(80)])
            db.executemany('insert into issues values(?,?,?,?,?)',
                           [(1, json.dumps('issue-%04d' % number), 'different_size', 1,
                             json.dumps({'rel': 'issue-%04d' % number})) for number in range(20)])
            db.execute('insert into failures values(1,2,?,123.25,0)', ('retry',))
        db.close()
        self.identity = capture.install(self.source)

    def tearDown(self):
        self.tmp.cleanup()

    def source_change(self, number):
        db = sqlite3.connect(self.source, timeout=3)
        try:
            rel = 'new-%d-\udcff' % number
            with db:
                db.execute('update dirs set updated=?, state=? where id=?',
                           (1794550000.1234567 + number, number % 3, 2 + number))
                db.execute('insert into dirs(rel) values(?)', (json.dumps(rel),))
                db.execute('insert into issues values(?,?,?,?,?)',
                           (1, json.dumps(rel + '/missing'), 'missing_target', 1,
                            json.dumps({'rel': rel + '/missing', 'issue': 'missing_target'})))
                db.execute('update meta set value=? where key=?', (json.dumps({'change': number}), 'stats'))
        finally:
            db.close()

    def dump(self, path):
        db = sqlite3.connect(path)
        try:
            order = {'dirs': 'id', 'issues': 'parent,rel,issue', 'failures': 'dir',
                     'meta': 'key', 'repair_outbox': 'id', 'handoff_changes': 'seq',
                     'handoff_meta': 'id', 'sqlite_sequence': 'name'}
            return {table: db.execute('select * from ' + table + ' order by ' + key).fetchall()
                    for table, key in order.items()}
        finally:
            db.close()

    def test_resume_preserves_exact_boundary_with_concurrent_changes(self):
        counts = {'copy': 0}
        def stop_during_copy(stage, detail):
            if stage == 'copy_committed':
                counts['copy'] += 1
                if counts['copy'] == 3:
                    raise Stopped('copy interrupted after commit')
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop_during_copy)
        self.source_change(1)

        expected = []
        def stop_after_boundary(stage, detail):
            if stage == 'before_boundary':
                self.source_change(2)
                expected.append(self.dump(self.source))
            elif stage == 'boundary_committed':
                self.source_change(3)
                raise Stopped('boundary committed')
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop_after_boundary)

        def stop_during_replay(stage, detail):
            if stage == 'replay_before_commit':
                raise Stopped('replay transaction interrupted')
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop_during_replay)

        def stop_during_finalize(stage, detail):
            if stage == 'finalize_before_commit':
                raise Stopped('finalization transaction interrupted')
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop_during_finalize)

        def stop_after_finalize(stage, detail):
            if stage == 'finalize_committed':
                raise Stopped('finalization committed')
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop_after_finalize)

        result = snapshot.snapshot(self.source, self.dest, page_size=4)
        self.assertTrue(result['already_completed'])
        self.assertEqual(result['capture_id'], self.identity)
        self.assertEqual(self.dump(self.dest), expected[0])
        self.assertNotEqual(self.dump(self.source), expected[0])
        db = sqlite3.connect(self.dest)
        try:
            self.assertEqual(db.execute('pragma integrity_check').fetchone()[0], 'ok')
            self.assertEqual(db.execute('pragma journal_mode').fetchone()[0].lower(), 'delete')
        finally:
            db.close()

    def test_abrupt_process_exits_at_each_transaction_boundary_recover(self):
        program = '''import os,sys
sys.path.insert(0,sys.argv[1])
import projset_resumable_snapshot_canary as snapshot
def stop(stage,detail):
    if stage == sys.argv[4]: os._exit(86)
snapshot.snapshot(sys.argv[2],sys.argv[3],page_size=4,hook=stop)
'''
        result = subprocess.run([sys.executable, '-S', '-B', '-c', program, str(OPS),
                                 str(self.source), str(self.dest), 'copy_before_commit'], timeout=20)
        self.assertEqual(result.returncode, 86)
        self.source_change(1)
        expected = self.dump(self.source)
        for stage in ('replay_before_commit', 'finalize_before_commit'):
            result = subprocess.run([sys.executable, '-S', '-B', '-c', program, str(OPS),
                                     str(self.source), str(self.dest), stage], timeout=20)
            self.assertEqual(result.returncode, 86, stage)
        result = snapshot.snapshot(self.source, self.dest, page_size=4)
        self.assertFalse(result['already_completed'])
        self.assertEqual(self.dump(self.dest), expected)

    def test_damaged_progress_cannot_skip_unscanned_tables(self):
        def stop(stage, detail):
            if stage == 'copy_committed':
                raise Stopped()
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop)
        db = sqlite3.connect(self.dest)
        try:
            raw = db.execute('select progress from snapshot_resume where id=1').fetchone()[0]
            state = json.loads(raw)
            state['table_index'] = 999
            db.execute('update snapshot_resume set progress=? where id=1', (json.dumps(state),))
            db.commit()
        finally:
            db.close()
        with self.assertRaises(ValueError):
            snapshot.snapshot(self.source, self.dest, page_size=4)

    def test_snapshot_with_real_read_pacing_and_advice_matches_source(self):
        self.source_change(1)
        expected = self.dump(self.source)
        pacer = pacing.ReadPacer(64 * 1024 * 1024, max_batch=8 * 1024 * 1024)
        result = snapshot.snapshot(self.source, self.dest, page_size=16, pacer=pacer,
                                   advise=pacing.disable_sqlite_readahead)
        self.assertEqual(result['capture_id'], self.identity)
        self.assertEqual(self.dump(self.dest), expected)

    def test_changed_source_schema_fails_closed(self):
        def stop(stage, detail):
            if stage == 'copy_committed':
                raise Stopped()
        with self.assertRaises(Stopped):
            snapshot.snapshot(self.source, self.dest, page_size=4, hook=stop)
        db = sqlite3.connect(self.source)
        try:
            db.execute('drop trigger handoff_capture_dirs_update'); db.commit()
        finally:
            db.close()
        with self.assertRaises(ValueError):
            snapshot.snapshot(self.source, self.dest, page_size=4)

    def test_refuses_concurrent_snapshot_writer_and_non_canary_source(self):
        lock = self.dest.with_name(self.dest.name + '.resume.lock')
        with lock.open('a+b') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                snapshot.snapshot(self.source, self.dest)
        with self.assertRaises(ValueError):
            snapshot.snapshot('/tmp/not-a-canary/legacy-canary.sqlite3', self.dest)


if __name__ == '__main__':
    unittest.main()
