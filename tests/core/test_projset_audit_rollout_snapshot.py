import contextlib
import fcntl
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    'audit_rollout_snapshot', ROOT / 'ops/deployment/projset_audit_rollout_snapshot.py')
snapshot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snapshot)


class AuditRolloutSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.run = self.root / 'run'
        self.work = self.run / 'recovery/audit'
        self.stage = self.work / 'upgrade-test'
        self.stage.mkdir(parents=True)
        (self.work / 'repair').mkdir()
        self.local = self.root / 'local-snapshot'
        (self.run / 'recovery/ssh.json').write_text('{}')
        (self.run / 'config.json').write_text('{}')
        (self.work / 'STOP').write_text(self.stage.name + '\n')
        (self.work / 'repair/status.json').write_text(json.dumps({'outbox_cursor': 2}))
        (self.stage / 'audit-before.json').write_text(json.dumps(
            {'done_dirs': 1, 'counts': {'files': 6, 'symlinks': 1, 'file_bytes': 900, 'entries': 8}}))
        stats = {'discovered_dirs': 4, 'done_dirs': 2,
                 'counts': {'files': 7, 'symlinks': 1, 'file_bytes': 1000, 'entries': 9}}
        db = sqlite3.connect(self.work / 'audit.sqlite3')
        db.executescript('''create table dirs(id integer primary key,state integer);
          create table meta(key text primary key,value text not null);
          create table repair_outbox(id integer primary key,rel text not null,data text not null);''')
        db.executemany('insert into dirs values(?,?)', [(1, 2), (2, 2), (3, 1), (4, 0)])
        db.execute('insert into meta values(?,?)', ('stats', json.dumps(stats)))
        db.executemany('insert into repair_outbox values(?,?,?)', [(1, 'a', '{}'), (2, 'b', '{}')])
        db.commit(); db.close()
        self.patches = mock.patch.multiple(snapshot, STAGE=self.stage, WORK=self.work, RUN=self.run,
                                           FENCE=self.stage.name, LOCAL=self.local, RATE=10**12)
        self.patches.start()
        self.identity = mock.patch.object(snapshot, 'validate_node')
        self.identity.start()

    def tearDown(self):
        self.identity.stop()
        self.patches.stop()
        self.tmp.cleanup()

    def test_snapshot_keeps_committed_stats_active_leases_and_outbox(self):
        with contextlib.redirect_stdout(io.StringIO()):
            snapshot.main()
        result = json.loads((self.stage / 'snapshot-ready.json').read_text())
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(result['directory_states'], {'0': 1, '1': 1, '2': 2})
        self.assertEqual((result['outbox_count'], result['outbox_max'], result['repair_cursor']), (2, 2, 2))
        with sqlite3.connect(self.local / 'audit.sqlite3') as db:
            self.assertEqual(db.execute('pragma quick_check').fetchone(), ('ok',))
            self.assertEqual(db.execute('select count(*) from dirs').fetchone()[0], 4)

    def test_snapshot_refuses_an_active_controller_or_missing_fence(self):
        with (self.work / 'controller.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                snapshot.main()
        self.assertFalse(self.local.exists())
        (self.work / 'STOP').unlink()
        with self.assertRaises(FileNotFoundError):
            snapshot.main()
        self.assertFalse(self.local.exists())

    def test_snapshot_rejects_repair_cursor_ahead_of_outbox(self):
        (self.work / 'repair/status.json').write_text(json.dumps({'outbox_cursor': 3}))
        with self.assertRaisesRegex(RuntimeError, 'repair cursor'):
            snapshot.main()
        self.assertFalse((self.stage / 'snapshot-ready.json').exists())
        with sqlite3.connect(self.work / 'audit.sqlite3') as db:
            self.assertEqual(db.execute('select count(*) from repair_outbox').fetchone()[0], 2)


if __name__ == '__main__':
    unittest.main()
