"""Private SQLite fixtures; no remote access or live process signalling."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
import uuid

import prepare_cold_assignments as prepare
from migration_io import read_json


class AssignmentPreparationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.cfg = dict(run_id='assignment-prefix-fixture-'+uuid.uuid4().hex)
        self.broker = Path('/tmp')/(self.cfg['run_id']+'-broker.sqlite')
        self.addCleanup(lambda: self.broker.unlink(missing_ok=True))
        self.db = sqlite3.connect(self.broker)
        self.db.execute('CREATE TABLE claims (id INTEGER PRIMARY KEY,stage TEXT,status TEXT,payload TEXT)')
        self.addCleanup(self.db.close)
        for i in range(3):
            task = dict(id=i, phase=3, round=3, op='sync',
                        entries=[dict(rel='datasets/f'+str(i), kind='file', size=1)])
            self.db.execute('INSERT INTO claims VALUES (?,?,?,?)', (i,'p3-r3','done',json.dumps(task)))
        self.db.commit()
        self.state = dict(next_task_id=3, task_publication=dict(pending=0))
        self.identity = dict(pid=42, start_ticks='123', argv=['fixture'])
        patch = mock.patch.object(prepare, 'validate_runtime',
            return_value=(self.cfg,self.state,self.identity))
        patch.start(); self.addCleanup(patch.stop)
        patch = mock.patch.object(prepare, 'resource_budget')
        patch.start(); self.addCleanup(patch.stop)

    def test_read_only_export_keeps_original_claims_and_builds_digest_bound_assignment_cache(self):
        directory = self.control/('warm-task-checkpoint-'+uuid.uuid4().hex)
        before = self.broker.read_bytes()
        record = prepare.prepare(self.control, directory, {})
        self.assertEqual(record['status'], 'prepared_assignment_prefix_only')
        self.assertFalse(record['authorizes_handover'])
        self.assertEqual(self.broker.read_bytes(), before)
        self.assertEqual(record['assigned_paths'], 3)
        ledger = directory/'assigned-tasks.jsonl'
        self.assertEqual(record['ledger']['sha256'], hashlib.sha256(ledger.read_bytes()).hexdigest())
        descriptor = read_json(directory/'assignment-checkpoint.json')
        cache = Path(descriptor['cache'])
        self.addCleanup(cache.parent.rmdir)
        self.addCleanup(cache.unlink)
        self.assertEqual(descriptor['prefix_tasks'],3)
        self.assertEqual(descriptor['maximum_id'],2)
        self.assertEqual(record['assignment_prefix_sha256'], descriptor['prefix_sha256'])
        self.assertEqual(record['tasks'], 3)

    def test_live_lease_or_gap_does_not_create_a_checkpoint(self):
        for statement in ("UPDATE claims SET status='leased' WHERE id=1",):
            self.db.execute(statement); self.db.commit()
        directory = self.control/('warm-task-checkpoint-'+uuid.uuid4().hex)
        with self.assertRaisesRegex(RuntimeError, 'leases'):
            prepare.prepare(self.control, directory, {})
        self.assertFalse(directory.exists())

    def test_lower_id_hole_is_not_inferred_complete_from_maximum_id(self):
        self.db.execute('DELETE FROM claims WHERE id=1'); self.db.commit()
        directory = self.control/('warm-task-checkpoint-'+uuid.uuid4().hex)
        with self.assertRaisesRegex(RuntimeError, 'gaps'):
            prepare.prepare(self.control, directory, {})
        self.assertFalse(directory.exists())


if __name__ == '__main__':
    unittest.main()
