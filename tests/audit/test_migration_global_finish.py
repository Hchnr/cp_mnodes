import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/audit'))
import migration_global_finish as finish


class ClosureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='global-closure-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.folder=self.root/'nodes/master-0/0'
        self.report=self.folder/'report';self.report.mkdir(parents=True)
        self.plan=dict(source='/share/project',target='/share-mixed/project',exclusions=finish.scan.EXCLUDED,
            coverage_roots=[''],units=[dict(rel='',leaves_only=False)])
        (self.folder/'plan.json').write_text(json.dumps(self.plan))
        self.result=dict(source=self.plan['source'],target=self.plan['target'],excluded=self.plan['exclusions'],
            status='complete',pending_directories=0,active_directories=0,updated=1,counts={},pid=123,
            payload_bytes_read=0,payload_bytes_written=0,copy_tasks_published=0)
        (self.report/'result.json').write_text(json.dumps(self.result))
        (self.folder/'scan.log').write_text(json.dumps(self.result)+'\n')
        self.rec=dict(node='master-0',lane=0,report=str(self.report),pid=123)
        with sqlite3.connect(self.report/'frontier.sqlite') as db:
            db.execute('CREATE TABLE frontier(rel TEXT PRIMARY KEY,leaves_only INTEGER,status TEXT)')
            db.execute("INSERT INTO frontier VALUES('',0,'done')")

    def test_closed(self):
        self.assertEqual(finish.validate_shard(self.root,self.rec,self.plan)[2]['closed_directories'],1)

    def test_pending_rejected(self):
        finish.validate_shard(self.root,self.rec,self.plan)
        with sqlite3.connect(self.report/'frontier.sqlite') as db:
            db.execute("UPDATE frontier SET status='pending'")
        with self.assertRaises(ValueError):finish.validate_shard(self.root,self.rec,self.plan)

    def test_unchanged_evidence_reuses_proof(self):
        proof=finish.validate_shard(self.root,self.rec,self.plan)[2]
        with patch.object(finish,'check_frontier',side_effect=AssertionError('duplicate scan')):
            self.assertEqual(finish.validate_shard(self.root,self.rec,self.plan)[2],proof)

    def test_footer_required(self):
        (self.folder/'scan.log').write_text('')
        with self.assertRaises(ValueError):finish.validate_shard(self.root,self.rec,self.plan)

    def test_ownership_change_rejected(self):
        plan=dict(self.plan,units=[dict(rel='unexpected',leaves_only=False)])
        (self.folder/'plan.json').write_text(json.dumps(plan))
        with self.assertRaises(ValueError):finish.validate_shard(self.root,self.rec,self.plan)


if __name__=='__main__':unittest.main()
