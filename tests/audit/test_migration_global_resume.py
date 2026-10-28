import collections
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/audit'))
import migration_global_check as control


class ResumeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle=tempfile.TemporaryDirectory(prefix='global-resume-bundle-')
        tools=Path(cls.bundle.name)
        shutil.copyfile(control.BASE/'readonly_scan_core.py',tools/'readonly_scan_core.py')
        shutil.copyfile(control.BASE/'cp_mnodes_readonly_scan_20261024.py',tools/'migration_reaudit_scan.py')
        sys.path.insert(0,str(tools))
        import migration_global_resume
        cls.module=migration_global_resume

    @classmethod
    def tearDownClass(cls):cls.bundle.cleanup()

    def fixture(self):
        tmp=tempfile.TemporaryDirectory(prefix='global-resume-fixture-');self.addCleanup(tmp.cleanup)
        root=Path(tmp.name);source=root/'source';target=root/'target';report=root/'report'
        (source/'d').mkdir(parents=True);target.mkdir();report.mkdir()
        (source/'d/first').write_bytes(b'abc');(source/'d/second').write_bytes(b'defgh')
        for name in ('missing','conflicts','metadata','errors','source_links'):
            (report/(name+'.jsonl')).touch()
        with sqlite3.connect(report/'frontier.sqlite') as db:
            db.execute('CREATE TABLE frontier(rel TEXT PRIMARY KEY,leaves_only INTEGER,status TEXT)')
            db.execute('CREATE INDEX frontier_status ON frontier(status)')
            db.executemany('INSERT INTO frontier VALUES(?,?,?)',[('',0,'done'),('d',0,'pending')])
        baseline=dict(excluded=['eai_pwm/datasets/raw'],counts=dict(entries=1,directories=1),
            groups={'':dict(entries=1,directories=1)},started=time.time()-100,pending_directories=1)
        return source,target,report,baseline

    def test_resumes_pending_only_and_keeps_counts(self):
        source,target,report,baseline=self.fixture()
        audit=self.module.ResumeAudit(source,target,report,baseline,threads=2,stat_threads=2)
        result=audit.run()
        self.assertEqual(result['counts']['entries'],4)
        self.assertEqual(result['counts']['missing_files'],2)
        self.assertEqual(result['counts']['missing_logical_bytes'],8)
        self.assertEqual(result['pending_directories'],0)
        self.assertEqual(result['status'],'complete')
        self.assertEqual(result['payload_bytes_written'],0)
        self.assertEqual(len((report/'missing.jsonl').read_text().splitlines()),3)

    def test_duplicate_children_do_not_inflate_pending(self):
        source,target,report,baseline=self.fixture()
        audit=self.module.ResumeAudit(source,target,report,baseline,threads=2,stat_threads=2)
        visited=[]
        def scan(task):
            rel,_=task;visited.append(rel);audit.record(rel,dict(entries=1))
            return [('d/child',False),('d/child',False)] if rel=='d' else []
        audit.scan_directory=scan
        result=audit.run()
        self.assertEqual(collections.Counter(visited),dict(d=1,**{'d/child':1}))
        self.assertEqual(result['counts']['entries'],3)
        self.assertEqual(result['pending_directories'],0)

    def test_controller_never_counts_entire_pending_index(self):
        source,target,report,baseline=self.fixture();queries=[]
        connect=self.module.sqlite3.connect
        def traced(*args,**kwargs):
            db=connect(*args,**kwargs);db.set_trace_callback(queries.append);return db
        audit=self.module.ResumeAudit(source,target,report,baseline,threads=2,stat_threads=2)
        with patch.object(self.module.sqlite3,'connect',side_effect=traced):audit.run()
        self.assertFalse(any('COUNT(' in sql.upper() for sql in queries))


if __name__=='__main__':unittest.main()
