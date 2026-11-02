import pathlib
import contextlib
import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import time
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'src/audit'))
import migration_global_balance as balance
import migration_global_finish as finish


class BalanceCoverage(unittest.TestCase):
    def test_missing_or_older_live_report_retains_whole_last_observation(self):
        manifest=dict(baseline=dict(counts=dict(entries=5),groups={},started=1),
            plans=[dict(slot=0,node='worker-0',report='/control/child',units=1,log_file='/control/log')])
        row=dict(pid=12,status='scanning',source='/share/project',target='/share-mixed/project',
            excluded=balance.scan.EXCLUDED,counts=dict(entries=100,files=90),groups={},updated=10,
            pending_directories=2,active_directories=1,payload_bytes_read=0,payload_bytes_written=0,copy_tasks_published=0)
        cache={};records={0:dict(pid=12)}
        with patch.object(balance.scan,'read',return_value=row),patch.object(balance.scan,'returned',return_value=False):
            value,_=balance.report(pathlib.Path('/control'),manifest,records,cache)
        self.assertEqual(value['counts']['entries'],105)
        with patch.object(balance.scan,'read',side_effect=FileNotFoundError),patch.object(balance.scan,'returned',return_value=False):
            value,_=balance.report(pathlib.Path('/control'),manifest,records,cache)
        self.assertEqual(value['counts']['entries'],105)
        self.assertTrue(value['physical_actors'][0]['observation_reused'])
        self.assertEqual(value['physical_actors'][0]['updated'],10)
        with patch.object(balance.scan,'read',return_value=dict(row,counts=dict(entries=20),updated=9)),patch.object(balance.scan,'returned',return_value=False):
            value,_=balance.report(pathlib.Path('/control'),manifest,records,cache)
        self.assertEqual(value['counts']['entries'],105)
        with patch.object(balance.scan,'read',return_value=dict(row,status='complete',counts=dict(entries=20),updated=11)):
            with self.assertRaises(ValueError):balance.report(pathlib.Path('/control'),manifest,records,cache)

    def write(self,path,value):
        path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));return path

    def test_pending_ownership(self):
        valid=[dict(rel='a',leaves_only=True),dict(rel='a/b',leaves_only=False),dict(rel='c',leaves_only=False)]
        self.assertEqual(balance.unit_digest(iter(valid)),balance.unit_digest(valid))
        for invalid in ([valid[1],valid[1]],[dict(rel='a',leaves_only=False),valid[1]],
                [dict(rel='eai_pwm/datasets/raw/x',leaves_only=False)]):
            with self.assertRaises(ValueError):balance.unit_digest(invalid)

    def db(self,path,rows):
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE frontier(rel TEXT PRIMARY KEY,leaves_only INTEGER,status TEXT)')
            db.executemany('INSERT INTO frontier VALUES(?,?,?)',rows)
        return path

    def test_child_must_finish_every_delegated_unit(self):
        with tempfile.TemporaryDirectory(prefix='balance-proof-test-') as t:
            root=pathlib.Path(t)
            base=self.db(root/'base.sqlite',[('',1,'done'),('a',0,'pending'),('b',0,'pending')])
            a=self.db(root/'a.sqlite',[('a',0,'done'),('a/sub',0,'done')])
            b=self.db(root/'b.sqlite',[('b',0,'done')])
            with self.assertRaises(ValueError):balance.merge_frontiers(root/'bad.sqlite',base,[a])
            balance.merge_frontiers(root/'good.sqlite',base,[a,b])
            with sqlite3.connect(root/'good.sqlite') as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM frontier WHERE status='done'").fetchone()[0],4)

    def test_unfinished_or_widened_child_rejected(self):
        with tempfile.TemporaryDirectory(prefix='balance-proof-test-') as t:
            root=pathlib.Path(t)
            base=self.db(root/'base.sqlite',[('a',1,'pending')])
            pending=self.db(root/'pending.sqlite',[('a',1,'running')])
            widened=self.db(root/'wide.sqlite',[('a',0,'done')])
            for child,name in [(pending,'pending-out'),(widened,'wide-out')]:
                with self.assertRaises(ValueError):balance.merge_frontiers(root/(name+'.sqlite'),base,[child])

    def test_whole_logical_shard_can_close_after_disjoint_continuations(self):
        with tempfile.TemporaryDirectory(prefix='balance-integration-') as t:
            root=pathlib.Path(t);folder=root/'nodes/master-0/0';base=folder/'report';base.mkdir(parents=True)
            global_plan=dict(source='/share/project',target='/share-mixed/project',exclusions=balance.scan.EXCLUDED,
                coverage_roots=[''],units=[dict(rel='',leaves_only=False)])
            self.write(folder/'plan.json',global_plan)
            baseline=dict(status='stopped',started=time.time(),updated=time.time(),pid=111,source='/share/project',
                target='/share-mixed/project',excluded=balance.scan.EXCLUDED,counts=dict(entries=1,directories=1),groups={},
                pending_directories=2,active_directories=0,payload_bytes_read=0,payload_bytes_written=0,copy_tasks_published=0)
            self.write(base/'result.json',baseline)
            self.db(base/'frontier.sqlite',[('',0,'done'),('a',0,'pending'),('b',0,'pending')])
            for category in balance.CATEGORIES:(base/(category+'.jsonl')).touch()
            directory=root/'balance/master-0-0';plans=[];records=[]
            for slot,rel in enumerate(['a','b']):
                sub=directory/('part-'+str(slot));report=root/'nodes'/('child-'+rel)/'report';report.mkdir(parents=True)
                plan=self.write(sub/'plan.json',dict(global_plan,units=[dict(rel=rel,leaves_only=False)]))
                result=dict(baseline,status='complete',pid=222+slot,counts=dict(entries=1,directories=1),pending_directories=0)
                self.write(report/'result.json',result);self.write(sub/'scan.log',result)
                self.db(report/'frontier.sqlite',[(rel,0,'done')])
                for category in balance.CATEGORIES:(report/(category+'.jsonl')).touch()
                spec=dict(slot=slot,node='worker-'+str(slot),path=str(plan),sha256=balance.digest(plan),units=1,
                    report=str(report),log_file=str(sub/'scan.log'))
                plans.append(spec);records.append(dict(spec,pid=222+slot))
            manifest=dict(owner='master-0-0',node='master-0',lane=0,original=dict(report=str(base)),baseline=baseline,
                parent_plan=str(folder/'plan.json'),parent_plan_sha256=balance.digest(folder/'plan.json'),
                pending_sha256=balance.unit_digest([dict(rel=x,leaves_only=False) for x in ['a','b']]),plans=plans)
            path=self.write(directory/'manifest.json',manifest)
            self.write(directory/'launch-results.json',dict(records=records,errors=[]))
            printed=io.StringIO()
            with contextlib.redirect_stdout(printed):balance.aggregate(root,path)
            (directory/'aggregate/scan.log').write_text(printed.getvalue())
            rec=dict(node='master-0',lane=0,pid=os.getpid(),report=str(directory/'aggregate/report'),
                balance=dict(manifest=str(path),manifest_sha256=balance.digest(path)))
            record,result,proof=finish.validate_shard(root,rec,global_plan)
            self.assertEqual(result['counts']['entries'],3)
            self.assertEqual(proof['closed_directories'],3)
            upgraded=directory/'aggregate-v2/report';printed=io.StringIO()
            with contextlib.redirect_stdout(printed):balance.aggregate(root,path,upgraded)
            upgraded.parent.joinpath('scan.log').write_text(printed.getvalue())
            old=dict(rec);rec=dict(rec,report=str(upgraded),aggregate_upgrade='cached-whole-observation-v2')
            self.write(upgraded.parent/'activation.json',dict(old=old,new=rec))
            _,result,proof=finish.validate_shard(root,rec,global_plan)
            self.assertEqual(result['counts']['entries'],3)
            self.assertEqual(proof['closed_directories'],3)
            self.write(upgraded.parent/'activation.json',dict(old=dict(old,lane=1),new=rec))
            with self.assertRaises(ValueError):finish.validate_shard(root,rec,global_plan)
            self.assertEqual(result['pending_directories'],0)
            # Tampering with either the ownership or the aggregate invalidates closure.
            (pathlib.Path(plans[0]['path'])).write_text('{}')
            with self.assertRaises(ValueError):finish.validate_shard(root,rec,global_plan)


if __name__=='__main__':unittest.main()
