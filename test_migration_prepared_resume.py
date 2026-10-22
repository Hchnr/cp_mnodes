import hashlib
import io
import json
import os
from pathlib import Path
import pickle
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock
import uuid

import migration_prepared_resume as prepared
from migration_io import write_json
from migration_scan import _stat_fields
from deploy_interleaved_producer import reconcile_delta, cancelled_entries
from deploy_pipelined_producer import AssignmentLedger


class PreparedTests(unittest.TestCase):
    def test_stop_recovery_only_retries_cancelled_completed_entries(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            c=Path(tmp);db=sqlite3.connect(':memory:')
            self.addCleanup(db.close)
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,status TEXT,payload TEXT)')
            entries=[{'rel':name,'kind':'file','size':size} for name,size in [('good',1),('paused',2),('cancelled',3)]]
            db.execute('INSERT INTO claims VALUES(?,?,?,?)',('p3-r3',10,'done',json.dumps({'entries':entries})))
            write_json(c/'stages/p3-r3/done/10/10.json',{'results':[
                {'rel':'good','status':'copied'},{'rel':'paused','status':'paused'},
                {'rel':'cancelled','status':'error','cancelled':True}]})
            self.assertEqual([e['rel'] for e in cancelled_entries(c,db,10)],['cancelled','paused'])
            db.execute('INSERT INTO claims VALUES(?,?,?,?)',('p3-r3',11,'leased',json.dumps({'entries':[entries[1]]})))
            with self.assertRaisesRegex(RuntimeError,'live reservation'):
                cancelled_entries(c,db,10)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root/'source'
        self.source.mkdir()
        self.control = self.root/'control'
        self.control.mkdir()
        self.bundle = self.control/'bundle'
        self.bundle.mkdir()
        self.run_id = 'prepared-fixture-'+uuid.uuid4().hex

    def test_unsorted_complete_assignments_prepare_and_load_without_reissuing(self):
        entries=[]
        for name in ('a','b','orphan'):
            p=self.source/name;p.write_bytes(name.encode())
            entries.append({'rel':name,'kind':'file',**_stat_fields(p.stat())})
        ledger=self.control/'ledger.jsonl'
        ledger.write_text(''.join(json.dumps({'id':i,'phase':3,'round':3,'op':'sync','entries':[entries[i]]})+'\n' for i in (1,0)))
        manifest=self.control/'manifest.jsonl'
        manifest.write_text(''.join(json.dumps(e)+'\n' for e in entries))
        handover={'assigned_tasks_path':str(ledger),'ledger_sha256':prepared.digest_file(ledger),'next_task_id':2}
        handover_path=self.control/'handover.json';write_json(handover_path,handover)
        write_json(self.control/'config.json',{'run_id':self.run_id,'source':str(self.source)})
        write_json(self.control/'state.json',{'recovery_warm':True,'stage':'p3-r3',
                   'recovery_replay':{'phase':'fresh_scan'},'foreground_handover':str(handover_path),
                   'active_manifest':str(manifest)})
        scratch=Path('/tmp')/(self.run_id+'-'+self.bundle.name)
        self.addCleanup(lambda:shutil.rmtree(scratch,ignore_errors=True))
        previous=os.umask(0o077)
        try:
            with mock.patch('builtins.print'):
                meta=prepared.prepare(self.control,self.bundle)
        finally:
            os.umask(previous)
            prepared.gc.enable()
        self.assertEqual(meta['orphans'],1)
        self.assertEqual(meta['base_next_task_id'],2)
        self.assertEqual(Path(meta['manifest']).read_bytes(),manifest.read_bytes())
        new=dict(handover,prepared_resume=str(self.bundle/'prepared.json'),predecessor_handover=str(handover_path))
        with mock.patch.object(prepared.gc,'freeze') as freeze:
            _,values=prepared.load_checkpoint(new,self.control)
            freeze.assert_called_once()
        self.assertEqual(values['assigned'],{'a','b'})
        self.assertEqual(values['seeded_paths'],{'a','b','orphan'})
        with open(meta['cache'],'ab') as handle:handle.write(b'tamper')
        with self.assertRaisesRegex(ValueError,'digest'):
            prepared.load_checkpoint(new,self.control)

    def test_checkpoint_never_resolves_executable_globals(self):
        payload=pickle.dumps(eval,protocol=5)
        with self.assertRaisesRegex(ValueError,'globals'):
            prepared.ContainerUnpickler(io.BytesIO(payload)).load()

    def test_real_constructor_uses_cached_prefix_and_validates_delta(self):
        from test_migration_overlap_parallel_filter import ParallelFilterTests
        import migration_overlap_parallel_filter as parent
        from migration_interleaved_target_filter import InterleavedTargetFilterResume
        from migration_io import read_json
        fixture=ParallelFilterTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        _,warm,partial,ledger,_=fixture.prepare()
        cfg=read_json(fixture.control/'config.json')
        cfg.update(target_probe_endpoints=['http://127.0.0.1:1'],broker_token='fixture-token',distributed_target_threads=2)
        write_json(fixture.control/'config.json',cfg)
        result=fixture.control/'seed-result.json'
        write_json(result,{'ok':True,'summary':{},'merge':{}})
        combined=fixture.control/'prepared-manifest.jsonl';combined.write_text(warm.read_text())
        delta=fixture.control/'delta.jsonl'
        delta.write_text(json.dumps({'id':5,'phase':3,'round':3,'op':'sync','entries':[{'rel':'other/new-reservation','kind':'file'}]})+'\n')
        handover=fixture.control/'prepared-handover.json'
        write_json(handover,{'control':str(fixture.control),'stage':'p3-r3',
            'old_producer':{'pid':987654321,'starttime_ticks':'1','argv':['old']},
            'seed_helper':{'pid':987654322,'starttime_ticks':'1','argv':['seed'],'result_path':str(result)},
            'assigned_tasks_path':str(ledger),'next_task_id':6,'warm_manifest':str(warm),'missing_manifest':str(partial),
            'prepared_resume':'fixture-cache','assignment_delta_path':str(delta),
            'assignment_delta_sha256':prepared.digest_file(delta)})
        meta={'maximum_id':4,'manifest':str(combined)}
        values={'assigned':{'other/already','other/assigned'}}
        original_records=parent._task_records
        def records(path):
            self.assertEqual(Path(path),delta,'constructor reparsed the expensive historical ledger')
            return original_records(path)
        with mock.patch.object(parent,'validate_mounts'),mock.patch.object(parent,'process_matches',return_value=False), \
                mock.patch.object(prepared,'load_checkpoint',return_value=(meta,values)), \
                mock.patch.object(parent,'_task_records',side_effect=records):
            obj=InterleavedTargetFilterResume(fixture.control,handover,publish_workers=2,publish_limit=4)
        self.addCleanup(fixture.close,obj)
        self.addCleanup(obj._task_publisher.close)
        self.addCleanup(lambda:prepared.gc.callbacks.remove(obj._gc_callback))
        self.assertEqual(obj.next_task_id,6)
        self.assertEqual(obj.assigned,{'other/already','other/assigned','other/new-reservation'})
        self.assertEqual(obj.combined_manifest,combined)

    def test_delta_captures_pending_and_claimed_once_with_payload_conflict_detection(self):
        db=sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,payload TEXT)')
        pending=self.control/'stages/p3-r3/pending'
        for i in range(64):(pending/str(i)).mkdir(parents=True)
        def task(i):return {'id':i,'phase':3,'round':3,'op':'sync','entries':[{'rel':str(i),'kind':'file','size':1}]}
        for i in (10,11):db.execute('INSERT INTO claims VALUES(?,?,?)',('p3-r3',i,json.dumps(task(i))))
        for i in (11,12):write_json(pending/str(i%64)/(str(i)+'.json'),task(i))
        ledger=AssignmentLedger(self.control/'delta.jsonl')
        reconcile_delta(self.control,db,ledger,10)
        summary=ledger.close()
        self.assertEqual(summary['tasks'],3)
        self.assertEqual(set(ledger.hashes),{10,11,12})


if __name__=='__main__':unittest.main()
