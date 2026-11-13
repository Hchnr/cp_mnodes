"""Recovery tests operate on private trees and processes, never the live migration."""
import importlib.util
import json
import os
import random
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO/'src/core'))

def load(name):
    source=REPO/'ops/copy'/(name+'.py')
    if not source.is_file():source=Path(__file__).with_name(name+'.py')
    spec=importlib.util.spec_from_file_location(name,source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

copy=load('projset_recovery_copy')
supervisor=load('projset_recovery_supervisor')


def write(path, rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))


class RecoveryTests(unittest.TestCase):
    def test_replay_preserves_holes_partial_receipts_and_discovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            run=Path(tmp)/'run';(run/'recovery').mkdir(parents=True)
            write(run/'tasks.jsonl',[dict(id=1,type='scan',rel=''),dict(id=2,type='copy',entries=[dict(rel='a')]),dict(id=3,type='scan',rel='child')])
            write(run/'completed.jsonl',[dict(id=3)]) # Completion order is not task order.
            (run/'recovery/original-counts.json').write_text(json.dumps({'node':{'copied':1,'copied_bytes':9}}))
            (run/'status.json').write_text(json.dumps({'state':'copying','updated':0,'nodes_ready':1,'active':1,
                'rates':{'300':{'seconds':10,'write_Bps':1,'copied_Bps':2,'files_ps':3}},
                'workers':{'node':{'ready':True,'alive':True,'age':2,'active':{'1':'scan'}}}}))
            write(run/'nodes/node/receipts.jsonl',[dict(job=2,rel='a',status='copied',logical_bytes=9)])
            write(run/'recovery/tasks.001.jsonl',[dict(id=4,type='copy',entries=[dict(rel='b')])])
            with (run/'recovery/tasks.001.jsonl').open('a') as f:f.write('{"id":5')
            write(run/'nodes/node/attempt-001/receipts.jsonl',[dict(job=4,rel='b',status='copied',logical_bytes=7)])
            (run/'recovery/code').mkdir()
            (run/'recovery/code/projset_parallel_receipts.py').write_text(
                'def rebuild_receipts(run, db, pending, original, report, ssh):\n'
                '    db.execute("insert into prior values (\'bad\', 999, \'{}\')")\n'
                '    raise RuntimeError("canary failure")\n')
            for fast, parallel in (('0', '0'), ('1', '0'), ('1', '1')):
                with patch.dict(os.environ, {'PROJSET_FAST_REBUILD': fast, 'PROJSET_PARALLEL_RECEIPTS': parallel}):
                    db=copy.rebuild(run,str(Path(tmp)/'cache.sqlite'))
                    self.assertEqual([1,2,4],[r[0] for r in db.execute('select id from jobs order by id')])
                    self.assertEqual({'a','b'},{r[0] for r in db.execute('select rel from files')})
                    self.assertEqual(2,db.execute('select count(*) from prior').fetchone()[0])
                    self.assertEqual(16,copy.read(run/'recovery/baseline.json')['node']['copied_bytes'])
                    status=copy.read(run/'status.json')
                    self.assertEqual(status['state'],'recovering')
                    self.assertEqual(status['nodes_ready'],0)
                    self.assertFalse(status['workers']['node']['ready'])
                    self.assertEqual(status['rates']['300']['copied_Bps'],0)
                    db.close()

    def test_completed_first_rejects_unknown_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            run=Path(tmp)/'run';(run/'recovery').mkdir(parents=True)
            write(run/'tasks.jsonl',[dict(id=1,type='scan',rel='')])
            write(run/'completed.jsonl',[dict(id=2)])
            with patch.dict(os.environ, {'PROJSET_FAST_REBUILD': '1'}):
                with self.assertRaisesRegex(ValueError, 'completion references missing task'):
                    copy.rebuild(run,str(Path(tmp)/'cache.sqlite'))

    def test_completed_first_matches_serial_on_shuffled_and_duplicate_records(self):
        for seed in range(8):
            rng=random.Random(seed)
            with tempfile.TemporaryDirectory() as tmp:
                run=Path(tmp)/'run';(run/'recovery').mkdir(parents=True)
                tasks=[]
                for key in range(1,201):
                    if rng.randrange(3)==0:
                        tasks.append(dict(id=key,type='scan',rel='dir-'+str(rng.randrange(100))))
                    else:
                        tasks.append(dict(id=key,type='copy',entries=[dict(rel='file-'+str(rng.randrange(100))) for _ in range(rng.randrange(1,4))]))
                completed=[dict(id=t['id']) for t in tasks if rng.randrange(3)]
                completed.extend(completed[:5]);rng.shuffle(completed)
                write(run/'tasks.jsonl',tasks);write(run/'completed.jsonl',completed)
                original={'node-a':{'processed':1},'node-b':{'processed':2}}
                (run/'recovery/original-counts.json').write_text(json.dumps(original))
                for node in original:
                    write(run/'nodes'/node/'receipts.jsonl',[])
                    rows=[dict(job=rng.randrange(1,201),rel='file-'+str(rng.randrange(100)),
                               status=rng.choice(['copied','skipped','error']),logical_bytes=rng.randrange(20))
                          for _ in range(100)]
                    write(run/'nodes'/node/'attempt-a'/'receipts.jsonl',rows)
                snapshots=[]
                for mode in ('0','1'):
                    with patch.dict(os.environ,{'PROJSET_FAST_REBUILD':mode,'PROJSET_PARALLEL_RECEIPTS':'0'}):
                        db=copy.rebuild(run,str(Path(tmp)/('cache-'+mode+'.sqlite')))
                        snapshots.append(([db.execute('select * from '+name+' order by 1').fetchall()
                                           for name in ('jobs','all_ids','scanned','files','prior')],
                                          copy.read(run/'recovery/baseline.json')))
                        db.close()
                self.assertEqual(snapshots[0],snapshots[1])

    def test_resume_rejects_changed_destination(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'source';target=Path(tmp)/'target';source.mkdir();target.mkdir()
            (source/'a').write_bytes(b'original');(target/'a').write_bytes(b'original')
            before_source,before_target=copy.SOURCE,copy.TARGET
            try:
                copy.SOURCE,copy.TARGET=str(source),str(target)
                prior=dict(status='copied',source_identity=copy.engine._record((source/'a').stat()),dst_stat=copy.engine._record((target/'a').stat()))
                self.assertTrue(copy.resume_valid({'rel':'a'},prior))
                (target/'a').write_bytes(b'changed')
                self.assertFalse(copy.resume_valid({'rel':'a'},prior))
                self.assertFalse(copy.resume_valid({'rel':'a'},dict(status='error',cancelled=True)))
            finally:copy.SOURCE,copy.TARGET=before_source,before_target

    def test_fence_checks_start_time_before_signalling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);folder=root/'nodes/node';folder.mkdir(parents=True)
            script=root/'worker.py';script.write_text('import time; time.sleep(60)\n')
            child=subprocess.Popen([sys.executable,'-S',str(script),'worker',str(root),'node'])
            try:
                start=Path('/proc/%d/stat'%child.pid).read_text().rsplit(') ',1)[1].split()[19]
                info=dict(pid=child.pid,start='wrong',boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),argv=[str(script)])
                (folder/'launch.json').write_text(json.dumps(info))
                subprocess.run([sys.executable,'-S','-c',supervisor.FENCE,str(root),'node'],check=True,capture_output=True)
                self.assertIsNone(child.poll())
                info['start']=start;(folder/'launch.json').write_text(json.dumps(info))
                subprocess.run([sys.executable,'-S','-c',supervisor.FENCE,str(root),'node'],check=True,capture_output=True)
                self.assertIsNotNone(child.poll())
            finally:
                if child.poll() is None:child.terminate()
                child.wait()

if __name__=='__main__':unittest.main()
