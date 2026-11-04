"""Recovery tests operate on private trees and processes, never the live migration."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

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
            write(run/'nodes/node/receipts.jsonl',[dict(job=2,rel='a',status='copied',logical_bytes=9)])
            write(run/'recovery/tasks.001.jsonl',[dict(id=4,type='copy',entries=[dict(rel='b')])])
            with (run/'recovery/tasks.001.jsonl').open('a') as f:f.write('{"id":5')
            write(run/'nodes/node/attempt-001/receipts.jsonl',[dict(job=4,rel='b',status='copied',logical_bytes=7)])
            for _ in range(2):
                db=copy.rebuild(run,str(Path(tmp)/'cache.sqlite'))
                self.assertEqual([1,2,4],[r[0] for r in db.execute('select id from jobs order by id')])
                self.assertEqual({'a','b'},{r[0] for r in db.execute('select rel from files')})
                self.assertEqual(2,db.execute('select count(*) from prior').fetchone()[0])
                self.assertEqual(16,copy.read(run/'recovery/baseline.json')['node']['copied_bytes'])
                db.close()

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
