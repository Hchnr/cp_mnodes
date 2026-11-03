import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO/'ops/copy/projset_copy.py'
if not SOURCE.is_file():
    SOURCE = Path(__file__).with_name('projset_copy.py')
CODE_DIR = SOURCE.parent
spec = importlib.util.spec_from_file_location('projset_copy', SOURCE)
copy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(copy)


class ProjsetCopyTests(unittest.TestCase):
    def test_fresh_scan_and_verified_no_replace(self):
        with tempfile.TemporaryDirectory() as root:
            src, dst = Path(root)/'source', Path(root)/'target'
            src.mkdir(); dst.mkdir()
            (src/'.hidden').write_bytes(b'payload\0data')
            (src/'conflict').write_text('new')
            (dst/'conflict').write_text('keep')
            (src/'empty').mkdir()
            (src/'link').symlink_to('.hidden')
            (src/'outside').symlink_to('/etc')
            os.mkfifo(src/'fifo')
            work, problems = [], []
            copy.scan_directory(str(src), str(dst), '', lambda d,e:work.append((d,e)), problems.append)
            self.assertTrue(any(x['status']=='unsupported' for x in problems))
            children = [d for ds, _ in work for d in ds]
            self.assertEqual([x['rel'] for x in children], ['empty'])
            entries = [e for _, es in work for e in es]
            results = {e['rel']:copy.engine.copy_entry(str(src),str(dst),e,'test',verify=True) for e in entries}
            self.assertEqual(results['conflict']['status'], 'conflict')
            self.assertEqual((dst/'conflict').read_text(), 'keep')
            self.assertEqual((dst/'.hidden').read_bytes(), b'payload\0data')
            self.assertEqual(os.readlink(dst/'outside'), '/etc')
            self.assertEqual(results['.hidden']['sha256'], copy.hashlib.sha256(b'payload\0data').hexdigest())
            copy.scan_directory(str(src),str(dst),'empty',lambda *_:None,problems.append, children[0]['ino'])
            self.assertTrue((dst/'empty').is_dir())
            e = next(e for e in entries if e['rel']=='.hidden')
            self.assertEqual(copy.engine.copy_entry(str(src),str(dst),e,'test')['status'],'skipped')
            (src/'.hidden').write_text('changed')
            self.assertEqual(copy.engine.copy_entry(str(src),str(dst),e,'test')['status'],'changed')

    def test_ancestor_links_and_inode_replacement_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            src,dst,out = [Path(root)/x for x in ('s','d','outside')]
            for p in (src,dst,out):p.mkdir()
            (src/'nested').mkdir(); (src/'nested/file').write_text('source')
            (dst/'nested').symlink_to(out)
            work,issues=[],[]
            copy.scan_directory(str(src),str(dst),'nested',lambda d,e:work.extend(e),issues.append)
            self.assertEqual(issues[0]['status'],'conflict')
            self.assertEqual(copy.engine.copy_entry(str(src),str(dst),work[0],'test')['status'],'error')
            self.assertFalse((out/'file').exists())
            with self.assertRaises(copy.engine.SourceChanged):
                copy.scan_directory(str(src),str(dst),'nested',lambda *_:None,issues.append,-1)
            (src/'bad').symlink_to(out)
            with self.assertRaises(OSError):
                copy.scan_directory(str(src),str(dst),'bad',lambda *_:None,issues.append)

    def test_streaming_batches_and_task_journals_end_to_end(self):
        # Run the REAL controller and worker protocol with two private local actors.
        # The fixture bypasses only production mount/hostname validation.
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);run=root/'run';(run/'code').mkdir(parents=True);(run/'logs').mkdir()
            src,dst=root/'s',root/'d';src.mkdir();dst.mkdir()
            for i in range(4):
                p=src/str(i);p.mkdir()
                for j in range(140): (p/str(j)).write_bytes(f'{i}:{j}'.encode())
            (src/'empty').mkdir()
            (dst/'0').mkdir();(dst/'0/0').write_bytes(b'preserved')
            actor=run/'code/projset_copy.py'
            actor.write_text('import sys\nsys.path.insert(0,'+repr(str(CODE_DIR))+')\nimport projset_copy as m\nm.validate=lambda *a:None\nm.identity=lambda:{}\na=m.argparse.ArgumentParser();a.add_argument("mode");a.add_argument("--run",type=m.Path);a.add_argument("--node");x=a.parse_args();m.worker(x.run,x.node)\n')
            cfg=dict(run_id='test-'+copy.uuid.uuid4().hex,source=str(src),target=str(dst),threads=4,nodes={'node-a':{},'node-b':{}},code={})
            (run/'config.json').write_text(json.dumps(cfg))
            harness=root/'controller.py'
            harness.write_text('import sys\nsys.path.insert(0,'+repr(str(CODE_DIR))+')\nimport projset_copy as m\nm.ssh=lambda node,command:["sh","-c",command]\nm.controller(m.Path('+repr(str(run))+'))\n')
            process=subprocess.run([sys.executable,'-S','-B',str(harness)],capture_output=True,text=True,timeout=90)
            self.assertEqual(process.returncode,0,process.stderr)
            status=copy.read(run/'status.json')
            self.assertEqual(status['finished_jobs'],status['discovered_jobs'])
            self.assertEqual(status['counts']['copied'],559)
            self.assertEqual(status['counts']['conflict'],1)
            self.assertTrue((dst/'empty').is_dir())
            self.assertEqual((dst/'0/0').read_bytes(),b'preserved')
            tasks=[json.loads(x) for x in (run/'tasks.jsonl').read_text().splitlines()]
            done=[json.loads(x) for x in (run/'completed.jsonl').read_text().splitlines()]
            self.assertEqual({x['id'] for x in tasks},{x['id'] for x in done})
            paths=[e['rel'] for t in tasks if t['type']=='copy' for e in t['entries']]
            self.assertEqual(len(paths),560)
            self.assertEqual(len(set(paths)),560)
            self.assertLessEqual(max(len(t['entries']) for t in tasks if t['type']=='copy'),128)


if __name__=='__main__':unittest.main()
