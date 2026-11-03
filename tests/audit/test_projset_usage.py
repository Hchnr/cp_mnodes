"""Read-only inventory checks using private trees and simulated SSH only."""
import importlib.util
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

SOURCE = Path(__file__).resolve().parents[2]/'ops/diagnostics/projset_usage.py'
if not SOURCE.is_file():
    SOURCE = Path(__file__).with_name('projset_usage.py')
spec=importlib.util.spec_from_file_location('projset_usage_test',SOURCE)
scan=importlib.util.module_from_spec(spec)
spec.loader.exec_module(scan)


class UsageTests(unittest.TestCase):
    def test_relative_paths_and_operand_guards(self):
        for path in ('..','a/../b','/tmp','a//b','a\x00b'):
            with self.assertRaises(ValueError):scan.components(path)
        self.assertEqual(scan.components('视频/a\nb'),['视频','a\nb'])

    def test_postorder_byte_accounting_no_double_count(self):
        meter=scan.DuProgress(['a','b'])
        for name,size in [('a/file',100),('a/sub/other',200),('a/sub',210),('a',320),('b',50)]:
            meter.record(name,size)
        self.assertEqual(meter.items,5)
        self.assertEqual(meter.bytes,370)
        self.assertEqual(meter.total,370)
        self.assertEqual(meter.root_totals,{'a':320,'b':50})

    def test_private_du_matches_native_du_with_hidden_links_and_odd_names(self):
        with tempfile.TemporaryDirectory(prefix='projset-usage-private-') as temp:
            root=Path(temp)/'source';root.mkdir()
            (root/'a/child').mkdir(parents=True)
            (root/'a/.hidden').write_bytes(b'x'*9000)
            (root/'a/child/data').write_bytes(b'y'*19000)
            (root/'outside').write_text('keep')
            (root/'a/link').symlink_to(root/'outside')
            os.link(root/'a/.hidden',root/'a/second-link')
            weird='-rf\n\t视频'
            (root/weird).write_bytes(b'z'*3000)
            task=dict(id=1,parent='',names=['a',weird])
            updates=[]
            result=scan.scan_job(str(root),task,threading.Event(),{},
                lambda *args:updates.append(args),set(),threading.Lock())
            expected=subprocess.check_output(['du','--summarize','--block-size=1',
                '--count-links','--one-file-system','--',str(root/'a')])
            self.assertEqual(result['rc'],0)
            self.assertTrue(result['all_operands_returned'])
            self.assertEqual(result['roots']['a'],int(expected.split(b'\t',1)[0]))
            self.assertEqual(result['bytes'],result['observed_bytes'])
            self.assertTrue((root/'outside').exists())
            self.assertTrue(any(path=='a/child' for path,size in result['rows']))
            self.assertTrue(updates)

    def test_symlinked_parent_is_not_scanned(self):
        with tempfile.TemporaryDirectory(prefix='projset-usage-private-') as temp:
            root=Path(temp)/'source';root.mkdir()
            outside=Path(temp)/'outside';outside.mkdir()
            (root/'escape').symlink_to(outside,target_is_directory=True)
            with self.assertRaises(OSError):scan.directory(str(root),'escape')

    def test_depth_five_filter_still_measures_deeper_contents(self):
        with tempfile.TemporaryDirectory(prefix='projset-usage-private-') as temp:
            root=Path(temp)
            parent=root/'a/b/c/d';parent.mkdir(parents=True)
            (parent/'e/f/g').mkdir(parents=True)
            (parent/'e/f/g/data').write_bytes(b'z'*100000)
            result=scan.scan_job(str(root),dict(id=1,parent='a/b/c/d',names=['e']),
                threading.Event(),{},lambda *a:None,set(),threading.Lock())
            self.assertEqual([p for p,s in result['rows']],['a/b/c/d/e'])
            self.assertGreaterEqual(result['bytes'],100000)

    def test_sibling_batch_never_accepts_nested_operands(self):
        with self.assertRaises(ValueError):
            scan.scan_job('/nonexistent',dict(id=1,parent='',names=['a/b']),
                threading.Event(),{},lambda *a:None,set(),threading.Lock())

    def test_controller_fixture_rolls_up_root_and_keeps_all_scope(self):
        with tempfile.TemporaryDirectory(prefix='projset-controller-private-') as temp:
            base=Path(temp);root=base/'source';root.mkdir();run=base/'run';run.mkdir()
            (run/'logs').mkdir()
            (root/'a/b/c/d/e/f').mkdir(parents=True)
            (root/'a/b/c/d/e/f/data').write_bytes(b'x'*17000)
            (root/'.hidden').write_bytes(b'h'*10000)
            (root/'keep').write_text('remain')
            settings=dict(source=str(root),root_inode=root.stat().st_ino,
                nodes={'fixture':dict(scan.identity(str(root)),node='fixture')},per_node=2,
                heavy=[],special_flat=[],script_sha256='private')
            scan.write_json(run/'settings.json',settings)
            (run/'scanner.py').write_bytes(SOURCE.read_bytes())
            settings['script_sha256']=scan.hashlib.sha256((run/'scanner.py').read_bytes()).hexdigest()
            scan.write_json(run/'settings.json',settings)
            expected=int(subprocess.check_output(['du','-s','-B1','-l','-x','--',str(root)]).split(b'\t',1)[0])
            def local_command(node,command):
                import shlex
                return [sys.executable,'-S','-B']+shlex.split(command)[3:]
            with mock.patch.object(scan,'ssh',side_effect=local_command):
                scan.controller(run)
            status=scan.read_json(run/'status.json')
            self.assertEqual(status['state'],'complete',scan.read_json(run/'errors.json'))
            self.assertEqual(status['completed_allocated_bytes'],expected)
            self.assertEqual(status['measured_allocated_bytes'],expected)
            self.assertTrue((root/'keep').exists())
            self.assertTrue((run/'directories-depth5.csv').exists())


if __name__=='__main__':unittest.main()
