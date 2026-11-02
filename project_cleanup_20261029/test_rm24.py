import importlib.util
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import subprocess
import sys
import time
import unittest

spec = importlib.util.spec_from_file_location('rm24_prepared', Path(__file__).with_name('rm24.py'))
rm24 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rm24)


class RmTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='rm24-private-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.target = self.base/'fixture'
        self.target.mkdir()
        self.root = rm24.Root(str(self.target), self.target.stat().st_ino)
        self.addCleanup(self.root.close)
        self.stop = threading.Event()
        self.children = set()
        self.lock = threading.Lock()
        self.errors = (self.base/'errors.log').open('ab')
        self.addCleanup(self.errors.close)

    def remove(self, task):
        return rm24.native_rm(self.root, task, self.stop, self.children, self.lock, self.errors)

    def test_native_rm_special_names_links_and_retained_root(self):
        outside = self.base/'keep'
        outside.write_text('keep')
        names = ['-rf', 'a\nb', '$(not-a-command)', '`literal`', '.hidden', '视频']
        for value in names:
            (self.target/value).write_text('test')
        (self.target/'link').symlink_to(outside)
        result = self.remove(dict(parent='', ino=self.root.inode, names=names+['link']))
        self.assertEqual(result['rc'], 0)
        self.assertEqual(list(self.target.iterdir()), [])
        self.assertEqual(outside.read_text(), 'keep')

    def test_root_and_path_escape_cannot_be_rm_arguments(self):
        for value in ('', '.', '..', '/share/project', 'a/b', 'a\0b'):
            with self.assertRaises(ValueError):
                self.remove(dict(parent='', ino=self.root.inode, names=[value]))

    def test_parent_symlink_is_rejected(self):
        outside = self.base/'outside'
        outside.mkdir()
        (outside/'keep').write_text('keep')
        (self.target/'escape').symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            self.remove(dict(parent='escape', ino=outside.stat().st_ino, names=['keep']))
        self.assertTrue((outside/'keep').exists())

    def test_shallow_partition_all_leaf_jobs_then_partition_parents(self):
        for path in ('a/b/c/deep/file', 'a/b/other', 'z/file', '.hidden', 'root-file'):
            target = self.target/path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('test')
        items = list(rm24.plan(self.root, depth=2))
        self.assertEqual(items[-1]['kind'], 'end')
        jobs = [i for i in items if i['kind'] == 'task']
        partitions = [i for i in items if i['kind'] == 'scaffold']
        with rm24.futures.ThreadPoolExecutor(max_workers=4) as pool:
            self.assertTrue(all(r['rc'] == 0 for r in pool.map(self.remove, jobs)))
        for depth in sorted({i['depth'] for i in partitions}, reverse=True):
            with rm24.futures.ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(self.remove, [i for i in partitions if i['depth'] == depth]))
            self.assertTrue(all(r['rc'] == 0 for r in results))
        self.assertEqual(list(self.target.iterdir()), [])

    def test_wide_flat_directory_is_streamed_in_bounded_batches(self):
        for i in range(260):
            (self.target/str(i)).touch()
        items = list(rm24.plan(self.root))
        jobs = [i for i in items if i['kind'] == 'task']
        self.assertEqual([len(i['names']) for i in jobs], [128, 128, 4])
        self.assertEqual(len({n for i in jobs for n in i['names']}), 260)

    def test_expansion_limit_keeps_large_subtrees_whole(self):
        (self.target/'a/b/c').mkdir(parents=True)
        items = list(rm24.plan(self.root, expansion_limit=1))
        self.assertEqual(items[0]['names'], ['a'])
        self.assertEqual(items[0]['parent'], '')
        self.assertFalse(any(i['kind'] == 'scaffold' for i in items))

    def test_stop_does_not_start_rm(self):
        (self.target/'keep').write_text('keep')
        self.stop.set()
        result = self.remove(dict(parent='', ino=self.root.inode, names=['keep']))
        self.assertEqual(result['rc'], 125)
        self.assertTrue((self.target/'keep').exists())

    def test_private_worker_protocol_uses_native_rm_and_returns_before_eof(self):
        run = self.base/'control'
        (run/'logs').mkdir(parents=True)
        (run/'workers').mkdir()
        module = Path(rm24.__file__).resolve()
        rm24.atomic(run/'config.json', dict(target=str(self.target), root_inode=self.root.inode,
            nodes=rm24.NODES, per_node=2, sha256=hashlib.sha256(module.read_bytes()).hexdigest()))
        # A stale shared file used to trigger SIGTERM. It must have no bearing
        # on a worker receiving control messages over its live SSH stream.
        rm24.atomic(run/'controller-heartbeat.json', dict(updated=time.time()-10000))
        for name in ('a', 'b'):
            (self.target/name).write_text('private fixture')
        # Test-only bindings remain inside this independently created temp tree.
        code = ('import importlib.util,pathlib\n'
            's=importlib.util.spec_from_file_location("rm_test",'+repr(str(module))+')\n'
            'm=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n'
            'm.TARGET='+repr(str(self.target))+';m.ROOT_INODE='+str(self.root.inode)+'\n'
            'm.node_check=lambda *a,**k:None\n'
            'm.worker(pathlib.Path('+repr(str(run))+'),"worker-0")\n')
        p = subprocess.Popen([sys.executable, '-S', '-B', '-u', '-c', code],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            p.stdin.write(json.dumps(dict(kind='ping'))+'\n')
            p.stdin.flush()
            self.assertEqual(json.loads(p.stdout.readline()), dict(kind='pong'))
            time.sleep(1.2)
            for name in ('a', 'b'):
                p.stdin.write(json.dumps(dict(id=name, parent='', ino=self.root.inode, names=[name]))+'\n')
            p.stdin.flush()
            import select
            results = []
            for _ in range(2):
                # Read one byte at a time to avoid TextIO read-ahead obscuring
                # the readiness of the second protocol message.
                line = b''
                deadline = time.monotonic()+10
                while not line.endswith(b'\n'):
                    timeout = max(0, deadline-time.monotonic())
                    ready, _, _ = select.select([p.stdout.fileno()], [], [], timeout)
                    self.assertTrue(ready, 'private worker timed out')
                    byte = os.read(p.stdout.fileno(), 1)
                    self.assertTrue(byte, 'private worker exited early')
                    line += byte
                results.append(json.loads(line))
            p.stdin.write(json.dumps(dict(kind='finish'))+'\n')
            p.stdin.flush()
            p.stdin.close()
            self.assertEqual(p.wait(timeout=10), 0, p.stderr.read())
            self.assertEqual({r['id'] for r in results}, {'a', 'b'})
            self.assertTrue(all(r['rc'] == 0 for r in results))
            self.assertEqual(list(self.target.iterdir()), [])
            self.assertFalse((run/'workers/worker-0-first-stop.json').exists())
        finally:
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=10)
            for stream in (p.stdin, p.stdout, p.stderr):
                stream.close()

    def test_control_lease_uses_local_monotonic_receipt_not_wall_clock(self):
        now = [10.]
        lease = rm24.ControlLease(timeout=3, clock=lambda:now[0])
        now[0] = 12.
        self.assertFalse(lease.expired())
        lease.touch()
        now[0] = 14.
        self.assertFalse(lease.expired())
        now[0] = 16.
        self.assertTrue(lease.expired())

    def controller_fixture(self, failures=0, supplement=False, streaming=False):
        from unittest.mock import patch
        import shutil
        run = self.base/'fleet-control'
        (run/'logs').mkdir(parents=True)
        (run/'workers').mkdir()
        script = run/'rm24.py'
        shutil.copyfile(rm24.__file__, script)
        nodes = ['worker-0', 'worker-1']
        cfg = dict(target=str(self.target), root_inode=self.root.inode, nodes=nodes, per_node=2,
                   sha256=hashlib.sha256(script.read_bytes()).hexdigest())
        if supplement:
            cfg.update(supplement_of='private-fixture-original',execution_nodes=nodes[:1],seed_tasks=[
                dict(kind='task',parent='',ino=self.root.inode,names=[n]) for n in ('a','d')])
        rm24.atomic(run/'config.json', cfg)
        for path in ('a/b/c/leaf', 'a/b/leaf', 'd/file', '.hidden'):
            p = self.target/path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('private test data')
        if failures:
            (self.target/'blocked').write_text('private temporary failure fixture')
        if streaming:
            cfg.update(supplement_of='private-fixture-original', execution_nodes=nodes[:1],
                       tail_scopes=[dict(rel=n, ino=(self.target/n).stat().st_ino) for n in ('a', 'd')])
            rm24.atomic(run/'config.json', cfg)

        def local_transport(node, script, action, run_arg=None):
            code = ('import importlib.util,pathlib,os,json\n'
                's=importlib.util.spec_from_file_location("fixture_rm",'+repr(str(script))+')\n'
                'm=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n'
                'target=pathlib.Path('+repr(str(self.target))+')\n'
                'assert target.parent.name.startswith("rm24-private-test-")\n'
                'm.TARGET=str(target);m.ROOT_INODE='+str(self.root.inode)+'\n'
                'm.NODES='+repr(nodes)+';m.node_check=lambda *a,**k:None\n')
            if failures:
                code += ('real=m.native_rm\n'
                    'def inject(root,task,*a,**k):\n'
                    ' if "blocked" in task["names"]:\n'
                    '  p=pathlib.Path('+repr(str(run/'failure-attempts'))+')\n'
                    '  n=int(p.read_text()) if p.exists() else 0\n'
                    '  p.write_text(str(n+1))\n'
                    '  if n<'+str(failures)+':return dict(rc=1)\n'
                    ' return real(root,task,*a,**k)\n'
                    'm.native_rm=inject\n')
            if action == '_worker':
                code += 'm.worker(pathlib.Path('+repr(str(run))+'),'+repr(node)+')\n'
            elif action == '_plan':
                code += ('r=m.Root(str(target),m.ROOT_INODE)\n'
                         'for item in m.plan(r):print(json.dumps(item),flush=True)\nr.close()\n')
            elif action == '_verify':
                code += 'print(json.dumps(dict(empty=next(target.iterdir(),None) is None)))\n'
            else:
                raise AssertionError(action)
            return [sys.executable, '-S', '-B', '-u', '-c', code]

        with patch.object(rm24, 'NODES', nodes), patch.object(rm24, 'TARGET', str(self.target)), \
                patch.object(rm24, 'ROOT_INODE', self.root.inode), \
                patch.object(rm24, 'ssh', side_effect=local_transport), \
                patch.object(rm24, 'preflight', return_value=[dict(node=n) for n in nodes]):
            rm24.controller(run)
        return run

    def test_full_controller_private_fleet_with_control_pings(self):
        run = self.controller_fixture()
        self.assertEqual(rm24.load(run/'status.json')['state'], 'complete_root_retained')
        self.assertFalse((run/'first-error.json').exists())
        self.assertFalse((run/'STOP').exists())
        self.assertEqual(list(self.target.iterdir()), [])

    def test_rm_partial_error_does_not_stop_fleet_and_retries_after_normal_work(self):
        run = self.controller_fixture(failures=1)
        status = rm24.load(run/'status.json')
        self.assertEqual(status['state'], 'complete_root_retained')
        self.assertEqual(status['failed_rm_events'], 1)
        self.assertEqual(status['unresolved_rm_jobs'], 0)
        self.assertEqual((run/'failure-attempts').read_text(), '2')
        self.assertFalse((run/'STOP').exists())
        self.assertFalse((run/'first-error.json').exists())
        self.assertEqual(list(self.target.iterdir()), [])

    def test_persistent_rm_error_is_bounded_and_remaining_not_claimed_complete(self):
        run = self.controller_fixture(failures=99)
        status = rm24.load(run/'status.json')
        self.assertEqual(status['state'], 'finished_with_remaining_entries')
        self.assertEqual(status['failed_rm_events'], 3)
        self.assertEqual(status['unresolved_rm_jobs'], 1)
        self.assertEqual((run/'failure-attempts').read_text(), '3')
        self.assertTrue((self.target/'blocked').exists())
        self.assertFalse((self.target/'a').exists())
        self.assertFalse((self.target/'d').exists())
        self.assertEqual(status['errors'], [])
        self.assertFalse((run/'STOP').exists())
        self.assertTrue((run/'remaining-jobs.json').exists())

    def test_supplement_uses_only_seed_tasks_and_does_not_clean_partition_parents(self):
        run = self.controller_fixture(supplement=True)
        self.assertEqual(rm24.load(run/'status.json')['state'], 'supplement_complete')
        self.assertTrue((self.target/'.hidden').exists())
        self.assertFalse((self.target/'a').exists())
        self.assertFalse((self.target/'d').exists())
        self.assertFalse((run/'STOP').exists())

    def test_cooperative_missing_parent_is_already_gone(self):
        self.assertEqual(self.remove(dict(parent='already-removed',ino=123,names=['a']))['rc'],0)
        self.assertTrue(self.target.is_dir())

    def test_supplement_selection_excludes_currently_open_branch(self):
        import boost
        for path in ('tree/busy/leaf/current','tree/free/a/data','tree/free/b/data'):
            p=self.target/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('fixture')
        tasks=boost.choose_tasks(self.root,[str(self.target/'tree')],[str(self.target/'tree/busy/leaf/current')])
        selected=[t['parent']+'/'+n for t in tasks for n in t['names']]
        self.assertEqual(set(selected),{'tree/free/a','tree/free/b'})


    def cooperative(self, rels, **options):
        scopes = [dict(rel=n, ino=(self.target/n).stat().st_ino) for n in rels]
        return rm24.cooperative_plan(self.root, scopes, **options)

    def test_streaming_frontier_is_fair_and_batches_flat_files(self):
        for rel, count in [('a', 260), ('z', 1)]:
            (self.target/rel).mkdir()
            for n in range(count):
                (self.target/rel/str(n)).touch()
        jobs = [t for t in self.cooperative(['a', 'z']) if t['kind'] == 'task']
        self.assertEqual([t['parent'] for t in jobs], ['a', 'z', 'a', 'a'])
        self.assertEqual([len(t['names']) for t in jobs], [128, 1, 128, 4])
        self.assertEqual(len(list((self.target/'a').iterdir())), 260)

    def test_streaming_frontier_is_not_limited_to_old_25_or_256_seed_jobs(self):
        for n in range(300):
            (self.target/'wide'/'branch'/str(n)).mkdir(parents=True)
        jobs = [t for t in self.cooperative(['wide']) if t['kind'] == 'task']
        self.assertEqual(len(jobs), 300)
        paths = [t['parent']+'/'+name for t in jobs for name in t['names']]
        self.assertEqual(len(set(paths)), 300)
        self.assertEqual(len(list((self.target/'wide/branch').iterdir())), 300)

    def test_streaming_frontier_preserves_protected_work_and_does_not_follow_symlinks(self):
        for rel in ('tree/free/a/data', 'tree/busy/deep/current', 'tree/busy/deep/spare',
                    'tree/shared/protected/data', 'tree/shared/free/data'):
            p = self.target/rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('private fixture')
        outside = self.base/'outside'
        outside.mkdir()
        (outside/'keep').write_text('keep')
        (self.target/'tree/alias').symlink_to(outside, target_is_directory=True)
        protected = str(self.target/'tree/shared/protected')
        busy = str(self.target/'tree/busy/deep/current')
        jobs = [t for t in self.cooperative(['tree'], blocked=[protected], busy=[busy])
                if t['kind'] == 'task']
        selected = [str(self.target/t['parent']/name) for t in jobs for name in t['names']]
        for path in selected:
            self.assertFalse(path == protected or path.startswith(protected+'/')
                             or protected.startswith(path+'/'))
            self.assertFalse(path == busy or busy.startswith(path+'/'))
        for job in jobs:
            self.assertEqual(self.remove(job)['rc'], 0)
        self.assertTrue((self.target/'tree/shared/protected/data').exists())
        self.assertTrue((self.target/'tree/busy/deep/current').exists())
        self.assertFalse((self.target/'tree/busy/deep/spare').exists())
        self.assertFalse((self.target/'tree/alias').exists())
        self.assertEqual((outside/'keep').read_text(), 'keep')

    def test_streaming_planner_closes_directory_fds_on_early_preview_exit(self):
        for rel in ('a/b/file', 'z/d/file'):
            p = self.target/rel
            p.parent.mkdir(parents=True)
            p.touch()
        before = len(list(Path('/proc/self/fd').iterdir()))
        stream = self.cooperative(['a', 'z'])
        for _ in range(4):
            next(stream)
        stream.close()
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), before)

    def test_streaming_planner_rejects_ancestor_scopes_and_escaped_protections(self):
        (self.target/'a/b').mkdir(parents=True)
        with self.assertRaises(ValueError):
            list(self.cooperative(['a', 'a/b']))
        with self.assertRaises(ValueError):
            list(self.cooperative(['a'], blocked=[str(self.base/'outside')]))
        with self.assertRaises(ValueError):
            list(rm24.cooperative_plan(self.root, [dict(rel='', ino=self.root.inode)]))

    def test_streaming_controller_uses_live_frontier_without_global_parent_cleanup(self):
        run = self.controller_fixture(streaming=True)
        status = rm24.load(run/'status.json')
        self.assertEqual(status['state'], 'supplement_complete')
        self.assertEqual(status['unresolved_rm_jobs'], 0)
        self.assertTrue((self.target/'.hidden').exists())
        self.assertTrue((self.target/'a').is_dir())
        self.assertTrue((self.target/'d').is_dir())
        self.assertFalse((self.target/'a/b/c/leaf').exists())
        self.assertFalse((self.target/'d/file').exists())
        self.assertFalse((run/'STOP').exists())

    def test_streaming_prepare_is_read_only_and_preserves_existing_run_pointer(self):
        import tailboost
        from unittest.mock import patch
        workspace = self.base/'workspace'
        original = workspace/'runs/original'
        original.mkdir(parents=True)
        (self.target/'tree/branch').mkdir(parents=True)
        (self.target/'tree/branch/keep').write_text('private fixture')
        rm24.atomic(original/'config.json', dict(target=str(self.target), root_inode=self.root.inode))
        rm24.atomic(original/'status.json', dict(state='rm_running', updated=time.time()))
        old_pointer = workspace/'runs/boost-current.json'
        old_pointer.write_text('unchanged old supplement pointer')
        observations = [dict(node='worker-0', rm_count=0, roots=[], opened=[]),
                        dict(node='worker-1', rm_count=1, roots=[str(self.target/'tree')], opened=[])]
        pointer = workspace/'runs/tail-boost-current.json'
        with patch.object(tailboost, 'HERE', workspace), patch.object(tailboost, 'POINTER', pointer), \
                patch.object(tailboost.rm24, 'RUN', original), \
                patch.object(tailboost.rm24, 'TARGET', str(self.target)), \
                patch.object(tailboost.rm24, 'ROOT_INODE', self.root.inode), \
                patch.object(tailboost.rm24, 'preflight', return_value=[dict(node='worker-0')]), \
                patch.object(tailboost.boost, 'observe', return_value=observations):
            tailboost.prepare(2, 12)
        prepared = Path(rm24.load(pointer)['run'])
        cfg = rm24.load(prepared/'config.json')
        self.assertEqual(cfg['execution_nodes'], ['worker-0'])
        self.assertEqual(cfg['per_node'], 2)
        self.assertEqual(cfg['tail_scopes'], [dict(rel='tree', ino=(self.target/'tree').stat().st_ino)])
        self.assertEqual((self.target/'tree/branch/keep').read_text(), 'private fixture')
        self.assertEqual(old_pointer.read_text(), 'unchanged old supplement pointer')
        self.assertFalse((prepared/'start-claim.json').exists())
        self.assertFalse((prepared/'controller.json').exists())


if __name__ == '__main__':
    unittest.main()
