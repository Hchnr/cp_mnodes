"""Full controller/worker integration only inside independently created fixtures."""
import collections
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('private_parent_rm24', Path(__file__).with_name('rm24.py'))
rm24 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rm24)


class ParentIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='parent-integration-private-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.target = self.base/'fixture'
        self.target.mkdir()
        self.ino = self.target.stat().st_ino
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.boots = dict.fromkeys(rm24.NODES, self.boot)

    def prepared(self, kind='flat', count=12, bad_boot=False, nodes=None):
        run = self.base/'run'
        nodes = ['worker-0', 'worker-1'] if nodes is None else nodes
        seeds = []
        for index in range(count):
            name = kind+'-'+str(index)
            if kind == 'directory':
                (self.target/name).mkdir()
                (self.target/name/'leaf').write_text('private fixture only')
            else:
                (self.target/name).write_text('private fixture only')
            item = dict(kind='task', parent='', ino=self.ino, names=[name])
            if kind != 'directory':
                item['flat_batch'] = True
            seeds.append(item)
        boots = dict(self.boots)
        if bad_boot:
            boots[nodes[0]] = 'not-the-live-boot'
        with patch.object(rm24, 'TARGET', str(self.target)), patch.object(rm24, 'ROOT_INODE', self.ino), \
                patch.object(rm24, 'preflight', return_value=[]), contextlib.redirect_stdout(io.StringIO()):
            rm24.prepare(run, 4, execution_nodes=nodes, seed_tasks=seeds, supplement_of='private-original',
                         runtime_join=True, parent_policy=dict(limit=2, node_boots=boots))
        return run, seeds

    def execute(self, run, fail_first=False, wrong_receipt=None):
        events = run/'private-execution.jsonl'
        def transport(node, script, action, run_arg=None):
            self.assertEqual(action, '_worker')
            code = ('import importlib.util,pathlib,time,json,fcntl,builtins\n'
                    's=importlib.util.spec_from_file_location("private_worker",'+repr(str(script))+')\n'
                    'm=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n'
                    'target=pathlib.Path('+repr(str(self.target))+')\n'
                    'assert target.parent.name.startswith("parent-integration-private-")\n'
                    'm.TARGET=str(target);m.ROOT_INODE='+str(self.ino)+'\n'
                    'm.node_check=lambda *a,**k:None\n'
                    'm.socket.gethostname=lambda:'+repr(rm24.NODE_HOSTS[node])+'\n'
                    'node='+repr(node)+'\n'
                    'original=m.native_rm\n'
                    'def event(task,kind):\n'
                    ' with open('+repr(str(events))+',"a") as f:\n'
                    '  fcntl.flock(f,fcntl.LOCK_EX)\n'
                    '  f.write(json.dumps(dict(kind=kind,node=node,id=task["id"],flat=task.get("flat_batch") is True,parent=task["parent"],ino=task["ino"]))+"\\n")\n'
                    '  f.flush()\n'
                    '  fcntl.flock(f,fcntl.LOCK_UN)\n'
                    'def instrumented(root,task,*args):\n'
                    ' event(task,"start")\n'
                    ' try:\n'
                    '  time.sleep(.04)\n')
            if fail_first:
                code += ('  if "flat-0" in task["names"] and not task.get("retry_attempt"):\n'
                         '   return dict(rc=1)\n')
            code += ('  return original(root,task,*args)\n'
                     ' finally:event(task,"end")\n'
                     'm.native_rm=instrumented\n')
            if wrong_receipt:
                code += ('real_print=builtins.print\nchanged=[False]\n'
                         'def injected(*args,**kwargs):\n'
                         ' if args and not changed[0] and isinstance(args[0],str):\n'
                         '  try:data=json.loads(args[0])\n'
                         '  except ValueError:data={}\n'
                         '  if "id" in data and "rc" in data:\n'
                         '   changed[0]=True\n')
                if wrong_receipt == 'unknown':
                    code += ('   data["id"]="never-leased"\n'
                             '   return real_print(json.dumps(data),**kwargs)\n')
                else:
                    code += ('   real_print(*args,**kwargs)\n'
                             '   return real_print(*args,**kwargs)\n')
                code += (' return real_print(*args,**kwargs)\n'
                         'builtins.print=injected\n')
            code += 'm.worker(pathlib.Path('+repr(str(run))+'),node)\n'
            return [sys.executable, '-S', '-B', '-u', '-c', code]

        before_threads = set(threading.enumerate())
        with patch.object(rm24, 'TARGET', str(self.target)), patch.object(rm24, 'ROOT_INODE', self.ino), \
                patch.object(rm24, 'preflight', return_value=[]), patch.object(rm24, 'ssh', side_effect=transport), \
                contextlib.redirect_stdout(io.StringIO()):
            rm24.controller(run)
        self.assertEqual(set(threading.enumerate()), before_threads)
        status = rm24.load(run/'status.json')
        history = ([json.loads(line) for line in (run/'jobs.jsonl').read_text().splitlines()]
                   if (run/'jobs.jsonl').exists() else [])
        rows = [json.loads(line) for line in events.read_text().splitlines()] if events.exists() else []
        for actor_path in (run/'workers').glob('worker-*.json'):
            if actor_path.name.endswith('-first-stop.json'):
                continue
            actor = rm24.load(actor_path)
            self.assertFalse(Path('/proc/'+str(actor['pid'])+'/stat').exists(), 'private worker leaked')
        return status, history, rows

    def test_flat_same_boot_cap_two_real_workers_receipts_and_rc1_retry_conserved(self):
        run, seeds = self.prepared()
        status, history, events = self.execute(run, fail_first=True)
        self.assertEqual(status['state'], 'supplement_complete')
        self.assertEqual(status['failed_rm_events'], 1)
        self.assertEqual(status['unresolved_rm_jobs'], 0)
        self.assertEqual(status['errors'], [])
        self.assertEqual(list(self.target.iterdir()), [])
        active = maximum = 0
        for event in events:
            active += 1 if event['kind'] == 'start' else -1
            maximum = max(maximum, active)
            self.assertGreaterEqual(active, 0)
            self.assertLessEqual(active, 2)
        self.assertEqual(active, 0)
        self.assertEqual(maximum, 2)
        successful = [row for row in history if row['result']['rc'] == 0]
        self.assertCountEqual([row['task']['names'][0] for row in successful], [s['names'][0] for s in seeds])
        self.assertEqual(status['parent_scheduler']['active_leases'], 0)
        self.assertEqual(status['parent_scheduler']['claims'], status['parent_scheduler']['releases'])
        self.assertEqual(status['parent_tuning']['limit'], 2)

    def test_independent_directory_subtrees_not_throttled_by_shared_parent(self):
        run, seeds = self.prepared(kind='directory', count=12)
        status, history, events = self.execute(run)
        self.assertEqual(status['state'], 'supplement_complete')
        active = maximum = 0
        for event in events:
            self.assertFalse(event['flat'])
            active += 1 if event['kind'] == 'start' else -1
            maximum = max(maximum, active)
        self.assertGreater(maximum, 2)
        self.assertEqual(len(history), len(seeds))
        self.assertEqual(status['parent_scheduler']['active_flat_groups'], 0)
        self.assertEqual(list(self.target.iterdir()), [])

    def test_initial_worker_boot_mismatch_rejected_before_any_claim(self):
        run, seeds = self.prepared(bad_boot=True, nodes=['worker-0'])
        status, history, events = self.execute(run)
        self.assertEqual(status['state'], 'stopped_or_error')
        self.assertEqual(status['parent_scheduler']['claims'], 0)
        self.assertEqual(history, [])
        self.assertEqual(events, [])
        self.assertEqual(len(list(self.target.iterdir())), len(seeds))

    def test_runtime_join_boot_mismatch_does_not_stop_existing_worker(self):
        run, seeds = self.prepared(nodes=['worker-0'])
        cfg = rm24.load(run/'config.json')
        cfg['parent_policy']['node_boots']['worker-1'] = 'wrong-joining-boot'
        rm24.atomic(run/'config.json', cfg)
        actor = rm24.identity(os.getpid())
        request = dict(version=1, nonce='bad-join', run=str(run), controller=actor,
                       config_sha256=hashlib.sha256((run/'config.json').read_bytes()).hexdigest(),
                       node='worker-1', per_node=4)
        rm24.atomic(run/'runtime-requests/bad-join.json', request)
        status, history, events = self.execute(run)
        self.assertEqual(status['state'], 'supplement_complete')
        self.assertEqual(status['errors'], [])
        self.assertTrue(all(row['node'] == 'worker-0' for row in history))
        self.assertEqual(rm24.load(run/'runtime-acks/bad-join.json')['state'], 'rejected')

    def test_unknown_or_duplicate_receipt_does_not_release_unconfirmed_lease(self):
        for mode in ('unknown', 'duplicate'):
            with self.subTest(mode=mode):
                fixture = ParentIntegrationTests(methodName='runTest')
                fixture.setUp()
                try:
                    run, seeds = fixture.prepared(nodes=['worker-0'])
                    status, history, events = fixture.execute(run, wrong_receipt=mode)
                    self.assertEqual(status['state'], 'stopped_or_error')
                    self.assertTrue(any('receipt' in reason for reason in status['errors']))
                    self.assertGreater(status['parent_scheduler']['active_leases'], 0)
                    self.assertLess(status['parent_scheduler']['releases'], status['parent_scheduler']['claims'])
                    self.assertFalse(any(row['result']['id'] == 'never-leased' for row in history))
                finally:
                    fixture.doCleanups()

    def test_helpers_are_sha_frozen_and_loaded_from_exact_run_not_import_cache(self):
        run, seeds = self.prepared()
        cfg = rm24.load(run/'config.json')
        with patch.object(rm24, 'TARGET', str(self.target)), patch.object(rm24, 'ROOT_INODE', self.ino):
            loaded = rm24.parent_helpers(run, cfg)
        self.assertEqual(set(loaded), {'parent_scheduler.py', 'parent_tuning.py'})
        self.assertEqual(Path(loaded['parent_scheduler.py'].__file__), run/'parent_scheduler.py')
        with (run/'parent_scheduler.py').open('a') as stream:
            stream.write('\n# private fixture tampering\n')
        with patch.object(rm24, 'TARGET', str(self.target)), patch.object(rm24, 'ROOT_INODE', self.ino):
            with self.assertRaisesRegex(ValueError, 'frozen parent helper changed'):
                rm24.controller(run)
        self.assertEqual(len(list(self.target.iterdir())), len(seeds))

    def test_bound_hot_policy_applies_to_main_and_future_retry_queue(self):
        run, seeds = self.prepared()
        actor = rm24.identity(os.getpid())
        request = dict(version=1, scope='flat-parent-per-boot-v1', run=str(run), controller=actor,
                       config_sha256=hashlib.sha256((run/'config.json').read_bytes()).hexdigest(),
                       generation=1, operation='a'*32, limit=1)
        rm24.atomic(run/'parent-policy.json', request)
        status, history, events = self.execute(run, fail_first=True)
        self.assertEqual(status['state'], 'supplement_complete')
        self.assertEqual(status['parent_tuning']['limit'], 1)
        self.assertEqual(status['parent_tuning']['generation'], 1)
        self.assertEqual(status['parent_tuning']['operation'], 'a'*32)
        self.assertEqual(status['parent_scheduler']['limit'], 1)
        active = maximum = 0
        for event in events:
            active += 1 if event['kind'] == 'start' else -1
            maximum = max(maximum, active)
        self.assertEqual(maximum, 1)

    def test_parent_limit_cli_is_explicit_and_uses_frozen_helper(self):
        run, seeds = self.prepared()
        from unittest.mock import Mock
        tuner = Mock()
        tuner.publish_limit.return_value = dict(submitted=True, applied=False)
        argv = ['rm24.py', 'parent-limit', '--run', str(run), '--limit', 'off']
        with patch.object(sys, 'argv', argv), patch.object(rm24, 'parent_helpers',
                return_value={'parent_tuning.py': tuner}) as helper, contextlib.redirect_stdout(io.StringIO()):
            rm24.main()
        helper.assert_called_once()
        tuner.publish_limit.assert_called_once_with(run, None, expected_target=rm24.TARGET,
                                                   expected_root_inode=rm24.ROOT_INODE)

    def test_both_planners_only_mark_non_directory_batches_as_flat(self):
        (self.target/'file').write_text('fixture')
        (self.target/'tree/deep').mkdir(parents=True)
        (self.target/'tree/deep/file').write_text('fixture')
        root = rm24.Root(str(self.target), self.ino)
        try:
            regular = list(rm24.plan(root, depth=1))
            self.assertTrue(next(t for t in regular if t.get('names') == ['file'])['flat_batch'])
            self.assertFalse(next(t for t in regular if t.get('names') == ['tree']).get('flat_batch', False))
            stream = list(rm24.cooperative_plan(root, [dict(rel='tree', ino=(self.target/'tree').stat().st_ino)], split_depth=1))
            self.assertFalse(next(t for t in stream if t.get('names') == ['deep']).get('flat_batch', False))
            flat_stream = list(rm24.cooperative_plan(root, [dict(rel='tree/deep', ino=(self.target/'tree/deep').stat().st_ino)]))
            self.assertTrue(next(t for t in flat_stream if t.get('names') == ['file'])['flat_batch'])
        finally:
            root.close()

    def test_invalid_policy_or_non_runtime_prepare_is_refused_before_ssh(self):
        for options in (dict(runtime_join=False, parent_policy=dict(limit=2, node_boots=self.boots)),
                        dict(runtime_join=True, parent_policy=dict(limit=2, node_boots={'worker-0': self.boot})),
                        dict(runtime_join=True, parent_policy=dict(limit=4, node_boots=self.boots))):
            with self.subTest(options=options), patch.object(rm24, 'preflight') as check:
                with self.assertRaises(ValueError):
                    rm24.prepare(self.base/'invalid', 4, **options)
                check.assert_not_called()


if __name__ == '__main__':
    unittest.main()
