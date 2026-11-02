"""Private filesystem fixtures/mocks only; never execute SSH, rm, or signals."""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import upgrade40_tailboost as upgrade


class Upgrade40Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='upgrade40-private-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.runs = self.base/'runs'
        self.runs.mkdir()
        self.old = self.runs/'tail-boost-private-old'
        self.original = self.runs/'prepared-v3'
        for run in (self.old, self.original):
            run.mkdir()
        self.target = self.base/'private-target'
        self.target.mkdir()
        (self.target/'scope').mkdir()
        (self.target/'missed').mkdir()
        self.inode = self.target.stat().st_ino
        self.old_nodes = ['master-0']+['worker-'+str(i) for i in range(23)]
        self.extras = ['extra-master-0']+['extra-worker-'+str(i) for i in range(15)]
        self.hosts = {n:'job-private-old-'+n for n in self.old_nodes}
        self.hosts.update({n:'job-private-extra-'+n.removeprefix('extra-') for n in self.extras})
        self.actor = dict(pid=99998, start='old-start', boot='private-boot')
        self.original_actor = dict(pid=99997, start='original-start', boot='private-boot')
        self.elapsed = 0.0
        self.draining = False
        self.atomic_paths = []
        self.inspections = []
        self.prepares = []
        self.starts = []
        self.stop_hook = None
        self.inspect_hook = None
        self.drain_calls = 0
        self.clock_now = 1700000000.0
        for name in upgrade.SOURCE_NAMES:
            (self.base/name).write_text('# private non-executable fixture '+name+'\n')
        for obj, name, value in (
                (upgrade, 'HERE', self.base), (upgrade, 'POINTER', self.runs/'upgrade40-current.json'),
                (upgrade.tailboost, 'POINTER', self.runs/'tail-current.json'),
                (upgrade.rm24, 'RUN', self.original), (upgrade.rm24, 'TARGET', str(self.target)),
                (upgrade.rm24, 'ROOT_INODE', self.inode), (upgrade.rm24, 'CLUSTER', 'private-old'),
                (upgrade.rm24, 'EXTRA_CLUSTER', 'private-extra'),
                (upgrade.rm24, 'NODE_HOSTS', self.hosts), (upgrade.rm24, 'NODES', list(self.hosts)),
                (upgrade.time, 'time', lambda:self.clock_now+self.elapsed),
                (upgrade.time, 'monotonic', lambda:self.elapsed),
                (upgrade.time, 'sleep', self.sleep)):
            patcher = mock.patch.object(obj, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        for run in (self.old, self.original):
            (run/'rm24.py').write_bytes(b'# private frozen fixture: not executable\n')
        original_cfg = dict(target=str(self.target), root_inode=self.inode, nodes=self.old_nodes,
                            sha256=upgrade.digest(self.original/'rm24.py'))
        self.cfg = dict(original_cfg, sha256=upgrade.digest(self.old/'rm24.py'),
            supplement_of=str(self.original), runtime_join=True, per_node=4,
            execution_nodes=self.old_nodes[:12],
            tail_scopes=[dict(rel='scope', ino=(self.target/'scope').stat().st_ino)],
            blocked_paths=[])
        self.write(self.original/'config.json', original_cfg)
        self.write(self.old/'config.json', self.cfg)
        self.write(self.old/'controller.json', self.actor)
        self.write(self.original/'controller.json', self.original_actor)
        self.write(upgrade.tailboost.POINTER, dict(run=str(self.old), original=str(self.original)))
        self.write(self.old/'status.json', dict(state='rm_running', updated=self.clock_now,
            runtime_join=dict(accepted=self.old_nodes[12:18], starting=[])))
        failed = dict(task=dict(parent='', ino=self.inode, names=['missed']))
        (self.original/'failed-jobs.jsonl').write_text(json.dumps(failed)+'\n')
        self.patch(upgrade, 'inspect_nodes', side_effect=self.inspect)
        self.preflight = self.patch(upgrade.rm24, 'preflight', side_effect=self.preflight_nodes)
        self.patch(upgrade.scale, 'alive', side_effect=lambda actor:not self.draining)
        self.patch(upgrade.rm24, 'prepare', side_effect=self.fake_prepare)
        self.patch(upgrade.subprocess, 'run', side_effect=self.fake_start)
        self.ready = self.patch(upgrade.scale, 'wait_ready', return_value=dict(
            state='rm_running', updated=self.clock_now))
        self.worker_ready = self.patch(upgrade.verify_workers, 'wait_workers_ready',
            return_value=dict(verified_nodes=35, status=dict(state='rm_running', updated=self.clock_now)))
        real_atomic = upgrade.rm24.atomic

        def atomic(path, value):
            path = Path(path)
            self.assertTrue(path.is_relative_to(self.base), 'fixture write escaped private directory')
            self.atomic_paths.append(path)
            real_atomic(path, value)
            if path == self.old/'STOP':
                self.draining = True
                if self.stop_hook:
                    self.stop_hook()
        self.patch(upgrade.rm24, 'atomic', side_effect=atomic)

    def patch(self, obj, name, **kwargs):
        patcher = mock.patch.object(obj, name, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def sleep(self, duration):
        self.elapsed += duration

    def rows(self, old=True):
        result = []
        for i, node in enumerate(self.old_nodes):
            row = dict(node=node, check=dict(node=node, host=self.hosts[node]),
                       helpers=dict(old=[], original=[]), rms=dict(old=[], original=[], unknown=[]))
            if old and i < 18:
                actor = dict(pid=1000+i, start=str(i), boot='private-boot')
                row['helpers']['old'] = [dict(pid=2000+i, start='helper', boot='private-boot')]
                row['rms']['old'] = [dict(actor, ppid=2000+i, opened=[])]
            if 18 <= i < 23:
                row['helpers']['original'] = [dict(pid=3000+i, start='original-helper', boot='private-boot')]
                row['rms']['original'] = [dict(pid=4000+i, start='original-rm', boot='private-boot',
                    ppid=3000+i, opened=[str(self.target/'scope'/'busy')])]
            result.append(row)
        return result

    def inspect(self, old, original, nodes, known=None):
        self.assertEqual(Path(old), self.old)
        self.assertEqual(Path(original), self.original)
        self.assertEqual(nodes, self.old_nodes)
        self.inspections.append(dict(nodes=list(nodes), known=copy.deepcopy(known), draining=self.draining))
        if self.draining:
            self.drain_calls += 1
        if self.inspect_hook:
            return self.inspect_hook(known)
        return self.rows(old=not self.draining)

    def preflight_nodes(self, script, nodes):
        self.assertEqual(Path(script), self.base/'rm24.py')
        self.assertEqual(nodes, self.extras)
        return [dict(node=n, host=self.hosts[n]) for n in nodes]

    def fake_prepare(self, run, per_node, **kwargs):
        self.assertTrue(self.draining)
        self.assertGreaterEqual(self.drain_calls, 2)
        self.assertEqual(per_node, 4)
        self.prepares.append((Path(run), kwargs))
        Path(run).mkdir()
        (Path(run)/'rm24.py').write_text('# new private fixture, never executed\n')
        self.write(Path(run)/'status.json', dict(state='rm_running', updated=self.clock_now))

    def fake_start(self, args, **kwargs):
        self.assertTrue(self.prepares)
        self.assertEqual(args[-3:-1], ['start', '--run'])
        self.assertTrue(Path(args[-1]).is_relative_to(self.runs))
        self.starts.append(args)
        return types.SimpleNamespace(returncode=0)

    def prepare(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return upgrade.prepare()

    def apply(self, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return upgrade.apply(**kwargs)

    def plan(self):
        pointer = upgrade.rm24.load(upgrade.POINTER)
        work = Path(pointer['work'])
        return work, upgrade.rm24.load(work/'plan.json')

    def assert_original_untouched(self):
        self.assertFalse((self.original/'STOP').exists())
        self.assertNotIn(self.original/'STOP', self.atomic_paths)
        self.assertEqual(upgrade.rm24.load(self.original/'controller.json'), self.original_actor)

    def test_prepare_is_readonly_to_runs_and_covers_all_dynamic_members(self):
        result = self.prepare()
        _, plan = self.plan()
        self.assertFalse(result['started'])
        self.assertEqual(plan['dynamic_members'], sorted(self.old_nodes[:18]))
        self.assertEqual(plan['old_nodes'], self.old_nodes)
        self.assertEqual(self.inspections[0]['nodes'], self.old_nodes)
        self.assertEqual(len(plan['execution_nodes']), 35)
        self.assertTrue(set(self.extras).issubset(plan['execution_nodes']))
        self.assertFalse(set(self.old_nodes[18:23]) & set(plan['execution_nodes']))
        self.assertEqual({s['rel'] for s in plan['scopes']}, {'scope', 'missed'})
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_apply_only_stops_supplement_and_waits_two_full_empty_observations(self):
        self.prepare()
        result = self.apply()
        self.assertTrue(result['ready'])
        self.assertEqual(self.drain_calls, 2)
        self.assertEqual(len(self.starts), 1)
        self.assertEqual(result['execution_nodes'], 35)
        self.assertEqual(result['rm_max'], 140)
        self.worker_ready.assert_called_once()
        _, options = self.prepares[0]
        self.assertTrue(options['runtime_join'])
        self.assertEqual(options['supplement_of'], self.original)
        self.assertEqual(options['busy_paths'], [str(self.target/'scope'/'busy')])
        self.assertEqual(upgrade.rm24.load(upgrade.tailboost.POINTER)['run'], result['new'])
        self.assert_original_untouched()

    def test_another_plan_run_claim_blocks_stop_and_replacement(self):
        self.prepare()
        upgrade.claim_upgrade(self.old, self.runs/'another-private-plan', 'private-sha')
        with self.assertRaises(FileExistsError):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_different_plans_cannot_both_claim_one_old_run(self):
        import concurrent.futures
        def claim(index):
            try:
                upgrade.claim_upgrade(self.old, self.runs/('private-plan-'+str(index)), str(index))
                return True
            except FileExistsError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, range(2)))
        self.assertEqual(results.count(True), 1)
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])

    def test_no_all_worker_readiness_means_no_ready_marker_or_pointer_switch(self):
        self.prepare()
        self.worker_ready.side_effect = RuntimeError('worker readiness unconfirmed')
        with self.assertRaisesRegex(RuntimeError, 'worker readiness'):
            self.apply()
        work, _ = self.plan()
        self.assertFalse((work/'ready.json').exists())
        self.assertTrue((work/'failure.json').exists())
        self.assertEqual(upgrade.rm24.load(upgrade.tailboost.POINTER)['run'], str(self.old))
        self.assert_original_untouched()

    def test_late_dynamic_member_and_its_orphan_prevent_premature_start(self):
        self.prepare()
        late = dict(pid=8888, start='late-rm', boot='private-boot', ppid=9999, opened=[])
        def inspection(known):
            rows = self.rows(old=not self.draining)
            if self.draining and self.drain_calls <= 2:
                rows[-1]['rms']['old'] = [late]
                if self.drain_calls == 1:
                    rows[-1]['helpers']['old'] = [dict(pid=9999, start='late-helper', boot='private-boot')]
                else:
                    self.assertIn(late, known[self.old_nodes[-1]]['old'])
            return rows
        self.inspect_hook = inspection
        self.apply()
        self.assertEqual(self.drain_calls, 4)
        self.assertEqual(len(self.starts), 1)
        self.assert_original_untouched()

    def test_remaining_owned_orphan_times_out_without_replacement(self):
        self.prepare()
        def inspection(_):
            rows = self.rows(old=not self.draining)
            if self.draining:
                rows[0]['rms']['old'] = [dict(pid=1000, start='0', boot='private-boot', ppid=1, opened=[])]
            return rows
        self.inspect_hook = inspection
        with self.assertRaisesRegex(RuntimeError, 'exit unconfirmed'):
            self.apply(timeout=1, interval=1)
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_unknown_rm_before_stop_refuses_apply_without_mutation(self):
        self.prepare()
        def inspection(_):
            rows = self.rows()
            rows[-1]['rms']['unknown'] = [dict(pid=7, start='unknown', boot='private-boot')]
            return rows
        self.inspect_hook = inspection
        with self.assertRaisesRegex(ValueError, 'unowned'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_unknown_rm_after_stop_blocks_replacement(self):
        self.prepare()
        def inspection(_):
            rows = self.rows(old=not self.draining)
            if self.draining:
                rows[-1]['rms']['unknown'] = [dict(pid=7, start='unknown', boot='private-boot')]
            return rows
        self.inspect_hook = inspection
        with self.assertRaisesRegex(ValueError, 'unowned'):
            self.apply()
        self.assertTrue((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_ssh_failure_before_stop_leaves_both_runs_untouched(self):
        self.prepare()
        self.inspect_hook = mock.Mock(side_effect=RuntimeError('SSH fixture failure'))
        with self.assertRaisesRegex(RuntimeError, 'SSH'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_ssh_failure_during_confirmation_does_not_start_replacement(self):
        self.prepare()
        def inspection(_):
            if self.draining:
                raise RuntimeError('SSH fixture failure')
            return self.rows()
        self.inspect_hook = inspection
        with self.assertRaisesRegex(RuntimeError, 'SSH'):
            self.apply()
        self.assertEqual(self.starts, [])
        self.assert_original_untouched()

    def test_extra_preflight_failure_occurs_before_stop(self):
        self.prepare()
        self.preflight.side_effect = RuntimeError('extra preflight fixture failure')
        with self.assertRaisesRegex(RuntimeError, 'extra preflight'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])

    def test_source_hash_change_blocks_apply_before_stop(self):
        self.prepare()
        (self.base/'rm24.py').write_text('# changed private source\n')
        with self.assertRaisesRegex(ValueError, 'source changed'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())
        self.assertEqual(self.starts, [])

    def test_old_frozen_hash_change_blocks_apply_before_stop(self):
        self.prepare()
        (self.old/'rm24.py').write_text('# changed frozen worker\n')
        with self.assertRaisesRegex(ValueError, 'frozen'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())

    def test_config_and_plan_hash_changes_are_rejected(self):
        self.prepare()
        work, plan = self.plan()
        plan['extras'] = []
        self.write(work/'plan.json', plan)
        with self.assertRaisesRegex(ValueError, 'plan changed'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())

    def test_actual_private_root_inode_guard_rejects_wrong_inode(self):
        with mock.patch.object(upgrade.rm24, 'ROOT_INODE', self.inode+1):
            with self.assertRaisesRegex(ValueError, 'root inode'):
                upgrade.root_check()
        self.assertFalse((self.old/'STOP').exists())

    def test_stop_and_stale_status_refuse_prepare(self):
        status = upgrade.rm24.load(self.old/'status.json')
        status['updated'] -= 31
        self.write(self.old/'status.json', status)
        with self.assertRaisesRegex(ValueError, 'stale'):
            self.prepare()
        status['updated'] = self.clock_now
        self.write(self.old/'status.json', status)
        self.write(self.old/'STOP', dict(reason='fixture'))
        with self.assertRaisesRegex(ValueError, 'stopped'):
            self.prepare()
        self.assertEqual(self.starts, [])

    def test_controller_change_before_apply_does_not_publish_stop(self):
        self.prepare()
        self.write(self.old/'controller.json', dict(self.actor, start='changed'))
        with self.assertRaisesRegex(ValueError, 'controller'):
            self.apply()
        self.assertFalse((self.old/'STOP').exists())

    def test_readiness_failure_keeps_replacement_record_without_false_ready(self):
        self.prepare()
        self.ready.side_effect = RuntimeError('readiness fixture failure')
        with self.assertRaisesRegex(RuntimeError, 'readiness'):
            self.apply()
        work, _ = self.plan()
        self.assertTrue((work/'replacement.json').exists())
        self.assertTrue((work/'failure.json').exists())
        self.assertFalse((work/'ready.json').exists())
        self.assertEqual(upgrade.rm24.load(upgrade.tailboost.POINTER)['run'], str(self.old))
        self.assertEqual(len(self.starts), 1)
        self.assert_original_untouched()

    def test_known_identity_merge_preserves_orphans_and_distinguishes_pid_reuse(self):
        rows = self.rows()
        known = upgrade.merge_known({}, rows)
        prior = copy.deepcopy(known[self.old_nodes[0]]['old'])
        rows[0]['rms']['old'] = [dict(prior[0], start='new-start')]
        upgrade.merge_known(known, rows)
        self.assertEqual(len(known[self.old_nodes[0]]['old']), 2)
        self.assertIn(prior[0], known[self.old_nodes[0]]['old'])

    def test_remote_inspection_classifies_private_proc_and_external_absolute_operands(self):
        proc = self.base/'private-proc'
        proc.mkdir()
        boot = self.base/'private-boot'
        boot.write_text('private-boot')
        module = self.base/'private-inspection-module.py'
        module.write_text('from rm24 import rm_touches_target\nTARGET='+repr(str(self.target))+
                          '\ndef node_check(node):\n return dict(node=node,host='+
                          repr(self.hosts['master-0'])+')\n')
        def process(pid, ppid, start, args, cwd):
            directory = proc/str(pid)
            directory.mkdir(exist_ok=True)
            (directory/'cmdline').write_bytes(b'\0'.join(a.encode() for a in args)+b'\0')
            (directory/'stat').write_text(str(pid)+' (private) '+
                ' '.join(['S', str(ppid)]+['0']*17+[start]))
            (directory/'cwd').symlink_to(cwd)
            (directory/'fd').mkdir()
        for pid, run in ((1001, self.old), (2001, self.original)):
            process(pid, 1, 'helper', ['python3', str(run/'rm24.py'), '_worker',
                    '--run', str(run), '--node', 'master-0'], self.base)
        for pid, ppid in ((1002, 1001), (2002, 2001), (1003, 1)):
            process(pid, ppid, 'rm-start', ['/usr/bin/rm', '-rf', '--', 'leaf'], self.target/'scope')
        process(4001, 1, 'unknown', ['/usr/bin/rm', '-rf', str(self.target/'scope')], self.base)
        process(4002, 1, 'unrelated', ['/usr/bin/rm', '-rf', str(self.base/'unrelated')], self.base)
        script = upgrade.INSPECT.replace("pathlib.Path('/proc/sys/kernel/random/boot_id')",
                                         'pathlib.Path('+repr(str(boot))+')')
        script = script.replace("pathlib.Path('/proc')", 'pathlib.Path('+repr(str(proc))+')')
        self.assertNotIn("Path('/proc", script)
        namespace = dict(MODULE=str(module), MODULE_SHA=upgrade.digest(module), NODE='master-0',
            OLD=str(self.old), ORIGINAL=str(self.original), KNOWN=dict(old=[
                dict(pid=1003, start='rm-start', boot='private-boot')]))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(script, namespace)
        row = json.loads(output.getvalue())
        self.assertEqual({a['pid'] for a in row['rms']['old']}, {1002, 1003})
        self.assertEqual({a['pid'] for a in row['rms']['original']}, {2002})
        self.assertEqual({a['pid'] for a in row['rms']['unknown']}, {4001})
        (proc/'1003/stat').write_text('1003 (private) '+' '.join(['S', '1']+['0']*17+['reused']))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(script, namespace)
        row = json.loads(output.getvalue())
        self.assertEqual({a['pid'] for a in row['rms']['unknown']}, {1003, 4001})


if __name__ == '__main__':
    unittest.main()
