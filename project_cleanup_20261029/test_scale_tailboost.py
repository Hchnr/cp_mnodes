import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

import scale_tailboost as scale


class ScaleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='tail-scale-private-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.workspace = self.base/'workspace'
        self.original = self.workspace/'runs/prepared-v3'
        self.old = self.workspace/'runs/tail-boost-private'
        self.original.mkdir(parents=True)
        self.old.mkdir()
        self.target = self.base/'target'
        (self.target/'tree/branch').mkdir(parents=True)
        (self.target/'tree/branch/keep').write_text('private fixture')
        (self.target/'failed').mkdir()
        (self.target/'failed/keep').write_text('private fixture')
        self.actor = dict(pid=123456789, start='private-fixture', boot='private-boot')
        self.cfg = dict(target=str(self.target), root_inode=self.target.stat().st_ino,
                        supplement_of=str(self.original), execution_nodes=['worker-0'], per_node=2,
                        tail_scopes=[dict(rel='tree', ino=(self.target/'tree').stat().st_ino)],
                        sha256=hashlib.sha256(Path(scale.rm24.__file__).read_bytes()).hexdigest())
        shutil.copyfile(scale.rm24.__file__, self.old/'rm24.py')
        scale.rm24.atomic(self.old/'config.json', self.cfg)
        scale.rm24.atomic(self.old/'controller.json', self.actor)
        scale.rm24.atomic(self.old/'status.json', dict(state='rm_running', updated=time.time()))
        self.current = self.workspace/'runs/tail-boost-current.json'
        scale.rm24.atomic(self.current, dict(run=str(self.old), original=str(self.original)))
        self.pointer = self.workspace/'runs/tail-scale-current.json'
        self.context = patch.multiple(scale, HERE=self.workspace, POINTER=self.pointer)
        self.context.start()
        self.addCleanup(self.context.stop)
        for obj, field, value in [(scale.rm24, 'RUN', self.original),
                                  (scale.rm24, 'TARGET', str(self.target)),
                                  (scale.rm24, 'ROOT_INODE', self.target.stat().st_ino),
                                  (scale.tailboost, 'POINTER', self.current)]:
            p = patch.object(obj, field, value)
            p.start()
            self.addCleanup(p.stop)
        # Sources are private copies too; no fixture source/control read or
        # process action is directed at the real deletion run.
        for name in ('rm24.py', 'boost.py', 'tailboost.py', 'scale_tailboost.py', 'join_tailboost.py'):
            shutil.copyfile(Path(scale.__file__).parent/name, self.workspace/name)
        self.rows = [dict(node='worker-0', helpers=[self.actor],
                         rms=[dict(pid=987654321, start='child', boot='private-boot', owned=True)])]

    def prepare(self):
        with patch.object(scale, 'alive', return_value=True), \
                patch.object(scale, 'inspect_nodes', return_value=self.rows):
            scale.prepare(4)
        return Path(scale.rm24.load(self.pointer)['work'])

    def test_prepare_does_not_stop_or_start_and_keeps_existing_pointer(self):
        work = self.prepare()
        self.assertEqual(scale.rm24.load(work/'plan.json')['per_node'], 4)
        self.assertFalse((self.old/'STOP').exists())
        self.assertFalse((work/'apply-claim.json').exists())
        self.assertEqual(scale.rm24.load(self.current)['run'], str(self.old))
        self.assertEqual((self.target/'tree/branch/keep').read_text(), 'private fixture')

    def test_unknown_rm_blocks_prepare_before_stop(self):
        rows = [dict(node='worker-0', helpers=[], rms=[dict(owned=False)])]
        with patch.object(scale, 'alive', return_value=True), \
                patch.object(scale, 'inspect_nodes', return_value=rows):
            with self.assertRaisesRegex(ValueError, 'another rm'):
                scale.prepare(4)
        self.assertFalse((self.old/'STOP').exists())
        self.assertFalse(self.pointer.exists())

    def test_inherited_and_failed_scope_roots_are_both_retained(self):
        task = dict(parent='', ino=self.target.stat().st_ino, names=['failed'])
        (self.original/'failed-jobs.jsonl').write_text(json.dumps(dict(task=task))+'\n')
        scopes = scale.scopes_for(self.cfg, self.original)
        self.assertEqual({s['rel'] for s in scopes}, {'tree', 'failed'})
        self.assertTrue((self.target/'failed/keep').exists())

    def test_ssh_inspection_failure_does_not_publish_stop(self):
        work = self.prepare()
        with patch.object(scale, 'inspect_nodes', side_effect=RuntimeError('SSH unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'SSH unavailable'):
                scale.apply()
        self.assertFalse((self.old/'STOP').exists())
        self.assertFalse((work/'apply-claim.json').exists())

    def test_apply_stops_only_bound_tail_then_requires_strict_prepare_and_readiness(self):
        work = self.prepare()
        empty = [dict(node='worker-0', helpers=[], rms=[])]
        scopes = self.cfg['tail_scopes']
        def prepared(run, per_node, **options):
            self.assertTrue((self.old/'STOP').exists())
            self.assertFalse((self.original/'STOP').exists())
            self.assertEqual(per_node, 4)
            self.assertEqual(options['execution_nodes'], ['worker-0'])
            self.assertEqual(options['tail_scopes'], scopes)
            run.mkdir()
        with patch.object(scale, 'inspect_nodes', side_effect=[self.rows, empty]), \
                patch.object(scale, 'alive', return_value=False), \
                patch.object(scale.boost, 'observe', return_value=[dict(opened=[])]), \
                patch.object(scale.rm24, 'prepare', side_effect=prepared) as prep, \
                patch.object(scale.subprocess, 'run') as launch, \
                patch.object(scale, 'wait_ready', return_value=dict(state='rm_running')) as ready:
            scale.apply()
        self.assertEqual(prep.call_count, 1)
        self.assertEqual(launch.call_count, 1)
        args = launch.call_args.args[0]
        self.assertIn('start', args)
        self.assertNotEqual(Path(args[4]).parent, self.old)
        ready.assert_called_once()
        self.assertTrue((work/'ready.json').exists())
        self.assertFalse((self.original/'STOP').exists())
        self.assertNotEqual(scale.rm24.load(self.current)['run'], str(self.old))

    def test_readiness_failure_never_records_ready(self):
        work = self.prepare()
        empty = [dict(node='worker-0', helpers=[], rms=[])]
        with patch.object(scale, 'inspect_nodes', side_effect=[self.rows, empty]), \
                patch.object(scale, 'alive', return_value=False), \
                patch.object(scale.boost, 'observe', return_value=[]), \
                patch.object(scale.rm24, 'prepare'), patch.object(scale.subprocess, 'run'), \
                patch.object(scale, 'wait_ready', side_effect=RuntimeError('readiness failed')):
            with self.assertRaisesRegex(RuntimeError, 'readiness failed'):
                scale.apply()
        self.assertTrue((work/'replacement.json').exists())
        self.assertFalse((work/'ready.json').exists())

    def test_wait_ready_rejects_failed_background_controller(self):
        run = self.base/'readiness'
        run.mkdir()
        scale.rm24.atomic(run/'controller.json', self.actor)
        scale.rm24.atomic(run/'status.json', dict(state='stopped_or_error', updated=time.time()))
        with patch.object(scale, 'alive', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'failed readiness'):
                scale.wait_ready(run)

    def test_orphaned_owned_rm_blocks_replacement_until_it_exits(self):
        self.prepare()
        orphan = [dict(node='worker-0', helpers=[], rms=list(self.rows[0]['rms']))]
        empty = [dict(node='worker-0', helpers=[], rms=[])]
        with patch.object(scale, 'inspect_nodes', side_effect=[self.rows, orphan, empty]) as inspect, \
                patch.object(scale, 'alive', return_value=False), \
                patch.object(scale.time, 'sleep'), \
                patch.object(scale.boost, 'observe', return_value=[]), \
                patch.object(scale.rm24, 'prepare'), \
                patch.object(scale.subprocess, 'run') as launch, \
                patch.object(scale, 'wait_ready', return_value=dict(state='rm_running')):
            scale.apply()
        self.assertEqual(inspect.call_count, 3)
        known = inspect.call_args_list[2].args[2]
        self.assertEqual(known[0]['rms'][0]['pid'], 987654321)
        launch.assert_called_once()

    def test_unknown_surviving_rm_after_stop_prevents_launch(self):
        self.prepare()
        unknown = [dict(node='worker-0', helpers=[],
                        rms=[dict(pid=123, start='unknown', boot='private-boot', owned=False)])]
        with patch.object(scale, 'inspect_nodes', side_effect=[self.rows, unknown]), \
                patch.object(scale, 'alive', return_value=False), \
                patch.object(scale.time, 'monotonic', side_effect=[0, 151]), \
                patch.object(scale.rm24, 'prepare') as prep, \
                patch.object(scale.subprocess, 'run') as launch:
            with self.assertRaisesRegex(RuntimeError, 'replacement NOT started'):
                scale.apply()
        self.assertTrue((self.old/'STOP').exists())
        self.assertFalse((self.original/'STOP').exists())
        prep.assert_not_called()
        launch.assert_not_called()

    def test_changed_scope_inode_is_rejected_without_removing_either_directory(self):
        (self.target/'tree').rename(self.target/'previous-tree')
        (self.target/'tree').mkdir()
        with self.assertRaisesRegex(ValueError, 'inode changed'):
            scale.scopes_for(self.cfg, self.original)
        self.assertTrue((self.target/'tree').is_dir())
        self.assertEqual((self.target/'previous-tree/branch/keep').read_text(), 'private fixture')

    def test_runtime_upgrade_prepares_same_cap_and_only_read_only_trial_check(self):
        with patch.object(scale, 'alive', return_value=True), \
                patch.object(scale, 'inspect_nodes', return_value=self.rows), \
                patch.object(scale.rm24, 'preflight', return_value=[dict(node='worker-3')]) as check:
            scale.prepare(2, runtime_join=True, trial_node='worker-3')
        work = Path(scale.rm24.load(self.pointer)['work'])
        plan = scale.rm24.load(work/'plan.json')
        self.assertTrue(plan['runtime_join'])
        self.assertEqual(plan['per_node'], 2)
        self.assertEqual(plan['trial_node'], 'worker-3')
        check.assert_called_once_with(self.workspace/'rm24.py', ['worker-3'])
        self.assertFalse((self.old/'STOP').exists())
        self.assertFalse((work/'apply-claim.json').exists())

    def test_runtime_enabled_run_cannot_use_static_membership_stop_start_scale(self):
        self.cfg['runtime_join'] = True
        scale.rm24.atomic(self.old/'config.json', self.cfg)
        with patch.object(scale, 'inspect_nodes') as inspect:
            with self.assertRaisesRegex(ValueError, 'online join only'):
                scale.prepare(4)
        inspect.assert_not_called()
        self.assertFalse((self.old/'STOP').exists())
        self.assertFalse(self.pointer.exists())

    def test_runtime_upgrade_only_requests_trial_after_base_is_ready(self):
        import join_tailboost
        with patch.object(scale, 'alive', return_value=True), \
                patch.object(scale, 'inspect_nodes', return_value=self.rows), \
                patch.object(scale.rm24, 'preflight', return_value=[dict(node='worker-3')]):
            scale.prepare(2, runtime_join=True, trial_node='worker-3')
        work = Path(scale.rm24.load(self.pointer)['work'])
        empty = [dict(node='worker-0', helpers=[], rms=[])]
        events = []
        def prepared(run, per_node, **options):
            self.assertEqual(per_node, 2)
            self.assertIs(options['runtime_join'], True)
            self.assertEqual(options['execution_nodes'], ['worker-0'])
            events.append('prepare')
        def ready(run):
            events.append('ready')
            return dict(state='rm_running')
        def join(run, node):
            self.assertEqual(events, ['prepare', 'ready'])
            self.assertEqual(node, 'worker-3')
            events.append('join')
            return dict(state='accepted', node=node)
        with patch.object(scale, 'inspect_nodes', side_effect=[self.rows, empty]), \
                patch.object(scale, 'alive', return_value=False), \
                patch.object(scale.boost, 'observe', return_value=[]), \
                patch.object(scale.rm24, 'prepare', side_effect=prepared), \
                patch.object(scale.subprocess, 'run'), \
                patch.object(scale, 'wait_ready', side_effect=ready), \
                patch.object(join_tailboost, 'request_join', side_effect=join):
            scale.apply()
        self.assertEqual(events, ['prepare', 'ready', 'join'])
        self.assertTrue((work/'trial-join.json').exists())
        self.assertFalse((self.original/'STOP').exists())

    def test_trial_failure_explicitly_preserves_ready_baseline(self):
        import join_tailboost
        with patch.object(scale, 'alive', return_value=True), \
                patch.object(scale, 'inspect_nodes', return_value=self.rows), \
                patch.object(scale.rm24, 'preflight', return_value=[]):
            scale.prepare(2, runtime_join=True, trial_node='worker-3')
        work = Path(scale.rm24.load(self.pointer)['work'])
        empty = [dict(node='worker-0', helpers=[], rms=[])]
        with patch.object(scale, 'inspect_nodes', side_effect=[self.rows, empty]), \
                patch.object(scale, 'alive', return_value=False), \
                patch.object(scale.boost, 'observe', return_value=[]), \
                patch.object(scale.rm24, 'prepare'), patch.object(scale.subprocess, 'run'), \
                patch.object(scale, 'wait_ready', return_value=dict(state='rm_running')), \
                patch.object(join_tailboost, 'request_join', side_effect=RuntimeError('trial preflight rejected')):
            with self.assertRaisesRegex(RuntimeError, 'baseline replacement has already been launched'):
                scale.apply()
        new_run = Path(scale.rm24.load(self.current)['run'])
        self.assertNotEqual(new_run, self.old)
        self.assertFalse((new_run/'STOP').exists())
        self.assertTrue((work/'ready.json').exists())
        self.assertTrue((work/'trial-join-error.json').exists())
        self.assertFalse((self.original/'STOP').exists())


if __name__ == '__main__':
    unittest.main()
