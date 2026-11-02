"""Mocked 40-to-40 policy deployment; all writes stay in legacy's private fixture."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_upgrade40_tailboost as legacy
import upgrade40_tailboost as upgrade


class ParentDeployTests(unittest.TestCase):
    def setUp(self):
        f = self.fixture = legacy.Upgrade40Tests(methodName='runTest')
        f.setUp()
        self.addCleanup(f.doCleanups)
        self.all_nodes = list(f.hosts)
        self.extra_nodes = list(f.extras)
        f.old_nodes, f.extras = self.all_nodes, []
        f.cfg.update(nodes=self.all_nodes, node_hosts=f.hosts)
        f.write(f.old/'config.json', f.cfg)
        f.write(f.old/'status.json', dict(state='rm_running', updated=f.clock_now, errors=[],
            runtime_join=dict(accepted=self.all_nodes[12:18]+self.extra_nodes, starting=['worker-22'])))
        f.patch(upgrade, 'PARENT_POINTER', new=f.runs/'parent-deploy-current.json')
        for name in upgrade.PARENT_HELPERS:
            (f.base/name).write_text('# private helper fixture '+name+'\n')
        old_rows = f.rows
        def rows(old=True):
            values = old_rows(old)
            for row in values:
                row['boot'] = 'private-boot'
                if row['node'] != 'worker-21':
                    row['helpers']['original'] = []
                    row['rms']['original'] = []
            return values
        f.rows = rows
        legacy_prepare = f.fake_prepare
        def frozen_prepare(run, per_node, **options):
            legacy_prepare(run, per_node, **options)
            run = Path(run)
            (run/'rm24.py').write_bytes((f.base/'rm24.py').read_bytes())
            for name in upgrade.PARENT_HELPERS:
                (run/name).write_bytes((f.base/name).read_bytes())
            cfg = dict(target=str(f.target), root_inode=f.inode, nodes=self.all_nodes,
                node_hosts=f.hosts, per_node=4, sha256=upgrade.digest(run/'rm24.py'),
                execution_nodes=options['execution_nodes'], runtime_join=True,
                supplement_of=str(f.original), parent_policy=options['parent_policy'],
                parent_helpers={name: upgrade.digest(run/name) for name in upgrade.PARENT_HELPERS})
            f.write(run/'config.json', cfg)
        self.prepare_mock = f.patch(upgrade.rm24, 'prepare', side_effect=frozen_prepare)
        f.worker_ready.side_effect = self.readiness

    def readiness(self, run, nodes):
        f = self.fixture
        cfg = upgrade.rm24.load(Path(run)/'config.json')
        status = dict(state='rm_running', updated=f.clock_now+f.elapsed, errors=[],
            parent_scheduler=dict(limit=cfg['parent_policy']['limit'], pending=100,
                                  active_leases=8, claims=12, releases=4),
            parent_tuning=dict(limit=cfg['parent_policy']['limit'], generation=0, error=None))
        return dict(state='workers_ready', verified_nodes=len(nodes), nodes=nodes, status=status,
                    proofs=[dict(node=node, boot=cfg['parent_policy']['node_boots'][node]) for node in nodes])

    def prepare(self, limit='2'):
        with contextlib.redirect_stdout(io.StringIO()):
            return upgrade.prepare(parent_limit=limit)

    def apply(self, limit='2', **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return upgrade.apply(parent_limit=limit, **kwargs)

    def plan(self):
        pointer = upgrade.rm24.load(upgrade.PARENT_POINTER)
        work = Path(pointer['work'])
        return work, upgrade.rm24.load(work/'plan.json')

    def test_parent_prepare_has_distinct_pointer_full40_boots_and_all_dynamic_members(self):
        f = self.fixture
        result = self.prepare()
        work, plan = self.plan()
        self.assertFalse(result['started'])
        self.assertEqual(plan['mode'], 'parent-policy-v1')
        self.assertEqual(plan['parent_limit'], 2)
        self.assertEqual(plan['old_nodes'], self.all_nodes)
        self.assertEqual(plan['extras'], [])
        self.assertEqual(set(plan['node_boots']), set(self.all_nodes))
        self.assertTrue(set(self.extra_nodes).issubset(plan['dynamic_members']))
        self.assertIn('worker-22', plan['dynamic_members'])
        self.assertEqual(len(plan['execution_nodes']), 39)
        self.assertNotIn('worker-21', plan['execution_nodes'])
        self.assertTrue(all(row['nodes'] == self.all_nodes for row in f.inspections))
        self.assertTrue((work/'previous-status.json').exists())
        self.assertFalse(upgrade.POINTER.exists())
        self.assertFalse((f.old/'STOP').exists())
        f.assert_original_untouched()

    def test_apply_preserves_original_and_waits_twice_then_passes_frozen_cap_policy(self):
        f = self.fixture
        self.prepare()
        result = self.apply()
        self.assertTrue(result['ready'])
        self.assertEqual(result['execution_nodes'], 39)
        self.assertEqual(result['rm_max'], 156)
        self.assertGreaterEqual(f.drain_calls, 3)  # two empty + immediately before start
        self.assertEqual(len(f.starts), 1)
        new_run, options = f.prepares[0]
        self.assertEqual(options['parent_policy'], dict(limit=2, node_boots=dict.fromkeys(self.all_nodes, 'private-boot')))
        self.assertTrue(options['runtime_join'])
        work, plan = self.plan()
        replacement = upgrade.rm24.load(work/'replacement.json')
        self.assertEqual(replacement['previous'], str(f.old))
        self.assertEqual(upgrade.rm24.load(upgrade.tailboost.POINTER)['run'], result['new'])
        f.assert_original_untouched()

    def test_off_mode_is_explicit_and_keeps_policy_enabled_with_no_quota(self):
        self.prepare('off')
        _, plan = self.plan()
        self.assertEqual(plan['mode'], upgrade.PARENT_MODE)
        self.assertIsNone(plan['parent_limit'])
        result = self.apply('off')
        self.assertIsNone(result['parent_limit'])
        self.assertIsNone(self.fixture.prepares[0][1]['parent_policy']['limit'])

    def test_changed_mode_or_limit_cannot_apply_another_plan(self):
        f = self.fixture
        self.prepare('2')
        with self.assertRaisesRegex(ValueError, 'mode/limit differs'):
            self.apply('1')
        with self.assertRaises(FileNotFoundError):
            upgrade.apply()
        self.assertFalse((f.old/'STOP').exists())

    def test_boot_change_before_stop_refuses_without_touching_original(self):
        f = self.fixture
        self.prepare()
        def changed(known):
            rows = f.rows(old=True)
            rows[-1]['boot'] = 'replaced-container-boot'
            return rows
        f.inspect_hook = changed
        with self.assertRaisesRegex(ValueError, 'boot identity changed'):
            self.apply()
        self.assertFalse((f.old/'STOP').exists())
        self.assertEqual(f.starts, [])
        f.assert_original_untouched()

    def test_boot_change_during_prepare_is_refused(self):
        f = self.fixture
        def changed(known):
            rows = f.rows(old=True)
            if len(f.inspections) > 1:
                rows[-1]['boot'] = 'changed-after-first-observation'
            return rows
        f.inspect_hook = changed
        with self.assertRaisesRegex(ValueError, 'boot identity changed'):
            self.prepare()
        self.assertFalse((f.old/'STOP').exists())

    def test_unknown_rm_and_source_helper_change_refuse_before_stop(self):
        f = self.fixture
        self.prepare()
        (f.base/'parent_scheduler.py').write_text('# changed source helper\n')
        with self.assertRaisesRegex(ValueError, 'prepared source changed'):
            self.apply()
        self.assertFalse((f.old/'STOP').exists())
        self.assertEqual(f.starts, [])

    def test_unknown_absolute_scope_rm_refuses_before_stop(self):
        f = self.fixture
        self.prepare()
        def unknown(known):
            rows = f.rows(old=True)
            rows[-1]['rms']['unknown'] = [dict(pid=800, start='8', boot='private-boot')]
            return rows
        f.inspect_hook = unknown
        with self.assertRaisesRegex(ValueError, 'unowned target rm'):
            self.apply()
        self.assertFalse((f.old/'STOP').exists())

    def test_old_frozen_helpers_are_verified_when_already_present(self):
        f = self.fixture
        for name in upgrade.PARENT_HELPERS:
            (f.old/name).write_bytes((f.base/name).read_bytes())
        f.cfg['parent_helpers'] = {name: upgrade.digest(f.old/name) for name in upgrade.PARENT_HELPERS}
        f.cfg['parent_policy'] = dict(limit=None, node_boots=dict.fromkeys(self.all_nodes, 'private-boot'))
        f.write(f.old/'config.json', f.cfg)
        (f.old/'parent_tuning.py').write_text('# tampered old helper\n')
        with self.assertRaisesRegex(ValueError, 'old frozen parent helper changed'):
            self.prepare()
        self.assertFalse((f.old/'STOP').exists())

    def test_old_fatal_status_refuses_prepare_but_rc1_counts_are_allowed(self):
        f = self.fixture
        status = upgrade.rm24.load(f.old/'status.json')
        status['errors'] = ['private fatal']
        f.write(f.old/'status.json', status)
        with self.assertRaisesRegex(ValueError, 'stopped, stale'):
            self.prepare()
        status['errors'], status['failed_rm_events'] = [], 99
        f.write(f.old/'status.json', status)
        self.assertFalse(self.prepare()['started'])

    def test_orphan_in_dynamic_extra_blocks_both_empty_confirmations(self):
        f = self.fixture
        self.prepare()
        orphan = dict(pid=888, start='8', boot='private-boot', ppid=1, opened=[])
        def orphaned(known):
            rows = f.rows(old=not f.draining)
            if f.draining and f.drain_calls <= 2:
                rows[-1]['rms']['old'] = [orphan]
                if f.drain_calls == 2:
                    self.assertIn(orphan, known[self.all_nodes[-1]]['old'])
            return rows
        f.inspect_hook = orphaned
        self.apply()
        self.assertGreaterEqual(f.drain_calls, 5)
        self.assertEqual(len(f.starts), 1)

    def test_bad_frozen_new_policy_never_starts_replacement(self):
        f = self.fixture
        self.prepare()
        original_prepare = self.prepare_mock.side_effect
        def tampered(run, per_node, **options):
            original_prepare(run, per_node, **options)
            cfg = upgrade.rm24.load(Path(run)/'config.json')
            cfg['parent_policy']['limit'] = 1
            f.write(Path(run)/'config.json', cfg)
        self.prepare_mock.side_effect = tampered
        with self.assertRaisesRegex(ValueError, 'replacement frozen policy'):
            self.apply()
        self.assertEqual(f.starts, [])
        self.assertTrue((self.plan()[0]/'failure.json').exists())
        f.assert_original_untouched()

    def test_helper_source_changed_during_freeze_blocks_start(self):
        f = self.fixture
        self.prepare()
        original_prepare = self.prepare_mock.side_effect
        def changed(run, per_node, **options):
            original_prepare(run, per_node, **options)
            (f.base/'parent_tuning.py').write_text('# source changed during freeze\n')
        self.prepare_mock.side_effect = changed
        with self.assertRaisesRegex(ValueError, 'prepared source changed'):
            self.apply()
        self.assertEqual(f.starts, [])

    def test_incomplete_boot_proofs_or_tuner_error_never_publish_ready_pointer(self):
        f = self.fixture
        self.prepare()
        def incomplete(run, nodes):
            result = self.readiness(run, nodes)
            result['proofs'][-1]['boot'] = 'wrong-boot'
            return result
        f.worker_ready.side_effect = incomplete
        with self.assertRaisesRegex(RuntimeError, 'worker boot'):
            self.apply()
        work, _ = self.plan()
        self.assertFalse((work/'ready.json').exists())
        self.assertEqual(upgrade.rm24.load(upgrade.tailboost.POINTER)['run'], str(f.old))
        f.assert_original_untouched()

    def test_terminal_complete_cannot_hide_pending_or_leased_work(self):
        self.prepare()
        _, plan = self.plan()
        nodes = plan['execution_nodes']
        fake = dict(state='terminal_complete', status=dict(state='supplement_complete',
            parent_scheduler=dict(limit=2, pending=1, active_leases=0, claims=5, releases=5),
            parent_tuning=dict(limit=2, generation=0, error=None)))
        with self.assertRaisesRegex(RuntimeError, 'contradicts'):
            upgrade.parent_readiness(fake, plan, nodes)

    def test_tuner_error_or_inconsistent_lease_snapshot_is_not_ready(self):
        self.prepare()
        _, plan = self.plan()
        nodes = plan['execution_nodes']
        good = dict(state='workers_ready', verified_nodes=len(nodes),
            proofs=[dict(node=node, boot=plan['node_boots'][node]) for node in nodes],
            status=dict(state='rm_running',
                parent_scheduler=dict(limit=2, pending=1, active_leases=2, claims=3, releases=1),
                parent_tuning=dict(limit=2, generation=0, error=None)))
        for kind in ('tuner', 'leases', 'missing'):
            bad = copy.deepcopy(good)
            if kind == 'tuner':bad['status']['parent_tuning']['error'] = 'private tuner error'
            elif kind == 'leases':bad['status']['parent_scheduler']['active_leases'] = 9
            else:bad['status'].pop('parent_scheduler')
            with self.subTest(kind=kind), self.assertRaisesRegex(RuntimeError, 'scheduler/tuner'):
                upgrade.parent_readiness(bad, plan, nodes)


if __name__ == '__main__':
    unittest.main()
