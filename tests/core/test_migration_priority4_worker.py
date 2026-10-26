"""Local P4 scope/dispatch fixtures; no live workers or remote paths touched."""
import copy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_io
import migration_priority4_worker as p4
import migration_run as core
import migration_worker as engine
import migration_worker128_run as auxiliary
from migration_state_cache import StateCache


CONTROL = '/share-mixed/project/.hcr-copy-p4-fixture-123'
PREDECESSOR = '/share-mixed/project/.hcr-copy-fixture-eai'


def config():
    return dict(source='/share/project', destination='/share-mixed/project',
                run_id='hcr-copy-p4-fixture-123', migration_scope='project_priority4',
                exclude=['eai_pwm'], covered_by_controls=[PREDECESSOR],
                protected_destination_relatives=['.hcr-copy-extra-proof'],
                threads=64, copy_threads_per_node=128, supplement_threads=64,
                io_bytes_per_second=10_000_000_000,
                supplement_io_bytes_per_second=10_000_000_000)


def task(**changes):
    return dict(dict(id=11, phase=4, round=1, op='copy'), **changes)


class Priority4GuardTests(unittest.TestCase):
    def setUp(self):
        self.cfg = config()
        self.guard = p4.ScopeGuard(CONTROL, self.cfg)
        self.delegate = mock.Mock(return_value={'status': 'copied', 'bytes': 7})
        self.process = self.guard.wrap(self.delegate)

    def entry(self, rel='datasets/a', kind='file'):
        return {'rel': rel, 'kind': kind}

    def test_exact_covered_and_reserved_destinations_never_reach_engine(self):
        for root in ['eai_pwm', '.hcr-copy-monitor', '.hcr-copy-p4-fixture-123',
                     '.hcr-copy-fixture-eai', '.hcr-copy-extra-proof']:
            for rel in [root, root + '/file']:
                with self.subTest(rel=rel):
                    result = self.process(self.cfg, task(), self.entry(rel))
                    self.assertEqual(result['status'], 'error')
                    self.assertTrue(result['scope_violation'])
        self.delegate.assert_not_called()

    def test_other_names_and_nested_identical_names_remain_in_scope(self):
        for rel in ['home/hcr/a', 'repos/cosmos-framework-pwm/a',
                    'other/eai_pwm/a', 'other/.hcr-copy-monitor/a', '.hidden/a',
                    '.hcr-copy-unregistered-source/a', 'eai_pwm_extra/a']:
            self.assertEqual(self.process(self.cfg, task(), self.entry(rel)),
                             {'status': 'copied', 'bytes': 7})
        self.assertEqual(self.delegate.call_count, 7)

    def test_root_and_unsafe_payload_paths_are_durable_errors(self):
        for rel in ['', '.', '../escape', '/absolute', 'a//b', 'a/./b',
                    'a/../b', 'a/', '\x00', None]:
            result = self.process(self.cfg, task(), self.entry(rel))
            self.assertTrue(result['scope_violation'], repr(rel))
        self.delegate.assert_not_called()

    def test_link_target_can_point_to_eai_without_following_it(self):
        entry = dict(self.entry('alias', 'symlink'), target='eai_pwm')
        self.process(self.cfg, task(), entry)
        self.delegate.assert_called_once_with(self.cfg, task(), entry)

    def test_invalid_tasks_and_directory_payload_are_not_executed(self):
        for changes in [dict(phase=3), dict(phase=True), dict(round=4),
                        dict(round=True), dict(id=True), dict(id=-1), dict(op='delete')]:
            self.assertTrue(self.process(self.cfg, task(**changes), self.entry())['scope_violation'])
        self.assertTrue(self.process(self.cfg, task(), self.entry(kind='directory'))['scope_violation'])
        self.delegate.assert_not_called()

    def test_sync_all_valid_rounds_use_the_original_engine(self):
        for n in (1, 2, 3):
            t = task(round=n, op='sync')
            entry = dict(self.entry(), known={'owned': True, 'sha256': 'previous-proof'})
            self.process(self.cfg, t, entry)
            self.delegate.assert_called_with(self.cfg, t, entry)

    def test_changed_scope_and_removed_protections_are_rejected(self):
        for changes in [dict(source='/share/project/eai_pwm'),
                        dict(destination='/home-mixed'), dict(exclude=[]),
                        dict(run_id='other'), dict(migration_scope='other'),
                        dict(covered_by_controls=[]), dict(protected_destination_relatives=[])]:
            cfg = dict(self.cfg, **changes)
            self.assertTrue(self.process(cfg, task(), self.entry())['scope_violation'])
        self.delegate.assert_not_called()

    def test_copy_budget_and_lanes_cannot_double_or_disappear(self):
        for changes in [dict(threads=128), dict(supplement_threads=128),
                        dict(copy_threads_per_node=64), dict(threads=True),
                        dict(io_bytes_per_second=20_000_000_000),
                        dict(supplement_io_bytes_per_second=0),
                        dict(io_bytes_per_second=float('nan'))]:
            with self.assertRaises(ValueError):
                p4.ScopeGuard(CONTROL, dict(self.cfg, **changes))

    def test_invalid_coverage_and_control_are_rejected(self):
        for coverage in [[], None, PREDECESSOR, [CONTROL],
                         ['/share/project/control'], ['/share-mixed/project/nested/control']]:
            with self.assertRaises(ValueError):
                p4.ScopeGuard(CONTROL, dict(self.cfg, covered_by_controls=coverage))
        for control in ['/tmp/not-p4', '/share-mixed/project/.hcr-copy-wrong',
                        '/share-mixed/project/nested/.hcr-copy-p4-fixture-123']:
            with self.assertRaises(ValueError):
                p4.ScopeGuard(control, self.cfg)

    def test_metadata_mount_root_is_preserved_and_covered_children_rejected(self):
        for rel in ['', '.']:
            self.assertEqual(self.guard.destination_metadata_policy(rel), 'preserve_mount_root')
        self.assertEqual(self.guard.destination_metadata_policy('other/eai_pwm'), 'copy_child_metadata')
        for rel in ['eai_pwm', '.hcr-copy-monitor', '.hcr-copy-fixture-eai/a']:
            with self.assertRaises(ValueError):
                self.guard.destination_metadata_policy(rel)

    def test_lexical_scope_checks_add_no_filesystem_probes(self):
        with mock.patch('os.stat', side_effect=AssertionError('unexpected stat')), \
             mock.patch.object(Path, 'resolve', side_effect=AssertionError('unexpected resolve')):
            self.guard.assert_path('other/eai_pwm/a')
            self.guard.assert_config(self.cfg)
            self.guard.assert_state({'stage': 'p4-r2', 'phase': 4, 'round': 2})

    def test_eai_warm_or_non_p4_state_cannot_feed_workers(self):
        for state in [dict(stage='p3-r3'), dict(recovery_warm=True), dict(phase=3),
                      dict(stage='p4-r2', round=1), dict(stage='p4-r4'), dict(round=True)]:
            with self.assertRaises(ValueError):
                self.guard.assert_state(state)


class Priority4OriginalEngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.source, self.destination = self.base / 'source', self.base / 'destination'
        self.source.mkdir()
        self.destination.mkdir()
        self.cfg = config()
        self.process = p4.ScopeGuard(CONTROL, self.cfg).wrap(core.process_entry)
        original_open_root = engine._open_root
        def mapped_open_root(root, create=False):
            mapped = {p4.SOURCE: self.source, p4.DESTINATION: self.destination}.get(str(root))
            if mapped is None:
                raise AssertionError('unexpected filesystem root')
            return original_open_root(str(mapped), create=create)
        patcher = mock.patch.object(engine, '_open_root', mapped_open_root)
        patcher.start()
        self.addCleanup(patcher.stop)
        engine.configure_rate_limit(None)
        engine.configure_cancel_event(None)
        engine.configure_progress_callback(None)

    def put(self, rel, value=b'copy-data'):
        path = self.source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        return dict(engine._record(path.stat()), rel=rel, kind='file')

    def test_actual_copy_sha_existing_reuse_and_unowned_conflict(self):
        entry = self.put('other/eai_pwm/a')
        copied = self.process(self.cfg, task(), entry)
        self.assertEqual(copied['status'], 'copied')
        self.assertEqual(copied['sha256'], hashlib.sha256(b'copy-data').hexdigest())
        self.assertEqual((self.destination / entry['rel']).read_bytes(), b'copy-data')
        self.assertEqual(self.process(self.cfg, task(), entry)['status'], 'skipped')
        conflict = self.put('different', b'new-source')
        (self.destination / 'different').write_bytes(b'existing-destination')
        self.assertEqual(self.process(self.cfg, task(), conflict)['status'], 'conflict')
        self.assertEqual((self.destination / 'different').read_bytes(), b'existing-destination')
        self.assertEqual((self.source / 'different').read_bytes(), b'new-source')

    def test_actual_symlink_copy_does_not_enter_eai(self):
        os.symlink('eai_pwm', self.source / 'alias')
        entry = dict(engine._record((self.source / 'alias').lstat()),
                     rel='alias', kind='symlink', target='eai_pwm')
        result = self.process(self.cfg, task(), entry)
        self.assertEqual(result['status'], 'copied')
        self.assertEqual(os.readlink(self.destination / 'alias'), 'eai_pwm')
        self.assertFalse((self.destination / 'eai_pwm').exists())

    def test_destination_parent_symlink_cannot_redirect_copy_into_covered_tree(self):
        entry = self.put('alias/a')
        (self.destination / 'eai_pwm').mkdir()
        os.symlink('eai_pwm', self.destination / 'alias')
        result = self.process(self.cfg, task(), entry)
        self.assertEqual(result['status'], 'error')
        self.assertFalse((self.destination / 'eai_pwm/a').exists())

    def test_stop_uses_unchanged_copy_cancellation(self):
        import threading
        entry = self.put('cancelled')
        stop = threading.Event()
        stop.set()
        engine.configure_cancel_event(stop)
        self.addCleanup(engine.configure_cancel_event, None)
        result = self.process(self.cfg, task(), entry)
        self.assertTrue(result['cancelled'])
        self.assertFalse((self.destination / 'cancelled').exists())


class Priority4WrapperTests(unittest.TestCase):
    def exercise(self, kind, failure=False):
        cfg, state = config(), {'stage': 'p4-r1', 'phase': 4, 'round': 1}
        calls, caches = [], []
        class ImmediateCache(StateCache):
            def start(self):
                caches.append(self)
                self.refresh_once()
                return self
        def reader(path):
            path = str(path)
            if path == CONTROL + '/config.json':
                return copy.deepcopy(cfg)
            if path == CONTROL + '/state.json':
                return copy.deepcopy(state)
            raise AssertionError('unexpected JSON ' + path)
        original_core = core.process_entry
        original_aux = auxiliary.process_entry
        original_readers = (core.read_json, auxiliary.read_json)
        module = core if kind == 'primary' else auxiliary
        def worker(control, threads):
            calls.append((control, threads))
            self.assertEqual(migration_io.read_json(CONTROL + '/state.json'), state)
            self.assertEqual(module.read_json(CONTROL + '/config.json'), cfg)
            result = module.process_entry(cfg, task(), {'rel': 'eai_pwm/never', 'kind': 'file'})
            self.assertTrue(result['scope_violation'])
            self.assertIs(core.process_entry, module.process_entry)
            if failure:
                raise RuntimeError('fixture worker failure')
        with mock.patch.object(migration_io, 'read_json', reader), \
             mock.patch.object(module, 'worker', worker), \
             mock.patch.object(p4, 'StateCache', ImmediateCache):
            if failure:
                with self.assertRaisesRegex(RuntimeError, 'fixture worker failure'):
                    p4.main([kind, '--control', CONTROL])
            else:
                self.assertEqual(p4.main([kind, '--control', CONTROL]), 0)
            self.assertIs(migration_io.read_json, reader)
        self.assertEqual(calls, [(CONTROL, 64)])
        self.assertEqual(caches[0].max_age, 30)
        with self.assertRaises(OSError):
            caches[0].read()
        self.assertIs(core.process_entry, original_core)
        self.assertIs(auxiliary.process_entry, original_aux)
        self.assertEqual((core.read_json, auxiliary.read_json), original_readers)

    def test_primary_module_globals_are_guarded_and_restored(self):
        self.exercise('primary')

    def test_supplement_imported_alias_is_guarded_and_restored(self):
        self.exercise('supplement')

    def test_failed_worker_restores_all_aliases_and_closes_cache(self):
        self.exercise('supplement', failure=True)

    def test_wrong_scope_is_rejected_before_import_or_cache_start(self):
        with mock.patch.object(migration_io, 'read_json', return_value=dict(config(), source='/home')), \
             mock.patch.object(p4, 'StateCache') as cache:
            with self.assertRaises(ValueError):
                p4.main(['primary', '--control', CONTROL])
            cache.assert_not_called()


if __name__ == '__main__':
    unittest.main()
