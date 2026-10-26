import hashlib
import os
from pathlib import Path
import tempfile
import unittest

import migration_linux_open as helper
import migration_worker as engine
import profile_priority4_copy as profile


class CopyPhaseProfileTests(unittest.TestCase):
    def setUp(self):
        engine.configure_directory_opener(None)
        engine.configure_cancel_event(None)
        engine.configure_rate_limit(0)

    def tearDown(self):
        engine.configure_directory_opener(None)

    def test_three_modes_keep_cold_parents_sha_metadata_and_source(self):
        with tempfile.TemporaryDirectory(prefix='copy-phase-profile-fixture-') as name:
            root = Path(name)
            source = root / 'source'
            (source / 'a/b').mkdir(parents=True)
            content = b'phase-profile\n' * 100
            original = source / 'a/b/f'
            original.write_bytes(content)
            original.chmod(0o640)
            entry = dict(rel='a/b/f', kind='file', **engine._record(original.stat()))
            identity = engine._identity(original.stat())
            hashes = {entry['rel']: hashlib.sha256(content).hexdigest()}
            saved = {k: getattr(engine, k) for k in ('os', '_open_parent', '_metadata_fd', '_hash_fd')}
            for mode in ('legacy', 'native_readonly', 'destination_opath',
                         'frontier_readonly', 'frontier_path_anchor'):
                target = root / mode
                target.mkdir()
                result = profile.run_trial(engine, helper, str(source), target, [entry], hashes,
                                           mode, 'fixture', lambda: None, threads=2)
                self.assertTrue(result['parent_scaffolding_timed'])
                self.assertEqual((result['files'], result['bytes']), (1, len(content)))
                self.assertEqual((target / entry['rel']).read_bytes(), content)
                self.assertEqual(engine._identity(original.stat()), identity)
                self.assertEqual(set(result['profile']['phases']),
                                 {'source_parent', 'destination_parent', 'metadata', 'readback_sha'})
                calls = result['profile']['syscalls']
                self.assertEqual(calls['destination_parent:mkdir']['calls'], 2)
                self.assertEqual(calls['body:fsync']['calls'], 1)
                self.assertEqual(calls['body:link']['calls'], 1)
                self.assertEqual(calls['body:unlink']['calls'], 1)
                for key, value in saved.items():
                    self.assertIs(getattr(engine, key), value)
                self.assertIs(engine.os, os)

    def test_exception_restores_private_engine_and_never_patches_global_os(self):
        saved = engine._open_parent
        with self.assertRaisesRegex(RuntimeError, 'fixture failure'):
            with profile.CopyProfiler(engine, '/source', '/target') as profiler:
                self.assertIsNot(engine.os, os)
                self.assertIs(engine.os.original, os)
                def fail():
                    raise RuntimeError('fixture failure')
                profiler.copy(fail)
        self.assertIs(engine.os, os)
        self.assertIs(engine._open_parent, saved)

    def test_readonly_failures_measured_without_changing_exception(self):
        with profile.CopyProfiler(engine, '/source', '/target') as profiler:
            def missing():
                try:
                    engine.os.stat('/__nonexistent_copy_phase_profile_fixture__')
                except FileNotFoundError:
                    return 'missing'
            result, record = profiler.copy(missing)
        self.assertEqual(result, 'missing')
        self.assertEqual(record['syscalls']['body:stat']['failures'], 1)
        self.assertGreater(record['copy_seconds'], 0)

    def test_summarize_counts_is_not_wall_time_or_double_counted(self):
        result = profile.summarize([
            dict(copy_seconds=2, phases={'source_parent': dict(calls=1, seconds=1,
                  max_seconds=1, failures=0)}, syscalls={}),
            dict(copy_seconds=4, phases={'source_parent': dict(calls=1, seconds=2,
                  max_seconds=2, failures=1)}, syscalls={})])
        self.assertEqual(result['copy_thread_seconds'], 6)
        self.assertEqual(result['phases']['source_parent']['calls'], 2)
        self.assertEqual(result['phases']['source_parent']['max_seconds'], 2)
        self.assertEqual(result['phases']['source_parent']['failures'], 1)
        self.assertAlmostEqual(result['phases']['source_parent']['fraction_of_copy_thread_time'], .5)

    def test_nonempty_or_symlink_scratch_is_not_modified(self):
        with tempfile.TemporaryDirectory(prefix='copy-phase-profile-fixture-') as name:
            root = Path(name)
            target = root / 'target'
            target.mkdir()
            (target / 'preserve').write_bytes(b'important')
            link = root / 'link'
            link.symlink_to(target, target_is_directory=True)
            for path in (target, link):
                with self.assertRaises(ValueError):
                    profile.run_trial(engine, helper, '/source', path, [{}], {},
                                      'legacy', 'fixture', lambda: None)
            self.assertEqual((target / 'preserve').read_bytes(), b'important')

    def test_frontier_observation_and_partial_scaffolding_do_not_create_production_dirs(self):
        with tempfile.TemporaryDirectory(prefix='copy-phase-frontier-fixture-') as name:
            root = Path(name)
            production, source, target = [root / x for x in ('production', 'source', 'target')]
            (production / 'a').mkdir(parents=True)
            (source / 'a/b/c').mkdir(parents=True)
            target.mkdir()
            file = source / 'a/b/c/f'
            file.write_bytes(b'partial-cold-parent')
            entry = dict(rel='a/b/c/f', kind='file', **engine._record(file.stat()))
            frontier = profile.observe_frontier(helper, production, [entry], lambda: None)
            self.assertEqual(frontier['boundaries'],
                             {'a/b/c': {'existing_prefix': 'a', 'missing_depth': 2}})
            self.assertFalse((production / 'a/b').exists())
            value = profile.run_trial(engine, helper, str(source), target, [entry],
                {entry['rel']: hashlib.sha256(file.read_bytes()).hexdigest()},
                'native_readonly', 'fixture', lambda: None, threads=2, scaffolding=frontier)
            self.assertEqual(value['scaffolding_mode'], 'production_frontier_snapshot')
            self.assertEqual(value['profile']['syscalls']['destination_parent:mkdir']['calls'], 2)
            self.assertFalse((production / 'a/b').exists())

    def test_frontier_observation_rejects_symlink_ancestors_and_missing_mount(self):
        with tempfile.TemporaryDirectory(prefix='copy-phase-frontier-fixture-') as name:
            root = Path(name)
            real = root / 'real'
            real.mkdir()
            (root / 'linked').symlink_to(real, target_is_directory=True)
            with self.assertRaises(OSError):
                profile.observe_frontier(helper, root, [{'rel': 'linked/deep/f'}], lambda: None)
            with self.assertRaises(RuntimeError):
                profile.observe_frontier(helper, root / 'missing', [{'rel': 'a/f'}], lambda: None)


if __name__ == '__main__':
    unittest.main()
