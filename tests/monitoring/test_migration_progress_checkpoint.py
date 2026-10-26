"""Local-only fail-closed counter inheritance and destination O_PATH fixtures."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_cached_worker as cached
import migration_io
import migration_linux_open as native
import test_migration_worker_native_open as copy_fixtures


@unittest.skipUnless(hasattr(os, 'O_PATH'), 'Linux O_PATH required')
class PathCopySafetyTests(copy_fixtures.NativeCopySafetyTests):
    def setUp(self):
        super().setUp()
        import migration_worker
        def opener(root, rel):
            fn = native.open_parent_path if Path(root) == self.destination and \
                rel != '__directory_open_root__' else native.open_parent_readonly
            return fn(root, rel)
        migration_worker.configure_directory_opener(opener)


class StartupProgressTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='progress-checkpoint-', dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.bundle = self.control / 'bundle'
        self.bundle.mkdir()
        self.host = 'fixture-host'
        self.node = self.control / 'nodes' / (self.host + '.json')
        self.checkpoint = self.bundle / 'cache-age-rollout' / (self.host + '-primary.json')
        self.before = dict(host=self.host, pid=123, active={}, run_started='original-run',
                           bytes=1000, streamed_bytes=2000, verified_bytes=1000,
                           verified_files=10, completed_batches=2, broker_retries=0,
                           counts={'copied': 8, 'skipped': 2},
                           stages={'p3-r3': {'bytes': 1000, 'counts': {'copied': 8}}})
        self.record = dict(host=self.host, lane='primary', operation='operation',
                           expected={'pid': 123}, status='replacing', lane_leases={},
                           idle=copy.deepcopy(self.before))
        migration_io.write_json(self.node, self.before)
        migration_io.write_json(self.checkpoint, self.record)
        patch = mock.patch.object(cached.socket, 'gethostname', return_value=self.host)
        patch.start()
        self.addCleanup(patch.stop)

    def guard(self, reader=migration_io.read_json):
        return cached.StartupProgressGuard(self.control, 'primary', self.checkpoint,
                                           'operation', reader, self.bundle)

    def test_actual_first_read_inherits_latest_history_not_frozen_baseline(self):
        guard = self.guard()
        guard.preflight()
        latest = dict(self.before, bytes=1001, ended='old worker stopped')
        migration_io.write_json(self.node, latest)
        self.assertEqual(guard.read_json(str(self.node)), latest)
        self.assertTrue(guard.consumed)
        migration_io.write_json(self.node, dict(latest, pid=456))
        self.assertEqual(guard.read_json(self.node)['pid'], 456)

    def test_error_between_preflight_and_actual_read_cannot_be_caught_as_empty_history(self):
        def reader(path):
            if Path(path) == self.node and fail[0]:
                raise OSError('simulated shared snapshot read failure')
            return migration_io.read_json(path)
        fail = [False]
        guard = self.guard(reader)
        guard.preflight()
        fail[0] = True
        with self.assertRaises(cached.StartupProgressError):
            try:
                guard.read_json(self.node)
            except (OSError, ValueError):
                self.fail('legacy fallback would silently reset counters')
        self.assertFalse(guard.consumed)
        self.assertEqual(migration_io.read_json(self.node)['bytes'], 1000)

    def test_counter_identity_or_nested_mapping_regression_rejected(self):
        for patch in ({'bytes': 999}, {'counts': {}}, {'stages': {}},
                      {'run_started': 'new-run'}, {'pid': 456}, {'active': {'one': {}}},
                      {'bytes': float('nan')}, {'bytes': True}, {'verified_files': -1},
                      {'host': 'other-host'}, {'thread_error': 'error'}):
            with self.subTest(patch=patch):
                migration_io.write_json(self.node, dict(self.before, **patch))
                with self.assertRaises(cached.StartupProgressError):
                    self.guard().read_json(self.node)

    def test_wrong_operation_lane_pid_and_missing_checkpoint_counter_rejected(self):
        for patch in ({'operation': 'other'}, {'lane': 'supplement'},
                      {'status': 'complete'}, {'lane_leases': {'stage': 1}},
                      {'expected': {'pid': 999}}, {'idle': {'pid': 123}}):
            with self.subTest(patch=patch):
                migration_io.write_json(self.checkpoint, dict(self.record, **patch))
                with self.assertRaises(cached.StartupProgressError):
                    self.guard()

    def test_unrelated_reads_not_cached_or_replaced(self):
        cfg = self.control / 'config.json'
        migration_io.write_json(cfg, {'value': 1})
        guard = self.guard()
        self.assertEqual(guard.read_json(cfg), {'value': 1})
        migration_io.write_json(cfg, {'value': 2})
        self.assertEqual(guard.read_json(cfg), {'value': 2})
        self.assertFalse(guard.consumed)

    def test_required_policy_without_checkpoint_never_starts_worker(self):
        migration_io.write_json(self.bundle / 'worker-directory-policy.json', cached.PATH_POLICY)
        migration_io.write_json(self.control / 'config.json', {'destination': str(self.control / 'dest')})
        with mock.patch.object(cached, '__file__', str(self.bundle / 'migration_cached_worker.py')), \
                mock.patch.object(cached.runpy, 'run_path') as run, \
                mock.patch.object(cached, 'StateCache') as cache:
            with self.assertRaises(cached.StartupProgressError):
                cached.main(['primary', '--control', str(self.control)])
            run.assert_not_called()
            cache.assert_not_called()

    def test_guarded_wrapper_keeps_actual_worker_read_fail_closed_and_restores_reader(self):
        migration_io.write_json(self.bundle / 'worker-directory-policy.json', cached.PATH_POLICY)
        migration_io.write_json(self.control / 'config.json', {'destination': str(self.control / 'dest')})
        migration_io.write_json(self.control / 'state.json', {'stage': 'p3-r3'})
        original = migration_io.read_json
        def run(*args, **kwargs):
            self.assertIsInstance(migration_io.read_json.__self__, cached.StartupProgressGuard)
            self.assertEqual(migration_io.read_json(self.node)['bytes'], 1000)
            self.assertNotIn('--progress-checkpoint', cached.sys.argv)
        with mock.patch.object(cached, '__file__', str(self.bundle / 'migration_cached_worker.py')), \
                mock.patch.object(cached.runpy, 'run_path', side_effect=run):
            cached.main(['primary', '--control', str(self.control),
                         '--progress-checkpoint', str(self.checkpoint),
                         '--progress-operation', 'operation'])
        self.assertIs(migration_io.read_json, original)


@unittest.skipUnless(hasattr(os, 'O_PATH'), 'Linux O_PATH required')
class DestinationPathTests(unittest.TestCase):
    def test_policy_changes_only_destination_parent_not_source_or_metadata_root(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            bundle = Path(directory)
            source, destination = bundle / 'source', bundle / 'destination'
            source.mkdir()
            destination.mkdir()
            migration_io.write_json(bundle / 'worker-directory-policy.json', cached.PATH_POLICY)
            migration_io.write_json(bundle / 'config.json', {'destination': str(destination)})
            opener = cached._directory_opener_for_bundle(bundle, migration_io.read_json, bundle)
            with mock.patch.object(native, 'open_parent_readonly', return_value=(1, 'file')) as regular, \
                    mock.patch.object(native, 'open_parent_path', return_value=(2, 'file')) as path:
                self.assertEqual(opener(source, 'file')[0], 1)
                self.assertEqual(opener(destination, 'file')[0], 2)
                self.assertEqual(opener(destination, '__directory_open_root__')[0], 1)
                self.assertEqual(regular.call_count, 2)
                self.assertEqual(path.call_count, 1)

    def test_kernel_and_fallback_path_fds_support_copy_at_operations_not_directory_reads(self):
        import fcntl
        for fallback in (False, True):
            with self.subTest(fallback=fallback), tempfile.TemporaryDirectory(dir='/tmp') as directory:
                root = Path(directory)
                parent = root / 'parent'
                parent.mkdir()
                with mock.patch.object(native, '_openat2_unavailable', fallback):
                    fd, name = native.open_parent_path(root, 'parent/file')
                try:
                    self.assertTrue(fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_PATH)
                    self.assertFalse(os.get_inheritable(fd))
                    self.assertEqual(os.fstat(fd).st_ino, parent.stat().st_ino)
                    file_fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
                    os.write(file_fd, b'payload')
                    os.fsync(file_fd)
                    os.close(file_fd)
                    os.link(name, 'final', src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
                    os.unlink(name, dir_fd=fd)
                    self.assertEqual(os.stat('final', dir_fd=fd).st_size, 7)
                    self.assertEqual((parent / 'final').read_bytes(), b'payload')
                    with self.assertRaises(OSError):
                        os.listdir(fd)
                finally:
                    os.close(fd)

    def test_path_mode_still_rejects_all_symlink_ancestors(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory)
            (root / 'real').mkdir()
            (root / 'link').symlink_to(root / 'real', target_is_directory=True)
            for fallback in (False, True):
                with self.subTest(fallback=fallback), \
                        mock.patch.object(native, '_openat2_unavailable', fallback):
                    with self.assertRaises(OSError):
                        native.open_parent_path(root, 'link/file')


if __name__ == '__main__':
    unittest.main()
