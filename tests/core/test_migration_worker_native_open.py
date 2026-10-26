"""Opt-in native directory opens preserve the copy worker's safety contract."""
import errno
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

import migration_cached_worker as cached
import migration_linux_open as native
import migration_worker as worker
import test_migration_worker_directory_fastpath as fixtures


class NativeDirectoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='native-worker-', dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'root'
        (self.root / 'a/b').mkdir(parents=True)
        self.previous = worker._directory_opener
        self.addCleanup(worker.configure_directory_opener, self.previous)
        worker.configure_directory_opener(native.open_parent_readonly)

    def test_existing_root_and_parent_have_real_anchored_fds(self):
        fd = worker._open_root(self.root)
        try:
            self.assertEqual(os.fstat(fd).st_ino, self.root.stat().st_ino)
        finally:
            os.close(fd)
        for create in (False, True):
            fd, name = worker._open_parent(self.root, 'a/b/file', create=create)
            try:
                self.assertEqual(os.fstat(fd).st_ino,
                                 (self.root / 'a/b').stat().st_ino)
                self.assertEqual(name, 'file')
                self.assertFalse(os.get_inheritable(fd))
            finally:
                os.close(fd)

    def test_existing_root_parent_each_use_one_native_syscall(self):
        if native.safe_open_backend() != 'openat2':
            self.skipTest('actual openat2 support required for syscall count')
        with mock.patch.object(native, '_syscall', wraps=native._syscall) as syscall, \
                mock.patch.object(worker, '_open_directory_component',
                                  side_effect=AssertionError('component walk')), \
                mock.patch.object(worker.os, 'mkdir') as mkdir:
            fd = worker._open_root(self.root, create=True)
            os.close(fd)
            self.assertEqual(syscall.call_count, 1)
            fd, _ = worker._open_parent(self.root, 'a/b/file', create=False)
            os.close(fd)
            self.assertEqual(syscall.call_count, 2)
            fd, _ = worker._open_parent(self.root, 'a/b/file', create=True)
            os.close(fd)
            self.assertEqual(syscall.call_count, 3)
            mkdir.assert_not_called()

    def test_missing_parent_creates_only_missing_directories(self):
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            fd, name = worker._open_parent(self.root, 'new/deep/file', create=True)
            os.close(fd)
        self.assertEqual(name, 'file')
        self.assertEqual([call.args[0] for call in mkdir.call_args_list], ['new', 'deep'])
        self.assertFalse((self.root / 'new/deep/file').exists())

    def test_missing_root_creation_is_safe_fallback(self):
        new_root = self.root / 'new-root/nested'
        fd = worker._open_root(new_root, create=True)
        try:
            self.assertEqual(os.fstat(fd).st_ino, new_root.stat().st_ino)
        finally:
            os.close(fd)
        self.assertEqual(list(new_root.iterdir()), [])

    def test_readonly_missing_never_falls_back_or_creates(self):
        with mock.patch.object(worker, '_open_directory_component') as component, \
                mock.patch.object(worker.os, 'mkdir') as mkdir:
            with self.assertRaises(FileNotFoundError):
                worker._open_parent(self.root, 'absent/file')
            with self.assertRaises(FileNotFoundError):
                worker._open_root(self.root / 'absent')
            component.assert_not_called()
            mkdir.assert_not_called()

    def test_nonmissing_native_errors_never_fallback_even_when_create(self):
        for code in (errno.EACCES, errno.ELOOP, errno.ENOTDIR, errno.EIO):
            with self.subTest(errno=code):
                worker.configure_directory_opener(
                    mock.Mock(side_effect=OSError(code, os.strerror(code))))
                with mock.patch.object(worker, '_open_directory_component') as component, \
                        mock.patch.object(worker.os, 'mkdir') as mkdir:
                    for operation in (lambda: worker._open_root(self.root, create=True),
                                      lambda: worker._open_parent(self.root, 'a/file', create=True)):
                        with self.assertRaises(OSError) as caught:
                            operation()
                        self.assertEqual(caught.exception.errno, code)
                    component.assert_not_called()
                    mkdir.assert_not_called()

    def test_symlink_in_root_or_parent_is_rejected_not_created_through(self):
        outside = self.root / 'outside'
        outside.mkdir()
        (self.root / 'link').symlink_to(outside, target_is_directory=True)
        with mock.patch.object(worker.os, 'mkdir') as mkdir:
            with self.assertRaises(OSError):
                worker._open_parent(self.root, 'link/new/file', create=True)
            with self.assertRaises(OSError):
                worker._open_root(self.root / 'link', create=True)
            mkdir.assert_not_called()
        self.assertEqual(list(outside.iterdir()), [])

    def test_unsafe_rel_is_rejected_before_calling_native(self):
        opener = mock.Mock()
        worker.configure_directory_opener(opener)
        for rel in ('', '/absolute', '../escape', 'a//file', 'a/./file', 'a\x00/file'):
            with self.subTest(rel=rel), self.assertRaises(ValueError):
                worker._open_parent(self.root, rel, create=True)
        opener.assert_not_called()

    def test_configure_none_restores_default_and_rejects_noncallable(self):
        with self.assertRaises(TypeError):
            worker.configure_directory_opener(True)
        worker.configure_directory_opener(None)
        with mock.patch.object(worker, '_open_directory_component',
                               wraps=worker._open_directory_component) as walk:
            fd, _ = worker._open_parent(self.root, 'a/b/file')
            os.close(fd)
            self.assertGreater(walk.call_count, 1)


class NativeCopySafetyTests(fixtures.CopyFastpathRegressionTests):
    def setUp(self):
        super().setUp()
        previous = worker._directory_opener
        self.addCleanup(worker.configure_directory_opener, previous)
        worker.configure_directory_opener(native.open_parent_readonly)

    def test_existing_target_sha_checks_remain_and_inode_is_not_replaced(self):
        body = b'verified identical target'
        entry = self.entry('one/file', body)
        target = self.destination / 'one/file'
        target.parent.mkdir()
        target.write_bytes(body)
        inode = target.stat().st_ino
        with mock.patch.object(worker, '_hash_fd', wraps=worker._hash_fd) as hashes:
            result = worker.copy_entry(self.source, self.destination, entry, 'native-fixture')
        self.assertEqual(result['status'], 'skipped', result)
        self.assertEqual(result['sha256'], hashlib.sha256(body).hexdigest())
        self.assertEqual(result['bytes'], 0)
        self.assertEqual(hashes.call_count, 2)
        self.assertEqual(target.stat().st_ino, inode)
        self.assertEqual(result['source_identity']['ino'], entry['ino'])

    def test_same_size_conflict_is_hashed_and_not_overwritten(self):
        entry = self.entry('file', b'AAAA')
        target = self.destination / 'file'
        target.write_bytes(b'BBBB')
        inode = target.stat().st_ino
        with mock.patch.object(worker, '_hash_fd', wraps=worker._hash_fd) as hashes:
            result = worker.copy_entry(self.source, self.destination, entry, 'native-fixture')
        self.assertEqual(result['status'], 'conflict', result)
        self.assertEqual(hashes.call_count, 2)
        self.assertEqual(target.read_bytes(), b'BBBB')
        self.assertEqual(target.stat().st_ino, inode)

    def test_source_changed_since_scan_is_not_copied(self):
        entry = self.entry('file', b'before')
        (self.source / 'file').write_bytes(b'after and longer')
        result = worker.copy_entry(self.source, self.destination, entry, 'native-fixture')
        self.assertEqual(result['status'], 'changed', result)
        self.assertFalse((self.destination / 'file').exists())

    def test_scan_inode_identity_is_still_enforced(self):
        entry = self.entry('file', b'unchanged payload')
        entry['ino'] += 1
        result = worker.copy_entry(self.source, self.destination, entry, 'native-fixture')
        self.assertEqual(result['status'], 'changed', result)
        self.assertIn('ino', result['detail'])
        self.assertFalse((self.destination / 'file').exists())

    def test_cancelled_copy_does_not_open_or_publish(self):
        entry = self.entry('file', b'payload')
        cancelled = threading.Event()
        cancelled.set()
        worker.configure_cancel_event(cancelled)
        self.addCleanup(worker.configure_cancel_event, None)
        with mock.patch.object(worker, '_open_parent') as opened:
            result = worker.copy_entry(self.source, self.destination, entry, 'native-fixture')
        self.assertEqual(result['status'], 'error', result)
        self.assertTrue(result['cancelled'])
        opened.assert_not_called()
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_new_copy_still_fsyncs_and_carries_full_source_identity(self):
        body = b'strict full copy verification'
        entry = self.entry('one/two/file', body)
        with mock.patch.object(worker.os, 'fsync', wraps=os.fsync) as sync, \
                mock.patch.object(worker, '_hash_fd', wraps=worker._hash_fd) as hashes:
            result = worker.copy_entry(self.source, self.destination, entry, 'native-fixture')
        self.assertEqual(result['status'], 'copied', result)
        self.assertEqual(result['sha256'], hashlib.sha256(body).hexdigest())
        self.assertGreaterEqual(sync.call_count, 1)
        self.assertGreaterEqual(hashes.call_count, 1)
        for field in worker.IDENTITY_FIELDS:
            self.assertEqual(result['source_identity'][field], entry[field])
        self.assertEqual((self.destination / entry['rel']).read_bytes(), body)


class NativeWrapperPolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='native-policy-', dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.bundle = Path(temporary.name)
        self.control = self.bundle / 'control'
        self.control.mkdir()

    def test_policy_exact_schema_and_absence(self):
        reader = lambda p: json.loads(p.read_text())
        self.assertIsNone(cached._directory_opener_for_bundle(self.bundle, reader))
        policy = self.bundle / 'worker-directory-policy.json'
        for value in (None, [], {}, {'scope': 'other'},
                      {'scope': 'native-safe-parent-open-v1', 'unsafe': True}):
            policy.write_text(json.dumps(value))
            with self.subTest(value=value), self.assertRaises(ValueError):
                cached._directory_opener_for_bundle(self.bundle, reader)
        policy.write_text('{"scope":"native-safe-parent-open-v1"}')
        self.assertIs(cached._directory_opener_for_bundle(self.bundle, reader),
                      native.open_parent_readonly)

    def test_dangling_policy_is_not_silently_ignored(self):
        (self.bundle / 'worker-directory-policy.json').symlink_to('missing-policy')
        with self.assertRaises(FileNotFoundError):
            cached._directory_opener_for_bundle(self.bundle,
                                               lambda p: json.loads(p.read_text()))

    def test_policy_configures_before_runpy_and_restores_afterward(self):
        for use_policy in (False, True):
            with self.subTest(policy=use_policy):
                policy = self.bundle / 'worker-directory-policy.json'
                if use_policy:
                    policy.write_text('{"scope":"native-safe-parent-open-v1"}')
                previous = worker._directory_opener
                starts = []

                class Cache:
                    def __init__(self, path, reader, **kwargs):
                        self.read_json = reader
                    def start(self):
                        starts.append(True)
                    def close(self):
                        pass

                def run(*args, **kwargs):
                    self.assertEqual(len(starts), 1)
                    self.assertIs(worker._directory_opener,
                                  native.open_parent_readonly if use_policy else None)

                with mock.patch.object(cached, '__file__', str(self.bundle / 'migration_cached_worker.py')), \
                        mock.patch.object(cached, 'StateCache', Cache), \
                        mock.patch.object(cached.runpy, 'run_path', side_effect=run) as runner:
                    self.assertEqual(cached.main(['primary', '--control', str(self.control)]), 0)
                    runner.assert_called_once()
                self.assertIs(worker._directory_opener, previous)

    def test_invalid_policy_rejected_before_cache_or_runpy(self):
        (self.bundle / 'worker-directory-policy.json').write_text('{"scope":"invalid"}')
        with mock.patch.object(cached, '__file__', str(self.bundle / 'migration_cached_worker.py')), \
                mock.patch.object(cached, 'StateCache') as cache, \
                mock.patch.object(cached.runpy, 'run_path') as runner:
            with self.assertRaises(ValueError):
                cached.main(['primary', '--control', str(self.control)])
            cache.assert_not_called()
            runner.assert_not_called()


if __name__ == '__main__':
    unittest.main()
