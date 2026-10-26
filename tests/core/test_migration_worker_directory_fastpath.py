"""Real local directory syscalls and copy-safety regressions; no deployment."""
import concurrent.futures
import errno
import hashlib
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

import migration_worker as worker


class DirectoryFastpathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='copy-open-first-', dir='/tmp')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base / 'destination'
        self.root.mkdir()

    def test_existing_root_parent_and_ensure_do_not_call_mkdir(self):
        (self.root / 'one' / 'two').mkdir(parents=True)
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            fd = worker._open_root(self.root, create=True)
            os.close(fd)
            fd, name = worker._open_parent(self.root, 'one/two/file', create=True)
            self.assertEqual(name, 'file')
            self.assertEqual(os.fstat(fd).st_ino, (self.root / 'one/two').stat().st_ino)
            os.close(fd)
            result = worker.ensure_destination_directory(self.root, 'one/two')
        self.assertEqual(result['ino'], (self.root / 'one/two').stat().st_ino)
        self.assertEqual(mkdir.call_count, 0)

    def test_missing_root_and_directories_are_created_once_each(self):
        root = self.base / 'new' / 'root'
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            result = worker.ensure_destination_directory(root, 'one/two')
        self.assertEqual([call.args[0] for call in mkdir.call_args_list],
                         ['new', 'root', 'one', 'two'])
        self.assertEqual(result['ino'], (root / 'one/two').stat().st_ino)

    def test_missing_parent_creates_directories_but_never_payload_name(self):
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            fd, name = worker._open_parent(self.root, 'one/two/file', create=True)
            os.close(fd)
        self.assertEqual(name, 'file')
        self.assertEqual([call.args[0] for call in mkdir.call_args_list], ['one', 'two'])
        self.assertFalse((self.root / 'one/two/file').exists())

    def test_read_only_missing_directory_does_not_create(self):
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            with self.assertRaises(FileNotFoundError):
                worker._open_parent(self.root, 'absent/file', create=False)
        mkdir.assert_not_called()

    def test_concurrent_directory_creation_tolerates_eexist(self):
        count = 12
        barrier = threading.Barrier(count)
        real_open = os.open

        def synchronized_open(name, flags, *args, **kwargs):
            try:
                return real_open(name, flags, *args, **kwargs)
            except FileNotFoundError:
                if name == 'shared':
                    barrier.wait(timeout=5)
                raise

        with mock.patch.object(worker.os, 'open', side_effect=synchronized_open), \
             mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
                futures = [pool.submit(worker.ensure_destination_directory,
                                       self.root, 'shared/nested') for _ in range(count)]
                results = [future.result(timeout=10) for future in futures]
        expected = (self.root / 'shared/nested').stat().st_ino
        self.assertTrue(all(result['ino'] == expected for result in results))
        # The barrier forces every caller to observe the same missing name;
        # only one mkdir can win, and all other EEXIST results must be safe.
        self.assertEqual(sum(call.args[0] == 'shared' for call in mkdir.call_args_list), count)

    def test_open_errors_other_than_enoent_never_attempt_mkdir(self):
        real_open = os.open
        for code in (errno.EACCES, errno.ENOTDIR, errno.ELOOP):
            with self.subTest(errno=code):
                def denied(name, flags, *args, **kwargs):
                    if name == 'blocked':
                        raise OSError(code, os.strerror(code))
                    return real_open(name, flags, *args, **kwargs)
                with mock.patch.object(worker.os, 'open', side_effect=denied), \
                     mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
                    with self.assertRaises(OSError) as error:
                        worker.ensure_destination_directory(self.root, 'blocked')
                self.assertEqual(error.exception.errno, code)
                mkdir.assert_not_called()

    def test_mkdir_permission_error_is_not_swallowed(self):
        with mock.patch.object(worker.os, 'mkdir',
                               side_effect=PermissionError(errno.EACCES, 'denied')):
            with self.assertRaises(PermissionError):
                worker.ensure_destination_directory(self.root, 'missing')

    def test_symlink_components_are_rejected_without_mkdir(self):
        outside = self.base / 'outside'
        outside.mkdir()
        (self.root / 'unsafe').symlink_to(outside, target_is_directory=True)
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            with self.assertRaises(OSError):
                worker.ensure_destination_directory(self.root, 'unsafe/new')
            with self.assertRaises(OSError):
                worker._open_root(self.root / 'unsafe', create=True)
            with self.assertRaises(OSError):
                worker._open_parent(self.root, 'unsafe/file', create=True)
        mkdir.assert_not_called()
        self.assertEqual(list(outside.iterdir()), [])

    def test_regular_file_component_is_not_treated_as_missing(self):
        (self.root / 'file').write_bytes(b'preserve')
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            with self.assertRaises(NotADirectoryError):
                worker.ensure_destination_directory(self.root, 'file/new')
        mkdir.assert_not_called()
        self.assertEqual((self.root / 'file').read_bytes(), b'preserve')

    def test_symlink_inserted_between_enoent_and_mkdir_is_rejected(self):
        outside = self.base / 'outside'
        outside.mkdir()
        real_mkdir = os.mkdir

        def insert_symlink(name, *args, **kwargs):
            if name == 'raced':
                os.symlink(outside, name, dir_fd=kwargs['dir_fd'])
            return real_mkdir(name, *args, **kwargs)

        with mock.patch.object(worker.os, 'mkdir', side_effect=insert_symlink):
            with self.assertRaises(OSError):
                worker.ensure_destination_directory(self.root, 'raced/new')
        self.assertTrue((self.root / 'raced').is_symlink())
        self.assertEqual(list(outside.iterdir()), [])

    def test_rename_and_symlink_replacement_after_mkdir_is_rejected(self):
        outside = self.base / 'outside'
        outside.mkdir()
        real_mkdir = os.mkdir

        def replace_created_directory(name, *args, **kwargs):
            result = real_mkdir(name, *args, **kwargs)
            if name == 'raced':
                fd = kwargs['dir_fd']
                os.rename(name, 'moved', src_dir_fd=fd, dst_dir_fd=fd)
                os.symlink(outside, name, dir_fd=fd)
            return result

        with mock.patch.object(worker.os, 'mkdir', side_effect=replace_created_directory):
            with self.assertRaises(OSError):
                worker.ensure_destination_directory(self.root, 'raced/new')
        self.assertTrue((self.root / 'moved').is_dir())
        self.assertFalse((self.root / 'moved/new').exists())
        self.assertEqual(list(outside.iterdir()), [])

    def test_open_fd_remains_anchored_when_ancestor_is_renamed(self):
        (self.root / 'raced').mkdir()
        outside = self.base / 'outside'
        outside.mkdir()
        real_open = os.open
        replaced = False

        def replace_after_open(name, flags, *args, **kwargs):
            nonlocal replaced
            fd = real_open(name, flags, *args, **kwargs)
            if name == 'raced' and not replaced:
                replaced = True
                parent = kwargs['dir_fd']
                os.rename(name, 'moved', src_dir_fd=parent, dst_dir_fd=parent)
                os.symlink(outside, name, dir_fd=parent)
            return fd

        with mock.patch.object(worker.os, 'open', side_effect=replace_after_open):
            result = worker.ensure_destination_directory(self.root, 'raced/new')
        self.assertTrue(replaced)
        self.assertEqual(result['ino'], (self.root / 'moved/new').stat().st_ino)
        self.assertEqual(list(outside.iterdir()), [])


class CopyFastpathRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='copy-open-first-data-', dir='/tmp')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.source, self.destination = self.base / 'source', self.base / 'destination'
        self.source.mkdir()
        self.destination.mkdir()
        worker.configure_rate_limit(None)
        worker.configure_cancel_event(None)
        worker.configure_progress_callback(None)

    def entry(self, rel, body):
        path = self.source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return dict(worker._record(path.stat()), rel=rel, kind='file')

    def test_copy_hash_receipt_and_reuse_preserve_verification(self):
        body = b'content with verified receipt\x00' * 100
        entry = self.entry('one/two/file', body)
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            copied = worker.copy_entry(self.source, self.destination, entry, 'fastpath-test')
        self.assertEqual([call.args[0] for call in mkdir.call_args_list], ['one', 'two'])
        self.assertEqual(copied['status'], 'copied', copied)
        self.assertEqual(copied['bytes'], len(body))
        self.assertEqual(copied['sha256'], hashlib.sha256(body).hexdigest())
        destination = self.destination / entry['rel']
        self.assertEqual(destination.read_bytes(), body)
        self.assertEqual(copied['source_identity']['ino'], entry['ino'])
        self.assertEqual(copied['dst_stat']['ino'], destination.stat().st_ino)
        self.assertEqual(copied['dst_stat']['ctime_ns'], destination.stat().st_ctime_ns)
        with mock.patch.object(worker.os, 'mkdir', wraps=os.mkdir) as mkdir:
            reused = worker.copy_entry(self.source, self.destination, entry, 'fastpath-test')
        mkdir.assert_not_called()
        self.assertEqual(reused['status'], 'skipped', reused)
        self.assertEqual(reused['bytes'], 0)
        self.assertEqual(reused['sha256'], copied['sha256'])
        self.assertEqual(reused['dst_stat']['ino'], copied['dst_stat']['ino'])
        self.assertEqual(list(destination.parent.iterdir()), [destination])

    def test_different_existing_copy_is_not_overwritten(self):
        entry = self.entry('file', b'new content')
        (self.destination / 'file').write_bytes(b'old content')
        result = worker.copy_entry(self.source, self.destination, entry, 'fastpath-test')
        self.assertEqual(result['status'], 'conflict', result)
        self.assertEqual((self.destination / 'file').read_bytes(), b'old content')

    def test_destination_symlink_cannot_redirect_copy(self):
        entry = self.entry('unsafe/file', b'copy content')
        outside = self.base / 'outside'
        outside.mkdir()
        (self.destination / 'unsafe').symlink_to(outside, target_is_directory=True)
        result = worker.copy_entry(self.source, self.destination, entry, 'fastpath-test')
        self.assertEqual(result['status'], 'error', result)
        self.assertEqual(list(outside.iterdir()), [])


if __name__ == '__main__':
    unittest.main()
