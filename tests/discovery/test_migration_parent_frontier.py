from concurrent.futures import ThreadPoolExecutor
import errno
import fcntl
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import migration_parent_frontier as frontier
import migration_linux_open as native
import migration_worker as engine


class FrontierParentTests(unittest.TestCase):
    def test_only_missing_suffix_is_created_and_returned_fd_is_readonly_directory(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            (root / 'a/b').mkdir(parents=True)
            observed = []
            def opener(base, rel):
                observed.append(rel)
                return native.open_parent_readonly(base, rel)
            fn = frontier.FrontierParentOpener(root, engine._open_parent, opener=opener)
            fd, leaf = fn(str(root), 'a/b/c/d/f', create=True)
            try:
                self.assertEqual(leaf, 'f')
                self.assertEqual(os.fstat(fd).st_ino, (root / 'a/b/c/d').stat().st_ino)
                self.assertEqual(observed, ['a/b/c/d/f', 'a/b/c/f', 'a/b/f'])
                self.assertEqual(os.listdir(fd), [])
                self.assertFalse((root / 'a/b/c/d/f').exists())
            finally:
                os.close(fd)

    def test_existing_parent_takes_one_probe_and_no_mkdir(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            (root / 'a/b').mkdir(parents=True)
            probes = []
            def opener(base, rel):
                probes.append(rel)
                return native.open_parent_readonly(base, rel)
            fn = frontier.FrontierParentOpener(root, engine._open_parent, opener=opener)
            with patch.object(fn.os, 'mkdir', side_effect=AssertionError('mkdir not allowed')):
                fd, _ = fn(root, 'a/b/f', create=True)
                os.close(fd)
            self.assertEqual(probes, ['a/b/f'])

    def test_source_and_create_false_delegate_without_creating(self):
        calls = []
        fn = frontier.FrontierParentOpener('/destination',
            lambda *args, **kwargs: calls.append((args, kwargs)) or ('delegated', 'f'))
        self.assertEqual(fn('/source', 'missing/f', create=True), ('delegated', 'f'))
        self.assertEqual(fn('/destination', 'missing/f'), ('delegated', 'f'))
        self.assertEqual(calls[0][1], {'create': True})
        self.assertEqual(calls[1][1], {'create': False})

    def test_missing_root_is_not_created(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name) / 'missing'
            fn = frontier.FrontierParentOpener(root, engine._open_parent)
            for rel in ('f', 'a/b/f'):
                with self.assertRaises(FileNotFoundError):
                    fn(root, rel, create=True)
            self.assertFalse(root.exists())

    def test_unsafe_relatives_and_symlinks_are_not_followed(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            (root / 'real').mkdir()
            (root / 'link').symlink_to(root / 'real', target_is_directory=True)
            fn = frontier.FrontierParentOpener(root, engine._open_parent)
            for rel in ('', '.', '../f', '/a/f', 'a//f', 'a/./f', 'a/../f', 'a\x00/f'):
                with self.assertRaises(ValueError):
                    fn(root, rel, create=True)
            with self.assertRaises(OSError):
                fn(root, 'link/new/f', create=True)
            self.assertFalse((root / 'real/new').exists())

    def test_permissions_and_nonmissing_errors_never_fall_back(self):
        def denied(*args):
            raise PermissionError(errno.EACCES, 'fixture')
        fn = frontier.FrontierParentOpener('/destination', engine._open_parent, opener=denied)
        with self.assertRaises(PermissionError):
            fn('/destination', 'a/b/f', create=True)

    def test_concurrent_same_suffix_mkdir_is_single_flight_and_has_no_fd_cache(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            fn = frontier.FrontierParentOpener(root, engine._open_parent)
            mkdir, lock, calls = os.mkdir, threading.Lock(), []
            def measured(name, *args, **kwargs):
                with lock:
                    calls.append(name)
                return mkdir(name, *args, **kwargs)
            def open_and_close(_):
                fd, leaf = fn(root, 'a/b/c/f', create=True)
                try:
                    return os.fstat(fd).st_ino, leaf
                finally:
                    os.close(fd)
            with patch.object(fn.os, 'mkdir', measured):
                with ThreadPoolExecutor(max_workers=16) as pool:
                    results = list(pool.map(open_and_close, range(64)))
            self.assertEqual(set(calls), {'a', 'b', 'c'})
            self.assertEqual(len(calls), 3)
            self.assertEqual(len(set(results)), 1)
            self.assertFalse(any(isinstance(x, int) for x in fn.__dict__.values()))

    def test_cancellation_during_missing_suffix_does_not_leak_fd(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            checks = 0
            def check():
                nonlocal checks
                checks += 1
                if checks >= 4:
                    raise RuntimeError('cancelled fixture')
            fn = frontier.FrontierParentOpener(root, engine._open_parent, check=check)
            count = len(list(Path('/proc/self/fd').iterdir()))
            with self.assertRaisesRegex(RuntimeError, 'cancelled fixture'):
                fn(root, 'a/b/f', create=True)
            self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), count)

    def test_eexist_symlink_race_is_rejected_and_descriptor_closed(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            outside = root / 'outside'
            outside.mkdir()
            fn = frontier.FrontierParentOpener(root, engine._open_parent)
            def race(name, *args, **kwargs):
                os.symlink(str(outside), name, dir_fd=kwargs['dir_fd'])
                raise FileExistsError(errno.EEXIST, 'external creator')
            count = len(list(Path('/proc/self/fd').iterdir()))
            with patch.object(fn.os, 'mkdir', race):
                with self.assertRaises(OSError):
                    fn(root, 'a/b/f', create=True)
            self.assertFalse((outside / 'b').exists())
            self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), count)

    def test_optional_path_anchor_does_not_return_path_descriptor(self):
        with tempfile.TemporaryDirectory(prefix='parent-frontier-fixture-') as name:
            root = Path(name)
            fn = frontier.FrontierParentOpener(root, engine._open_parent,
                                               anchor_opener=native.open_parent_path)
            fd, leaf = fn(root, 'a/b/f', create=True)
            try:
                flags = fcntl.fcntl(fd, fcntl.F_GETFL)
                self.assertEqual(flags & os.O_ACCMODE, os.O_RDONLY)
                self.assertFalse(flags & getattr(os, 'O_PATH', 0))
                self.assertEqual(os.listdir(fd), [])
                self.assertEqual(leaf, 'f')
            finally:
                os.close(fd)

    def test_bad_constructor_arguments_fail_before_filesystem_access(self):
        for value in (0, -1, 4097, True, 1.5):
            with self.assertRaises(ValueError):
                frontier.FrontierParentOpener('/destination', engine._open_parent, stripes=value)
        with self.assertRaises(TypeError):
            frontier.FrontierParentOpener('/destination', engine._open_parent, anchor_opener=False)


if __name__ == '__main__':
    unittest.main()
