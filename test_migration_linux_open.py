import concurrent.futures
import ctypes
import errno
import os
import pathlib
import tempfile
import unittest
from unittest import mock

import migration_linux_open as opening


class ParentOpenTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary.name)
        self.parent = self.root / 'a' / 'b'
        self.parent.mkdir(parents=True)
        self.unavailable = opening._openat2_unavailable
        self.succeeded = opening._openat2_succeeded
        opening._openat2_unavailable = False
        opening._openat2_succeeded = False

    def tearDown(self):
        opening._openat2_unavailable = self.unavailable
        opening._openat2_succeeded = self.succeeded
        self.temporary.cleanup()

    def assert_open_parent(self, root=None, rel='a/b/payload'):
        fd, name = opening.open_parent_readonly(root or self.root, rel)
        try:
            expected = os.stat(self.parent)
            actual = os.fstat(fd)
            self.assertEqual((expected.st_dev, expected.st_ino),
                             (actual.st_dev, actual.st_ino))
            self.assertEqual(name, 'payload')
            self.assertFalse(os.get_inheritable(fd))
        finally:
            os.close(fd)

    def test_actual_kernel_parent_identity_and_cloexec(self):
        self.assert_open_parent()
        self.assert_open_parent(os.fsencode(self.root))

    def test_no_payload_open_or_creation(self):
        self.assert_open_parent()
        self.assertFalse((self.parent / 'payload').exists())
        (self.parent / 'payload').symlink_to('/unneeded/payload')
        # Only ancestors, not the eventual filename, are opened here.
        self.assert_open_parent()

    def test_root_itself_for_single_component_rel(self):
        fd, name = opening.open_parent_readonly(self.root, 'payload')
        try:
            self.assertEqual(os.fstat(fd).st_ino, os.stat(self.root).st_ino)
            self.assertEqual(name, 'payload')
        finally:
            os.close(fd)

    def test_relative_root_matches_abspath_normalization(self):
        relative = os.path.relpath(self.root)
        self.assert_open_parent(relative)

    def test_invalid_relative_paths_fail_before_syscall(self):
        invalid = ('', '.', '..', '/payload', 'a//payload', 'a/./payload',
                   'a/../payload', 'a/b/', 'a/\x00/payload', b'a/b', None)
        with mock.patch.object(opening, '_open_absolute_directory') as op:
            for rel in invalid:
                with self.subTest(rel=rel), self.assertRaises(ValueError):
                    opening.open_parent_readonly(self.root, rel)
            op.assert_not_called()

    def test_nul_root_is_rejected_before_c_string_truncation(self):
        with mock.patch.object(opening, '_open_absolute_directory') as op:
            with self.assertRaises(ValueError):
                opening.open_parent_readonly(str(self.root) + '\x00/other',
                                             'a/b/payload')
            op.assert_not_called()

    def test_real_parent_symlink_rejected_in_kernel_and_fallback(self):
        (self.root / 'link').symlink_to(self.parent, target_is_directory=True)
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                with mock.patch.object(opening, '_openat2_unavailable', fallback):
                    with self.assertRaises(OSError) as caught:
                        opening.open_parent_readonly(self.root, 'link/payload')
                    self.assertIn(caught.exception.errno,
                                  (errno.ELOOP, errno.ENOTDIR))

    def test_symlink_in_root_ancestors_rejected(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.root / 'a', target_is_directory=True)
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                with mock.patch.object(opening, '_openat2_unavailable', fallback):
                    with self.assertRaises(OSError) as caught:
                        opening.open_parent_readonly(alias / 'b', 'payload')
                    self.assertIn(caught.exception.errno,
                                  (errno.ELOOP, errno.ENOTDIR))

    @unittest.skipUnless(os.path.isdir('/proc/self/fd'), 'Linux procfs required')
    def test_magic_link_root_rejected(self):
        original = os.open(self.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            for fallback in (False, True):
                with self.subTest(fallback=fallback):
                    with mock.patch.object(opening, '_openat2_unavailable', fallback):
                        with self.assertRaises(OSError):
                            opening.open_parent_readonly('/proc/self/fd/' +
                                                         str(original), 'payload')
        finally:
            os.close(original)

    def test_missing_and_nondirectory_ancestors_are_not_created(self):
        (self.root / 'file').write_text('ordinary file')
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                with mock.patch.object(opening, '_openat2_unavailable', fallback):
                    for rel, expected in (('missing/payload', errno.ENOENT),
                                          ('file/payload', errno.ENOTDIR)):
                        with self.assertRaises(OSError) as caught:
                            opening.open_parent_readonly(self.root, rel)
                        self.assertEqual(caught.exception.errno, expected)
        self.assertFalse((self.root / 'missing').exists())

    def test_syscall_abi_flags_and_how_size(self):
        def fake(number, directory, path, how_pointer, size):
            self.assertEqual(number.value, 437)
            self.assertEqual(directory.value, -100)
            self.assertEqual(os.fsdecode(path.value), str(self.parent))
            self.assertEqual(size.value, 24)
            how = ctypes.cast(how_pointer,
                              ctypes.POINTER(opening._OpenHow)).contents
            self.assertEqual(how.mode, 0)
            self.assertEqual(how.resolve, 0x06)
            self.assertEqual(how.flags, opening._directory_flags())
            return os.open(self.parent, opening._directory_flags())

        with mock.patch.object(opening, '_syscall', fake), \
                mock.patch.object(opening, '_syscall_number', 437):
            self.assert_open_parent()

    def test_unsupported_syscall_errors_fallback_and_disable(self):
        for code in (errno.ENOSYS, errno.EINVAL, errno.E2BIG, errno.EPERM):
            with self.subTest(errno=code):
                opening._openat2_unavailable = False

                def unsupported(*args):
                    ctypes.set_errno(code)
                    return -1

                with mock.patch.object(opening, '_syscall',
                                       side_effect=unsupported) as syscall:
                    self.assert_open_parent()
                    self.assert_open_parent()
                    self.assertEqual(syscall.call_count, 1)
                    self.assertTrue(opening._openat2_unavailable)

    def test_ordinary_errno_is_preserved_without_fallback(self):
        for code in (errno.EACCES, errno.ENOENT, errno.ENOTDIR, errno.ELOOP,
                     errno.EMFILE, errno.ENFILE, errno.EIO):
            with self.subTest(errno=code):
                def failure(*args):
                    ctypes.set_errno(code)
                    return -1

                with mock.patch.object(opening, '_syscall', failure), \
                        mock.patch.object(opening, '_walk_absolute_directory') as walk:
                    with self.assertRaises(OSError) as caught:
                        opening.open_parent_readonly(self.root, 'a/b/payload')
                    self.assertEqual(caught.exception.errno, code)
                    self.assertEqual(caught.exception.filename, str(self.parent))
                    self.assertFalse(opening._openat2_unavailable)
                    walk.assert_not_called()

    def test_eintr_retries_the_same_restricted_syscall(self):
        calls = []

        def interrupted_then_success(*args):
            calls.append(args)
            if len(calls) == 1:
                ctypes.set_errno(errno.EINTR)
                return -1
            return os.open(self.parent, opening._directory_flags())

        with mock.patch.object(opening, '_syscall', interrupted_then_success):
            self.assert_open_parent()
            self.assertEqual(len(calls), 2)
            self.assertFalse(opening._openat2_unavailable)

    def test_backend_is_reported_only_after_real_success(self):
        with mock.patch.object(opening, '_syscall', None):
            self.assertEqual(opening.safe_open_backend(), 'fallback')
        calls = []

        def successful(*args):
            calls.append(args)
            return os.open('/', opening._directory_flags())

        with mock.patch.object(opening, '_syscall', successful):
            self.assertEqual(opening.safe_open_backend(), 'openat2')
            self.assertEqual(opening.safe_open_backend(), 'openat2')
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][2].value, b'/')
        opening._openat2_succeeded = False

        def unsupported(*args):
            ctypes.set_errno(errno.ENOSYS)
            return -1

        with mock.patch.object(opening, '_syscall', unsupported):
            self.assertEqual(opening.safe_open_backend(), 'fallback')
            self.assertFalse(opening._openat2_succeeded)

    def test_unsupported_platform_and_arch_do_not_load_ctypes(self):
        for system, machine in (('darwin', 'x86_64'), ('linux', 'mips64'),
                                ('linux', 'i686')):
            with self.subTest(system=system, machine=machine):
                with mock.patch.object(opening.sys, 'platform', system), \
                        mock.patch.object(opening.platform, 'machine',
                                          return_value=machine), \
                        mock.patch.object(opening.ctypes, 'CDLL') as library:
                    self.assertEqual(opening._load_syscall(), (None, None))
                    library.assert_not_called()
        with mock.patch.object(opening, '_syscall', None):
            self.assert_open_parent()

    def test_unavailable_libc_falls_back(self):
        with mock.patch.object(opening.sys, 'platform', 'linux'), \
                mock.patch.object(opening.platform, 'machine',
                                  return_value='x86_64'), \
                mock.patch.object(opening.ctypes, 'CDLL', side_effect=OSError('missing')):
            self.assertEqual(opening._load_syscall(), (None, None))

    def test_fallback_closes_all_fds_when_component_open_fails(self):
        real_open = os.open
        created = []

        def instrumented(path, flags, **kwargs):
            fd = real_open(path, flags, **kwargs)
            created.append(fd)
            return fd

        with mock.patch.object(opening, '_syscall', None), \
                mock.patch.object(opening.os, 'open', side_effect=instrumented):
            with self.assertRaises(FileNotFoundError):
                opening.open_parent_readonly(self.root, 'a/missing/payload')
        self.assertGreater(len(created), 1)
        for fd in created:
            with self.assertRaises(OSError) as caught:
                os.fstat(fd)
            self.assertEqual(caught.exception.errno, errno.EBADF)

    @unittest.skipUnless(os.path.isdir('/proc/self/fd'), 'Linux procfs required')
    def test_concurrent_real_opens_do_not_leak_fds(self):
        before = len(os.listdir('/proc/self/fd'))
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(lambda _: self.assert_open_parent(), range(256)))
        self.assertEqual(len(os.listdir('/proc/self/fd')), before)


if __name__ == '__main__':
    unittest.main()
