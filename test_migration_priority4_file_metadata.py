"""Linux real temporary-file ACL/xattr fixtures; no migration roots are used."""
import ctypes
import errno
import os
from pathlib import Path
import stat
import sys
import tempfile
import threading
import unittest
from unittest import mock

import migration_priority4_file_metadata as metadata


@unittest.skipUnless(sys.platform.startswith('linux') and hasattr(os, 'listxattr'),
                     'requires Linux ACL and xattr integration')
class FileMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='p4-file-metadata-fixture-', dir='/tmp')
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / 'source'
        self.target = Path(self.temp.name) / 'target'
        self.source.write_bytes(b'local fixture')
        self.target.write_bytes(b'local fixture')
        for path in (self.source, self.target):
            path.chmod(0o640)
            os.utime(path, ns=(1_600_000_000_100_000_000, 1_600_000_000_200_000_000))
        self.src_fd, self.dst_fd = os.open(self.source, os.O_RDONLY), os.open(self.target, os.O_RDONLY)
        self.addCleanup(os.close, self.src_fd); self.addCleanup(os.close, self.dst_fd)
        self.reader = metadata.LinuxPosixACL()

    def compare(self, **kwargs):
        return metadata.compare(self.src_fd, self.dst_fd, **kwargs)

    def set_acl(self, fd, qualifier):
        library = self.reader.library
        library.acl_from_text.argtypes = [ctypes.c_char_p]
        library.acl_from_text.restype = ctypes.c_void_p
        library.acl_set_fd.argtypes = [ctypes.c_int, ctypes.c_void_p]
        library.acl_set_fd.restype = ctypes.c_int
        text = ('user::rw-\nuser:' + str(qualifier) + ':r--\ngroup::r--\nmask::r--\nother::---\n').encode()
        acl = library.acl_from_text(text)
        self.assertTrue(acl)
        try:
            self.assertEqual(library.acl_set_fd(fd, acl), 0)
        finally:
            library.acl_free(acl)

    def test_equal_current_metadata_is_observed_without_writing(self):
        before = (self.source.stat(), self.target.stat())
        metadata._native_acl()  # Resolve the library before blocking all Python FD reads.
        with mock.patch.object(os, 'read', side_effect=AssertionError('payload read')):
            result = self.compare()
        self.assertTrue(result['equivalent_observed_metadata'])
        self.assertFalse(result['publication_ready'])
        self.assertFalse(result['metadata_written'])
        self.assertFalse(result['payload_read'])
        for old, path in zip(before, (self.source, self.target)):
            self.assertEqual(metadata._signature(old), metadata._signature(path.stat()))
        self.assertEqual(result['source']['uid'], os.getuid())
        self.assertIn('atime_ns_observed', result['source'])

    def test_mode_difference_is_reported(self):
        self.target.chmod(0o600)
        self.assertIn('mode', self.compare()['differences'])

    def test_mtime_difference_is_reported(self):
        st = self.target.stat(); os.utime(self.target, ns=(st.st_atime_ns, st.st_mtime_ns + 1))
        self.assertIn('mtime_ns', self.compare()['differences'])

    def test_visible_xattr_values_are_compared_without_persisting_values(self):
        os.setxattr(self.source, 'user.p4-fixture', b'private-source-value')
        os.setxattr(self.target, 'user.p4-fixture', b'different-target-value')
        result = self.compare()
        self.assertIn('visible_xattrs', result['differences'])
        self.assertNotIn('private-source-value', str(result))
        os.setxattr(self.target, 'user.p4-fixture', b'private-source-value')
        self.assertTrue(self.compare()['equivalent_observed_metadata'])

    def test_missing_or_extra_xattrs_cannot_be_treated_as_equal(self):
        os.setxattr(self.source, 'user.source-only', b'one')
        os.setxattr(self.target, 'user.target-only', b'two')
        result = self.compare()
        self.assertIn('visible_xattrs', result['differences'])
        self.assertEqual(set(result['source']['visible_xattrs']), {'user.source-only'})
        self.assertEqual(set(result['destination']['visible_xattrs']), {'user.target-only'})

    def test_named_acl_ids_are_numeric_and_distinct_even_with_equal_mode(self):
        self.set_acl(self.src_fd, 12345); self.set_acl(self.dst_fd, 12346)
        self.assertEqual(stat.S_IMODE(self.source.stat().st_mode), stat.S_IMODE(self.target.stat().st_mode))
        self.assertIn(b'user:12345:r--', self.reader(self.src_fd))
        result = self.compare()
        self.assertIn('posix_access_acl', result['differences'])
        self.set_acl(self.dst_fd, 12345)
        self.assertTrue(self.compare()['equivalent_observed_metadata'])

    def test_unsupported_xattr_api_fails_closed(self):
        with mock.patch.object(os, 'listxattr', None):
            with self.assertRaises(metadata.MetadataUnavailable): self.compare()

    def test_xattr_permission_failure_is_not_an_empty_attribute_set(self):
        os.setxattr(self.source, 'user.p4-fixture', b'value')
        with mock.patch.object(os, 'getxattr', side_effect=PermissionError(errno.EACCES, 'fixture denial')):
            with self.assertRaises(PermissionError): self.compare()

    def test_acl_unsupported_or_permission_failure_is_not_an_empty_acl(self):
        for error in (errno.ENOTSUP, errno.EACCES):
            with self.subTest(error=error):
                reader = mock.Mock(side_effect=OSError(error, 'fixture ACL denial'))
                with self.assertRaises(OSError): self.compare(acl_reader=reader)

    def test_xattr_change_during_observation_is_rejected(self):
        def mutate(fd):
            os.setxattr(self.source, 'user.appeared', b'value')
            return self.reader(fd)
        with self.assertRaises(metadata.MetadataChanged):
            metadata.snapshot(self.src_fd, acl_reader=mutate)

    def test_mode_change_during_observation_is_rejected(self):
        def mutate(fd):
            self.source.chmod(0o600)
            return self.reader(fd)
        with self.assertRaises(metadata.MetadataChanged):
            metadata.snapshot(self.src_fd, acl_reader=mutate)

    def test_source_change_while_target_is_observed_is_rejected(self):
        changed = False
        def mutate(fd):
            nonlocal changed
            if fd == self.dst_fd and not changed:
                self.source.chmod(0o600); changed = True
            return self.reader(fd)
        with self.assertRaises(metadata.MetadataChanged): self.compare(acl_reader=mutate)

    def test_nonregular_and_closed_fds_are_rejected(self):
        directory = os.open(self.temp.name, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with self.assertRaises(ValueError): metadata.snapshot(directory)
        finally:
            os.close(directory)
        invalid = os.dup(self.src_fd); os.close(invalid)
        with self.assertRaises(OSError): metadata.snapshot(invalid)

    def test_stop_before_and_during_read_aborts_without_metadata_changes(self):
        stop = threading.Event(); stop.set()
        with self.assertRaises(metadata.MetadataCancelled): self.compare(stop=stop)
        stop.clear()
        before = metadata._signature(self.source.stat())
        def cancel(fd):
            stop.set()
            return self.reader(fd)
        with self.assertRaises(metadata.MetadataCancelled):
            self.compare(acl_reader=cancel, stop=stop)
        self.assertEqual(before, metadata._signature(self.source.stat()))


if __name__ == '__main__':
    unittest.main()
