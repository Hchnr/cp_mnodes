"""Linux integration of the two read-only P4 proof components in owned /tmp."""
import ctypes
import os
from pathlib import Path
import sys
import unittest
from unittest import mock

import migration_priority4_file_metadata as metadata
import migration_priority4_hardlink_proof as proof
import migration_worker as engine
from test_migration_priority4_hardlink_proof import Priority4HardlinkProofTests
from test_migration_priority4_worker import CONTROL


@unittest.skipUnless(sys.platform.startswith('linux') and hasattr(os, 'listxattr'),
                     'requires Linux ACL and xattr integration')
class CombinedProofTests(unittest.TestCase):
    # Reuse only the existing temporary-tree/anchored-FD fixture helpers, not
    # their test methods. No live control or migration payload is opened.
    setUp = Priority4HardlinkProofTests.setUp
    group = Priority4HardlinkProofTests.group
    alias = Priority4HardlinkProofTests.alias

    def equal_group(self):
        entry, canonical = self.group()
        source = self.source / entry['rel']
        target = self.destination / entry['hardlink_to']
        for path in (source, target):
            path.chmod(0o640)
            os.utime(path, ns=(1_600_000_000_100_000_000, 1_600_000_000_200_000_000))
        entry.update(engine._record(source.stat()))
        canonical['source'] = engine._record(source.stat())
        canonical['destination'] = engine._record(target.stat())
        return entry, canonical

    def verify(self, entry, canonical, **kwargs):
        with proof.HardlinkProofSession(CONTROL, self.cfg, self.stop) as session:
            return session.verify_with_metadata(entry, entry['hardlink_to'], canonical, **kwargs)

    def set_acl(self, path, uid):
        library = metadata._native_acl().library
        library.acl_from_text.argtypes = [ctypes.c_char_p]
        library.acl_from_text.restype = ctypes.c_void_p
        library.acl_set_fd.argtypes = [ctypes.c_int, ctypes.c_void_p]
        library.acl_set_fd.restype = ctypes.c_int
        acl = library.acl_from_text(
            ('user::rw-\nuser:' + str(uid) + ':r--\ngroup::r--\nmask::r--\nother::---\n').encode())
        self.assertTrue(acl)
        fd = os.open(path, os.O_RDONLY)
        try:
            self.assertEqual(library.acl_set_fd(fd, acl), 0)
        finally:
            os.close(fd)
            library.acl_free(acl)

    def test_same_fds_bind_current_sha_and_metadata_without_publication(self):
        entry, canonical = self.equal_group()
        with mock.patch.object(engine, '_hash_fd', wraps=engine._hash_fd) as hashed, \
                mock.patch.object(os, 'link', side_effect=AssertionError('link publication')), \
                mock.patch.object(engine, '_metadata_fd', side_effect=AssertionError('metadata write')):
            value = self.verify(entry, canonical)
        self.assertEqual(hashed.call_count, 2)
        self.assertEqual(value['alias_action'], 'missing_alias')
        observed = value['metadata_observation']
        self.assertTrue(observed['bound_to_current_checksum'])
        self.assertTrue(observed['canonical']['equivalent_observed_metadata'])
        self.assertEqual(observed['canonical']['source']['identity']['ino'], entry['ino'])
        self.assertFalse(value['publication_ready'])
        self.assertTrue(value['metadata_validation_required'])  # Atime/other ACL models still pending.
        self.assertEqual(value['hash_budget_bytes_per_second'], 32 * 1024 * 1024)
        self.assertFalse((self.destination / 'other').exists())

    def test_equal_content_with_different_mode_is_rejected(self):
        entry, canonical = self.equal_group()
        (self.destination / entry['hardlink_to']).chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'metadata differs:.*mode'):
            self.verify(entry, canonical)

    def test_current_xattr_difference_cannot_pass_the_content_proof(self):
        entry, canonical = self.equal_group()
        os.setxattr(self.destination / entry['hardlink_to'], 'user.combined-fixture', b'target-only')
        with self.assertRaisesRegex(ValueError, 'metadata differs:.*visible_xattrs'):
            self.verify(entry, canonical)

    def test_numeric_acl_difference_with_equal_mode_is_rejected(self):
        entry, canonical = self.equal_group()
        self.set_acl(self.source / entry['rel'], 12345)
        self.set_acl(self.destination / entry['hardlink_to'], 12346)
        entry.update(engine._record((self.source / entry['rel']).stat()))
        with self.assertRaisesRegex(ValueError, 'metadata differs:.*posix_access_acl'):
            self.verify(entry, canonical)

    def test_unowned_separate_alias_content_and_attributes_are_not_read(self):
        entry, canonical = self.equal_group()
        self.alias(entry, b'unowned-different-body')
        alias_inode = (self.destination / entry['rel']).stat().st_ino
        observed_inodes = []
        original = metadata.snapshot
        def observe(fd, **kwargs):
            observed_inodes.append(os.fstat(fd).st_ino)
            return original(fd, **kwargs)
        with mock.patch.object(metadata, 'snapshot', side_effect=observe), \
                mock.patch.object(engine, '_hash_fd', wraps=engine._hash_fd) as hashed:
            value = self.verify(entry, canonical)
        self.assertNotIn(alias_inode, observed_inodes)
        self.assertEqual(hashed.call_count, 2)
        self.assertEqual(value['alias_action'], 'preserve_unowned_alias')
        self.assertFalse(value['alias_owned'])
        self.assertIsNone(value['metadata_observation']['candidate_alias'])

    def test_owned_older_alias_keeps_its_stable_metadata_observation(self):
        entry, canonical = self.equal_group()
        owned = self.alias(entry, b'older-owned-body')
        path = self.destination / entry['rel']
        path.chmod(0o600)
        with mock.patch.object(engine, '_hash_fd', wraps=engine._hash_fd) as hashed:
            value = self.verify(entry, canonical, alias_proof=owned)
        self.assertEqual(hashed.call_count, 3)
        self.assertEqual(value['alias_action'], 'owned_alias')
        self.assertTrue(value['alias_owned'])
        self.assertEqual(value['metadata_observation']['candidate_alias']['mode'], 0o600)
        self.assertEqual(path.read_bytes(), b'older-owned-body')

    def test_metadata_change_during_checksum_invalidates_combined_proof(self):
        entry, canonical = self.equal_group()
        original = engine._hash_fd
        calls = []
        def mutate(fd):
            value = original(fd)
            calls.append(fd)
            if len(calls) == 2:
                os.setxattr(self.destination / entry['hardlink_to'], 'user.changed', b'late')
            return value
        with mock.patch.object(engine, '_hash_fd', side_effect=mutate):
            with self.assertRaises(metadata.MetadataChanged):
                self.verify(entry, canonical)

    def test_parent_exchange_after_metadata_observation_is_rejected(self):
        entry, canonical = self.equal_group()
        original = metadata.compare
        def exchange(*args, **kwargs):
            value = original(*args, **kwargs)
            parent = (self.destination / entry['hardlink_to']).parent
            parent.rename(parent.with_name('moved'))
            parent.mkdir()
            (parent / 'canonical').write_bytes(b'fixture-data')
            return value
        with mock.patch.object(metadata, 'compare', side_effect=exchange):
            with self.assertRaisesRegex(RuntimeError, 'parent binding changed'):
                self.verify(entry, canonical)

    def test_stop_during_metadata_aborts_and_restores_hash_budget(self):
        entry, canonical = self.equal_group()
        original = metadata.snapshot
        saved = engine._io_limiter.rate
        def cancel(*args, **kwargs):
            value = original(*args, **kwargs)
            self.stop.set()
            return value
        with mock.patch.object(metadata, 'snapshot', side_effect=cancel), \
                mock.patch.object(engine, '_hash_fd') as hashed:
            with self.assertRaises(metadata.MetadataCancelled):
                self.verify(entry, canonical)
        hashed.assert_not_called()
        self.assertEqual(engine._io_limiter.rate, saved)

    def test_unreadable_metadata_fails_before_checksum_or_publication(self):
        entry, canonical = self.equal_group()
        with mock.patch.object(os, 'listxattr', side_effect=PermissionError('fixture denial')), \
                mock.patch.object(engine, '_hash_fd') as hashed:
            with self.assertRaises(PermissionError):
                self.verify(entry, canonical)
        hashed.assert_not_called()


def load_tests(loader, tests, pattern):
    # The imported fixture class is not a second run of its original16 tests.
    return loader.loadTestsFromTestCase(CombinedProofTests)


if __name__ == '__main__':
    unittest.main()
