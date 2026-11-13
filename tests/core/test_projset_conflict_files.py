import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'src/core'))
import migration_linux_open as native

spec = importlib.util.spec_from_file_location(
    'projset_conflict_files', REPO / 'ops/diagnostics/projset_conflict_files.py')
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class ConflictFileTests(unittest.TestCase):
    @staticmethod
    def limits(max_bytes=10000, max_file_bytes=1000):
        return subject.Limits(10**12, 10**12, max_bytes, max_file_bytes)

    def test_equal_different_and_size_mismatch_without_modifying_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'src', Path(tmp) / 'dst'
            src.mkdir(); dst.mkdir()
            (src / 'a').write_bytes(b'abc'); (dst / 'a').write_bytes(b'abc')
            limits = self.limits()
            equal = subject.compare_file(native, str(src), str(dst), 'a', limits)
            self.assertEqual(equal['result'], 'same_sha256')
            self.assertEqual(equal['hash_passes'], 2)
            self.assertEqual(limits.reserved, 12)
            (dst / 'a').write_bytes(b'xyz')
            self.assertEqual(subject.compare_file(native, str(src), str(dst), 'a', self.limits())['result'],
                             'different_sha256')
            (dst / 'a').write_bytes(b'longer')
            self.assertEqual(subject.compare_file(native, str(src), str(dst), 'a', self.limits())['result'],
                             'different_size')
            self.assertEqual((src / 'a').read_bytes(), b'abc')
            self.assertEqual((dst / 'a').read_bytes(), b'longer')

    def test_limits_do_not_hash_large_or_over_budget_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'src', Path(tmp) / 'dst'
            src.mkdir(); dst.mkdir(); (src / 'a').write_bytes(b'12345'); (dst / 'a').write_bytes(b'12345')
            for limits in (self.limits(max_bytes=100, max_file_bytes=4), self.limits(max_bytes=9)):
                result = subject.compare_file(native, str(src), str(dst), 'a', limits)
                self.assertEqual(result['result'], 'not_hashed_limit')
                self.assertEqual(limits.reserved, 0)

    def test_changed_during_comparison_is_not_reported_as_content_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'src', Path(tmp) / 'dst'
            src.mkdir(); dst.mkdir(); (src / 'a').write_bytes(b'abc'); (dst / 'a').write_bytes(b'abc')
            original_read = os.read
            reads = []

            def mutate(fd, size):
                block = original_read(fd, size)
                reads.append(True)
                if len(reads) == 2:
                    (dst / 'a').write_bytes(b'xyz')
                return block

            with mock.patch.object(subject.os, 'read', side_effect=mutate):
                result = subject.compare_file(native, str(src), str(dst), 'a', self.limits())
            self.assertEqual(result['result'], 'changed_during_comparison')

    def test_symlink_ancestor_is_not_followed(self):
        with tempfile.TemporaryDirectory() as tmp:
            src, dst, outside = (Path(tmp) / n for n in ('src', 'dst', 'outside'))
            src.mkdir(); dst.mkdir(); outside.mkdir()
            (outside / 'file').write_bytes(b'private'); (src / 'link').symlink_to(outside)
            with self.assertRaises(OSError):
                subject.compare_file(native, str(src), str(dst), 'link/file', self.limits())
            self.assertEqual((outside / 'file').read_bytes(), b'private')


if __name__ == '__main__':
    unittest.main()
