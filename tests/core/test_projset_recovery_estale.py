"""Exercise NFS stale-handle retries using isolated source and target trees."""
import errno
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'src/core'))
spec = importlib.util.spec_from_file_location(
    'projset_recovery_estale', REPO / 'ops/copy/projset_recovery_copy.py')
copy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(copy)


def stale():
    return OSError(errno.ESTALE, os.strerror(errno.ESTALE))


def entry(path):
    return dict(copy.engine._record(path.stat()), rel=path.name, kind='file')


def immediate(_delay):
    return False


class EntriesWithStaleStat:
    """Make the first stat of each real DirEntry fail with ESTALE."""
    def __init__(self, original, directory_fd):
        self.original = original
        self.directory_fd = directory_fd

    def __enter__(self):
        self.context = self.original(self.directory_fd)
        return (self.wrap(item) for item in self.context.__enter__())

    def __exit__(self, *args):
        return self.context.__exit__(*args)

    @staticmethod
    def wrap(item):
        class StaleEntry:
            name = item.name

            def stat(self, *, follow_symlinks):
                raise stale()
        return StaleEntry()


class StaleRetryTests(unittest.TestCase):
    def test_retry_after_stale_copies_and_verifies_original_file(self):
        with tempfile.TemporaryDirectory() as folder:
            src, dst = Path(folder) / 'src', Path(folder) / 'dst'
            src.mkdir(); dst.mkdir(); (src / 'a').write_bytes(b'payload')
            original = copy.engine.copy_entry
            calls = []

            def transient(*args, **kwargs):
                calls.append(1)
                if len(calls) == 1:
                    return {'status': 'error', 'detail': 'OSError: [Errno 116] Stale file handle'}
                return original(*args, **kwargs)

            with mock.patch.object(copy.engine, 'copy_entry', side_effect=transient):
                result = copy.copy_entry_with_estale_retry(str(src), str(dst), entry(src / 'a'),
                                                          'test', immediate)
            self.assertEqual(result['status'], 'copied')
            self.assertEqual(result['estale_retries'], 1)
            self.assertEqual((dst / 'a').read_bytes(), b'payload')
            self.assertEqual(len(calls), 2)

    def test_retry_preserves_a_new_destination_conflict_and_changed_source(self):
        for change in ('destination', 'source'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                src, dst = Path(folder) / 'src', Path(folder) / 'dst'
                src.mkdir(); dst.mkdir(); (src / 'a').write_bytes(b'payload')
                snapshot = entry(src / 'a')
                original = copy.engine.copy_entry
                calls = []

                def transient(*args, **kwargs):
                    calls.append(1)
                    if len(calls) == 1:
                        (dst / 'a' if change == 'destination' else src / 'a').write_bytes(b'new data')
                        return {'status': 'error', 'detail': 'OSError: [Errno 116] Stale file handle'}
                    return original(*args, **kwargs)

                with mock.patch.object(copy.engine, 'copy_entry', side_effect=transient):
                    result = copy.copy_entry_with_estale_retry(str(src), str(dst), snapshot, 'test', immediate)
                self.assertEqual(result['status'], 'conflict' if change == 'destination' else 'changed')
                if change == 'destination':
                    self.assertEqual((dst / 'a').read_bytes(), b'new data')
                else:
                    self.assertFalse((dst / 'a').exists())

    def test_retry_is_bounded_and_other_errors_and_stop_do_not_retry(self):
        with mock.patch.object(copy.engine, 'copy_entry',
                               return_value={'status': 'error', 'detail': 'OSError: [Errno 116] Stale file handle'}) as op:
            result = copy.copy_entry_with_estale_retry('s', 'd', {}, 'r', immediate)
            self.assertEqual(op.call_count, 1 + len(copy.ESTALE_RETRY_DELAYS))
            self.assertEqual(result['estale_retries'], len(copy.ESTALE_RETRY_DELAYS))
            op.reset_mock()
            copy.copy_entry_with_estale_retry('s', 'd', {}, 'r', lambda _delay: True)
            self.assertEqual(op.call_count, 1)
        with mock.patch.object(copy.engine, 'copy_entry',
                               return_value={'status': 'error', 'detail': 'OSError: [Errno 13] Permission denied'}) as op:
            copy.copy_entry_with_estale_retry('s', 'd', {}, 'r', immediate)
            self.assertEqual(op.call_count, 1)
        with self.assertRaises(PermissionError):
            copy.retry_estale(lambda: (_ for _ in ()).throw(PermissionError(errno.EACCES, 'denied')), immediate)

    def test_scan_reopens_a_stale_root_and_child_without_duplicate_tasks(self):
        with tempfile.TemporaryDirectory() as folder:
            src, dst = Path(folder) / 'src', Path(folder) / 'dst'
            src.mkdir(); dst.mkdir()
            (src / 'a').write_bytes(b'a'); (src / 'sub').mkdir(); (src / 'link').symlink_to('a')
            original_open, original_scandir = copy.native._open_absolute_directory, os.scandir
            open_calls = []

            def transient_open(path, *args, **kwargs):
                if path == str(src):
                    open_calls.append(1)
                    if len(open_calls) == 1:
                        raise stale()
                return original_open(path, *args, **kwargs)

            jobs, issues = [], []
            with mock.patch.object(copy.native, '_open_absolute_directory', side_effect=transient_open), \
                 mock.patch.object(copy.os, 'scandir', side_effect=lambda fd: EntriesWithStaleStat(original_scandir, fd)):
                result = copy.scan_directory(str(src), str(dst), '', lambda d, e: jobs.append((d, e)),
                                             issues.append, retry_wait=immediate)
            self.assertFalse(issues)
            self.assertEqual(result['estale_retries'], 4)  # root and three child lookups
            children = [x['rel'] for ds, _ in jobs for x in ds]
            entries = [x for _, es in jobs for x in es]
            self.assertEqual(children, ['sub'])
            self.assertCountEqual([x['rel'] for x in entries], ['a', 'link'])
            self.assertEqual(next(x for x in entries if x['rel'] == 'link')['target'], 'a')

    def test_scan_refuses_reopened_parent_with_changed_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            src, dst, other = (Path(folder) / n for n in ('src', 'dst', 'other'))
            src.mkdir(); dst.mkdir(); other.mkdir()
            (src / 'a').write_bytes(b'original'); (other / 'a').write_bytes(b'unrelated')
            original_scandir = os.scandir
            original_parent = copy.native.open_parent_readonly

            def replaced(root, rel):
                if root == str(src) and rel == 'a':
                    return os.open(other, os.O_RDONLY | os.O_DIRECTORY), 'a'
                return original_parent(root, rel)

            jobs, issues = [], []
            with mock.patch.object(copy.native, 'open_parent_readonly', side_effect=replaced), \
                 mock.patch.object(copy.os, 'scandir', side_effect=lambda fd: EntriesWithStaleStat(original_scandir, fd)):
                copy.scan_directory(str(src), str(dst), '', lambda d, e: jobs.append((d, e)), issues.append,
                                    retry_wait=immediate)
            self.assertFalse(jobs)
            self.assertEqual(len(issues), 1)
            self.assertEqual(issues[0]['status'], 'error')
            self.assertIn('source directory replaced', issues[0]['detail'])

    def test_target_estale_is_an_error_and_source_scan_continues(self):
        with tempfile.TemporaryDirectory() as folder:
            src, dst = Path(folder) / 'src', Path(folder) / 'dst'
            src.mkdir(); dst.mkdir(); (src / 'file').write_bytes(b'payload')
            jobs, issues = [], []
            def fail(*_args):
                raise stale()
            with mock.patch.object(copy.engine, 'ensure_destination_directory', side_effect=fail) as op:
                result = copy.scan_directory(str(src), str(dst), '',
                                             lambda d, e: jobs.extend(e), issues.append,
                                             retry_wait=immediate)
            self.assertEqual(op.call_count, 1 + len(copy.ESTALE_RETRY_DELAYS))
            self.assertEqual(result['estale_retries'], len(copy.ESTALE_RETRY_DELAYS))
            self.assertEqual([row['rel'] for row in jobs], ['file'])
            self.assertEqual(len(issues), 1)
            self.assertEqual(issues[0]['status'], 'error')
            self.assertIn('[Errno 116]', issues[0]['detail'])


if __name__ == '__main__':
    unittest.main()
