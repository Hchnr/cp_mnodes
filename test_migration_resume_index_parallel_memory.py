"""Local equivalence, ordering, concurrency, and abort fixtures for parallel seed."""

import concurrent.futures
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

WORK = Path(__file__).absolute().parent
sys.path.insert(0, str(WORK))
import migration_resume_index as reference
import migration_resume_index_parallel_memory as parallel
from migration_io import write_json
from test_migration_resume_index import identity, result, receipt


class ParallelResumeTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=WORK, prefix='parallel-resume-fixture-')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.control = self.base / 'current'
        self.control.mkdir()

    def rows(self, path):
        with sqlite3.connect(path) as db:
            return db.execute('SELECT * FROM files ORDER BY rel').fetchall()

    def compare(self):
        first = reference.build_resume_index(self.control, self.base / 'reference.sqlite')
        second = parallel.build_resume_index(self.control, self.base / 'parallel.sqlite', read_workers=4)
        self.assertEqual(self.rows(first['db_path']), self.rows(second['db_path']))
        first.pop('db_path')
        second.pop('db_path')
        self.assertEqual(first, second)
        return second

    def test_mixed_ancestors_versions_proofs_errors_and_latest_immutable_equal_reference(self):
        old = self.base / 'old'
        old.mkdir()
        write_json(self.control / 'config.json', {'resume_from': str(old)})
        receipt(old / 'stages/p1-r1/done/0/1.json', [
            result('owned', ino=11), result('inode-changed', ino=21),
            result('sha-changed', ino=31), result('same-event', ino=41),
            result('warn', metadata_warnings=['fixture warning']),
            result('not-good', status='error')])
        d = self.control / 'stages/p3-r1/done'
        receipt(d / 'nested/2.json', [
            result('owned', status='verified', ino=11, stamp=9),
            result('inode-changed', status='verified', ino=22),
            result('sha-changed', status='verified', ino=31, digest='b' * 64),
            result('replay', ino=51, replay_reused_copy=True),
            result('same-event', status='verified', ino=41, stamp=2),
            result('../unsafe'), {'status': 'copied', 'rel': 'incomplete'},
            'not-result', result('bad-digest', digest='bad'),
            result('unknown-kind', kind='socket'),
            result('symlink', kind='symlink', sha256=None, target='../target',
                   source_identity=identity(500, mode=stat.S_IFLNK | 0o777),
                   dst_stat=identity(501, mode=stat.S_IFLNK | 0o777))], day=2)
        receipt(d / 'nested/3.json', [result('same-event', 'verified', ino=41, stamp=3)], day=2)
        latest = d / 'latest.json'
        receipt(latest, [result('immutable-latest', ino=61)], day=1)
        receipt(latest, [result('immutable-latest', 'verified', ino=61, stamp=99)], day=3)
        receipt(d / '.hidden/4.json', [result('hidden')])
        fallback = d / 'fallback.json'
        write_json(fallback, {'results': [result('fallback')]})
        os.utime(fallback, (1234, 1234))
        write_json(d / 'bad-results.json', {'results': 'bad'})
        write_json(d / 'bad-object.json', [])
        (d / 'bad-json.json').write_text('{broken')
        (d / 'directory.json').mkdir()
        (d / 'linked-file.json').symlink_to(latest)
        outside = self.base / 'outside'
        receipt(outside / 'ignore.json', [result('must-not-import')])
        (d / 'linked-tree').symlink_to(outside, target_is_directory=True)
        snapshots = d / '.snapshots'
        snapshots.mkdir(exist_ok=True)
        (snapshots / 'ignore.json').write_text('{should never enumerate}')
        self.assertEqual(parallel._receipt_paths(self.control), reference._receipt_paths(self.control))
        summary = self.compare()
        rows = {row[0]: row for row in self.rows(self.base / 'parallel.sqlite')}
        self.assertEqual(rows['owned'][3], 1)
        for rel in ('inode-changed', 'sha-changed', 'replay', 'immutable-latest'):
            self.assertEqual(rows[rel][3], 0)
        self.assertEqual(json.loads(rows['immutable-latest'][2])['mtime_ns'], 99)
        self.assertNotIn('must-not-import', rows)
        self.assertGreater(summary['issues'], 5)

    def make_receipts(self, count):
        for n in range(count):
            receipt(self.control / f'stages/p3-r1/done/{n % 3}/{n:04}.json',
                    [result(f'file-{n}', ino=100 + n)])

    def test_bounded_concurrency_preserves_consumption_order_and_time_throttles(self):
        self.make_receipts(60)
        original_read = parallel.read_json
        lock = threading.Lock()
        stats = {'active': 0, 'peak': 0, 'outstanding': 0, 'peak_outstanding': 0}
        completed = []
        original_executor = concurrent.futures.ThreadPoolExecutor

        def slow_read(path):
            with lock:
                stats['active'] += 1
                stats['peak'] = max(stats['peak'], stats['active'])
            try:
                # Reverse timing creates out-of-order completions while
                # ordered SQL consumption must still equal the reference.
                time.sleep(0.004 * (1 + int(path.stem) % 4))
                value = original_read(path)
                with lock:
                    completed.append(path)
                return value
            finally:
                with lock:
                    stats['active'] -= 1

        class TrackingExecutor(original_executor):
            def submit(inner_self, *args, **kwargs):
                with lock:
                    stats['outstanding'] += 1
                    stats['peak_outstanding'] = max(stats['peak_outstanding'], stats['outstanding'])
                future = super().submit(*args, **kwargs)
                def done(_):
                    with lock:
                        stats['outstanding'] -= 1
                future.add_done_callback(done)
                return future

        progress = []
        with mock.patch.object(parallel, 'read_json', side_effect=slow_read), \
                mock.patch.object(parallel, 'ThreadPoolExecutor', TrackingExecutor):
            summary = parallel.build_resume_index(self.control, self.base / 'parallel.sqlite',
                                                 read_workers=4, progress_callback=progress.append,
                                                 progress_interval=3600)
        reference_summary = reference.build_resume_index(self.control, self.base / 'reference.sqlite')
        self.assertEqual(self.rows(summary['db_path']), self.rows(reference_summary['db_path']))
        summary.pop('db_path')
        reference_summary.pop('db_path')
        self.assertEqual(summary, reference_summary)
        self.assertGreater(stats['peak'], 1)
        self.assertLessEqual(stats['peak'], 4)
        self.assertLessEqual(stats['peak_outstanding'], 8)
        self.assertEqual(stats['active'], 0)
        self.assertTrue(all(p['processed_receipts'] in (0, 60) for p in progress))
        self.assertEqual(progress[-1]['status'], 'publishing')
        # No per-100 writes are allowed merely because throughput increased.
        self.assertLessEqual(len(progress), 5)

    def test_callback_stop_cancels_pending_joins_threads_and_removes_unpublished_db(self):
        self.make_receipts(90)
        original_read = parallel.read_json
        lock = threading.Lock()
        state = {'active': 0, 'read': 0}
        def slow_read(path):
            with lock:
                state['active'] += 1
                state['read'] += 1
            try:
                time.sleep(0.02)
                return original_read(path)
            finally:
                with lock:
                    state['active'] -= 1
        seen = []
        def stop(payload):
            seen.append(payload)
            if payload['processed_receipts'] >= 1:
                raise RuntimeError('fixture STOP')
        target = self.base / 'stopped.sqlite'
        with mock.patch.object(parallel, 'read_json', side_effect=slow_read):
            with self.assertRaisesRegex(RuntimeError, 'fixture STOP'):
                parallel.build_resume_index(self.control, target, read_workers=4,
                                            progress_callback=stop, progress_interval=0.001)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.base.glob('.stopped.sqlite.*.tmp*')), [])
        self.assertEqual(state['active'], 0)
        self.assertLess(state['read'], 90)
        self.assertFalse(any(t.name.startswith('resume-receipt') for t in threading.enumerate()))
        self.assertTrue(any(p['processed_receipts'] >= 1 for p in seen))

    def test_snapshot_and_symlink_directories_are_not_entered(self):
        self.make_receipts(3)
        d = self.control / 'stages/p3-r1/done'
        outside = self.base / 'outside'
        receipt(outside / 'outside.json', [result('outside')])
        (d / 'link').symlink_to(outside, target_is_directory=True)
        original = parallel.os.scandir
        def watched(path):
            self.assertNotIn('.snapshots', Path(path).parts)
            self.assertNotIn('link', Path(path).parts)
            return original(path)
        with mock.patch.object(parallel.os, 'scandir', side_effect=watched):
            self.assertEqual(len(parallel._receipt_paths(self.control)), 3)

    def test_existing_target_and_bad_worker_count_fail_without_mutating(self):
        target = self.base / 'existing.sqlite'
        target.write_text('keep')
        with self.assertRaises(FileExistsError):
            parallel.build_resume_index(self.control, target)
        self.assertEqual(target.read_text(), 'keep')
        for workers in (0, -1, 129, True, 3.0):
            with self.assertRaises(ValueError):
                parallel.build_resume_index(self.control, self.base / 'new.sqlite', read_workers=workers)


if __name__ == '__main__':
    unittest.main()
