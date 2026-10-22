"""Small local recovery-index fixtures; never inspect a real copy control."""

import json
import itertools
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import unittest
from unittest import mock


WORK = Path(__file__).absolute().parent
sys.path.insert(0, str(WORK))
import migration_resume_index as resume
from migration_io import write_json


def identity(ino, size=7, mode=stat.S_IFREG | 0o640, stamp=1, dev=1):
    return {'dev': dev, 'ino': ino, 'size': size, 'mode': mode, 'uid': 123, 'gid': 456,
            'mtime_ns': stamp, 'ctime_ns': stamp}


def result(rel, status='copied', ino=100, digest='a' * 64, stamp=1, **extra):
    value = {'rel': rel, 'kind': 'file', 'status': status, 'sha256': digest,
             'source_identity': identity(ino + 1000, stamp=stamp),
             'dst_stat': identity(ino, stamp=stamp)}
    value.update(extra)
    return value


def receipt(path, results, day=1):
    value = {'finished': f'2026-01-{day:02d}T00:00:00+00:00', 'results': results}
    write_json(path, value)


class ResumeIndexTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(dir=WORK, prefix='resume-index-fixture-')
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name)
        self.control = self.base / 'control'
        self.control.mkdir()
        self.db = self.base / 'new.sqlite'

    def rows(self, path=None):
        with sqlite3.connect(path or self.db) as db:
            return {rel: {'source': json.loads(src), 'destination': json.loads(dst),
                          'owned': owned, 'sha256': digest}
                    for rel, src, dst, owned, digest in db.execute('SELECT * FROM files')}

    def test_nested_receipts_sorted_and_snapshot_and_symlink_trees_not_walked(self):
        done = self.control / 'stages' / 'p3-r1' / 'done'
        paths = [done / 'root.json', done / '0' / 'a.json',
                 done / '0' / 'deeper' / 'b.json', done / '.hidden' / 'c.json']
        for number, path in enumerate(paths):
            receipt(path, [result('file-' + str(number), ino=100 + number)])
        hidden = done / '.snapshots' / 'unrelated' / '0.json'
        hidden.parent.mkdir(parents=True)
        hidden.write_text('not a canonical receipt')
        outsider = self.base / 'outside'
        receipt(outsider / 'done' / 'outsider.json', [result('must-not-import')])
        (done / 'linked-directory').symlink_to(outsider / 'done', target_is_directory=True)
        (self.control / 'stages' / 'linked-stage').symlink_to(outsider, target_is_directory=True)
        linked_stage = self.control / 'stages' / 'another-stage'
        linked_stage.mkdir()
        (linked_stage / 'done').symlink_to(outsider / 'done', target_is_directory=True)
        real_scandir = os.scandir
        visited = []

        def watched_scandir(path):
            visited.append(Path(path))
            self.assertNotIn('.snapshots', Path(path).parts)
            self.assertNotIn('linked-directory', Path(path).parts)
            self.assertNotIn('linked-stage', Path(path).parts)
            return real_scandir(path)

        with mock.patch.object(resume.os, 'scandir', side_effect=watched_scandir):
            found = resume._receipt_paths(self.control)
        self.assertEqual(found, sorted(paths))
        summary = resume.build_resume_index(self.control, self.db)
        self.assertEqual(summary['receipts'], 4)
        self.assertEqual(set(self.rows()), {'file-' + str(n) for n in range(4)})

    def test_latest_snapshot_is_read_without_importing_old_version_twice(self):
        path = self.control / 'stages' / 'p1-r1' / 'done' / '0' / '1.json'
        receipt(path, [result('latest', ino=101, stamp=1)], day=1)
        receipt(path, [result('latest', 'verified', ino=101, stamp=99)], day=2)
        self.assertEqual(json.loads(path.read_text())['results'][0]['dst_stat']['mtime_ns'], 1)
        summary = resume.build_resume_index(self.control, self.db)
        self.assertEqual(summary['receipts'], 1)
        self.assertEqual(summary['results'], 1)
        self.assertEqual(self.rows()['latest']['destination']['mtime_ns'], 99)
        # The old canonical copied record is superseded by latest immutable
        # receipt. It must not accidentally supply an extra ownership proof.
        self.assertEqual(self.rows()['latest']['owned'], 0)

    def test_two_ancestor_proofs_keep_latest_identity_and_require_same_inode_and_sha(self):
        old = self.base / 'old-control'
        old.mkdir()
        write_json(self.control / 'config.json', {'resume_from': str(old)})
        receipt(old / 'stages' / 'p1-r1' / 'done' / '1.json', [
            result('owned', ino=101), result('new-inode', ino=201),
            result('new-content', ino=301, digest='b' * 64)])
        latest = [result('owned', 'verified', ino=101, stamp=99),
                  result('new-inode', 'verified', ino=202, stamp=99),
                  result('new-content', 'verified', ino=301, digest='c' * 64, stamp=99),
                  result('replay-only', ino=401, replay_reused_copy=True)]
        latest[0]['dst_stat']['dev'] = 999
        receipt(self.control / 'stages' / 'p1-r2' / 'done' / 'x' / 'y' / '2.json', latest, day=3)
        receipt(self.control / 'stages' / 'p1-r3' / 'done' / '999.json',
                [result('owned', 'verified', ino=101, stamp=2)], day=2)
        summary = resume.build_resume_index(self.control, self.db)
        rows = self.rows()
        self.assertEqual(summary['controls'], [str(old.resolve()), str(self.control.resolve())])
        self.assertEqual(summary['control_count'], 2)
        self.assertEqual(rows['owned']['owned'], 1)
        self.assertEqual(rows['owned']['destination']['mtime_ns'], 99)
        self.assertEqual(rows['owned']['destination']['dev'], 999)
        self.assertEqual(rows['new-inode']['owned'], 0)
        self.assertEqual(rows['new-content']['owned'], 0)
        self.assertEqual(rows['replay-only']['owned'], 0)
        self.assertEqual(summary['owned_files'], 1)

    def make_many(self, count=123):
        done = self.control / 'stages' / 'p3-r1' / 'done'
        for number in range(count):
            receipt(done / str(number % 4) / (str(number) + '.json'),
                    [result('file-' + str(number), ino=100 + number)])

    def test_progress_reports_real_counts_at_100_receipts_and_phases(self):
        self.make_many()
        progress = []
        def record(payload):
            progress.append(payload)
            payload['issue_examples'].append({'fixture-only': True})
        summary = resume.build_resume_index(self.control, self.db,
                                           progress_callback=record, progress_interval=3600)
        self.assertEqual(summary['receipts'], 123)
        self.assertEqual(summary['results'], 123)
        self.assertEqual(summary['issue_examples'], [])
        self.assertTrue(any(p['status'] == 'enumerating' and p['receipt_paths_discovered'] == 100 for p in progress))
        self.assertTrue(any(p['status'] == 'reading_receipts' and p['receipts'] == 100
                            and p['processed_receipts'] == 100 and p['results'] == 100 for p in progress))
        self.assertEqual(progress[-1]['status'], 'publishing')
        self.assertEqual(progress[-1]['verified_files'], 123)
        self.assertEqual(progress[-1]['owned_files'], 123)

    def test_callback_abort_removes_temporary_and_does_not_publish(self):
        self.make_many(101)
        def stop(payload):
            if payload['processed_receipts'] >= 100:
                raise RuntimeError('STOP requested by fixture')
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            resume.build_resume_index(self.control, self.db, progress_callback=stop)
        self.assertFalse(self.db.exists())
        self.assertEqual(list(self.base.glob('.new.sqlite.*.tmp*')), [])

    def test_time_interval_reports_before_100_receipts(self):
        self.make_many(3)
        ticks = itertools.count(0, 6)
        progress = []
        with mock.patch.object(resume.time, 'monotonic', side_effect=lambda: next(ticks)):
            summary = resume.build_resume_index(self.control, self.db,
                                               progress_callback=progress.append, progress_interval=5)
        self.assertEqual(summary['receipts'], 3)
        self.assertTrue(any(p['status'] == 'reading_receipts' and p['processed_receipts'] == 1
                            and p['receipts'] == 1 for p in progress))


if __name__ == '__main__':
    unittest.main()
