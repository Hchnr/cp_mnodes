import collections
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

import migration_manifest_copy as subject


class ManifestCopyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cp-fixed-test-')
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.target = self.root / 'target'
        self.source.mkdir()
        self.target.mkdir()
        self.cfg = dict(source=str(self.source), target=str(self.target), run_id='test-fixed')

    def tearDown(self):
        self.temp.cleanup()

    def task(self, rel='a/file', data=b'payload\0' * 100):
        path = self.source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        source = subject.engine._record(path.stat())
        source.pop('dev')
        entry = subject.validate_row(dict(rel=rel, source=source, kind='file', reason='target_absent'))
        return dict(id=15, entry=entry)

    def test_copy_sha_fsync_and_resume(self):
        task = self.task()
        result = subject.copy_one(self.cfg, task)
        self.assertEqual(result['status'], 'copied')
        self.assertEqual(result['sha256'], subject.digest(self.target / task['entry']['rel']))
        self.assertTrue(subject.resume_valid(self.cfg, task['entry'], result))
        resumed = subject.copy_one(self.cfg, task, result)
        self.assertTrue(resumed['resumed'])
        self.assertEqual(result['dst_stat'], resumed['dst_stat'])
        self.assertFalse(list(self.target.rglob('.migration-*')))

    def test_source_changed_not_copied(self):
        task = self.task()
        (self.source / task['entry']['rel']).write_bytes(b'changed')
        result = subject.copy_one(self.cfg, task)
        self.assertEqual(result['status'], 'changed')
        self.assertFalse((self.target / task['entry']['rel']).exists())

    def test_conflicting_target_not_overwritten(self):
        task = self.task()
        path = self.target / task['entry']['rel']
        path.parent.mkdir()
        path.write_bytes(b'keep original')
        result = subject.copy_one(self.cfg, task)
        self.assertEqual(result['status'], 'conflict')
        self.assertEqual(path.read_bytes(), b'keep original')

    def test_existing_identical_is_hashed_and_reused(self):
        task = self.task()
        first = subject.copy_one(self.cfg, task)
        second = subject.copy_one(self.cfg, task)
        self.assertEqual(first['status'], 'copied')
        self.assertEqual(second['status'], 'skipped')
        self.assertEqual(first['sha256'], second['sha256'])

    def test_destination_symlink_ancestor_rejected(self):
        task = self.task()
        elsewhere = self.root / 'elsewhere'
        elsewhere.mkdir()
        (self.target / 'a').symlink_to(elsewhere, target_is_directory=True)
        result = subject.copy_one(self.cfg, task)
        self.assertEqual(result['status'], 'error')
        self.assertFalse(list(elsewhere.iterdir()))

    def test_source_symlink_ancestor_rejected(self):
        task = self.task()
        (self.source / 'a').rename(self.source / 'elsewhere')
        (self.source / 'a').symlink_to('elsewhere', target_is_directory=True)
        result = subject.copy_one(self.cfg, task)
        self.assertEqual(result['status'], 'error')
        self.assertFalse((self.target / 'a' / 'file').exists())

    def test_scope_rejects_raw_symlink_and_unsafe(self):
        task = self.task()
        for rel in ('eai_pwm/datasets/raw', 'eai_pwm/datasets/raw/a', '../a', '/a', 'a//b', 'a/./b'):
            with self.subTest(rel=rel), self.assertRaises(ValueError):
                subject.validate_row(dict(rel=rel, source=task['entry'], kind='file', reason='target_absent'))
        for kind in ('symlink', 'directory'):
            with self.assertRaises(ValueError):
                subject.validate_row(dict(rel='allowed', source=task['entry'], kind=kind, reason='target_absent'))
        subject.validate_row(dict(rel='other/datasets/raw/a', source=task['entry'], kind='file', reason='target_absent'))

    def test_resume_detects_target_mutation(self):
        task = self.task()
        result = subject.copy_one(self.cfg, task)
        (self.target / task['entry']['rel']).write_bytes(b'changed target')
        self.assertFalse(subject.resume_valid(self.cfg, task['entry'], result))
        again = subject.copy_one(self.cfg, task, result)
        self.assertEqual(again['status'], 'conflict')
        self.assertEqual((self.target / task['entry']['rel']).read_bytes(), b'changed target')

    def test_receipts_recover_without_editing_torn_tail(self):
        folder = self.root / 'receipts'
        folder.mkdir()
        a = folder / 'receipts-first.jsonl'
        row = dict(id=1, finished=100, status='copied', rel='a')
        a.write_bytes((json.dumps(row) + '\n{"id":').encode())
        b = folder / 'receipts-second.jsonl'
        newer = dict(row, finished=102, status='conflict')
        b.write_text(json.dumps(newer) + '\n')
        self.assertEqual(subject.load_receipts(folder), {1: newer})
        self.assertTrue(a.read_bytes().endswith(b'{"id":'))

    def fixture_report(self, rows):
        manifest = self.root / 'manifest.jsonl.gz'
        with gzip.open(manifest, 'wt', encoding='ascii') as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=True) + '\n')
        report = self.root / 'final-report.json'
        report.write_text(json.dumps(dict(status='complete', source='/share/project',
            target='/share-mixed/project', exclusions=[subject.EXCLUDED],
            outputs=dict(files=dict(path=str(manifest), sha256=subject.digest(manifest), rows=len(rows))),
            missing=dict(file=dict(paths=len(rows), logical_bytes=sum(row['source']['size'] for row in rows))))))
        return report

    def test_prepare_exact_disjoint_total_and_frozen_code(self):
        task = self.task()
        rows = [dict(rel='fixture/' + str(i).zfill(4), kind='file',
                     reason='target_absent', source=task['entry']) for i in range(385)]
        report = self.fixture_report(rows)
        control = self.root / 'prepared'
        with mock.patch('builtins.print'):
            cfg = subject.prepare(control, report, 'test-cluster')
        found = []
        for shard in cfg['shards'].values():
            self.assertEqual(subject.digest(shard['path']), shard['sha256'])
            with gzip.open(shard['path'], 'rt') as stream:
                found.extend(json.loads(line) for line in stream)
        self.assertEqual(sorted(row['id'] for row in found), list(range(385)))
        self.assertEqual(len({row['entry']['rel'] for row in found}), 385)
        self.assertEqual(sum(shard['files'] for shard in cfg['shards'].values()), 385)
        self.assertEqual(sum(shard['bytes'] for shard in cfg['shards'].values()), cfg['bytes'])
        for name, checksum in cfg['code'].items():
            self.assertEqual(subject.digest(control / 'code' / name), checksum)

    def test_prepare_duplicate_path_fails_without_launchable_config(self):
        task = self.task()
        row = dict(rel='fixture/a', kind='file', reason='target_absent', source=task['entry'])
        report = self.fixture_report([row, row])
        control = self.root / 'bad'
        with self.assertRaises(ValueError):
            subject.prepare(control, report, 'test-cluster')
        self.assertFalse((control / 'config.json').exists())

    def test_wrong_cluster_rejected_before_dataset_access(self):
        with self.assertRaisesRegex(ValueError, 'wrong cluster'):
            subject.validate_worker(self.root, dict(cluster='not-this-cluster'), 'master-0')

    def test_full_worker_canary_resume_counts_do_not_duplicate(self):
        tasks = [self.task('a/file-' + str(i), b'payload-' + bytes([i])) for i in range(3)]
        for i, task in enumerate(tasks):
            task['id'] = i
        shard = self.root / 'node.jsonl.gz'
        with gzip.open(shard, 'wt') as stream:
            for task in tasks:
                stream.write(json.dumps(task) + '\n')
        control = self.root / 'control'
        control.mkdir()
        cfg = dict(self.cfg, threads=2, shards={'master-0':dict(path=str(shard),
            files=3, bytes=sum(row['entry']['size'] for row in tasks))})
        subject.atomic(control / 'config.json', cfg)
        with mock.patch.object(subject, 'validate_worker'), mock.patch.object(subject, 'install_engine'), \
                mock.patch.object(subject.signal, 'signal'), mock.patch('builtins.print'):
            subject.worker(control, 'master-0', limit=1)
            first = subject.read_json(control / 'nodes/master-0/progress.json')
            self.assertEqual(first['status'], 'canary_complete')
            self.assertEqual(first['counts']['copied'], 1)
            subject.worker(control, 'master-0')
        final = subject.read_json(control / 'nodes/master-0/progress.json')
        self.assertEqual(final['status'], 'complete')
        self.assertEqual(final['counts']['copied'], 3)
        self.assertEqual(final['counts']['resumed'], 1)
        self.assertEqual(final['counts']['completed_bytes'], cfg['shards']['master-0']['bytes'])


if __name__ == '__main__':
    unittest.main()
