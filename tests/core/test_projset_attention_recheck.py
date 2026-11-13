import importlib.util
import hashlib
import csv
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/core'))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


subject = load('attention', ROOT / 'ops/diagnostics/projset_attention_recheck.py')
recovery = load('recovery', ROOT / 'ops/copy/projset_recovery_copy.py')
files = load('conflict_files', ROOT / 'ops/diagnostics/projset_conflict_files.py')
links = load('conflict_links', ROOT / 'ops/diagnostics/projset_conflict_links.py')


class AttentionRecheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        base = Path(self.temp.name)
        self.source, self.target = base / 'source', base / 'target'
        self.source.mkdir(); self.target.mkdir()
        recovery.engine.configure_directory_opener(recovery.native.open_parent_readonly)
        self.cfg = {'source': str(self.source), 'target': str(self.target), 'run_id': 'test'}

    def tearDown(self):
        self.temp.cleanup()

    def limits(self):
        return files.Limits(10**9, 10**6, 10**9, 10**8)

    def test_exclusion_requires_a_full_path_component(self):
        self.assertTrue(subject.excluded('eai_pwm/datasets/Sekai'))
        self.assertTrue(subject.excluded('eai_pwm/datasets/Sekai/file'))
        self.assertFalse(subject.excluded('eai_pwm/datasets/Sekai-other/file'))

    def test_interrupted_scan_retries_copy_and_keeps_different_target(self):
        (self.source / 'dir').mkdir(); (self.target / 'dir').mkdir()
        (self.source / 'dir/existing').write_text('same')
        (self.target / 'dir/existing').write_text('same')
        (self.source / 'dir/missing').write_text('new')
        (self.source / 'dir/conflict').write_text('source')
        (self.target / 'dir/conflict').write_text('target')
        result = subject.retry_scan({'rel': 'dir'}, recovery, self.cfg)
        self.assertEqual(result['result'], 'scan_manual_attention')
        self.assertEqual((result['copied'], result['skipped'], result['manual']), (1, 1, 1))
        self.assertEqual((self.target / 'dir/missing').read_text(), 'new')
        self.assertEqual((self.target / 'dir/conflict').read_text(), 'target')

    def test_file_conflict_review_is_read_only_and_content_based(self):
        (self.source / 'x').write_text('abc')
        (self.target / 'x').write_text('xyz')
        result = subject.review_conflict({'rel': 'x'}, recovery, self.cfg, files, links, self.limits())
        self.assertEqual(result['result'], 'different_sha256')
        self.assertEqual((self.target / 'x').read_text(), 'xyz')
        (self.target / 'x').write_text('abc')
        result = subject.review_conflict({'rel': 'x'}, recovery, self.cfg, files, links, self.limits())
        self.assertEqual(result['result'], 'same_sha256')

    def test_missing_target_is_copied_without_replacement(self):
        (self.source / 'x').write_text('data')
        result = subject.review_conflict({'rel': 'x'}, recovery, self.cfg, files, links, self.limits())
        self.assertEqual(result['result'], 'copied_missing')
        self.assertEqual((self.target / 'x').read_text(), 'data')

    def test_archive_preserves_original_destination_and_source_version(self):
        (self.source / 'x').write_text('source')
        (self.target / 'x').write_text('target')
        archive = Path(self.temp.name) / 'archive'; archive.mkdir()
        cfg = dict(self.cfg, attention_archive=str(archive))
        result = subject.archive_conflict({'rel': 'x'}, recovery, cfg)
        self.assertEqual(result['result'], 'archived_source')
        self.assertEqual((archive / 'x').read_text(), 'source')
        self.assertEqual((self.target / 'x').read_text(), 'target')
        (archive / 'x').write_text('previous')
        result = subject.archive_conflict({'rel': 'x'}, recovery, cfg)
        self.assertEqual(result['result'], 'manual_archive_collision')
        self.assertEqual((archive / 'x').read_text(), 'previous')

    def test_archive_symlink_saves_link_without_following_it(self):
        (self.source / 'x').symlink_to('/unavailable/source/data')
        archive = Path(self.temp.name) / 'archive'; archive.mkdir()
        cfg = dict(self.cfg, attention_archive=str(archive))
        result = subject.archive_conflict({'rel': 'x'}, recovery, cfg)
        self.assertEqual(result['result'], 'archived_source')
        self.assertTrue((archive / 'x').is_symlink())
        self.assertEqual(str((archive / 'x').readlink()), '/unavailable/source/data')

    def test_changed_source_is_preserved_but_not_reported_as_reviewed_version(self):
        (self.source / 'x').write_text('new')
        archive = Path(self.temp.name) / 'archive'; archive.mkdir()
        cfg = dict(self.cfg, attention_archive=str(archive))
        result = subject.archive_conflict({'rel': 'x', 'expected_kind': 'file',
                    'review_source_sha256': 'a' * 64}, recovery, cfg)
        self.assertEqual(result['result'], 'manual_source_changed_since_review')
        self.assertEqual((archive / 'x').read_text(), 'new')

    def test_independent_archive_readback_detects_corruption(self):
        archive = Path(self.temp.name) / 'archive'; archive.mkdir()
        (archive / 'x').write_bytes(b'data')
        cfg = dict(self.cfg, attention_verify_archive=str(archive))
        item = {'rel': 'x', 'expected_kind': 'file', 'verify_size': '4',
                'verify_sha256': hashlib.sha256(b'data').hexdigest()}
        self.assertEqual(subject.verify_archive(item, recovery, cfg, self.limits())['result'], 'archive_verified')
        (archive / 'x').write_bytes(b'bad!')
        with self.assertRaises(subject.RetryWork):
            subject.verify_archive(item, recovery, cfg, self.limits())

    def test_independent_archive_readback_checks_link_text(self):
        archive = Path(self.temp.name) / 'archive'; archive.mkdir()
        (archive / 'x').symlink_to('/unavailable/source/data')
        cfg = dict(self.cfg, attention_verify_archive=str(archive))
        item = {'rel': 'x', 'expected_kind': 'symlink',
                'verify_size': str(len('/unavailable/source/data')),
                'verify_link_target': '/unavailable/source/data', 'verify_sha256': ''}
        self.assertEqual(subject.verify_archive(item, recovery, cfg, self.limits())['result'], 'archive_verified')
        item['verify_link_target'] = '/other'
        with self.assertRaises(subject.RetryWork):
            subject.verify_archive(item, recovery, cfg, self.limits())

    def test_stale_directory_conflict_is_queued_as_an_error_retry(self):
        base = Path(self.temp.name)
        run = base / 'run'; (run / 'recovery').mkdir(parents=True)
        outcomes = base / 'outcomes'; outcomes.mkdir()
        row = {'relative_path': 'dir', 'job': '', 'reason': '[Errno 116] Stale file handle',
               'receipt_path': '/old/receipts', 'receipt_offset': '3', 'kind': 'directory'}
        with (outcomes / 'conflict-paths.csv').open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row)); writer.writeheader(); writer.writerow(row)
        work = run / 'recovery/work'
        manifest = subject.prepare_stale_directory_retry(run, work, outcomes)
        shard = json.loads((work / 'shards/cpu-master-0.json').read_text())
        self.assertEqual((manifest['errors'], manifest['conflicts']), (1, 0))
        self.assertEqual((shard['items'][0]['category'], shard['items'][0]['rel']), ('error', 'dir'))

    def test_swanlab_cadence_is_shared_across_attention_runs(self):
        recovery_dir = Path(self.temp.name) / 'recovery'; recovery_dir.mkdir()
        first, second = recovery_dir / 'first', recovery_dir / 'second'
        first.mkdir(); second.mkdir()
        clock = [1000.0]
        def sleep(seconds):
            clock[0] += seconds
        with mock.patch.object(subject.time, 'time', side_effect=lambda: clock[0]), \
                mock.patch.object(subject.time, 'sleep', side_effect=sleep):
            with subject.swanlab_upload_slot(first) as one:
                self.assertEqual(one, 1000.0)
            with subject.swanlab_upload_slot(second) as two:
                self.assertEqual(two, 1120.0)
        saved = json.loads((recovery_dir / 'attention-swanlab-cadence.json').read_text())
        self.assertEqual(saved['enqueued_at'], 1120.0)

    def test_incomplete_receipt_is_trimmed_but_completed_item_is_retained(self):
        path = self.target / 'receipts.jsonl'
        first = {'key': 'a', 'state': 'done', 'result': {'result': 'same_sha256'}}
        path.write_bytes((json.dumps(first) + '\n{"key":"b"').encode())
        self.assertEqual(subject.prior_results(path), {'a': first})
        self.assertEqual(path.read_bytes(), (json.dumps(first) + '\n').encode())


if __name__ == '__main__':
    unittest.main()
