import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from migration_io import write_json
import migration_control_observation as census


class ControlObservationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hcr-control-census-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.control = Path(self.temp.name) / 'control'
        self.control.mkdir()
        (self.control / 'stages').mkdir()

    def stage(self, label):
        root = self.control / 'stages' / label
        (root / 'running').mkdir(parents=True)
        for shard in range(64):
            (root / 'pending' / str(shard)).mkdir(parents=True)
        return root

    def test_counts_other_stage_without_payload_reads(self):
        a, b = self.stage('p1-r1'), self.stage('p3-r3')
        write_json(a / 'pending' / '63' / '999.json', {'secret': 'must not read'})
        write_json(b / 'running' / '0.json', {'secret': 'must not read'})
        with mock.patch('builtins.open', side_effect=AssertionError('file content opened')):
            result = census.observe_stage_queues(self.control)
        self.assertTrue(result['complete'])
        self.assertFalse(result['all_queues_empty'])
        self.assertEqual(result['stages']['p1-r1']['pending'], 1)
        self.assertEqual(result['stages']['p3-r3']['running'], 1)

    def test_empty_stage_is_observed(self):
        self.stage('p1-r1')
        result = census.observe_stage_queues(self.control)
        self.assertTrue(result['all_queues_empty'])
        self.assertFalse(result['atomic_snapshot'])

    def test_missing_shard_is_unknown_not_zero(self):
        stage = self.stage('p3-r3')
        (stage / 'pending' / '8').rmdir()
        result = census.observe_stage_queues(self.control)
        self.assertFalse(result['complete'])
        self.assertIsNone(result['stages']['p3-r3']['pending'])
        self.assertFalse(result['all_queues_empty'])

    def test_stage_symlink_is_not_followed(self):
        stage = self.stage('p1-r1')
        (self.control / 'stages' / 'p2-r1').symlink_to(stage, target_is_directory=True)
        result = census.observe_stage_queues(self.control)
        self.assertFalse(result['complete'])
        self.assertIsNone(result['stages']['p2-r1']['running'])

    def test_running_symlink_is_not_followed(self):
        stage = self.stage('p1-r1')
        (stage / 'running').rmdir()
        (stage / 'running').symlink_to(stage / 'pending', target_is_directory=True)
        result = census.observe_stage_queues(self.control)
        self.assertFalse(result['complete'])
        self.assertIsNone(result['stages']['p1-r1']['running'])

    def test_flat_and_64_sharded_running_markers_are_counted_and_empty_is_correct(self):
        stage = self.stage('p3-r3')
        markers = [stage / 'running' / 'broker--legacy--128.json']
        markers.extend(stage / 'running' / str(i) / ('broker--new--%d.json' % i)
                       for i in range(64))
        for index, path in enumerate(markers):
            write_json(path, {'id': index})
        with mock.patch('builtins.open', side_effect=AssertionError('file content opened')):
            result = census.observe_stage_queues(self.control)
        self.assertTrue(result['complete'])
        self.assertEqual(result['stages']['p3-r3']['running'], 65)
        self.assertEqual(result['stages']['p3-r3']['other_entries'], [])
        self.assertFalse(result['all_queues_empty'])
        for path in markers:
            path.unlink()
        result = census.observe_stage_queues(self.control)
        self.assertTrue(result['complete'])
        self.assertEqual(result['stages']['p3-r3']['running'], 0)
        self.assertTrue(result['all_queues_empty'])

    def test_running_shard_symlink_is_unknown_and_never_followed(self):
        stage = self.stage('p3-r3')
        (stage / 'running' / '0').symlink_to(stage / 'pending' / '0', target_is_directory=True)
        result = census.observe_stage_queues(self.control)
        self.assertFalse(result['complete'])
        self.assertIsNone(result['stages']['p3-r3']['running'])
        self.assertFalse(result['all_queues_empty'])

    def test_unpublished_or_unknown_entries_prevent_empty_claim(self):
        stage = self.stage('p1-r1')
        (stage / 'pending' / '0' / '.task.tmp').touch()
        result = census.observe_stage_queues(self.control)
        self.assertTrue(result['complete'])
        self.assertFalse(result['all_queues_empty'])
        self.assertIn('pending/0/.task.tmp', result['stages']['p1-r1']['other_entries'])

    def test_snapshots_are_not_queue_tasks(self):
        stage = self.stage('p1-r1')
        write_json(stage / 'pending' / '0' / '.snapshots' / 'orphan.json', {'retained': True})
        result = census.observe_stage_queues(self.control)
        self.assertTrue(result['all_queues_empty'])

    def test_directory_exchange_is_reported(self):
        stage = self.stage('p1-r1')
        inode = (stage / 'pending').stat().st_ino
        original = census._Census.names
        changed = []

        def names(instance, fd):
            result = original(instance, fd)
            if os.fstat(fd).st_ino == inode and not changed:
                (stage / 'pending').rename(stage / 'old-pending')
                (stage / 'pending').mkdir()
                changed.append(True)
            return result

        with mock.patch.object(census._Census, 'names', names):
            result = census.observe_stage_queues(self.control)
        self.assertFalse(result['complete'])
        self.assertIsNone(result['stages']['p1-r1']['pending'])
        self.assertTrue(any('exchanged' in error['detail'] for error in result['errors']))

    def test_metadata_issue_json_batches_and_jsonl_are_included(self):
        root_issue = self.control / 'p1-r1-issues.jsonl'
        root_issue.write_text('an issue\n')
        batch = self.control / 'metadata' / 'p3-r2' / 'issues' / 'batch.json'
        write_json(batch, {'issues': [{'detail': 'metadata mismatch'}]})
        write_json(batch, {'issues': [{'detail': 'metadata still mismatched'}]})
        with mock.patch('builtins.open', side_effect=AssertionError('JSON content opened')):
            result = census.observe_issue_files(self.control)
        self.assertTrue(result['complete'])
        self.assertEqual({item['path'] for item in result['files']}, {str(root_issue), str(batch)})
        self.assertTrue(all(item['bytes'] > 0 for item in result['files']))

    def test_issue_symlink_is_reported_without_following(self):
        target = self.control / 'target'
        target.write_text('secret')
        (self.control / 'p1-r1-issues.jsonl').symlink_to(target)
        result = census.observe_issue_files(self.control)
        self.assertFalse(result['complete'])
        self.assertEqual(result['files'], [])

    def test_optional_metadata_absence_is_distinct_from_unreadable(self):
        result = census.observe_issue_files(self.control)
        self.assertTrue(result['complete'])
        (self.control / 'metadata').write_text('not a directory')
        self.assertFalse(census.observe_issue_files(self.control)['complete'])

    def test_missing_control_is_explicit_failure(self):
        result = census.observe_stage_queues(self.control / 'missing')
        self.assertFalse(result['complete'])
        self.assertFalse(result['all_queues_empty'])

    def test_every_open_uses_directory_nofollow_flags(self):
        self.stage('p1-r1')
        original = os.open
        calls = []

        def opened(path, flags, **kwargs):
            self.assertTrue(flags & os.O_DIRECTORY)
            self.assertTrue(flags & os.O_NOFOLLOW)
            calls.append(path)
            return original(path, flags, **kwargs)

        with mock.patch.object(census.os, 'open', opened):
            result = census.observe_stage_queues(self.control)
        self.assertTrue(result['complete'])
        self.assertTrue(calls)


if __name__ == '__main__':
    unittest.main()
