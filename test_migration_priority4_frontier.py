import os
from pathlib import Path
import stat
import unittest

from migration_priority4_frontier import (Priority4Prefix, FifoFrontier, relative_source_directory,
                                         observe_source_frames)
from migration_scan_boundary import identity


def directory(rel):
    return dict(rel=rel, kind='directory', mode=stat.S_IFDIR | 0o755, dev=1, ino=abs(hash(rel)) + 1,
                mtime_ns=1, ctime_ns=1)


def file(rel):
    return dict(rel=rel, kind='file', mode=stat.S_IFREG | 0o644, dev=1, ino=abs(hash(rel)) + 1,
                size=1, nlink=1, blocks=1, mtime_ns=1, ctime_ns=1)


class FifoFrontierTests(unittest.TestCase):
    def fixture(self):
        prefix = Priority4Prefix()
        for entry in (directory(''), directory('closed'), file('closed/a'), directory('active'),
                      file('active/a'), directory('not-entered'), directory('other'),
                      directory('other/closed'), file('other/closed/a')):
            prefix.add(entry)
        frames = dict(source_root='/share/project', directories=['', 'active'], process_signals_sent=0)
        frontier = FifoFrontier(prefix, frames, completed_tasks=10, capture_offset=100,
                                queue_capacity=3, backpressure=4, poll_every=2)
        return prefix, frontier

    def test_prefix_and_closed_descriptor_are_not_alone_completion_proof(self):
        prefix, frontier = self.fixture()
        self.assertIn('closed', frontier.candidates)
        self.assertNotIn('active', frontier.candidates)
        self.assertNotIn('not-entered', frontier.candidates)
        self.assertFalse(frontier.progress(100, prefix.records)['adoption_authorized'])
        self.assertEqual(frontier.progress(100, prefix.records)['status'], 'awaiting_later_flush')
        # A merely newer saved state may describe a pre-capture accepted task.
        frontier.observe_flush(18, 110)
        self.assertIsNone(frontier.flush_observed_size)

    def test_later_flush_then_more_than_full_fifo_capacity_is_required(self):
        prefix, frontier = self.fixture()
        frontier.observe_flush(19, 150)
        self.assertEqual(frontier.progress(140, prefix.records)['status'], 'awaiting_later_flush')
        self.assertEqual(frontier.progress(150, prefix.records)['status'], 'awaiting_post_flush_fifo_drain')
        for i in range(3):
            frontier.entry(file('new-' + str(i)))
        self.assertEqual(frontier.progress(200, prefix.records + 3)['status'], 'awaiting_post_flush_fifo_drain')
        frontier.entry(file('new-3'))
        proof = frontier.progress(220, prefix.records + 4)
        self.assertEqual(proof['status'], 'prepared_fifo_frontier_only')
        self.assertFalse(proof['adoption_authorized'])
        self.assertTrue(proof['graceful_exit_and_final_tail_still_required'])

    def test_new_directories_errors_and_their_ancestors_are_retained(self):
        prefix, frontier = self.fixture()
        frontier.entry(directory('other/new'))
        self.assertNotIn('other', frontier.candidates)
        self.assertIn('other/closed', frontier.candidates)
        frontier.entry(dict(rel='other/closed', kind='error'))
        self.assertNotIn('other/closed', frontier.candidates)
        self.assertIn('closed', frontier.candidates)

    def test_missing_record_accounting_and_namespace_escape_fail_closed(self):
        prefix, frontier = self.fixture()
        with self.assertRaises(ValueError):
            frontier.progress(200, prefix.records + 1)
        for rel in ('eai_pwm', 'eai_pwm/home/hcr'):
            with self.assertRaises(ValueError):
                prefix.add(directory(rel))
        self.assertEqual(relative_source_directory('/share/project/other/eai_pwm', '/share/project'), 'other/eai_pwm')
        with self.assertRaises(ValueError):
            relative_source_directory('/share/project/eai_pwm', '/share/project')
        with self.assertRaises(ValueError):
            relative_source_directory('/share/project/renamed (deleted)', '/share/project')

    def test_fd_observation_is_readonly_and_requires_a_live_root_anchor(self):
        import tempfile
        with tempfile.TemporaryDirectory(prefix='p4-fd-observation-') as source:
            fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
            try:
                proof = observe_source_frames(identity(os.getpid()), source)
                self.assertIn('', proof['directories'])
                self.assertEqual(proof['process_signals_sent'], 0)
            finally:
                os.close(fd)


if __name__ == '__main__':
    unittest.main()
