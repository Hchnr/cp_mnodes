import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import threading
import unittest

from migration_parallel_scan import ScanCancelled, scan_phase
from migration_scan import _stat_fields


class ParentOnlyPaths(set):
    def __reduce_ex__(self, protocol):
        raise AssertionError('resume paths must never be copied into readers')


class ProcessScanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = self.base / 'source'
        self.source.mkdir()
        self.manifest = self.base / 'manifest.jsonl'
        self.emitted = []

    def put(self, path, payload=b'payload'):
        target = self.source / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return target

    def scan(self, **kwargs):
        return scan_phase(self.source, 3, self.emitted.extend, self.manifest,
                          processes=3, threads_per_process=4, process_batch_size=8,
                          batch_files=2, queue_size=2, **kwargs)

    def rows(self):
        return [json.loads(line) for line in self.manifest.read_text().splitlines()]

    def test_spawn_readers_preserve_hardlinks_symlinks_exclusions_and_parent_callbacks(self):
        for directory in range(24):
            for index in range(5):
                self.put(f'd{directory}/file-{index}')
        original = self.put('links/original')
        os.link(original, self.source / 'd0' / 'hardlink')
        os.symlink('../links/original', self.source / 'd1' / 'symlink')
        self.put('home/hcr/excluded')
        self.put('repos/cosmos-framework-pwm/excluded')
        self.put('unusual\nname')
        os.mkfifo(self.source / 'unsupported-fifo')
        owner = (os.getpid(), threading.get_ident())
        owners = []

        def emit(batch):
            owners.append((os.getpid(), threading.get_ident()))
            self.emitted.extend(batch)

        stats = scan_phase(self.source, 3, emit, self.manifest,
                           processes=3, threads_per_process=4,
                           process_batch_size=8, queue_size=2, batch_files=2)
        self.assertEqual(set(owners), {owner})
        self.assertEqual(stats['files'], 123)
        self.assertEqual(stats['hardlinks'], 1)
        self.assertEqual(stats['symlinks'], 1)
        self.assertEqual(stats['unsupported'], 1)
        self.assertTrue(stats['scan_complete'])
        self.assertFalse(stats['fully_copyable'])
        rows = self.rows()
        self.assertEqual(len([row for row in rows if row.get('hardlink_to')]), 1)
        self.assertEqual(len([row for row in self.emitted if row['kind'] == 'file']), 122)
        self.assertFalse(any(row['rel'].endswith('excluded') for row in rows))
        self.assertTrue(any(row['rel'] == 'unusual\nname' for row in rows))
        metrics = stats['process_readers']
        self.assertEqual(metrics['start_method'], 'spawn')
        self.assertEqual(len(set(metrics['pids'])), 3)
        self.assertNotIn(os.getpid(), metrics['pids'])
        self.assertGreaterEqual(sum(n > 0 for n in metrics['directory_tasks_by_pid'].values()), 2)
        self.assertLessEqual(metrics['peak_pending_directories'], 2)
        self.assertGreater(metrics['reader_cpu_seconds'], 0)

    def test_resume_paths_remain_parent_only_and_known_leaves_are_not_statted(self):
        known = ParentOnlyPaths()
        for directory in range(4):
            for index in range(12):
                known.add(f'd{directory}/old-{index}')
                self.put(f'd{directory}/old-{index}')
        self.put('d0/fresh')
        self.manifest.write_text('')
        stats = self.scan(initial_seeded_paths=known, append_manifest=True,
                          skip_seeded_nondirectories=True)
        self.assertEqual([entry['rel'] for entry in self.emitted], ['d0/fresh'])
        metrics = stats['process_readers']
        self.assertEqual(metrics['skipped_seeded_leaves'], 48)
        self.assertEqual(metrics['selected_leaves'], 1)

    def test_seeded_leaf_now_directory_is_walked(self):
        old = self.put('old')
        entry = {'rel': 'old', 'kind': 'file', **_stat_fields(old.stat())}
        old.unlink()
        self.put('old/fresh')
        stats = self.scan(resume_records=[entry], skip_seeded_nondirectories=True)
        self.assertEqual([row['rel'] for row in self.emitted], ['old/fresh'])
        self.assertEqual(stats['files'], 2)

    def test_bounded_directory_queue_uses_local_dfs_without_losing_entries(self):
        for index in range(20):
            self.put(f'branch-{index}/nested/leaf')
        stats = scan_phase(self.source, 3, self.emitted.extend, self.manifest,
                           processes=1, threads_per_process=2, process_batch_size=4,
                           queue_size=1, batch_files=2)
        self.assertEqual(stats['files'], 20)
        self.assertEqual({entry['rel'] for entry in self.emitted},
                         {f'branch-{i}/nested/leaf' for i in range(20)})
        self.assertGreater(stats['process_readers']['local_directory_overflows'], 0)
        self.assertLessEqual(stats['process_readers']['peak_pending_directories'], 1)

    def test_directory_replacement_by_symlink_cannot_escape_source(self):
        self.put('branch/original')
        outside = self.base / 'outside'
        outside.mkdir()
        (outside / 'secret').write_text('not source')
        # The reader's safe open helper is also used by every assigned task.
        from migration_process_scan import _open_task
        from migration_worker import _open_root
        anchor = _open_root(self.source)
        expected = (self.source / 'branch').stat()
        (self.source / 'branch').rename(self.source / 'saved')
        os.symlink(outside, self.source / 'branch')
        try:
            with self.assertRaises(OSError):
                _open_task(anchor, 'branch', (expected.st_dev, expected.st_ino))
        finally:
            os.close(anchor)
        stats = self.scan()
        self.assertTrue(stats['scan_complete'])
        self.assertNotIn('branch/secret', {entry['rel'] for entry in self.rows()})
        self.assertIn('branch', {entry['rel'] for entry in self.emitted if entry['kind'] == 'symlink'})

    def test_stop_and_callback_failure_clean_up_spawned_readers(self):
        for index in range(100):
            self.put(f'directory/file-{index}')
        control = self.base / 'control'
        control.mkdir()
        children_before = {child.pid for child in multiprocessing.active_children()}

        def emit_and_stop(batch):
            self.emitted.extend(batch)
            (control / 'STOP').touch()

        with self.assertRaises(ScanCancelled):
            scan_phase(self.source, 3, emit_and_stop, self.manifest, processes=2,
                       process_batch_size=2, batch_files=1, control=control)
        self.assertTrue(self.emitted)
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, children_before)
        self.assertTrue(all(json.loads(line) for line in self.manifest.read_text().splitlines()))

        def fail(_batch):
            raise RuntimeError('consumer failed')

        (control / 'STOP').unlink()
        with self.assertRaisesRegex(RuntimeError, 'consumer failed'):
            scan_phase(self.source, 3, fail, self.base / 'second.jsonl', processes=2,
                       process_batch_size=2, batch_files=1, control=control)
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, children_before)


if __name__ == '__main__':
    unittest.main()
