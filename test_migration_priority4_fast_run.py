import copy
import json
from pathlib import Path
import types
import unittest
from unittest import mock

import migration_io
import migration_priority4_fast_run as fast
import migration_priority4_scan as scanner
from migration_priority4_frontier import Priority4Prefix
from test_migration_priority4_run import Priority4CoordinatorTests
from test_migration_priority4_frontier import directory, file


class ContinuationProofTests(unittest.TestCase):
    def test_assignment_delta_keeps_hardlinked_symlinks_independent(self):
        from test_prepare_priority4_assignments import Guard, task
        prepared = fast.PreparedContinuation.__new__(fast.PreparedContinuation)
        prepared.guard = Guard()
        prepared.assigned, prepared.hashes, prepared.assignment_hardlinks = set(), [], {}
        for number in (0, 1):
            value = task(number)
            value['entries'][0].update(kind='symlink', nlink=2, dev=1, ino=2)
            prepared.add_task(value)
        self.assertEqual(prepared.assigned, {'0', '1'})
        self.assertEqual(prepared.assignment_hardlinks, {})
    def prefix(self):
        prefix = Priority4Prefix()
        for value in (directory(''), directory('active'), file('active/a'), file('active/b')):
            prefix.add(value)
        return prefix

    def test_unoffered_tail_requires_full_counter_equality_and_assignment_membership(self):
        prefix = self.prefix()
        self.assertEqual([e['rel'] for e in fast.unoffered_tail(prefix, {'active/a'})], ['active/b'])
        for assigned in ({'outside'}, {'active/a', 'active/b', 'active'}):
            with self.assertRaises(ValueError):
                fast.unoffered_tail(prefix, assigned)
        prefix.tail.clear()
        with self.assertRaisesRegex(ValueError, 'accounted'):
            fast.unoffered_tail(prefix, {'active/a'})

    def test_tail_new_directories_and_errors_invalidate_candidates(self):
        prefix = self.prefix()
        values = prefix.containers()
        values['completed_candidates'] = {'active': prefix.directories['active']}
        continued = fast.ContinuingPrefix(values)
        continued.add(directory('active/new'))
        self.assertEqual(continued.candidates, {})
        self.assertEqual(continued.records, prefix.records + 1)


class FastCoordinatorTests(Priority4CoordinatorTests):
    def make_fast(self):
        stage, manifest, stats = self.scan()
        prefix = Priority4Prefix()
        for line in manifest.read_text().splitlines():
            prefix.add(json.loads(line))
        prefix.candidates = {}
        assigned = {e['rel'] for p in self.run.archives(stage) for e in migration_io.read_json(p)['entries']}
        prepared = types.SimpleNamespace(prefix=prefix, assigned=assigned,
            hashes=[None] * len(self.run.archives(stage)), manifest=manifest)
        bundle = self.control / 'fixture-successor'
        bundle.mkdir()
        value = fast.FastPriority4Coordinator(self.control, prepared, bundle, {}, [])
        self.addCleanup(value.close)
        return value, manifest, prepared

    def process_scan(self, coordinator, number):
        actual = scanner.scan_phase
        def mapped(source, *args, **kwargs):
            self.assertEqual(kwargs['processes'], 16)
            self.assertTrue(kwargs['parallel_directory_metadata'])
            kwargs.update(processes=2, threads_per_process=2, concurrency_limit=lambda: 2)
            return actual(self.source, *args, **kwargs)
        with mock.patch.object(fast, 'scan_phase', side_effect=mapped):
            return coordinator.scan(4, number)

    def test_same_manifest_append_preserves_ids_assignments_and_totals(self):
        self.make_broker()
        self.put('active/old')
        coordinator, manifest, prepared = self.make_fast()
        before, inode = manifest.read_bytes(), manifest.stat().st_ino
        self.put('active/new')
        _, current, stats = self.process_scan(coordinator, 1)
        self.assertEqual(current, manifest)
        self.assertEqual(manifest.stat().st_ino, inode)
        self.assertTrue(manifest.read_bytes().startswith(before))
        tasks = [migration_io.read_json(p) for p in coordinator.archives('p4-r1')]
        self.assertEqual([t['id'] for t in tasks], [0, 1])
        self.assertEqual([e['rel'] for t in tasks for e in t['entries']], ['active/old', 'active/new'])
        self.assertEqual(stats['files'], 2)
        self.assertEqual(coordinator.state['next_task_id'], 2)
        self.assertEqual(coordinator.state['task_publication']['pending'], 0)

    def test_second_round_has_no_seed_shortcut_and_observes_changed_files(self):
        self.make_broker()
        self.put('active/old', b'old')
        coordinator, manifest, _ = self.make_fast()
        self.put('active/old', b'changed-content')
        _, second, stats = self.process_scan(coordinator, 2)
        self.assertNotEqual(second, manifest)
        task = migration_io.read_json(coordinator.archives('p4-r2')[0])
        self.assertEqual(task['op'], 'sync')
        self.assertEqual(task['entries'][0]['size'], len(b'changed-content'))
        self.assertEqual(stats['files'], 1)
        self.assertFalse(coordinator.state['source_scan_acceleration']['same_pass_append'])

    def test_new_batches_amortize_control_requests_but_preserve_every_path_and_id(self):
        self.make_broker()
        self.put('active/old')
        coordinator, manifest, _ = self.make_fast()
        for i in range(520):
            self.put('active/new-' + str(i), b'small')
        _, _, stats = self.process_scan(coordinator, 1)
        tasks = [migration_io.read_json(p) for p in coordinator.archives('p4-r1')]
        self.assertEqual([t['id'] for t in tasks], [0, 1, 2, 3])
        self.assertEqual([len(t['entries']) for t in tasks], [1, 256, 256, 8])
        self.assertEqual(len({e['rel'] for t in tasks for e in t['entries']}), 521)
        self.assertEqual(stats['files'], 521)

    def test_empty_index_is_checked_once_but_nonempty_known_evidence_is_preserved(self):
        self.make_broker()
        self.put('active/old')
        coordinator, _, _ = self.make_fast()
        self.put('active/new')
        queries = []
        coordinator.db.set_trace_callback(queries.append)
        self.process_scan(coordinator, 1)
        self.assertFalse(any('WHERE rel=' in query for query in queries))
        self.assertEqual(sum('SELECT 1 FROM files LIMIT 1' in q for q in queries), 1)
        coordinator.db.execute('INSERT INTO files VALUES (?,?,?,?,?)',
            ('active/new', '{}', '{}', 1, 'proof'))
        coordinator.db.commit()
        self.process_scan(coordinator, 2)
        entries = [e for p in coordinator.archives('p4-r2') for e in migration_io.read_json(p)['entries']]
        self.assertEqual(next(e for e in entries if e['rel'] == 'active/new')['known']['sha256'], 'proof')


if __name__ == '__main__':
    unittest.main()
