"""Local fixtures: fresh copy supply must not wait for old orphan probes."""

import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest import mock

import migration_priority_feed_target_filter as priority
from migration_io import read_json, write_json
from migration_overlap_parallel_filter import ParallelFilterResume
from migration_overlap_resume import WARM_STAGE
from migration_parallel_scan import scan_phase
from migration_scan import _stat_fields
from migration_task_publisher import BoundedTaskPublisher, PublicationError


class PriorityFeedTargetFilterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='priority-feed-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source'
        self.destination = self.root / 'destination'
        self.control = self.root / 'control'
        for path in (self.source, self.destination, self.control):
            path.mkdir()
        self.snapshots = []
        self.objects = []
        self.addCleanup(self.close)
        self.save_patch = mock.patch.object(
            priority.PipelinedTargetFilterResume.__mro__[1], 'save',
            autospec=True, side_effect=self.parent_save)
        self.save_patch.start()
        self.addCleanup(self.save_patch.stop)

    def parent_save(self, obj, **updates):
        snapshot = copy.deepcopy(updates)
        self.snapshots.append(snapshot)
        obj.state.update(snapshot)

    def close(self):
        for obj in self.objects:
            if obj._task_publisher is not None:
                try:
                    obj._task_publisher.close()
                except PublicationError:
                    pass
            obj.db.close()

    def entry(self, rel, **updates):
        path = self.source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(rel.encode())
        result = {'rel': rel, 'kind': 'file', **_stat_fields(path.stat())}
        result.update(updates)
        return result

    def make(self, old=(), assigned=(), tail=b'', writer=write_json):
        obj = object.__new__(priority.PriorityFeedTargetFilterResume)
        self.objects.append(obj)
        obj.control = self.control
        obj.cfg = {'run_id': 'priority-feed-local', 'source': str(self.source),
                   'destination': str(self.destination)}
        obj.stage = WARM_STAGE
        obj.state = {'recovery_warm': True, 'status': 'scanning'}
        obj.db = sqlite3.connect(':memory:')
        obj.db.execute('CREATE TABLE files (rel TEXT PRIMARY KEY, source TEXT, '
                       'destination TEXT, owned INTEGER, sha256 TEXT)')
        obj.assigned = {e['rel'] for e in assigned}
        obj.next_task_id = 700
        obj.enqueued_batches = 1
        obj.discovered_files = 5
        obj.discovered_bytes = 50
        obj.phase_progress = {'3': {'discovered_files': 5, 'discovered_bytes': 50}}
        obj.current_scan = {'files': 5, 'bytes': 50}
        obj.last_scan_save = time.monotonic()
        obj._check_stop = mock.Mock()
        obj.scan_workers = 2
        obj.scan_batch_files = 1
        obj.filter_workers = 4
        obj.filter_limit = 8
        obj.adoption = self.control / 'adoption'
        obj.adoption.mkdir()
        obj.old_manifests = [self.control / 'old-a.jsonl', self.control / 'old-b.jsonl']
        obj.old_manifests[0].write_bytes(
            b''.join(json.dumps(e).encode() + b'\n' for e in old) + tail)
        obj.old_manifests[1].write_text('')
        obj.assigned_tasks = self.control / 'assigned.jsonl'
        obj.assigned_tasks.write_text(json.dumps({
            'id': 699, 'phase': 3, 'round': 3, 'op': 'sync',
            'entries': list(assigned)}) + '\n')
        obj.combined_manifest = self.control / 'combined.jsonl'
        obj.recovery_replay = {'phase': 'inventory_replay', 'seeded_records': 0,
                               'deferred': 0, 'consumed': 0}
        obj._task_publisher = BoundedTaskPublisher(workers=2, max_pending=4, writer=writer)
        obj._publication_tasks = {}
        obj._publication_error = None
        obj._publication_failures = []
        obj._publication_successes = 0
        obj._publication_workers = 2
        obj._publication_limit = 4
        obj._publication_enabled = True
        obj._publication_failure_path = None
        obj._publication_finished = False
        obj._publication_last_tuning_check = 0
        obj._publication_tuning_id = None
        obj._publication_tuning_error = None
        return obj

    def published(self):
        return [read_json(path) for path in
                (self.control / 'stages' / WARM_STAGE / 'pending').glob('*/*.json')]

    def test_fresh_tasks_visible_before_old_probe_seed_has_no_probes_and_old_all_checked(self):
        orphan = self.entry('other/orphan')
        existing = self.entry('other/existing')
        assigned = self.entry('other/assigned')
        assigned_tail = self.entry('other/assigned-beyond-tail', known={'proof': 'kept-in-task'})
        fresh = self.entry('other/fresh')
        (self.destination / 'other').mkdir()
        (self.destination / existing['rel']).write_bytes(b'exists')

        def slow_fresh_writer(path, task):
            if any(e['rel'] == fresh['rel'] for e in task['entries']):
                time.sleep(.05)
            return write_json(path, task)

        obj = self.make(old=[orphan, existing, assigned],
                        assigned=[assigned, assigned_tail], tail=b'{"rel":',
                        writer=slow_fresh_writer)
        # A duplicated old record must not be deferred/probed twice.
        obj.old_manifests[1].write_text(json.dumps(orphan) + '\n')
        preserved = {p: p.read_bytes() for p in obj.old_manifests + [obj.assigned_tasks]}
        calls = []
        main_ident = threading.get_ident()

        def probe(entry):
            calls.append((entry['rel'], obj.recovery_replay['phase'],
                          bool(entry.get('_refresh_source'))))
            self.assertNotEqual(obj.recovery_replay['phase'], 'inventory_replay')
            if entry.get('_refresh_source'):
                # Test the actual pending files, not just a submit callback.
                published_rels = {e['rel'] for t in self.published() for e in t['entries']}
                self.assertIn(fresh['rel'], published_rels)
                self.assertEqual(obj.recovery_replay['phase'], 'deferred_orphans')
                time.sleep(.01)
            return ParallelFilterResume._probe_missing(obj, entry)

        obj._probe_missing = probe
        original_emit = obj._emit_tasks

        def emit(*args, **kwargs):
            self.assertEqual(threading.get_ident(), main_ident)
            return original_emit(*args, **kwargs)

        obj._emit_tasks = emit
        obj.supplement_missing()
        self.assertEqual({rel for rel, _, _ in calls},
                         {orphan['rel'], existing['rel'], fresh['rel']})
        self.assertEqual(len(calls), 3)
        tasks = self.published()
        self.assertEqual(sorted(t['id'] for t in tasks), [700, 701])
        self.assertEqual({e['rel'] for t in tasks for e in t['entries']},
                         {orphan['rel'], fresh['rel']})
        self.assertTrue(all('_refresh_source' not in e for t in tasks for e in t['entries']))
        combined = [json.loads(line) for line in obj.combined_manifest.read_text().splitlines()]
        rels = [entry['rel'] for entry in combined]
        self.assertEqual(len(rels), len(set(rels)))
        self.assertIn(assigned_tail['rel'], rels)
        self.assertNotIn('known', next(e for e in combined if e['rel'] == assigned_tail['rel']))
        self.assertEqual(obj.state['recovery_replay']['phase'], 'complete')
        self.assertEqual(obj.state['recovery_replay']['deferred'], 2)
        self.assertEqual(obj.state['recovery_replay']['consumed'], 2)
        self.assertEqual(obj.state['target_filter']['checked'], 3)
        self.assertLessEqual(obj.state['target_filter']['peak_inflight'], 8)
        self.assertTrue(obj.state['scan_complete'])
        self.assertEqual(len(read_json(obj.adoption / 'manifest-tail-issues.json')['issues']), 1)
        for path, before in preserved.items():
            self.assertEqual(path.read_bytes(), before)
        self.assertIsNone(obj._task_publisher)
        self.assertEqual([s['recovery_replay']['phase'] for s in self.snapshots],
                         ['fresh_scan', 'deferred_orphans', 'complete'])

    def test_seeded_hardlink_canonical_is_not_duplicated_by_fresh_scan(self):
        canonical = self.entry('other/canonical')
        os.link(self.source / canonical['rel'], self.source / 'other/new-alias')
        canonical = self.entry(canonical['rel'])
        obj = self.make(old=[canonical])
        calls = []

        def probe(entry):
            calls.append(entry['rel'])
            return ParallelFilterResume._probe_missing(obj, entry)

        obj._probe_missing = probe
        obj.supplement_missing()
        self.assertEqual(calls, [canonical['rel']])
        combined = [json.loads(line) for line in obj.combined_manifest.read_text().splitlines()]
        alias = next(e for e in combined if e['rel'] == 'other/new-alias')
        self.assertEqual(alias['hardlink_to'], canonical['rel'])
        self.assertEqual(obj.state['warm_missing_scan']['hardlinks'], 1)

    def test_seed_checkpoints_without_submitting_probes_and_cancel_never_marks_complete(self):
        orphan = self.entry('other/orphan')
        obj = self.make(old=[orphan])
        obj._probe_missing = mock.Mock()
        real_monotonic = time.monotonic
        clock = [real_monotonic()]

        def tick():
            clock[0] += 4
            return clock[0]

        def cancel_scan(*args, **kwargs):
            list(kwargs['resume_records'])
            self.assertTrue(kwargs['skip_seeded_nondirectories'])
            obj._probe_missing.assert_not_called()
            raise RuntimeError('fixture cancelled before fresh walk')

        with mock.patch.object(priority, 'scan_phase', side_effect=cancel_scan), \
                mock.patch.object(priority.time, 'monotonic', side_effect=tick):
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                obj.supplement_missing()
        self.assertTrue(any(s['recovery_replay']['phase'] == 'inventory_replay'
                            for s in self.snapshots))
        self.assertFalse(any(s.get('scan_complete') for s in self.snapshots))
        self.assertIsNone(obj._task_publisher)
        self.assertEqual(obj.recovery_replay['consumed'], 0)

    def test_fresh_publication_failure_does_not_start_old_probes_or_mark_complete(self):
        orphan = self.entry('other/orphan')
        self.entry('other/fresh')

        def failed_writer(path, task):
            raise OSError('fixture publication failure')

        obj = self.make(old=[orphan], writer=failed_writer)
        calls = []

        def probe(entry):
            calls.append(entry['rel'])
            return ParallelFilterResume._probe_missing(obj, entry)

        obj._probe_missing = probe
        with self.assertRaises(PublicationError):
            obj.supplement_missing()
        self.assertEqual(calls, ['other/fresh'])
        self.assertFalse(any(s.get('scan_complete') for s in self.snapshots))
        self.assertNotEqual(obj.recovery_replay['phase'], 'complete')
        self.assertEqual(obj.recovery_replay['consumed'], 0)
        self.assertIsNone(obj._task_publisher)


if __name__ == '__main__':
    unittest.main()
