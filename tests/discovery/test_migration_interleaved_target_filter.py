import gc
import json
from pathlib import Path
import threading
import time
import unittest
from unittest import mock

import migration_interleaved_target_filter as interleaved
from migration_overlap_parallel_filter import ParallelFilterResume
from migration_parallel_scan import _seed, scan_phase
from migration_scan import _Scanner
from test_migration_priority_feed_target_filter import PriorityFeedTargetFilterTests


class InterleavedTests(unittest.TestCase):
    def setUp(self):
        self.fixture = PriorityFeedTargetFilterTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def make(self, old, assigned=()):
        f = self.fixture
        obj = f.make(old=old, assigned=assigned)
        obj.__class__ = interleaved.InterleavedTargetFilterResume
        obj.supply_metrics = {'stop_polls': 0, 'gc_seconds': 0, 'gc_collections': 0}
        obj._gc_started = None
        obj._gc_callback = obj._observe_gc
        gc.callbacks.append(obj._gc_callback)
        self.addCleanup(lambda: gc.callbacks.remove(obj._gc_callback)
                        if obj._gc_callback in gc.callbacks else None)
        source = obj.old_manifests[0]
        obj.combined_manifest.write_bytes(source.read_bytes())
        scanner = _Scanner(None, None, 1024**3, 64, {}, None, 3)
        for entry in old:
            _seed(scanner, entry)
        spool = obj.control / 'orphans.jsonl'
        spool.write_text(''.join(json.dumps(e)+'\n' for e in old
                                if e['rel'] not in obj.assigned and not e.get('hardlink_to')))
        st = source.stat()
        obj.prepared_resume = {'source_manifest': str(source), 'source_manifest_dev': st.st_dev,
                               'source_manifest_ino': st.st_ino, 'manifest_offset': st.st_size,
                               'cache': str(obj.control/'cache.pickle'), 'spools': [str(spool)],
                               'orphans': len(old)}
        obj.prepared_values = {'seeded_paths': {e['rel'] for e in old},
                               'stats': scanner.stats, 'hardlinks': scanner.hardlinks}
        obj.handover = {'assignment_delta_path': str(obj.assigned_tasks)}
        obj._probe_missing = lambda e: ParallelFilterResume._probe_missing(obj, e)
        obj.fresh_entry = lambda e: {k: v for k, v in e.items() if k != '_refresh_source'}
        return obj

    def test_orphans_publish_during_scan_and_assigned_paths_are_never_reissued(self):
        f = self.fixture
        orphan, fresh, assigned = [f.entry('other/'+n) for n in ('orphan', 'fresh', 'assigned')]
        obj = self.make([orphan, assigned], assigned=[assigned])
        original_scan = scan_phase

        def delayed_scan(*args, **kwargs):
            until = time.monotonic()+3
            while time.monotonic() < until:
                kwargs['on_tick']()
                if any(e['rel'] == orphan['rel'] for t in f.published() for e in t['entries']):
                    break
                time.sleep(.01)
            else:
                self.fail('historical candidate was not published while scan was unfinished')
            self.assertFalse(obj.state.get('scan_complete', False))
            return original_scan(*args, **kwargs)

        with mock.patch.object(interleaved, 'scan_phase', side_effect=delayed_scan):
            obj.supplement_missing()
        rels = [e['rel'] for t in f.published() for e in t['entries']]
        self.assertCountEqual(rels, [orphan['rel'], fresh['rel']])
        self.assertTrue(obj.state['scan_complete'])
        self.assertLessEqual(obj.state['target_filter']['peak_inflight'], obj.filter_limit)

    def test_tail_and_assignment_beyond_tail_retained_and_existing_not_copied(self):
        f = self.fixture
        old, tail, owned = [f.entry('other/'+n) for n in ('existing', 'tail', 'owned')]
        obj = self.make([old], assigned=[owned])
        with open(obj.old_manifests[0], 'ab') as handle:
            handle.write((json.dumps(tail)+'\n').encode()+b'{"partial":')
        (f.destination/'other').mkdir()
        (f.destination/old['rel']).write_bytes(b'present')
        obj.supplement_missing()
        rels = [e['rel'] for t in f.published() for e in t['entries']]
        self.assertEqual(rels, [tail['rel']])
        rows = [json.loads(x) for x in obj.combined_manifest.read_text().splitlines()]
        for entry in [old, tail, owned]:
            self.assertEqual(sum(x['rel'] == entry['rel'] for x in rows), 1)
        self.assertEqual(len(json.loads((obj.adoption/'manifest-tail-issues.json').read_text())['issues']), 1)

    def test_stop_poll_coalesced_and_sticky(self):
        obj = object.__new__(interleaved.InterleavedTargetFilterResume)
        obj.control = Path('/unused-fixture')
        obj._stop_poll_lock = threading.Lock()
        obj._stop_poll_until = 0
        obj._stop_seen = False
        obj.supply_metrics = {'stop_polls': 0}
        with mock.patch.object(interleaved.time, 'monotonic', return_value=1), \
                mock.patch.object(Path, 'exists', return_value=False) as exists:
            for _ in range(10000):
                obj._check_stop()
            self.assertEqual(exists.call_count, 1)
        with mock.patch.object(interleaved.time, 'monotonic', return_value=1.11), \
                mock.patch.object(Path, 'exists', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                obj._check_stop()
        with mock.patch.object(Path, 'exists') as exists:
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                obj._check_stop()
            exists.assert_not_called()

    def test_prepared_append_requires_existing_manifest(self):
        f = self.fixture
        with self.assertRaises(FileNotFoundError):
            scan_phase(f.source, 3, lambda entries: None, f.control/'missing.jsonl',
                       append_manifest=True, initial_seeded_paths=set())

    def test_spawned_readers_and_probe_clients_keep_one_assignment_owner(self):
        from test_migration_target_probe import TargetProbeTests
        from migration_distributed_target_filter import DistributedTargetFilterResume
        f=self.fixture
        entries=[f.entry('other/'+name) for name in ('old','fresh','reserved','exists')]
        obj=self.make([entries[0],entries[2],entries[3]],assigned=[entries[2]])
        service_fixture=TargetProbeTests();service_fixture.setUp()
        self.addCleanup(service_fixture.doCleanups)
        service_fixture.service.source_root=str(f.source)
        service_fixture.service.destination_root=str(f.destination)
        server=service_fixture.server()
        (f.destination/'other').mkdir();(f.destination/'other/exists').write_bytes(b'present')
        obj.cfg.update(target_probe_endpoints=['http://127.0.0.1:'+str(server.server_port)],
            broker_token=service_fixture.cfg['broker_token'],metadata_client_processes=2,
            metadata_client_batch_size=2,metadata_threads_per_process=2,
            source_scan_processes=2,source_scan_threads_per_process=2)
        DistributedTargetFilterResume._init_probe_client(obj,obj.cfg)
        obj.supplement_missing()
        published=[e['rel'] for t in f.published() for e in t['entries']]
        self.assertCountEqual(published,['other/old','other/fresh'])
        self.assertEqual(obj.state['target_filter']['submitted'],obj.state['target_filter']['checked'])
        self.assertTrue(obj.state['scan_complete'])
        self.assertTrue(obj.supply_metrics['metadata_child_pids'])
        self.assertLessEqual(obj.state['target_filter']['peak_inflight'],obj.filter_limit)


if __name__ == '__main__':
    unittest.main()
