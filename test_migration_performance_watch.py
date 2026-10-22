"""Metadata-only observer fixtures: fake publications, no SSH or copy actions."""
import copy
import datetime
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).absolute().parent))
from migration_performance_watch import PerformanceWatch, acquire_lock
from test_migration_performance import RUN, metrics, seed, state


class Store:
    def __init__(self):
        self.values = {}
        self.reads = []
        self.writes = []
        self.events = []
        self.stopped = False
        self.warning = None
        self.now = 300
        self.update()

    def update(self, now=300, rate=3e9):
        self.now = now
        self.values['state.json'] = state(now)
        self.values['metrics.json'] = metrics(now, copied_rate=rate)

    def read(self, path):
        name = pathlib.Path(path).name
        self.reads.append(name)
        value = self.values.get(name)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise FileNotFoundError(name)
        return copy.deepcopy(value)

    def write(self, path, value):
        self.writes.append(pathlib.Path(path).name)
        self.values[pathlib.Path(path).name] = copy.deepcopy(value)

    def nodes(self, control, warnings):
        if self.warning:
            warnings.append(self.warning)
        return [{'host': 'node-0', 'pid': 11, 'updated': self.now,
                 'counts': {'copied': self.now * 500, 'verified': self.now * 10},
                 'active': {str(i): {'size': 64 * 1024**2} for i in range(16)},
                 'components': [{'lane': lane, 'pid': pid, 'started': 0, 'updated': self.now}
                                for lane, pid in [('primary', 11), ('extra64', 12)]]}]

    def watch(self, control=None):
        return PerformanceWatch(control or '/nonexistent-fixture/.' + RUN,
                    reader=self.read, writer=self.write, node_loader=self.nodes,
                    clock=lambda: self.now, stop_reader=lambda: self.stopped,
                    event_writer=self.events.append, expected_nodes=1)


class WatchTests(unittest.TestCase):
    def test_copy_and_seed_are_independent_and_only_private_outputs_written(self):
        store = Store()
        store.values['seed-progress.json'] = seed(rate=25)
        result = store.watch().sample()
        self.assertTrue(result['lanes']['copy']['trigger'])
        self.assertTrue(result['lanes']['seed']['trigger'])
        self.assertEqual(result['lanes']['seed']['mode'], 'seed_reading_receipts')
        self.assertEqual({e['lane'] for e in store.events}, {'copy', 'seed'})
        self.assertEqual(store.writes, ['performance.json'])
        self.assertFalse(set(store.reads) & {'config.json', 'broker.json'})
        self.assertFalse(result['constraints']['deploy_allowed'])

    def test_trigger_recovery_events_only_on_transitions_and_restart_preserves_incident(self):
        store = Store()
        watch = store.watch()
        first = watch.sample()
        self.assertTrue(first['optimize'])
        watch.sample()
        self.assertEqual(len(store.events), 1)
        resumed = store.watch()
        store.update(600, rate=9e9)
        last = resumed.sample()
        self.assertFalse(last['lanes']['copy']['trigger'])
        self.assertEqual([e['event'] for e in store.events], ['trigger', 'recovered'])

    def test_stop_preserves_incident_without_optimization_or_false_recovery(self):
        store = Store()
        watch = store.watch()
        watch.sample()
        store.update(600, rate=9e9)
        store.stopped = True
        result = watch.sample()
        self.assertTrue(result['terminal'])
        self.assertEqual(result['status'], 'stopped')
        self.assertTrue(result['lanes']['copy']['trigger'])
        self.assertFalse(result['optimize'])
        self.assertEqual([e['event'] for e in store.events], ['trigger'])

    def test_seed_complete_requires_matching_published_proof(self):
        store = Store()
        store.values['seed-progress.json'] = seed(rate=25)
        watch = store.watch()
        watch.sample()
        store.update(600)
        summary = {'verified_files': 90, 'verified_bytes': 900, 'verified_symlinks': 1,
                   'receipts': 5, 'controls': ['/old-control'], 'db_path': '/tmp/' + RUN + '-resume-candidate.sqlite'}
        store.values['seed-progress.json'] = dict(summary, run_id=RUN, status='complete', updated=600)
        unproven = watch.sample()
        self.assertTrue(unproven['lanes']['seed']['trigger'])
        self.assertIn('seed_completion_unproven', unproven['lanes']['seed']['diagnostics'])
        store.values['overlap-seed-result.json'] = {
            'ok': True, 'summary': summary, 'merge': {'rows_processed': 91}, 'finished': 600}
        complete = watch.sample()
        self.assertFalse(complete['lanes']['seed']['trigger'])
        self.assertEqual(complete['lanes']['seed']['completion_evidence']['kind'], 'overlap_seed_result')
        self.assertEqual(complete['lanes']['seed']['incident']['resolution'], 'verified_seed_completion')
        self.assertEqual(store.events[-1]['event'], 'recovered')
        self.assertEqual(store.events[-1]['resolution'], 'verified_seed_completion')

    def test_completed_seed_never_closes_on_stop_or_mismatched_run(self):
        store = Store()
        store.values['seed-progress.json'] = seed(rate=25)
        watch = store.watch()
        watch.sample()
        summary = {'verified_files': 90, 'verified_bytes': 900, 'verified_symlinks': 0,
                   'receipts': 5, 'controls': ['/old-control'], 'db_path': '/tmp/' + RUN + '-index.sqlite'}
        store.values['resume-index.json'] = summary
        store.values['seed-progress.json'] = dict(summary, run_id='other-run', status='complete', updated=300)
        result = watch.sample()
        self.assertTrue(result['lanes']['seed']['trigger'])
        self.assertNotIn('completion_evidence', result['lanes']['seed'])
        store.values['seed-progress.json']['run_id'] = RUN
        store.stopped = True
        result = watch.sample()
        self.assertTrue(result['lanes']['seed']['trigger'])
        self.assertNotIn('completion_evidence', result['lanes']['seed'])

    def test_missing_lane_and_latest_reader_failure_cannot_clear_incident(self):
        store = Store()
        watch = store.watch()
        watch.sample()
        store.update(600, rate=9e9)
        original = store.nodes
        def one_lane(control, warnings):
            value = original(control, warnings)
            value[0]['components'].pop()
            return value
        watch.node_loader = one_lane
        incomplete = watch.sample()
        self.assertTrue(incomplete['lanes']['copy']['trigger'])
        self.assertFalse(incomplete['optimize'])
        self.assertIn('node_components_incomplete', incomplete['warnings'])
        watch.node_loader = original
        store.values['state.json'] = OSError('latest version unavailable')
        unavailable = watch.sample()
        self.assertTrue(unavailable['lanes']['copy']['trigger'])
        self.assertFalse(unavailable['optimize'])
        self.assertTrue(unavailable['read_errors'])

    def test_dense_counts_and_saved_histories_are_bounded_to_thirty_minutes(self):
        store = Store()
        watch = store.watch()
        for now in range(300, 2501, 10):
            store.update(now, rate=1e9)
            store.values['state.json']['workload'] = 'small'
            store.values['metrics.json'] = metrics(now, mode='copy_small', copied_rate=1e9)
            result = watch.sample()
        self.assertTrue(result['lanes']['copy']['trigger'])
        self.assertEqual(result['lanes']['copy']['rates']['copied_files_per_second'], 500)
        self.assertTrue(all(p['sample_time'] >= 700 for p in result['samples']))
        self.assertLessEqual(len(result['samples']), 182)
        observations = result['lanes']['copy']['next_state']['observations']
        self.assertTrue(all(p['sample_time'] >= 700 for p in observations))
        self.assertLessEqual(len(observations), 182)

    def test_unreadable_saved_evidence_fails_instead_of_erasing_incident(self):
        store = Store()
        store.values['performance.json'] = OSError('cannot read saved evidence')
        with self.assertRaises(OSError):
            store.watch()
        self.assertEqual(store.writes, [])

    def test_real_single_seed_snapshots_accumulate_five_minute_evidence(self):
        store = Store()
        watch = store.watch()
        for now in range(300, 601, 10):
            store.update(now)
            store.values['seed-progress.json'] = {
                'run_id': RUN, 'updated': now, 'status': 'reading_receipts',
                'processed_receipts': now * 25, 'receipt_paths_discovered': 150000,
                'verified_files': 0, 'verified_symlinks': 0}
            result = watch.sample()
            if now < 600:
                self.assertFalse(result['lanes']['seed']['armed'])
        self.assertTrue(result['lanes']['seed']['trigger'])
        self.assertEqual(result['lanes']['seed']['rates']['processed_receipts_per_second'], 25)
        self.assertEqual(result['lanes']['seed']['window']['seconds'], 300)

    def test_telemetry_insufficient_events_mark_both_transitions_without_actions(self):
        store = Store()
        watch = store.watch()
        for now in range(300, 601, 10):
            store.update(now)
            store.values['metrics.json'] = metrics(now, start=now - 20)
            result = watch.sample()
        self.assertIn('telemetry_insufficient', result['lanes']['copy']['diagnostics'])
        self.assertEqual([(e['event'], e.get('active')) for e in store.events],
                         [('telemetry_insufficient', True)])
        store.update(610)
        result = watch.sample()
        self.assertTrue(result['optimize'])
        self.assertEqual([(e['event'], e.get('active')) for e in store.events],
                         [('telemetry_insufficient', True), ('trigger', None),
                          ('telemetry_insufficient', False)])
        self.assertEqual(set(store.writes), {'performance.json'})

    def test_terminal_migration_blocks_seed_rate_alarm_before_exit(self):
        store = Store()
        store.values['state.json']['status'] = 'failed'
        store.values['seed-progress.json'] = seed(rate=25)
        result = store.watch().sample()
        self.assertTrue(result['terminal'])
        self.assertFalse(result['optimize'])
        self.assertFalse(result['lanes']['seed']['armed'])

    def test_real_imported_files_updated_samples_evaluate_importing_lane(self):
        store = Store()
        watch = store.watch()
        for now in range(300, 601, 10):
            store.update(now)
            store.values['seed-progress.json'] = {
                'run_id': RUN, 'status': 'importing',
                'updated': datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat(),
                'imported_files': now * 4000, 'verified_files': 9_623_742,
                'verified_symlinks': 60_169, 'receipts': 190_544}
            result = watch.sample()
            if now < 600:
                self.assertFalse(result['lanes']['seed']['armed'])
        lane = result['lanes']['seed']
        self.assertEqual(lane['mode'], 'seed_importing')
        self.assertTrue(lane['trigger'])
        self.assertEqual(lane['rates']['imported_files_per_second'], 4000)
        self.assertEqual(lane['unit'], 'rows/s')
        self.assertEqual(result['samples'][-1]['seed_imported_files'], 2_400_000)
        self.assertNotIn('seed_phase_has_no_rate_threshold', lane['reason_codes'])

    def test_imported_files_completion_alias_must_match_actual_unique_rows(self):
        store = Store()
        summary = {'verified_files': 90, 'verified_bytes': 900, 'verified_symlinks': 1,
                   'receipts': 5, 'controls': ['/old-control'], 'db_path': '/tmp/' + RUN + '-resume-candidate.sqlite'}
        store.values['seed-progress.json'] = dict(summary, run_id=RUN, status='complete', updated=300)
        store.values['overlap-seed-result.json'] = {
            'ok': True, 'summary': summary, 'merge': {'imported_files': 91}, 'finished': 300}
        watch = store.watch()
        valid = watch.sample()
        self.assertEqual(valid['lanes']['seed']['completion_evidence']['imported_files'], 91)
        store.values['overlap-seed-result.json']['merge']['rows_processed'] = 90
        conflict = watch.sample()
        self.assertNotIn('completion_evidence', conflict['lanes']['seed'])
        self.assertIn('seed_completion_unproven', conflict['lanes']['seed']['diagnostics'])

    def test_master_local_flock_excludes_second_publisher(self):
        with tempfile.TemporaryDirectory() as temp:
            control = pathlib.Path(temp) / ('.perf-watch-fixture-' + pathlib.Path(temp).name)
            first = acquire_lock(control)
            try:
                with self.assertRaises(BlockingIOError):
                    acquire_lock(control)
            finally:
                first.close()
            second = acquire_lock(control)
            second.close()


if __name__ == '__main__':
    unittest.main()
