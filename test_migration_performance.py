"""Synthetic, deterministic performance fixtures; no I/O or runtime tuning."""

import copy
import datetime
import json
from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).absolute().parent))
from migration_performance import evaluate_performance


RUN = 'new-run'
STAGE = 'p3-r1'
BASE_POLICY = {'expected_nodes': 1}


def state(now, workload='large', status='copying', **extra):
    value = {'run_id': RUN, 'stage': STAGE, 'status': status, 'updated': now,
             'workload': workload, 'scan_complete': False}
    value.update(extra)
    return value


def nodes(now, copied=0, verified=0, size=64 * 1024**2, count=16):
    return [{'host': 'node-0', 'pid': 11, 'updated': now,
             'counts': {'copied': copied, 'verified': verified, 'skipped': 0},
             'active': {str(i): {'size': size} for i in range(count)}}]


def metrics(end=300, mode='copy_large', copied_rate=3e9, streamed_rate=None,
            file_rate=None, revalidation_rate=None, start=None, rate_fn=None):
    start = end - 300 if start is None else start
    streamed_rate = copied_rate if streamed_rate is None else streamed_rate
    samples = []
    for tick in range(int(start), int(end) + 1, 5):
        point = {'sample_time': tick, 'run_id': RUN, 'stage': STAGE, 'mode': mode,
                 'nodebytes': rate_fn(tick) if rate_fn else tick * copied_rate,
                 'streamed_bytes': rate_fn(tick) if rate_fn else tick * streamed_rate,
                 'verified_files': 1234}
        if file_rate is not None:
            point['copied_files'] = tick * file_rate
        if revalidation_rate is not None:
            point['revalidated_files'] = tick * revalidation_rate
        samples.append(point)
    return {'run_id': RUN, 'sample_time': end, 'current_stage': STAGE, 'samples': samples}


def seed(end=300, status='reading_receipts', rate=25):
    samples = []
    for tick in range(end - 300, end + 1, 5):
        samples.append({'sample_time': tick, 'run_id': RUN, 'status': status,
                        'processed_receipts': tick * rate if status == 'reading_receipts' else 150000,
                        'verified_files': tick * rate if status == 'finalizing' else 9000000,
                        'verified_symlinks': 0, 'receipt_paths_discovered': tick * rate})
    return {'run_id': RUN, 'updated': end, 'status': status, 'samples': samples}


class PerformanceTests(unittest.TestCase):
    def evaluate(self, s=None, m=None, n=None, now=300, **kwargs):
        policy = dict(BASE_POLICY, **kwargs.pop('policy', {}))
        return evaluate_performance(s or state(now), m or metrics(now),
                                    n if n is not None else nodes(now), now=now,
                                    policy=policy, **kwargs)

    def test_first_full_five_minute_low_copy_window_triggers(self):
        result = self.evaluate()
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertTrue(result['optimize'])
        self.assertEqual(result['window']['seconds'], 300)
        self.assertEqual(result['low_threshold'], 6e9)
        self.assertEqual(result['recovery_target'], 8e9)
        self.assertEqual(result['rates']['effective_Bps'], 3e9)
        self.assertFalse(result['constraints']['stop_copy_allowed'])
        self.assertFalse(result['constraints']['restart_copy_allowed'])

    def test_large_inflight_stream_prevents_zero_completion_false_alarm(self):
        result = self.evaluate(m=metrics(copied_rate=0, streamed_rate=9e9))
        self.assertTrue(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertTrue(result['recovery_target_met'])

    def test_zero_write_large_file_needs_real_readback_evidence(self):
        result = self.evaluate(m=metrics(copied_rate=0), n=nodes(300, size=2 * 1024**3))
        self.assertFalse(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertIn('inflight_unobservable', result['reason_codes'])
        self.assertTrue(result['diagnostic_trigger'])
        n = nodes(300, size=2 * 1024**3)
        n[0]['readback_progress'] = {'sample_time': 300, 'fd_count': 1,
                                     'bytes_delta': 50e9, 'seconds': 5}
        result = self.evaluate(m=metrics(copied_rate=0), n=n)
        self.assertIn('inflight_readback_progress', result['reason_codes'])
        self.assertFalse(result['diagnostic_trigger'])
        n[0]['readback_progress']['bytes_delta'] = 1e6
        slow = self.evaluate(m=metrics(copied_rate=0), n=n)
        self.assertFalse(slow['armed'], 'a short readback probe is not a five-minute rate')
        self.assertIn('inspect_low_readback_throughput', slow['diagnostics'])

    def test_small_files_use_copied_count_not_unique_verified(self):
        result = self.evaluate(s=state(300, 'small'),
                               m=metrics(mode='copy_small', copied_rate=1e9, file_rate=500),
                               n=nodes(300, copied=150000, size=4096))
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['rates']['copied_files_per_second'], 500)
        self.assertEqual(result['low_threshold'], 1000)

    def test_small_file_low_is_suppressed_by_healthy_mixed_byte_progress(self):
        result = self.evaluate(s=state(300, 'small'),
                               m=metrics(mode='copy_small', copied_rate=7e9, file_rate=500),
                               n=nodes(300, copied=150000, size=4096))
        self.assertTrue(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertIn('healthy_byte_throughput_suppresses_small_file_low', result['evidence'])

    def test_fast_revalidation_uses_raw_counts_with_zero_write(self):
        result = self.evaluate(s=state(300, 'revalidation'),
                               m=metrics(mode='copy_revalidation', copied_rate=0, revalidation_rate=2000),
                               n=nodes(300, verified=600000, size=4096))
        self.assertTrue(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertEqual(result['rates']['revalidated_files_per_second'], 2000)

    def test_seed_reading_uses_receipts_and_root_targets_not_copy_zero(self):
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               m=metrics(copied_rate=0), seed_progress=seed(rate=25))
        self.assertEqual(result['mode'], 'seed_reading_receipts')
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['rates']['processed_receipts_per_second'], 25)
        self.assertEqual(result['low_threshold'], 100)
        self.assertEqual(result['recovery_target'], 200)

    def test_seed_finalizing_has_actual_index_file_rate(self):
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=seed(status='finalizing', rate=4000))
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['rates']['indexed_files_per_second'], 4000)
        self.assertEqual(result['low_threshold'], 5000)
        self.assertEqual(result['recovery_target'], 10000)

    def test_enumeration_growth_has_no_wrong_reading_rate_alarm(self):
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=seed(status='enumerating', rate=1000))
        self.assertFalse(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertFalse(result['diagnostic_trigger'])
        stalled = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                                seed_progress=seed(status='enumerating', rate=0))
        self.assertIn('progress_unobservable', stalled['diagnostics'])

    def test_recovery_requires_full_post_trigger_window_and_survives_json(self):
        triggered = json.loads(json.dumps(self.evaluate()))
        payload = lambda tick: 3e9 * min(tick, 300) + 9e9 * max(0, tick - 300)
        partial = self.evaluate(now=550, m=metrics(550, rate_fn=payload), previous=triggered)
        self.assertTrue(partial['armed'])
        self.assertTrue(partial['recovery_target_met'])
        self.assertTrue(partial['trigger'])
        self.assertNotIn('node_process_changed', partial['reason_codes'])
        recovered = self.evaluate(now=600, m=metrics(600, rate_fn=payload), previous=partial)
        self.assertTrue(recovered['armed'])
        self.assertFalse(recovered['trigger'])
        self.assertEqual(recovered['incident']['recovered_at'], 600)

    def test_stale_gap_reset_and_stop_never_clear_existing_incident(self):
        triggered = self.evaluate()
        stale = self.evaluate(now=600, m=metrics(500, copied_rate=9e9), previous=triggered)
        self.assertFalse(stale['armed'])
        self.assertTrue(stale['trigger'])
        self.assertFalse(stale['optimize'])
        self.assertIn('stale_samples', stale['reason_codes'])
        stopped = self.evaluate(previous=triggered, stop=True)
        self.assertTrue(stopped['trigger'])
        self.assertFalse(stopped['optimize'])
        self.assertFalse(stopped['diagnostic_trigger'])
        for modification, reason in (('gap', 'sample_gap'), ('reset', 'counter_reset')):
            m = metrics()
            if modification == 'gap':
                m['samples'] = [p for p in m['samples'] if not 100 <= p['sample_time'] <= 200]
            else:
                m['samples'][20]['streamed_bytes'] = 0
            result = self.evaluate(m=m, previous=triggered)
            self.assertFalse(result['armed'])
            self.assertTrue(result['trigger'])
            self.assertIn(reason, result['reason_codes'])

    def test_incomplete_history_eventually_requests_telemetry_without_resetting_wait(self):
        first = self.evaluate(m=metrics(start=240))
        self.assertFalse(first['armed'])
        self.assertIn('insufficient_samples', first['reason_codes'])
        later = self.evaluate(now=600, m=metrics(600, start=540), previous=first)
        self.assertFalse(later['armed'])
        self.assertIn('telemetry_insufficient', later['diagnostics'])
        self.assertEqual(later['next_state']['first_awaiting_since'], 300)

    def test_per_node_reset_cannot_be_hidden_by_another_node_growth(self):
        n = nodes(300, copied=100) + [dict(nodes(300, copied=100)[0], host='node-1', pid=22)]
        prior = self.evaluate(n=n, policy={'expected_nodes': 2})
        current = nodes(600, copied=90) + [dict(nodes(600, copied=120)[0], host='node-1', pid=22)]
        result = self.evaluate(now=600, n=current, m=metrics(600), previous=prior,
                               policy={'expected_nodes': 2})
        self.assertFalse(result['armed'])
        self.assertIn('counter_reset', result['reason_codes'])
        self.assertTrue(result['trigger'])

    def test_mixed_run_stage_unknown_node_and_tail_are_not_cluster_low(self):
        m = metrics()
        m['run_id'] = 'old-run'
        self.assertIn('metrics_run_mismatch', self.evaluate(m=m)['reason_codes'])
        m = metrics()
        m['samples'][30]['stage'] = 'p1-r1'
        self.assertFalse(self.evaluate(m=m)['armed'])
        n = nodes(300)
        n[0]['updated'] = 200
        self.assertIn('stale_nodes', self.evaluate(n=n)['reason_codes'])
        s = state(300, scan_complete=True, queue={'stage': STAGE, 'sample_time': 300, 'pending': 0, 'running': 2})
        self.assertIn('tail_workload_not_cluster_copy', self.evaluate(s=s)['reason_codes'])
        s['queue']['stage'] = 'old-stage'
        self.assertTrue(self.evaluate(s=s)['armed'], 'old-stage queue cannot manufacture a tail exemption')

    def test_pure_inputs_configurable_policy_and_minimum_window(self):
        inputs = [state(300), metrics(), nodes(300)]
        before = copy.deepcopy(inputs)
        result = self.evaluate(*inputs, policy={'large_low_Bps': 2e9, 'large_recovery_Bps': 4e9})
        self.assertFalse(result['trigger'])
        self.assertEqual(inputs, before)
        json.dumps(result, allow_nan=False)
        with self.assertRaises(ValueError):
            self.evaluate(policy={'window_seconds': 270})

    def test_actual_untagged_metrics_cannot_borrow_context_before_observation(self):
        previous = None
        for now in range(300, 601, 10):
            m = metrics(now)
            m.pop('run_id')
            m['control'] = '/share-mixed/project/.' + RUN
            for point in m['samples']:
                for key in ('run_id', 'stage', 'mode'):
                    point.pop(key)
            result = self.evaluate(now=now, m=m, previous=previous)
            if now < 600:
                self.assertFalse(result['armed'])
                self.assertIn('insufficient_samples', result['reason_codes'])
            previous = result
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['window']['start'], 300)

    def test_dense_sampler_observations_supply_real_small_file_count_history(self):
        previous = None
        for now in range(300, 601, 10):
            m = metrics(now, mode='copy_small', copied_rate=1e9)
            result = self.evaluate(now=now, s=state(now, 'small'), m=m,
                                   n=nodes(now, copied=now * 500, size=4096), previous=previous)
            if now < 600:
                self.assertFalse(result['armed'])
            previous = json.loads(json.dumps(result))
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['rates']['copied_files_per_second'], 500)

    def test_same_pid_new_instance_cannot_hide_worker_restart(self):
        old_nodes = nodes(300, copied=100)
        old_nodes[0].update(started=10, instance='old-worker')
        prior = self.evaluate(n=old_nodes)
        new_nodes = nodes(600, copied=100)
        new_nodes[0].update(started=590, instance='new-worker')
        result = self.evaluate(now=600, n=new_nodes, m=metrics(600), previous=prior)
        self.assertFalse(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertIn('node_process_changed', result['reason_codes'])

    def test_stale_observation_requires_full_later_recovery_window(self):
        triggered = self.evaluate()
        stale = self.evaluate(now=450, m=metrics(450, copied_rate=9e9),
                              n=nodes(400), previous=triggered)
        self.assertFalse(stale['armed'])
        self.assertEqual(stale['next_state']['last_disruption_at'], 450)
        fresh = self.evaluate(now=600, m=metrics(600, copied_rate=9e9), previous=stale)
        self.assertTrue(fresh['recovery_target_met'])
        self.assertTrue(fresh['trigger'])
        recovered = self.evaluate(now=750, m=metrics(750, copied_rate=9e9), previous=fresh)
        self.assertFalse(recovered['trigger'])

    def test_stop_requires_full_later_recovery_window(self):
        triggered = self.evaluate()
        stopped = self.evaluate(now=450, m=metrics(450, copied_rate=9e9),
                                previous=triggered, stop=True)
        early = self.evaluate(now=600, m=metrics(600, copied_rate=9e9), previous=stopped)
        self.assertTrue(early['armed'])
        self.assertTrue(early['trigger'])
        recovered = self.evaluate(now=750, m=metrics(750, copied_rate=9e9), previous=early)
        self.assertFalse(recovered['trigger'])

    def test_workload_oscillation_does_not_silence_missing_telemetry(self):
        previous = None
        for now in range(300, 601, 10):
            workload = 'small' if now % 20 else 'large'
            result = self.evaluate(now=now, s=state(now, workload),
                                   m=metrics(now, mode='copy_' + workload, start=now - 20),
                                   n=nodes(now, size=4096 if workload == 'small' else 64 * 1024**2),
                                   previous=previous)
            previous = result
        self.assertFalse(result['armed'])
        self.assertIn('telemetry_insufficient', result['diagnostics'])
        self.assertEqual(result['next_state']['first_awaiting_since'], 300)

    def test_less_than_five_minutes_does_not_trigger_and_exact_threshold_is_healthy(self):
        partial = self.evaluate(m=metrics(start=5))
        self.assertFalse(partial['trigger'])
        self.assertFalse(partial['armed'])
        exact = self.evaluate(m=metrics(copied_rate=6e9))
        self.assertTrue(exact['armed'])
        self.assertFalse(exact['trigger'])
        self.assertFalse(exact['recovery_target_met'])

    def test_low_incident_stays_latched_in_hysteresis_band(self):
        triggered = self.evaluate()
        between = self.evaluate(now=600, m=metrics(600, copied_rate=7e9), previous=triggered)
        self.assertTrue(between['armed'])
        self.assertTrue(between['optimize'])
        self.assertTrue(between['trigger'])
        self.assertFalse(between['below_threshold'])
        self.assertFalse(between['recovery_target_met'])

    def test_unknown_seed_phase_does_not_use_copy_byte_threshold(self):
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=seed(status='publishing', rate=20000))
        self.assertFalse(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertIsNone(result['low_threshold'])
        self.assertIn('seed_phase_has_no_rate_threshold', result['reason_codes'])

    def test_actual_imported_files_updated_json_has_independent_row_threshold(self):
        samples = [{'run_id': RUN, 'status': 'importing',
                    'updated': datetime.datetime.fromtimestamp(tick, datetime.timezone.utc).isoformat(),
                    'imported_files': tick * 4000, 'verified_files': 9_623_742,
                    'verified_symlinks': 60_169, 'processed_receipts': 190_544}
                   for tick in range(0, 301, 10)]
        progress = dict(samples[-1], samples=samples)
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=progress)
        self.assertTrue(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['mode'], 'seed_importing')
        self.assertEqual(result['rates']['imported_files_per_second'], 4000)
        self.assertEqual(result['unit'], 'rows/s')
        self.assertEqual(result['low_threshold'], 5000)
        self.assertEqual(result['recovery_target'], 10000)
        self.assertEqual(result['next_state']['seed_samples'][-1]['imported_files'], 1_200_000)

    def test_importing_legacy_rows_processed_is_supported_without_receipt_alias(self):
        samples = [{'sample_time': tick, 'run_id': RUN, 'status': 'importing',
                    'rows_processed': tick * 12000, 'processed_receipts': 190544,
                    'verified_files': 9623742, 'verified_symlinks': 60169}
                   for tick in range(0, 301, 10)]
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=dict(samples[-1], samples=samples))
        self.assertTrue(result['armed'])
        self.assertFalse(result['trigger'])
        self.assertTrue(result['recovery_target_met'])
        self.assertEqual(result['rates']['imported_files_per_second'], 12000)
        for point in samples:
            point.pop('rows_processed')
        absent = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=dict(samples[-1], samples=samples))
        self.assertFalse(absent['armed'])
        self.assertIn('insufficient_samples', absent['reason_codes'])
        self.assertEqual(absent['rates'], {}, 'fixed verified_files is not an import counter')

    def test_importing_rate_requires_full_window_and_post_incident_recovery(self):
        def progress(end, rate_fn):
            points = [{'sample_time': tick, 'run_id': RUN, 'status': 'importing',
                       'imported_files': rate_fn(tick)} for tick in range(end - 300, end + 1, 10)]
            return dict(points[-1], samples=points)
        low = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                            seed_progress=progress(300, lambda t: 4000*t))
        self.assertTrue(low['trigger'])
        count = lambda t: 4000 * min(t, 300) + 12000 * max(0, t - 300)
        partial = self.evaluate(now=550, s=state(550, status='preparing_resume', stage=None),
                                seed_progress=progress(550, count), previous=low)
        self.assertTrue(partial['recovery_target_met'])
        self.assertTrue(partial['trigger'])
        full = self.evaluate(now=600, s=state(600, status='preparing_resume', stage=None),
                             seed_progress=progress(600, count), previous=partial)
        self.assertFalse(full['trigger'])

    def test_import_alias_conflict_blocks_rate_without_clearing_incident(self):
        low_samples = [{'sample_time': tick, 'run_id': RUN, 'status': 'importing',
                        'imported_files': tick * 4000} for tick in range(0, 301, 10)]
        low = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                            seed_progress=dict(low_samples[-1], samples=low_samples))
        conflict = copy.deepcopy(low_samples)
        conflict[-1]['rows_processed'] = 25
        result = self.evaluate(s=state(300, status='preparing_resume', stage=None),
                               seed_progress=dict(conflict[-1], samples=conflict), previous=low)
        self.assertTrue(result['trigger'])
        self.assertFalse(result['armed'])
        self.assertIn('seed_counter_conflict', result['reason_codes'])
        self.assertIn('inspect_seed_counter_identity', result['diagnostics'])

    def test_stale_metadata_diagnoses_delay_immediately_without_zero_data_rate(self):
        prior = self.evaluate()
        result = self.evaluate(now=600, m=metrics(600, copied_rate=9e9),
                               n=nodes(535), previous=prior)
        self.assertFalse(result['armed'])
        self.assertTrue(result['trigger'])
        self.assertFalse(result['optimize'])
        self.assertTrue(result['diagnostic_trigger'])
        self.assertIn('inspect_node_metadata_latency', result['diagnostics'])
        self.assertEqual(result['rates']['effective_Bps'], 9e9)
        metadata = next(e for e in result['evidence'] if isinstance(e, dict)
                        and 'delayed_node_metadata' in e)
        self.assertEqual(metadata['delayed_node_metadata'][0]['age_seconds'], 65)
        self.assertEqual(metadata['rate_interpretation'], 'incomplete_metadata_not_zero_throughput')
        stopped = self.evaluate(now=600, m=metrics(600, copied_rate=9e9),
                                n=nodes(535), previous=prior, stop=True)
        self.assertFalse(stopped['diagnostic_trigger'])
        self.assertTrue(stopped['trigger'])


if __name__ == '__main__':
    unittest.main()
