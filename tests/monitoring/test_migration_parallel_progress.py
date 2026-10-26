import copy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from migration_io import write_json
import migration_parallel_progress as parallel
from migration_progress import Rate


class ParallelRateTests(unittest.TestCase):
    def tracker(self, values):
        rate = Rate()
        rate.samples.extend(dict(time=t, copied_bytes=v, streamed_bytes=2*v) for t, v in values)
        return rate

    def test_rates_use_the_same_time_interval_not_sums_of_different_averages(self):
        a = self.tracker([(0, 0), (50, 50), (100, 250)])
        b = self.tracker([(50, 0), (100, 500)])
        rate = parallel.aligned_rates([a, b], 100)['5m']
        self.assertEqual(rate['seconds'], 50)
        self.assertEqual(rate['copied_bytes_per_second'], 14)
        self.assertEqual(rate['streamed_bytes_per_second'], 28)

    def test_counter_reset_is_unknown_not_negative_or_zero(self):
        a = self.tracker([(0, 100), (100, 50)])
        rate = parallel.aligned_rates([a], 100)['1m']
        self.assertIsNone(rate['copied_bytes_per_second'])


class ParallelSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='parallel-progress-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.a, self.b = self.root / 'old', self.root / 'p4'
        self.a.mkdir(); self.b.mkdir()
        self.cfg = dict(run_id='fixture', parallel_with_control=str(self.a))
        write_json(self.b / 'config.json', self.cfg)
        write_json(self.b / 'isolation.json', dict(control=str(self.b), run_id='fixture',
                   predecessor_control=str(self.a), verified=True))
        self.first = dict(control=str(self.a), status='scanning', recovery_warm=True,
                          overall=dict(inventory_complete=False), phases={'3': {}},
                          updated='2026-10-24T00:00:00+00:00',
                          extension_plan={'parallel_execution': {'mode': 'independent_concurrent_p4', 'control': str(self.b)}},
                          distributed_target_probe={'counts': {'remote_existing': 17}},
                          recovery_warm_progress={'discovered_files': 10, 'discovered_bytes': 100},
                          current_stage={'verified_files': 10, 'verified_bytes': 100},
                          recovery_baseline={'work': {'copied_files': 50, 'copied_bytes': 500, 'existing_checks': 0}},
                          new_copied_bytes=100, streamed_bytes=200, warnings=[],
                          nodes=[dict(host='fixture-master-0', counts={'copied': 10}, bytes=100,
                                      streamed_bytes=200, threads=128, active_threads=0, verified_files=10, verified_bytes=100,
                                      heartbeat_age_seconds=1, components=[])])
        self.second = dict(control=str(self.b), status='scanning', recovery_warm=False,
                           overall=dict(inventory_complete=False), phases={'4': {}},
                           updated='2026-10-24T00:00:00+00:00',
                           current_scan={'files': 8, 'bytes': 80},
                           current_stage={'verified_files': 5, 'verified_bytes': 50},
                           new_copied_bytes=20, streamed_bytes=40, warnings=[],
                           nodes=[dict(host='fixture-master-0', counts={'copied': 2, 'skipped': 3}, bytes=20,
                                       streamed_bytes=40, threads=128, active_threads=1, verified_files=5, verified_bytes=50,
                                       heartbeat_age_seconds=2, components=[])])
        self.mon = parallel.CombinedMonitor()

    def snapshot(self):
        with mock.patch.object(parallel.base, 'snapshot', side_effect=[copy.deepcopy(self.first), copy.deepcopy(self.second)]), \
                mock.patch.object(parallel, 'ScopeGuard'):
            return self.mon.snapshot(self.a)

    def test_disjoint_scopes_sum_once_and_keep_historical_baseline_once(self):
        view = self.snapshot()
        work = view['parallel_work_progress']
        self.assertEqual(work['copied_files'], 12)
        self.assertEqual(work['cumulative_copied_files'], 62)
        self.assertEqual(work['cumulative_copied_bytes'], 620)
        self.assertEqual(work['existing_checks'], 20)
        self.assertEqual(work['pending_files'], 3)
        self.assertEqual(len(view['nodes']), 1)
        self.assertEqual(view['nodes'][0]['threads'], 256)
        self.assertEqual(view['nodes'][0]['active_threads'], 1)
        self.assertFalse(view['overall']['inventory_complete'])

    def test_wrong_predecessor_does_not_merge_p4(self):
        write_json(self.b / 'isolation.json', dict(control=str(self.b), run_id='fixture',
                   predecessor_control='/different', verified=True))
        view = self.snapshot()
        self.assertNotIn('parallel_scopes', view)
        self.assertTrue(any('P4' in item for item in view['warnings']))

    def test_compact_render_keeps_one_node_table_and_restores_original_renderer(self):
        view = self.snapshot()
        original = parallel.base.copy_work_progress
        text = parallel.render(view)
        self.assertIn('1–3 查漏与 P4 并行复制', text)
        self.assertEqual(text.count('【节点明细】'), 1)
        self.assertIn('1/256', text)
        self.assertIs(parallel.base.copy_work_progress, original)

    def test_combined_monitor_keeps_p3_scan_progress_outside_the_p4_copy_counters(self):
        self.first.update(stage='p3-r3', scan_complete=False,
            source_state_updated='2026-10-24T00:00:00+00:00',
            source_reader_progress=dict(leaf_names=1000, directory_entries=20,
                error_entries=0, pruned_completed_directories=10),
            target_filter=dict(checked=5000, missing=600, inflight=7))
        self.second.update(source_reader_progress=dict(leaf_names=99999999),
                           target_filter=dict(checked=88888888))
        view = self.snapshot()
        text = parallel.render(view)
        self.assertIn('P3 查漏进度：扫描中', text)
        self.assertIn('1,020 条目', text)
        self.assertIn('5,000 次', text)
        self.assertNotIn('99,999,999', text)
        self.assertNotIn('88,888,888', text)
        self.assertEqual(text.count('【节点明细】'), 1)

    def test_delayed_telemetry_is_not_reported_as_a_confirmed_process_exit(self):
        self.second['nodes'][0]['heartbeat_age_seconds'] = 240
        view = self.snapshot()
        self.assertTrue(any('统计延迟' in item for item in view['warnings']))
        # Keep the renderer fixture's intentional 1/12-node and old timestamp
        # warnings from hiding this specific warning behind the compact limit.
        view['warnings'] = []
        text = parallel.render(view)
        self.assertIn('心跳延迟，需另行核对实际进程', text)
        self.assertNotIn('：进程已结束或心跳延迟', text)


if __name__ == '__main__':
    unittest.main()
