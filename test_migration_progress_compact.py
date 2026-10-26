"""Compact progress stays honest about scope, absent counters and queue estimates."""
import contextlib
import copy
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock

import migration_progress as monitor
from migration_io import write_json


class SourceRateTests(unittest.TestCase):
    def view(self,stamp,leaves,dirs,checked,handover='first'):
        return dict(control='/fixture',foreground_handover=handover,recovery_warm=True,
                    stage='p3-r3',scan_complete=False,source_state_updated=stamp,
                    source_reader_progress=dict(leaf_names=leaves,directory_entries=dirs,error_entries=0,
                                                processes=64,reader_limit=32),target_filter={'checked':checked})

    def test_uses_producer_time_and_separates_source_entries_from_target_checks(self):
        rate=monitor.SourceRate();a=self.view(100,100,20,10);rate.add(a,100)
        b=self.view(160,700,80,310);rate.add(b,170)
        self.assertEqual(b['source_rates']['1m']['source_per_second'],11)
        self.assertEqual(b['source_rates']['1m']['checked_per_second'],5)
        self.assertEqual(b['source_rates']['dispatch_limit'],32)
        rate.add(b,171)
        self.assertEqual(len(rate.samples),2)
        self.assertEqual(b['source_rates']['1m']['source_per_second'],11)

    def test_handover_resets_and_stale_samples_do_not_report_a_fresh_speed(self):
        rate=monitor.SourceRate();rate.add(self.view(100,100,20,10),100)
        b=self.view(160,700,80,310);rate.add(b,221)
        self.assertIsNone(b['source_rates']['1m']['source_per_second'])
        c=self.view(230,10,1,400,handover='second');rate.add(c,230)
        self.assertEqual(len(rate.samples),1)
        self.assertIsNone(c['source_rates']['1m']['checked_per_second'])
        c['recovery_warm']=False;rate.add(c,240)
        self.assertEqual(c['source_rates'],{})

    def test_new_missing_is_window_delta_not_inherited_missing_total(self):
        rate = monitor.SourceRate()
        first = self.view(100, 100, 20, 1000)
        first['target_filter']['missing'] = 59554089
        rate.add(first, 100)
        second = self.view(160, 700, 80, 1500)
        second['target_filter']['missing'] = 59554089
        rate.add(second, 160)
        self.assertEqual(second['source_rates']['1m']['new_missing'], 0)
        third = self.view(220, 1300, 140, 2000)
        third['target_filter']['missing'] = 59554092
        rate.add(third, 220)
        self.assertEqual(third['source_rates']['1m']['new_missing'], 3)
        self.assertEqual(third['source_rates']['5m']['new_missing'], 3)
        rate.add(third, 281)
        self.assertIsNone(third['source_rates']['1m']['new_missing'])

    def test_regressed_missing_counter_resets_window_instead_of_reporting_negative_delta(self):
        rate = monitor.SourceRate()
        first = self.view(100, 100, 20, 1000)
        first['target_filter']['missing'] = 500
        rate.add(first, 100)
        second = self.view(160, 700, 80, 1500)
        second['target_filter']['missing'] = 1
        rate.add(second, 160)
        self.assertEqual(len(rate.samples), 1)
        self.assertIsNone(second['source_rates']['1m']['new_missing'])


class ScanProgressTests(unittest.TestCase):
    def view(self):
        return dict(recovery_warm=True, stage='p3-r3', status='scanning',
            scan_complete=False, source_state_updated=100,
            source_reader_progress=dict(leaf_names=1000, directory_entries=200,
                error_entries=3, skipped_assigned_leaves=600,
                skipped_seeded_leaves=10, pruned_completed_directories=90),
            target_filter=dict(checked=9000000, missing=1000000, inflight=24))

    def test_source_and_target_scopes_stay_separate_and_denominator_is_unknown(self):
        result = monitor.scan_progress(self.view())
        self.assertEqual(result['source_entries'], 1203)
        self.assertEqual(result['target_checked'], 9000000)
        self.assertEqual(result['historical_and_current_missing'], 1000000)
        self.assertEqual(result['pruned_completed_directories'], 90)
        for field in ('total_entries', 'percent', 'eta_seconds'):
            self.assertIsNone(result[field])

    def test_missing_source_counters_are_unknown_not_zero(self):
        view = self.view()
        del view['source_reader_progress']['error_entries']
        result = monitor.scan_progress(view)
        self.assertIsNone(result['source_entries'])
        self.assertIsNone(result['source_errors'])
        self.assertEqual(result['target_inflight'], 24)

    def test_first_frame_has_progress_without_rate_history_or_monitor_warmup(self):
        with mock.patch.object(monitor.time, 'time', return_value=110):
            text = '\n'.join(monitor.scan_progress_lines(self.view()))
        self.assertIn('P3 查漏进度：扫描中', text)
        self.assertIn('1,203 条目', text)
        self.assertIn('9,000,000 次', text)
        self.assertIn('检查点年龄 10s', text)
        self.assertIn('90 个', text)
        self.assertIn('采样中/未知', text)
        self.assertNotIn('1,000,000', text)
        self.assertIn('完成百分比/剩余时间未知', text)

    def test_rates_show_zero_new_missing_without_claiming_zero_checks(self):
        view = self.view()
        view['source_rates'] = dict(unit='条目', **{
            key: dict(seconds=60, source_per_second=3000,
                      checked_per_second=2800, new_missing=0) for key in ('1m', '5m')})
        with mock.patch.object(monitor.time, 'time', return_value=110):
            text = '\n'.join(monitor.scan_progress_lines(view))
        self.assertIn('3,000', text)
        self.assertIn('2,800', text)
        self.assertIn('新发现缺失', text)
        self.assertNotIn('采样中', text)
        self.assertNotIn('1,000,000', text)

    def test_stale_producer_is_not_hidden_by_a_fresh_aggregate_or_speed(self):
        view = self.view()
        view.update(updated=200, source_rates={
            '1m': dict(seconds=60, source_per_second=3000, checked_per_second=2800, new_missing=0)})
        with mock.patch.object(monitor.time, 'time', return_value=200):
            text = '\n'.join(monitor.scan_progress_lines(view))
        self.assertIn('检查点年龄 100s', text)
        self.assertIn('已过期', text)
        self.assertNotIn('3,000', text)

    def test_enumeration_complete_and_paused_are_not_reported_as_actively_scanning(self):
        view = self.view()
        view['scan_complete'] = True
        self.assertEqual(monitor.scan_progress(view)['status'], 'enumeration_complete')
        view.update(scan_complete=False, status='producer_paused')
        self.assertEqual(monitor.scan_progress(view)['status'], 'paused')

    def test_other_stages_do_not_display_old_p3_scan_counters(self):
        view = self.view()
        view['stage'] = 'p1-r1'
        self.assertEqual(monitor.scan_progress_lines(view), [])
        view.update(stage='p3-r3', recovery_warm=False)
        self.assertEqual(monitor.scan_progress(view), {})


class ProbeBaselineProgressTests(unittest.TestCase):
    """Small local JSON fixtures only; no queues, manifests or remote reads."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='progress-probe-baseline-')
        self.addCleanup(self.temporary.cleanup)
        self.control = (Path(self.temporary.name) / 'control').resolve()
        self.control.mkdir()
        self.operation = 'a' * 32
        self.bundle = self.control / ('pipelined-producer-' + self.operation)
        self.config = {'run_id': 'fixture-run', 'source': '/fixture/source',
                       'destination': '/fixture/destination'}
        self.probe = {'counts': {'remote_existing': 17, 'requests': 30, 'remote_errors': 3},
                      'endpoints': {'fixture:1234': {'requests': 30, 'remote_errors': 3}}}
        self.state = {'status': 'scanning', 'stage': 'p3-r3', 'phase': 3, 'round': 3,
                      'run_id': 'fixture-run', 'recovery_warm': True,
                      'distributed_target_probe': copy.deepcopy(self.probe)}
        write_json(self.control / 'config.json', self.config)

    def bundle_records(self):
        self.bundle.mkdir(exist_ok=True)
        self.state.update(task_publication={'scope': 'warm-recovery-only'},
                          foreground_handover=str(self.bundle / 'handover.json'))
        self.handover = {'control': str(self.control), 'stage': 'p3-r3',
                         'operation': self.operation}
        self.checkpoint = {'version': 1, 'status': 'frozen_checkpoint',
                           'control': str(self.control), 'bundle': str(self.bundle),
                           'operation': self.operation, 'handover': copy.deepcopy(self.handover),
                           'frozen_state': {'stage': 'p3-r3', 'recovery_warm': True,
                               'run_id': 'fixture-run', 'distributed_target_probe': {'counts': {
                                   'remote_existing': 5400000, 'requests': 99999999,
                                   'remote_errors': 8888888}}}}
        self.write_bundle()

    def write_bundle(self):
        write_json(self.bundle / 'handover.json', self.handover)
        write_json(self.bundle / 'frozen-checkpoint.json', self.checkpoint)

    def snapshot(self):
        write_json(self.control / 'state.json', self.state)
        with mock.patch.object(monitor, 'load_nodes', return_value=[]):
            return monitor.snapshot(self.control)

    def test_legacy_producer_does_not_read_or_change_historical_probe_counts(self):
        self.state['foreground_handover'] = str(self.control / 'old-handover.json')
        with mock.patch.object(monitor, 'read_json', wraps=monitor.read_json) as read:
            view = self.snapshot()
        self.assertIsNone(view['distributed_target_probe_baseline'])
        self.assertEqual(view['copy_work_progress']['existing_checks'], 17)
        self.assertFalse(view['copy_work_progress']['existing_checks_lower_bound'])
        self.assertFalse(view['warnings'])
        self.assertNotIn('old-handover.json', [Path(call.args[0]).name for call in read.call_args_list])

    def test_cumulative_scan_handover_does_not_add_the_old_probe_total_again(self):
        self.bundle_records()
        self.state['target_probe_counts_scope'] = 'cumulative-across-producer-handover'
        self.state['distributed_target_probe']['counts']['remote_existing'] = 5400017
        view = self.snapshot()
        self.assertIsNone(view['distributed_target_probe_baseline'])
        self.assertEqual(view['copy_work_progress']['existing_checks'],5400017)
        self.assertFalse(view['warnings'])

    def test_inventory_replay_is_not_labelled_as_active_copy_supply(self):
        self.state['recovery_replay'] = {'phase': 'inventory_replay', 'seeded_records': 12}
        self.state['task_publication'] = {'published_since_handover': 0}
        view = self.snapshot()
        self.assertEqual(view['recovery_replay']['seeded_records'], 12)
        self.assertEqual(view['task_publication']['published_since_handover'], 0)
        self.assertIn('恢复清单（尚未恢复新任务供给）', monitor.render(view))
        self.state['recovery_replay']['phase'] = 'fresh_scan'
        self.assertNotIn('尚未恢复新任务供给', monitor.render(self.snapshot()))

    def test_producer_failure_is_explicit_and_historical_error_not_active(self):
        self.state.update(status='producer_paused', producer_error='Traceback\nOSError: Too many open files',
                          producer_nofile=[1024, 1048576])
        view = self.snapshot()
        self.assertEqual(view['producer_nofile'], [1024, 1048576])
        text = monitor.render(view)
        self.assertIn('生产端故障，已停止供给', text)
        self.assertIn('生产端错误：OSError: Too many open files', text)
        self.state['status'] = 'scanning'
        self.assertNotIn('生产端错误：', monitor.render(self.snapshot()))

    def test_restarted_producer_preserves_historical_existing_observations_only(self):
        self.bundle_records()
        view = self.snapshot()
        self.assertEqual(view['copy_work_progress']['existing_checks'], 5400017)
        self.assertFalse(view['copy_work_progress']['existing_checks_lower_bound'])
        baseline = view['distributed_target_probe_baseline']
        self.assertTrue(baseline['complete'])
        self.assertEqual(baseline['counts'], {'remote_existing': 5400000})
        self.assertEqual(view['distributed_target_probe'], self.probe)
        self.assertFalse(view['warnings'])
        rendered = monitor.render(view)
        self.assertIn('5,400,017 次命中', rendered)
        self.assertNotIn('5,400,017 文件', rendered)
        self.assertNotIn('5,400,017 项', rendered)

    def test_missing_checkpoint_is_a_visible_known_lower_bound_not_zero_history(self):
        self.bundle.mkdir()
        self.state.update(task_publication={'scope': 'warm-recovery-only'},
                          foreground_handover=str(self.bundle / 'handover.json'))
        view = self.snapshot()
        self.assertFalse(view['distributed_target_probe_baseline']['complete'])
        self.assertEqual(view['copy_work_progress']['existing_checks'], 17)
        self.assertTrue(view['copy_work_progress']['existing_checks_lower_bound'])
        self.assertTrue(any('历史目标存在检查基线不可用' in warning for warning in view['warnings']))
        self.assertIn('至少 17 次命中（已知下限）', monitor.render(view))

    def test_current_counter_missing_keeps_historical_observations_as_lower_bound(self):
        self.bundle_records()
        self.state['distributed_target_probe']['counts'].pop('remote_existing')
        view = self.snapshot()
        self.assertEqual(view['copy_work_progress']['existing_checks'], 5400000)
        self.assertTrue(view['copy_work_progress']['existing_checks_lower_bound'])
        self.assertIn('至少 5,400,000 次命中', monitor.render(view))

    def test_explicit_zero_history_and_current_counts_remain_known_zero(self):
        self.bundle_records()
        self.checkpoint['frozen_state']['distributed_target_probe']['counts']['remote_existing'] = 0
        self.write_bundle()
        self.state['distributed_target_probe']['counts']['remote_existing'] = 0
        view = self.snapshot()
        self.assertEqual(view['copy_work_progress']['existing_checks'], 0)
        self.assertFalse(view['copy_work_progress']['existing_checks_lower_bound'])

    def test_handover_outside_control_is_rejected_without_reading_external_files(self):
        outside = Path(self.temporary.name) / 'outside' / self.bundle.name
        self.state.update(task_publication={'scope': 'warm-recovery-only'},
                          foreground_handover=str(outside / 'handover.json'))
        with mock.patch.object(monitor, 'read_json', wraps=monitor.read_json) as read:
            view = self.snapshot()
        self.assertFalse(view['distributed_target_probe_baseline']['complete'])
        self.assertEqual(view['copy_work_progress']['existing_checks'], 17)
        self.assertTrue(view['copy_work_progress']['existing_checks_lower_bound'])
        self.assertTrue(all(Path(call.args[0]).is_relative_to(self.control)
                            for call in read.call_args_list))

    def test_checkpoint_symlink_outside_control_is_rejected_before_read(self):
        self.bundle.mkdir()
        outside = Path(self.temporary.name) / 'untrusted.json'
        write_json(outside, {'untrusted': True})
        (self.bundle / 'frozen-checkpoint.json').symlink_to(outside)
        self.state.update(task_publication={'scope': 'warm-recovery-only'},
                          foreground_handover=str(self.bundle / 'handover.json'))
        with mock.patch.object(monitor, 'read_json', wraps=monitor.read_json) as read:
            view = self.snapshot()
        self.assertFalse(view['distributed_target_probe_baseline']['complete'])
        self.assertNotIn('frozen-checkpoint.json', [Path(call.args[0]).name for call in read.call_args_list])

    def test_mismatched_handover_operation_bundle_or_run_cannot_supply_baseline(self):
        for field in ('operation', 'bundle', 'control', 'handover', 'run_id'):
            with self.subTest(field=field):
                self.bundle_records()
                if field == 'run_id':
                    self.checkpoint['frozen_state']['run_id'] = 'another-run'
                elif field == 'handover':
                    self.checkpoint['handover']['operation'] = 'b' * 32
                elif field == 'operation':
                    self.checkpoint['operation'] = 'b' * 32
                else:
                    self.checkpoint[field] = str(self.control / 'other')
                self.write_bundle()
                view = self.snapshot()
                self.assertFalse(view['distributed_target_probe_baseline']['complete'])
                self.assertEqual(view['copy_work_progress']['existing_checks'], 17)
                self.assertTrue(view['warnings'])

    def test_invalid_historical_count_does_not_masquerade_as_zero_or_unique_files(self):
        for count in (None, -1, True, 12.5):
            with self.subTest(count=count):
                self.bundle_records()
                self.checkpoint['frozen_state']['distributed_target_probe']['counts']['remote_existing'] = count
                self.write_bundle()
                view = self.snapshot()
                self.assertFalse(view['distributed_target_probe_baseline']['complete'])
                self.assertEqual(view['copy_work_progress']['existing_checks'], 17)
                self.assertTrue(view['copy_work_progress']['existing_checks_lower_bound'])


class CompactProgressTests(unittest.TestCase):
    def view(self):
        return {
            'control': '/fixture/control',
            'status': 'copying', 'phase': 3, 'round': 3, 'stage': 'p3-r3',
            'recovery_warm': True,
            'source': '/fixture/source', 'destination': '/fixture/destination',
            'sampled_at': '2026-10-22T00:00:00+00:00',
            'updated': '2026-10-22T00:00:00+00:00',
            'overall': {
                'discovered_files': 9000000, 'discovered_bytes': 90000000000,
                'verified_files': 8000000, 'verified_bytes': 80000000000,
                'inventory_complete': False, 'percent': None,
            },
            'recovery_warm_progress': {
                'discovered_files': 1000, 'discovered_bytes': 10000,
                'verified_files': 650, 'verified_bytes': 6500,
            },
            'current_stage': {'verified_files': 700, 'verified_bytes': 7000},
            'current_scan': {'files': 9000000, 'bytes': 90000000000},
            'distributed_target_probe': {'counts': {
                'remote_existing': 12000, 'remote_missing': 600,
                'requests': 22000, 'remote_errors': 9000,
            }},
            'target_filter': {'checked': 20000, 'missing': 1000, 'inflight': 30},
            'new_copied_bytes': 6000,
            'nodes': [
                {'host': 'fixture-master-0', 'threads': 128, 'active_threads': 4,
                 'bytes': 4000, 'counts': {'copied': 40, 'skipped': 500},
                 'verified_files': 540, 'verified_bytes': 5400,
                 'heartbeat_age_seconds': 1},
                {'host': 'fixture-worker-0', 'threads': 128, 'active_threads': 5,
                 'bytes': 2000, 'counts': {'copied': 20, 'skipped': 100},
                 'verified_files': 120, 'verified_bytes': 2400,
                 'heartbeat_age_seconds': 2},
            ],
            'resume_index': {'verified_files': 999999, 'verified_bytes': 999999999999},
            'cumulative_evidence': {
                'historical_verified_files': 999999,
                'historical_verified_symlinks': 100,
                'historical_verified_entries': 1000099,
                'historical_verified_bytes': 999999999999,
                'current_verified_entries': 700, 'current_verified_bytes': 7000,
                'unique_entries_lower_bound': 1000099,
                'unique_entries_upper_bound': 1000799,
                'unique_bytes_lower_bound': 999999999999,
                'unique_bytes_upper_bound': 1000000006999,
            },
            'phases': {}, 'warnings': [],
        }

    def test_copy_work_uses_warm_queue_scope_and_copied_status_only(self):
        view = self.view()
        result = monitor.copy_work_progress(view)
        self.assertEqual(result['copied_files'], 60)
        self.assertEqual(result['copied_bytes'], 6000)
        self.assertEqual(result['existing_checks'], 12000)
        self.assertIsNone(result['existing_bytes'])
        self.assertEqual(result['pending_files'], 300)
        self.assertEqual(result['pending_bytes'], 3000)
        self.assertIs(result['pending_estimated'], True)
        self.assertIs(result['snapshot_skew'], False)

    def test_historical_evidence_and_inventory_do_not_inflate_work_categories(self):
        view = self.view()
        expected = monitor.copy_work_progress(view)
        changed = copy.deepcopy(view)
        changed['resume_index']['verified_bytes'] *= 1000
        changed['cumulative_evidence']['unique_bytes_upper_bound'] *= 1000
        changed['overall']['verified_files'] = 123456789
        changed['overall']['verified_bytes'] = 987654321000
        changed['overall']['discovered_files'] = 123456790
        changed['overall']['discovered_bytes'] = 987654321001
        changed['current_scan'] = {'files': 123456790, 'bytes': 987654321001}
        self.assertEqual(monitor.copy_work_progress(changed), expected)

    def test_existing_checks_never_use_checked_minus_missing_or_request_count(self):
        view = self.view()
        del view['distributed_target_probe']['counts']['remote_existing']
        result = monitor.copy_work_progress(view)
        self.assertIsNone(result['existing_checks'])
        self.assertIsNone(result['existing_bytes'])
        view['distributed_target_probe']['counts']['remote_existing'] = 0
        self.assertEqual(monitor.copy_work_progress(view)['existing_checks'], 0)

    def test_missing_queue_fields_stay_unknown_independently(self):
        for container, field, output, preserved in (
            ('recovery_warm_progress', 'discovered_files', 'pending_files', 'pending_bytes'),
            ('recovery_warm_progress', 'discovered_bytes', 'pending_bytes', 'pending_files'),
            ('current_stage', 'verified_files', 'pending_files', 'pending_bytes'),
            ('current_stage', 'verified_bytes', 'pending_bytes', 'pending_files'),
        ):
            with self.subTest(container=container, field=field):
                view = self.view()
                del view[container][field]
                result = monitor.copy_work_progress(view)
                self.assertIsNone(result[output])
                self.assertIsNotNone(result[preserved])
                self.assertIs(result['snapshot_skew'], False)

    def test_negative_queue_difference_is_unknown_instead_of_zero(self):
        for field, output, preserved in (
            ('verified_files', 'pending_files', 'pending_bytes'),
            ('verified_bytes', 'pending_bytes', 'pending_files'),
        ):
            with self.subTest(field=field):
                view = self.view()
                view['current_stage'][field] = 100000
                result = monitor.copy_work_progress(view)
                self.assertIsNone(result[output])
                self.assertIsNotNone(result[preserved])
                self.assertIs(result['snapshot_skew'], True)

    def test_completed_known_queue_keeps_zero_as_a_known_value(self):
        view = self.view()
        view['current_stage'] = {'verified_files': 1000, 'verified_bytes': 10000}
        result = monitor.copy_work_progress(view)
        self.assertEqual(result['pending_files'], 0)
        self.assertEqual(result['pending_bytes'], 0)
        self.assertIs(result['snapshot_skew'], False)

    def test_default_layout_contains_only_overall_and_node_sections(self):
        text = monitor.render(self.view())
        self.assertEqual(re.findall(r'^【(.+)】$', text, flags=re.MULTILINE),
                         ['整体进度', '节点明细'])
        self.assertIn('本次新复制', text)
        self.assertIn('已探明无需新复制', text)
        self.assertIn('已探明待复制/确认', text)
        self.assertIn('12,000', text)
        self.assertIn('未知', text)
        self.assertIn('master-0', text)
        self.assertIn('worker-0', text)
        self.assertIn('活跃/总线程', text)
        self.assertNotIn('历史 + 本次累计工作量', text)
        self.assertNotIn('累计版本证据范围', text)
        self.assertNotIn('各优先级进度', text)
        self.assertNotIn('当前轮次与配置', text)
        self.assertNotIn('近期平均速度', text)

    def speed_view(self):
        view = self.view()
        view['metrics_sample_age_seconds'] = 2
        view['rates'] = {
            '1m': {'seconds': 60, 'copied_bytes_per_second': 1024,
                   'streamed_bytes_per_second': 2048},
            '5m': {'seconds': 300, 'copied_bytes_per_second': 3072,
                   'streamed_bytes_per_second': 4096},
        }
        return view

    def test_copy_speed_shows_each_fresh_window_and_names_both_counters(self):
        view = self.speed_view()
        view['metrics_sample_age_seconds'] = 30
        view['rates']['1m']['seconds'] = 30
        lines = monitor.copy_speed_lines(view)
        text = '\n'.join(lines)
        self.assertIn('新复制完成', text)
        self.assertIn('传输写入', text)
        minute = next(line for line in lines if '近1分钟' in line)
        five_minutes = next(line for line in lines if '近5分钟' in line)
        self.assertIn('1.0 KiB/s', minute)
        self.assertIn('2.0 KiB/s', minute)
        self.assertIn('3.0 KiB/s', five_minutes)
        self.assertIn('4.0 KiB/s', five_minutes)
        self.assertIn('逻辑', text)

    def test_copy_speed_zero_is_a_real_measurement_not_unknown(self):
        view = self.speed_view()
        for window in view['rates'].values():
            window.update(copied_bytes_per_second=0, streamed_bytes_per_second=0)
        for line in monitor.copy_speed_lines(view):
            if '近1分钟' in line or '近5分钟' in line:
                self.assertEqual(line.count('0.0 B/s'), 2)
                self.assertNotIn('未知', line)
                self.assertNotIn('采样中', line)
                self.assertNotIn('已过期', line)

    def test_copy_speed_short_windows_are_still_sampling(self):
        view = self.speed_view()
        view['rates']['1m']['seconds'] = 29
        view['rates']['5m']['seconds'] = 0
        text = '\n'.join(monitor.copy_speed_lines(view))
        self.assertIn('近1分钟', text)
        self.assertIn('近5分钟', text)
        self.assertIn('采样中', text)
        for old_value in ('1.0 KiB/s', '2.0 KiB/s', '3.0 KiB/s', '4.0 KiB/s'):
            self.assertNotIn(old_value, text)

    def test_copy_speed_stale_samples_do_not_present_old_rates_as_current(self):
        view = self.speed_view()
        view['metrics_sample_age_seconds'] = 31
        text = '\n'.join(monitor.copy_speed_lines(view))
        self.assertIn('已过期', text)
        self.assertIn('近1分钟', text)
        self.assertIn('近5分钟', text)
        for old_value in ('1.0 KiB/s', '2.0 KiB/s', '3.0 KiB/s', '4.0 KiB/s'):
            self.assertNotIn(old_value, text)

    def test_copy_speed_without_rates_does_not_invent_zero_throughput(self):
        view = self.speed_view()
        del view['rates']
        text = '\n'.join(monitor.copy_speed_lines(view))
        self.assertIn('近1分钟', text)
        self.assertIn('近5分钟', text)
        self.assertTrue('未知' in text or '采样中' in text)
        self.assertNotIn('0.0 B/s', text)

    def test_copy_speed_missing_counter_is_unknown_but_other_counter_remains(self):
        view = self.speed_view()
        del view['rates']['1m']['streamed_bytes_per_second']
        minute = next(line for line in monitor.copy_speed_lines(view) if '近1分钟' in line)
        self.assertIn('1.0 KiB/s', minute)
        self.assertIn('未知', minute)
        self.assertNotIn('0.0 B/s', minute)

    def test_copy_speed_without_sample_age_cannot_claim_fresh_rates(self):
        view = self.speed_view()
        del view['metrics_sample_age_seconds']
        text = '\n'.join(monitor.copy_speed_lines(view))
        self.assertTrue('未知' in text or '采样中' in text or '新鲜度' in text)
        for old_value in ('1.0 KiB/s', '2.0 KiB/s', '3.0 KiB/s', '4.0 KiB/s'):
            self.assertNotIn(old_value, text)

    def test_default_layout_embeds_both_copy_speeds_in_overall_without_a_new_section(self):
        text = monitor.render(self.speed_view())
        self.assertEqual(re.findall(r'^【(.+)】$', text, flags=re.MULTILINE),
                         ['整体进度', '节点明细'])
        overall, _nodes = text.split('【节点明细】', 1)
        self.assertIn('近1分钟', overall)
        self.assertIn('近5分钟', overall)
        self.assertIn('新复制完成', overall)
        self.assertIn('传输写入', overall)
        self.assertIn('1.0 KiB/s', overall)
        self.assertIn('4.0 KiB/s', overall)
        self.assertNotIn('【近期平均速度】', text)

    def test_details_explicitly_retains_old_diagnostic_layout(self):
        text = monitor.render(self.view(), details=True)
        self.assertIn('【整体进度与完成预期】', text)
        self.assertIn('【各优先级进度】', text)
        self.assertIn('【当前轮次与配置】', text)
        self.assertIn('【近期平均速度】', text)
        self.assertIn('【节点明细】', text)

    def test_cli_routes_default_and_details_without_starting_other_processes(self):
        for argv, details in (([], False), (['--details'], True)):
            with self.subTest(argv=argv):
                view = self.view()
                output = io.StringIO()
                with mock.patch.object(monitor, 'snapshot', return_value=view), \
                        mock.patch.object(monitor, 'Rate'), \
                        mock.patch.object(monitor, 'render', return_value='fixture output') as render, \
                        contextlib.redirect_stdout(output):
                    self.assertEqual(monitor.main(argv), 0)
                render.assert_called_once_with(view, details=details)
                self.assertEqual(output.getvalue(), 'fixture output\n')

    def test_cli_json_keeps_original_fields_and_does_not_render_text(self):
        view = self.view()
        output = io.StringIO()
        with mock.patch.object(monitor, 'snapshot', return_value=view), \
                mock.patch.object(monitor, 'Rate'), \
                mock.patch.object(monitor, 'render') as render, \
                contextlib.redirect_stdout(output):
            self.assertEqual(monitor.main(['--json']), 0)
        render.assert_not_called()
        self.assertEqual(json.loads(output.getvalue()), view)


if __name__ == '__main__':
    unittest.main()
