"""Mock-only monitor checks: warm copies are visible but never formal success."""
import copy
import json
from pathlib import Path
import unittest
from unittest import mock

import migration_progress as monitor


class OverlapProgressTests(unittest.TestCase):
    def fixture(self, warm=True, phase=3, stage='p3-r3'):
        stamp = '2026-10-21T14:23:00+00:00'
        counts = {'discovered_files': 100, 'discovered_bytes': 1000,
                  'verified_files': 30, 'verified_bytes': 300, 'inventory_complete': False}
        files = {
            'config.json': {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                            'threads': 64, 'parallel_scan_threads': 64, 'scan_entries_per_second': 0},
            'state.json': {'status': 'copying', 'stage': stage, 'phase': phase, 'round': 3,
                           'recovery_warm': warm, 'resume_index_building': True,
                           'recovery_label': '先复制未完成内容，同时恢复历史索引',
                           'resume_index': {'verified_files': 80, 'verified_symlinks': 5,
                                            'verified_bytes': 800},
                           'phases': {}, 'updated': stamp},
            'progress.json': {'updated': stamp, 'phases': {'1': dict(counts, verified_files=0, verified_bytes=0),
                              '2': dict(counts, verified_files=0, verified_bytes=0), '3': counts},
                              'totals': {'discovered_files': 300, 'discovered_bytes': 3000,
                                         'verified_files': 30, 'verified_bytes': 300}},
            'seed-progress.json': {'status': 'reading_receipts', 'processed_receipts': 12345,
                                   'receipts': 12300, 'results': 54321, 'issues': 2,
                                   'receipt_paths_discovered': 20000, 'updated': stamp},
            'extension-plan.json': {'priority': 4, 'status': 'queued', 'source': '/share/project',
                                    'destination': '/share-mixed/project', 'exclude': ['eai_pwm']},
        }
        node = {'host': 'fixture-worker-0', 'threads': 128, 'active': {'0': {}}, 'bytes': 400,
                'verified_files': 30, 'verified_bytes': 300, 'updated': stamp,
                'stages': {stage: {'verified_files': 30, 'verified_bytes': 300}}}
        return files, node

    def view(self, files, node):
        with mock.patch.object(monitor, 'read_json',
                               lambda path, *a, **kw: copy.deepcopy(files.get(Path(path).name, {}))), \
                mock.patch.object(monitor, 'load_nodes', lambda *a, **kw: [copy.deepcopy(node)]):
            return monitor.snapshot('/fixture/control')

    def test_warm_copying_preserves_flags_and_actual_seed_progress_without_formal_credit(self):
        files, node = self.fixture()
        view = self.view(files, node)
        self.assertTrue(view['recovery_warm'])
        self.assertTrue(view['resume_index_building'])
        self.assertEqual(view['current_stage']['verified_files'], 30)
        self.assertEqual(view['recovery_warm_progress']['verified_files'], 30)
        self.assertEqual(view['overall']['verified_files'], 0)
        self.assertEqual(view['phases']['3']['verified_bytes'], 0)
        self.assertEqual(view['completed_rounds'], {})
        self.assertEqual(view['phases']['4']['status'], 'queued')
        self.assertEqual(view['cumulative_evidence']['unique_entries_lower_bound'], 85)
        self.assertEqual(view['cumulative_evidence']['unique_entries_upper_bound'], 115)
        self.assertEqual(view['cumulative_evidence']['unique_bytes_lower_bound'], 800)
        self.assertEqual(view['cumulative_evidence']['unique_bytes_upper_bound'], 1100)
        text = monitor.render(view)
        self.assertTrue(text.startswith('=' * 96 + '\n复制进度开始'))
        self.assertIn('恢复续传队列（非正式第3轮）', text)
        self.assertIn('历史 + 本次累计工作量（含重叠）：115 项 / 1.1 KiB', text)
        self.assertIn('正式轮次计数：0 文件 / 0.0 B', text)
        self.assertIn('恢复队列已确认：30 文件', text)
        self.assertIn('历史唯一成功证据：80 文件 + 5 符号链接 / 800.0 B', text)
        self.assertIn('累计版本证据范围：85 — 115 项 / 800.0 B — 1.1 KiB', text)
        self.assertIn('以上范围仅描述历史与本次版本证据，不代表目标当前容量或本轮完成量。', text)
        self.assertNotIn('去重后当前唯一量', text)
        self.assertIn('实际处理：12,345 份回执', text)
        self.assertNotIn('|  第3轮', text)
        self.assertEqual(json.loads(json.dumps(view))['overall']['verified_files'], 0)

    def test_distributed_target_details_and_observed_low_activity_are_preserved(self):
        files, node = self.fixture()
        files['config.json'].update(target_probe_slots_per_node=64,
                                    parallel_target_threads=768,
                                    target_filter_inflight=1536)
        files['state.json'].update(
            distributed_target_nodes=12,
            distributed_target_probe={'endpoints': {
                'fixture-worker-0:37652': {'requests': 200, 'remote_errors': 120}}},
            target_filter={'checked': 1000, 'missing': 100, 'inflight': 30})
        view = self.view(files, node)
        self.assertEqual(view['distributed_target_nodes'], 12)
        self.assertEqual(view['distributed_target_slots_per_node'], 64)
        self.assertEqual(view['distributed_target_probe']['endpoints']
                         ['fixture-worker-0:37652']['remote_errors'], 120)
        text = monitor.render(view)
        self.assertIn('分布在 12 个节点；每节点最多 64 路只读元数据检查', text)
        self.assertIn('目标检查端点 fixture-worker-0:37652 错误率 60.0%', text)
        self.assertIn('当前复制线程活跃 1/128', text)
        self.assertIn('仅凭活跃率无法确定瓶颈', text)
        self.assertNotIn('瓶颈在恢复队列生产/目标检查', text)
        self.assertNotIn('继续增加复制线程不会提高吞吐', text)

    def test_cumulative_bounds_handle_larger_current_side_without_formal_credit(self):
        files, node = self.fixture()
        files['state.json']['resume_index'] = {
            'verified_files': 5, 'verified_symlinks': 2, 'verified_bytes': 50}
        view = self.view(files, node)
        evidence = view['cumulative_evidence']
        self.assertIs(evidence['exact'], False)
        self.assertEqual(evidence['unique_entries_lower_bound'], 30)
        self.assertEqual(evidence['unique_entries_upper_bound'], 37)
        self.assertEqual(evidence['unique_bytes_lower_bound'], 300)
        self.assertEqual(evidence['unique_bytes_upper_bound'], 350)
        self.assertEqual(view['overall']['verified_files'], 0)
        self.assertIsNone(view['overall']['percent'])
        self.assertEqual(view['phases']['4']['status'], 'queued')

    def test_completed_warm_counts_do_not_leak_into_p1_or_current_p3_formal_progress(self):
        files, node = self.fixture(warm=False, phase=1, stage='p1-r1')
        files['state.json']['warm_summary'] = {'verified_files': 30}
        node['stages'] = {'p1-r1': {'verified_files': 2, 'verified_bytes': 20}}
        view = self.view(files, node)
        self.assertEqual(view['phases']['3']['verified_files'], 0)
        self.assertEqual(view['overall']['verified_files'], 2)
        files['state.json'].update(phase=3, stage='p3-r1')
        node['stages'] = {'p3-r1': {'verified_files': 4, 'verified_bytes': 40}}
        view = self.view(files, node)
        self.assertEqual(view['phases']['3']['verified_files'], 4)
        self.assertEqual(view['overall']['verified_files'], 4)

    def test_formal_stage_layout_remains_and_index_error_is_visible(self):
        files, node = self.fixture(warm=False, phase=3, stage='p3-r1')
        files['state.json'].update(round=1, resume_index_building=False,
                                  resume_index_error='fixture import failed')
        files['state.json']['phases'] = {'3': {'rounds': [{'summary': {'verified_files': 30}}]}}
        files['state.json']['warm_summary'] = {'verified_files': 30}
        view = self.view(files, node)
        self.assertEqual(view['overall']['verified_files'], 30)
        text = monitor.render(view)
        self.assertIn('|  第1轮', text)
        self.assertNotIn('恢复续传队列（非正式第3轮）', text)
        self.assertNotIn('实际处理：', text)
        self.assertIn('后台恢复索引异常：fixture import failed', text)


if __name__ == '__main__':
    unittest.main()
