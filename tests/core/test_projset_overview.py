import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).resolve().parents[2]/'ops/copy/projset_overview.py'
if not MODULE.is_file():
    MODULE = Path(__file__).with_name('projset_overview.py')
spec = importlib.util.spec_from_file_location('projset_overview', MODULE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class OverviewTests(unittest.TestCase):
    def status(self):
        return dict(state='copying', started=0, updated=100, nodes_total=3,
            counts=dict(copied_bytes=2e12, skipped_bytes=1e12, payload_written=9e12,
                        copied=20, skipped=10, conflict=2, error=1, metadata_warnings=4),
            rates={'300': {'copied_Bps':1e9}}, pending={'scan':100,'copy':10},
            workers={'a':dict(ready=True,alive=True,age=2),
                     'b':dict(ready=True,alive=True,age=45),
                     'c':dict(ready=False,alive=False,age=2)})

    def test_verified_totals_exclude_inflight_and_stale_workers(self):
        s=self.status();v=m.overview_metrics(s,105)
        self.assertEqual(v['overview/已确认容量_TB'],3)
        self.assertEqual(v['overview/近5分钟完成速度_TB每小时'],3.6)
        self.assertEqual(v['overview/在线节点数'],1)
        self.assertEqual(v['overview/需复核结果数'],3)
        self.assertEqual(m.overview_metrics(s,200)['overview/在线节点数'],0)
        self.assertFalse(any('percent' in k.lower() for k in v))
        rows=m.overview_rows(s,105)
        self.assertEqual(next(r[1] for r in rows if r[0]=='全量总大小'),'尚未确定')
        self.assertEqual(next(r[1] for r in rows if r[0]=='总进度百分比 / 预计完成时间'),'尚不能可靠计算')
        self.assertEqual(next(r[1] for r in rows if r[0]=='已确认容量'),'3.000 TB')

    def test_rebuilding_never_reports_old_workers_or_speed(self):
        s=self.status()
        s.update(state='recovering', recovery_stage='completed-index',
                 recovery_file='completed.jsonl', recovery_records=123,
                 recovery_log_progress={'done_TB':0.02,'total_TB':0.04,'percent':50},
                 prefetch_progress={'state':'running','done_TB':0.01,'total_TB':0.04,'percent':25})
        v=m.overview_metrics(s,105)
        self.assertEqual(v['overview/在线节点数'],0)
        self.assertEqual(v['overview/近5分钟完成速度_TB每小时'],0)
        rows=m.overview_rows(s,105)
        self.assertEqual(next(r[1] for r in rows if r[0]=='当前日志已读记录'),'123')
        self.assertEqual(next(r[1] for r in rows if r[0]=='恢复阶段'),'completed-index')
        self.assertIn('50.0%',next(r[1] for r in rows if r[0]=='当前阶段日志读取进度'))
        self.assertIn('25.0%',next(r[1] for r in rows if r[0]=='历史回执提前读取'))
        self.assertIn('尚未恢复复制', rows[0][2])


if __name__=='__main__':unittest.main()
