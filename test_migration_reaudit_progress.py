import contextlib
import io
import time
import unittest
from pathlib import Path

from migration_reaudit_progress import copy_progress, copy_view,namespace_postcheck_progress


class ProgressTests(unittest.TestCase):
    def test_namespace_handoff_is_attention_not_successful_postcheck(self):
        root=Path('/fixture')
        records={root/'original-postcheck-watchers-v2/launch-results.json':dict(watchers=[dict(node='worker-1')]),
            root/'nodes/worker-1/post-copy-audit-original/result.json':dict(status='original_actor_replaced_or_handed_off')}
        def read(path):
            if path not in records:raise FileNotFoundError(path)
            return records[path]
        output=io.StringIO()
        with contextlib.redirect_stdout(output):namespace_postcheck_progress(root,read)
        self.assertIn('已返回 0/1，需检查 1',output.getvalue())

    def test_finite_attention_pass_counts_are_separate_from_main(self):
        root=Path('/fixture')
        records={root/'conditional-copy-state.json':dict(status='copy_running',controls={}),
            root/'final/attention-pass-1/progress.json':dict(completed_groups=1,max_groups=7,
                counts=dict(identity_verified=40,original_attention=64,changed=24),status='copying')}
        def read(path):
            if path not in records:raise FileNotFoundError(path)
            return records[path]
        report=dict(missing=dict(file=dict(paths=1,logical_bytes=1),directory=dict(paths=0),symlink=dict(paths=0)))
        output=io.StringIO()
        with contextlib.redirect_stdout(output):copy_view(root,report,read)
        self.assertIn('已闭合 1/7 组；已复核 40/64 项，再次变化 24',output.getvalue())

    def test_rollout_replaces_obsolete_single_pilot_label(self):
        root=Path('/fixture');folder=root/'rollout'
        records={root/'conditional-copy-state.json':dict(status='copy_running',controls={}),
            root/'final/namespace-handoff-rollout.json':dict(folder=str(folder)),
            folder/'progress.json':dict(completed=['master-0'],expected=11,node='worker-1',status='staging_receiver'),
            root/'final/namespace-handoff-pilot.json':dict(plan='/pilot/plan.json',node='worker-0'),
            Path('/pilot/receiver-state.json'):dict(status='receiver_returned')}
        def read(path):
            if path not in records:raise FileNotFoundError(path)
            return records[path]
        report=dict(missing=dict(file=dict(paths=1,logical_bytes=1),directory=dict(paths=0),symlink=dict(paths=0)))
        output=io.StringIO()
        with contextlib.redirect_stdout(output):copy_view(root,report,read)
        self.assertIn('试点 1 + 后续 1/11',output.getvalue())
        self.assertNotIn('其他分片不动',output.getvalue())

    def test_terminal_worker_does_not_report_historical_transfer_speed(self):
        def read(path):
            if path.name=='config.json':return dict(files=1,bytes=1024,shards=dict(master=1))
            return dict(status='complete',updated=time.time(),counts=dict(processed=1,copied=1,copied_bytes=1024),
                        active=1,write_bytes_per_second=10*1024**3)
        output=io.StringIO()
        with contextlib.redirect_stdout(output):copy_progress(dict(files='/fixture'),read)
        self.assertIn('近窗口写入 0.00 GiB/s',output.getvalue())
        self.assertIn('在途任务 0',output.getvalue())
        self.assertIn('处理 1/1',output.getvalue())

    def test_replayed_namespace_proof_is_not_reported_pending(self):
        root=Path('/fixture')
        records={
            root/'conditional-copy-state.json':dict(status='copy_running',controls={}),
            root/'final/incremental-namespace-closure/runtime-progress.json':dict(
                captured_directories=2198,observations=22958,pending_directories=0),
            root/'final/incremental-namespace-closure/verified/report.json':dict(
                status='namespace_proof_replayed_new_findings_classified'),
        }
        def read(path):
            if path not in records:raise FileNotFoundError(path)
            return records[path]
        report=dict(missing=dict(file=dict(paths=1,logical_bytes=1),directory=dict(paths=0),symlink=dict(paths=0)),
                    whole_tree_completion_proven=False)
        output=io.StringIO()
        with contextlib.redirect_stdout(output):copy_view(root,report,read)
        self.assertIn('覆盖证明已回放',output.getvalue())
        self.assertNotIn('覆盖证明及新缺失仍需合并复核',output.getvalue())
        self.assertIn('不等于全树零遗漏',output.getvalue())

    def test_small_file_write_speed_is_visible_below_gib_rounding(self):
        def read(path):
            if path.name=='config.json':return dict(files=10,bytes=1024,shards=dict(master=1))
            return dict(status='copying',updated=time.time(),counts=dict(processed=1,copied=1,copied_bytes=1024),
                        active=1,write_bytes_per_second=2*1024**2,complete_bytes_per_second=2*1024**2)
        output=io.StringIO()
        with contextlib.redirect_stdout(output):copy_progress(dict(files='/fixture'),read)
        self.assertIn('0.00 GiB/s（2.0 MiB/s）',output.getvalue())


if __name__=='__main__':unittest.main()
