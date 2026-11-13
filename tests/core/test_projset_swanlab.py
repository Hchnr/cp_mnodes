import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'ops/copy'))
SPEC = importlib.util.spec_from_file_location('projset_swanlab', REPO / 'ops/copy/projset_swanlab.py')
swanlab_reporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(swanlab_reporter)


class SupplementalMetricsTests(unittest.TestCase):
    def test_supplemental_progress_is_separate_and_uses_tb(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp)
            folder = run / 'recovery' / 'prior-error-retry'
            folder.mkdir(parents=True)
            (folder / 'manifest.json').write_text(json.dumps({
                'entries': [{'size': 1_000_000}, {'size': 2_000_000}],
            }))
            (folder / 'status.json').write_text(json.dumps({
                'state': 'running', 'total_files': 2,
                'counts': {'copied': 1, 'verified_bytes': 1_000_000},
            }))
            metrics = swanlab_reporter.supplemental_metrics(run)
            self.assertEqual(metrics['retry/verified_files'], 1)
            self.assertEqual(metrics['retry/total_files'], 2)
            self.assertEqual(metrics['retry/verified_TB'], 0.000001)
            self.assertEqual(metrics['retry/total_TB'], 0.000003)
            self.assertEqual(metrics['retry/running'], 1)

    def test_missing_or_corrupt_retry_does_not_break_reporting(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp)
            self.assertEqual(swanlab_reporter.supplemental_metrics(run), {})
            folder = run / 'recovery' / 'broken-retry'
            folder.mkdir(parents=True)
            (folder / 'manifest.json').write_text('{')
            (folder / 'status.json').write_text('{}')
            self.assertEqual(swanlab_reporter.supplemental_metrics(run), {})

    def test_recovery_progress_counts_previous_and_current_journals(self):
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp);run=base/'run';recovery=run/'recovery';recovery.mkdir(parents=True)
            (run/'tasks.jsonl').write_bytes(b'x'*10)
            current=recovery/'tasks.001.jsonl';current.write_bytes(b'y'*20)
            (recovery/'supervisor.json').write_text(json.dumps({'state':'running','updated':100,'controller_pid':123}))
            proc=base/'proc'/'123';(proc/'fd').mkdir(parents=True);(proc/'fdinfo').mkdir()
            (proc/'cmdline').write_bytes(b'python\0/tmp/projset_recovery_copy.py\0controller\0--run\0'+str(run).encode()+b'\0')
            (proc/'fd'/'3').symlink_to(current)
            (proc/'fdinfo'/'3').write_text('pos:\t5\n')
            status={'state':'recovering','recovery_stage':'tasks','recovery_file':current.name}
            value=swanlab_reporter.recovery_log_progress(run,status,base/'proc',now=105)
            self.assertEqual(value['percent'],50)
            self.assertEqual(value['done_TB'],15/1e12)
            self.assertEqual(value['total_TB'],30/1e12)
            self.assertIsNone(swanlab_reporter.recovery_log_progress(run,status,base/'proc',now=200))
            status['state']='copying'
            self.assertIsNone(swanlab_reporter.recovery_log_progress(run,status,base/'proc',now=105))

    def test_parallel_receipt_progress_is_aggregated_and_validated(self):
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp);run=base/'run';recovery=run/'recovery';recovery.mkdir(parents=True)
            (recovery/'supervisor.json').write_text(json.dumps({'state':'running','updated':100,
                'controller_pid':123,'attempt':'attempt-a'}))
            proc=base/'proc'/'123';proc.mkdir(parents=True)
            (proc/'cmdline').write_bytes(b'python\0/tmp/projset_recovery_copy.py\0controller\0--run\0'+str(run).encode()+b'\0')
            folder=recovery/'receipt-shards-attempt-a';folder.mkdir()
            (folder/'manifest.json').write_text(json.dumps({'attempt':'attempt-a',
                'node_total_bytes':{'node-a':100,'node-b':100},'total_bytes':200}))
            (folder/'node-a.result.json').write_text(json.dumps({'node':'node-a','done_bytes':100,'total_bytes':100}))
            (folder/'node-b.progress.json').write_text(json.dumps({'node':'node-b','done_bytes':20,'total_bytes':100}))
            status={'state':'recovering','recovery_stage':'receipts-parallel'}
            value=swanlab_reporter.recovery_log_progress(run,status,base/'proc',now=105)
            self.assertEqual(value['percent'],60)
            self.assertEqual(value['done_TB'],120/1e12)
            (folder/'node-b.progress.json').write_text(json.dumps({'node':'node-b','done_bytes':101,'total_bytes':100}))
            self.assertIsNone(swanlab_reporter.recovery_log_progress(run,status,base/'proc',now=105))

    def test_prefetch_progress_is_separate_and_uses_tb(self):
        with tempfile.TemporaryDirectory() as temp:
            run=Path(temp);recovery=run/'recovery';recovery.mkdir()
            path=recovery/'prefetch-status.json'
            path.write_text(json.dumps({'state':'running','updated':100,'parent_attempt':'main',
                                        'done_bytes':25,'total_bytes':100}))
            p=swanlab_reporter.prefetch_progress(run,now=105)
            self.assertEqual((p['state'],p['done_TB'],p['total_TB'],p['percent']),('running',25/1e12,100/1e12,25))
            path.write_text(json.dumps({'state':'ready','updated':100,'parent_attempt':'main','total_bytes':100}))
            self.assertEqual(swanlab_reporter.prefetch_progress(run,now=105)['percent'],100)
            (recovery/'prefetch-reuse.json').write_text(json.dumps({'state':'rejected','attempt':'main'}))
            p=swanlab_reporter.prefetch_progress(run,now=105)
            self.assertEqual(p['state'],'rejected')
            self.assertNotIn('percent',p)


if __name__ == '__main__':
    unittest.main()
