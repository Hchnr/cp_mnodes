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


if __name__ == '__main__':
    unittest.main()
