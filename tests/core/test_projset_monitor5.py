import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
CODE = ROOT / 'ops/diagnostics/projset_monitor20.py'
spec = importlib.util.spec_from_file_location('projset_monitor5', CODE)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)

REFERENCE = {'schema': 1, 'kind': 'historical_unique_index_approximation',
             'excluded': 'eai_pwm/datasets/Sekai', 'files_links': 1000}
ORIGINAL = {'counts': {'processed': 900, 'copied_bytes': 9_000_000_000_000,
                       'skipped_bytes': 1_000_000_000_000}}


def sample(at, files, size, dirs):
    return {'audit_updated': at, 'files_links': files,
            'logical_bytes': size, 'done_dirs': dirs}


class Monitor5Tests(unittest.TestCase):
    def test_windows_use_actual_elapsed_time_and_separate_references(self):
        previous = sample(1000, 100, 1_000_000_000_000, 20)
        current = sample(1300, 160, 2_000_000_000_000, 30)
        result = monitor.rate_window(current, previous, ORIGINAL, REFERENCE)
        self.assertTrue(result['valid'])
        self.assertEqual(result['seconds'], 300)
        self.assertEqual(result['files_links_per_hour'], 720)
        self.assertEqual(result['logical_TB_per_hour'], 12)
        self.assertEqual(result['directories_per_hour'], 120)
        self.assertEqual(result['files_progress_percent'], 16)
        self.assertEqual(result['logical_TB_progress_percent'], 20)
        self.assertAlmostEqual(result['files_remaining_hours'], 840 / 720)
        self.assertAlmostEqual(result['logical_TB_remaining_hours'], 8 / 12)

    def test_invalid_or_exhausted_data_never_produces_eta(self):
        previous = sample(1000, 100, 1000, 20)
        for bad in (sample(1000, 110, 1100, 21), sample(1300, 99, 1100, 21),
                    sample(1300, 110, 999, 21), sample(1300, 110, 1100, True)):
            self.assertFalse(monitor.rate_window(bad, previous, ORIGINAL, REFERENCE)['valid'])
        exhausted = monitor.rate_window(sample(1300, 1000, 10_000_000_000_000, 21),
                                        previous, ORIGINAL, REFERENCE)
        self.assertNotIn('files_remaining_hours', exhausted)
        self.assertNotIn('logical_TB_remaining_hours', exhausted)
        fallback = monitor.rate_window(sample(1300, 110, 1100, 21), previous, ORIGINAL,
                                       {'files_links': 10000})
        self.assertEqual(fallback['files_reference'], 900)
        self.assertEqual(fallback['files_reference_kind'], 'first_pass_processed_approximation')

    def test_small_files_do_not_cause_false_degradation(self):
        baseline = {'valid': True, 'files_links_per_hour': 1200,
                    'logical_TB_per_hour': 100, 'directories_per_hour': 600}
        small_files = dict(baseline, files_links_per_hour=1300, logical_TB_per_hour=0.001)
        result = monitor.performance_assessment(small_files, baseline, {'degraded_streak': 1})
        self.assertEqual(result['state'], 'observing')
        self.assertEqual(result['degraded_streak'], 0)
        self.assertTrue(result['low_dimensions']['logical_TB_per_hour'])

    def test_persistent_decline_is_detected_and_restart_windows_not_compared(self):
        baseline = {'valid': True, 'files_links_per_hour': 1200,
                    'logical_TB_per_hour': 100, 'directories_per_hour': 600}
        slow = {'valid': True, 'files_links_per_hour': 100,
                'logical_TB_per_hour': 1, 'directories_per_hour': 500}
        first = monitor.performance_assessment(slow, baseline, {})
        second = monitor.performance_assessment(slow, baseline, first)
        self.assertEqual(first['degraded_streak'], 1)
        self.assertEqual(second['degraded_streak'], 2)
        self.assertFalse(second['low_dimensions']['directories_per_hour'])
        restarted = dict(baseline, spans_controller_restart=True)
        self.assertEqual(monitor.performance_assessment(slow, restarted, second)['state'],
                         'insufficient_data')
        malformed = dict(slow, logical_TB_per_hour=float('nan'))
        self.assertEqual(monitor.performance_assessment(malformed, baseline, second)['state'],
                         'insufficient_data')

    def test_history_ignores_torn_lines_and_finds_nearest_actual_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'history.jsonl'
            first, second = sample(100, 10, 100, 1), sample(400, 20, 200, 2)
            path.write_text(json.dumps({'sample': first}) + '\n' +
                            json.dumps({'sample': second}) + '\n' + '{"sample":')
            samples = monitor.recent_samples(path)
            self.assertEqual(samples, [first, second])
            self.assertEqual(monitor.select_sample(samples, 720, 300, 180, 600), second)
            self.assertIsNone(monitor.select_sample(samples, 900, 300, 180, 400))

    def test_long_baseline_survives_short_window_normalization_and_skips_restart(self):
        short = {'valid': True, 'files_links_per_hour': 100,
                 'logical_TB_per_hour': 0.1, 'directories_per_hour': 300}
        one_hour = dict(short, files_links_per_hour=150, logical_TB_per_hour=0.2)
        six_hours = dict(short, files_links_per_hour=800, logical_TB_per_hour=5)
        windows = {'last_6h': six_hours, 'last_1h': one_hour, 'last_20m': one_hour}
        name, baseline = monitor.performance_baseline(windows)
        self.assertEqual(name, 'last_6h')
        self.assertEqual(monitor.performance_assessment(short, baseline, {})['state'], 'degraded')
        windows['last_6h'] = dict(six_hours, spans_controller_restart=True)
        name, baseline = monitor.performance_baseline(windows)
        self.assertEqual(name, 'last_1h')
        self.assertEqual(monitor.performance_assessment(short, baseline, {})['state'], 'observing')

    def run_one_monitor_check(self, started, audit_age=0):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / 'run'
            audit = run / 'audit'
            work = audit / 'monitor20'
            (work / 'code').mkdir(parents=True)
            (work / 'code/projset_monitor20.py').write_bytes(CODE.read_bytes())
            (work / 'manifest.json').write_text(json.dumps({
                'code_sha256': hashlib.sha256(CODE.read_bytes()).hexdigest()}))
            (run / 'status.json').write_text(json.dumps(ORIGINAL))
            (work / 'historical-reference.json').write_text(json.dumps(REFERENCE))
            shift = 21600
            audit_status = {'updated': 7200 + shift, 'started': started, 'done_dirs': 100,
                            'counts': {'files': 500, 'symlinks': 0,
                                       'file_bytes': 5_000_000_000_000}}
            (audit / 'status.json').write_text(json.dumps(audit_status))
            samples = [sample(7200, 30, 300_000_000_000, 6),
                       sample(3600 + shift, 140, 1_400_000_000_000, 28),
                       sample(6000 + shift, 380, 3_800_000_000_000, 76),
                       sample(6900 + shift, 470, 4_700_000_000_000, 94)]
            (work / 'history.jsonl').write_text(''.join(json.dumps({'sample': x}) + '\n' for x in samples))
            (work / 'status.json').write_text(json.dumps({'updated': 0, 'sample': samples[-1]}))

            def inspect(_run, _work, previous):
                self.assertEqual(previous, samples[-2])
                result = monitor.summarize(audit_status, {}, previous, ORIGINAL)
                result.update(state='finished', audit_state='running',
                              audit_age_seconds=audit_age, problems=[], updated=7200 + shift)
                return result

            with mock.patch.object(monitor, 'inspect_once', side_effect=inspect):
                self.assertEqual(monitor.run_monitor(run, work), 0)
            return json.loads((work / 'status.json').read_text())

    def test_five_minute_checks_preserve_twenty_minute_chart_window(self):
        result = self.run_one_monitor_check(started=1)
        self.assertEqual(result['interval_seconds'], 300)
        self.assertEqual(result['rate_interval_seconds'], 1200)
        self.assertEqual(result['rates']['audit_updated_seconds'], 1200)
        self.assertEqual(result['windows']['last_5m']['seconds'], 300)
        self.assertEqual(result['windows']['last_20m']['seconds'], 1200)
        self.assertEqual(result['windows']['last_1h']['seconds'], 3600)
        self.assertEqual(result['windows']['last_6h']['seconds'], 21600)
        self.assertEqual(result['performance']['state'], 'observing')
        self.assertEqual(result['performance']['baseline_window'], 'last_6h')

    def test_restart_windows_omit_eta_and_degradation_assessment(self):
        result = self.run_one_monitor_check(started=7100 + 21600)
        for window in result['windows'].values():
            self.assertTrue(window['spans_controller_restart'])
            self.assertNotIn('files_remaining_hours', window)
            self.assertNotIn('logical_TB_remaining_hours', window)
        self.assertEqual(result['performance']['state'], 'insufficient_data')

    def test_stale_controller_cannot_publish_eta_or_trigger_rate_alert(self):
        result = self.run_one_monitor_check(started=1, audit_age=301)
        for window in result['windows'].values():
            self.assertTrue(window['stale'])
            self.assertNotIn('files_remaining_hours', window)
            self.assertNotIn('logical_TB_remaining_hours', window)
        self.assertEqual(result['performance']['state'], 'insufficient_data')


if __name__ == '__main__':
    unittest.main()
