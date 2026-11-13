import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'ops/diagnostics/projset_swanlab_historical.py'
spec = importlib.util.spec_from_file_location('swanlab_historical', SCRIPT)
historical = importlib.util.module_from_spec(spec)
spec.loader.exec_module(historical)


class HistoricalReporterTests(unittest.TestCase):
    def setUp(self):
        self.reference = {'schema': 1, 'kind': 'historical_unique_index_approximation',
            'excluded': 'eai_pwm/datasets/Sekai', 'directories': 1000, 'files_links': 10000}
        self.scan = {'state': 'running', 'started': 800, 'updated': 2200, 'done_dirs': 200,
                     'counts': {'files': 100, 'symlinks': 30}}
        self.monitor = {'audit_state': 'running', 'updated': 2190, 'sample': {'audit_updated': 2180},
                        'rates': {'audit_updated_seconds': 1200, 'audit_directories_per_hour': 100,
                                  'audit_files_links_per_hour': 1800}}

    def test_historical_scenarios_are_explicit_and_use_days(self):
        values = historical.historical_metrics(self.scan, self.monitor, self.reference, 2220)
        self.assertEqual(values['reference/首轮历史目录路径_万项'], .1)
        self.assertEqual(values['reference/首轮历史文件及链接路径_万项'], 1)
        self.assertEqual(values['estimate20/历史目录情景数据有效'], 1)
        self.assertEqual(values['estimate20/历史文件情景数据有效'], 1)
        self.assertAlmostEqual(values['estimate20/历史目录规模假设_剩余天'], 8 / 24)
        self.assertAlmostEqual(values['estimate20/历史文件规模假设_剩余天'], 9870 / 1800 / 24)

    def test_stale_restarted_zero_and_exhausted_inputs_do_not_publish_eta(self):
        stale = historical.historical_metrics(self.scan, self.monitor, self.reference, 6000)
        self.assertEqual(stale['estimate20/历史目录情景数据有效'], 0)
        self.assertNotIn('estimate20/历史目录规模假设_剩余天', stale)
        self.scan['started'] = 2100
        crossed = historical.historical_metrics(self.scan, self.monitor, self.reference, 2220)
        self.assertEqual(crossed['estimate20/历史目录情景数据有效'], 0)
        self.scan['started'] = 800
        self.scan['done_dirs'] = 1000
        self.monitor['rates']['audit_files_links_per_hour'] = 0
        exhausted = historical.historical_metrics(self.scan, self.monitor, self.reference, 2220)
        self.assertEqual(exhausted['estimate20/历史目录情景数据有效'], 0)
        self.assertEqual(exhausted['estimate20/历史文件情景数据有效'], 0)
        self.assertNotIn('estimate20/历史目录规模假设_剩余天', exhausted)
        self.assertNotIn('estimate20/历史文件规模假设_剩余天', exhausted)
        self.reference['directories'] = True
        self.assertNotIn('reference/首轮历史目录路径_万项',
                         historical.historical_metrics(self.scan, self.monitor, self.reference, 2220))

    def test_outbox_chart_is_a_fresh_bounded_sample_and_never_fakes_zero(self):
        monitor = {'updated': 2100, 'outbox_probe': {'state': 'ok', 'at': 2090, 'cursor': 10,
                    'max_id': 15, 'pending_events_capped': 5, 'pending_cap': 10001}}
        values = historical.outbox_metrics(monitor, 2200)
        self.assertEqual(values['monitor/待接入事件探测数据有效'], 1)
        self.assertEqual(values['repair/上次巡检待接入事件_上限10001'], 5)
        self.assertAlmostEqual(values['monitor/待接入事件采样距今_分钟'], 110 / 60)
        monitor['outbox_probe']['state'] = 'busy'
        busy = historical.outbox_metrics(monitor, 2200)
        self.assertEqual(busy['monitor/待接入事件探测数据有效'], 0)
        self.assertNotIn('repair/上次巡检待接入事件_上限10001', busy)
        monitor['outbox_probe']['state'] = 'ok'
        self.assertEqual(historical.outbox_metrics(monitor, 6000)['monitor/待接入事件探测数据有效'], 0)

    def test_reporter_waits_for_existing_lock_and_preserves_original_metrics(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            (work / 'code').mkdir()
            (work / 'monitor20').mkdir()
            (work / 'historical-reporter').mkdir()
            (work / 'monitor20/historical-reference.json').write_text(json.dumps(self.reference))
            deployed = work / 'code/projset_full_audit.py'
            deployed.write_text(textwrap.dedent('''
                import fcntl, json
                def monitor_values(scan, monitor, now, repair=None): return {'original_metric': 7}
                def repair_values(state): return {'original_repair_metric': 8}
                def reporter(work):
                    with (work / 'reporter.lock').open('a') as lock:
                        try:
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            return 0
                        scan = {'state':'running','started':800,'updated':2200,'done_dirs':200,
                                'counts':{'files':100,'symlinks':30}}
                        monitor = {'audit_state':'running','updated':2190,'sample':{'audit_updated':2180},
                                   'rates':{'audit_updated_seconds':1200,'audit_directories_per_hour':100,
                                            'audit_files_links_per_hour':1800}}
                        values = monitor_values(scan,monitor,2220)
                        values.update(repair_values({'outbox_busy_retries':4}))
                        (work / 'observed.json').write_text(json.dumps(values))
                    return 0
            '''))
            (work / 'manifest.json').write_text(json.dumps({'code_sha256': hashlib.sha256(deployed.read_bytes()).hexdigest()}))
            (work / 'historical-reporter/manifest.json').write_text(json.dumps(
                {'code_sha256': hashlib.sha256(SCRIPT.read_bytes()).hexdigest()}))
            with (work / 'reporter.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                proc = subprocess.Popen([sys.executable, '-S', '-B', str(SCRIPT), '--work', str(work)],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    time.sleep(.25)
                    self.assertIsNone(proc.poll())
                    self.assertFalse((work / 'observed.json').exists())
                    fcntl.flock(lock, fcntl.LOCK_UN)
                    out, err = proc.communicate(timeout=10)
                    self.assertEqual(proc.returncode, 0, (out, err))
                    values = json.loads((work / 'observed.json').read_text())
                    self.assertEqual(values['original_metric'], 7)
                    self.assertEqual(values['original_repair_metric'], 8)
                    self.assertEqual(values['repair/当前主控队列锁重试次数'], 4)
                    self.assertEqual(values['estimate20/历史目录情景数据有效'], 1)
                    owner = json.loads((work / 'historical-reporter/owner.json').read_text())
                    self.assertEqual(owner['pid'], proc.pid)
                    self.assertEqual(owner['host'], os.uname().nodename)
                    self.assertGreater(owner['start_ticks'], 0)
                    # Loss of this optional diagnostic record cannot stop uploads.
                    (work / 'historical-reporter/owner.json').unlink()
                    with mock.patch.object(historical.os, 'fsync', side_effect=OSError('unavailable')):
                        self.assertEqual(historical.load_blocking_reporter(work).reporter(work), 0)
                    self.assertTrue((work / 'observed.json').exists())
                    self.assertFalse((work / 'historical-reporter/owner.json').exists())
                finally:
                    if proc.poll() is None:
                        proc.kill(); proc.communicate()

    def test_reporter_rejects_changed_deployed_code(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            (work / 'code').mkdir()
            (work / 'code/projset_full_audit.py').write_text('changed')
            (work / 'manifest.json').write_text(json.dumps({'code_sha256': 'not-the-hash'}))
            with self.assertRaises(ValueError):
                historical.load_blocking_reporter(work)

    def test_guard_retries_until_final_swanlab_state_is_published(self):
        with tempfile.TemporaryDirectory() as temp:
            work = Path(temp)
            (work / 'repair').mkdir()
            (work / 'historical-reporter').mkdir()
            (work / 'status.json').write_text('{"state":"finished"}')
            (work / 'repair/status.json').write_text('{"state":"finished"}')
            (work / 'swanlab.json').write_text('{"state":"streaming"}')
            fake = work / 'fake-python'
            fake.write_text('#!' + sys.executable + '\n' + textwrap.dedent('''
                import json, sys
                from pathlib import Path
                w = Path(sys.argv[-1])
                count = w / 'count'
                n = int(count.read_text()) + 1 if count.exists() else 1
                count.write_text(str(n))
                if n == 2: (w / 'swanlab.json').write_text(json.dumps({'state':'finished'}))
            '''))
            fake.chmod(0o755)
            sleep = work / 'sleep'
            sleep.write_text('#!/bin/sh\nexec /bin/sleep 0.01\n')
            sleep.chmod(0o755)
            env = dict(os.environ, PATH=str(work) + ':' + os.environ.get('PATH', '/usr/bin:/bin'))
            guard = ROOT / 'ops/diagnostics/projset_swanlab_historical_guard.sh'
            result = subprocess.run(['/bin/sh', str(guard), str(work), str(fake)], env=env,
                                    capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((work / 'count').read_text(), '2')


if __name__ == '__main__':
    unittest.main()
