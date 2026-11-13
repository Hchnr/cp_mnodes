import copy
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


PATH = Path(__file__).resolve().parents[2] / 'ops/diagnostics/projset_reporter_guard.py'
SPEC = importlib.util.spec_from_file_location('projset_reporter_guard', PATH)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)
AUDIT_PATH = PATH.with_name('projset_full_audit.py')
AUDIT_SPEC = importlib.util.spec_from_file_location('projset_full_audit_for_guard', AUDIT_PATH)
audit = importlib.util.module_from_spec(AUDIT_SPEC)
AUDIT_SPEC.loader.exec_module(audit)


class Clock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


class ReporterGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='reporter-guard-', dir=os.environ.get('REPORTER_TEST_PARENT'))
        self.parent = Path(self.tmp.name)
        self.work = self.parent / 'audit'
        self.work.mkdir()
        self.run_path = 'account/project/run123'
        self.clock = Clock(1120)
        self.values = {
            'audit/已核对文件及链接': 1000, 'audit/已核对文件_TB': 500.0,
            'audit/已核对项': 1100, 'queue/已完成目录': 50, 'queue/已发现目录': 100,
            'repair/已接入路径': 30, 'repair/已处理路径': 30,
            'repair/补传操作累计': 20, 'repair/补传_TB': 0.004,
        }
        self.sample = {'scan_time': 1119, 'files_links': 1000, 'file_bytes': 500_000_000_000_000,
                       'repair_time': 1119, 'copied_operations': 20, 'copied_bytes': 4_000_000_000}
        self.cloud = {key: {'step': 200, 'value': value} for key, value in self.values.items()}
        self.cloud['nodes/在线'] = {'step': 201, 'value': 16}
        self.cloud['other/metric'] = {'step': 202, 'value': 3}
        self.state = {'run_path': self.run_path, 'run_id': 'run123', 'state': 'streaming',
                      'step': 199, 'enqueued_at': 1000, 'interval_seconds': 120, 'last_sample': {
                          'scan_time': 900, 'files_links': 900, 'file_bytes': 499_000_000_000_000,
                          'repair_time': 900, 'copied_operations': 20, 'copied_bytes': 4_000_000_000}}
        self.write_state(self.state)
        self.cadence.write_text(json.dumps({'enqueued_at': 1000, 'interval_seconds': 120}))

    def tearDown(self):
        self.tmp.cleanup()

    @property
    def state_path(self):
        return self.work / 'swanlab.json'

    @property
    def cadence(self):
        return self.parent / 'attention-swanlab-cadence.json'

    def write_state(self, state):
        self.state_path.write_text(json.dumps(state))

    def reporter(self):
        return guard.ReporterGuard(self.work, self.run_path, clock=self.clock)

    def anchor(self, reporter):
        def fetch(run_path):
            self.assertEqual(run_path, self.run_path)
            with self.assertRaises(BlockingIOError):
                with self.reporter():
                    pass
            return copy.deepcopy(self.cloud)
        return reporter.anchor_cloud(fetch)

    def test_cloud_anchor_reservation_and_shared_cadence(self):
        calls = []
        with self.reporter() as reporter:
            self.assertEqual(self.anchor(reporter)['step'], 202)

            def send(values, step):
                with (self.parent / 'attention-swanlab-cadence.lock').open('a+b') as competing:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
                persisted = json.loads(self.state_path.read_text())
                self.assertEqual((persisted['state'], persisted['step']), ('sending', 203))
                calls.append((values, step))

            self.assertEqual(reporter.publish(self.values, self.sample, send), {'sent': True, 'step': 203})
            self.assertEqual(reporter.publish(self.values, self.sample, send)['reason'], 'cadence')
        state = json.loads(self.state_path.read_text())
        self.assertEqual((state['state'], state['step']), ('streaming', 203))
        self.assertEqual(state['last_sample'], self.sample)
        self.assertEqual(len(calls), 1)

    def test_real_payload_builder_keeps_both_speed_dimensions(self):
        scan = {'updated': 1119, 'counts': {'files': 900, 'symlinks': 100,
                'file_bytes': 500_000_000_000_000, 'entries': 1100},
                'discovered_dirs': 100, 'done_dirs': 50, 'pending_dirs': 50,
                'retry_dirs': 0, 'nodes_ready': 16}
        repair = {'updated': 1119, 'total': 30, 'done': 30, 'pending': 0, 'active': 0,
                  'retry': 0, 'copied_operations': 20, 'copied_bytes': 4_000_000_000, 'nodes_ready': 16}
        payload, sample = audit.reporter_values(scan, repair, self.state['last_sample'])
        self.assertEqual({k: payload[k] for k in guard.PROGRESS_METRICS}, self.values)
        self.assertEqual(sample, self.sample)
        self.assertAlmostEqual(payload['speed/复核文件及链接_项每小时'], 100 * 3600 / 219)
        self.assertAlmostEqual(payload['speed/复核逻辑容量_TB每小时'], 3600 / 219)
        sent = []
        with self.reporter() as reporter:
            self.anchor(reporter)
            reporter.publish(payload, sample, lambda values, step: sent.append((values, step)))
        self.assertEqual(sent, [(payload, 203)])
        without_repair, _ = audit.reporter_values(scan, None, self.state['last_sample'])
        self.assertNotIn('repair/补传_TB', without_repair)
        empty_repair, _ = audit.reporter_values(scan, {}, self.state['last_sample'])
        self.assertEqual(empty_repair['repair/补传_TB'], 0)

    def test_missing_cloud_or_mismatched_run_never_calls_sender(self):
        broken = copy.deepcopy(self.cloud)
        del broken['repair/补传_TB']
        with self.reporter() as reporter:
            with self.assertRaises(ValueError):
                reporter.anchor_cloud(lambda _: broken)
            with self.assertRaises(RuntimeError):
                reporter.publish(self.values, self.sample, lambda *_: self.fail('unexpected send'))
        self.state['run_id'] = 'other'
        self.write_state(self.state)
        with self.reporter() as reporter:
            with self.assertRaises(ValueError):
                reporter.anchor_cloud(lambda _: self.cloud)

    def test_regression_and_sample_mismatch_fail_closed(self):
        with self.reporter() as reporter:
            self.anchor(reporter)
            for key in guard.PROGRESS_METRICS:
                values = dict(self.values)
                values[key] -= 0.001 if key in guard.REAL_METRICS else 1
                sample = dict(self.sample)
                if key == 'audit/已核对文件及链接': sample['files_links'] -= 1
                if key == 'audit/已核对文件_TB': sample['file_bytes'] -= 1_000_000_000
                if key == 'repair/补传操作累计': sample['copied_operations'] -= 1
                if key == 'repair/补传_TB': sample['copied_bytes'] -= 1_000_000_000
                with self.subTest(key=key):
                    self.assertEqual(reporter.publish(values, sample, lambda *_: self.fail('unexpected send'))['reason'],
                                     'progress_regressed')
            with self.assertRaises(ValueError):
                reporter.publish(self.values, dict(self.sample, file_bytes=1), lambda *_: self.fail('unexpected send'))
            self.assertEqual(reporter.publish(self.values, dict(self.sample, scan_time=899),
                                              lambda *_: self.fail('unexpected send'))['reason'], 'sample_time_regressed')

    def test_shared_cadence_is_reserved_before_local_state(self):
        real_atomic = guard._atomic
        with self.reporter() as reporter:
            self.anchor(reporter)

            def fail_state(path, value):
                if path == self.state_path:
                    raise OSError('injected local state failure')
                real_atomic(path, value)

            with mock.patch.object(guard, '_atomic', side_effect=fail_state):
                with self.assertRaises(OSError):
                    reporter.publish(self.values, self.sample, lambda *_: self.fail('unexpected send'))
            self.assertEqual(json.loads(self.cadence.read_text())['enqueued_at'], 1120)
            self.assertEqual(json.loads(self.state_path.read_text())['step'], 199)
            self.assertEqual(reporter.publish(self.values, self.sample, lambda *_: self.fail('unexpected send'))['reason'],
                             'cadence')

    def test_sender_exception_preserves_step_and_cooldown(self):
        with self.reporter() as reporter:
            self.anchor(reporter)

            def send(*_):
                self.clock.value = 1125
                raise RuntimeError('injected after send')

            with self.assertRaises(RuntimeError):
                reporter.publish(self.values, self.sample, send)
        state = json.loads(self.state_path.read_text())
        self.assertEqual((state['state'], state['step']), ('sending', 203))
        self.assertEqual(json.loads(self.cadence.read_text())['enqueued_at'], 1125)
        self.clock.value = 1130
        with self.reporter() as reporter:
            self.anchor(reporter)
            self.clock.value = 1245
            self.assertEqual(reporter.publish(self.values, self.sample, lambda *_: self.fail('too soon'))['reason'],
                             'cadence')
            self.clock.value = 1250
            self.assertEqual(reporter.publish(self.values, self.sample, lambda *_: None)['step'], 204)

    def test_longer_existing_cadence_is_preserved(self):
        self.state['interval_seconds'] = 180
        self.write_state(self.state)
        self.cadence.write_text(json.dumps({'enqueued_at': 1000, 'interval_seconds': 240}))
        with self.reporter() as reporter:
            self.anchor(reporter)
            self.clock.value = 1239
            self.assertEqual(reporter.publish(self.values, self.sample, lambda *_: self.fail('too soon'))['reason'],
                             'cadence')
            self.clock.value = 1240
            self.assertEqual(reporter.publish(self.values, self.sample, lambda *_: None)['step'], 203)
        self.assertEqual(json.loads(self.state_path.read_text())['interval_seconds'], 240)
        self.assertEqual(json.loads(self.cadence.read_text())['interval_seconds'], 240)

    def test_invalid_cadence_and_mutable_anchor_fail_closed(self):
        with self.reporter() as reporter:
            anchor = self.anchor(reporter)
            anchor['watermark']['audit/已核对文件及链接'] = 0
            lower = dict(self.values, **{'audit/已核对文件及链接': 999})
            self.assertEqual(reporter.publish(lower, dict(self.sample, files_links=999), lambda *_: None)['reason'],
                             'progress_regressed')
            self.cadence.write_text(json.dumps({'enqueued_at': 1000, 'interval_seconds': 119}))
            with self.assertRaises(ValueError):
                reporter.publish(self.values, self.sample, lambda *_: self.fail('unexpected send'))
            self.assertEqual(json.loads(self.state_path.read_text())['step'], 199)

    @unittest.skipUnless(hasattr(os, 'fork'), 'requires a real process exit')
    def test_process_exit_after_send_cannot_reuse_or_regress_step(self):
        sent = self.parent / 'fake-cloud-point.json'
        pid = os.fork()
        if pid == 0:
            try:
                with self.reporter() as reporter:
                    reporter.anchor_cloud(lambda _: self.cloud)
                    def send(values, step):
                        sent.write_text(json.dumps({'step': step, 'value': values['audit/已核对文件及链接']}))
                        os._exit(86)
                    reporter.publish(self.values, self.sample, send)
            finally:
                os._exit(87)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 86)
        self.assertEqual(json.loads(sent.read_text())['step'], 203)
        self.assertEqual(json.loads(self.state_path.read_text())['state'], 'sending')
        self.clock.value = 1130
        with self.reporter() as reporter:
            self.anchor(reporter)
            self.clock.value = 1250
            smaller = dict(self.values, **{'audit/已核对文件及链接': 999})
            self.assertEqual(reporter.publish(smaller, dict(self.sample, files_links=999), lambda *_: None)['reason'],
                             'progress_regressed')
            self.assertEqual(reporter.publish(self.values, self.sample, lambda *_: None)['step'], 204)


if __name__ == '__main__':
    unittest.main()
