"""Private policy fixtures only: no SSH, real run control, or target deletion."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('private_parent_tuning',
                                            Path(__file__).with_name('parent_tuning.py'))
tuning = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tuning)


class ParentTuningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='parent-policy-private-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.run = self.base/'run'
        self.run.mkdir()
        self.target = self.base/'private-target'
        self.target.mkdir()
        self.actor = tuning._identity(os.getpid())
        for name in ('rm24.py', *tuning.HELPERS):
            (self.run/name).write_text('private fixture '+name)
        self.cfg = dict(target=str(self.target), root_inode=self.target.stat().st_ino,
                        runtime_join=True,
                        parent_policy=dict(limit=2, node_boots={'node-a': 'boot-a'}),
                        parent_helpers={name: tuning._sha(self.run/name) for name in tuning.HELPERS},
                        sha256=tuning._sha(self.run/'rm24.py'))
        self.write('config.json', self.cfg)
        self.sha = tuning._sha(self.run/'config.json')
        self.write('controller.json', self.actor)
        self.write('status.json', dict(state='rm_running', updated=time.time(), errors=[]))
        self.applied = []
        self.tuner = tuning.ParentPolicyTuner(self.run, self.actor, self.sha, 2, self.applied.append)

    def write(self, name, value):
        (self.run/name).write_text(json.dumps(value))

    def request(self, generation=1, **changes):
        value = dict(version=1, scope=tuning.SCOPE, run=str(self.run), controller=self.actor,
                     config_sha256=self.sha, generation=generation, operation='a'*32, limit=1)
        value.update(changes)
        self.write('parent-policy.json', value)
        return value

    def publish(self, limit):
        return tuning.publish_limit(self.run, limit, expected_target=str(self.target),
                                    expected_root_inode=self.target.stat().st_ino)

    def test_policy_shape_and_types_are_strict(self):
        for value in ({}, dict(limit=2, node_boots={}),
                      dict(limit=True, node_boots={'a': 'b'}),
                      dict(limit=3, node_boots={'a': 'b'}),
                      dict(limit=2, node_boots={'a': ''}),
                      dict(limit=2, node_boots={'a': 'b'}, extra=True)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                tuning.validate_policy(value)
        for limit in (None, 1, 2):
            self.assertEqual(tuning.validate_policy(dict(limit=limit, node_boots={'a': 'b'}))['limit'], limit)

    def test_missing_initial_request_is_normal_and_snapshot_has_no_io(self):
        self.assertFalse(self.tuner.poll(force=True))
        self.assertIsNone(self.tuner.snapshot()['error'])
        with patch.object(Path, 'open', side_effect=AssertionError('snapshot performed IO')):
            self.assertEqual(self.tuner.snapshot()['limit'], 2)

    def test_missing_request_error_handler_does_not_probe_shared_storage_again(self):
        with patch.object(tuning, '_load', side_effect=FileNotFoundError('request missing')):
            with patch.object(Path, 'exists', side_effect=OSError('second IO failed')):
                self.assertFalse(self.tuner.poll(force=True))
        self.assertEqual(self.tuner.limit, 2)
        self.assertIsNone(self.tuner.snapshot()['error'])

    def test_missing_config_after_request_is_not_silently_treated_as_missing_request(self):
        self.request()
        (self.run/'config.json').unlink()
        self.assertFalse(self.tuner.poll(force=True))
        self.assertTrue(self.tuner.snapshot()['error'])
        self.assertEqual(self.applied, [])

    def test_applies_once_then_hot_disable_and_reenable(self):
        self.request()
        self.assertTrue(self.tuner.poll(force=True))
        self.assertFalse(self.tuner.poll(force=True))
        self.request(2, limit=None, operation='b'*32)
        self.assertTrue(self.tuner.poll(force=True))
        self.assertIsNone(self.tuner.limit)
        self.request(3, limit=2, operation='c'*32)
        self.assertTrue(self.tuner.poll(force=True))
        self.assertEqual(self.applied, [1, None, 2])
        self.assertEqual(self.tuner.snapshot()['generation'], 3)

    def test_poll_is_throttled_and_never_on_task_path(self):
        self.request()
        self.tuner.poll(force=True)
        with patch.object(tuning, '_load', side_effect=AssertionError('re-read too soon')):
            self.assertFalse(self.tuner.poll())
            self.assertEqual(self.tuner.limit, 1)

    def test_bad_binding_and_types_keep_last_good_limit(self):
        self.request()
        self.tuner.poll(force=True)
        invalid = [dict(version=True), dict(scope='all-directories'), dict(run='/other'),
                   dict(controller=dict(self.actor, start='999')), dict(config_sha256='0'*64),
                   dict(generation=True), dict(generation=0), dict(operation='not-a-uuid'),
                   dict(limit=True), dict(limit=4)]
        for change in invalid:
            with self.subTest(change=change):
                values = dict(generation=2)
                values.update(change)
                self.request(**values)
                self.assertFalse(self.tuner.poll(force=True))
                self.assertEqual(self.tuner.limit, 1)
                self.assertTrue(self.tuner.snapshot()['error'])
        self.assertEqual(self.applied, [1])

    def test_stale_or_reused_generation_cannot_rollback(self):
        self.request(2)
        self.tuner.poll(force=True)
        for generation, changes in ((1, {}), (2, dict(operation='b'*32)), (2, dict(limit=2))):
            self.request(generation, **changes)
            self.assertFalse(self.tuner.poll(force=True))
            self.assertEqual(self.tuner.limit, 1)
        self.assertEqual(self.applied, [1])

    def test_bad_json_missing_request_and_changed_config_keep_last_good(self):
        self.request()
        self.tuner.poll(force=True)
        path = self.run/'parent-policy.json'
        path.write_text('{')
        self.assertFalse(self.tuner.poll(force=True))
        path.unlink()
        self.assertFalse(self.tuner.poll(force=True))
        self.assertTrue(self.tuner.snapshot()['error'])
        self.request(2, limit=2)
        self.write('config.json', dict(self.cfg, mutated=True))
        self.assertFalse(self.tuner.poll(force=True))
        self.assertIn('config changed', self.tuner.snapshot()['error'])
        self.assertEqual(self.applied, [1])

    def test_os_error_is_reported_without_callback_or_abort(self):
        with patch.object(tuning, '_load', side_effect=OSError('shared storage unavailable')):
            self.assertFalse(self.tuner.poll(force=True))
        self.assertEqual(self.tuner.limit, 2)
        self.assertIn('unavailable', self.tuner.snapshot()['error'])
        self.assertEqual(self.applied, [])

    def test_publish_is_only_a_bound_submission_not_application(self):
        submitted = self.publish(1)
        self.assertTrue(submitted['submitted'])
        self.assertFalse(submitted['applied'])
        self.assertEqual(self.applied, [])
        request = json.loads((self.run/'parent-policy.json').read_text())
        self.assertEqual(request['controller'], self.actor)
        self.assertEqual(request['config_sha256'], self.sha)
        self.assertTrue(self.tuner.poll(force=True))
        self.assertEqual(self.tuner.snapshot()['operation'], submitted['operation'])
        self.assertEqual(list(self.run.glob('*.part')), [])

    def test_successful_atomic_replace_has_no_extra_cleanup_io(self):
        with patch.object(Path, 'exists', side_effect=OSError('stale shared stat')):
            with patch.object(Path, 'unlink', side_effect=OSError('cleanup failed')):
                tuning._atomic(self.run/'private-request.json', dict(private=True))
        self.assertEqual(json.loads((self.run/'private-request.json').read_text()), dict(private=True))

    def test_cleanup_failure_does_not_hide_original_write_failure(self):
        with patch.object(Path, 'replace', side_effect=OSError('original replace failed')):
            with patch.object(Path, 'unlink', side_effect=OSError('cleanup failed')):
                with self.assertRaisesRegex(OSError, 'original replace failed'):
                    tuning._atomic(self.run/'private-request.json', dict(private=True))

    def test_publisher_uses_monotonic_generation_above_disk_and_ack(self):
        self.request(100)
        self.write('status.json', dict(state='rm_running', updated=time.time(), errors=[],
                                     parent_tuning=dict(generation=200)))
        with patch.object(tuning.time, 'time_ns', return_value=1):
            first = self.publish(None)
            second = self.publish(2)
        self.assertEqual((first['generation'], second['generation']), (201, 202))
        self.assertNotEqual(first['operation'], second['operation'])

    def test_publisher_can_hot_disable_after_corrupt_or_obsolete_request(self):
        for previous in ('{', json.dumps(dict(broken=True)),
                         json.dumps(self.request(controller=dict(self.actor, start='0')))):
            (self.run/'parent-policy.json').write_text(previous)
            submitted = self.publish(None)
            self.assertTrue(submitted['replaced_invalid_request'])
            self.assertFalse(submitted['applied'])
            self.assertTrue(self.tuner.poll(force=True))
            self.assertIsNone(self.tuner.limit)
            self.assertEqual(self.tuner.snapshot()['operation'], submitted['operation'])

    def test_publisher_refuses_legacy_run_and_changed_frozen_files(self):
        for change in (dict(parent_policy=None), dict(runtime_join=False),
                       dict(parent_helpers={}), dict(target='/different'), dict(root_inode=-1)):
            with self.subTest(change=change):
                self.write('config.json', dict(self.cfg, **change))
                with self.assertRaises(ValueError):
                    self.publish(1)
                self.assertFalse((self.run/'parent-policy.json').exists())
        self.write('config.json', self.cfg)
        for name in ('rm24.py', *tuning.HELPERS):
            original = (self.run/name).read_text()
            (self.run/name).write_text('mutated fixture')
            with self.assertRaises(ValueError):
                self.publish(1)
            (self.run/name).write_text(original)

    def test_publisher_refuses_stopped_stale_unhealthy_or_wrong_controller(self):
        invalid = [dict(state='done', updated=time.time()),
                   dict(state='rm_running', updated=time.time()-16),
                   dict(state='rm_running', updated=time.time()+100),
                   dict(state='rm_running', updated=time.time(), errors=['failure'])]
        for status in invalid:
            self.write('status.json', status)
            with self.assertRaises(ValueError):
                self.publish(1)
        self.write('status.json', dict(state='rm_running', updated=time.time()))
        self.write('STOP', dict(reason='private fixture'))
        with self.assertRaises(ValueError):
            self.publish(1)
        (self.run/'STOP').unlink()
        self.write('controller.json', dict(self.actor, start='0'))
        with self.assertRaises(ValueError):
            self.publish(1)
        self.assertFalse((self.run/'parent-policy.json').exists())


if __name__ == '__main__':
    unittest.main()
