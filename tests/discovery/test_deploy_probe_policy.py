import tempfile
from pathlib import Path
import unittest
from unittest import mock

import deploy_probe_policy as policy
from migration_io import read_json, write_json


class ProbePolicyDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.control = Path(self.tmp.name) / 'control'
        self.bundle = self.control / 'target-probe-policy-fixture'
        self.bundle.mkdir(parents=True)
        self.cfg = {'control': str(self.control), 'run_id': 'fixture'}
        self.identities = {
            'primary': {'pid': 123, 'argv': ['primary'], 'starttime_ticks': '1'},
            'supplement': {'pid': 124, 'argv': ['supplement'], 'starttime_ticks': '2'}}
        self.protected = {'control': str(self.control), 'run_id': 'fixture',
                          'hosts': {'worker-0': 'fixture-worker-0'},
                          'workers': {'worker-0': self.identities}, 'master': {}}
        self.default = {'scope': 'fresh-small-copy-worker-precheck-v1',
                        'max_file_bytes': 1048576, 'min_missing': 16,
                        'recheck_every': 64, 'max_directories': 4096}
        write_json(self.control / 'config.json', self.cfg)
        write_json(self.bundle / 'protected-services.json', self.protected)
        write_json(self.bundle / 'probe-policy.json', self.default)

    def test_probe_transaction_is_guarded_before_and_after_only(self):
        with mock.patch.object(policy.deploy, 'validate_sources'), \
                mock.patch.object(policy, 'guard_node') as guard, \
                mock.patch.object(policy.deploy, 'run_node', return_value={'state': 'ready'}) as run:
            result = policy.run_node(self.control, self.bundle, 'worker-0')
        self.assertTrue(result['protected_processes_unchanged'])
        self.assertEqual(guard.call_args_list, [mock.call(self.control, self.bundle, 'worker-0')] * 2)
        run.assert_called_once_with(self.control, self.bundle, 'worker-0')

    def test_changed_copy_identity_prevents_any_probe_replacement(self):
        with mock.patch.object(policy.deploy, 'validate_sources'), \
                mock.patch.object(policy.socket, 'gethostname', return_value='fixture-worker-0'), \
                mock.patch.object(policy.deploy, 'identity', return_value=None), \
                mock.patch.object(policy.deploy, 'run_node') as run:
            with self.assertRaisesRegex(RuntimeError, 'protected process identity'):
                policy.run_node(self.control, self.bundle, 'worker-0')
        run.assert_not_called()

    def test_no_retry_after_ambiguous_probe_transaction(self):
        with mock.patch.object(policy.deploy, 'validate_sources'), \
                mock.patch.object(policy, 'guard_node') as guard, \
                mock.patch.object(policy.deploy, 'run_node', side_effect=TimeoutError('inspect')) as run:
            with self.assertRaises(TimeoutError):
                policy.run_node(self.control, self.bundle, 'worker-0')
        self.assertEqual(guard.call_count, 1)
        self.assertEqual(run.call_count, 1)

    def test_runtime_policy_does_not_edit_immutable_sources(self):
        raw = (self.bundle / 'probe-policy.json').read_bytes()
        with mock.patch.object(policy.deploy, 'validate_sources'), \
                mock.patch.object(policy.deploy, 'run_node') as run, \
                mock.patch.object(policy.deploy.os, 'kill') as kill:
            result = policy.tune(self.bundle, 65536)
            self.assertTrue(result['published_only'])
            self.assertEqual(read_json(self.bundle / 'roles' / 'probe-policy-runtime.json'),
                             dict(self.default, max_file_bytes=65536))
            self.assertEqual((self.bundle / 'probe-policy.json').read_bytes(), raw)
            run.assert_not_called()
            kill.assert_not_called()

    def test_invalid_threshold_or_stop_never_publishes_runtime_policy(self):
        with mock.patch.object(policy.deploy, 'validate_sources'):
            with self.assertRaises(ValueError):
                policy.tune(self.bundle, 1048577)
            (self.control / 'STOP').touch()
            with self.assertRaises(ValueError):
                policy.tune(self.bundle, 65536)
        self.assertFalse((self.bundle / 'roles').exists())


if __name__ == '__main__':
    unittest.main()
