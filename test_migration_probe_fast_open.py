"""Run the complete existing probe protocol/safety contract with the wrapper."""
import unittest
from unittest import mock
from pathlib import Path
import tempfile

import migration_linux_open
import migration_probe_fast_open as wrapper
import migration_target_probe as probe
import test_migration_target_probe as fixtures


class FastTargetProbeContractTests(fixtures.TargetProbeTests):
    def setUp(self):
        super().setUp()
        # Restore global hooks after every fixture, so the original service's
        # own tests and other importers remain unaffected by this wrapper.
        patch = mock.patch.object(probe, '_open_parent', probe._open_parent)
        patch.start()
        self.addCleanup(patch.stop)
        patch = mock.patch.object(probe.ProbeService, 'health',
                                  probe.ProbeService.health)
        patch.start()
        self.addCleanup(patch.stop)
        wrapper.install()

    def test_install_is_idempotent_and_preserves_identity_and_health_fields(self):
        wrapper.install()
        self.service.identity.update(argv=['python3', 'wrapper.py', '--slots', '2'],
                                     starttime_ticks='123', control='/fixture')
        for backend in ('openat2', 'fallback'):
            with mock.patch.object(migration_linux_open, 'safe_open_backend',
                                   return_value=backend):
                health = self.service.health()
            self.assertEqual(health['safe_open_backend'], backend)
            self.assertEqual(health['argv'], self.service.identity['argv'])
            self.assertEqual(health['starttime_ticks'], '123')
            self.assertEqual(health['control'], '/fixture')
            self.assertEqual(health['slots'], 2)
            self.assertEqual(health['batch_limit'], 64)
            self.assertNotIn('broker_token', health)
        self.assertIs(probe._open_parent,
                      migration_linux_open.open_parent_readonly)

    def test_main_delegates_without_rewriting_arguments(self):
        with mock.patch.object(probe, 'main') as main:
            wrapper.main()
        main.assert_called_once_with()

    def test_missing_runtime_override_keeps_current_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'roles' / 'probe-policy-runtime.json'
            controller = mock.Mock()
            controller.current_policy.return_value = {'max_file_bytes': 123}
            self.assertEqual(wrapper.runtime_policy(path, controller), {'max_file_bytes': 123})
            controller.current_policy.assert_called_once_with()

    def test_runtime_override_reads_snapshot_not_stale_canonical(self):
        from migration_io import write_json
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'roles' / 'probe-policy-runtime.json'
            write_json(path, {'max_file_bytes': 65536})
            write_json(path, {'max_file_bytes': 1024 * 1024})
            self.assertEqual(wrapper.runtime_policy(path, mock.Mock()),
                             {'max_file_bytes': 1024 * 1024})


if __name__ == '__main__':
    unittest.main()
