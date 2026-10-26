"""Run the unchanged auth, batch, symlink and STOP contract with O_PATH."""
import unittest
from unittest import mock

import migration_linux_open as opening
import migration_probe_path_open as wrapper
import migration_probe_fast_open as fast
import migration_target_probe as probe
import test_migration_target_probe as fixtures


class PathProbeContractTests(fixtures.TargetProbeTests):
    def setUp(self):
        super().setUp()
        for obj, name in ((probe, '_open_parent'), (probe.ProbeService, 'health')):
            patch = mock.patch.object(obj, name, getattr(obj, name))
            patch.start(); self.addCleanup(patch.stop)
        wrapper.install()

    def test_install_idempotent_and_mode_reported(self):
        wrapper.install()
        self.assertIs(probe._open_parent, opening.open_parent_path)
        value = self.service.health()
        self.assertEqual(value['parent_open_mode'], 'O_PATH')
        self.assertEqual(value['slots'], self.service.slots)
        self.assertNotIn('broker_token', value)

    def test_main_preserves_policy_and_cli_entry(self):
        with mock.patch.object(fast, 'main') as main:
            wrapper.main()
        main.assert_called_once_with(installer=wrapper.install)


if __name__ == '__main__':
    unittest.main()
