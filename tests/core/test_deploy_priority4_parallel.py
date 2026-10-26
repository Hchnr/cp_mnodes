import ast
from pathlib import Path
import unittest
from unittest import mock

import deploy_priority4_parallel as deploy


class Priority4DeployTests(unittest.TestCase):
    def test_dynamic_worker_and_metrics_roots_are_frozen(self):
        result = deploy.sources()
        for filename in ['migration_priority4_run.py', 'migration_priority4_worker.py',
                         'migration_priority4_broker.py', 'migration_worker128_run.py',
                         'migration_run.py', 'migration_worker.py', 'migration_task_publisher.py',
                         'migration_priority4_scan.py', 'migration_priority4_metadata.py',
                         'migration_metadata.py', 'migration_state_cache.py', 'migration_metrics.py']:
            self.assertIn(filename, result)
        self.assertTrue(all(len(value) == 64 for value in result.values()))

    def test_remote_preflight_code_compiles_and_is_read_only(self):
        def inspect(node, code):
            ast.parse(code)
            self.assertNotIn('write_json(', code)
            self.assertNotIn('Popen(', code)
            self.assertNotIn('os.kill', code)
            return {'node': node}
        with mock.patch.object(deploy, 'ssh', side_effect=inspect):
            self.assertEqual(deploy.preflight('master-0', '/control', 'fixture'), {'node': 'master-0'})

    def test_remote_role_code_compiles_without_process_signals(self):
        def inspect(node, code):
            ast.parse(code)
            self.assertNotIn('os.kill', code)
            self.assertNotIn('killpg', code)
            self.assertIn('start_ticks', code)
            self.assertIn('source-sha256.json', code)
            return {'node': node}
        with mock.patch.object(deploy, 'ssh', side_effect=inspect):
            for role in ['broker', 'producer', 'metrics', 'primary', 'supplement']:
                self.assertEqual(deploy.role('master-0', '/control', role), {'node': 'master-0'})

    def test_wrong_job_is_rejected_before_any_network_or_writes(self):
        class Args:
            job = 'old-cluster'
        with mock.patch.object(deploy, 'ssh') as ssh:
            with self.assertRaises(ValueError):
                deploy.prepare(Args())
            ssh.assert_not_called()


if __name__ == '__main__':
    unittest.main()
