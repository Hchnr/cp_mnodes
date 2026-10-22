"""Mocked local role-launch fixtures; no child process or remote node runs."""

import concurrent.futures
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock


WORK = Path(__file__).absolute().parent
sys.path.insert(0, str(WORK))
import migration_recovery_role as role
from migration_io import read_json, write_json


class RoleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(dir=WORK, prefix='role-launch-fixture-')
        self.addCleanup(temp.cleanup)
        self.control = Path(temp.name)
        self.locks = self.control / 'locks'
        self.locks.mkdir()
        self.host = 'fixture-master-0'
        self.run_id = 'fixture-run'
        write_json(self.control / 'config.json', {'run_id': self.run_id})
        write_json(self.control / 'state.json', {'status': 'starting'})
        self.expected = role.role_argv(self.control, 'metrics')
        self.info = {'pid': 12345, 'start_ticks': '98765', 'argv': self.expected,
                     'state': 'S', 'nice': 15}
        self.process = mock.Mock(pid=12345)
        self.process.poll.return_value = None
        patches = [mock.patch.object(role, 'LOCK_ROOT', self.locks),
                   mock.patch.object(role.socket, 'gethostname', return_value=self.host),
                   mock.patch.object(role, 'matching_processes', return_value=[]),
                   mock.patch.object(role, 'process_info', return_value=None),
                   mock.patch.object(role, 'raise_nofile', return_value=(65536, 1048576)),
                   mock.patch.object(role.shutil, 'which', side_effect=lambda name: '/usr/bin/' + name),
                   mock.patch.object(role.subprocess, 'Popen', return_value=self.process),
                   mock.patch.object(role, 'wait_for_exec', return_value=self.info)]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def evidence_path(self, kind='metrics'):
        return self.control / 'recovery-roles' / (self.host + '-' + kind + '.json')

    def evidence(self, kind='metrics', **extra):
        expected = role.role_argv(self.control, kind)
        value = {'run_id': self.run_id, 'control': str(self.control), 'host': self.host,
                 'role': kind, 'node_index': None, 'expected_argv': expected,
                 'pid': 12345, 'start_ticks': '98765', 'argv': expected, 'status': 'launched'}
        value.update(extra)
        write_json(self.evidence_path(kind), value)
        return value

    def test_exact_identity_is_reused_without_spawn(self):
        self.evidence()
        role.process_info.return_value = self.info
        value = role.launch_role(self.control, 'metrics')
        self.assertEqual(value['status'], 'reused')
        role.subprocess.Popen.assert_not_called()
        role.matching_processes.assert_not_called()

    def test_unrecorded_exact_process_is_adopted(self):
        role.matching_processes.return_value = [self.info]
        value = role.launch_role(self.control, 'metrics')
        self.assertEqual(value['status'], 'adopted')
        self.assertEqual(read_json(self.evidence_path())['start_ticks'], '98765')
        role.subprocess.Popen.assert_not_called()

    def test_dead_producer_never_restarts_even_with_flag(self):
        self.evidence('producer')
        with self.assertRaisesRegex(RuntimeError, 'manual recovery'):
            role.launch_role(self.control, 'producer', restart=True)
        role.subprocess.Popen.assert_not_called()
        self.assertEqual(read_json(self.evidence_path('producer'))['pid'], 12345)

    def test_dead_other_role_requires_restart_and_preserves_old_identity(self):
        self.evidence()
        with self.assertRaisesRegex(RuntimeError, '--restart'):
            role.launch_role(self.control, 'metrics')
        role.subprocess.Popen.assert_not_called()
        value = role.launch_role(self.control, 'metrics', restart=True)
        self.assertEqual(value['status'], 'launched')
        self.assertEqual(value['previous_identity']['pid'], 12345)
        self.assertEqual(value['argv'], self.expected)
        command = role.subprocess.Popen.call_args.args[0]
        self.assertEqual(command[:6], ['/usr/bin/ionice', '-c', '3', '/usr/bin/nice', '-n', '15'])
        self.assertEqual(command[6:], self.expected)
        kwargs = role.subprocess.Popen.call_args.kwargs
        self.assertTrue(kwargs['start_new_session'])
        self.assertTrue(kwargs['close_fds'])
        self.assertEqual(value['nofile_soft'], 65536)

    def test_pid_reuse_or_live_argv_mismatch_never_reuses_or_kills(self):
        self.evidence()
        role.process_info.return_value = dict(self.info, start_ticks='new-start')
        with self.assertRaisesRegex(RuntimeError, 'dead or replaced'):
            role.launch_role(self.control, 'metrics')
        role.process_info.return_value = dict(self.info, argv=['other-program'])
        with self.assertRaisesRegex(RuntimeError, 'live process argv differs'):
            role.launch_role(self.control, 'metrics', restart=True)
        role.subprocess.Popen.assert_not_called()

    def test_stopped_or_terminal_control_refuses_launch(self):
        (self.control / 'STOP').touch()
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            role.launch_role(self.control, 'metrics')
        (self.control / 'STOP').unlink()
        write_json(self.control / 'state.json', {'status': 'complete'})
        with self.assertRaisesRegex(RuntimeError, 'terminal'):
            role.launch_role(self.control, 'metrics', restart=True)
        role.subprocess.Popen.assert_not_called()

    def test_exec_failure_retains_child_pid_and_never_kills(self):
        role.wait_for_exec.side_effect = RuntimeError('exec not confirmed')
        with self.assertRaisesRegex(RuntimeError, 'exec not confirmed'):
            role.launch_role(self.control, 'metrics')
        failed = read_json(self.evidence_path())
        self.assertEqual(failed['status'], 'launch_failed')
        self.assertEqual(failed['pid'], 12345)
        self.assertEqual(failed['expected_argv'], self.expected)
        self.process.kill.assert_not_called()
        self.process.terminate.assert_not_called()

    def test_concurrent_launchers_cannot_spawn_twice(self):
        entered, release = threading.Event(), threading.Event()
        def waiting_exec(*args):
            entered.set()
            self.assertTrue(release.wait(2))
            return self.info
        role.wait_for_exec.side_effect = waiting_exec
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(role.launch_role, self.control, 'metrics')
            self.assertTrue(entered.wait(1))
            second = pool.submit(role.launch_role, self.control, 'metrics')
            try:
                with self.assertRaises(BlockingIOError):
                    second.result(timeout=1)
            finally:
                release.set()
            self.assertEqual(first.result(timeout=1)['status'], 'launched')
        self.assertEqual(role.subprocess.Popen.call_count, 1)

    def test_role_cli_mapping_and_inventory_index_validation(self):
        inventory = role.role_argv(self.control, 'inventory', 11)
        self.assertEqual(inventory[-4:], ['--node-index', '11', '--rate', '100'])
        self.assertEqual(role.role_argv(self.control, 'producer')[3], 'run')
        self.assertEqual(role.role_argv(self.control, 'supplement')[-2:], ['--nice', '0'])
        with self.assertRaises(ValueError):
            role.role_argv(self.control, 'inventory')
        with self.assertRaises(ValueError):
            role.role_argv(self.control, 'primary', 1)

    def test_cli_never_prints_unknown_configuration_fields_from_evidence(self):
        self.evidence(unexpected_config={'broker_token': 'fixture-secret'})
        role.process_info.return_value = self.info
        out = io.StringIO()
        with mock.patch('sys.stdout', out):
            self.assertEqual(role.main(['--control', str(self.control), '--role', 'metrics']), 0)
        self.assertNotIn('fixture-secret', out.getvalue())
        self.assertNotIn('broker_token', out.getvalue())


class LimitsTests(unittest.TestCase):
    def test_nofile_raises_soft_and_refuses_insufficient_hard(self):
        with mock.patch.object(role.resource, 'getrlimit', side_effect=[(1024, 131072), (65536, 131072)]), \
                mock.patch.object(role.resource, 'setrlimit') as setting:
            self.assertEqual(role.raise_nofile(), (65536, 131072))
            setting.assert_called_once_with(role.resource.RLIMIT_NOFILE, (65536, 131072))
        with mock.patch.object(role.resource, 'getrlimit', return_value=(1024, 32768)), \
                mock.patch.object(role.resource, 'setrlimit') as setting:
            with self.assertRaisesRegex(RuntimeError, 'hard limit'):
                role.raise_nofile()
            setting.assert_not_called()


if __name__ == '__main__':
    unittest.main()
