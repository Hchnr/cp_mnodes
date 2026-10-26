import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import experiment_idle_affinity as trial
from migration_io import read_json


class IdleAffinityGuards(unittest.TestCase):
    def test_data_fd_is_not_treated_as_idle(self):
        control = Path('/share-mixed/project/control-fixture')
        fd = Path('/proc/123/fd/7')
        with mock.patch.object(Path, 'iterdir', return_value=iter([fd])), \
                mock.patch.object(os, 'readlink', return_value='/share/project/data/file'):
            with self.assertRaisesRegex(RuntimeError, 'unexpected/data file descriptor'):
                trial.require_no_data_fds(123, control, 'fixture')

    def test_control_log_and_anonymous_socket_are_not_data_fds(self):
        control = Path('/share-mixed/project/control-fixture')
        for target in (str(control / 'recovery-logs/node-primary.log'), 'socket:[123]',
                       '/dev/null', '/tmp/fixture-worker.lock'):
            with self.subTest(target=target), \
                    mock.patch.object(Path, 'iterdir', return_value=iter([Path('/proc/123/fd/7')])), \
                    mock.patch.object(os, 'readlink', return_value=target):
                trial.require_no_data_fds(123, control, 'fixture')

    def test_exact_system_ca_file_is_not_a_payload_fd(self):
        control = Path('/share-mixed/project/control-fixture')
        default = mock.Mock(cafile='/usr/lib/ssl/cert.pem')
        with mock.patch.object(Path, 'iterdir', return_value=iter([Path('/proc/123/fd/7')])), \
                mock.patch.object(os, 'readlink', return_value='/etc/ssl/certs/ca-certificates.crt'), \
                mock.patch.object(trial.ssl, 'get_default_verify_paths', return_value=default), \
                mock.patch.object(os.path, 'realpath', return_value='/etc/ssl/certs/ca-certificates.crt'):
            trial.require_no_data_fds(123, control, 'fixture')

    def test_watchdog_independently_restores_on_new_work(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        self.addCleanup(os.close, write_fd)
        with tempfile.TemporaryDirectory(prefix='idle-affinity-new-work-') as directory, \
                mock.patch.object(trial.signal, 'signal'), \
                mock.patch.object(trial.select, 'select', return_value=([], [], [])), \
                mock.patch.object(trial, 'restore', return_value={'status': 'restored'}) as restore:
            guard = mock.Mock(side_effect=RuntimeError('new lease'))
            output = Path(directory) / 'result.json'
            self.assertEqual(trial.watchdog({}, read_fd, output, time.monotonic() + 10, guard), 0)
            restore.assert_called_once_with({})
            self.assertEqual(read_json(output)['reason'], 'idle_guard_failed')

    def test_restore_does_not_overwrite_another_operator(self):
        self.assertEqual(trial.restore_mask({0, 1}, {0, 1, 2, 3}, {0, 1}), {0, 1, 2, 3})
        self.assertEqual(trial.restore_mask({0, 1, 2, 3}, {0, 1, 2, 3}, {0, 1}), {0, 1, 2, 3})
        self.assertIsNone(trial.restore_mask({2, 3}, {0, 1, 2, 3}, {0, 1}))

    def test_reused_or_exited_pid_never_changes_affinity(self):
        original = dict(pid=123, start_ticks='5', argv=['fixture'])
        for change in (dict(original, start_ticks='6', state='S'),
                       dict(original, argv=['changed'], state='S'), dict(original, state='Z')):
            with mock.patch.object(trial, 'identity', return_value=change), \
                    mock.patch.object(os, 'sched_setaffinity') as update:
                result = trial.restore(dict(expected=original, original=[0, 1], trial=[0]))
            self.assertEqual(result['status'], 'target_changed_no_write')
            update.assert_not_called()

    @unittest.skipUnless(hasattr(os, 'sched_setaffinity') and len(os.sched_getaffinity(0)) > 1,
                         'requires Linux thread affinity')
    def test_independent_watchdog_restores_after_controller_pipe_disappears(self):
        # The target is a newly created private sleeper, never a production PID.
        child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(20)'])
        self.addCleanup(lambda: child.poll() is None and child.terminate())
        original = os.sched_getaffinity(child.pid)
        plan = dict(expected=trial.identity(child.pid), original=sorted(original),
                    trial=[min(original)])
        with tempfile.TemporaryDirectory(prefix='idle-affinity-watchdog-') as directory:
            output = Path(directory) / 'watchdog.json'
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(write_fd)
                code = trial.watchdog(plan, read_fd, output, time.monotonic() + 3)
                os._exit(code)
            os.close(read_fd)
            try:
                os.sched_setaffinity(child.pid, set(plan['trial']))
                os.close(write_fd)  # same EOF the guard observes on SIGKILL
                _, status = os.waitpid(pid, 0)
                self.assertEqual(status, 0)
                self.assertEqual(os.sched_getaffinity(child.pid), original)
                self.assertEqual(read_json(output)['status'], 'restored')
            finally:
                child.terminate()
                child.wait(timeout=5)

    @unittest.skipUnless(hasattr(os, 'sched_setaffinity') and len(os.sched_getaffinity(0)) > 1,
                         'requires Linux thread affinity')
    def test_watchdog_deadline_restores_even_if_controller_pipe_stays_open(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(20)'])
        original = os.sched_getaffinity(child.pid)
        plan = dict(expected=trial.identity(child.pid), original=sorted(original), trial=[min(original)])
        with tempfile.TemporaryDirectory(prefix='idle-affinity-deadline-') as directory:
            read_fd, write_fd = os.pipe()
            os.sched_setaffinity(child.pid, set(plan['trial']))
            pid = os.fork()
            if pid == 0:
                os.close(write_fd)
                os._exit(trial.watchdog(plan, read_fd, Path(directory) / 'watchdog.json',
                                       time.monotonic() + 0.05))
            os.close(read_fd)
            try:
                _, status = os.waitpid(pid, 0)
                self.assertEqual(status, 0)
                self.assertEqual(os.sched_getaffinity(child.pid), original)
            finally:
                os.close(write_fd)
                child.terminate()
                child.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
