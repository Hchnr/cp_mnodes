"""P4 process evidence fixtures, with an owned Linux child lifecycle test."""
import errno
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import migration_priority4_process_observation as observation


class ProcessObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hcr-p4-process-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control = '/share-mixed/project/.hcr-copy-fixture'
        self.expected = [{'role': 'primary', 'pid': 1001, 'start_ticks': '12345',
                          'argv': ['/usr/bin/python3', self.control + '/migration_cached_worker.py',
                                   'primary', '--control', self.control, '--threads', '64']}]

    def process(self, pid=1001, ticks='12345', state='S', argv=None, fd_names=()):
        directory = self.root / str(pid)
        directory.mkdir()
        fields = [state] + ['0'] * 49
        fields[17], fields[19] = '67', ticks
        (directory / 'stat').write_text('%s (fixture ) process) %s\n' % (pid, ' '.join(fields)))
        if argv is None:
            argv = self.expected[0]['argv']
        (directory / 'cmdline').write_bytes(('\0'.join(argv) + '\0').encode() if argv else b'')
        (directory / 'fd').mkdir()
        for name in fd_names:
            (directory / 'fd' / name).symlink_to('/payload-is-never-opened')
        return directory

    def observe(self):
        return observation.observe_node(self.control, self.expected, self.root)

    def test_live_exact_role_blocks_quiescence(self):
        self.process(fd_names=('0', '1'))
        value = self.observe()
        self.assertTrue(value['complete'])
        self.assertFalse(value['quiescent_snapshot'])
        self.assertEqual(value['registered'][0]['status'], 'running_exact')
        self.assertEqual(value['registered'][0]['observation']['open_fd_count'], 2)
        self.assertEqual(len(value['live_associated']), 1)
        self.assertFalse(value['launch_ready'])

    def test_absent_pid_can_be_recorded_without_fake_zero_counters(self):
        value = self.observe()
        self.assertTrue(value['complete'])
        self.assertTrue(value['quiescent_snapshot'])
        self.assertEqual(value['registered'][0]['status'], 'pid_absent')
        self.assertIsNone(value['registered'][0]['observation'])
        self.assertFalse(value['launch_ready'])

    def test_zombie_requires_empty_cmdline_and_fd_directory(self):
        self.process(state='Z', argv=[])
        value = self.observe()
        self.assertTrue(value['quiescent_snapshot'])
        self.assertEqual(value['registered'][0]['status'], 'zombie_no_execution_or_fds')

    def test_zombie_with_fd_or_argv_is_unknown(self):
        self.process(state='Z', argv=[], fd_names=('3',))
        value = self.observe()
        self.assertFalse(value['complete'])
        self.assertFalse(value['quiescent_snapshot'])
        self.assertEqual(value['registered'][0]['status'], 'unknown')

    def test_pid_reuse_does_not_adopt_unrelated_process(self):
        self.process(ticks='99999', argv=['/bin/sleep', '1'])
        value = self.observe()
        self.assertTrue(value['quiescent_snapshot'])
        self.assertEqual(value['registered'][0]['status'], 'pid_reused')
        self.assertEqual(value['registered'][0]['start_ticks'], '12345')
        self.assertEqual(value['registered'][0]['observation']['start_ticks'], '99999')

    def test_same_control_replacement_blocks_even_when_old_pid_gone(self):
        self.process(pid=2002, ticks='99999')
        value = self.observe()
        self.assertTrue(value['registered'][0]['old_identity_exited'])
        self.assertFalse(value['quiescent_snapshot'])
        self.assertEqual(value['live_associated'][0]['pid'], 2002)

    def test_same_pid_start_changed_argv_blocks(self):
        self.process(argv=self.expected[0]['argv'] + ['--cache-max-age', '900'])
        value = self.observe()
        self.assertTrue(value['complete'])
        self.assertFalse(value['quiescent_snapshot'])
        self.assertEqual(value['registered'][0]['status'], 'same_pid_start_argv_changed')

    def test_exact_scope_excludes_sibling_but_finds_shell_wrapper(self):
        self.process(pid=2002, argv=['python', self.control + '-sibling/run.py'])
        value = self.observe()
        self.assertTrue(value['quiescent_snapshot'])
        self.process(pid=2003, argv=['sh', '-c', 'exec python "' + self.control + '/run.py"'])
        value = self.observe()
        self.assertFalse(value['quiescent_snapshot'])
        self.assertEqual([x['pid'] for x in value['live_associated']], [2003])

    def test_missing_child_record_is_not_absent_pid(self):
        directory = self.process()
        (directory / 'cmdline').unlink()
        value = self.observe()
        self.assertFalse(value['complete'])
        self.assertEqual(value['registered'][0]['status'], 'unknown')

    def test_permission_failure_preserved_instead_of_empty_census(self):
        self.process(pid=2002)
        original = observation._process

        def denied(root, pid, count_fds=False):
            if pid == 2002:
                raise PermissionError(errno.EACCES, 'fixture denied')
            return original(root, pid, count_fds)

        with mock.patch.object(observation, '_process', denied):
            value = self.observe()
        self.assertFalse(value['complete'])
        self.assertFalse(value['quiescent_snapshot'])
        self.assertIn('PermissionError', value['errors'][0]['error'])

    def test_symlink_proc_root_and_pid_are_refused(self):
        linked = self.root / 'linked'
        linked.symlink_to(self.root, target_is_directory=True)
        value = observation.observe_node(self.control, self.expected, linked)
        self.assertFalse(value['complete'])
        (self.root / '1001').symlink_to(self.root, target_is_directory=True)
        value = self.observe()
        self.assertFalse(value['complete'])

    def test_changed_root_binding_refused(self):
        original = observation.os.stat

        def changed(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            if path == self.root and kwargs.get('follow_symlinks') is False:
                return type('Changed', (), {'st_dev': value.st_dev, 'st_ino': value.st_ino + 1})()
            return value

        with mock.patch.object(observation.os, 'stat', changed):
            value = self.observe()
        self.assertFalse(value['complete'])
        self.assertIn('root binding changed', value['errors'][-1]['error'])

    def test_identity_change_within_read_is_unknown(self):
        self.process()
        original = observation._stat
        calls = 0

        def changed(directory, pid):
            nonlocal calls
            calls += 1
            value = original(directory, pid)
            if calls == 2:
                value['start_ticks'] = '99999'
            return value

        with mock.patch.object(observation, '_stat', changed):
            value = self.observe()
        self.assertFalse(value['complete'])
        self.assertFalse(value['quiescent_snapshot'])

    def test_invalid_or_duplicate_inventory_refused(self):
        for identities in ([], [None], self.expected * 2, [dict(self.expected[0], pid=True)],
                           [dict(self.expected[0], argv=['python', '/unrelated/run.py'])]):
            with self.assertRaises(ValueError):
                observation.observe_node(self.control, identities, self.root)

    def test_reads_only_proc_directory_files_without_following_payload_fds(self):
        self.process(fd_names=('8',))
        original = observation.os.open
        opened = []

        def traced(path, flags, *args, **kwargs):
            opened.append((str(path), flags))
            return original(path, flags, *args, **kwargs)

        with mock.patch.object(observation.os, 'open', traced):
            value = self.observe()
        self.assertTrue(value['complete'])
        self.assertTrue(all(flags & os.O_NOFOLLOW for _, flags in opened))
        self.assertTrue(all(name in (str(self.root), '1001', 'stat', 'cmdline', 'fd') for name, _ in opened))
        self.assertFalse(value['migration_storage_accessed'])
        self.assertFalse(value['runtime_mutation_performed'])
        with mock.patch.object(observation.os, 'open', side_effect=AssertionError('must reject before any open')):
            refused = observation.observe_node(self.control, self.expected, self.control)
        self.assertFalse(refused['complete'])
        self.assertIn('dedicated temporary fixture tree', refused['errors'][0]['error'])

    @unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'WNOWAIT'), 'requires real Linux /proc and unreaped child')
    def test_real_owned_child_live_zombie_then_reaped(self):
        control = str(self.root / 'owned-fixture-control')
        command = [sys.executable, '-c', 'import time; time.sleep(60)', control]
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            fields = Path('/proc', str(child.pid), 'stat').read_text().rsplit(') ', 1)[1].split()
            expected = [{'role': 'owned_fixture_child', 'pid': child.pid,
                         'start_ticks': fields[19], 'argv': command}]

            def complete_observation():
                # An unrelated /proc entry can disappear during a census. A
                # fresh complete census is required; errors are never ignored
                # in the production observer or treated as an empty process.
                value = None
                for _ in range(3):
                    value = observation.observe_node(control, expected)
                    if value['complete']:
                        return value
                self.fail('no complete real /proc census: ' + repr(value['errors']))

            live = complete_observation()
            self.assertEqual(live['registered'][0]['status'], 'running_exact')
            self.assertFalse(live['quiescent_snapshot'])
            child.terminate()
            deadline = time.monotonic() + 5
            while os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                if time.monotonic() >= deadline:
                    self.fail('owned child did not exit within five seconds')
                time.sleep(0.01)
            zombie = complete_observation()
            self.assertEqual(zombie['registered'][0]['status'], 'zombie_no_execution_or_fds')
            self.assertTrue(zombie['registered'][0]['old_identity_exited'])
            self.assertTrue(zombie['quiescent_snapshot'])
            child.wait(timeout=5)
            reaped = complete_observation()
            self.assertEqual(reaped['registered'][0]['status'], 'pid_absent')
            self.assertTrue(reaped['quiescent_snapshot'])
            self.assertFalse(reaped['launch_ready'])
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
