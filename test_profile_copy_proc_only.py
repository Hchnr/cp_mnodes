"""Identity validation fixtures; synthetic /proc files stay in owned temp trees."""
import copy
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

import profile_copy_proc_only as profile


CONTROL = '/share-mixed/project/.hcr-copy-fixture'
JOB = 'fixture-job'


def evidence():
    rich = {'control': CONTROL, 'job': JOB, 'nodes': []}
    argv = {'control': CONTROL, 'job': JOB, 'nodes': [], 'errors': [],
            'valid_processes': 24, 'rate_complete': True}
    for i, name in enumerate(sorted(profile.EXPECTED_NODES)):
        identities, lanes = {}, {}
        for j, lane in enumerate(('primary', 'extra64')):
            kind = 'primary' if lane == 'primary' else 'supplement'
            command = ['/usr/bin/python3', '-u', CONTROL + '/migration_cached_worker.py',
                       kind, '--control', CONTROL, '--threads', '64']
            if lane == 'extra64':
                command += ['--nice', '0']
            if name == 'worker-5' and lane == 'primary':
                command += ['--cache-max-age', '30.0']
            pid, ticks = 1000 + i * 2 + j, 2000 + i * 2 + j
            identities[lane] = {'pid': pid, 'start_ticks': ticks, 'identity_matches': True}
            lanes[lane] = {'pid': pid, 'start_ticks': str(ticks), 'identity_valid': True,
                           'state': 'S', 'argv': command}
        rich['nodes'].append({'host': 'job-' + JOB + '-' + name, 'identities': identities})
        argv['nodes'].append({'node': name, 'first': {'lanes': lanes},
                              'second': {'lanes': copy.deepcopy(lanes)}})
    return rich, argv


class BaselineBindingTests(unittest.TestCase):
    def setUp(self):
        self.rich, self.argv = evidence()

    def bind(self):
        return profile.bind_worker_argv(self.rich, self.argv, CONTROL, JOB)

    def test_all24_bound_with_canary_and_companion_arguments(self):
        bound = self.bind()
        self.assertEqual(len(bound), 12)
        self.assertEqual(bound['worker-5']['primary']['argv'][-2:], ['--cache-max-age', '30.0'])
        self.assertEqual(bound['worker-5']['extra64']['argv'][-2:], ['--nice', '0'])
        self.assertNotIn('argv', self.rich['nodes'][0]['identities']['primary'])

    def test_wrong_control_or_job_refused(self):
        for target, key in ((self.rich, 'control'), (self.argv, 'job')):
            original = target[key]
            target[key] = 'other'
            with self.assertRaises(ValueError):
                self.bind()
            target[key] = original

    def test_wrong_pid_or_start_cannot_be_adopted(self):
        for key in ('pid', 'start_ticks'):
            sample = self.argv['nodes'][0]['first']['lanes']['primary']
            original = sample[key]
            sample[key] = 999999
            with self.assertRaises(ValueError):
                self.bind()
            sample[key] = original

    def test_duplicate_or_missing_node_refused(self):
        self.argv['nodes'][-1] = copy.deepcopy(self.argv['nodes'][0])
        with self.assertRaises(ValueError):
            self.bind()
        self.rich, self.argv = evidence()
        self.argv['nodes'].pop()
        with self.assertRaises(ValueError):
            self.bind()

    def test_missing_lane_or_unvalidated_identity_refused(self):
        self.argv['nodes'][0]['second']['lanes'].pop('extra64')
        with self.assertRaises(ValueError):
            self.bind()
        self.rich, self.argv = evidence()
        self.rich['nodes'][0]['identities']['primary']['identity_matches'] = False
        with self.assertRaises(ValueError):
            self.bind()

    def test_changed_full_argument_with_same_pid_and_start_refused(self):
        self.argv['nodes'][0]['second']['lanes']['primary']['argv'] += ['--cache-max-age', '900']
        with self.assertRaises(ValueError):
            self.bind()

    def test_wrong_worker_lane_or_scope_refused_even_when_both_samples_agree(self):
        for sample in ('first', 'second'):
            self.argv['nodes'][0][sample]['lanes']['primary']['argv'][3] = 'supplement'
        with self.assertRaises(ValueError):
            self.bind()

    def test_failed_or_zombie_baseline_refused(self):
        self.argv['errors'] = [{'node': 'missing'}]
        with self.assertRaises(ValueError):
            self.bind()
        self.argv['errors'] = []
        self.argv['nodes'][0]['second']['lanes']['primary']['state'] = 'Z'
        with self.assertRaises(ValueError):
            self.bind()

    def test_normalization_only_removes_terminal_empty_fields(self):
        value = ['/usr/bin/python3', '', 'arg', '', '']
        self.assertEqual(profile.normalized_argv(value), ['/usr/bin/python3', '', 'arg'])
        for bad in ([], None, [1], [''], ['python', 'a\0b']):
            with self.assertRaises(ValueError):
                profile.normalized_argv(bad)


class RemoteReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='hcr-proc-identity-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.expected = profile.bind_worker_argv(*evidence(), CONTROL, JOB)['worker-5']['primary']
        self.proc = self.root / str(self.expected['pid'])
        self.proc.mkdir()
        (self.proc / 'stat').write_text(self.stat())
        (self.proc / 'cmdline').write_bytes(('\0'.join(self.expected['argv']) + '\0').encode())
        (self.proc / 'io').write_text('rchar: 100\nwchar: 200\n')
        task = self.proc / 'task' / str(self.expected['pid'])
        task.mkdir(parents=True)
        (task / 'wchan').write_text('futex_wait_queue_me\n')
        code = profile.REMOTE.split('first=snapshot();', 1)[0]
        code = code.replace('IDENTITIES', '{}', 1).replace('ROLES', '{}', 1).replace('CONTROL', repr(CONTROL), 1)
        fake_pathlib = types.SimpleNamespace(Path=lambda value: self.root if value == '/proc' else pathlib.Path(value))
        namespace = {}
        with mock.patch.dict(sys.modules, {'pathlib': fake_pathlib}):
            exec(compile(code, '<stdin-proc-fixture>', 'exec'), namespace)
        self.read = namespace['process']

    def stat(self, ticks=None, state='S'):
        fields = [state] + ['0'] * 49
        fields[17] = '67'
        fields[19] = str(ticks or self.expected['start_ticks'])
        return '%s (fixture ) process) %s\n' % (self.expected['pid'], ' '.join(fields))

    def test_exact_argv_and_identity_read_success(self):
        result = self.read(self.expected, 'primary')
        self.assertIs(result['identity_valid'], True)
        self.assertEqual(result['argv'], self.expected['argv'])
        self.assertEqual(result['io'], {'rchar': 100, 'wchar': 200})

    def test_live_same_pid_start_unapproved_argument_refused(self):
        command = self.expected['argv'][:-1] + ['900.0']
        (self.proc / 'cmdline').write_bytes(('\0'.join(command) + '\0').encode())
        result = self.read(self.expected, 'primary')
        self.assertIn('full argv differs', result['error'])
        self.assertNotIn('identity_valid', result)

    def test_pid_replacement_during_sample_refused(self):
        original = pathlib.Path.read_text
        calls = 0

        def changing(path, *args, **kwargs):
            nonlocal calls
            if path == self.proc / 'stat':
                calls += 1
                if calls >= 2:
                    return self.stat(ticks=999999)
            return original(path, *args, **kwargs)

        with mock.patch.object(pathlib.Path, 'read_text', changing):
            result = self.read(self.expected, 'primary')
        self.assertIn('changed while sampling', result['error'])

    def test_exec_argv_change_during_sample_refused(self):
        original = pathlib.Path.read_bytes
        calls = 0

        def changing(path, *args, **kwargs):
            nonlocal calls
            if path == self.proc / 'cmdline':
                calls += 1
                if calls >= 2:
                    return b'/usr/bin/python3\0different\0'
            return original(path, *args, **kwargs)

        with mock.patch.object(pathlib.Path, 'read_bytes', changing):
            result = self.read(self.expected, 'primary')
        self.assertIn('changed while sampling', result['error'])

    def test_zombie_and_missing_proc_are_errors_not_zero_rates(self):
        (self.proc / 'stat').write_text(self.stat(state='Z'))
        result = self.read(self.expected, 'primary')
        self.assertIn('no longer identifies', result['error'])
        (self.proc / 'stat').unlink()
        result = self.read(self.expected, 'primary')
        self.assertIn('FileNotFoundError', result['error'])
        self.assertNotIn('io', result)


if __name__ == '__main__':
    unittest.main()
