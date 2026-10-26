"""Local fixtures only: no SSH, remote signals, or remote process launches."""
from contextlib import contextmanager, redirect_stdout
import copy
import datetime
import io
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from migration_io import read_json, write_json
import rollout_cache_cluster as cluster


IDENTITY = {'pid': 10, 'starttime_ticks': '20', 'state': 'S',
            'argv': ['python3', '/control/migration_cached_worker.py', 'primary',
                     '--control', '/control', '--cache-max-age', '3.0']}


class Backend:
    def __init__(self, *, targets=(), waiting=None, fail=None, healthy=True, helper_hash=None):
        self.targets = set(targets)
        self.waiting = copy.deepcopy(waiting or {})
        self.fail, self.healthy = fail, healthy
        self.helper_hash = helper_hash or cluster.source_versions()['rollout_cache_age.py']
        self.events, self.instances, self.holds = [], [], 0

    def factory(self, **kwargs):
        backend = self
        node, lane = kwargs['node'], kwargs['lane']
        pair = (node, lane)
        operation = kwargs['operation']
        identity = copy.deepcopy(IDENTITY)
        if pair in self.targets:
            identity['argv'][-1] = '30.0'
        class RPC:
            def __call__(self, called_node, action, **arguments):
                backend.events.append((pair, action, operation))
                if backend.fail == (pair, action):
                    raise RuntimeError('fixture ambiguous RPC')
                if action == 'inspect':
                    return {'identity': identity, 'record_expected': identity,
                            'record_operation': operation, 'record_status': 'idle_verified',
                            'active_count': 0, 'payload_fd_count': 0, 'checkpoint_protocol': 2}
                if action == 'drain':
                    return {'draining': True}
                if action == 'gate':
                    waits = backend.waiting.get(pair, [])
                    ready = not waits.pop(0) if waits else True
                    return {'ready': ready, 'operation': operation, 'checkpoint_protocol': 2}
                if action == 'replace':
                    backend.targets.add(pair)
                    return {'ready': True}
                if action == 'release':
                    return {'released': True}
                if action == 'observe':
                    return {'drain_owned_current': True, 'draining': True, 'status': 'unknown'}
                raise AssertionError(action)

            def health(self, expected):
                backend.events.append((pair, 'health', operation))
                return {'healthy': backend.healthy, 'identity': identity,
                        'helper_sha256': backend.helper_hash}

            @contextmanager
            def hold(self):
                backend.holds += 1
                if backend.holds != 1:
                    raise AssertionError('overlapping host drains')
                backend.events.append((pair, 'hold', operation))
                try:
                    yield {'locked': True}
                finally:
                    backend.events.append((pair, 'unhold', operation))
                    backend.holds -= 1
        value = RPC()
        self.instances.append(kwargs)
        return value


class ClusterTests(unittest.TestCase):
    def run_fixture(self, directory, backend, **overrides):
        kwargs = dict(control='/control', job='fixture-job', remote_script='/control/helper/rollout_cache_age.py',
                      output=Path(directory) / 'rollout.json', nodes=['worker-0', 'worker-1'],
                      lanes=['primary', 'supplement'], rpc_factory=backend.factory)
        kwargs.update(overrides)
        return cluster.run_cluster(**kwargs)

    def test_defaults_cover_twelve_physical_nodes_and_both_lanes(self):
        self.assertEqual(len(cluster.NODES), 12)
        backend = Backend()
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_fixture(directory, backend, nodes=None, lanes=None)
            self.assertEqual(result['summary'], {'complete': 24})
            self.assertEqual(len(backend.instances), 24)
            self.assertEqual(len({x['operation'] for x in backend.instances}), 24)
            self.assertEqual(backend.holds, 0)
            self.assertEqual(read_json(Path(directory) / 'rollout.json')['status'], 'pass_finished')

    def test_only_waiting_pairs_retry_in_later_round_and_operations_are_new(self):
        backend = Backend(waiting={('worker-0', 'primary'): [True, False]})
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_fixture(directory, backend, rounds=3)
        self.assertEqual(result['summary'], {'complete': 4})
        first = result['entries'][0]
        self.assertEqual([x['status'] for x in first['attempts']], ['waiting_payload', 'complete'])
        self.assertNotEqual(first['attempts'][0]['operation'], first['attempts'][1]['operation'])
        self.assertTrue(all(len(x['attempts']) == 1 for x in result['entries'][1:]))
        self.assertEqual(sum(event[1] == 'replace' for event in backend.events), 4)

    def test_actual_target_identity_skips_without_drain_even_when_old_records_are_absent(self):
        backend = Backend(targets={('worker-0', 'primary')})
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_fixture(directory, backend, nodes=['worker-0'], lanes=['primary'], rounds=3)
        self.assertEqual(result['summary'], {'at_target': 1})
        self.assertEqual([event[1] for event in backend.events], ['hold', 'inspect', 'health', 'unhold'])

    def test_invalid_health_or_wrong_helper_hash_stops_before_drain(self):
        for kwargs in ({'healthy': False}, {'helper_hash': 'foreign-helper'}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as directory:
                backend = Backend(**kwargs)
                with self.assertRaises(RuntimeError):
                    self.run_fixture(directory, backend)
                result = read_json(Path(directory) / 'rollout.json')
                self.assertEqual(result['status'], 'attention')
                self.assertEqual(len(backend.instances), 1)
                self.assertNotIn('drain', [event[1] for event in backend.events])
                self.assertEqual(backend.holds, 0)

    def test_unknown_replacement_stops_without_release_or_next_lane(self):
        backend = Backend(fail=(('worker-0', 'primary'), 'replace'))
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, 'inspection'):
                self.run_fixture(directory, backend)
            result = read_json(Path(directory) / 'rollout.json')
            self.assertEqual(result['summary'], {'attention': 1, 'pending': 3})
        self.assertEqual(sum(event[1] == 'replace' for event in backend.events), 1)
        self.assertNotIn('release', [event[1] for event in backend.events])
        self.assertEqual(len(backend.instances), 1)
        self.assertEqual(backend.holds, 0)

    def test_fixed_canary_limits_and_alias_validation_before_any_ssh(self):
        for kwargs in ({'max_age': 31}, {'gate_timeout': 31}, {'gate_timeout': 0}, {'rounds': 0},
                       {'visibility_timeout': -1}, {'visibility_timeout': float('inf')},
                       {'control_timeout': 0}, {'control_timeout': 61},
                       {'control_timeout': float('inf')}, {'control_timeout': float('nan')},
                       {'ssh_prefix': '-oProxyCommand='}, {'nodes': ['worker-12']},
                       {'nodes': ['worker-0', 'worker-0']}, {'lanes': []}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as directory:
                backend = Backend()
                with self.assertRaises(ValueError):
                    self.run_fixture(directory, backend, **kwargs)
                self.assertEqual(backend.instances, [])

    def test_gate_thirty_and_visibility_budget_are_recorded_and_forwarded(self):
        backend = Backend()
        original = cluster.protocol.roll_one
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(cluster.protocol, 'roll_one', wraps=original) as roll:
            result = self.run_fixture(directory, backend, nodes=['worker-0'], lanes=['primary'],
                                      gate_timeout=30, visibility_timeout=30)
        self.assertEqual(result['gate_timeout'], 30)
        self.assertEqual(result['visibility_timeout'], 30)
        self.assertEqual(backend.instances[0]['gate_timeout'], 30)
        self.assertEqual(roll.call_args.kwargs['visibility_timeout'], 30)

    def test_control_timeout_is_recorded_and_passed_to_each_operation(self):
        backend = Backend()
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_fixture(directory, backend, control_timeout=30)
        self.assertEqual(result['control_timeout'], 30)
        self.assertTrue(all(value['control_timeout'] == 30 for value in backend.instances))

    def test_control_timeout_defaults_and_cli_bounds_preserve_old_helpers(self):
        arguments = ['rollout_cache_age.py', '--action', 'hold', '--control', '/control']
        with mock.patch.object(cluster.protocol.sys, 'argv', arguments), \
                mock.patch.object(cluster.protocol, 'remote') as remote:
            cluster.protocol.main()
            self.assertEqual(remote.call_args.args[0].control_timeout, 10)
        with mock.patch.object(cluster.protocol.sys, 'argv', arguments + ['--control-timeout', '60']), \
                mock.patch.object(cluster.protocol, 'remote') as remote:
            cluster.protocol.main()
            self.assertEqual(remote.call_args.args[0].control_timeout, 60)
        for value in ('0', '-1', '61', 'inf', 'nan'):
            with self.subTest(value=value), \
                    mock.patch.object(cluster.protocol.sys, 'argv', arguments + ['--control-timeout', value]), \
                    mock.patch.object(cluster.protocol, 'remote') as remote, \
                    mock.patch.object(cluster.protocol.sys, 'stderr', io.StringIO()):
                with self.assertRaises(SystemExit):
                    cluster.protocol.main()
                remote.assert_not_called()

    def test_remote_control_get_uses_configured_timeout_or_legacy_default(self):
        config = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                  'broker_url': 'http://fixture', 'broker_token': 'fixture-only'}
        for configured in (None, 30):
            with self.subTest(configured=configured), tempfile.TemporaryDirectory() as directory:
                arguments = SimpleNamespace(control=directory, host='fixture-host', lane='primary',
                                            action='observe', operation='fixture-operation')
                if configured is not None:
                    arguments.control_timeout = configured
                opener = mock.Mock()
                opener.open.return_value = io.StringIO('{"draining_hosts": []}')
                def read(path):
                    return config if Path(path).name == 'config.json' else {}
                with mock.patch.object(cluster.protocol, 'read_json', side_effect=read), \
                        mock.patch.object(cluster.protocol.urllib.request, 'build_opener', return_value=opener), \
                        redirect_stdout(io.StringIO()):
                    cluster.protocol.remote(arguments)
                self.assertEqual(opener.open.call_args.kwargs['timeout'], 10 if configured is None else configured)

    def test_argument_target_detection_rejects_incomplete_or_ambiguous_identity(self):
        identity = copy.deepcopy(IDENTITY)
        identity['argv'][-1] = '30'
        self.assertTrue(cluster.target_age(identity))
        self.assertFalse(cluster.target_age({'pid': 10, 'argv': identity['argv']}))
        identity['argv'].extend(['--cache-max-age', '3'])
        self.assertFalse(cluster.target_age(identity))

    def rpc(self, directory):
        return cluster.BridgeRPC(control='/control', job='fixture-job', node='worker-0', lane='primary',
                                 remote_script='/control/helper/rollout_cache_age.py', ssh_prefix='copy-',
                                 output=Path(directory) / 'rollout.json', operation='fixture-operation')

    def test_ssh_alias_preserves_logical_target_for_master_actions_without_port_override(self):
        with tempfile.TemporaryDirectory() as directory:
            command = self.rpc(directory).command('master-0', 'drain', IDENTITY)
        self.assertNotIn('-p', command)
        self.assertEqual(command[-2], 'copy-master-0')
        remote = shlex.split(command[-1])
        self.assertEqual(remote[remote.index('--host') + 1], 'job-fixture-job-worker-0')
        self.assertEqual(remote[remote.index('--lane') + 1], 'primary')
        self.assertEqual(json.loads(remote[remote.index('--identity-json') + 1]), IDENTITY)

    def test_control_timeout_reaches_helper_and_ssh_wait_covers_each_action_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.rpc(directory)
            rpc.control_timeout, rpc.gate_timeout = 30, 20
            for action, expected in (('inspect', 50), ('drain', 50), ('release', 50),
                                     ('gate', 70), ('replace', 120)):
                with self.subTest(action=action), mock.patch.object(rpc, 'execute', return_value={}) as execute:
                    rpc('worker-0', action, lane='primary')
                    self.assertEqual(execute.call_args.args[-1], expected)
                    command = shlex.split(execute.call_args.args[2][-1])
                    self.assertEqual(command[command.index('--control-timeout') + 1], '30.0')

    def test_rpc_failure_is_logged_and_never_repeated(self):
        for result in (subprocess.CompletedProcess([], 255, '', 'fixture disconnect'),
                       subprocess.CompletedProcess([], 0, 'RESULT bad json\n', ''),
                       subprocess.TimeoutExpired([], 1, output=b'partial', stderr=b'fixture timeout')):
            with self.subTest(result=result), tempfile.TemporaryDirectory() as directory:
                rpc = self.rpc(directory)
                if isinstance(result, BaseException):
                    patch = mock.patch.object(cluster.subprocess, 'run', side_effect=result)
                else:
                    patch = mock.patch.object(cluster.subprocess, 'run', return_value=result)
                with patch as run:
                    with self.assertRaises(RuntimeError):
                        rpc('worker-0', 'replace', lane='primary')
                    self.assertEqual(run.call_count, 1)
                paths = list(rpc.runtime.glob('*.json'))
                self.assertEqual(len(paths), 1)
                self.assertEqual(paths[0].stat().st_mode & 0o777, 0o600)
                self.assertEqual(read_json(paths[0])['action'], 'replace')

    def test_hold_is_released_by_stdin_close_even_when_operation_fails(self):
        process = mock.Mock()
        process.stdin = mock.Mock()
        process.stdout = io.StringIO('RESULT {"locked": true}\n')
        process.stderr = io.StringIO('')
        process.returncode = 0
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(cluster.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cluster.select, 'select', return_value=([process.stdout], [], [])):
            rpc = self.rpc(directory)
            with self.assertRaisesRegex(RuntimeError, 'fixture operation failed'):
                with rpc.hold():
                    raise RuntimeError('fixture operation failed')
        process.stdin.close.assert_called_once()
        process.wait.assert_called_once_with(timeout=10)
        process.kill.assert_not_called()
        process.terminate.assert_not_called()

    def test_hold_exit_timeout_never_sends_a_signal(self):
        process = mock.Mock()
        process.stdin = mock.Mock()
        process.stdout = io.StringIO('RESULT {"locked": true}\n')
        process.stderr = io.StringIO('')
        process.wait.side_effect = subprocess.TimeoutExpired([], 10)
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(cluster.subprocess, 'Popen', return_value=process), \
                mock.patch.object(cluster.select, 'select', return_value=([process.stdout], [], [])):
            with self.assertRaisesRegex(RuntimeError, 'exit not confirmed'):
                with self.rpc(directory).hold():
                    pass
        process.stdin.close.assert_called_once()
        process.kill.assert_not_called()
        process.terminate.assert_not_called()

    def test_health_probe_accepts_real_helper_identity_including_extra_state(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory)
            expected = cluster.protocol.proc_identity(os.getpid())
            self.assertIn('state', expected)
            stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            write_json(control / 'nodes' / (socket.gethostname() + '.json'),
                       {'pid': os.getpid(), 'updated': stamp, 'threads': 64})
            namespace = {'HELPER': cluster.protocol.__file__, 'CONTROL': str(control),
                         'HOST': socket.gethostname(), 'LANE': 'primary', 'EXPECTED': expected}
            output = io.StringIO()
            with mock.patch.object(sys, 'path', list(sys.path)), \
                    mock.patch.object(sys, 'dont_write_bytecode', sys.dont_write_bytecode), redirect_stdout(output):
                exec(cluster.HEALTH_SCRIPT, namespace)
            result = json.loads(output.getvalue().split('RESULT ', 1)[1])
            self.assertTrue(result['healthy'])
            self.assertEqual(result['helper_sha256'], cluster.source_versions()['rollout_cache_age.py'])


if __name__ == '__main__':
    unittest.main()
