"""Local immutable staging and mocked rollout checks; never contact the cluster."""
from contextlib import contextmanager, redirect_stdout
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import deploy_directory_fastpath as deploy
from migration_io import read_json, write_json


class Fixture:
    def __init__(self, root):
        self.control = root / 'control'
        self.source = root / 'source'
        self.control.mkdir()
        self.source.mkdir()
        for name in deploy.RUNTIME_FILES:
            (self.control / name).write_text('# original ' + name + '\n')
        for name in deploy.HELPER_FILES:
            (self.source / name).write_text('# helper ' + name + '\n')
        (self.source / 'migration_worker.py').write_text('# open-first worker\n')
        write_json(self.control / 'config.json', {
            'control': str(self.control), 'source': '/share/project/eai_pwm',
            'destination': '/share-mixed/project/eai_pwm', 'job': 'fixture-job',
        })
        write_json(self.control / 'state.json', {'status': 'scanning'})

    def prepare(self):
        with mock.patch.object(deploy, '__file__', str(self.source / 'deploy_directory_fastpath.py')):
            self.result = deploy.prepare(self.control)
        self.bundle = Path(self.result['bundle'])
        return self.bundle


class FakeRPC:
    """Record holds/inspection; the protocol transaction is mocked separately."""
    def __init__(self, backend, **kwargs):
        self.backend = backend
        self.operation = kwargs['operation']
        self.pair = kwargs['node'], kwargs['lane']
        self.wrapper = str(Path(kwargs['remote_script']).parent / 'migration_cached_worker.py')
        backend.instances.append(kwargs)

    def __call__(self, node, action, **kwargs):
        self.backend.events.append((self.pair, action))
        if action != 'inspect':
            raise AssertionError('unexpected direct mutation: ' + action)
        wrapper = (self.wrapper if self.pair in self.backend.upgraded else
                   getattr(self.backend, 'previous_wrapper', '/old/migration_cached_worker.py'))
        return {'identity': {'pid': 12, 'starttime_ticks': '34',
                             'argv': ['python3', wrapper, self.pair[1]]}}

    def health(self, expected):
        self.backend.events.append((self.pair, 'health'))
        return {'healthy': self.backend.healthy}

    @contextmanager
    def hold(self):
        self.backend.events.append((self.pair, 'hold'))
        self.backend.holds += 1
        if self.backend.holds != 1:
            raise AssertionError('overlapping rollout holds')
        try:
            yield {'locked': True}
        finally:
            self.backend.holds -= 1
            self.backend.events.append((self.pair, 'unhold'))


class Backend:
    def __init__(self, outcomes=None, upgraded=(), healthy=True):
        self.instances, self.events = [], []
        self.outcomes = list(outcomes or [])
        self.upgraded, self.healthy, self.holds = set(upgraded), healthy, 0

    def factory(self, **kwargs):
        return FakeRPC(self, **kwargs)

    def roll(self, rpc, node, lane, **kwargs):
        self.events.append(((node, lane), 'roll_one'))
        outcome = self.outcomes.pop(0) if self.outcomes else {'status': 'complete'}
        if isinstance(outcome, Exception):
            raise outcome
        if outcome['status'] == 'complete':
            self.upgraded.add((node, lane))
            outcome = dict(outcome, replacement={'replacement': rpc(node, 'inspect')['identity']})
        return outcome


class DirectoryFastpathTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='directory-fastpath-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fixture = Fixture(self.root)

    def rollout(self, backend, nodes=('worker-0',), lanes=('primary', 'supplement'), **kwargs):
        with mock.patch.object(deploy, 'DirectoryRPC', side_effect=backend.factory), \
                mock.patch.object(deploy.protocol, 'roll_one', side_effect=backend.roll) as roll, \
                redirect_stdout(io.StringIO()):
            result = deploy.rollout(self.fixture.control, self.fixture.bundle, nodes, lanes, **kwargs)
        return result, roll

    def test_import_has_no_rollout_or_process_side_effects(self):
        repository = str(Path(deploy.__file__).resolve().parent)
        script = (
            'from unittest import mock\n'
            'with mock.patch("subprocess.Popen",side_effect=AssertionError("launch")), '
            'mock.patch("os.kill",side_effect=AssertionError("signal")), '
            'mock.patch("shutil.copyfile",side_effect=AssertionError("stage")):\n'
            ' import deploy_directory_fastpath\n'
        )
        result = subprocess.run([sys.executable, '-c', script], cwd=self.root,
                                env=dict(os.environ, PYTHONPATH=repository,
                                         PYTHONDONTWRITEBYTECODE='1'),
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(set(self.root.iterdir()), {self.fixture.control, self.fixture.source})

    def test_prepare_stages_only_worker_change_and_exact_full_manifest(self):
        original = {name: (self.fixture.control / name).read_bytes()
                    for name in deploy.RUNTIME_FILES}
        bundle = self.fixture.prepare()
        cfg, manifest = deploy.verify_bundle(self.fixture.control, bundle)
        self.assertEqual(set(manifest), set(deploy.RUNTIME_FILES + deploy.HELPER_FILES +
                                            ('engine-transition.json',)))
        self.assertTrue(self.fixture.result['prepared_only'])
        self.assertTrue(self.fixture.result['worker_changed'])
        for name in deploy.RUNTIME_FILES:
            self.assertEqual((self.fixture.control / name).read_bytes(), original[name])
            expected = (self.fixture.source / name).read_bytes() if name == 'migration_worker.py' else original[name]
            self.assertEqual((bundle / name).read_bytes(), expected)
        self.assertEqual(cfg['job'], 'fixture-job')
        self.assertFalse(any(path.name.startswith('rollout-') for path in bundle.iterdir()))

    def test_verify_forwards_transition_validator_arguments_in_required_order(self):
        bundle = self.fixture.prepare()
        with mock.patch.object(deploy.protocol, 'validate_engine_transition', return_value={}) as validate:
            deploy.verify_bundle(self.fixture.control, bundle)
        validate.assert_called_once_with(self.fixture.control.resolve(), bundle.resolve(),
                                         bundle.resolve() / 'engine-transition.json',
                                         self.fixture.control.resolve())

    def test_prepare_rejects_stop_before_creating_bundle(self):
        (self.fixture.control / 'STOP').touch()
        with self.assertRaisesRegex(ValueError, 'STOP'):
            self.fixture.prepare()
        self.assertEqual(list(self.fixture.control.glob('directory-open-first-*')), [])

    def test_verify_rejects_changed_staged_source_and_wrong_scope(self):
        bundle = self.fixture.prepare()
        source = bundle / 'migration_worker.py'
        original = source.read_bytes()
        source.write_text('# unexpected later mutation\n')
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            deploy.verify_bundle(self.fixture.control, bundle)
        source.write_bytes(original)
        cfg = read_json(self.fixture.control / 'config.json')
        write_json(self.fixture.control / 'config.json', dict(cfg, destination='/different'))
        with self.assertRaisesRegex(ValueError, 'copy scope'):
            deploy.verify_bundle(self.fixture.control, bundle)

    def test_verify_rejects_changed_old_engine_or_nested_bundle(self):
        bundle = self.fixture.prepare()
        (self.fixture.control / 'migration_scan.py').write_text('# changed old engine\n')
        with self.assertRaisesRegex(RuntimeError, 'engine differs'):
            deploy.verify_bundle(self.fixture.control, bundle)
        with self.assertRaisesRegex(ValueError, 'direct immutable'):
            deploy.verify_bundle(self.fixture.control, bundle / 'directory-open-first-nested')

    def test_transition_digest_is_part_of_bundle_manifest(self):
        bundle = self.fixture.prepare()
        transition = bundle / 'engine-transition.json'
        original = json.loads(transition.read_text())
        transition.write_text(json.dumps(dict(original, new_sha256='0' * 64)))
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            deploy.verify_bundle(self.fixture.control, bundle)

    def rpc(self, **kwargs):
        return deploy.DirectoryRPC(control=str(self.fixture.control), job='fixture-job',
                                   node='worker-0', lane='primary',
                                   remote_script='/control/path with space/rollout_cache_age.py',
                                   ssh_prefix='copy-', output=str(self.root / 'output.json'),
                                   operation='fixture-operation', **kwargs)

    def test_ssh_master_and_worker_disable_auth_agent_including_jump(self):
        rpc = self.rpc()
        master = rpc.command('master-0', 'drain')
        self.assertEqual(master[-2], 'copy-cluster-entry-1e56')
        self.assertIn('IdentityAgent=none', master)
        worker = rpc.command('worker-0', 'inspect')
        self.assertEqual(worker[-2], 'copy-worker-0')
        jump = next(value.partition('=')[2] for value in worker if value.startswith('ProxyCommand='))
        self.assertIn('IdentityAgent=none', shlex.split(jump))
        self.assertIn('%h:%p', shlex.split(jump))
        with self.assertRaisesRegex(ValueError, 'unknown physical'):
            rpc.ssh_command('worker-12', 'true')

    def test_alias_transport_preserves_user_agent_and_uses_only_known_aliases(self):
        rpc = self.rpc(ssh_aliases=True)
        for node in ('master-0', 'worker-0', 'worker-10'):
            with self.subTest(node=node):
                command = rpc.command(node, 'inspect')
                self.assertEqual(command[-2], node)
                self.assertIn('BatchMode=yes', command)
                self.assertFalse(any(value.startswith(('IdentityAgent=', 'ProxyCommand='))
                                     for value in command))
                remote = shlex.split(command[-1])
                self.assertEqual(remote[remote.index('--host') + 1], 'job-fixture-job-worker-0')
        with self.assertRaisesRegex(ValueError, 'unknown physical'):
            rpc.ssh_command('-oProxyCommand=anything', 'true')
        with self.assertRaisesRegex(ValueError, 'boolean'):
            self.rpc(ssh_aliases='true')

    def test_alias_rollout_records_transport_and_retains_guard_protocol(self):
        self.fixture.prepare()
        backend = Backend()
        result, roll = self.rollout(backend, ssh_aliases=True)
        self.assertEqual(result['status'], 'complete')
        self.assertTrue(all(instance['ssh_aliases'] is True for instance in backend.instances))
        self.assertEqual(read_json(Path(result['output']))['transport'], 'ssh-aliases')
        self.assertEqual(roll.call_args.kwargs,
                         {'gate_timeout': 30, 'max_age': 30, 'visibility_timeout': 30})

    def test_only_replace_appends_explicit_wrapper_and_transition_without_shell_breakage(self):
        rpc = self.rpc()
        identity = {'pid': 1, 'starttime_ticks': '2', 'argv': ['python3', 'with space']}
        for action in ('hold', 'inspect', 'drain', 'gate', 'release', 'observe', 'replace'):
            with self.subTest(action=action):
                command = shlex.split(rpc.command('worker-0', action, identity)[-1])
                self.assertEqual(command[command.index('--action') + 1], action)
                self.assertEqual(json.loads(command[command.index('--identity-json') + 1]), identity)
                if action == 'replace':
                    self.assertEqual(command[command.index('--wrapper') + 1],
                                     '/control/path with space/migration_cached_worker.py')
                    self.assertEqual(command[command.index('--engine-manifest') + 1],
                                     '/control/path with space/engine-transition.json')
                else:
                    self.assertNotIn('--wrapper', command)
                    self.assertNotIn('--engine-manifest', command)

    def test_gate_rechecks_only_three_readonly_windows_with_same_identity(self):
        rpc = self.rpc()
        identity = {'pid': 12, 'starttime_ticks': '34', 'argv': ['python3', 'worker']}
        waiting = {'ready': False,
                   'reason': 'lane not idle within gate; payload retained'}
        with mock.patch.object(deploy.BridgeRPC, '__call__',
                               return_value=waiting) as bridge, \
                redirect_stdout(io.StringIO()) as output:
            result = rpc('worker-0', 'gate', lane='primary', identity=identity)
        self.assertIs(result, waiting)
        self.assertEqual(bridge.call_args_list, [mock.call(
            'worker-0', 'gate', lane='primary', identity=identity)] * 3)
        observations = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([item['observation_window'] for item in observations], [1, 2, 3])
        self.assertTrue(all(item['status'] == 'waiting_existing_tasks'
                            for item in observations))
        self.assertEqual(rpc.operation, 'fixture-operation')

    def test_gate_returns_immediately_when_second_window_ready(self):
        rpc = self.rpc()
        waiting = {'ready': False,
                   'reason': 'lane not idle within gate; payload retained'}
        ready = {'ready': True, 'active_count': 0}
        with mock.patch.object(deploy.BridgeRPC, '__call__',
                               side_effect=[waiting, ready]) as bridge, \
                redirect_stdout(io.StringIO()):
            result = rpc('worker-0', 'gate')
        self.assertIs(result, ready)
        self.assertEqual(bridge.call_count, 2)

    def test_gate_terminal_unexpected_and_similar_reasons_never_retry(self):
        responses = (
            {'ready': False, 'reason': 'STOP or terminal state'},
            {'ready': False, 'reason': 'worker identity changed'},
            {'ready': False, 'reason': 'lane not idle within gate; payload retained '},
            {'ready': False},
            {'reason': 'lane not idle within gate; payload retained'},
            {'ready': 0, 'reason': 'lane not idle within gate; payload retained'},
        )
        for response in responses:
            with self.subTest(response=response), \
                    mock.patch.object(deploy.BridgeRPC, '__call__',
                                      return_value=response) as bridge, \
                    redirect_stdout(io.StringIO()) as output:
                self.assertIs(self.rpc()('worker-0', 'gate'), response)
            bridge.assert_called_once_with('worker-0', 'gate', lane=None, identity=None)
            self.assertEqual(output.getvalue(), '')

    def test_gate_transport_failure_is_not_retried(self):
        failure = RuntimeError('gate RPC timeout; inspect before retry')
        with mock.patch.object(deploy.BridgeRPC, '__call__',
                               side_effect=failure) as bridge:
            with self.assertRaises(RuntimeError) as raised:
                self.rpc()('worker-0', 'gate')
        self.assertIs(raised.exception, failure)
        bridge.assert_called_once()

    def test_replace_success_or_exception_is_never_repeated(self):
        waiting = {'ready': False,
                   'reason': 'lane not idle within gate; payload retained'}
        with mock.patch.object(deploy.BridgeRPC, '__call__',
                               return_value=waiting) as bridge:
            self.assertIs(self.rpc()('worker-0', 'replace'), waiting)
        bridge.assert_called_once_with('worker-0', 'replace', lane=None, identity=None)
        failure = RuntimeError('ambiguous replacement transport failure')
        with mock.patch.object(deploy.BridgeRPC, '__call__',
                               side_effect=failure) as bridge:
            with self.assertRaises(RuntimeError) as raised:
                self.rpc()('worker-0', 'replace')
        self.assertIs(raised.exception, failure)
        bridge.assert_called_once()

    def test_two_lane_rollout_delegates_exact_guard_protocol_and_checks_health(self):
        self.fixture.prepare()
        backend = Backend()
        result, roll = self.rollout(backend)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['lanes_complete'], 2)
        self.assertEqual(roll.call_count, 2)
        self.assertEqual(roll.call_args.kwargs,
                         {'gate_timeout': 30, 'max_age': 30, 'visibility_timeout': 30})
        self.assertTrue(all(instance['control_timeout'] == 30 for instance in backend.instances))
        self.assertEqual(backend.holds, 0)
        self.assertEqual(sum(action == 'health' for _, action in backend.events), 2)
        self.assertEqual(len({instance['operation'] for instance in backend.instances}), 2)

    def test_busy_gate_is_partial_and_retry_skips_finished_lane(self):
        self.fixture.prepare()
        backend = Backend(outcomes=[{'status': 'waiting_payload'}, {'status': 'complete'}])
        first, _ = self.rollout(backend)
        self.assertEqual(first['status'], 'partial_busy')
        self.assertEqual(first['lanes_complete'], 1)
        second, roll = self.rollout(backend)
        self.assertEqual(second['status'], 'complete')
        self.assertEqual(roll.call_count, 1)
        record = read_json(Path(second['output']))
        self.assertEqual([entry['status'] for entry in record['entries']],
                         ['complete', 'already_upgraded'])

    def test_ambiguous_transaction_stops_without_next_lane_and_records_attention(self):
        self.fixture.prepare()
        backend = Backend(outcomes=[RuntimeError('ambiguous replacement')])
        with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
            self.rollout(backend)
        self.assertEqual(len(backend.instances), 1)
        self.assertEqual(backend.holds, 0)
        record_path = next(self.fixture.bundle.glob('rollout-*.json'))
        record = read_json(record_path)
        self.assertEqual(record['status'], 'inspect_before_retry')
        self.assertEqual(len(record['entries']), 1)

    def test_unhealthy_already_upgraded_lane_stops_before_transaction(self):
        self.fixture.prepare()
        backend = Backend(upgraded={('worker-0', 'primary')}, healthy=False)
        with self.assertRaisesRegex(RuntimeError, 'unhealthy'):
            self.rollout(backend)
        self.assertNotIn('roll_one', [action for _, action in backend.events])
        self.assertEqual(len(backend.instances), 1)

    def test_stop_or_terminal_prevents_rpc_creation(self):
        self.fixture.prepare()
        backend = Backend()
        write_json(self.fixture.control / 'state.json', {'status': 'complete'})
        with self.assertRaisesRegex(RuntimeError, 'terminal'):
            self.rollout(backend)
        self.assertEqual(backend.instances, [])

    def prepare_native(self):
        previous = self.fixture.prepare()
        (self.fixture.source / 'migration_worker.py').write_text('# native-safe worker\n')
        (self.fixture.source / 'migration_cached_worker.py').write_text('# policy-aware wrapper\n')
        (self.fixture.source / 'migration_linux_open.py').write_text('# native syscall helper\n')
        with mock.patch.object(deploy, '__file__',
                               str(self.fixture.source / 'deploy_directory_fastpath.py')):
            result = deploy.prepare(self.fixture.control, previous_bundle=previous)
        self.fixture.bundle = Path(result['bundle'])
        return previous, self.fixture.bundle, result

    def test_native_prepare_uses_exact_previous_runtime_and_explicit_changed_sources(self):
        previous, bundle, result = self.prepare_native()
        cfg, manifest = deploy.verify_bundle(self.fixture.control, bundle)
        self.assertEqual(set(manifest), set(deploy.RUNTIME_FILES + deploy.HELPER_FILES +
                                            deploy.NATIVE_FILES + ('engine-transition.json',)))
        self.assertEqual(read_json(bundle / 'worker-directory-policy.json'), deploy.NATIVE_POLICY)
        transition = read_json(bundle / 'engine-transition.json')
        self.assertEqual(transition['previous_bundle'], str(previous))
        self.assertEqual(transition['old_sha256'], deploy.digest(previous / 'migration_worker.py'))
        self.assertEqual(transition['new_sha256'], deploy.digest(bundle / 'migration_worker.py'))
        self.assertEqual(result['previous_bundle'], str(previous))
        self.assertTrue(result['prepared_only'])
        for name in deploy.RUNTIME_FILES:
            expected = self.fixture.source if name in ('migration_worker.py',
                                                       'migration_cached_worker.py') else previous
            self.assertEqual((bundle / name).read_bytes(), (expected / name).read_bytes())
        self.assertEqual((bundle / 'migration_linux_open.py').read_bytes(),
                         (self.fixture.source / 'migration_linux_open.py').read_bytes())
        self.assertEqual(cfg['job'], 'fixture-job')
        self.assertFalse(list(bundle.glob('rollout-*.json')))

    def test_native_verify_validates_previous_then_passes_exact_base_to_engine_gate(self):
        previous, bundle, _ = self.prepare_native()
        with mock.patch.object(deploy.protocol, 'validate_engine_transition', return_value={}) as gate:
            deploy.verify_bundle(self.fixture.control, bundle)
        self.assertEqual(gate.call_args_list, [
            mock.call(self.fixture.control, previous, previous / 'engine-transition.json',
                      self.fixture.control),
            mock.call(previous, bundle, bundle / 'engine-transition.json', self.fixture.control)])

    def test_destination_path_prepare_preserves_every_data_engine_file(self):
        _, previous, _ = self.prepare_native()
        with mock.patch.object(deploy, '__file__',
                               str(self.fixture.source / 'deploy_directory_fastpath.py')):
            result = deploy.prepare(self.fixture.control, previous_bundle=previous,
                                    destination_opath=True)
        bundle = Path(result['bundle'])
        deploy.verify_bundle(self.fixture.control, bundle)
        self.assertFalse(result['worker_changed'])
        self.assertEqual(read_json(bundle / 'worker-directory-policy.json'), deploy.PATH_POLICY)
        for name in deploy.protocol.ENGINE_FILES:
            self.assertEqual((bundle / name).read_bytes(), (previous / name).read_bytes())
        transition = read_json(bundle / 'engine-transition.json')
        self.assertEqual(transition['scope'], deploy.PATH_POLICY['scope'])
        self.assertEqual(set(transition['launcher_sha256']), {
            'migration_cached_worker.py', 'migration_linux_open.py', 'worker-directory-policy.json'})

    def test_destination_path_requires_previous_bundle_before_staging(self):
        with self.assertRaisesRegex(ValueError, 'exact previous'):
            deploy.prepare(self.fixture.control, destination_opath=True)
        self.assertFalse(list(self.fixture.control.glob('directory-open-first-*')))

    def test_native_verify_rejects_policy_manifest_base_or_wrapper_tampering(self):
        previous, bundle, _ = self.prepare_native()
        for name in ('migration_cached_worker.py', 'migration_linux_open.py',
                     'worker-directory-policy.json'):
            with self.subTest(name=name):
                path = bundle / name
                original = path.read_bytes()
                path.write_text('# tampered\n')
                with self.assertRaisesRegex(ValueError, 'digest mismatch'):
                    deploy.verify_bundle(self.fixture.control, bundle)
                path.write_bytes(original)
        old = previous / 'migration_cached_worker.py'
        old.write_text('# mutated previous wrapper\n')
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            deploy.verify_bundle(self.fixture.control, bundle)

    def rewrite_manifest(self, bundle):
        manifest = read_json(bundle / 'source-sha256.json')
        # Static test fixture publication: match the exact canonical digests.
        (bundle / 'source-sha256.json').write_text(json.dumps(
            {name: deploy.digest(bundle / name) for name in manifest}))

    def test_native_policy_and_previous_binding_cannot_be_omitted_or_redirected(self):
        previous, bundle, _ = self.prepare_native()
        transition = bundle / 'engine-transition.json'
        original = json.loads(transition.read_text())
        for value in (None, str(self.fixture.control), str(bundle), '/outside/previous'):
            with self.subTest(previous=value):
                data = dict(original)
                if value is None:
                    data.pop('previous_bundle')
                else:
                    data['previous_bundle'] = value
                transition.write_text(json.dumps(data))
                self.rewrite_manifest(bundle)
                with self.assertRaisesRegex(ValueError, 'previous bundle'):
                    deploy.verify_bundle(self.fixture.control, bundle)
        transition.write_text(json.dumps(original))
        (bundle / 'worker-directory-policy.json').write_text(json.dumps({'scope': 'wrong'}))
        self.rewrite_manifest(bundle)
        with self.assertRaisesRegex(ValueError, 'directory policy'):
            deploy.verify_bundle(self.fixture.control, bundle)

    def test_native_runtime_still_permits_only_worker_change_in_six_file_engine(self):
        previous, bundle, _ = self.prepare_native()
        (bundle / 'migration_scan.py').write_text('# unrelated engine change\n')
        self.rewrite_manifest(bundle)
        with self.assertRaisesRegex(RuntimeError, 'engine differs'):
            deploy.verify_bundle(self.fixture.control, bundle)

    def test_native_rollout_requires_actual_previous_wrapper_and_keeps_guard_parameters(self):
        previous, bundle, _ = self.prepare_native()
        backend = Backend()
        backend.previous_wrapper = str(previous / 'migration_cached_worker.py')
        result, roll = self.rollout(backend)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(roll.call_count, 2)
        self.assertEqual(roll.call_args.kwargs,
                         {'gate_timeout': 30, 'max_age': 30, 'visibility_timeout': 30})
        second, skip = self.rollout(backend)
        self.assertEqual(second['status'], 'complete')
        skip.assert_not_called()

    def test_native_wrong_live_base_stops_before_replacement(self):
        self.prepare_native()
        backend = Backend()
        with self.assertRaisesRegex(RuntimeError, 'live wrapper.*previous bundle'):
            self.rollout(backend)
        self.assertNotIn('roll_one', [action for _, action in backend.events])
        self.assertEqual(backend.holds, 0)

    def test_native_prepare_rejects_wrong_previous_before_creating_successor(self):
        previous = self.fixture.prepare()
        before = set(self.fixture.control.iterdir())
        with self.assertRaisesRegex(ValueError, 'direct immutable'):
            deploy.prepare(self.fixture.control, previous_bundle=self.fixture.control)
        self.assertEqual(set(self.fixture.control.iterdir()), before)

    def native_root(self):
        control, source = self.fixture.control, self.fixture.source
        (control / 'migration_linux_open.py').write_text('# frozen native syscall helper\n')
        write_json(control / 'worker-directory-policy.json', deploy.NATIVE_POLICY)
        write_json(control / 'source-sha256.json', {
            name: deploy.digest(control / name)
            for name in deploy.RUNTIME_FILES + deploy.NATIVE_FILES})
        (source / 'migration_cached_worker.py').write_text('# guarded O_PATH wrapper\n')
        (source / 'migration_linux_open.py').write_text('# reviewed O_PATH helper\n')
        return control

    def prepare_root_path(self):
        with mock.patch.object(deploy, '__file__',
                               str(self.fixture.source / 'deploy_directory_fastpath.py')):
            result = deploy.prepare(self.fixture.control,
                                    previous_bundle=self.fixture.control,
                                    destination_opath=True)
        self.fixture.bundle = Path(result['bundle'])
        return self.fixture.bundle, result

    def test_cold_native_root_path_canary_binds_previous_sources_and_keeps_engine_exact(self):
        control = self.native_root()
        original = {name: (control / name).read_bytes()
                    for name in deploy.RUNTIME_FILES + deploy.NATIVE_FILES}
        bundle, result = self.prepare_root_path()
        deploy.verify_bundle(control, bundle)
        self.assertTrue(result['prepared_only'])
        self.assertFalse(result['worker_changed'])
        transition = read_json(bundle / 'engine-transition.json')
        self.assertEqual(transition['previous_bundle'], str(control))
        self.assertEqual(transition['previous_root_sha256'], deploy.verify_native_root(control))
        for name, payload in original.items():
            self.assertEqual((control / name).read_bytes(), payload)
        for name in deploy.protocol.ENGINE_FILES:
            self.assertEqual((bundle / name).read_bytes(), original[name])
        backend = Backend()
        backend.previous_wrapper = str(control / 'migration_cached_worker.py')
        result, roll = self.rollout(backend, lanes=('supplement',), ssh_aliases=True)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(roll.call_count, 1)

    def test_cold_root_requires_every_runtime_digest_before_staging(self):
        control = self.native_root()
        manifest = read_json(control / 'source-sha256.json')
        for name in deploy.RUNTIME_FILES + deploy.NATIVE_FILES:
            with self.subTest(missing=name):
                write_json(control / 'source-sha256.json',
                           {key: value for key, value in manifest.items() if key != name})
                with self.assertRaisesRegex(ValueError, 'previous bundle.*incomplete'):
                    self.prepare_root_path()
                self.assertFalse(list(control.glob('directory-open-first-*')))

    def test_cold_root_rejects_source_or_policy_change_before_staging(self):
        control = self.native_root()
        worker = control / 'migration_worker.py'
        worker.write_text('# not the manifested running engine\n')
        with self.assertRaisesRegex(ValueError, 'previous bundle source digest'):
            self.prepare_root_path()
        self.assertFalse(list(control.glob('directory-open-first-*')))
        for policy in (deploy.PATH_POLICY, {'scope': 'unknown'}):
            with self.subTest(policy=policy):
                self.native_root()
                write_json(control / 'worker-directory-policy.json', policy)
                manifest = read_json(control / 'source-sha256.json')
                manifest['worker-directory-policy.json'] = deploy.digest(
                    control / 'worker-directory-policy.json')
                write_json(control / 'source-sha256.json', manifest)
                with self.assertRaisesRegex(ValueError, 'native read-only policy'):
                    self.prepare_root_path()
                self.assertFalse(list(control.glob('directory-open-first-*')))

    def test_cold_root_binding_survives_later_rewrite_of_root_source_manifest(self):
        control = self.native_root()
        bundle, _ = self.prepare_root_path()
        (control / 'migration_state_cache.py').write_text('# later root runtime\n')
        manifest = read_json(control / 'source-sha256.json')
        manifest['migration_state_cache.py'] = deploy.digest(control / 'migration_state_cache.py')
        write_json(control / 'source-sha256.json', manifest)
        with self.assertRaisesRegex(ValueError, 'root previous bundle binding'):
            deploy.verify_bundle(control, bundle)

    def test_cold_root_binding_cannot_be_omitted(self):
        control = self.native_root()
        bundle, _ = self.prepare_root_path()
        transition = read_json(bundle / 'engine-transition.json')
        transition.pop('previous_root_sha256')
        (bundle / 'engine-transition.json').write_text(json.dumps(transition))
        self.rewrite_manifest(bundle)
        with self.assertRaisesRegex(ValueError, 'root previous bundle binding'):
            deploy.verify_bundle(control, bundle)

    def test_cold_root_runtime_symlink_is_rejected_even_if_contents_match(self):
        control = self.native_root()
        path = control / 'migration_worker.py'
        external = self.root / 'same-worker.py'
        path.rename(external)
        path.symlink_to(external)
        with self.assertRaisesRegex(ValueError, 'root previous bundle source digest'):
            self.prepare_root_path()
        self.assertFalse(list(control.glob('directory-open-first-*')))


if __name__ == '__main__':
    unittest.main()
