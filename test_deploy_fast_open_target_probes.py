"""Local-only safety tests: no real process is signalled or service restarted."""
import hashlib
import io
import json
from pathlib import Path
import signal
import tempfile
import unittest
from unittest import mock

import deploy_fast_open_target_probes as deploy


class FastOpenProbeRolloutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='fast-open-probe-guard-')
        self.addCleanup(self.tmp.cleanup)
        self.control = Path(self.tmp.name) / 'control'
        self.bundle = self.control / 'new-bundle'
        self.bundle.mkdir(parents=True)
        self.node = 'worker-0'
        self.host = 'fixture-job-worker-0'
        self.cfg = {'run_id': 'fixture-copy', 'broker_token': 'private-fixture-token',
                    'source': '/share/project/eai_pwm',
                    'destination': '/share-mixed/project/eai_pwm'}
        (self.control / 'config.json').write_text(json.dumps(self.cfg))
        predecessor = self.control / 'old-bundle'
        self.old = {'pid': 777, 'starttime_ticks': '12345', 'host': self.host,
                    'control': str(self.control), 'run_id': self.cfg['run_id'],
                    'source': self.cfg['source'], 'destination': self.cfg['destination'],
                    'slots': 64, 'stop_cache_seconds': .1, 'batch_limit': 64,
                    'port': 45678, 'endpoint': 'http://172.24.102.30:45678'}
        self.old['argv'] = deploy.probe_argv(
            predecessor / 'migration_target_probe.py', self.control, self.old['port'],
            '172.24.102.30', predecessor / 'roles' / self.node / 'probe.json')
        for name in deploy.REQUIRED_SOURCES - {'expected-roles.json'}:
            (self.bundle / name).write_text('# fixture staged source\n')
        self.write_roles()
        self.hostname = mock.patch.object(deploy.socket, 'gethostname', return_value=self.host)
        self.hostname.start()
        self.addCleanup(self.hostname.stop)

    def write_roles(self, old=None):
        (self.bundle / 'expected-roles.json').write_text(json.dumps({self.node: old or self.old}))
        self.manifest()

    def manifest(self):
        entries = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in self.bundle.iterdir()
                   if path.is_file() and path.name != 'source-sha256.json'}
        (self.bundle / 'source-sha256.json').write_text(json.dumps(entries))

    def assert_no_signal(self, exception=Exception):
        with mock.patch.object(deploy.os, 'kill') as kill, \
                mock.patch.object(deploy.subprocess, 'Popen') as popen:
            with self.assertRaises(exception):
                deploy.run_node(self.control, self.bundle, self.node)
            kill.assert_not_called()
            popen.assert_not_called()

    def test_full_identity_and_pid_reuse_guards(self):
        live = dict(self.old, state='S')
        self.assertTrue(deploy.same_process(live, self.old))
        for changed in (dict(live, pid=778), dict(live, starttime_ticks='999'),
                        dict(live, argv=['unknown']), dict(live, state='Z'), {}, None):
            self.assertFalse(deploy.same_process(changed, self.old))

    def test_root_cold_probe_requires_exact_run_host_entry_and_argv(self):
        cfg = dict(self.cfg, cold_frontier_v1=True, job='fixture')
        host = 'job-fixture-' + self.node
        old = dict(self.old, host=host)
        old['argv'] = ['/usr/bin/python3', '-u', str(self.control/'migration_probe_fast_open.py'),
            '--control', str(self.control), '--port', str(old['port']), '--slots', '64',
            '--stop-cache-seconds', '0.1', '--advertise-address', '172.24.102.30',
            '--record', str(self.control/'probe-roles'/(host+'.json'))]
        self.write_roles(old)
        with mock.patch.object(deploy.socket, 'gethostname', return_value=host):
            value, _ = deploy.validate_role(self.control, self.bundle, self.node, cfg)
            self.assertEqual(value, old)
            for changes in ({'cold_frontier_v1':False}, {'job':'wrong'}):
                with self.assertRaises(ValueError):
                    deploy.validate_role(self.control, self.bundle, self.node, dict(cfg, **changes))
            changed = dict(old, argv=old['argv'][:-1]+['/wrong-record'])
            self.write_roles(changed)
            with self.assertRaises(ValueError):
                deploy.validate_role(self.control, self.bundle, self.node, cfg)

    def test_opath_policy_and_entry_are_digest_and_run_bound(self):
        self.assertEqual(deploy.new_entrypoint(self.control,self.bundle,self.cfg).name,
                         'migration_probe_fast_open.py')
        policy={'version':1,'control':str(self.control),'run_id':self.cfg['run_id'],'parent_open':'O_PATH'}
        path=self.bundle/'parent-open.json';path.write_text(json.dumps(policy))
        with self.assertRaises(ValueError):deploy.new_entrypoint(self.control,self.bundle,self.cfg)
        (self.bundle/'migration_probe_path_open.py').write_text('# fixture entry\n')
        self.manifest()
        self.assertEqual(deploy.new_entrypoint(self.control,self.bundle,self.cfg).name,
                         'migration_probe_path_open.py')
        path.write_text(json.dumps(dict(policy,run_id='wrong')))
        with self.assertRaises(ValueError):deploy.new_entrypoint(self.control,self.bundle,self.cfg)

    def test_child_path_probe_can_be_bound_for_a_guarded_reverse_rollout(self):
        old = dict(self.old, argv=list(self.old['argv']))
        old['argv'][2] = str(self.control/'old-bundle/migration_probe_path_open.py')
        self.write_roles(old)
        value, _ = deploy.validate_role(self.control,self.bundle,self.node,self.cfg)
        self.assertEqual(value,old)
        old['argv'][2] = str(self.control/'../outside/migration_probe_path_open.py')
        self.write_roles(old)
        with self.assertRaises(ValueError):
            deploy.validate_role(self.control,self.bundle,self.node,self.cfg)

    def test_batch_parent_entry_requires_exact_frozen_policy_and_health(self):
        policy = {'version': 1, 'control': str(self.control), 'run_id': self.cfg['run_id'],
                  'parent_reuse': 'request-scoped'}
        path = self.bundle / 'batch-parent.json'
        path.write_text(json.dumps(policy))
        entry = self.bundle / 'migration_probe_batch_parent.py'
        entry.write_text('# fixture entry\n')
        self.manifest()
        self.assertEqual(deploy.new_entrypoint(self.control, self.bundle, self.cfg), entry)
        path.write_text(json.dumps(dict(policy, run_id='wrong')))
        self.manifest()
        with self.assertRaises(ValueError):
            deploy.new_entrypoint(self.control, self.bundle, self.cfg)
        path.write_text(json.dumps(policy))
        (self.bundle / 'parent-open.json').write_text('{}')
        self.manifest()
        with self.assertRaises(ValueError):
            deploy.new_entrypoint(self.control, self.bundle, self.cfg)

    def test_batch_parent_predecessor_can_be_rolled_back_with_full_identity(self):
        old = dict(self.old, argv=list(self.old['argv']))
        old['argv'][2] = str(self.control / 'old-bundle/migration_probe_batch_parent.py')
        self.write_roles(old)
        value, _ = deploy.validate_role(self.control, self.bundle, self.node, self.cfg)
        self.assertEqual(value, old)

    def test_manifest_must_cover_all_files_and_exact_digests(self):
        digest = deploy.validate_sources(self.bundle)
        self.assertEqual(len(digest), 64)
        (self.bundle / 'uncovered.py').write_text('# not frozen\n')
        self.assert_no_signal(ValueError)
        (self.bundle / 'uncovered.py').unlink()
        (self.bundle / 'migration_linux_open.py').write_text('# changed\n')
        self.assert_no_signal(RuntimeError)

    def test_required_entry_and_frozen_roles_must_be_manifest_bound(self):
        manifest = json.loads((self.bundle / 'source-sha256.json').read_text())
        manifest.pop('expected-roles.json')
        (self.bundle / 'source-sha256.json').write_text(json.dumps(manifest))
        self.assert_no_signal(ValueError)

    def test_node_run_host_limits_argv_and_path_mismatches_are_nonmutating(self):
        variants = [dict(self.old, host='other-worker-0'), dict(self.old, run_id='other'),
                    dict(self.old, control='/other'), dict(self.old, slots=128),
                    dict(self.old, stop_cache_seconds=1), dict(self.old, batch_limit=128),
                    dict(self.old, pid=True), dict(self.old, port=True),
                    dict(self.old, endpoint='http://example.test:45678'),
                    dict(self.old, source='/other'), dict(self.old, argv=['unknown'])]
        for old in variants:
            with self.subTest(old=old):
                self.write_roles(old)
                self.assert_no_signal(ValueError)
                self.assertFalse((self.bundle / 'roles').exists())
        self.write_roles()
        with mock.patch.object(deploy, 'identity', return_value=None):
            self.assert_no_signal(RuntimeError)
        self.assertFalse((self.bundle / 'roles').exists())

    def test_stop_existing_operation_and_runtime_symlink_never_signal(self):
        (self.control / 'STOP').touch()
        self.assert_no_signal(RuntimeError)
        (self.control / 'STOP').unlink()
        role_dir = self.bundle / 'roles' / self.node
        role_dir.mkdir(parents=True)
        operation = role_dir / 'operation.json'
        operation.write_text('{"state":"failed_inspect_before_retry"}')
        with mock.patch.object(deploy, 'identity', return_value=dict(self.old, state='S')):
            self.assert_no_signal(FileExistsError)
        self.assertEqual(json.loads(operation.read_text())['state'], 'failed_inspect_before_retry')

    def test_health_must_bind_every_role_dimension_and_actual_process(self):
        new_argv = deploy.probe_argv(self.bundle / 'migration_probe_fast_open.py', self.control,
                                    self.old['port'], '172.24.102.30',
                                    self.bundle / 'roles' / self.node / 'probe.json')
        health = dict(self.old, pid=888, starttime_ticks='54321', argv=new_argv, ok=True)
        with mock.patch.object(deploy, 'identity', return_value=dict(health, state='S')):
            self.assertTrue(deploy.valid_health(health, 888, new_argv, self.old,
                                              self.control, self.cfg))
            for field, value in [('host', 'wrong'), ('pid', 889), ('control', '/wrong'),
                                 ('slots', 32), ('batch_limit', 63), ('stop_cache_seconds', 0),
                                 ('argv', ['unknown']), ('starttime_ticks', '999')]:
                with self.subTest(field=field):
                    self.assertFalse(deploy.valid_health(dict(health, **{field: value}), 888,
                                                         new_argv, self.old,
                                                         self.control, self.cfg))

    def test_protocol_checks_auth_order_missing_and_unsafe_paths(self):
        seen = []
        def request(route, body=None, authenticated=True):
            seen.append((route, body, authenticated))
            if not authenticated:
                return 401, {'ok': False}
            entries = body.get('entries', [body.get('entry')])
            if any(e['rel'].startswith('../') for e in entries):
                return 400, {'ok': False}
            single = {'ok': True, 'missing': True, 'entry': entries[0], 'reason': 'missing'}
            if route == '/probe':
                return 200, single
            return 200, {'ok': True, 'results': [{'status': 200, 'result': single}] * len(entries)}
        checks = deploy.protocol_checks(request, 'fixture-unique-marker')
        self.assertTrue(checks['unsafe_path_400'])
        self.assertEqual(len(seen), 5)
        with self.assertRaisesRegex(RuntimeError, 'authentication'):
            deploy.protocol_checks(lambda *args, **kwargs: (200, {}), 'marker')

        busy_once = set()
        def busy_request(route, body=None, authenticated=True):
            if authenticated and route in ('/probe', '/probe-batch') and route not in busy_once:
                busy_once.add(route)
                return 503, {'ok': False}
            return request(route, body, authenticated)
        with mock.patch.object(deploy.time, 'sleep'):
            self.assertTrue(deploy.protocol_checks(busy_request, 'marker')['batch_probe_200'])
        self.assertEqual(busy_once, {'/probe', '/probe-batch'})

        batch_seen = []
        def partial_busy(route, body=None, authenticated=True):
            status, value = request(route, body, authenticated)
            if route == '/probe-batch' and status == 200:
                batch_seen.append(True)
                if len(batch_seen) == 1:
                    value['results'][-1] = {'status': 503, 'reason': 'metadata slots busy'}
            return status, value
        with mock.patch.object(deploy.time, 'sleep'):
            self.assertTrue(deploy.protocol_checks(partial_busy, 'marker')['batch_probe_200'])
        self.assertEqual(len(batch_seen), 2)

        def bad_item(route, body=None, authenticated=True):
            status, value = request(route, body, authenticated)
            if route == '/probe-batch' and status == 200:
                value['results'][-1] = {'status': 409, 'reason': 'unsafe parent'}
            return status, value
        with mock.patch.object(deploy.time, 'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'compatibility'):
                deploy.protocol_checks(bad_item, 'marker')
            sleep.assert_not_called()

    def test_safe_single_node_launch_and_durable_one_shot(self):
        killed = []
        new_argv = deploy.probe_argv(self.bundle / 'migration_probe_fast_open.py', self.control,
                                    self.old['port'], '172.24.102.30',
                                    self.bundle / 'roles' / self.node / 'probe.json')
        health = dict(self.old, pid=888, starttime_ticks='54321', argv=new_argv, ok=True)
        def process(pid):
            if pid == self.old['pid']:
                return None if killed else dict(self.old, state='S')
            return dict(health, state='S')
        def signal_old(pid, sig):
            operation = json.loads((self.bundle / 'roles' / self.node / 'operation.json').read_text())
            self.assertEqual(operation['state'], 'validated')
            self.assertEqual(operation['old_identity'], self.old)
            self.assertEqual((pid, sig), (self.old['pid'], signal.SIGTERM))
            killed.append(pid)
        class Response(io.BytesIO):
            status = 200
        opener = mock.Mock()
        opener.open.side_effect = lambda *args, **kwargs: Response(json.dumps(health).encode())
        lock = tempfile.TemporaryFile(mode='w+')
        self.addCleanup(lock.close)
        original_open = open
        def guarded_open(path, *args, **kwargs):
            if str(path) == '/tmp/fixture-copy-targetprobe.lock':
                return mock.MagicMock(__enter__=lambda _: lock, __exit__=lambda *_: None)
            return original_open(path, *args, **kwargs)
        child = mock.Mock(pid=888)
        child.poll.return_value = None
        with mock.patch.object(deploy, 'identity', side_effect=process), \
                mock.patch.object(deploy.os, 'kill', side_effect=signal_old), \
                mock.patch.object(deploy.subprocess, 'Popen', return_value=child) as popen, \
                mock.patch.object(deploy.urllib.request, 'build_opener', return_value=opener), \
                mock.patch.object(deploy, 'protocol_checks', return_value={'fixture': True}), \
                mock.patch('builtins.open', side_effect=guarded_open):
            result = deploy.run_node(self.control, self.bundle, self.node)
        self.assertEqual(result['state'], 'ready')
        self.assertEqual(killed, [777])
        args, kwargs = popen.call_args
        self.assertEqual(args[0], ['nice', '-n', '15', 'ionice', '-c', '3', *new_argv])
        self.assertTrue(kwargs['start_new_session'])
        self.assertTrue(kwargs['close_fds'])
        operation = deploy.read_json(self.bundle / 'roles' / self.node / 'operation.json')
        self.assertEqual(operation['state'], 'ready')
        self.assertNotIn(self.cfg['broker_token'], json.dumps(operation))
        with mock.patch.object(deploy, 'identity', return_value=dict(self.old, state='S')):
            self.assert_no_signal(RuntimeError)

    def test_unknown_signal_outcome_is_durable_and_never_retried(self):
        with mock.patch.object(deploy, 'identity', return_value=dict(self.old, state='S')), \
                mock.patch.object(deploy.os, 'kill', side_effect=OSError('unknown outcome')) as kill, \
                mock.patch.object(deploy.subprocess, 'Popen') as popen:
            with self.assertRaisesRegex(OSError, 'unknown outcome'):
                deploy.run_node(self.control, self.bundle, self.node)
            self.assertEqual(kill.call_count, 1)
            popen.assert_not_called()
        operation = deploy.read_json(self.bundle / 'roles' / self.node / 'operation.json')
        self.assertEqual(operation['state'], 'failed_inspect_before_retry')
        with mock.patch.object(deploy, 'identity', return_value=dict(self.old, state='S')):
            self.assert_no_signal(FileExistsError)

    def test_runtime_role_symlink_is_rejected_before_operation_or_signal(self):
        destination = self.control / 'outside-runtime'
        destination.mkdir()
        (self.bundle / 'roles').symlink_to(destination, target_is_directory=True)
        with mock.patch.object(deploy, 'identity', return_value=dict(self.old, state='S')):
            self.assert_no_signal(ValueError)
        self.assertEqual(list(destination.iterdir()), [])

    def test_stop_rechecked_after_initial_identity_before_signal(self):
        def process(pid):
            (self.control / 'STOP').touch()
            return dict(self.old, state='S')
        with mock.patch.object(deploy, 'identity', side_effect=process):
            self.assert_no_signal(RuntimeError)
        operation = deploy.read_json(self.bundle / 'roles' / self.node / 'operation.json')
        self.assertEqual(operation['state'], 'failed_inspect_before_retry')


if __name__ == '__main__':
    unittest.main()
