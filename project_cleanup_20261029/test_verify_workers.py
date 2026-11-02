"""Private mocked readiness tests: never SSH, execute a worker, stop, or signal."""
import contextlib
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
spec = importlib.util.spec_from_file_location('private_verify_workers', Path(__file__).with_name('verify_workers.py'))
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class Clock:
    def __init__(self):
        self.elapsed = 0
        self.callback = None

    def monotonic(self):
        return self.elapsed

    def time(self):
        return 1800000000+self.elapsed

    def sleep(self, duration):
        self.elapsed += duration
        if self.callback:
            self.callback(duration)


class VerifyWorkersTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='verify-workers-private-')
        self.addCleanup(self.temp.cleanup)
        self.run = Path(self.temp.name)/'run'
        (self.run/'workers').mkdir(parents=True)
        self.nodes = ['master-0', 'extra-worker-0']
        self.controller = dict(pid=999999, start='controller-start', boot='private-boot')
        (self.run/'rm24.py').write_bytes(b'# private fixture, never executed\n')
        self.cfg = dict(target=verify.rm24.TARGET, root_inode=verify.rm24.ROOT_INODE,
            nodes=verify.rm24.NODES, node_hosts=verify.rm24.NODE_HOSTS,
            supplement_of=str(verify.rm24.RUN.resolve()), runtime_join=True, per_node=4,
            execution_nodes=self.nodes, sha256=hashlib.sha256((self.run/'rm24.py').read_bytes()).hexdigest())
        self.clock = Clock()
        self.write('config.json', self.cfg)
        self.write('controller.json', self.controller)
        self.status = dict(state='rm_running', updated=self.clock.time(), errors=[])
        self.write('status.json', self.status)
        self.claims = {node: dict(pid=100+i, start='start-'+node, boot='private-boot', node=node,
                                 host=verify.rm24.NODE_HOSTS[node], script=str(self.run/'rm24.py'))
                       for i, node in enumerate(self.nodes)}
        for obj, name, value in ((verify.time, 'time', self.clock.time),
                                 (verify.time, 'monotonic', self.clock.monotonic),
                                 (verify.time, 'sleep', self.clock.sleep)):
            patcher = mock.patch.object(obj, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = mock.patch.object(verify, '_controller_alive', return_value=True)
        self.alive = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(verify.subprocess, 'run',
            side_effect=AssertionError('real SSH/process execution prohibited in private tests'))
        self.transport = patcher.start()
        self.addCleanup(patcher.stop)

    def write(self, name, data):
        (self.run/name).write_text(json.dumps(data))

    def publish_claims(self):
        for node, actor in self.claims.items():
            self.write('workers/'+node+'.json', actor)

    def proof(self, run, node, actor, deadline):
        return dict(actor, node=node, host=verify.rm24.NODE_HOSTS[node], state='S',
                    cmdline=['python3']+verify._expected_cmdline(run, node))

    def wait(self, timeout=3):
        return verify.wait_workers_ready(self.run, self.nodes, timeout=timeout)

    def test_no_claims_never_report_ready_or_probe_ssh(self):
        with mock.patch.object(verify, '_probe_worker') as probe:
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed at timeout'):
                self.wait(timeout=.2)
            probe.assert_not_called()
        self.transport.assert_not_called()
        self.assertFalse((self.run/'STOP').exists())

    def test_all_live_proofs_required_and_final_status_is_returned(self):
        self.publish_claims()
        with mock.patch.object(verify, '_probe_worker', side_effect=self.proof) as probe:
            result = self.wait()
        self.assertEqual(result['state'], 'workers_ready')
        self.assertEqual(result['verified_nodes'], 2)
        self.assertEqual(result['nodes'], self.nodes)
        self.assertEqual(len(result['proofs']), 2)
        self.assertEqual(result['status'], self.status)
        self.assertEqual(probe.call_count, 2)
        self.assertGreaterEqual(self.clock.elapsed, verify.SETTLE_SECONDS)

    def test_one_unconfirmed_node_cannot_be_reported_as_all_ready(self):
        self.publish_claims()
        def partial(run, node, actor, deadline):
            if node == self.nodes[1]:
                raise OSError('private SSH unavailable')
            return self.proof(run, node, actor, deadline)
        with mock.patch.object(verify, '_probe_worker', side_effect=partial):
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed at timeout'):
                self.wait(timeout=2)
        self.assertFalse((self.run/'STOP').exists())

    def test_transient_ssh_failure_retries_within_deadline(self):
        self.publish_claims()
        attempted = []
        def flaky(run, node, actor, deadline):
            attempted.append(node)
            if node == self.nodes[1] and attempted.count(node) == 1:
                raise OSError('temporary private transport failure')
            return self.proof(run, node, actor, deadline)
        with mock.patch.object(verify, '_probe_worker', side_effect=flaky):
            result = self.wait(timeout=4)
        self.assertEqual(result['verified_nodes'], 2)
        self.assertEqual(attempted.count(self.nodes[1]), 2)

    def test_wrong_shared_claim_binding_is_rejected_before_ssh(self):
        self.publish_claims()
        node = self.nodes[1]
        self.write('workers/'+node+'.json', dict(self.claims[node], host='wrong-cluster'))
        with self.assertRaisesRegex(ValueError, 'claim binding differs'):
            self.wait()
        self.transport.assert_not_called()

    def test_bad_live_identity_is_not_treated_as_transient_ready(self):
        self.publish_claims()
        with mock.patch.object(verify, '_probe_worker', side_effect=ValueError('identity differs')):
            with self.assertRaisesRegex(ValueError, 'identity differs'):
                self.wait()
        self.assertFalse((self.run/'STOP').exists())

    def test_stop_fatal_and_dead_controller_are_refused(self):
        self.publish_claims()
        self.write('STOP', dict(reason='private fixture'))
        with self.assertRaisesRegex(RuntimeError, 'stopped or failed'):
            self.wait()
        (self.run/'STOP').unlink()
        self.write('status.json', dict(self.status, state='stopped_or_error'))
        with self.assertRaisesRegex(RuntimeError, 'stopped or failed'):
            self.wait()
        self.write('status.json', self.status)
        self.alive.return_value = False
        with self.assertRaisesRegex(RuntimeError, 'controller exited'):
            self.wait()

    def test_stale_status_does_not_report_ready(self):
        self.publish_claims()
        self.write('status.json', dict(self.status, updated=self.clock.time()-20))
        with mock.patch.object(verify, '_probe_worker') as probe:
            with self.assertRaisesRegex(RuntimeError, 'unconfirmed at timeout'):
                self.wait(timeout=.2)
            probe.assert_not_called()

    def test_natural_completion_is_not_claimed_as_active_workers(self):
        complete = dict(self.status, state='supplement_complete')
        self.write('status.json', complete)
        self.alive.return_value = False
        result = self.wait()
        self.assertEqual(result['state'], 'terminal_complete')
        self.assertEqual(result['verified_nodes'], 0)
        self.assertEqual(result['proofs'], [])
        self.assertEqual(result['status'], complete)
        self.transport.assert_not_called()

    def test_controller_or_config_change_during_stability_is_refused(self):
        self.publish_claims()
        self.clock.callback = lambda duration: self.write('controller.json', dict(self.controller, start='changed'))
        with mock.patch.object(verify, '_probe_worker', side_effect=self.proof):
            with self.assertRaisesRegex(ValueError, 'binding changed'):
                self.wait()

    def test_completion_during_stability_returns_terminal_status(self):
        self.publish_claims()
        complete = dict(self.status, state='supplement_complete')
        self.clock.callback = lambda duration: self.write('status.json', complete)
        with mock.patch.object(verify, '_probe_worker', side_effect=self.proof):
            result = self.wait()
        self.assertEqual(result['state'], 'terminal_complete')
        self.assertEqual(result['verified_nodes'], 0)
        self.assertEqual(result['status'], complete)

    def test_registry_and_entire_initial_node_set_are_required(self):
        with self.assertRaisesRegex(ValueError, 'complete initial'):
            verify.wait_workers_ready(self.run, self.nodes[:1], timeout=1)
        self.write('config.json', dict(self.cfg, node_hosts={}))
        with self.assertRaisesRegex(ValueError, 'scope/registry differs'):
            self.wait()

    def test_actual_probe_verifies_exact_cmdline_and_read_only_ssh(self):
        node, actor = self.nodes[1], self.claims[self.nodes[1]]
        good = self.proof(self.run, node, actor, 10)
        self.transport.side_effect = None
        self.transport.return_value = types.SimpleNamespace(returncode=0, stdout=json.dumps(good), stderr='')
        result = verify._probe_worker(self.run, node, actor, 10)
        self.assertEqual(result, good)
        args, kwargs = self.transport.call_args
        self.assertEqual(args[0][0], 'ssh')
        self.assertEqual(args[0][-2], node)
        self.assertEqual(args[0][-1], 'python3 -S -B -')
        self.assertLessEqual(kwargs['timeout'], 10)
        for change in (dict(boot='wrong'), dict(state='Z'), dict(host='wrong'),
                       dict(cmdline=['python3', str(self.run/'rm24.py'), '_worker'])):
            self.transport.return_value = types.SimpleNamespace(returncode=0,
                stdout=json.dumps(dict(good, **change)), stderr='')
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'identity/hostname/cmdline differs'):
                verify._probe_worker(self.run, node, actor, 10)


if __name__ == '__main__':
    unittest.main()
