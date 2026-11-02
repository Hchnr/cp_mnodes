"""Private control-plane fixtures; never run a worker, SSH, signal, or rm."""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))
import join_tailboost as join


class FakeClock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def sleep(self, duration):
        self.elapsed += duration


class JoinRequestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='join-tailboost-private-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.run = self.base/'runtime-supplement'
        self.run.mkdir()
        self.original = self.base/'original-run'
        self.target = self.base/'private-target'
        self.actor = dict(pid=8675309, start='123456', boot='private-fixture-boot')
        self.now = 1700000000.0
        self.nonce = 'a'*32
        self.clock = FakeClock()
        self.output = io.StringIO()
        self.sent = []
        self.on_send = None
        worker = self.run/'rm24.py'
        worker.write_bytes(b'# private fixture: never executed\n')
        self.cfg = dict(target=str(self.target), root_inode=314159,
                        supplement_of=str(self.original), runtime_join=True,
                        per_node=4, sha256=hashlib.sha256(worker.read_bytes()).hexdigest())
        self.write(self.run/'config.json', self.cfg)
        self.write(self.run/'status.json', dict(state='rm_running', updated=self.now))
        self.write(self.run/'controller.json', self.actor)
        for obj, name, value in (
                (join.rm24, 'TARGET', str(self.target)),
                (join.rm24, 'ROOT_INODE', 314159),
                (join.rm24, 'RUN', self.original),
                (join.rm24, 'NODES', ['master-0', 'worker-0', 'worker-1']),
                (join.time, 'time', lambda: self.now),
                (join.time, 'monotonic', self.clock.monotonic),
                (join.time, 'sleep', self.clock.sleep),
                (join.uuid, 'uuid4', lambda: types.SimpleNamespace(hex=self.nonce))):
            patcher = mock.patch.object(obj, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = mock.patch.object(join.rm24, 'identity', return_value=copy.deepcopy(self.actor))
        self.identity = patcher.start()
        self.addCleanup(patcher.stop)
        for obj, name in ((join.rm24, 'ssh'), (join.rm24, 'preflight'),
                          (join.rm24, 'native_rm'), (join.rm24, 'node_check'),
                          (join.rm24.subprocess, 'Popen'), (join.rm24.subprocess, 'run')):
            patcher = mock.patch.object(obj, name, side_effect=AssertionError(
                'private request tests must not invoke workers, SSH, or rm'))
            patcher.start()
            self.addCleanup(patcher.stop)
        real_atomic = join.rm24.atomic

        def fixture_atomic(path, value):
            path = Path(path)
            self.assertEqual(path.parent, self.run/'runtime-requests')
            self.assertEqual(path.name, self.nonce+'.json')
            self.sent.append(copy.deepcopy(value))
            real_atomic(path, value)
            if self.on_send:
                self.on_send(value)

        patcher = mock.patch.object(join.rm24, 'atomic', side_effect=fixture_atomic)
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def config(self, **changes):
        self.cfg.update(changes)
        self.write(self.run/'config.json', self.cfg)

    def ack(self, state='accepted', **changes):
        result = dict(version=1, nonce=self.nonce, node='worker-0', state=state,
                      reason='private fixture', at=self.now)
        result.update(changes)
        return result

    def answer(self, result):
        def respond(_):
            self.write(self.run/'runtime-acks'/(self.nonce+'.json'), result)
        self.on_send = respond

    def request(self, node='worker-0', timeout=3):
        with contextlib.redirect_stdout(self.output):
            return join.request_join(self.run, node, timeout=timeout)

    def assert_no_request(self):
        self.assertEqual(self.sent, [])
        self.assertFalse((self.run/'runtime-requests').exists())
        self.assertFalse((self.run/'STOP').exists())

    def test_request_is_bound_to_resolved_run_controller_config_and_concurrency(self):
        expected = self.ack()
        self.answer(expected)
        before_config = (self.run/'config.json').read_bytes()
        before_worker = (self.run/'rm24.py').read_bytes()
        self.assertEqual(self.request(), expected)
        request, = self.sent
        self.assertEqual(request, dict(version=1, nonce=self.nonce, run=str(self.run),
            controller=self.actor, config_sha256=hashlib.sha256(before_config).hexdigest(),
            node='worker-0', per_node=4))
        self.assertEqual((self.run/'config.json').read_bytes(), before_config)
        self.assertEqual((self.run/'rm24.py').read_bytes(), before_worker)
        self.assertFalse((self.run/'STOP').exists())

    def test_wrong_scope_and_unsupported_runtime_are_rejected_before_publication(self):
        baseline = copy.deepcopy(self.cfg)
        changes = [dict(target=str(self.base/'different-target')), dict(root_inode=1),
                   dict(supplement_of=str(self.base/'different-original')),
                   dict(runtime_join=False), dict(runtime_join='true'), dict(runtime_join=1)]
        for change in changes:
            with self.subTest(change=change):
                self.cfg = copy.deepcopy(baseline)
                self.config(**change)
                with self.assertRaises(ValueError):
                    self.request()
                self.assert_no_request()

    def test_per_node_type_and_bounds_are_rejected_before_publication(self):
        for value in (False, 0, -1, 9, '4', 4.0):
            with self.subTest(per_node=value):
                self.config(per_node=value)
                with self.assertRaises(ValueError):
                    self.request()
                self.assert_no_request()

    def test_unknown_node_is_rejected_before_publication(self):
        for node in ('worker-999', '../worker-0', '', None):
            with self.subTest(node=node):
                with self.assertRaises(ValueError):
                    self.request(node=node)
                self.assert_no_request()

    def test_changed_frozen_worker_hash_is_rejected_before_publication(self):
        (self.run/'rm24.py').write_bytes(b'# changed private fixture\n')
        with self.assertRaisesRegex(ValueError, 'frozen'):
            self.request()
        self.assert_no_request()

    def test_stale_nonrunning_and_stopped_runs_are_rejected_before_publication(self):
        for status in (dict(state='rm_running', updated=self.now-16),
                       dict(state='supplement_complete', updated=self.now),
                       dict(state='retrying_failed_jobs', updated=self.now)):
            with self.subTest(status=status):
                self.write(self.run/'status.json', status)
                with self.assertRaises(ValueError):
                    self.request()
                self.assert_no_request()
        self.write(self.run/'status.json', dict(state='rm_running', updated=self.now))
        self.write(self.run/'STOP', dict(reason='private fixture'))
        with self.assertRaises(ValueError):
            self.request()
        self.assertEqual(self.sent, [])

    def test_controller_identity_change_is_rejected_before_publication(self):
        self.identity.return_value = dict(self.actor, start='changed')
        with self.assertRaisesRegex(ValueError, 'controller'):
            self.request()
        self.assert_no_request()

    def test_accepted_and_already_active_acknowledgements_are_successful(self):
        for state in ('accepted', 'already_active'):
            with self.subTest(state=state):
                result = self.ack(state=state)
                self.answer(result)
                self.assertEqual(self.request(), result)

    def test_acknowledgement_without_optional_version_is_compatible(self):
        result = self.ack()
        result.pop('version')
        self.answer(result)
        self.assertEqual(self.request(), result)

    def test_rejected_and_unknown_state_acknowledgements_are_not_success(self):
        for state in ('rejected', 'starting', 'pending', 'complete', None):
            with self.subTest(state=state):
                self.answer(self.ack(state=state))
                with self.assertRaises(RuntimeError if state == 'rejected' else ValueError):
                    self.request()
                self.assertFalse((self.run/'STOP').exists())

    def test_wrong_acknowledgement_node_and_nonce_are_rejected(self):
        for change in (dict(node='worker-1'), dict(nonce='b'*32)):
            with self.subTest(change=change):
                self.answer(self.ack(**change))
                with self.assertRaises(ValueError):
                    self.request()

    def test_acknowledgement_schema_and_explicit_version_are_validated(self):
        for payload in ({}, [], 'accepted', 1, self.ack(version=999), self.ack(version=True)):
            with self.subTest(payload=payload):
                self.answer(payload)
                with self.assertRaises(ValueError):
                    self.request()

    def test_optional_acknowledgement_bindings_must_match_request(self):
        invalid = [dict(run=str(self.base/'other-run')),
                   dict(controller=dict(self.actor, boot='different')),
                   dict(config_sha256='0'*64), dict(per_node=8), dict(per_node=True)]
        for change in invalid:
            with self.subTest(change=change):
                self.answer(self.ack(**change))
                with self.assertRaises(ValueError):
                    self.request()

    def test_matching_optional_acknowledgement_bindings_are_accepted(self):
        result = self.ack(run=str(self.run), controller=self.actor, per_node=4,
                          config_sha256=hashlib.sha256((self.run/'config.json').read_bytes()).hexdigest())
        self.answer(result)
        self.assertEqual(self.request(), result)

    def test_timeout_keeps_pending_request_and_does_not_claim_success(self):
        with self.assertRaisesRegex(RuntimeError, 'timed out'):
            self.request(timeout=3)
        request, = self.sent
        self.assertEqual(json.loads((self.run/'runtime-requests'/(self.nonce+'.json')).read_text()), request)
        self.assertFalse((self.run/'runtime-acks'/(self.nonce+'.json')).exists())
        self.assertFalse((self.run/'STOP').exists())
        messages = [json.loads(line) for line in self.output.getvalue().splitlines()]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['requested'], 'worker-0')
        self.assertNotIn('state', messages[0])

    def test_controller_change_while_waiting_is_not_success(self):
        self.identity.side_effect = [copy.deepcopy(self.actor), dict(self.actor, start='changed')]
        with self.assertRaisesRegex(RuntimeError, 'controller'):
            self.request()
        self.assertEqual(len(self.sent), 1)
        self.assertTrue((self.run/'runtime-requests'/(self.nonce+'.json')).exists())

    def test_controller_disappearance_while_waiting_is_reported_clearly(self):
        self.identity.side_effect = [copy.deepcopy(self.actor), FileNotFoundError('private fixture')]
        with self.assertRaisesRegex(RuntimeError, 'controller'):
            self.request()

    def test_stop_while_waiting_preserves_pending_without_false_success(self):
        self.on_send = lambda _: self.write(self.run/'STOP', dict(reason='private fixture'))
        with self.assertRaisesRegex(RuntimeError, 'stopped'):
            self.request()
        self.assertTrue((self.run/'runtime-requests'/(self.nonce+'.json')).exists())

    def test_success_ack_cannot_hide_controller_change(self):
        self.answer(self.ack())
        self.identity.side_effect = [copy.deepcopy(self.actor), dict(self.actor, start='changed')]
        with self.assertRaisesRegex(RuntimeError, 'controller'):
            self.request()

    def test_success_ack_cannot_hide_a_stop(self):
        def respond(_):
            self.write(self.run/'runtime-acks'/(self.nonce+'.json'), self.ack())
            self.write(self.run/'STOP', dict(reason='private fixture'))
        self.on_send = respond
        with self.assertRaisesRegex(RuntimeError, 'stopped'):
            self.request()

    def test_transient_ack_json_parse_error_can_recover(self):
        real_load = join.rm24.load
        reads = 0

        def flaky_load(path):
            nonlocal reads
            if Path(path).parent == self.run/'runtime-acks':
                reads += 1
                if reads == 1:
                    raise json.JSONDecodeError('private partial JSON', '{', 1)
            return real_load(path)

        self.answer(self.ack())
        with mock.patch.object(join.rm24, 'load', side_effect=flaky_load):
            self.assertEqual(self.request()['state'], 'accepted')
        self.assertEqual(reads, 2)


if __name__ == '__main__':
    unittest.main()
