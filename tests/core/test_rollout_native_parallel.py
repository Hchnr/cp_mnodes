"""Mocked scheduling/control serialization tests; never contact copy nodes."""
from contextlib import contextmanager
import hashlib
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

import rollout_native_parallel as runner
from migration_io import read_json, write_json


class Backend:
    def __init__(self, barrier=None, failing=None):
        self.lock = threading.Lock()
        self.holds = self.releases = self.in_control = self.max_control = 0
        self.held = False
        self.calls = []
        self.arguments = []
        self.barrier = barrier
        self.failing = failing
        self.failed = threading.Event()
        self.open_nodes = set()
        self.max_nodes = 0

    def rpc_class(self):
        backend = self
        class RPC:
            def __init__(self, **kwargs):
                self.node, self.lane = kwargs['node'], kwargs['lane']
                self.operation = kwargs['operation']
            @contextmanager
            def hold(self):
                with backend.lock:
                    backend.holds += 1
                    if backend.held:
                        raise AssertionError('duplicate real global hold')
                    backend.held = True
                try:
                    yield {'locked': True}
                finally:
                    with backend.lock:
                        backend.held = False
                        backend.releases += 1
            def __call__(self, node, action, *, lane=None, identity=None):
                with backend.lock:
                    if not backend.held:
                        raise AssertionError('remote action without real global hold')
                    backend.calls.append((self.node, self.lane, action))
                    if action in ('drain', 'release'):
                        backend.in_control += 1
                        backend.max_control = max(backend.max_control, backend.in_control)
                if action in ('drain', 'release'):
                    time.sleep(.008)
                    with backend.lock:
                        backend.in_control -= 1
                if action == 'gate' and self.lane == 'primary' and backend.barrier:
                    backend.barrier.wait(timeout=5)
                if action == 'replace' and backend.failing:
                    if self.node == backend.failing:
                        backend.failed.set()
                        raise RuntimeError('ambiguous replacement')
                    backend.failed.wait(timeout=5)
                return {'ready': True}
        return RPC

    def rollout(self, control, bundle, nodes, lanes):
        if len(nodes) != 1:
            raise AssertionError('one physical node per existing controller')
        node = nodes[0]
        self.arguments.append((control, bundle, nodes, lanes))
        with self.lock:
            self.open_nodes.add(node)
            self.max_nodes = max(self.max_nodes, len(self.open_nodes))
        try:
            for lane in lanes:
                rpc = runner.deploy.DirectoryRPC(node=node, lane=lane, operation=node+'-'+lane)
                with rpc.hold() as shared:
                    if not shared.get('shared_hold'):
                        raise AssertionError('inner hold did not mark shared ownership')
                    rpc('master-0', 'drain', lane=lane)
                    try:
                        rpc('master-0', 'gate', lane=lane)
                        rpc(node, 'inspect', lane=lane)
                        rpc(node, 'replace', lane=lane)
                    finally:
                        rpc('master-0', 'release', lane=lane)
            return {'status': 'complete', 'output': 'existing-controller-record',
                    'lanes_complete': len(lanes)}
        finally:
            with self.lock:
                self.open_nodes.remove(node)


class ParallelNativeRolloutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='native-parallel-test-')
        self.addCleanup(self.tmp.cleanup)
        self.control = Path(self.tmp.name) / 'control'
        self.bundle = self.control / 'directory-open-first-native'
        self.bundle.mkdir(parents=True)
        write_json(self.control / 'state.json', {'status': 'scanning'})
        self.manifest = {'worker-directory-policy.json': 'frozen-digest'}

    def execute(self, backend, nodes, concurrency=3):
        original = backend.rpc_class()
        with mock.patch.object(runner.deploy, 'DirectoryRPC', original), \
                mock.patch.object(runner.deploy, 'verify_bundle',
                                  return_value=({'job': 'fixture-job'}, self.manifest)), \
                mock.patch.object(runner.deploy, 'rollout', side_effect=backend.rollout):
            try:
                return runner.run(self.control, self.bundle, nodes, concurrency)
            finally:
                self.assertIs(runner.deploy.DirectoryRPC, original)

    def group(self):
        paths = list(self.control.glob('native-parallel-rollout-*/group.json'))
        self.assertEqual(len(paths), 1)
        return read_json(paths[0])

    def test_one_real_hold_serialized_control_parallel_nodes_and_sequential_lanes(self):
        nodes = ['worker-0', 'worker-1', 'worker-2']
        backend = Backend(barrier=threading.Barrier(3))
        result = self.execute(backend, nodes)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['nodes_complete'], 3)
        self.assertEqual(backend.holds, 1)
        self.assertEqual(backend.releases, 1)
        self.assertEqual(backend.max_control, 1)
        self.assertEqual(backend.max_nodes, 3)
        for node in nodes:
            calls = [(lane, action) for actual, lane, action in backend.calls if actual == node]
            self.assertEqual(calls, [(lane, action) for lane in ('primary', 'supplement')
                                    for action in ('drain', 'gate', 'inspect', 'replace', 'release')])
        group = self.group()
        self.assertEqual(group['runner_sha256'], hashlib.sha256(Path(runner.__file__).read_bytes()).hexdigest())
        self.assertEqual(group['not_started'], [])
        self.assertEqual(group['source_sha256'], self.manifest)
        self.assertIn('hold_released', group)
        self.assertEqual(len(group['entries']), 3)

    def test_failure_stops_future_nodes_never_retries_replace_and_restores_hold_and_binding(self):
        backend = Backend(failing='worker-0')
        with self.assertRaisesRegex(RuntimeError, 'inspect'):
            self.execute(backend, ['worker-0', 'worker-1', 'worker-2'], concurrency=1)
        self.assertEqual(backend.holds, 1)
        self.assertEqual(backend.releases, 1)
        self.assertEqual(sum(action == 'replace' for _, _, action in backend.calls), 1)
        self.assertEqual({node for node, _, _ in backend.calls}, {'worker-0'})
        group = self.group()
        self.assertEqual(group['status'], 'inspect_before_retry')
        self.assertEqual(group['not_started'], ['worker-1', 'worker-2'])
        self.assertEqual(group['entries'][0]['status'], 'inspect_before_retry')

    def test_active_other_node_finishes_current_cleanup_but_no_next_lane_or_node(self):
        backend = Backend(barrier=threading.Barrier(2), failing='worker-0')
        with self.assertRaisesRegex(RuntimeError, 'inspect'):
            self.execute(backend, ['worker-0', 'worker-1', 'worker-2'], concurrency=2)
        self.assertIn(('worker-1', 'primary', 'release'), backend.calls)
        self.assertFalse(any(lane == 'supplement' for _, lane, _ in backend.calls))
        self.assertNotIn('worker-2', {node for node, _, _ in backend.calls})
        self.assertEqual(backend.releases, 1)
        group = self.group()
        self.assertEqual({e['status'] for e in group['entries']},
                         {'inspect_before_retry', 'stopped_before_next_lane'})

    def test_invalid_inputs_do_not_create_records_or_call_controller(self):
        for nodes, concurrency in [([], 1), (['worker-0', 'worker-0'], 2),
                                   (['unknown'], 1), (['worker-0'], 0),
                                   (['worker-0'], 4), (['worker-0'], True)]:
            with self.subTest(nodes=nodes, concurrency=concurrency), \
                    mock.patch.object(runner.deploy, 'verify_bundle') as verify:
                with self.assertRaises(ValueError):
                    runner.run(self.control, self.bundle, nodes, concurrency)
                verify.assert_not_called()
        self.assertEqual(list(self.control.glob('native-parallel-rollout-*')), [])

    def test_legacy_bundle_stop_and_terminal_all_fail_before_real_hold(self):
        backend = Backend()
        self.manifest = {'migration_worker.py': 'legacy-digest'}
        with self.assertRaisesRegex(ValueError, 'native bundle'):
            self.execute(backend, ['worker-0'])
        self.manifest = {'worker-directory-policy.json': 'native-digest'}
        (self.control / 'STOP').touch()
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            self.execute(backend, ['worker-0'])
        (self.control / 'STOP').unlink()
        write_json(self.control / 'state.json', {'status': 'complete'})
        with self.assertRaisesRegex(RuntimeError, 'terminal'):
            self.execute(backend, ['worker-0'])
        self.assertEqual(backend.holds, 0)
        self.assertEqual(list(self.control.glob('native-parallel-rollout-*')), [])

    def test_global_hold_failure_records_inspection_and_does_not_start_node(self):
        backend = Backend()
        original = backend.rpc_class()
        @contextmanager
        def broken_hold(self):
            raise RuntimeError('existing pass still holds dispatch lock')
            yield
        original.hold = broken_hold
        with mock.patch.object(runner.deploy, 'DirectoryRPC', original), \
                mock.patch.object(runner.deploy, 'verify_bundle',
                                  return_value=({'job': 'fixture-job'}, self.manifest)), \
                mock.patch.object(runner.deploy, 'rollout') as rollout:
            with self.assertRaisesRegex(RuntimeError, 'dispatch lock'):
                runner.run(self.control, self.bundle, ['worker-0'])
            self.assertIs(runner.deploy.DirectoryRPC, original)
            rollout.assert_not_called()
        group = self.group()
        self.assertEqual(group['status'], 'inspect_before_retry')
        self.assertEqual(group['not_started'], ['worker-0'])


if __name__ == '__main__':
    unittest.main()
