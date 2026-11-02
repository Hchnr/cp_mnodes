"""Runtime admission tests: private temporary fixtures and local transports only."""
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('runtime_fixture_rm24', Path(__file__).with_name('rm24.py'))
rm24 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rm24)


class RuntimeJoinTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='rm24-runtime-private-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.run = self.base/'run'
        self.run.mkdir()
        self.cfg = dict(per_node=2, execution_nodes=['worker-0'], runtime_join=True)
        rm24.atomic(self.run/'config.json', self.cfg)
        self.source, self.finished, self.stop = queue.Queue(), threading.Event(), threading.Event()
        self.source.put(dict(id='private-task'))
        self.actor = dict(pid=123, start='456', boot='private-boot')
        self.joins = rm24.RuntimeJoins(self.run, self.cfg, self.source,
                                     self.finished, self.stop, ['worker-0'], actor=self.actor)

    def request(self, nonce='request1', node='worker-1', **changes):
        request = dict(version=1, nonce=nonce, node=node, run=str(self.run.resolve()),
                       controller=self.actor, config_sha256=self.joins.config_sha256, per_node=2)
        request.update(changes)
        rm24.atomic(self.run/'runtime-requests'/(nonce+'.json'), request)
        return request

    def ack(self, nonce='request1'):
        return rm24.load(self.run/'runtime-acks'/(nonce+'.json'))

    def test_duplicate_node_and_nonce_never_submit_another_worker(self):
        submitted = []
        request = self.request()
        self.joins.poll(lambda r: submitted.append(r))
        self.assertEqual(submitted, [request])
        self.assertEqual(self.joins.snapshot()['starting'], ['worker-1'])
        self.request(nonce='duplicate')
        self.joins.poll(lambda r: submitted.append(r))
        self.assertEqual(self.ack('duplicate')['state'], 'rejected')
        self.assertEqual(len(submitted), 1)
        self.assertTrue(self.joins.ready(request))
        self.assertEqual(self.ack()['state'], 'accepted')
        self.request(nonce='accepted-again')
        self.joins.poll(lambda r: submitted.append(r))
        self.assertEqual(self.ack('accepted-again')['state'], 'already_active')
        self.assertEqual(self.joins.nodes, ['worker-0', 'worker-1'])
        self.assertEqual(self.joins.claim()['id'], 'private-task')
        with self.assertRaises(queue.Empty):
            self.joins.claim()

    def test_duplicate_of_starting_join_never_reports_success_when_preflight_fails(self):
        first = self.request()
        submitted = []
        self.joins.poll(lambda r: submitted.append(r))
        self.request(nonce='second')
        self.joins.poll(lambda r: submitted.append(r))
        self.assertEqual(self.ack('second')['state'], 'rejected')
        self.assertIn('still starting', self.ack('second')['reason'])
        self.assertEqual(len(submitted), 1)
        self.joins.reject(first, 'join preflight failed: private fixture')
        self.assertEqual(self.ack()['state'], 'rejected')
        self.assertEqual(self.joins.nodes, ['worker-0'])
        self.assertEqual(self.joins.snapshot()['starting'], [])

    def test_bad_request_cannot_remove_an_existing_accepted_node(self):
        first = self.request()
        self.joins.poll(lambda r: None)
        self.assertTrue(self.joins.ready(first))
        self.request(nonce='bad-duplicate', config_sha256='invalid')
        self.joins.poll(lambda r: self.fail('invalid duplicate submitted'))
        self.assertEqual(self.ack('bad-duplicate')['state'], 'rejected')
        self.assertEqual(self.joins.nodes, ['worker-0', 'worker-1'])
        self.assertEqual(self.joins.snapshot()['accepted'], ['worker-1'])

    def test_binding_failures_are_rejected_without_consuming_work(self):
        changes = [dict(version=True), dict(run='/different/run'),
                   dict(controller=dict(self.actor, start='stale')),
                   dict(config_sha256='wrong'), dict(per_node=4),
                   dict(per_node=True), dict(node='unknown-node'), dict(node=[])]
        for i, values in enumerate(changes):
            self.request(nonce='bad'+str(i), **values)
        self.joins.poll(lambda r: self.fail('invalid request submitted'))
        for i in range(len(changes)):
            self.assertEqual(self.ack('bad'+str(i))['state'], 'rejected')
        self.assertEqual(self.source.qsize(), 1)
        self.assertFalse(self.stop.is_set())

    def test_changed_frozen_config_is_rejected(self):
        self.request()
        rm24.atomic(self.run/'config.json', dict(self.cfg, per_node=4))
        self.joins.poll(lambda r: self.fail('changed config submitted'))
        self.assertEqual(self.ack()['state'], 'rejected')
        self.assertIn('config changed', self.ack()['reason'])

    def test_invalid_json_and_transient_missing_file_do_not_abort(self):
        invalid = self.run/'runtime-requests/invalid.json'
        invalid.write_text('{')
        original = rm24.load
        self.request(nonce='retry')
        missing = [True]

        def transient(path):
            if Path(path).name == 'retry.json' and missing[0]:
                missing[0] = False
                raise FileNotFoundError('simulated shared filesystem lookup')
            return original(path)

        submitted = []
        with patch.object(rm24, 'load', side_effect=transient):
            self.joins.poll(lambda r: submitted.append(r))
            self.joins.poll(lambda r: submitted.append(r))
        self.assertEqual(self.ack('invalid')['state'], 'rejected')
        self.assertEqual([r['nonce'] for r in submitted], ['retry'])
        self.assertFalse(self.stop.is_set())

    def test_stop_empty_queue_and_phase_close_reject_late_admission(self):
        for why in ('stop', 'empty', 'phase'):
            with self.subTest(why=why):
                self.source = queue.Queue()
                if why != 'empty':
                    self.source.put(dict(id='retained'))
                self.finished, self.stop = threading.Event(), threading.Event()
                joins = rm24.RuntimeJoins(self.run, self.cfg, self.source,
                    self.finished, self.stop, ['worker-0'], actor=self.actor)
                if why == 'stop':
                    self.stop.set()
                elif why == 'empty':
                    self.finished.set()
                else:
                    joins.close('retry phase')
                self.request(nonce='late-'+why)
                joins.poll(lambda r: self.fail('late request submitted'))
                self.assertEqual(self.ack('late-'+why)['state'], 'rejected')

    def test_published_stop_and_closure_during_ready_are_fenced(self):
        request = self.request()
        self.joins.poll(lambda r: None)
        rm24.atomic(self.run/'STOP', dict(reason='private fixture'))
        self.assertFalse(self.joins.ready(request))
        self.assertEqual(self.ack()['state'], 'rejected')
        self.assertEqual(self.joins.snapshot()['starting'], [])
        with self.assertRaises(queue.Empty):
            self.joins.claim()
        self.assertEqual(self.source.qsize(), 1)

    def test_admission_failure_frees_only_reserved_node(self):
        request = self.request()
        self.joins.poll(lambda r: None)
        self.joins.reject(request, 'private preflight failure')
        self.assertEqual(self.joins.nodes, ['worker-0'])
        self.assertEqual(self.joins.snapshot()['starting'], [])
        self.assertFalse(self.stop.is_set())
        self.request(nonce='retry-node')
        submitted = []
        self.joins.poll(lambda r: submitted.append(r))
        self.assertEqual(len(submitted), 1)

    def test_claim_does_not_stat_shared_stop_file(self):
        with patch.object(Path, 'exists', side_effect=AssertionError('per-task shared stat')):
            self.assertEqual(self.joins.claim()['id'], 'private-task')
            self.stop.set()
            with self.assertRaises(queue.Empty):
                self.joins.claim()

    def test_ready_pong_has_bounded_timeout_and_no_reader_thread(self):
        proc = subprocess.Popen([sys.executable, '-S', '-B', '-c',
                                 'import time;time.sleep(.2)'], stdout=subprocess.PIPE)
        try:
            before = set(threading.enumerate())
            with self.assertRaises(TimeoutError):
                rm24.ready_pong(proc, self.stop, timeout=.02)
            self.assertEqual(set(threading.enumerate()), before)
        finally:
            proc.wait(timeout=3)
            proc.stdout.close()

    def integration(self, join_failure=None, fail_first=False, cleanup_failure=None,
                    fleet_failure=None):
        run = self.base/'integration'
        (run/'logs').mkdir(parents=True)
        (run/'workers').mkdir()
        script = run/'rm24.py'
        shutil.copyfile(rm24.__file__, script)
        target = self.base/'private-target'
        target.mkdir()
        ino = target.stat().st_ino
        names = ['file-'+str(i) for i in range(64)]
        for name in names:
            (target/name).write_text('private fixture only')
        cfg = dict(target=str(target), root_inode=ino, nodes=rm24.NODES,
                   per_node=2, execution_nodes=['worker-0', 'worker-2'] if fleet_failure == 'future' else ['worker-0'], runtime_join=True,
                   sha256=hashlib.sha256(script.read_bytes()).hexdigest(),
                   supplement_of='private-fixture-no-root-verify',
                   seed_tasks=[dict(kind='task', parent='', ino=ino, names=[name]) for name in names])
        rm24.atomic(run/'config.json', cfg)
        (run/'runtime-requests').mkdir()
        actor = rm24.identity(os.getpid())
        request = dict(version=1, nonce='integration', node='worker-1', run=str(run),
                       controller=actor, config_sha256=hashlib.sha256((run/'config.json').read_bytes()).hexdigest(),
                       per_node=2)
        rm24.atomic(run/'runtime-requests/integration.json', request)
        transports, checks, command_nodes = [], [], {}
        original_popen = subprocess.Popen

        class ErrorOnClose:
            def __init__(self, stream):
                self.stream = stream

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def close(self):
                self.stream.close()
                raise BrokenPipeError('private fixture cleanup failure')

        def local_popen(command, *args, **kwargs):
            node = command_nodes[tuple(command)]
            if fleet_failure == 'future' and node == 'worker-0':
                raise RuntimeError('private unexpected dispatch future failure')
            proc = original_popen(command, *args, **kwargs)
            if node == 'worker-1':
                if cleanup_failure == 'unclaimed':
                    proc.stdin = ErrorOnClose(proc.stdin)
                elif cleanup_failure == 'claimed':
                    proc.stdout = ErrorOnClose(proc.stdout)
            return proc

        def local_transport(node, frozen, action, run_arg=None):
            self.assertEqual(action, '_worker')
            transports.append(node)
            if node == 'worker-1' and join_failure == 'ready':
                command = [sys.executable, '-S', '-B', '-u', '-c', 'print("{}",flush=True)']
                command_nodes[tuple(command)] = node
                return command
            code = ('import importlib.util,pathlib,time\n'
                's=importlib.util.spec_from_file_location("private_fixture",'+repr(str(frozen))+')\n'
                'm=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n'
                'target=pathlib.Path('+repr(str(target))+')\n'
                'assert target.parent.name.startswith("rm24-runtime-private-")\n'
                'm.TARGET=str(target);m.ROOT_INODE='+str(ino)+'\n'
                'm.node_check=lambda *a,**k:None\n'
                'original=m.native_rm\n'
                'def delayed(root,task,*args):\n'
                ' time.sleep(.025)\n')
            if fail_first:
                code += (' if "file-0" in task["names"] and not task.get("retry_attempt"):\n'
                         '  return dict(rc=1)\n')
            code += (' return original(root,task,*args)\n'
                     'm.native_rm=delayed\n'
                     'm.worker(pathlib.Path('+repr(str(run))+'),'+repr(node)+')\n')
            command = [sys.executable, '-S', '-B', '-u', '-c', code]
            command_nodes[tuple(command)] = node
            return command

        def check(frozen, nodes=None):
            checks.append(list(nodes))
            if nodes == ['worker-1'] and join_failure == 'preflight':
                raise RuntimeError('existing rm under target: private fixture')
            return [dict(node=node) for node in nodes]

        before_threads = set(threading.enumerate())
        original_poll = rm24.RuntimeJoins.poll
        fatal = cleanup_failure == 'claimed' or fleet_failure is not None

        def admission_poll(manager, submit=None):
            if fleet_failure == 'poll' and submit is not None:
                raise RuntimeError('private unexpected admission poll failure')
            return original_poll(manager, submit)

        started = time.monotonic()
        with patch.object(rm24, 'TARGET', str(target)), patch.object(rm24, 'ROOT_INODE', ino), \
                patch.object(rm24, 'ssh', side_effect=local_transport), \
                patch.object(rm24, 'preflight', side_effect=check), \
                patch.object(rm24.subprocess, 'Popen', side_effect=local_popen), \
                patch.object(rm24.RuntimeJoins, 'poll', admission_poll), contextlib.redirect_stdout(None):
            if fleet_failure:
                with self.assertRaises(RuntimeError):
                    rm24.controller(run)
            else:
                rm24.controller(run)
        self.assertLess(time.monotonic()-started, 5, 'private controller shutdown hung')
        self.assertEqual(set(threading.enumerate()), before_threads)
        status = rm24.load(run/'status.json')
        if fatal:
            self.assertEqual(status['state'], 'stopped_or_error')
            self.assertTrue(status['errors'])
            self.assertTrue((run/'STOP').exists())
            self.assertTrue((run/'first-error.json').exists())
            if fleet_failure:
                self.assertTrue(list(target.iterdir()), 'pending tasks were incorrectly claimed complete')
        else:
            self.assertEqual(list(target.iterdir()), [])
            self.assertEqual(status['state'], 'supplement_complete')
            self.assertEqual(status['errors'], [])
            self.assertFalse((run/'STOP').exists())
            self.assertFalse((run/'first-error.json').exists())
        history = ([json.loads(line) for line in (run/'jobs.jsonl').read_text().splitlines()]
                   if (run/'jobs.jsonl').exists() else [])
        if not fatal:
            successful = [row for row in history if row['result']['rc'] == 0]
            self.assertEqual(len(successful), len(names))
            self.assertEqual(len({row['task']['id'] for row in successful}), len(names))
            self.assertCountEqual([row['task']['names'][0] for row in successful], names)
        for actor_file in (run/'workers').glob('worker-*.json'):
            if actor_file.name.endswith('-first-stop.json'):
                if fatal:
                    continue
                self.fail('private worker did not finish gracefully: '+actor_file.name)
            actor = rm24.load(actor_file)
            proc_stat = Path('/proc/'+str(actor['pid'])+'/stat')
            self.assertFalse(proc_stat.exists(), 'fixture worker process leaked')
        return run, checks, transports, history

    def test_shared_queue_exactly_once_and_joined_node_retained_for_retry(self):
        run, checks, transports, history = self.integration(fail_first=True)
        self.assertEqual(rm24.load(run/'runtime-acks/integration.json')['state'], 'accepted')
        self.assertTrue(any(row['node'] == 'worker-1' for row in history))
        self.assertIn('worker-1', rm24.load(run/'status.json')['runtime_join']['accepted'])
        self.assertEqual(transports.count('worker-1'), 2)  # main + bounded retry fleet
        self.assertEqual(checks[:2], [['worker-0'], ['worker-1']])

    def test_preflight_failure_does_not_abort_original_worker(self):
        run, checks, transports, history = self.integration(join_failure='preflight')
        self.assertEqual(rm24.load(run/'runtime-acks/integration.json')['state'], 'rejected')
        self.assertEqual(transports, ['worker-0'])

    def test_ready_failure_does_not_claim_tasks_or_abort_original_worker(self):
        run, checks, transports, history = self.integration(join_failure='ready')
        self.assertEqual(rm24.load(run/'runtime-acks/integration.json')['state'], 'rejected')
        self.assertTrue(all(row['node'] == 'worker-0' for row in history))
        self.assertEqual(rm24.load(run/'status.json')['runtime_join']['accepted'], [])

    def test_unclaimed_join_cleanup_broken_pipe_only_rejects_that_join(self):
        run, checks, transports, history = self.integration(join_failure='ready', cleanup_failure='unclaimed')
        ack = rm24.load(run/'runtime-acks/integration.json')
        self.assertEqual(ack['state'], 'rejected')
        self.assertIn('cleanup failed', ack['reason'])
        self.assertTrue(all(row['node'] == 'worker-0' for row in history))

    def test_claimed_join_cleanup_failure_keeps_fatal_safety_policy(self):
        run, checks, transports, history = self.integration(cleanup_failure='claimed')
        self.assertIn('dispatch cleanup failed', rm24.load(run/'first-error.json')['reason'])

    def test_runtime_future_exception_stops_workers_with_pending_queue(self):
        self.integration(fleet_failure='future')

    def test_runtime_poll_exception_stops_workers_with_pending_queue(self):
        self.integration(fleet_failure='poll')


if __name__ == '__main__':
    unittest.main()
