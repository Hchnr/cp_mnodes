"""Local correctness/concurrency fixtures; no remote calls or payload benchmarks.

Run: python3 -m unittest -v work/test_broker_parallel.py
Every slow-I/O gate is synthetic and explicitly released in finally blocks.
"""
import collections
import concurrent.futures
import contextlib
import copy
import json
import pathlib
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import migration_broker as legacy
import migration_broker_parallel as candidate
from migration_io import read_json, write_json

STAGE = 'p3-r1'


def is_running(path):
    return pathlib.Path(path).parent.name == 'running'


def is_done(path):
    return pathlib.Path(path).parent.parent.name == 'done'


@contextlib.contextmanager
def patch_io(name, value):
    with contextlib.ExitStack() as stack:
        for module in (legacy, candidate):
            if hasattr(module, name):
                stack.enter_context(mock.patch.object(module, name, value))
        yield


class Fixture:
    def __init__(self, tasks=3, old=False):
        self.temp = tempfile.TemporaryDirectory(prefix='broker-parallel-', dir=pathlib.Path(__file__).parent)
        self.control = pathlib.Path(self.temp.name).resolve()
        self.run_id = 'broker-parallel-test-' + uuid.uuid4().hex
        write_json(self.control / 'config.json', {'run_id': self.run_id, 'broker_token': 'local-fixture'})
        write_json(self.control / 'state.json', {'stage': STAGE, 'status': 'copying'})
        self.root = self.control / 'stages' / STAGE
        (self.root / 'running').mkdir(parents=True)
        for shard in range(64):
            (self.root / 'pending' / str(shard)).mkdir(parents=True)
            (self.root / 'done' / str(shard)).mkdir(parents=True)
        self.tasks = {}
        for task_id in range(tasks):
            task = {'id': task_id, 'phase': 3, 'round': 1, 'op': 'copy',
                    'entries': [{'rel': 'data-' + str(task_id), 'kind': 'file', 'size': task_id + 17}]}
            self.tasks[task_id] = task
            write_json(self.pending(task_id), task)
        self.brokers = []
        self.broker = self.open(legacy.Broker if old else candidate.Broker)

    def open(self, cls=candidate.Broker):
        broker = cls(self.control)
        self.brokers.append(broker)
        return broker

    def pending(self, task_id):
        return self.root / 'pending' / str(task_id % 64) / (str(task_id) + '.json')

    def done_path(self, task_id):
        return self.root / 'done' / str(task_id % 64) / (str(task_id) + '.json')

    def row(self, task_id):
        with self.broker.lock:
            return self.broker.db.execute('SELECT owner,lease,status,running FROM claims WHERE stage=? AND id=?',
                                          (STAGE, task_id)).fetchone()

    def completion(self, claim, status='copied'):
        task = claim['task']
        return {'stage': STAGE, 'id': task['id'], 'lease': claim['lease'],
                'receipt': {'id': task['id'], 'host': 'fixture-host', 'thread': 64,
                            'results': [{'rel': e['rel'], 'kind': e['kind'], 'status': status,
                                         'bytes': e['size'] if status == 'copied' else 0,
                                         'source_identity': {'ino': 100 + task['id'], 'size': e['size']},
                                         'dst_stat': {'ino': 200 + task['id'], 'size': e['size']},
                                         'sha256': 'verified-fixture-checksum'} for e in task['entries']]}}

    def close(self):
        for broker in self.brokers:
            broker.db.close()
        for suffix in ('-broker.sqlite', '-broker.sqlite-wal', '-broker.sqlite-shm'):
            pathlib.Path('/tmp/' + self.run_id + suffix).unlink(missing_ok=True)
        self.temp.cleanup()


class BrokerParallelTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = []

    def tearDown(self):
        for fixture in reversed(self.fixtures):
            fixture.close()

    def fixture(self, *args, **kwargs):
        fixture = Fixture(*args, **kwargs)
        self.fixtures.append(fixture)
        return fixture

    def test_slow_running_publication_does_not_block_other_task_claim_and_done(self):
        fixture = self.fixture()
        entered, release = threading.Event(), threading.Event()
        original = legacy.atomic_json
        def slow(path, value):
            if is_running(path) and value.get('id') == 0:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError('test gate was not released')
            return original(path, value)
        with patch_io('atomic_json', slow), concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(fixture.broker.claim, STAGE, 'node-a-0')
            try:
                self.assertTrue(entered.wait(2), 'first claim did not reach its synthetic NFS gate')
                second = pool.submit(fixture.broker.claim, STAGE, 'node-b-64').result(timeout=2)
                self.assertEqual(second['task']['id'], 1)
                self.assertTrue(pool.submit(fixture.broker.done, fixture.completion(second)).result(timeout=2)['ok'])
                self.assertFalse(release.is_set(), 'independent task only progressed after the I/O gate opened')
            finally:
                release.set()
            self.assertEqual(first.result(timeout=2)['task']['id'], 0)

    def test_same_owner_concurrent_gets_return_one_task_and_one_lease(self):
        fixture = self.fixture(tasks=8)
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: fixture.broker.claim(STAGE, 'node-a-127'), range(12)))
        self.assertEqual(len({r['task']['id'] for r in results}), 1)
        self.assertEqual(len({r['lease'] for r in results}), 1)
        with fixture.broker.lock:
            live = fixture.broker.db.execute("SELECT COUNT(*) FROM claims WHERE owner=? AND status='leased'",
                                             ('node-a-127',)).fetchone()[0]
        self.assertEqual(live, 1)
        self.assertEqual(sum(fixture.pending(i).exists() for i in range(8)), 7)

    def test_slow_receipt_publication_does_not_block_other_task_claim_and_done(self):
        fixture = self.fixture(tasks=2)
        first = fixture.broker.claim(STAGE, 'node-a-0')
        entered, release = threading.Event(), threading.Event()
        original = legacy.atomic_json
        def slow(path, value):
            if is_done(path) and value.get('id') == 0:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError('test gate was not released')
            return original(path, value)
        with patch_io('atomic_json', slow), concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            blocked_done = pool.submit(fixture.broker.done, fixture.completion(first))
            try:
                self.assertTrue(entered.wait(2))
                second = pool.submit(fixture.broker.claim, STAGE, 'node-b-64').result(timeout=2)
                self.assertEqual(second['task']['id'], 1)
                response = pool.submit(fixture.broker.done, fixture.completion(second)).result(timeout=2)
                self.assertTrue(response['ok'])
                self.assertFalse(release.is_set())
            finally:
                release.set()
            self.assertTrue(blocked_done.result(timeout=2)['ok'])

    def test_concurrent_done_publishes_receipt_only_once(self):
        fixture = self.fixture(tasks=1)
        claim = fixture.broker.claim(STAGE, 'node-a-64')
        publications, lock = [], threading.Lock()
        original = legacy.atomic_json
        def counted(path, value):
            if is_done(path):
                with lock:
                    publications.append(str(path))
            return original(path, value)
        payload = fixture.completion(claim)
        with patch_io('atomic_json', counted), concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            responses = list(pool.map(lambda _: fixture.broker.done(copy.deepcopy(payload)), range(12)))
        self.assertTrue(all(r['ok'] for r in responses))
        self.assertEqual(len(publications), 1)
        self.assertEqual(fixture.row(0)[2], 'done')
        self.assertFalse(pathlib.Path(fixture.row(0)[3]).exists())

    def test_get_of_old_lease_racing_done_never_resurrects_running(self):
        fixture = self.fixture(tasks=1)
        claim = fixture.broker.claim(STAGE, 'node-a-64')
        entered, release = threading.Event(), threading.Event()
        task_lock = fixture.broker.task_locks[(STAGE, 0)]
        class GatedLock:
            def __enter__(self):
                if threading.current_thread().name.startswith('old-lease-get'):
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError('test gate was not released')
                task_lock.acquire()
                return self
            def __exit__(self, *args):
                task_lock.release()
        fixture.broker.task_locks[(STAGE, 0)] = GatedLock()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix='old-lease-get') as pool:
            old_get = pool.submit(fixture.broker.claim, STAGE, 'node-a-64')
            try:
                self.assertTrue(entered.wait(2))
                fixture.broker.done(fixture.completion(claim))
            finally:
                release.set()
            response = old_get.result(timeout=2)
        self.assertIsNone(response['task'])
        self.assertFalse(pathlib.Path(fixture.row(0)[3]).exists())
        self.assertEqual(fixture.row(0)[2], 'done')

    def test_published_receipt_with_leased_db_recovers_copied_ownership_proof(self):
        fixture = self.fixture(tasks=1)
        claim = fixture.broker.claim(STAGE, 'node-a-64')
        original = fixture.completion(claim)
        write_json(fixture.done_path(0), original['receipt'])
        self.assertEqual(fixture.row(0)[2], 'leased')
        replay = fixture.completion(claim, status='skipped')
        self.assertTrue(fixture.broker.done(replay)['ok'])
        result = read_json(fixture.done_path(0))['results'][0]
        self.assertEqual(result['status'], 'copied')
        self.assertEqual(result['bytes'], 17)
        self.assertTrue(result['replay_reused_copy'])
        self.assertEqual(fixture.row(0)[2], 'done')

    def test_running_publication_failure_preserves_pending_and_recoverable_assignment(self):
        fixture = self.fixture(tasks=1)
        original = legacy.atomic_json
        def fail(path, value):
            if is_running(path):
                raise OSError('synthetic NFS running publication failure')
            return original(path, value)
        with patch_io('atomic_json', fail):
            with self.assertRaises(OSError):
                fixture.broker.claim(STAGE, 'node-a-64')
        self.assertTrue(fixture.pending(0).exists())
        before = fixture.row(0)
        recovered = fixture.broker.claim(STAGE, 'node-a-64')
        self.assertEqual(recovered['task']['id'], 0)
        if before is not None:
            self.assertEqual(recovered['lease'], before[1])
        self.assertTrue(pathlib.Path(fixture.row(0)[3]).exists())
        self.assertFalse(fixture.pending(0).exists())

    def test_receipt_publication_failure_preserves_running_and_leased_db(self):
        fixture = self.fixture(tasks=1)
        claim = fixture.broker.claim(STAGE, 'node-a-64')
        running = pathlib.Path(fixture.row(0)[3])
        original = legacy.atomic_json
        def fail(path, value):
            if is_done(path):
                raise OSError('synthetic NFS receipt publication failure')
            return original(path, value)
        with patch_io('atomic_json', fail):
            with self.assertRaises(OSError):
                fixture.broker.done(fixture.completion(claim))
        self.assertTrue(running.exists())
        self.assertEqual(fixture.row(0)[2], 'leased')
        self.assertTrue(fixture.broker.done(fixture.completion(claim))['ok'])
        self.assertFalse(running.exists())
        self.assertEqual(fixture.row(0)[2], 'done')

    def test_drain_after_pending_read_before_insert_prevents_fresh_claim(self):
        fixture = self.fixture(tasks=1)
        entered, release = threading.Event(), threading.Event()
        original = legacy.read_json
        def gate(path):
            value = original(path)
            if pathlib.Path(path) == fixture.pending(0):
                entered.set()
                if not release.wait(5):
                    raise RuntimeError('test gate was not released')
            return value
        with patch_io('read_json', gate), concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(fixture.broker.claim, STAGE, 'node-a-64')
            try:
                self.assertTrue(entered.wait(2))
                fixture.broker.update_control({'draining_hosts': ['node-a']})
            finally:
                release.set()
            response = future.result(timeout=2)
        self.assertIsNone(response['task'])
        self.assertTrue(response.get('draining'))
        self.assertIsNone(fixture.row(0))
        self.assertTrue(fixture.pending(0).exists())

    def test_new_broker_recovers_legacy_sqlite_leased_and_done(self):
        fixture = self.fixture(tasks=2, old=True)
        first = fixture.broker.claim(STAGE, 'node-a-0')
        second = fixture.broker.claim(STAGE, 'node-b-64')
        fixture.broker.done(fixture.completion(second))
        upgraded = fixture.open()
        recovered = upgraded.claim(STAGE, 'node-a-0')
        self.assertEqual(recovered['task'], first['task'])
        self.assertEqual(recovered['lease'], first['lease'])
        self.assertTrue(upgraded.done(fixture.completion(second))['already_done'])
        self.assertTrue(upgraded.done(fixture.completion(recovered))['ok'])
        with upgraded.lock:
            rows = upgraded.db.execute('SELECT status,COUNT(*) FROM claims GROUP BY status').fetchall()
        self.assertEqual(rows, [('done', 2)])


if __name__ == '__main__':
    unittest.main(verbosity=2)
