import concurrent.futures
import pathlib
import threading
import time
import unittest
from unittest import mock

import migration_broker_sharded as candidate
from migration_io import read_json, write_json
from migration_run import queued_count
from test_broker_parallel import Fixture, STAGE


class BrokerShardedTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = []

    def tearDown(self):
        for fixture in reversed(self.fixtures):
            fixture.close()

    def fixture(self, tasks=3, old=False):
        fixture = Fixture(tasks=tasks, old=old)
        self.fixtures.append(fixture)
        return fixture

    def test_new_markers_use_64_shards_and_all_leases_are_unique(self):
        fixture = self.fixture(tasks=128)
        upgraded = fixture.open(candidate.Broker)
        fixture.broker = upgraded
        self.assertTrue(all((fixture.root / 'running' / str(i)).is_dir() for i in range(64)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            claims = list(pool.map(lambda i: upgraded.claim(STAGE, 'node-a-' + str(i)), range(128)))
        self.assertEqual({claim['task']['id'] for claim in claims}, set(range(128)))
        self.assertEqual(len({claim['lease'] for claim in claims}), 128)
        for claim in claims:
            task_id = claim['task']['id']
            running = pathlib.Path(fixture.row(task_id)[3])
            self.assertEqual(running.parent, fixture.root / 'running' / str(task_id % 64))
            self.assertEqual(read_json(running), claim['task'])
        self.assertEqual(queued_count(fixture.root), (0, 128))

    def test_consecutive_claim_windows_cover_all_64_shards_without_rescanning(self):
        fixture = self.fixture(tasks=128)
        upgraded = fixture.open(candidate.Broker)
        original = pathlib.Path.iterdir
        scans = []

        def counted(path):
            if path.parent.name == 'pending':
                scans.append(path)
            return original(path)

        with mock.patch.object(pathlib.Path, 'iterdir', counted):
            claims = [upgraded.claim(STAGE, 'node-a-' + str(i)) for i in range(128)]
        ids = [claim['task']['id'] for claim in claims]
        self.assertEqual(set(ids), set(range(128)))
        for start in range(65):
            self.assertEqual({task_id % 64 for task_id in ids[start:start + 64]}, set(range(64)))
        self.assertEqual(len(scans), 64)

    def test_pending_refill_reads_eight_shards_concurrently_without_sqlite_lock(self):
        fixture = self.fixture(tasks=128)
        upgraded = fixture.open(candidate.Broker)
        original = pathlib.Path.iterdir
        lock = threading.Lock()
        entered, release = threading.Event(), threading.Event()
        active = peak = 0
        readers = set()

        def blocked(path):
            nonlocal active, peak
            if path.parent.name != 'pending':
                return original(path)
            with lock:
                active += 1
                peak = max(peak, active)
                readers.add(threading.get_ident())
                if active == 8:
                    entered.set()
            try:
                if not release.wait(5):
                    raise RuntimeError('shard reader gate was not released')
                return list(original(path))
            finally:
                with lock:
                    active -= 1

        with mock.patch.object(pathlib.Path, 'iterdir', blocked), \
                concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(upgraded._candidate, STAGE)
            try:
                self.assertTrue(entered.wait(3), 'eight shard readers did not overlap')
                self.assertTrue(upgraded.lock.acquire(blocking=False), 'scan held SQLite mutex')
                upgraded.lock.release()
                self.assertEqual(peak, 8)
            finally:
                release.set()
            first = pending.result(timeout=3)
        paths = [first] + [upgraded._candidate(STAGE) for _ in range(127)]
        self.assertEqual(len(set(paths)), 128)
        self.assertEqual({int(path.stem) for path in paths}, set(range(128)))
        for start in range(65):
            self.assertEqual({int(path.stem) % 64 for path in paths[start:start + 64]}, set(range(64)))
        self.assertGreater(len(readers), 1)
        self.assertLessEqual(len(readers), 8)

    def test_pending_parallel_readers_obey_smaller_shared_fs_slot_budget(self):
        fixture = self.fixture(tasks=64)
        upgraded = fixture.open(candidate.Broker)
        upgraded.fs_slots = threading.BoundedSemaphore(2)
        original = pathlib.Path.iterdir
        lock = threading.Lock()
        entered, release = threading.Event(), threading.Event()
        active = peak = 0

        def blocked(path):
            nonlocal active, peak
            if path.parent.name != 'pending':
                return original(path)
            with lock:
                active += 1
                peak = max(peak, active)
                if active == 2:
                    entered.set()
            try:
                if not release.wait(5):
                    raise RuntimeError('shared metadata slot gate was not released')
                return list(original(path))
            finally:
                with lock:
                    active -= 1

        with mock.patch.object(pathlib.Path, 'iterdir', blocked), \
                concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(upgraded._candidate, STAGE)
            try:
                self.assertTrue(entered.wait(3))
                self.assertEqual(peak, 2)
                self.assertFalse(upgraded.fs_slots.acquire(blocking=False),
                                 'directory readers bypassed the shared fs_slots budget')
            finally:
                release.set()
            self.assertIsNotNone(pending.result(timeout=3))
        self.assertEqual(peak, 2)
        self.assertEqual(active, 0)
        self.assertTrue(upgraded.fs_slots.acquire(blocking=False))
        self.assertTrue(upgraded.fs_slots.acquire(blocking=False))
        self.assertFalse(upgraded.fs_slots.acquire(blocking=False))
        upgraded.fs_slots.release()
        upgraded.fs_slots.release()

    def test_pending_enumeration_error_is_not_empty_or_partial_cache(self):
        fixture = self.fixture(tasks=128)
        upgraded = fixture.open(candidate.Broker)
        original = pathlib.Path.iterdir

        def fail(path):
            if path.parent.name == 'pending' and path.name == '3':
                raise PermissionError('synthetic pending shard failure')
            return original(path)

        with mock.patch.object(pathlib.Path, 'iterdir', fail):
            with self.assertRaisesRegex(PermissionError, 'synthetic pending shard failure'):
                upgraded._candidate(STAGE)
        self.assertFalse(upgraded.ready[STAGE])
        self.assertNotIn(STAGE, upgraded.next_scan)
        with upgraded.lock:
            self.assertEqual(upgraded.db.execute('SELECT COUNT(*) FROM claims').fetchone()[0], 0)
        paths = [upgraded._candidate(STAGE) for _ in range(128)]
        self.assertEqual(len(set(paths)), 128)
        self.assertEqual({int(path.stem) for path in paths}, set(range(128)))

    def test_64_simultaneous_publishers_use_64_distinct_directory_locks(self):
        fixture = self.fixture(tasks=128)
        upgraded = fixture.open(candidate.Broker)
        upgraded.state_cache_until = time.monotonic() + 60
        entered, release = threading.Event(), threading.Event()
        lock, parents = threading.Lock(), []
        original = upgraded.publish_json

        def gated(path, value):
            with lock:
                parents.append(path.parent)
                if len(parents) == 64:
                    entered.set()
            if not release.wait(5):
                raise RuntimeError('publisher gate was not released')
            return original(path, value)

        with mock.patch.object(upgraded, 'publish_json', gated), concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            futures = [pool.submit(upgraded.claim, STAGE, 'node-a-' + str(i)) for i in range(64)]
            try:
                self.assertTrue(entered.wait(4), '64 publishers did not enter independent FS slots')
                self.assertEqual({parent.name for parent in parents}, {str(i) for i in range(64)})
                self.assertEqual(len(set(parents)), 64)
            finally:
                release.set()
            claims = [future.result(timeout=3) for future in futures]
        self.assertEqual(len({claim['task']['id'] for claim in claims}), 64)
        self.assertEqual(len({claim['lease'] for claim in claims}), 64)

    def test_uneven_empty_shards_and_stale_candidates_are_skipped_without_duplicate_leases(self):
        fixture = self.fixture(tasks=0)
        ids = [0, 64, 128, 192, 1, 65, 130, 191]
        for task_id in ids:
            task = {'id': task_id, 'phase': 3, 'round': 1, 'op': 'copy',
                    'entries': [{'rel': 'data-' + str(task_id), 'kind': 'file', 'size': 1}]}
            fixture.tasks[task_id] = task
            write_json(fixture.pending(task_id), task)
        upgraded = fixture.open(candidate.Broker)
        first = upgraded.claim(STAGE, 'node-a-0')
        self.assertEqual(first['task']['id'], 0)
        # Simulate an old NFS pending entry plus a candidate disappearing after
        # ready-cache enumeration; SQLite remains the assignment authority.
        write_json(fixture.pending(0), fixture.tasks[0])
        fixture.pending(65).unlink()
        with upgraded.ready_lock:
            upgraded.ready[STAGE].appendleft(fixture.pending(0))
        claims = [upgraded.claim(STAGE, 'node-a-' + str(i)) for i in range(1, 7)]
        selected = [first['task']['id']] + [claim['task']['id'] for claim in claims]
        self.assertEqual(set(selected), set(ids) - {65})
        self.assertEqual(len(selected), len(set(selected)))
        self.assertEqual(selected[:4], [0, 1, 130, 191])
        replay = upgraded.claim(STAGE, 'node-a-0')
        self.assertEqual(replay, first)

    def test_legacy_lease_and_completion_keep_their_persisted_flat_paths(self):
        fixture = self.fixture(tasks=3, old=True)
        first = fixture.broker.claim(STAGE, 'node-a-0')
        second = fixture.broker.claim(STAGE, 'node-b-0')
        old_running = pathlib.Path(fixture.row(first['task']['id'])[3])
        fixture.broker.done(fixture.completion(second))
        # Recover the durable claim even when publication had not completed.
        old_running.unlink()
        upgraded = fixture.open(candidate.Broker)
        fixture.broker = upgraded
        recovered = upgraded.claim(STAGE, 'node-a-0')
        self.assertEqual(recovered, first)
        self.assertEqual(pathlib.Path(fixture.row(first['task']['id'])[3]), old_running)
        self.assertEqual(old_running.parent, fixture.root / 'running')
        self.assertTrue(old_running.exists())
        self.assertTrue(upgraded.done(fixture.completion(second))['already_done'])
        fresh = upgraded.claim(STAGE, 'node-c-0')
        self.assertEqual(pathlib.Path(fixture.row(fresh['task']['id'])[3]).parent.name, '2')
        self.assertEqual(queued_count(fixture.root), (0, 2))
        self.assertTrue(upgraded.done(fixture.completion(recovered))['ok'])
        self.assertFalse(old_running.exists())
        self.assertTrue(upgraded.done(fixture.completion(fresh))['ok'])
        self.assertEqual(queued_count(fixture.root), (0, 0))

    def test_same_owner_retries_and_restarted_broker_keep_one_sharded_lease(self):
        fixture = self.fixture(tasks=8)
        upgraded = fixture.open(candidate.Broker)
        fixture.broker = upgraded
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            claims = list(pool.map(lambda _: upgraded.claim(STAGE, 'node-a-127'), range(12)))
        self.assertEqual(len({claim['task']['id'] for claim in claims}), 1)
        self.assertEqual(len({claim['lease'] for claim in claims}), 1)
        before = fixture.row(claims[0]['task']['id'])
        running = pathlib.Path(before[3])
        running.unlink()
        restarted = fixture.open(candidate.Broker)
        replay = restarted.claim(STAGE, 'node-a-127')
        self.assertEqual(replay, claims[0])
        self.assertTrue(running.exists())
        self.assertTrue(restarted.done(fixture.completion(replay))['ok'])
        self.assertFalse(running.exists())
        self.assertTrue(restarted.done(fixture.completion(replay))['already_done'])

    def test_missing_running_publication_preserves_pending_and_assignment(self):
        fixture = self.fixture(tasks=1)
        upgraded = fixture.open(candidate.Broker)
        fixture.broker = upgraded
        original = candidate.write_json

        def fail(path, value, **kwargs):
            if pathlib.Path(path).parent.parent.name == 'running':
                raise OSError('synthetic shard publication failure')
            return original(path, value, **kwargs)

        with mock.patch.object(candidate, 'write_json', fail):
            with self.assertRaisesRegex(OSError, 'synthetic'):
                upgraded.claim(STAGE, 'node-a-0')
        before = fixture.row(0)
        self.assertEqual(before[2], 'leased')
        self.assertTrue(fixture.pending(0).exists())
        replay = upgraded.claim(STAGE, 'node-a-0')
        self.assertEqual(replay['lease'], before[1])
        self.assertTrue(pathlib.Path(before[3]).exists())
        self.assertFalse(fixture.pending(0).exists())

    def test_hot_path_never_recreates_all_running_shards(self):
        fixture = self.fixture(tasks=2)
        upgraded = fixture.open(candidate.Broker)
        original = pathlib.Path.mkdir
        shard_creations = []

        def count(path, *args, **kwargs):
            if path.parent.name == 'running' and path.name.isdigit():
                shard_creations.append(path)
            return original(path, *args, **kwargs)

        with mock.patch.object(pathlib.Path, 'mkdir', count):
            upgraded.claim(STAGE, 'node-a-0')
            upgraded.claim(STAGE, 'node-b-0')
        self.assertEqual(shard_creations, [])

    def test_hot_running_and_done_publish_without_parent_mkdir(self):
        fixture = self.fixture(tasks=1)
        upgraded = fixture.open(candidate.Broker)
        original = pathlib.Path.mkdir
        touched = []

        def count(path, *args, **kwargs):
            touched.append(path)
            return original(path, *args, **kwargs)

        with mock.patch.object(pathlib.Path, 'mkdir', count):
            claim = upgraded.claim(STAGE, 'node-a-0')
            self.assertTrue(upgraded.done(fixture.completion(claim))['ok'])
        self.assertEqual(touched, [])

    def test_missing_prepared_parent_fails_without_losing_lease_or_done_retry(self):
        fixture = self.fixture(tasks=1)
        upgraded = fixture.open(candidate.Broker)
        fixture.broker = upgraded
        running_parent = fixture.root / 'running' / '0'
        running_parent.rmdir()
        with self.assertRaises(FileNotFoundError):
            upgraded.claim(STAGE, 'node-a-0')
        before = fixture.row(0)
        self.assertEqual(before[2], 'leased')
        self.assertTrue(fixture.pending(0).exists())
        running_parent.mkdir()
        replay = upgraded.claim(STAGE, 'node-a-0')
        self.assertEqual(replay['lease'], before[1])
        done_parent = fixture.root / 'done' / '0'
        done_parent.rmdir()
        with self.assertRaises(FileNotFoundError):
            upgraded.done(fixture.completion(replay))
        self.assertEqual(fixture.row(0)[2], 'leased')
        self.assertTrue(pathlib.Path(before[3]).exists())
        done_parent.mkdir()
        self.assertTrue(upgraded.done(fixture.completion(replay))['ok'])
        self.assertEqual(fixture.row(0)[2], 'done')

    def test_retried_receipt_keeps_immutable_canonical_and_original_copy_proof(self):
        fixture = self.fixture(tasks=1)
        upgraded = fixture.open(candidate.Broker)
        fixture.broker = upgraded
        claim = upgraded.claim(STAGE, 'node-a-0')
        target = fixture.done_path(0)
        write_json(target, fixture.completion(claim)['receipt'])
        original_inode = target.stat().st_ino
        original_bytes = target.read_bytes()
        self.assertTrue(upgraded.done(fixture.completion(claim, status='skipped'))['ok'])
        self.assertEqual(target.stat().st_ino, original_inode)
        self.assertEqual(target.read_bytes(), original_bytes)
        result = read_json(target)['results'][0]
        self.assertEqual(result['status'], 'copied')
        self.assertEqual(result['bytes'], 17)
        self.assertTrue(result['replay_reused_copy'])


class ShardedQueueCountTests(unittest.TestCase):
    def setUp(self):
        self.fixture = Fixture(tasks=0)
        self.addCleanup(self.fixture.close)
        self.root = self.fixture.root

    def test_legacy_mixed_sharded_busy_and_empty(self):
        self.assertEqual(queued_count(self.root), (0, 0))
        old = self.root / 'running' / 'broker--legacy--1.json'
        new = self.root / 'running' / '63' / 'broker--new--127.json'
        write_json(old, {'id': 1})
        write_json(new, {'id': 127})
        write_json(self.root / 'pending' / '0' / '128.json', {'id': 128})
        self.assertEqual(queued_count(self.root), (1, 2))
        old.unlink()
        self.assertEqual(queued_count(self.root), (1, 1))
        new.unlink()
        (self.root / 'pending' / '0' / '128.json').unlink()
        self.assertEqual(queued_count(self.root), (0, 0))

    def test_snapshots_temporary_files_and_unknown_directories_are_not_tasks(self):
        write_json(self.root / 'running' / '.snapshots' / 'old.json', {'kept': True})
        (self.root / 'running' / '.tmp').touch()
        (self.root / 'running' / 'unknown').mkdir()
        self.assertEqual(queued_count(self.root), (0, 0))

    def test_numeric_shard_symlink_is_rejected_without_traversal(self):
        outside = self.fixture.control / 'outside'
        outside.mkdir()
        (outside / 'secret.json').write_text('{}')
        (self.root / 'running' / '0').symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            queued_count(self.root)

    def test_task_symlink_or_directory_cannot_produce_false_empty(self):
        outside = self.fixture.control / 'outside.json'
        outside.write_text('{}')
        task = self.root / 'running' / 'task.json'
        task.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'regular file'):
            queued_count(self.root)
        task.unlink()
        task.mkdir()
        with self.assertRaisesRegex(ValueError, 'regular file'):
            queued_count(self.root)


if __name__ == '__main__':
    unittest.main()
