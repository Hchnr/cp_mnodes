import concurrent.futures
import pathlib
import unittest
from unittest import mock

import migration_broker_sharded as candidate
import migration_broker_parallel as parallel
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
        original = parallel.atomic_json

        def fail(path, value):
            if pathlib.Path(path).parent.parent.name == 'running':
                raise OSError('synthetic shard publication failure')
            return original(path, value)

        with mock.patch.object(parallel, 'atomic_json', fail):
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
        # write_json still ensures the selected marker parent exists. Each
        # claim touches one shard, rather than preparing all 64 again.
        self.assertEqual(len(shard_creations), 2)


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
