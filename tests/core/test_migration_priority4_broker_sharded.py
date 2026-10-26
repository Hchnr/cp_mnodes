"""Run the established sharding/lease contract against the independent P4 scope."""

import unittest
from unittest import mock

import migration_priority4_broker_sharded as candidate
import test_broker_sharded as contracts
from migration_io import write_json
from test_migration_priority4_broker import Fixture as Priority4Fixture


class Priority4ShardedBrokerTests(contracts.BrokerShardedTests):
    def setUp(self):
        super().setUp()

        def fixture(tasks=3, old=False):
            # The deployed predecessor for P4 is its flat compatible broker,
            # not the original eai_pwm-only legacy entrypoint.
            return Priority4Fixture(tasks=tasks, old=False)

        for name, value in (('candidate', candidate), ('Fixture', fixture), ('STAGE', 'p4-r1')):
            patch = mock.patch.object(contracts, name, value)
            patch.start()
            self.addCleanup(patch.stop)

    def test_uneven_empty_shards_and_stale_candidates_are_skipped_without_duplicate_leases(self):
        fixture = self.fixture(tasks=0)
        ids = [0, 64, 128, 192, 1, 65, 130, 191]
        for task_id in ids:
            task = {'id': task_id, 'phase': 4, 'round': 1, 'op': 'copy',
                    'entries': [{'rel': 'data-' + str(task_id), 'kind': 'file', 'size': 1}]}
            fixture.tasks[task_id] = task
            write_json(fixture.pending(task_id), task)
        upgraded = fixture.open(candidate.Broker)
        first = upgraded.claim('p4-r1', 'node-a-0')
        first_id = first['task']['id']
        # READDIR order within a shard is filesystem-dependent. The broker
        # promises round-robin shards, not sorted task IDs within each shard.
        self.assertEqual(first_id % 64, 0)
        with upgraded.ready_lock:
            cached = list(upgraded.ready['p4-r1'])[:3]
        self.assertEqual([int(path.stem) % 64 for path in cached], [1, 2, 63])
        write_json(fixture.pending(first_id), fixture.tasks[first_id])
        fixture.pending(65).unlink()
        with upgraded.ready_lock:
            upgraded.ready['p4-r1'].appendleft(fixture.pending(first_id))
        claims = [upgraded.claim('p4-r1', 'node-a-' + str(i)) for i in range(1, 7)]
        selected = [first['task']['id']] + [claim['task']['id'] for claim in claims]
        self.assertEqual(set(selected), set(ids) - {65})
        self.assertEqual(len(selected), len(set(selected)))
        # The deliberately vanished task can be the first name in shard 1;
        # that hole must not force a sorted or strict post-removal sequence.
        self.assertEqual(upgraded.claim('p4-r1', 'node-a-0'), first)


if __name__ == '__main__':
    unittest.main()
