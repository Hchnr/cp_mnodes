#!/usr/bin/env python3
"""Parallel broker with 64 independent running-marker directories.

The local SQLite database, including every pre-existing lease and its exact
running path, is unchanged. Only newly allocated leases use sharded markers.
Directory creation happens once per stage before new assignments are accepted.
"""
import re
import threading

from migration_broker_parallel import Broker as ParallelBroker


class Broker(ParallelBroker):
    def __init__(self, control):
        super().__init__(control)
        self.running_prepare_lock = threading.Lock()
        self.prepared_running_stages = set()
        stage = self._state().get('stage')
        if isinstance(stage, str) and re.fullmatch(r'p[123]-r[123]', stage):
            self._prepare_running(stage)

    def _prepare_running(self, stage):
        # The common path only reads an already populated in-process set. The
        # one-time preparation never holds the SQLite mutex.
        if stage in self.prepared_running_stages:
            return
        with self.running_prepare_lock:
            if stage in self.prepared_running_stages:
                return
            with self.fs_slots:
                root = self.control / 'stages' / stage / 'running'
                root.mkdir(parents=True, exist_ok=True)
                for shard in range(64):
                    (root / str(shard)).mkdir(exist_ok=True)
            self.prepared_running_stages.add(stage)

    def running_path(self, stage, owner, task_id):
        self._prepare_running(stage)
        return self.control / 'stages' / stage / 'running' / str(task_id % 64) / (
            'broker--' + owner + '--' + str(task_id) + '.json')


def main():
    # Keep authentication, port, local singleton lock and HTTP contract intact.
    import migration_broker
    migration_broker.Broker = Broker
    migration_broker.main()


if __name__ == '__main__':
    main()
