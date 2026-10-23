#!/usr/bin/env python3
"""Parallel broker with 64 independent running-marker directories.

The local SQLite database, including every pre-existing lease and its exact
running path, is unchanged. Only newly allocated leases use sharded markers.
Directory creation happens once per stage before new assignments are accepted.
"""
import collections
import concurrent.futures
import re
import threading
import time
from pathlib import Path

from migration_broker_parallel import Broker as ParallelBroker
from migration_io import write_json


class Broker(ParallelBroker):
    def __init__(self, control):
        super().__init__(control)
        self.running_prepare_lock = threading.Lock()
        self.prepared_running_stages = set()
        self.prepared_queue_parents = set()
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
                parents = {root}
                done = self.control / 'stages' / stage / 'done'
                done.mkdir(parents=True, exist_ok=True)
                for shard in range(64):
                    for parent in (root / str(shard), done / str(shard)):
                        parent.mkdir(exist_ok=True)
                        parents.add(parent)
            self.prepared_queue_parents.update(parents)
            self.prepared_running_stages.add(stage)

    def running_path(self, stage, owner, task_id):
        self._prepare_running(stage)
        return self.control / 'stages' / stage / 'running' / str(task_id % 64) / (
            'broker--' + owner + '--' + str(task_id) + '.json')

    def publish_json(self, path, value):
        path = Path(path)
        if path.parent in self.prepared_queue_parents:
            # mkdir(EEXIST) is still a directory mutation on the shared mount.
            # Removing it here avoids taking the same parent inode lock for
            # every claim/completion while retaining immutable publication.
            return write_json(path, value, ensure_parent=False)
        return super().publish_json(path, value)

    def _candidate(self, stage):
        # Allocation order must be spread as well as marker placement. Draining
        # one pending shard first would send every concurrent publisher to the
        # same running/done directory despite those directories being sharded.
        with self.ready_lock:
            ready = self.ready.setdefault(stage, collections.deque())
            if not ready and time.monotonic() >= self.next_scan.get(stage, 0):
                root = self.control / 'stages' / stage / 'pending'

                def scan_shard(shard):
                    # Directory reads remain inside the same global metadata
                    # budget as claim/completion I/O; never hold SQLite here.
                    with self.fs_slots:
                        parent = root / str(shard)
                        if parent.is_dir():
                            return collections.deque(p for p in parent.iterdir()
                                                     if p.name.endswith('.json'))
                        return collections.deque()

                # Ordered map preserves the previous shard round-robin exactly.
                # Finish every read before modifying ready, so an enumeration
                # error propagates without being mistaken for an empty queue or
                # leaving a partial cache. No persistent reader threads survive.
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    groups = collections.deque(paths for paths in pool.map(scan_shard, range(64))
                                               if paths)
                while groups:
                    paths = groups.popleft()
                    ready.append(paths.popleft())
                    if paths:
                        groups.append(paths)
                self.next_scan[stage] = time.monotonic() + .5
            return ready.popleft() if ready else None


def main():
    # Keep authentication, port, local singleton lock and HTTP contract intact.
    import migration_broker
    migration_broker.Broker = Broker
    migration_broker.main()


if __name__ == '__main__':
    main()
