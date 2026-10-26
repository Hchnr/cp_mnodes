#!/usr/bin/env python3
"""P4 broker path sharding; requires an explicit broker handover.

The existing SQLite, owner/lease checks and receipt protocol are unchanged.
Pre-existing flat running paths are recovered exactly from their stored rows;
only new claims use 64 independent running directories. No worker is started,
signalled or retuned by this entrypoint.
"""

import collections
import concurrent.futures
from pathlib import Path
import re
import threading
import time

from migration_io import write_json
from migration_priority4_broker import Broker as Priority4Broker


class Broker(Priority4Broker):
    def __init__(self, control):
        super().__init__(control)
        self.running_prepare_lock = threading.Lock()
        self.prepared_running_stages = set()
        self.prepared_queue_parents = set()
        stage = self._state().get('stage')
        if isinstance(stage, str) and re.fullmatch(r'p4-r[123]', stage):
            self._prepare_running(stage)

    def _prepare_running(self, stage):
        if not re.fullmatch(r'p4-r[123]', stage):
            raise ValueError('sharded P4 paths require a priority4 stage')
        if stage in self.prepared_running_stages:
            return
        with self.running_prepare_lock:
            if stage in self.prepared_running_stages:
                return
            with self.fs_slots:
                root = self.control / 'stages' / stage / 'running'
                done = self.control / 'stages' / stage / 'done'
                root.mkdir(parents=True, exist_ok=True)
                done.mkdir(parents=True, exist_ok=True)
                parents = {root, done}
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
            return write_json(path, value, ensure_parent=False)
        return super().publish_json(path, value)

    def _candidate(self, stage):
        with self.ready_lock:
            ready = self.ready.setdefault(stage, collections.deque())
            if not ready and time.monotonic() >= self.next_scan.get(stage, 0):
                root = self.control / 'stages' / stage / 'pending'

                def scan_shard(shard):
                    with self.fs_slots:
                        parent = root / str(shard)
                        return (collections.deque(p for p in parent.iterdir() if p.name.endswith('.json'))
                                if parent.is_dir() else collections.deque())

                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    groups = collections.deque(paths for paths in pool.map(scan_shard, range(64)) if paths)
                while groups:
                    paths = groups.popleft()
                    ready.append(paths.popleft())
                    if paths:
                        groups.append(paths)
                self.next_scan[stage] = time.monotonic() + .5
            return ready.popleft() if ready else None


def main():
    import migration_broker
    migration_broker.Broker = Broker
    migration_broker.main()


if __name__ == '__main__':
    main()
