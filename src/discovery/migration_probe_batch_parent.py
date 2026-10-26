#!/usr/bin/env python3
"""Reuse safe target parent opens within one authenticated metadata batch.

Each leaf still goes through the original probe, STOP checks, no-follow stat,
filesystem semaphore and optional delegation policy. Directory descriptors
never survive a request and are closed only after its submitted jobs finish.
No file content, task queue, resume ledger or source enumeration is changed.
"""

import concurrent.futures
import os
import threading

import migration_linux_open as opening
import migration_probe_fast_open as fast
import migration_target_probe as probe


_context = threading.local()


class BatchParents:
    def __init__(self, service):
        self.service = service
        self.root = service.destination_root
        self.mutex = threading.Lock()
        self.parents = {}

    def open(self, rel):
        parent, _, name = rel.rpartition('/')
        with self.mutex:
            future = self.parents.get(parent)
            first = future is None
            if first:
                future = concurrent.futures.Future()
                self.parents[parent] = future
        if first:
            try:
                fd, actual_name = opening.open_parent_readonly(self.root, rel)
                if actual_name != name:
                    os.close(fd)
                    raise ValueError('parent opener returned a different leaf')
                future.set_result(fd)
                with self.service.mutex:
                    self.service.counts['batch_parent_opens'] += 1
            except BaseException as exc:
                future.set_exception(exc)
                raise
        else:
            with self.service.mutex:
                self.service.counts['batch_parent_reuses'] += 1
        # The ordinary probe owns and closes its duplicate. The request owns
        # the original; every submitted job is drained before originals close.
        return os.dup(future.result()), name

    def close(self):
        for future in self.parents.values():
            if not future.done():
                raise RuntimeError('cannot close a parent while its opener is active')
            if future.exception() is None:
                os.close(future.result())


def open_parent(root, rel):
    parents = getattr(_context, 'parents', None)
    if parents is not None and root == parents.root:
        return parents.open(rel)
    return opening.open_parent_readonly(root, rel)


def probe_batch(self, entries):
    # Preserve whole-request validation before any target observation, including
    # an invalid trailing item, and do not mutate caller-owned wire entries.
    if not isinstance(entries, list) or not 1 <= len(entries) <= probe.MAX_BATCH:
        raise probe.ProbeError(400, 'entries must contain between1 and64 objects')
    for entry in entries:
        probe.phase3_relative(entry)
    with self.mutex:
        self.counts['batch_requests'] += 1
        self.counts['batch_entries'] += len(entries)
    parents = BatchParents(self)
    futures = []

    def one(entry):
        previous = getattr(_context, 'parents', None)
        _context.parents = parents
        try:
            return self._batch_one(entry)
        finally:
            _context.parents = previous

    try:
        try:
            for entry in entries:
                futures.append(self.batch_pool.submit(one, entry))
            results = [future.result() for future in futures]
        except RuntimeError:
            raise probe.ProbeError(503, 'metadata service is stopping') from None
    finally:
        # Executor shutdown can reject a later submission while earlier jobs
        # still use these descriptors. Drain those jobs before closing anything.
        if futures:
            concurrent.futures.wait(futures)
        parents.close()
    with self.mutex:
        self.counts['batch_failed_entries'] += sum(row['status'] != 200 for row in results)
    return {'ok': True, 'results': results}


def health(self):
    return dict(fast._health_with_backend(self), batch_parent_reuse='request-scoped')


def install():
    fast.install()
    probe._open_parent = open_parent
    probe.ProbeService.probe_batch = probe_batch
    probe.ProbeService.health = health


def main():
    fast.main(installer=install)


if __name__ == '__main__':
    main()
