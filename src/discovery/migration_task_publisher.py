"""Bounded asynchronous publication of fresh immutable pending task files.

Only the creating thread may submit or collect results. Worker threads perform
filesystem publication only; SQLite, assignment reservation, task IDs, counters,
and coordinator state remain the caller's responsibility. Reserve a task before
submitting it, and do not count it as published until its result is harvested.

``submit`` returns False without accepting the task when the bounded capacity is
full. The caller must then ``harvest(wait=True)`` and retry the same submission.
Capacity includes completed-but-unharvested tasks, so neither queued payloads
nor uncollected results can grow without bound. A write failure permanently
closes submission, but finish/close still wait for every accepted write without
cancelling anything. PublicationError.results contains any successful results
collected by that call; failures must never be counted as published tasks.

Task IDs and target paths must be unique to this coordinator. Existing paths,
including migration_io snapshot paths, are rejected. As with migration_io, this
is not a distributed lock: competing publishers of the same ID are forbidden.

The optional check_on_submit=False omits only the duplicate filesystem check on
the owner thread. The publication worker always checks the exact same existing
path/snapshot/symlink guards before writing; its rejection is delivered through
PublicationError rather than a synchronous submit exception. In-memory duplicate
ID/path checks and publication capacity limits remain synchronous in both modes.
"""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from copy import deepcopy
from dataclasses import dataclass
import os
from pathlib import Path
import threading

from migration_io import write_json


@dataclass(frozen=True)
class PublicationResult:
    task_id: int
    target: Path


@dataclass(frozen=True)
class PublicationFailure:
    task_id: int
    target: Path
    error: BaseException


class PublicationError(RuntimeError):
    """Fail-closed publication error, including successes not yet reported."""

    def __init__(self, results, failures):
        self.results = tuple(results)
        self.failures = tuple(failures)
        detail = "; ".join("task %s: %s" % (failure.task_id, failure.error)
                           for failure in self.failures)
        super().__init__("task publication failed: " + detail)


def _fresh_target(target):
    # lexists also rejects dangling links rather than silently following them.
    snapshot = target.parent / '.snapshots' / target.name
    if os.path.lexists(target) or os.path.lexists(snapshot):
        raise FileExistsError("refusing existing task or snapshot path: " + str(target))


class BoundedTaskPublisher:
    """Single-owner bounded writer pool for independent fresh task paths.

    finish/close permanently close submissions, wait for ALL accepted writes,
    and report each successful result exactly once. Repeated finish/close return
    an empty list on success, or re-raise the recorded fatal failures with empty
    results. They never cancel queued futures, even after a write has failed.
    """

    def __init__(self, workers=8, max_pending=64, writer=write_json, *, check_on_submit=True):
        if type(workers) is not int or not 1 <= workers <= 128:
            raise ValueError("workers must be an integer from 1 to 128")
        if type(max_pending) is not int or not workers <= max_pending <= 4096:
            raise ValueError("max_pending must be an integer from workers to 4096")
        if not callable(writer):
            raise TypeError("writer must be callable")
        if type(check_on_submit) is not bool:
            raise TypeError('check_on_submit must be boolean')
        self.workers = workers
        self.max_pending = max_pending
        self._writer = writer
        self.check_on_submit = check_on_submit
        self._owner = threading.get_ident()
        self._executor = ThreadPoolExecutor(max_workers=workers,
                                            thread_name_prefix='task-publisher')
        self._pending = {}
        self._seen_ids = set()
        self._seen_targets = set()
        self._failures = []
        self._closed = False
        self._shutdown = False

    def _assert_owner(self):
        if threading.get_ident() != self._owner:
            raise RuntimeError("publisher must be used only by its creating thread")

    @property
    def pending_count(self):
        self._assert_owner()
        return len(self._pending)

    def submit(self, task_id, payload, target):
        """Accept a copied payload, or return False when harvest is required.

        Rejected/full submissions do not reserve an ID inside this object.
        Failure of any accepted writer poisons all future submissions. A failed
        write may already be visible on disk; never retry it with the same ID.
        """
        self._assert_owner()
        if self._closed:
            raise RuntimeError("publisher is closed")
        # Detect already-finished failures before accepting more work. Do not
        # silently harvest successes here: only the owner counts their results.
        for future, result in self._pending.items():
            if future.done() and future.exception() is not None:
                return self._raise_pending_failure()
        if self._failures:
            raise PublicationError([], self._failures)
        if type(task_id) is not int or task_id < 0:
            raise ValueError("task_id must be a nonnegative integer")
        target = Path(os.path.abspath(os.fspath(target)))
        if task_id in self._seen_ids:
            raise ValueError("refusing reused task ID: " + str(task_id))
        if target in self._seen_targets:
            raise ValueError("refusing reused target path: " + str(target))
        if len(self._pending) >= self.max_pending:
            return False
        if self.check_on_submit:
            _fresh_target(target)
        # A shallow copy is insufficient: scanner batches and nested known
        # metadata can be mutated as soon as this method returns.
        private_payload = deepcopy(payload)
        result = PublicationResult(task_id, target)
        future = self._executor.submit(self._publish, result, private_payload)
        self._pending[future] = result
        self._seen_ids.add(task_id)
        self._seen_targets.add(target)
        return True

    def _raise_pending_failure(self):
        # harvest raises once it records the already-observed failed future;
        # successful results from the same batch travel in the exception.
        self.harvest()
        raise AssertionError("observed failed future was not harvested")

    def _publish(self, result, private_payload):
        _fresh_target(result.target)
        published = self._writer(result.target, private_payload)
        # migration_io upgrades reused paths to snapshots. Such an unexpected
        # publication is not a successful fresh pending task, even if writable.
        if published is not None and Path(published) != result.target:
            raise RuntimeError("task unexpectedly published to a snapshot: " + str(published))

    def harvest(self, wait=False):
        """Return all currently ready successes, optionally waiting for one.

        On any write failure, raise PublicationError carrying this call's ready
        successes plus every failure seen so far. Unfinished work remains owned
        by the pool and MUST still be drained with finish/close in a finally.
        """
        self._assert_owner()
        if wait and self._pending and not any(f.done() for f in self._pending):
            # The keyword is deliberately also named wait for the public API.
            _wait_first(self._pending)
        ready = [future for future in self._pending if future.done()]
        results = []
        for future in ready:
            result = self._pending.pop(future)
            try:
                future.result()
            except BaseException as error:
                self._failures.append(PublicationFailure(result.task_id, result.target, error))
            else:
                results.append(result)
        if self._failures:
            raise PublicationError(results, self._failures)
        return results

    def finish(self):
        """Close submissions and await all writes; idempotent and never cancel."""
        self._assert_owner()
        self._closed = True
        results = []
        try:
            while self._pending:
                try:
                    results.extend(self.harvest(wait=True))
                except PublicationError as error:
                    results.extend(error.results)
        finally:
            if not self._shutdown:
                self._executor.shutdown(wait=True, cancel_futures=False)
                self._shutdown = True
        if self._failures:
            raise PublicationError(results, self._failures)
        return results

    close = finish

    def __enter__(self):
        self._assert_owner()
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _wait_first(futures):
    wait(futures, return_when=FIRST_COMPLETED)
