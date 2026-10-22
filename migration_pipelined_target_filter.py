#!/usr/bin/env python3
"""Warm-recovery producer with bounded, parallel immutable task publication.

Only the producer main thread allocates IDs, reserves relative paths, reads
SQLite and updates counters. Publisher threads only publish independent task
files. This is a fresh-process entrypoint requiring an explicit handover,
not a hot reload or permission to start a second live coordinator.

The optimization is deliberately limited to the recovery warm queue. All
formal rounds keep the inherited implementation after the publisher closes.
"""

import argparse
import json
import os
import resource
import time
import uuid

from migration_distributed_target_filter import DistributedTargetFilterResume
from migration_io import read_json, write_json
from migration_overlap_resume import WARM_STAGE
from migration_task_publisher import BoundedTaskPublisher, PublicationError


def ensure_producer_nofile(minimum=65536):
    """Validate and raise the producer's own limit before opening any state.

    Never rely on the launching SSH shell to inherit the earlier producer's
    limit. A low hard limit is a startup error, before coordinator side effects.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < minimum:
        raise RuntimeError('producer NOFILE hard limit is below ' + str(minimum))
    if soft != resource.RLIM_INFINITY and soft < minimum:
        resource.setrlimit(resource.RLIMIT_NOFILE, (minimum, hard))
    actual = resource.getrlimit(resource.RLIMIT_NOFILE)
    if actual[0] != resource.RLIM_INFINITY and actual[0] < minimum:
        raise RuntimeError('producer NOFILE limit did not reach ' + str(minimum))
    return actual


class PipelinedTargetFilterResume(DistributedTargetFilterResume):
    def __init__(self, control, handover, *, publish_workers=8, publish_limit=64):
        if type(publish_workers) is not int or not 1 <= publish_workers <= 64:
            raise ValueError('publish_workers must be an integer between 1 and 64')
        if type(publish_limit) is not int or not publish_workers <= publish_limit <= 4096:
            raise ValueError('publish_limit must be between publish_workers and 4096')
        self._producer_nofile = ensure_producer_nofile()
        # Ancestor constructors call the virtual save() method.
        self._task_publisher = None
        self._publication_tasks = {}
        self._publication_error = None
        self._publication_failures = []
        self._publication_successes = 0
        self._publication_workers = publish_workers
        self._publication_limit = publish_limit
        self._publication_enabled = False
        self._publication_failure_path = None
        self._publication_finished = False
        self._publication_last_tuning_check = 0
        self._publication_tuning_id = None
        self._publication_tuning_error = None
        super().__init__(control, handover)
        self._task_publisher = BoundedTaskPublisher(
            workers=publish_workers, max_pending=publish_limit)
        self._publication_enabled = True
        self.save()

    def _apply_publications(self, results):
        for result in results:
            record = self._publication_tasks.pop(result.task_id)
            self._publication_successes += 1
            self.enqueued_batches += 1
            if record['count_discovery']:
                self.discovered_files += record['files']
                self.discovered_bytes += record['bytes']
            counters = self.phase_progress[str(record['phase'])]
            counters['discovered_files'] = max(
                counters['discovered_files'], self.discovered_files)
            counters['discovered_bytes'] = max(
                counters['discovered_bytes'], self.discovered_bytes)
            self.current_scan.update(files=self.discovered_files,
                                     bytes=self.discovered_bytes)

    def _collect_publications(self, *, wait=False, finish=False, raise_error=True):
        publisher = self._task_publisher
        if publisher is None:
            return
        try:
            if finish:
                self._publication_finished = True
            results = publisher.finish() if finish else publisher.harvest(wait=wait)
        except PublicationError as exc:
            self._apply_publications(exc.results)
            self._publication_error = str(exc)
            self._publication_failures.extend(str(value) for value in exc.failures)
            if raise_error:
                raise
        else:
            self._apply_publications(results)

    def _publication_snapshot(self):
        return {
            'workers': self._publication_workers,
            'limit': self._publication_limit,
            'pending': len(self._publication_tasks),
            'published_since_handover': self._publication_successes,
            'failed': self._publication_error is not None,
            'error': self._publication_error,
            'next_task_id': getattr(self, 'next_task_id', None),
            'scope': 'warm-recovery-only',
            'tuning_operation': getattr(self, '_publication_tuning_id', None),
            'tuning_error': getattr(self, '_publication_tuning_error', None),
        }

    def _refresh_publication_tuning(self):
        """New entrypoint only: bounded pool replacement at a drained barrier."""
        if (self._task_publisher is None or self.stage != WARM_STAGE
                or getattr(self, '_publication_finished', False)):
            return
        current = time.monotonic()
        if current - getattr(self, '_publication_last_tuning_check', 0) < 10:
            return
        self._publication_last_tuning_check = current
        path = self.control / 'producer-tuning.json'
        try:
            if not path.exists():
                return
            value = read_json(path)
            if (not isinstance(value, dict)
                    or value.get('control') != str(self.control)
                    or value.get('run_id') != self.cfg['run_id']
                    or value.get('scope') != 'warm-publication-v1'):
                raise ValueError('producer tuning has the wrong control/run/scope')
            operation = value.get('operation_id')
            if (not isinstance(operation, str) or len(operation) != 32
                    or any(char not in '0123456789abcdef' for char in operation)):
                raise ValueError('producer tuning requires a lowercase UUID hex operation_id')
            workers, limit = value.get('publish_workers'), value.get('publish_limit')
            if type(workers) is not int or not 1 <= workers <= 64:
                raise ValueError('publish_workers must be an integer between 1 and 64')
            if type(limit) is not int or not workers <= limit <= 4096:
                raise ValueError('publish_limit must be between publish_workers and 4096')
            if operation == getattr(self, '_publication_tuning_id', None):
                if (workers, limit) != (self._publication_workers, self._publication_limit):
                    raise ValueError('a producer tuning operation cannot change its payload')
                self._publication_tuning_error = None
                return
        except (OSError, ValueError, KeyError, TypeError) as exc:
            # Invalid tuning does not change the last validated pool or pause
            # a healthy copy task supply. It remains visible in state.
            self._publication_tuning_error = type(exc).__name__ + ': ' + str(exc)
            return
        if (workers, limit) != (self._publication_workers, self._publication_limit):
            self._collect_publications(finish=True)
            self._task_publisher = BoundedTaskPublisher(workers=workers, max_pending=limit)
            self._publication_finished = False
            self._publication_workers, self._publication_limit = workers, limit
        self._publication_tuning_id = operation
        self._publication_tuning_error = None

    def save(self, **updates):
        if hasattr(self, '_producer_nofile'):
            updates['producer_nofile'] = list(self._producer_nofile)
        if self._publication_enabled:
            failing = updates.get('status') == 'failed'
            if not failing and updates.get('scan_complete') is not True:
                self._refresh_publication_tuning()
            self._collect_publications(
                finish=failing or updates.get('scan_complete') is True,
                raise_error=not failing)
            updates['task_publication'] = self._publication_snapshot()
            if self.stage == WARM_STAGE:
                updates['enqueued_batches'] = self.enqueued_batches
                updates['next_task_id'] = self.next_task_id
            if failing and self.state.get('recovery_warm'):
                # A terminal "failed" state would make idle copy threads
                # return forever. Keep their services alive for inspection;
                # a present STOP is still honored directly by all workers.
                updates['status'] = 'producer_paused'
                updates['producer_error'] = updates.pop('error', self._publication_error)
                updates['next_task_id'] = self.next_task_id
                if self._publication_failure_path is None:
                    path = self.control / 'task-publication-failures' / (uuid.uuid4().hex + '.json')
                    write_json(path, {
                        'stage': self.stage, 'next_task_id': self.next_task_id,
                        'error': updates['producer_error'],
                        'unconfirmed': list(self._publication_tasks.values()),
                        'failures': self._publication_failures,
                    })
                    self._publication_failure_path = str(path)
                updates['task_publication']['failure_record'] = self._publication_failure_path
        return super().save(**updates)

    def _emit_tasks(self, entries, phase, round_no, count_discovery=True):
        if self.stage != WARM_STAGE or self._task_publisher is None:
            return super()._emit_tasks(entries, phase, round_no, count_discovery)
        self._check_stop()
        self._collect_publications()
        fresh, local = [], set()
        for original in entries:
            rel = original['rel']
            if rel in self.assigned or rel in local:
                continue
            entry = dict(original)
            row = self.db.execute(
                'SELECT source,destination,owned,sha256 FROM files WHERE rel=?',
                (rel,)).fetchone()
            if row:
                entry['known'] = {
                    'source': json.loads(row[0]), 'destination': json.loads(row[1]),
                    'owned': bool(row[2]), 'sha256': row[3],
                }
            fresh.append(entry)
            local.add(rel)
        if not fresh:
            return
        task_id = self.next_task_id
        target = self.control / 'stages' / self.stage / 'pending' / str(task_id % 64) / (str(task_id) + '.json')
        task = {'id': task_id, 'phase': phase, 'round': round_no,
                'op': 'sync', 'entries': fresh}
        # Reservation is main-thread-only and is never rolled back after an
        # uncertain publication. IDs are not a contiguous success watermark.
        self.assigned.update(local)
        self.next_task_id += 1
        self._publication_tasks[task_id] = {
            'id': task_id, 'phase': phase, 'count_discovery': count_discovery,
            'files': len(fresh),
            'bytes': sum(e.get('size', 0) for e in fresh if e['kind'] == 'file'),
            'target': str(target), 'task': task,
        }
        while True:
            try:
                accepted = self._task_publisher.submit(task_id, task, target)
            except PublicationError as exc:
                self._apply_publications(exc.results)
                self._publication_error = str(exc)
                self._publication_failures.extend(str(value) for value in exc.failures)
                raise
            if accepted:
                break
            self._check_stop()
            self._collect_publications(wait=True)
        self._collect_publications()
        if time.monotonic() - self.last_scan_save >= 3:
            self.save(enqueued_batches=self.enqueued_batches,
                      next_task_id=self.next_task_id)
            self.last_scan_save = time.monotonic()

    def supplement_missing(self):
        try:
            # save(scan_complete=True) is intercepted above and cannot become
            # visible until every accepted publication has finished.
            return super().supplement_missing()
        finally:
            try:
                self._collect_publications(finish=True)
            finally:
                self._task_publisher = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    parser.add_argument('--publish-workers', type=int, default=8)
    parser.add_argument('--publish-limit', type=int, default=64)
    args = parser.parse_args()
    os.umask(0o077)
    PipelinedTargetFilterResume(
        args.control, args.handover, publish_workers=args.publish_workers,
        publish_limit=args.publish_limit).run()


if __name__ == '__main__':
    main()
