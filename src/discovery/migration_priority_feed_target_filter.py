#!/usr/bin/env python3
"""Prioritize fresh inventory before rechecking old, unassigned inventory.

The old manifests remain immutable. Their records seed the complete new
manifest and hardlink/path indexes, but old orphan probes are deferred to a
bounded pool after fresh scan tasks have actually been published. The deferred
JSONL spool is process-local /tmp storage, not a second assignment ledger.
"""

import argparse
import concurrent.futures
import json
import os
import tempfile
import time

from migration_io import write_json
from migration_overlap_parallel_filter import _task_records
from migration_overlap_resume import WARM_STAGE
from migration_parallel_pipeline import complete_records
from migration_parallel_scan import scan_phase
from migration_pipelined_target_filter import PipelinedTargetFilterResume
from migration_run import queue_dirs
from migration_worker import _components


class PriorityFeedTargetFilterResume(PipelinedTargetFilterResume):
    def __init__(self, control, handover, *, publish_workers=8, publish_limit=64):
        # Ancestor constructors publish state through the virtual save method.
        self.recovery_replay = {
            'phase': 'inventory_replay', 'seeded_records': 0,
            'deferred': 0, 'consumed': 0,
            'scope': 'warm-recovery-only', 'spool': 'local-temporary-file',
        }
        super().__init__(control, handover, publish_workers=publish_workers,
                         publish_limit=publish_limit)

    def save(self, **updates):
        if hasattr(self, 'recovery_replay'):
            updates['recovery_replay'] = dict(self.recovery_replay)
        return super().save(**updates)

    def supplement_missing(self):
        """Seed without probes; supply fresh tasks before deferred orphans."""
        self._check_stop()
        queue_dirs(self.control, WARM_STAGE)
        pending = set()
        selected = []
        selected_bytes = 0
        filter_counts = {'submitted': 0, 'checked': 0, 'missing': 0,
                         'peak_inflight': 0}
        issues = []
        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.filter_workers, thread_name_prefix='target-exists')
        last_replay_save = time.monotonic()
        last_stop_check = 0

        def checkpoint(force=False):
            nonlocal last_replay_save, last_stop_check
            current = time.monotonic()
            # The scanner uses the same bounded 100 ms cancellation cadence.
            # Re-statting STOP for every one of tens of millions of replayed
            # records needlessly serializes metadata work on the main thread.
            if force or current - last_stop_check >= .1:
                self._check_stop()
                last_stop_check = current
            if force or current - last_replay_save >= 3:
                self.save(target_filter=dict(filter_counts, inflight=len(pending)),
                          next_task_id=self.next_task_id)
                last_replay_save = current
                self.last_scan_save = current

        def flush():
            nonlocal selected_bytes
            if selected:
                self._emit_tasks(selected, 3, 3)
                selected.clear()
                selected_bytes = 0

        def consume_done(block=False):
            nonlocal selected_bytes
            if not pending:
                checkpoint()
                return
            done, _ = concurrent.futures.wait(
                pending, timeout=.1 if block else 0,
                return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                entry = future.result()
                filter_counts['checked'] += 1
                if entry is None or entry['rel'] in self.assigned:
                    continue
                size = entry.get('size', 0) if entry.get('kind') == 'file' else 0
                if selected and (len(selected) >= self.scan_batch_files
                                 or selected_bytes + size > 1024**3):
                    flush()
                selected.append(entry)
                selected_bytes += size
                filter_counts['missing'] += 1
                if len(selected) >= self.scan_batch_files or selected_bytes >= 1024**3:
                    flush()
            checkpoint()

        def submit(entries):
            for entry in entries:
                self._check_stop()
                if entry['rel'] in self.assigned:
                    continue
                while len(pending) >= self.filter_limit:
                    consume_done(True)
                    self._check_stop()
                pending.add(pool.submit(self._probe_missing, entry))
                filter_counts['submitted'] += 1
                filter_counts['peak_inflight'] = max(
                    filter_counts['peak_inflight'], len(pending))
                checkpoint()
            consume_done(False)

        def drain_probes():
            while pending:
                self._check_stop()
                consume_done(True)
            flush()

        try:
            # Explicit /tmp prevents TMPDIR from redirecting millions of old
            # records back to the shared metadata filesystem.
            with tempfile.TemporaryFile(mode='w+t', encoding='utf-8', dir='/tmp',
                                        prefix='migration-deferred-orphans-') as spool:
                def records():
                    seen = set()
                    for path in self.old_manifests:
                        local_issues = []
                        for entry in complete_records(path, local_issues):
                            rel = entry['rel']
                            if rel in seen:
                                checkpoint()
                                continue
                            _components(rel) if rel else None
                            seen.add(rel)
                            self.recovery_replay['seeded_records'] += 1
                            if (entry.get('kind') in ('file', 'symlink')
                                    and not entry.get('hardlink_to')
                                    and rel not in self.assigned):
                                # No network/source metadata work while replay
                                # seeds paths and hardlink canonicals.
                                spool.write(json.dumps(dict(entry, _refresh_source=True),
                                                       ensure_ascii=True,
                                                       separators=(',', ':')) + '\n')
                                self.recovery_replay['deferred'] += 1
                            checkpoint()
                            yield entry
                        issues.extend(dict(issue, manifest=str(path))
                                      for issue in local_issues)
                    # Assignment can extend past the last complete manifest
                    # line. Preserve every such task path, without republishing.
                    for task in _task_records(self.assigned_tasks):
                        for entry in task['entries']:
                            checkpoint()
                            if entry['rel'] not in seen:
                                seen.add(entry['rel'])
                                self.recovery_replay['seeded_records'] += 1
                                checkpoint()
                                yield {key: value for key, value in entry.items()
                                       if key != 'known'}
                    self.recovery_replay['phase'] = 'fresh_scan'
                    checkpoint(True)

                stats = scan_phase(
                    self.cfg['source'], 3, submit, self.combined_manifest,
                    workers=self.scan_workers, batch_files=self.scan_batch_files,
                    batch_bytes=1024**3, hardlink_index={}, entries_per_second=0,
                    control=str(self.control), resume_records=records(),
                    skip_seeded_nondirectories=True)
                # A submitted publication is not yet a visible task. Wait for
                # fresh publications without closing the reusable writer pool.
                drain_probes()
                while self._publication_tasks:
                    self._check_stop()
                    self._collect_publications(wait=True)
                    checkpoint()
                self.recovery_replay['phase'] = 'deferred_orphans'
                checkpoint(True)
                spool.flush()
                spool.seek(0)
                for line in spool:
                    self._check_stop()
                    submit([json.loads(line)])
                    self.recovery_replay['consumed'] += 1
                    checkpoint()
                drain_probes()
                write_json(self.adoption / 'manifest-tail-issues.json', {
                    'issues': issues,
                    'preserved_manifests': list(map(str, self.old_manifests)),
                })
                self.state['warm_missing_scan'] = stats
                self._collect_publications(finish=True)
                self.recovery_replay['phase'] = 'complete'
                # Parent waits for all accepted task publications before this
                # complete marker can become visible to workers/coordinators.
                self.save(status='copying', scan_complete=True,
                          target_filter=dict(filter_counts, inflight=0))
        finally:
            try:
                pool.shutdown(wait=True, cancel_futures=True)
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
    PriorityFeedTargetFilterResume(
        args.control, args.handover, publish_workers=args.publish_workers,
        publish_limit=args.publish_limit).run()


if __name__ == '__main__':
    main()
