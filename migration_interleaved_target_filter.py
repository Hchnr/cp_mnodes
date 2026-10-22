#!/usr/bin/env python3
"""One warm producer feeding fresh inventory and historical candidates together.

The prepared checkpoint removes the full-ledger/full-manifest startup replay.
All task IDs, path reservations, index access and counters remain on one owner
thread. Probe threads are read-only; existing copy/verification is unchanged.
"""
import argparse
import concurrent.futures
import gc
import json
import os
from pathlib import Path
import threading
import time

from migration_io import read_json, write_json
from migration_overlap_parallel_filter import _task_records
from migration_overlap_resume import WARM_STAGE
from migration_parallel_scan import _seed, scan_phase
from migration_priority_feed_target_filter import PriorityFeedTargetFilterResume
from migration_run import queue_dirs
from migration_scan import _Scanner


class InterleavedTargetFilterResume(PriorityFeedTargetFilterResume):
    def __init__(self, *args, **kwargs):
        self._stop_poll_lock = threading.Lock()
        self._stop_poll_until = 0
        self._stop_seen = False
        self.supply_metrics = {'stop_polls': 0, 'gc_seconds': 0.0, 'gc_collections': 0}
        self._gc_started = None
        super().__init__(*args, **kwargs)
        if not self.prepared_resume:
            raise ValueError('interleaved entry requires a prepared checkpoint')
        self.cancelled_retries = []
        if self.handover.get('cancelled_retry_path'):
            from migration_prepared_resume import digest_file
            retry_path = Path(self.handover['cancelled_retry_path']).resolve()
            if (not retry_path.is_relative_to(self.control)
                    or digest_file(retry_path) != self.handover['cancelled_retry_sha256']):
                raise ValueError('cancelled retry evidence changed')
            self.cancelled_retries = read_json(retry_path)['entries']
            self.assigned.difference_update(e['rel'] for e in self.cancelled_retries)
        self._gc_callback = self._observe_gc
        gc.callbacks.append(self._gc_callback)

    def _observe_gc(self, phase, info):
        if phase == 'start':
            self._gc_started = time.monotonic()
        elif self._gc_started is not None:
            self.supply_metrics['gc_seconds'] += time.monotonic() - self._gc_started
            self.supply_metrics['gc_collections'] += 1
            self._gc_started = None

    def _check_stop(self):
        # One shared 100-ms poll replaces thousands of identical shared-FS
        # stats per second. All threads see the same sticky cancellation.
        if self._stop_seen:
            raise RuntimeError('STOP requested')
        if time.monotonic() >= self._stop_poll_until:
            with self._stop_poll_lock:
                if time.monotonic() >= self._stop_poll_until:
                    self._stop_seen = (self.control / 'STOP').exists()
                    self.supply_metrics['stop_polls'] += 1
                    self._stop_poll_until = time.monotonic() + .1
        if self._stop_seen:
            raise RuntimeError('STOP requested')

    def save(self, **updates):
        if hasattr(self, 'supply_metrics'):
            updates['supply_metrics'] = dict(self.supply_metrics)
        return super().save(**updates)

    def _finish_prepared_inventory(self):
        """Append only the old producer's short post-preparation tail."""
        meta, cache = self.prepared_resume, self.prepared_values
        source = Path(meta['source_manifest'])
        scanner = _Scanner(None, None, 1024**3, 64, cache['hardlinks'], None, 3)
        scanner.stats = cache['stats']
        seeded = cache['seeded_paths']
        extra = Path(meta['cache']).parent / 'tail-orphans.jsonl'
        source_dev = os.stat(self.cfg['source']).st_dev
        issues = []
        with open(source, 'rb') as src, open(self.combined_manifest, 'ab') as out, open(extra, 'xb') as spool:
            info = os.fstat(src.fileno())
            if ((info.st_dev, info.st_ino) != (meta['source_manifest_dev'], meta['source_manifest_ino'])
                    or info.st_size < meta['manifest_offset']
                    or out.tell() != meta['manifest_offset']):
                raise RuntimeError('prepared manifest prefix identity/size changed')
            src.seek(meta['manifest_offset'])

            def add(entry, line):
                rel = entry['rel']
                if rel in seeded:
                    return
                out.write(line)
                if entry.get('kind') != 'error':
                    seeded.add(rel)
                _seed(scanner, entry, source_dev, source_dev)
                if (entry.get('kind') in ('file', 'symlink')
                        and not entry.get('hardlink_to') and rel not in self.assigned):
                    spool.write(line)

            for line in src:
                if not line.endswith(b'\n'):
                    issues.append({'manifest': str(source), 'error': 'incomplete final line preserved in predecessor'})
                    break
                add(json.loads(line), line)
            # A durable publication may follow a line still buffered in the
            # previous manifest. Retain those records without republishing.
            for task in _task_records(self.handover['assignment_delta_path']):
                for original in task['entries']:
                    entry = {k: v for k, v in original.items() if k != 'known'}
                    add(entry, (json.dumps(entry, ensure_ascii=True, separators=(',', ':'))+'\n').encode())
            for handle in (out, spool):
                handle.flush()
                os.fsync(handle.fileno())
        self.recovery_replay.update(phase='fresh_scan', seeded_records=len(seeded),
                                   deferred=meta['orphans'], consumed=0,
                                   strategy='interleaved-large-first')
        return scanner, [*meta['spools'], str(extra)], issues

    def supplement_missing(self):
        self._check_stop()
        queue_dirs(self.control, WARM_STAGE)
        seed, spool_paths, issues = self._finish_prepared_inventory()
        self.save(status='scanning')
        pending, selected, selected_rels = {}, [], set()
        selected_bytes = 0
        counts = {'submitted': 0, 'checked': 0, 'missing': 0, 'peak_inflight': 0}
        last_save = time.monotonic()
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.filter_workers,
                                                     thread_name_prefix='target-exists')

        def checkpoint(force=False):
            nonlocal last_save
            self._check_stop()
            if force or time.monotonic()-last_save >= 3:
                self.save(target_filter=dict(counts, inflight=len(pending)), next_task_id=self.next_task_id)
                last_save = time.monotonic()

        def flush():
            nonlocal selected_bytes
            if selected:
                self._emit_tasks(selected, 3, 3)
                selected.clear()
                selected_rels.clear()
                selected_bytes = 0

        def harvest(block=False):
            nonlocal selected_bytes
            if not pending:
                return
            done, _ = concurrent.futures.wait(pending, timeout=.1 if block else 0,
                                              return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                pending.pop(future)
                entry = future.result()
                counts['checked'] += 1
                if entry is None or entry['rel'] in self.assigned or entry['rel'] in selected_rels:
                    continue
                size = entry.get('size', 0) if entry['kind'] == 'file' else 0
                if selected and (len(selected) >= self.scan_batch_files or selected_bytes+size > 1024**3):
                    flush()
                selected.append(entry)
                selected_rels.add(entry['rel'])
                selected_bytes += size
                counts['missing'] += 1
                if len(selected) >= self.scan_batch_files or selected_bytes >= 1024**3:
                    flush()
            checkpoint()

        def offer(entry):
            if entry['rel'] in self.assigned or entry['rel'] in selected_rels:
                return
            pending[pool.submit(self._probe_missing, entry)] = entry['rel']
            counts['submitted'] += 1
            counts['peak_inflight'] = max(counts['peak_inflight'], len(pending))

        def fresh(entries):
            for entry in entries:
                while len(pending) >= self.filter_limit:
                    harvest(True)
                    self._check_stop()
                offer(entry)
            harvest()
            checkpoint()

        def records():
            for original in getattr(self, 'cancelled_retries', []):
                yield dict(original, _refresh_source=True)
            for path in spool_paths:
                with open(path, encoding='utf-8') as handle:
                    for line in handle:
                        entry = json.loads(line)
                        entry['_refresh_source'] = True
                        yield entry

        candidates = iter(records())
        exhausted = False

        def pump():
            nonlocal exhausted
            harvest()
            # Leave room for one fresh batch. Both streams use the same
            # reservation owner and same checked publication path.
            room = max(0, self.filter_limit-self.scan_batch_files-len(pending))
            for _ in range(min(256, room)):
                try:
                    entry = next(candidates)
                except StopIteration:
                    exhausted = True
                    break
                self.recovery_replay['consumed'] += 1
                offer(entry)
            # Don't retain a short large-file batch while source traversal is
            # idle: publication latency matters more than filling 64 slots.
            flush()
            checkpoint()

        try:
            pump()
            stats = scan_phase(self.cfg['source'], 3, fresh, self.combined_manifest,
                               workers=self.scan_workers, batch_files=self.scan_batch_files,
                               batch_bytes=1024**3, hardlink_index=seed.hardlinks,
                               control=str(self.control), initial_seeded_paths=self.prepared_values['seeded_paths'],
                               seed_stats=seed.stats, skip_seeded_nondirectories=True,
                               append_manifest=True, on_tick=pump)
            self.recovery_replay['phase'] = 'deferred_orphans'
            while not exhausted:
                pump()
                if len(pending) >= self.filter_limit-self.scan_batch_files:
                    harvest(True)
            while pending:
                harvest(True)
            flush()
            self._collect_publications(finish=True)
            write_json(self.adoption / 'manifest-tail-issues.json', {'issues': issues})
            self.state['warm_missing_scan'] = stats
            self.recovery_replay['phase'] = 'complete'
            self.save(status='copying', scan_complete=True, target_filter=dict(counts, inflight=0))
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
            try:
                self._collect_publications(finish=True)
            finally:
                self._task_publisher = None
                gc.callbacks.remove(self._gc_callback)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    parser.add_argument('--publish-workers', type=int, default=8)
    parser.add_argument('--publish-limit', type=int, default=64)
    args = parser.parse_args()
    os.umask(0o077)
    InterleavedTargetFilterResume(args.control, args.handover,
                                 publish_workers=args.publish_workers, publish_limit=args.publish_limit).run()
