#!/usr/bin/env python3
"""Adopt the small current queue, then feed the old inventory from its tail.

One coordinator retains IDs, SQLite, path reservations and all worker leases.
The old inventory is read-only. Every candidate is checked at the destination
and its source identity refreshed before the unchanged checked copy workers run.
"""
import argparse
import json
import os
from pathlib import Path
import threading
import time

from migration_crash_resume import CrashResume
from migration_frontier_inputs import InputPipeline
from migration_io import read_json
from migration_parallel_scan import scan_phase
from migration_pipelined_target_filter import PipelinedTargetFilterResume
from migration_worker import _components


MAX_RECORD = 1024 * 1024
MAX_TASK_BYTES = 1024**3


def reverse_chunks(path, block_bytes=8 * 1024 * 1024, end_offset=None):
    """Bounded reverse JSONL pages, with a durable next-byte boundary.

    Only the final unterminated record may be ignored after a crashed writer.
    Malformed complete records fail visibly instead of silently omitting work.
    """
    if type(block_bytes) is not int or block_bytes < 1:
        raise ValueError('positive block size required')
    with open(path, 'rb') as handle:
        upper = handle.seek(0, 2)
        if end_offset is not None:
            if type(end_offset) is not int or not 0 <= end_offset <= upper:
                raise ValueError('invalid reverse inventory checkpoint')
            upper = end_offset
        initial_end = upper
        while upper:
            tentative = max(0, upper - block_bytes)
            handle.seek(tentative)
            if tentative:
                handle.readline(MAX_RECORD + 1)
                lower = handle.tell()
                if lower >= upper:
                    tentative = max(0, upper - max(block_bytes, MAX_RECORD + 2))
                    handle.seek(tentative)
                    if tentative:
                        handle.readline(MAX_RECORD + 1)
                    lower = handle.tell()
            else:
                lower = 0
            if lower >= upper:
                raise ValueError('inventory record exceeds bounded page')
            raw = handle.read(upper - lower)
            records = []
            for line in reversed(raw.splitlines(keepends=True)):
                if not line.endswith(b'\n'):
                    if upper == initial_end:
                        continue
                    raise ValueError('unterminated interior inventory record')
                if not line.strip():
                    continue
                if len(line) > MAX_RECORD:
                    raise ValueError('inventory record exceeds size limit')
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError('inventory record must be an object')
                records.append(value)
            yield lower, upper, records
            upper = lower


def candidate(entry):
    if entry.get('kind') not in ('file', 'symlink') or entry.get('hardlink_to'):
        return None
    rel = entry.get('rel')
    _components(rel)
    if (rel == 'home/hcr' or rel.startswith('home/hcr/')
            or rel == 'repos/cosmos-framework-pwm'
            or rel.startswith('repos/cosmos-framework-pwm/')):
        return None
    return dict({k: v for k, v in entry.items() if k != 'known'}, _refresh_source=True)


class FrontierResume(CrashResume):
    def __init__(self, control, handover):
        self._stop_seen = False
        self._stop_until = 0
        self._stop_lock = threading.Lock()
        handover_value = read_json(Path(handover))
        frontier = Path(handover_value['frontier_manifest']).resolve()
        cfg = read_json(Path(control) / 'config.json')
        predecessor = Path(cfg['resume_from']).resolve()
        if (not frontier.is_relative_to(predecessor) or not frontier.is_file()
                or not (predecessor / 'STOP').is_file()):
            raise ValueError('frontier must be a fenced predecessor inventory')
        if frontier.stat().st_size != handover_value['frontier_size']:
            raise ValueError('predecessor inventory changed')
        self._metadata_batching = {'scope':'current-producer', 'submitted_batches':0,
            'submitted_entries':0, 'full_batches':0, 'completed_batches':0,
            'batch_seconds':0.0, 'child_cpu_seconds':0.0}
        # Adopt only the CURRENT small assignment ledger and its live SQLite.
        # Never replay the 94 GB predecessor inventory or rebuild its path set.
        PipelinedTargetFilterResume.__init__(self, control, handover, check_on_submit=False)
        # At most 11 x 64 probes execute on the read-only services. Thirty-two
        # batch clients hide whole-batch reply tails without raising those limits.
        self.cfg = dict(self.cfg, metadata_client_processes=32, distributed_target_threads=2048)
        self._distributed_workers = self.filter_workers = 2048
        self.enqueued_batches = handover_value['preserved_enqueued_batches']
        self.frontier = frontier
        self.pending_probes, self.offered, self.selected = {}, [], []
        self.inflight = 0
        self.filter_counts = {'submitted': 0, 'checked': 0, 'missing': 0, 'peak_inflight': 0}
        self.last_filter_save = time.monotonic()
        self.probe_pool = None
        self._formal_scan_batch_files = self.scan_batch_files
        self._inputs = InputPipeline(self, candidate)
        self._task_batch_policy = {'max_files':self.scan_batch_files, 'max_bytes':MAX_TASK_BYTES,
            'oversize':'singleton', 'new_tasks':0, 'oversize_singletons':0,
            'largest_multi_file_task_bytes':0, 'warm_only':True, 'revision':0,
            'history':[]}
        # The checkpoint and publisher used independent three-second timers.
        # Both synchronously aggregate all node streams. Share the completion
        # boundary only after a successful warm save; explicit/forced saves
        # and formal-round durability barriers remain unchanged.
        self._save_timing = {'scope':'current-producer',
            'periodic_timer_coalescing':'warm-only',
            **{kind:{'calls':0, 'failures':0, 'seconds':0.0,
                     'last_seconds':0.0, 'max_seconds':0.0}
               for kind in ('save', 'progress')}}
        self.state.pop('producer_handover', None)
        self.save(recovery_label='11 节点目录差集优先供给；保留原 worker 与索引恢复',
            frontier_input_parallelism={'metadata_processes':32,'json_readers':16,
                'directory_readers':8,'inflight_limit':self.filter_limit,
                'input_groups':2,'ack_groups':2,
                'ack_storage_lanes':len(self._inputs.ack_io),
                'ack_group_writers':2,
                'acknowledgement':'group-receipts-v1',
                'ack_pool_isolated':True,
                'historical_fallback':'only-after-input-queues-empty'},
            task_batch_policy=self._task_batch_policy)

    def start_seed(self):
        # ParallelFilterResume already adopted the exact live helper identity.
        self._poll_seed()

    def enqueue_warm(self):
        # Existing queue and receipts are adopted, never replayed or reset.
        self.save(status='scanning', scan_complete=False)

    def pump_inputs(self, force=False):
        self._inputs.pump(force)

    def set_copy_batch(self, files):
        """Main-thread hot tuning of future warm tasks only, never old payloads."""
        if type(files) is not int or files not in (64,128,256,512):
            raise ValueError('invalid warm copy batch size')
        if self.stage != 'p3-r3' or self.state.get('recovery_warm') is not True:
            raise ValueError('copy batching is limited to warm recovery')
        if files == self.scan_batch_files:
            return
        self.scan_batch_files = files
        policy = self._task_batch_policy
        policy['max_files'] = files
        policy['revision'] += 1
        policy['effective_from_task_id'] = self.next_task_id
        policy['history'] = (policy['history'] + [{'max_files':files,
            'from_task_id':self.next_task_id, 'at':time.time()}])[-8:]

    def save(self, **updates):
        if hasattr(self, '_metadata_batching'):
            updates['metadata_batching'] = dict(self._metadata_batching)
        timing = getattr(self, '_save_timing', None)
        if timing is None:  # Base constructors save before adoption is ready.
            return super().save(**updates)
        updates['producer_save_timing'] = {
            key:dict(value) if isinstance(value,dict) else value
            for key,value in timing.items()}
        if self.stage == 'p3-r3' and self.state.get('recovery_warm') is True:
            # Any successful save now satisfies both periodic callers. Do not
            # leave target_filter at the older checkpoint caller's snapshot.
            updates['target_filter'] = dict(self.filter_counts, inflight=self.inflight)
        began = time.monotonic()
        try:
            result = super().save(**updates)
        except BaseException:
            self._record_save_time('save', began, failed=True)
            raise
        completed = self._record_save_time('save', began)
        if self.stage == 'p3-r3' and self.state.get('recovery_warm') is True:
            self.last_scan_save = self.last_filter_save = completed
        return result

    def _record_save_time(self, kind, began, *, failed=False):
        completed = time.monotonic()
        seconds = max(0.0, completed - began)
        values = self._save_timing[kind]
        values['calls'] += 1
        values['failures'] += int(failed)
        values['seconds'] += seconds
        values['last_seconds'] = seconds
        values['max_seconds'] = max(values['max_seconds'], seconds)
        return completed

    def save_progress(self):
        if not hasattr(self, '_save_timing'):
            return super().save_progress()
        began = time.monotonic()
        try:
            result = super().save_progress()
        except BaseException:
            self._record_save_time('progress', began, failed=True)
            raise
        self._record_save_time('progress', began)
        return result

    def wait_for_inputs(self):
        """Never spend probe capacity on old inventory while live hints wait."""
        self.pump_inputs(force=True)
        while self._inputs.busy():
            self._check_stop()
            self._harvest_probes(True)
            self.pump_inputs()
            time.sleep(.005)

    def _submit_offered(self):
        before = set(self.pending_probes)
        super()._submit_offered()
        submitted = set(self.pending_probes) - before
        for future in submitted:
            size = len(self.pending_probes[future])
            self._metadata_batching['submitted_batches'] += 1
            self._metadata_batching['submitted_entries'] += size
            self._metadata_batching['full_batches'] += size == 64
        self._inputs.submitted(submitted)

    def _harvest_probes(self, block=False):
        before = set(self.pending_probes)
        super()._harvest_probes(block)
        harvested = before - set(self.pending_probes)
        for future in harvested:
            result = future.result()  # Already completed and checked by parent.
            self._metadata_batching['completed_batches'] += 1
            self._metadata_batching['batch_seconds'] += result.get('seconds', 0)
            self._metadata_batching['child_cpu_seconds'] += result.get('cpu_seconds', 0)
        self._inputs.harvested(harvested)

    def _flush_selected(self):
        """Keep small-file batching; large files cannot trap 159 GiB tails."""
        batch, size = [], 0
        def emit():
            nonlocal batch, size
            if not batch:
                return
            before = self.next_task_id
            self._emit_tasks(batch, 3, 3)
            if self.next_task_id > before:
                self._task_batch_policy['new_tasks'] += 1
            if self.next_task_id > before and len(batch) == 1 and size > MAX_TASK_BYTES:
                self._task_batch_policy['oversize_singletons'] += 1
            if self.next_task_id > before and len(batch) > 1:
                self._task_batch_policy['largest_multi_file_task_bytes'] = max(
                    size, self._task_batch_policy['largest_multi_file_task_bytes'])
            batch, size = [], 0
        for entry in self.selected:
            entry_size = entry.get('size', 0) if entry['kind'] == 'file' else 0
            if batch and (len(batch) >= self.scan_batch_files or size + entry_size > MAX_TASK_BYTES):
                emit()
            batch.append(entry)
            size += entry_size
            if size >= MAX_TASK_BYTES:
                emit()
        emit()
        self.selected.clear()

    def feed_frontier(self):
        self.wait_for_inputs()
        previous = self.state.get('frontier_feed', {})
        size = self.frontier.stat().st_size
        resume_at = previous.get('next_window_end') if (previous.get('manifest') == str(self.frontier)
            and previous.get('size') == size) else None
        seen = previous.get('records_seen', 0) if resume_at is not None else 0
        for lower, upper, records in reverse_chunks(self.frontier, end_offset=resume_at):
            self._check_stop()
            self.wait_for_inputs()
            batch = []
            for entry in records:
                value = candidate(entry)
                if value is not None:
                    batch.append(value)
                if len(batch) == 64:
                    self._offer(batch)
                    self.wait_for_inputs()
                    batch = []
            if batch:
                self._offer(batch)
            # Checkpoint only pages whose probes AND accepted publications
            # finished; reserved paths remain single-owner across all pages.
            self._drain_probes()
            seen += len(records)
            self.save(frontier_feed={'manifest': str(self.frontier), 'size': size,
                'next_window_end': lower, 'bytes_consumed': size - lower,
                'records_seen': seen, 'direction': 'reverse', 'complete': lower == 0},
                next_task_id=self.next_task_id)

    def run(self):
        try:
            return super().run()
        finally:
            self._inputs.close()

    def supplement_missing(self):
        try:
            self.feed_frontier()
            # Do not omit source additions or paths outside the old inventory.
            # This complete scan follows useful tail work, not the reverse.
            self.save(status='scanning', recovery_label='旧清单供给结束；补充当前源目录变化')
            stats = scan_phase(self.cfg['source'], 3, self._offer,
                self.combined_manifest, workers=self.scan_workers,
                batch_files=self.scan_batch_files, batch_bytes=1024**3,
                hardlink_index={}, entries_per_second=0, control=str(self.control),
                processes=int(self.cfg.get('source_scan_processes', 8)),
                threads_per_process=int(self.cfg.get('source_scan_threads_per_process', 16)),
                on_tick=lambda: (self.pump_inputs(), self._harvest_probes(), self._flush_selected()))
            self._drain_probes()
            self._inputs.finish()
            self._collect_publications(finish=True)
            self.state['warm_missing_scan'] = stats
            self.save(status='copying', scan_complete=True)
        finally:
            self._collect_publications(finish=True)
            self._task_publisher = None
            # Formal rounds retain their original scan/task configuration.
            self.scan_batch_files = self._formal_scan_batch_files


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=('run',))
    p.add_argument('--control', required=True)
    p.add_argument('--handover', required=True)
    args = p.parse_args()
    FrontierResume(args.control, args.handover).run()


if __name__ == '__main__':
    main()
