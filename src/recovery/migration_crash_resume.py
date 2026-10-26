#!/usr/bin/env python3
"""Fresh, isolated crash recovery: unfinished tasks first, no manifest replay."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import threading
import time
import traceback

from migration_distributed_target_filter import DistributedTargetFilterResume
from migration_io import read_json
from migration_overlap_resume import OverlapResume, WARM_STAGE
from migration_parallel_scan import scan_phase
from migration_pipelined_target_filter import PipelinedTargetFilterResume, ensure_producer_nofile
from migration_probe_processes import ProcessProbePool
from migration_run import queue_dirs
from migration_task_publisher import BoundedTaskPublisher


class CrashResume(PipelinedTargetFilterResume):
    def __init__(self, control, warm_tasks):
        self._producer_nofile = ensure_producer_nofile()
        self._task_publisher = None
        self._publication_tasks = {}
        self._publication_error = None
        self._publication_failures = []
        self._publication_successes = 0
        self._publication_workers, self._publication_limit = 8, 64
        self._publication_enabled = False
        self._publication_failure_path = None
        self._publication_finished = False
        self._publication_last_tuning_check = 0
        self._publication_tuning_id = self._publication_tuning_error = None
        self.stage = WARM_STAGE
        self._stop_seen = False
        self._stop_until = 0
        self._stop_lock = threading.Lock()
        cfg = read_json(Path(control) / 'config.json')
        self._init_probe_client(cfg)
        # Deliberately invoke the fresh bootstrap, not the live-handover
        # constructors: no predecessor /tmp database or pickle is adopted.
        OverlapResume.__init__(self, control, warm_tasks)
        self.filter_workers, self.filter_limit = self._distributed_workers, self._distributed_limit
        self._task_publisher = BoundedTaskPublisher(workers=8, max_pending=64)
        self._publication_enabled = True
        self.pending_probes, self.offered, self.selected = {}, [], []
        self.inflight = 0
        self.filter_counts = {'submitted': 0, 'checked': 0, 'missing': 0, 'peak_inflight': 0}
        self.last_filter_save = time.monotonic()
        self.probe_pool = None
        self.save(recovery_label='崩溃恢复：先处理未完成批次，历史索引后台重建')

    def _check_stop(self):
        if not self._stop_seen and time.monotonic() >= self._stop_until:
            with self._stop_lock:
                self._stop_seen = (self.control / 'STOP').exists()
                self._stop_until = time.monotonic() + .1
        if self._stop_seen:
            raise RuntimeError('STOP requested')

    def save(self, **updates):
        if hasattr(self, 'cfg'):
            updates['workers_expected'] = len(self.cfg.get('copy_hosts', [])) or 11
        return super().save(**updates)

    def _checkpoint(self, force=False):
        self._check_stop()
        if force or time.monotonic() - self.last_filter_save >= 3:
            self.save(target_filter=dict(self.filter_counts, inflight=self.inflight),
                      next_task_id=self.next_task_id)
            self.last_filter_save = time.monotonic()

    def _flush_selected(self):
        if self.selected:
            self._emit_tasks(self.selected, 3, 3)
            self.selected.clear()

    def _submit_offered(self):
        if self.offered:
            batch, self.offered = self.offered, []
            self.pending_probes[self.probe_pool.submit(batch)] = batch

    def _harvest_probes(self, block=False):
        if self.pending_probes:
            done, _ = concurrent.futures.wait(self.pending_probes, timeout=.1 if block else 0,
                         return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                batch = self.pending_probes.pop(future)
                result = future.result()
                entries = result['entries']
                if len(entries) != len(batch) or any(e is not None and e['rel'] != old['rel']
                                                     for e, old in zip(entries, batch)):
                    raise ValueError('metadata process returned mismatched task paths')
                delta = result['probe_delta']
                with self._probe_lock:
                    self._probe_counts.update(delta['counts'])
                    for endpoint, values in delta['endpoints'].items():
                        self._endpoint_counts[endpoint].update(values)
                self.inflight -= len(batch)
                self.filter_counts['checked'] += len(batch)
                for entry in entries:
                    if entry is None or entry['rel'] in self.assigned:
                        continue
                    self.selected.append(entry)
                    self.filter_counts['missing'] += 1
                    if len(self.selected) >= self.scan_batch_files:
                        self._flush_selected()
        self._checkpoint()

    def _offer(self, entries):
        for entry in entries:
            if entry['rel'] in self.assigned:
                continue
            while self.inflight >= self.filter_limit:
                self._submit_offered()
                self._harvest_probes(True)
            self.offered.append(entry)
            self.inflight += 1
            self.filter_counts['submitted'] += 1
            self.filter_counts['peak_inflight'] = max(self.filter_counts['peak_inflight'], self.inflight)
            if len(self.offered) == 64:
                self._submit_offered()
        self._submit_offered()
        self._harvest_probes()

    def _drain_probes(self):
        self._submit_offered()
        while self.pending_probes:
            self._harvest_probes(True)
        self._flush_selected()
        self._collect_publications(wait=True)
        self._checkpoint(True)

    def enqueue_warm(self):
        queue_dirs(self.control, WARM_STAGE)
        self.stage = WARM_STAGE
        self.save(status='copying', phase=3, round=3, stage=WARM_STAGE, scan_complete=False)
        count = 0
        with open(self.warm_tasks, encoding='utf-8') as handle:
            for line in handle:
                self._check_stop()
                payload = json.loads(line)
                entries = [dict(e, _refresh_source=True) for e in payload.get('task', payload)['entries']]
                self._offer(entries)
                count += len(entries)
        self._drain_probes()
        self.state['warm_inventory'] = {'entries_seen': count, 'batches': self.enqueued_batches}
        self.save()

    def supplement_missing(self):
        self.save(status='scanning', recovery_label='新扫描供给缺失目标；历史索引后台恢复')
        try:
            stats = scan_phase(self.cfg['source'], 3, self._offer,
                self.control / 'manifests' / 'recovery-missing.jsonl',
                workers=self.scan_workers, batch_files=self.scan_batch_files,
                batch_bytes=1024**3, hardlink_index={}, entries_per_second=0,
                control=str(self.control), processes=int(self.cfg.get('source_scan_processes', 8)),
                threads_per_process=int(self.cfg.get('source_scan_threads_per_process', 16)),
                on_tick=lambda: (self._harvest_probes(), self._flush_selected()))
            self._drain_probes()
            self._collect_publications(finish=True)
            self.state['warm_missing_scan'] = stats
            self.save(status='copying', scan_complete=True)
        finally:
            self._collect_publications(finish=True)
            self._task_publisher = None

    def run(self):
        self.probe_pool = ProcessProbePool(self.control, self.cfg,
            processes=int(self.cfg.get('metadata_client_processes', 8)), threads=64)
        try:
            return OverlapResume.run(self)
        except BaseException:
            self.save(status='failed', error=traceback.format_exc())
            raise
        finally:
            self.probe_pool.shutdown(wait=True, cancel_futures=True)
            if self._task_publisher is not None:
                self._collect_publications(finish=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=('run',))
    p.add_argument('--control', required=True)
    p.add_argument('--warm-tasks', required=True)
    args = p.parse_args()
    os.umask(0o077)
    CrashResume(args.control, args.warm_tasks).run()


if __name__ == '__main__':
    main()
