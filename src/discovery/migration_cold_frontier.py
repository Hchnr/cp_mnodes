#!/usr/bin/env python3
"""Fresh cold namespace with the bounded frontier pipeline, no dead /tmp adoption."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time

from migration_crash_resume import CrashResume
from migration_frontier_inputs import InputPipeline
from migration_frontier_resume import FrontierResume, candidate, MAX_TASK_BYTES
from migration_io import read_json
from migration_overlap_resume import OverlapResume
from migration_task_publisher import BoundedTaskPublisher


class ColdFrontierResume(FrontierResume):
    def __init__(self, control, warm_tasks):
        self._metadata_batching = {'scope':'current-producer', 'submitted_batches':0,
            'submitted_entries':0, 'full_batches':0, 'completed_batches':0,
            'batch_seconds':0.0, 'child_cpu_seconds':0.0}
        CrashResume.__init__(self, control, warm_tasks)
        self.cfg = dict(self.cfg, metadata_client_processes=32, distributed_target_threads=2048)
        self._distributed_workers = self.filter_workers = 2048
        self._publication_check_on_submit = False
        self._task_publisher.finish()
        self._task_publisher = BoundedTaskPublisher(workers=16, max_pending=64, check_on_submit=False)
        self._publication_workers, self._publication_limit = 16, 64
        self.adopted_task_ids = set()  # This is genuinely a new namespace.
        self.combined_manifest = self.control / 'manifests' / 'recovery-missing.jsonl'
        self._formal_scan_batch_files = self.scan_batch_files
        self._inputs = InputPipeline(self, candidate)
        self._task_batch_policy = {'max_files':self.scan_batch_files, 'max_bytes':MAX_TASK_BYTES,
            'oversize':'singleton', 'new_tasks':0, 'oversize_singletons':0,
            'largest_multi_file_task_bytes':0, 'warm_only':True, 'revision':0, 'history':[]}
        self._save_timing = {'scope':'current-producer', 'periodic_timer_coalescing':'warm-only',
            **{k:{'calls':0,'failures':0,'seconds':0.0,'last_seconds':0.0,'max_seconds':0.0}
               for k in ('save','progress')}}
        self.feed_reader = ThreadPoolExecutor(max_workers=1, thread_name_prefix='feed-health')
        self.feed_future = None
        self.feed_next = 0
        self.set_copy_batch(256)
        self.save(task_batch_policy=self._task_batch_policy,
            recovery_label='冷恢复：未完成队列优先，旧候选与分布式发现并行供给')

    def start_seed(self):
        return OverlapResume.start_seed(self)

    def enqueue_warm(self):
        return CrashResume.enqueue_warm(self)

    def _read_feeds(self):
        result = []
        for host in self.cfg['copy_hosts']:
            for name in ('progress.json', 'import-progress.json'):
                try:
                    value = read_json(self.control / 'frontier-input' / host / name)
                except FileNotFoundError:
                    result.append({'host':host,'feed':name,'status':'waiting'})
                    continue
                if value.get('run_id') != self.cfg['run_id'] or value.get('host') != host:
                    raise ValueError('feed identity differs')
                if value.get('status') == 'failed':
                    raise RuntimeError('distributed feed failed: '+host+' '+name)
                result.append({'host':host,'feed':name,'status':value.get('status')})
        return result

    def feed_frontier(self):
        """Discovery remains required; old hints are acceleration, not completeness proof."""
        complete = False
        while not complete or self._inputs.busy():
            self._check_stop()
            self.pump_inputs(force=True)
            self._harvest_probes(True)
            if self.feed_future is not None and self.feed_future.done():
                rows = self.feed_future.result()
                complete = all(r['status']=='complete' for r in rows)
                self.state['distributed_feeds'] = rows
                self.feed_future = None
                self.feed_next = time.monotonic()+5
            if not complete and self.feed_future is None and time.monotonic() >= self.feed_next:
                self.feed_future = self.feed_reader.submit(self._read_feeds)
            self._checkpoint()
            time.sleep(.005)
        self._drain_probes()
        # These producers have all published their terminal records and the
        # accepted input groups have crossed their probe/publication/ack
        # barriers.  The following mandatory current-source scan discovers
        # later additions independently; keeping the hint readers alive here
        # only rebuilds the completed-path set and periodically re-enumerates
        # immutable, already acknowledged ready files on the producer's GIL.
        # Do not retire on a missing/failed feed or while a group is in flight.
        self._inputs.close()
        self.state['distributed_input_retired'] = {
            'reason': 'all-feeds-complete-and-inputs-drained',
            'files': self._inputs.files,
            'entries': self._inputs.entries,
            'current_source_full_scan_required': True,
        }

    def run(self):
        try:
            return super().run()
        finally:
            self.feed_reader.shutdown(wait=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=('run',));p.add_argument('--control',required=True)
    p.add_argument('--warm-tasks',required=True);a=p.parse_args()
    ColdFrontierResume(a.control,a.warm_tasks).run()


if __name__=='__main__':main()
