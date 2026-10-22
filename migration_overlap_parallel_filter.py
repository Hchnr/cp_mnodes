#!/usr/bin/env python3
"""Adopt the live recovery queue and parallelize destination existence probes.

Copy workers and the existing index seed child retain their PIDs and leases.
Only the foreground producer is replaced; new IDs cover the durable assignment
ledger, and all old manifests stay immutable.
"""
import argparse
import concurrent.futures
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
import traceback

from migration_fresh_resume import FreshResumePipeline
from migration_io import read_json, write_json
from migration_overlap_resume import OverlapResume, WARM_STAGE
from migration_parallel_pipeline import complete_records, process_matches
from migration_parallel_scan import scan_phase
from migration_pipeline_v2 import PipelineCoordinator as AsyncPipeline
from migration_run import Coordinator, now, queue_dirs, validate_mounts
from migration_worker import _components, _open_parent


class AdoptedSeed:
    """Observe identity, never create, signal or waitpid somebody else's child."""
    def __init__(self, identity):
        self.identity = identity
        self.pid = identity['pid']

    def poll(self):
        return None if process_matches(self.identity) else 0


def _identity(value, name):
    if (not isinstance(value, dict) or type(value.get('pid')) is not int or value['pid'] <= 0 or
            not str(value.get('starttime_ticks', '')).isdigit() or
            not isinstance(value.get('argv'), list) or not value['argv'] or
            not all(isinstance(arg, str) for arg in value['argv'])):
        raise ValueError('complete PID/starttime/argv identity required for ' + name)
    return value


def _task_records(path):
    with open(path, encoding='utf-8') as handle:
        for line in handle:
            if not line.endswith('\n'):
                raise ValueError('assignment ledger must contain complete JSONL records')
            payload = json.loads(line)
            task = payload.get('task', payload)
            if (not isinstance(task, dict) or type(task.get('id')) is not int or task['id'] < 0 or
                    not isinstance(task.get('entries'), list)):
                raise ValueError('assignment ledger requires complete task payloads')
            if (task.get('phase'), task.get('round')) != (3, 3):
                raise ValueError('assignment belongs to a different stage')
            for entry in task['entries']:
                if not isinstance(entry, dict) or not isinstance(entry.get('rel'), str):
                    raise ValueError('invalid assigned entry')
                _components(entry['rel'])
            yield task


class ParallelFilterResume(OverlapResume):
    def __init__(self, control, handover):
        # Never call either bootstrap constructor: the live SQLite/WAL and the
        # old seed child already own durable work that must be retained.
        self.control = Path(control).resolve()
        self.cfg = read_json(self.control / 'config.json')
        if (self.cfg.get('source'), self.cfg.get('destination')) != (
                '/share/project/eai_pwm', '/share-mixed/project/eai_pwm'):
            raise ValueError('parallel recovery remains limited to eai_pwm')
        run_id = self.cfg['run_id']
        import re
        if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', run_id):
            raise ValueError('invalid run ID')
        self._lock = open('/tmp/' + run_id + '-coordinator.lock', 'a')
        fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        FreshResumePipeline._require_isolation(self)
        validate_mounts(self.cfg)
        self.handover_path = Path(handover).resolve()
        self.handover = read_json(self.handover_path)
        if (self.handover.get('control'), self.handover.get('stage')) != (str(self.control), WARM_STAGE):
            raise ValueError('handover must identify this warm recovery queue')
        old = _identity(self.handover.get('old_producer'), 'old producer')
        if process_matches(old):
            raise RuntimeError('old foreground producer remains live')
        helper = _identity(self.handover.get('seed_helper'), 'seed helper')
        self.seed_result = Path(helper['result_path']).resolve()
        if os.path.commonpath((str(self.control), str(self.seed_result))) != str(self.control):
            raise ValueError('seed result must remain within this control')
        if not process_matches(helper) and not self.seed_result.is_file():
            raise RuntimeError('original seed exited without a result; explicit repair required')
        self.seed_process = AdoptedSeed(helper)
        self.seed_summary = None
        self.seed_error = None
        self.live_path = Path('/tmp') / (run_id + '-index.sqlite')
        if not self.live_path.is_file():
            raise FileNotFoundError('existing live index required')
        self.db = sqlite3.connect(str(self.live_path), timeout=120)
        self.db.execute('PRAGMA busy_timeout=120000')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('SELECT rel FROM files LIMIT 1').fetchone()
        self.state = read_json(self.control / 'state.json')
        if self.state.get('recovery_warm') is not True or self.state.get('stage') != WARM_STAGE:
            raise RuntimeError('foreground handover is only valid during warm recovery')
        progress = read_json(self.control / 'progress.json')
        self.phase_progress = progress.get('phases', {})
        for p in (1, 2, 3):
            self.phase_progress.setdefault(str(p), {'discovered_files': 0, 'discovered_bytes': 0,
                'verified_files': 0, 'verified_bytes': 0, 'inventory_complete': False})
        self.current_scan = progress.get('current_scan', {})
        self.pipeline = read_json(self.control / 'pipeline.json')
        if self.pipeline.get('stages'):
            raise RuntimeError('formal rounds have already started; new handover required')
        self.stage = WARM_STAGE
        self.hardlinks = {}
        self.jobs = {}
        self.children = {}
        self.children_lock = threading.Lock()
        self.hardlink_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.directory_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.pending_adoption = None
        self.assigned = set()
        self.assigned_tasks = Path(self.handover['assigned_tasks_path']).resolve()
        maximum = -1
        self.prepared_resume = None
        assignment_input = self.assigned_tasks
        if self.handover.get('prepared_resume'):
            from migration_prepared_resume import load_checkpoint
            self.prepared_resume, self.prepared_values = load_checkpoint(self.handover, self.control)
            self.assigned = self.prepared_values['assigned']
            maximum = self.prepared_resume['maximum_id']
            assignment_input = Path(self.handover['assignment_delta_path']).resolve()
            if not assignment_input.is_relative_to(self.control):
                raise ValueError('assignment delta must remain in this control')
            from migration_prepared_resume import digest_file
            if digest_file(assignment_input) != self.handover.get('assignment_delta_sha256'):
                raise ValueError('assignment delta digest mismatch')
        for task in _task_records(assignment_input):
            maximum = max(maximum, task['id'])
            self.assigned.update(entry['rel'] for entry in task['entries'])
        self.next_task_id = self.handover['next_task_id']
        if type(self.next_task_id) is not int or self.next_task_id <= maximum:
            raise ValueError('next_task_id must exceed every durable assigned task ID')
        self.enqueued_batches = self.next_task_id
        self.discovered_files = int(self.current_scan.get('files', 0))
        self.discovered_bytes = int(self.current_scan.get('bytes', 0))
        self.scan_workers = max(1, int(self.cfg.get('parallel_scan_threads', 64)))
        self.scan_batch_files = max(1, int(self.cfg.get('scan_batch_files', 64)))
        self.filter_workers = max(1, min(128, int(self.cfg.get('parallel_target_threads', 64))))
        self.filter_limit = max(self.filter_workers, min(4096, int(self.cfg.get('target_filter_inflight', 4096))))
        self.last_scan_save = time.monotonic()
        digest = hashlib.sha256(self.handover_path.read_bytes()).hexdigest()[:24]
        self.adoption = self.control / 'parallel-filter-adoptions' / digest
        if self.adoption.exists():
            raise RuntimeError('this foreground handover has already been consumed')
        self.adoption.mkdir(parents=True)
        self.combined_manifest = self.control / 'manifests' / ('recovery-missing-parallel-' + digest + '.jsonl')
        if self.prepared_resume:
            self.combined_manifest = Path(self.prepared_resume['manifest']).resolve()
            if not self.combined_manifest.is_relative_to(self.control) or not self.combined_manifest.is_file():
                raise ValueError('prepared manifest must exist within this control')
        elif self.combined_manifest.exists():
            raise FileExistsError(str(self.combined_manifest))
        self.old_manifests = [Path(self.handover[key]).resolve() for key in ('warm_manifest', 'missing_manifest')]
        for path in self.old_manifests:
            if os.path.commonpath((str(self.control), str(path))) != str(self.control) or not path.is_file():
                raise ValueError('old manifest must be a preserved file within this control')
        write_json(self.adoption / 'started.json', {'started': now(), 'handover': str(self.handover_path),
            'seed_helper': helper, 'assigned_paths': len(self.assigned), 'next_task_id': self.next_task_id,
            'combined_manifest': str(self.combined_manifest)})
        self.save(status='scanning', recovery_label=str(self.filter_workers)+'路并行检查目标，恢复续传与历史索引同时进行',
                  parallel_target_threads=self.filter_workers, target_filter_inflight=self.filter_limit,
                  active_manifest=str(self.combined_manifest), foreground_handover=str(self.handover_path))

    def _probe_missing(self, entry):
        parent = None
        try:
            parent, name = _open_parent(self.cfg['destination'], entry['rel'])
            os.stat(name, dir_fd=parent, follow_symlinks=False)
            return None
        except FileNotFoundError:
            if entry.get('_refresh_source'):
                try:
                    return self.fresh_entry(entry)
                except (OSError, ValueError, RuntimeError):
                    # Online source changes are revisited by all formal rounds.
                    return None
            return entry
        except OSError:
            # Conflict/unsafe targets remain for the formal checked passes.
            return None
        finally:
            if parent is not None:
                os.close(parent)

    def supplement_missing(self):
        self._check_stop()
        queue_dirs(self.control, WARM_STAGE)
        pending = set()
        selected = []
        selected_bytes = 0
        filter_counts = {'submitted': 0, 'checked': 0, 'missing': 0, 'peak_inflight': 0}
        issues = []
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.filter_workers,
                                                     thread_name_prefix='target-exists')
        def flush():
            nonlocal selected_bytes
            if selected:
                self._emit_tasks(selected, 3, 3)
                selected.clear()
                selected_bytes = 0
        def consume_done(block=False):
            nonlocal selected_bytes
            if not pending:
                return
            done, _ = concurrent.futures.wait(pending, timeout=.1 if block else 0,
                                              return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                entry = future.result()
                filter_counts['checked'] += 1
                if entry is None or entry['rel'] in self.assigned:
                    continue
                size = entry.get('size', 0) if entry.get('kind') == 'file' else 0
                if selected and (len(selected) >= self.scan_batch_files or selected_bytes + size > 1024**3):
                    flush()
                selected.append(entry)
                selected_bytes += size
                filter_counts['missing'] += 1
                if len(selected) >= self.scan_batch_files or selected_bytes >= 1024**3:
                    flush()
            if time.monotonic() - self.last_scan_save >= 3:
                self.save(target_filter=dict(filter_counts, inflight=len(pending)),
                          next_task_id=self.next_task_id)
                self.last_scan_save = time.monotonic()
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
                filter_counts['peak_inflight'] = max(filter_counts['peak_inflight'], len(pending))
            consume_done(False)
        def records():
            seen = set()
            # All complete old inventory records are retained in the combined
            # manifest; only unassigned copyable orphans need target probes.
            for path in self.old_manifests:
                local_issues = []
                for entry in complete_records(path, local_issues):
                    self._check_stop()
                    rel = entry['rel']
                    if rel in seen:
                        continue
                    _components(rel) if rel else None
                    seen.add(rel)
                    if (entry.get('kind') in ('file', 'symlink') and not entry.get('hardlink_to')
                            and rel not in self.assigned):
                        # Old records may need current source identities. Do
                        # that work in the same probe thread, only if their
                        # target is missing, rather than creating a new serial
                        # source-stat bottleneck during manifest adoption.
                        submit([dict(entry, _refresh_source=True)])
                    yield entry
                issues.extend(dict(issue, manifest=str(path)) for issue in local_issues)
            # A claim may have been published beyond a partial manifest tail.
            for task in _task_records(self.assigned_tasks):
                for entry in task['entries']:
                    if entry['rel'] not in seen:
                        seen.add(entry['rel'])
                        yield {key: value for key, value in entry.items() if key != 'known'}
        try:
            stats = scan_phase(self.cfg['source'], 3, submit, self.combined_manifest,
                               workers=self.scan_workers, batch_files=self.scan_batch_files,
                               batch_bytes=1024**3, hardlink_index={}, entries_per_second=0,
                               control=str(self.control), resume_records=records())
            while pending:
                self._check_stop()
                consume_done(True)
            flush()
            write_json(self.adoption / 'manifest-tail-issues.json', {'issues': issues,
                       'preserved_manifests': list(map(str, self.old_manifests))})
            self.state['warm_missing_scan'] = stats
            self.save(status='copying', scan_complete=True, target_filter=filter_counts)
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    def run(self):
        self._check_stop()
        try:
            self.supplement_missing()
            self.drain(WARM_STAGE)
            summary = Coordinator.collect(self, WARM_STAGE)
            self.state['warm_summary'] = summary
            write_json(self.control / 'recovery-warm-summary.json', summary)
            while self.seed_summary is None:
                self._check_stop()
                self.save(status='waiting_resume_index', recovery_label='缺失目标已复制；等待索引恢复/修复')
                if self.seed_summary is None:
                    time.sleep(2)
            self.state.update(recovery_warm=False, recovery_label=None)
            self.hardlinks = {}
            self.save(status='starting')
        except BaseException:
            self.save(status='stopped' if (self.control / 'STOP').exists() else 'failed',
                      error=traceback.format_exc())
            raise
        return AsyncPipeline.run(self)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    ParallelFilterResume(args.control, args.handover).run()


if __name__ == '__main__':
    main()
