#!/usr/bin/env python3
"""Explicit partial-stage handover with fresh task IDs and parallel scanning.

Original manifests and active broker leases remain untouched. The immutable
handover describes all assigned task payloads and any preserved helper PIDs.
"""
import argparse
import hashlib
import json
import os
import pathlib
import sqlite3
import time
import traceback

from migration_io import read_json, write_json
from migration_pipeline import PipelineCoordinator as FirstPipeline
from migration_pipeline_v2 import PipelineCoordinator as SecondPipeline, PENDING
from migration_run import now, validate_mounts


def complete_records(path, issues=None):
    """Read only complete JSONL lines; retain the original file unchanged."""
    with open(path, 'rb') as handle:
        while True:
            offset = handle.tell()
            line = handle.readline()
            if not line:
                break
            try:
                if not line.endswith(b'\n'):
                    raise ValueError('incomplete trailing JSONL record')
                value = json.loads(line)
                if not isinstance(value, dict) or not isinstance(value.get('rel'), str):
                    raise ValueError('invalid manifest record')
            except (ValueError, UnicodeError) as exc:
                if issues is not None:
                    issues.append({'offset': offset, 'bytes': len(line), 'detail': str(exc)})
                # Only the final incomplete line is recoverable by a new walk.
                if not handle.read(1):
                    break
                raise RuntimeError('invalid non-tail manifest record at ' + str(offset)) from exc
            yield value


def task_entries(path):
    with open(path, encoding='utf-8') as handle:
        for line in handle:
            task = json.loads(line)
            entries = task.get('entries') if isinstance(task, dict) else None
            if not isinstance(entries, list):
                raise ValueError('assigned_tasks JSONL requires complete task payloads')
            for entry in entries:
                if not isinstance(entry, dict) or not isinstance(entry.get('rel'), str):
                    raise ValueError('invalid assigned task entry')
                yield entry


def process_matches(helper):
    root = pathlib.Path('/proc') / str(helper['pid'])
    try:
        fields = (root / 'stat').read_text().rsplit(') ', 1)[1].split()
        if fields[0] == 'Z' or str(fields[19]) != str(helper['starttime_ticks']):
            return False
        expected = helper.get('argv')
        if expected:
            actual = (root / 'cmdline').read_bytes().decode(errors='surrogateescape').split('\x00')
            if actual and actual[-1] == '':
                actual.pop()
            if expected and expected[-1] == '':
                expected = expected[:-1]
            if actual != expected:
                return False
        return True
    except (FileNotFoundError, ProcessLookupError):
        return False


class ParallelPipeline(SecondPipeline):
    def __init__(self, control, handover):
        # V2 normally refuses a partial manifest. This explicit handover is its
        # only exception, and still uses V1's non-destructive index constructor.
        FirstPipeline.__init__(self, control)
        self.pending_adoption = None
        self.handover_path = pathlib.Path(handover).resolve()
        self.handover = read_json(self.handover_path)
        if self.handover.get('control') not in (None, str(self.control)):
            raise ValueError('handover belongs to a different control directory')
        self.stage = self.handover['stage']
        if self.state.get('stage') != self.stage or self.stage in self.pipeline['stages']:
            raise RuntimeError('handover must name the currently unfinished stage')
        self.phase = int(self.handover.get('phase', self.stage[1]))
        self.round_no = int(self.handover.get('round', self.stage[4]))
        if self.stage != 'p%d-r%d' % (self.phase, self.round_no):
            raise ValueError('handover phase/round mismatch')
        self.next_task_id = self.handover['next_task_id']
        if not isinstance(self.next_task_id, int) or self.next_task_id < 0:
            raise ValueError('next_task_id must be nonnegative')
        broker_db = pathlib.Path('/tmp') / (self.cfg['run_id'] + '-broker.sqlite')
        if not broker_db.is_file():
            raise FileNotFoundError('existing local broker index required')
        reader = sqlite3.connect(broker_db.as_uri() + '?mode=ro', uri=True, timeout=5)
        try:
            maximum = reader.execute('SELECT MAX(id) FROM claims WHERE stage=?', (self.stage,)).fetchone()[0]
        finally:
            reader.close()
        if maximum is not None and self.next_task_id <= maximum:
            raise RuntimeError('handover next_task_id would reuse a durable broker task ID')
        self.old_manifest = pathlib.Path(self.handover['old_manifest'])
        self.assigned_path = pathlib.Path(self.handover['assigned_tasks_path'])
        self.assigned = {entry['rel'] for entry in task_entries(self.assigned_path)}
        self.assigned_at_handover = len(self.assigned)
        digest = hashlib.sha256(self.handover_path.read_bytes()).hexdigest()[:24]
        self.adoption = self.control / 'parallel-adoptions' / digest
        if self.adoption.exists():
            raise RuntimeError('handover already consumed; a new stable ID/coverage handover is required')
        self.adoption.mkdir(parents=True)
        self.combined_manifest = self.control / 'manifests' / (self.stage + '-parallel-' + digest + '.jsonl')
        if self.combined_manifest.exists():
            raise FileExistsError(str(self.combined_manifest))
        self._restore_finished_results()
        self._register_external_helpers()
        self.enqueued_batches = 0
        self.discovered_files = 0
        self.discovered_bytes = 0
        self.scan_workers = max(1, int(self.cfg.get('parallel_scan_threads', 32)))
        self.scan_batch_files = max(1, int(self.cfg.get('scan_batch_files', 64)))
        self.last_scan_save = time.monotonic()
        write_json(self.adoption / 'started.json', {
            'handover': str(self.handover_path), 'started': now(),
            'stage': self.stage, 'next_task_id': self.next_task_id,
            'combined_manifest': str(self.combined_manifest)})

    def _wait_external(self, helper):
        while process_matches(helper):
            self._check_stop()
            time.sleep(1)
        result = pathlib.Path(helper['result_path'])
        for attempt in range(5):
            try:
                return read_json(result)
            except (OSError, ValueError):
                if attempt == 4:
                    raise RuntimeError('preserved helper exited without a complete result: ' + str(result))
                time.sleep(1)

    def _register_external_helpers(self):
        for helper in self.handover.get('live_helpers', []):
            kind, stage = helper['kind'], helper['stage']
            if kind not in ('hardlinks', 'directories', 'collect-hardlinks'):
                raise ValueError('unsupported preserved helper kind')
            key = ('hardlinks' if kind == 'collect-hardlinks' else kind) + ':' + stage
            if key in self.jobs:
                raise ValueError('duplicate preserved helper for ' + key)
            record = self.pipeline['stages'].get(stage)
            if record is None:
                raise ValueError('preserved helper has no durable stage record: ' + stage)
            pool = self.directory_pool if kind == 'directories' else self.hardlink_pool
            future = pool.submit(self._wait_external, helper)
            self.jobs[key] = {'future': future, 'kind': 'directories' if kind == 'directories' else 'hardlinks',
                              'stage': stage, 'phase': record['phase'],
                              'dependencies': self._manifest_info(pathlib.Path(record['manifest'])),
                              'external_pid': helper['pid']}

    def _emit_tasks(self, entries, phase, round_no, count_discovery=True):
        self._check_stop()
        fresh, local = [], set()
        for original in entries:
            rel = original['rel']
            if rel in self.assigned or rel in local:
                continue
            entry = dict(original)
            row = self.db.execute('SELECT source,destination,owned,sha256 FROM files WHERE rel=?', (rel,)).fetchone()
            if row:
                entry['known'] = {'source': json.loads(row[0]), 'destination': json.loads(row[1]),
                                  'owned': bool(row[2]), 'sha256': row[3]}
            fresh.append(entry)
            local.add(rel)
        if not fresh:
            return
        task_id = self.next_task_id
        target = self.control / 'stages' / self.stage / 'pending' / str(task_id % 64) / (str(task_id) + '.json')
        if target.exists():
            raise RuntimeError('refusing to reuse existing pending task ID ' + str(task_id))
        write_json(target, {'id': task_id, 'phase': phase, 'round': round_no,
                            'op': 'sync', 'entries': fresh})
        self.assigned.update(local)
        self.next_task_id += 1
        self.enqueued_batches += 1
        if count_discovery:
            self.discovered_files += len(fresh)
            self.discovered_bytes += sum(e.get('size', 0) for e in fresh if e['kind'] == 'file')
        counters = self.phase_progress[str(phase)]
        counters['discovered_files'] = max(counters['discovered_files'], self.discovered_files)
        counters['discovered_bytes'] = max(counters['discovered_bytes'], self.discovered_bytes)
        self.current_scan.update(files=self.discovered_files, bytes=self.discovered_bytes)
        if time.monotonic() - self.last_scan_save >= 3:
            self.save(enqueued_batches=self.enqueued_batches, next_task_id=self.next_task_id)
            self.last_scan_save = time.monotonic()

    def _seed_and_orphans(self):
        seed = self.adoption / 'resume-records.jsonl'
        listed = set()
        issues = []
        orphan_batch = []
        with open(seed, 'x', encoding='utf-8') as handle:
            for entry in complete_records(self.old_manifest, issues):
                handle.write(json.dumps(entry, ensure_ascii=True, separators=(',', ':')) + '\n')
                listed.add(entry['rel'])
                if entry.get('kind') in ('file', 'symlink'):
                    self.discovered_files += 1
                    if entry['kind'] == 'file':
                        self.discovered_bytes += entry.get('size', 0)
                    if not entry.get('hardlink_to') and entry['rel'] not in self.assigned:
                        orphan_batch.append(entry)
                        if len(orphan_batch) >= self.scan_batch_files:
                            self._emit_tasks(orphan_batch, self.phase, self.round_no, count_discovery=False)
                            orphan_batch.clear()
            # Payloads are authoritative for assigned records missing from a
            # complete old line (e.g. a preserved damaged/truncated tail).
            supplemented = 0
            for original in task_entries(self.assigned_path):
                if original['rel'] in listed:
                    continue
                entry = {k: v for k, v in original.items() if k != 'known'}
                handle.write(json.dumps(entry, ensure_ascii=True, separators=(',', ':')) + '\n')
                listed.add(entry['rel'])
                supplemented += 1
                self.discovered_files += 1
                if entry.get('kind') == 'file':
                    self.discovered_bytes += entry.get('size', 0)
        if orphan_batch:
            self._emit_tasks(orphan_batch, self.phase, self.round_no, count_discovery=False)
        write_json(self.adoption / 'manifest-tail-issues.json', {
            'old_manifest': str(self.old_manifest), 'issues': issues,
            'supplemented_assigned_records': supplemented, 'assigned_paths': self.assigned_at_handover})
        return seed

    def _parallel_scan(self, phase, round_no, manifest, resume_records=None):
        from migration_parallel_scan import scan_phase
        self._check_stop()
        self.current_scan = {'phase': phase, 'stage': self.stage,
                             'files': self.discovered_files, 'bytes': self.discovered_bytes}
        self.save(status='scanning', phase=phase, round=round_no, stage=self.stage,
                  scan_complete=False, active_manifest=str(manifest), parallel_scan_threads=self.scan_workers,
                  next_task_id=self.next_task_id)
        stats = scan_phase(self.cfg['source'], phase,
                           lambda entries: self._emit_tasks(entries, phase, round_no), str(manifest),
                           batch_bytes=1024**3, batch_files=self.scan_batch_files, hardlink_index=self.hardlinks,
                           entries_per_second=0, workers=self.scan_workers, control=str(self.control),
                           resume_records=resume_records)
        self.phase_progress[str(phase)].update(discovered_files=stats['files'] + stats['symlinks'],
                                               discovered_bytes=stats['bytes'], inventory_complete=True)
        self.save(status='copying', scan_complete=True, scan=stats,
                  enqueued_batches=self.enqueued_batches, next_task_id=self.next_task_id)
        self.event('scan_complete', phase=phase, round=round_no, stats=stats,
                   batches=self.enqueued_batches, parallel=True, manifest=str(manifest))
        return self.stage, pathlib.Path(manifest), stats

    def scan(self, phase, round_no):
        from migration_run import queue_dirs
        self.stage = 'p%d-r%d' % (phase, round_no)
        queue_dirs(self.control, self.stage)
        self.next_task_id = 0
        self.assigned = set()
        self.enqueued_batches = self.discovered_files = self.discovered_bytes = 0
        manifest = self.control / 'manifests' / (self.stage + '.jsonl')
        return self._parallel_scan(phase, round_no, manifest)

    def run(self):
        validate_mounts(self.cfg)
        try:
            for record in list(self.pipeline['stages'].values()):
                self._queue_hardlinks(record)
                self._queue_directories(record)
            # Same stage throughout: existing workers keep their active leases.
            self.save(status='scanning', stage=self.stage, phase=self.phase, round=self.round_no,
                      active_manifest=str(self.combined_manifest), parallel_handover=str(self.handover_path))
            seed = self._seed_and_orphans()
            stage, manifest, stats = self._parallel_scan(
                self.phase, self.round_no, self.combined_manifest, complete_records(seed))
            self.drain(stage)
            record = {'phase': self.phase, 'round': self.round_no, 'stage': stage,
                      'manifest': str(manifest), 'scan': stats, 'summary': {PENDING: True},
                      'partial_handover': str(self.handover_path)}
            self.pipeline['stages'][stage] = record
            self._queue_hardlinks(record)
            self._queue_directories(record)
            self.save(status='payload_round_complete')
        except BaseException:
            # Preserved helpers are deliberately not killed/restarted here.
            with self.children_lock:
                for proc in self.children.values():
                    if proc.poll() is None:
                        proc.terminate()
            self.save(status='stopped' if (self.control / 'STOP').exists() else 'failed',
                      error=traceback.format_exc(), failed=now())
            self.hardlink_pool.shutdown(wait=False, cancel_futures=True)
            self.directory_pool.shutdown(wait=False, cancel_futures=True)
            self.db.close()
            raise
        return super().run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    ParallelPipeline(args.control, args.handover).run()


if __name__ == '__main__':
    main()
