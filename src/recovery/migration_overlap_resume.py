#!/usr/bin/env python3
"""Copy uncompleted payloads while rebuilding the historical index separately.

Warm work uses the otherwise unused p3-r3 namespace. It never becomes a formal
pipeline round. The subsequent eight stages rescan every path, so source
changes after warm copies cannot be hidden by an assigned-path skip set.
"""
import argparse
import concurrent.futures
import fcntl
import json
import os
import pathlib
import re
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import traceback

from migration_fresh_resume import FreshResumePipeline
from migration_io import read_json, write_json
from migration_parallel_pipeline import ParallelPipeline
from migration_pipeline_v2 import PipelineCoordinator as AsyncPipeline
from migration_resume_index import _resume_controls
from migration_run import Coordinator, now, queue_dirs, validate_mounts
from migration_scan import _stat_fields
from migration_worker import _components, _open_parent


WARM_STAGE = 'p3-r3'


def merge_history(candidate, live_path, progress=None, chunk_size=1000):
    """Never replace a current-run row; retain matching historical ownership."""
    source = sqlite3.connect(pathlib.Path(candidate).as_uri() + '?mode=ro', uri=True)
    target = sqlite3.connect(str(live_path), timeout=120)
    target.execute('PRAGMA busy_timeout=120000')
    target.execute('PRAGMA journal_mode=WAL')
    result = {'rows_processed': 0, 'rows_inserted': 0, 'ownership_upgraded': 0}
    try:
        cursor = source.execute('SELECT rel,source,destination,owned,sha256 FROM files')
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            before = target.total_changes
            target.executemany('INSERT OR IGNORE INTO files VALUES (?,?,?,?,?)', rows)
            result['rows_inserted'] += target.total_changes - before
            # An equal-content warm skip may precede history import. Preserve
            # its fresh snapshots while adding only independently proven
            # ownership for the same regular inode and checksum.
            owned = {row[0]: row for row in rows if row[3] and row[4]}
            if owned:
                query = ('SELECT rel,destination,sha256 FROM files WHERE owned=0 AND rel IN (' +
                         ','.join('?' for _ in owned) + ')')
                for rel, destination, digest in target.execute(query, tuple(owned)):
                    old = json.loads(owned[rel][2]); current = json.loads(destination)
                    if (digest == owned[rel][4] and current.get('ino') == old.get('ino') and
                            stat.S_ISREG(current.get('mode', 0)) and stat.S_ISREG(old.get('mode', 0))):
                        target.execute('UPDATE files SET owned=1 WHERE rel=? AND owned=0', (rel,))
                        result['ownership_upgraded'] += 1
            target.commit()
            result['rows_processed'] += len(rows)
            if progress:
                progress(dict(result))
        return result
    finally:
        target.close()
        source.close()


class OverlapResume(ParallelPipeline):
    def __init__(self, control, warm_tasks):
        self.control = pathlib.Path(control).resolve()
        self.cfg = read_json(self.control / 'config.json')
        run_id = self.cfg['run_id']
        if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', run_id):
            raise ValueError('invalid run_id')
        if (self.cfg.get('source'), self.cfg.get('destination')) != (
                '/share/project/eai_pwm', '/share-mixed/project/eai_pwm'):
            raise ValueError('overlap recovery is restricted to eai_pwm priorities 1-3')
        self._lock = open('/tmp/' + run_id + '-coordinator.lock', 'a')
        fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        FreshResumePipeline._require_isolation(self)
        validate_mounts(self.cfg)
        self.warm_tasks = pathlib.Path(warm_tasks).resolve()
        if not self.warm_tasks.is_file():
            raise FileNotFoundError(str(self.warm_tasks))
        for root in (self.cfg['source'], self.cfg['destination']):
            if os.path.commonpath((os.path.realpath(root), str(self.control))) == os.path.realpath(root):
                raise ValueError('control must remain outside both data trees')
        self.live_path = pathlib.Path('/tmp') / (run_id + '-index.sqlite')
        self.candidate = pathlib.Path('/tmp') / (run_id + '-resume-candidate.sqlite')
        for path in (self.live_path, self.candidate):
            if any(pathlib.Path(str(path) + suffix).exists() for suffix in ('', '-wal', '-shm')):
                raise FileExistsError('fresh overlap refuses existing local index: ' + str(path))
        for name in ('pipeline.json', 'overlap-bootstrap.json'):
            if (self.control / name).exists() or (self.control / '.snapshots' / name).exists():
                raise RuntimeError('overlap bootstrap already attempted')
        for folder in ('stages', 'manifests'):
            root = self.control / folder
            if root.is_symlink() or (root.exists() and any(root.rglob('*.json*'))):
                raise RuntimeError('fresh overlap refuses existing payload/manifest files')
        controls = _resume_controls(self.cfg['resume_from'])
        for ancestor in controls:
            old = read_json(ancestor / 'config.json')
            if (old.get('run_id') == run_id or
                    (old.get('source'), old.get('destination')) !=
                    (self.cfg['source'], self.cfg['destination'])):
                raise ValueError('predecessor identity or scope mismatch')
        self.db = sqlite3.connect(str(self.live_path), timeout=120)
        self.db.execute('PRAGMA busy_timeout=120000')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE files (rel TEXT PRIMARY KEY,source TEXT,destination TEXT,owned INTEGER,sha256 TEXT)')
        self.db.commit()
        started = now()
        self.state = {'status': 'starting', 'started': started, 'run_id': run_id, 'phases': {},
                      'stage': None, 'pipeline': True, 'workers_expected': 12,
                      'threads_per_node': int(self.cfg.get('copy_threads_per_node', 128)),
                      'resumed_from': str(controls[0]), 'resume_index_building': True,
                      'recovery_warm': True, 'recovery_label': '先复制未完成内容，同时恢复历史索引'}
        self.phase_progress = {str(p): {'discovered_files': 0, 'discovered_bytes': 0,
                                       'verified_files': 0, 'verified_bytes': 0,
                                       'inventory_complete': False} for p in (1, 2, 3)}
        self.current_scan = {}; self.hardlinks = {}; self.jobs = {}; self.children = {}
        self.children_lock = threading.Lock()
        self.hardlink_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.directory_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.pipeline = {'version': 2, 'started': started, 'stages': {}, 'directory_jobs': {}}
        self.pending_adoption = None
        self.enqueued_batches = self.discovered_files = self.discovered_bytes = 0
        self.assigned = set(); self.next_task_id = 0
        self.scan_workers = max(1, int(self.cfg.get('parallel_scan_threads', 64)))
        self.scan_batch_files = max(1, int(self.cfg.get('scan_batch_files', 64)))
        self.last_scan_save = time.monotonic()
        self.seed_process = None; self.seed_result = self.control / 'overlap-seed-result.json'
        self.seed_summary = None; self.seed_error = None
        FreshResumePipeline._copy_extension(self, controls[0], [str(p) for p in reversed(controls)])
        write_json(self.control / 'overlap-bootstrap.json', {
            'run_id': run_id, 'started': started, 'warm_tasks': str(self.warm_tasks),
            'live_index': str(self.live_path), 'candidate_index': str(self.candidate)})
        self.save(status='starting')

    def start_seed(self):
        log = self.control / 'overlap-seed.log'
        with open(log, 'ab', buffering=0) as output:
            self.seed_process = subprocess.Popen([
                sys.executable, '-u', str(pathlib.Path(__file__).resolve()), 'seed',
                '--control', str(self.control), '--candidate', str(self.candidate),
                '--result', str(self.seed_result)], stdin=subprocess.DEVNULL,
                stdout=output, stderr=subprocess.STDOUT, close_fds=True)

    def _poll_seed(self):
        if self.seed_summary is not None or self.seed_process is None or self.seed_process.poll() is None:
            return
        try:
            result = read_json(self.seed_result)
            if result.get('ok') is True:
                self.seed_summary = result['summary']
                self.state.update(resume_index=self.seed_summary, resume_index_building=False,
                                  resume_index_import=result['merge'])
                write_json(self.control / 'resume-index.json', self.seed_summary)
            else:
                self.seed_error = result.get('error', 'index recovery failed')
                self.state.update(resume_index_error=self.seed_error, resume_index_building=False)
        except (OSError, ValueError, KeyError) as exc:
            self.seed_error = type(exc).__name__ + ': ' + str(exc)
            self.state.update(resume_index_error=self.seed_error, resume_index_building=False)

    def save(self, **updates):
        self._poll_seed()
        super().save(**updates)

    def fresh_entry(self, original):
        rel = original['rel']; _components(rel)
        if rel == 'home/hcr' or rel.startswith('home/hcr/') or rel == 'repos/cosmos-framework-pwm' or rel.startswith('repos/cosmos-framework-pwm/'):
            raise ValueError('warm tasks must be from priority3 only')
        parent, name = _open_parent(self.cfg['source'], rel)
        try:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            entry = {'rel': rel, **_stat_fields(info)}
            if stat.S_ISREG(info.st_mode):
                entry['kind'] = 'file'
            elif stat.S_ISLNK(info.st_mode):
                entry.update(kind='symlink', target=os.readlink(name, dir_fd=parent))
                entry['link_target'] = entry['target']
                after = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if (after.st_ino, after.st_ctime_ns) != (info.st_ino, info.st_ctime_ns):
                    raise RuntimeError('warm symlink changed during inventory')
            else:
                raise ValueError('warm source no longer is a regular file or symlink')
            if isinstance(original.get('known'), dict):
                entry['known'] = original['known']
            return entry
        finally:
            os.close(parent)

    def enqueue_warm(self):
        self._check_stop()
        self.stage = WARM_STAGE
        queue_dirs(self.control, WARM_STAGE)
        (self.control / 'manifests').mkdir(exist_ok=True)
        self.save(status='copying', phase=3, round=3, stage=WARM_STAGE, scan_complete=False)
        manifest = self.control / 'manifests' / 'recovery-warm.jsonl'
        issues = self.control / 'recovery-warm-issues.jsonl'
        seen = set(); batch = []; failures = 0
        with open(manifest, 'x', encoding='utf-8') as inventory, open(issues, 'x', encoding='utf-8') as errors:
            def flush():
                if batch:
                    inventory.flush()
                    self._emit_tasks(batch, 3, 3)
                    batch.clear()
            with open(self.warm_tasks, encoding='utf-8') as source:
                for line in source:
                    self._check_stop()
                    payload = json.loads(line); task = payload.get('task', payload)
                    for original in task['entries']:
                        rel = original['rel']
                        if rel in seen:
                            continue
                        seen.add(rel)
                        try:
                            entry = self.fresh_entry(original)
                        except (OSError, ValueError, RuntimeError) as exc:
                            failures += 1
                            errors.write(json.dumps({'rel': rel, 'error': str(exc)}) + '\n')
                            continue
                        inventory.write(json.dumps(entry, ensure_ascii=True) + '\n')
                        batch.append(entry)
                        if len(batch) >= self.scan_batch_files:
                            flush()
            flush()
        self.state['warm_inventory'] = {'entries_seen': len(seen), 'errors': failures,
                                        'manifest': str(manifest), 'batches': self.enqueued_batches}
        self.save()

    def supplement_missing(self):
        self._check_stop()
        from migration_parallel_scan import scan_phase
        def emit(entries):
            missing = []
            for entry in entries:
                if entry['rel'] in self.assigned:
                    continue
                parent = None
                try:
                    parent, name = _open_parent(self.cfg['destination'], entry['rel'])
                    os.stat(name, dir_fd=parent, follow_symlinks=False)
                except FileNotFoundError:
                    missing.append(entry)
                except OSError:
                    # Existing conflicting/unsafe paths are handled by the
                    # formal passes, not mistaken for safe missing targets.
                    pass
                finally:
                    if parent is not None:
                        os.close(parent)
            self._emit_tasks(missing, 3, 3)
        self.save(status='scanning', recovery_label='继续复制缺失目标，同时恢复历史索引')
        stats = scan_phase(self.cfg['source'], 3, emit,
                           self.control / 'manifests' / 'recovery-missing.jsonl',
                           workers=self.scan_workers, batch_files=self.scan_batch_files,
                           batch_bytes=1024**3, hardlink_index={}, entries_per_second=0,
                           control=str(self.control))
        self.state['warm_missing_scan'] = stats
        self.save(status='copying', scan_complete=True)

    def run(self):
        self._check_stop()
        self.start_seed()
        try:
            self.enqueue_warm()
            if self.cfg.get('warm_scan_missing', True):
                self.supplement_missing()
            self.drain(WARM_STAGE)
            summary = Coordinator.collect(self, WARM_STAGE)
            self.state['warm_summary'] = summary
            write_json(self.control / 'recovery-warm-summary.json', summary)
            # This barrier waits only after all safely missing targets have
            # been supplied. A seed failure never marks data workers terminal.
            while self.seed_summary is None:
                self._check_stop()
                self.save(status='waiting_resume_index', recovery_label='缺失目标已复制；等待索引恢复/修复')
                if self.seed_summary is None:
                    time.sleep(2)
            self.state.update(recovery_warm=False, recovery_label=None)
            self.hardlinks = {}
            self.save(status='starting')
        except BaseException:
            if (self.control / 'STOP').exists():
                self.save(status='stopped', error=traceback.format_exc())
            raise
        return AsyncPipeline.run(self)


def seed_child(control, candidate, result_path):
    control = pathlib.Path(control)
    cfg = read_json(control / 'config.json')
    started = now()
    def report(payload):
        if (control / 'STOP').exists():
            raise RuntimeError('STOP requested during background index recovery')
        write_json(control / 'seed-progress.json', dict(payload, run_id=cfg['run_id'],
                   started=started, updated=now(), resume_from=cfg['resume_from']))
    try:
        from migration_resume_index_parallel_memory import build_resume_index
        summary = build_resume_index(cfg['resume_from'], candidate, progress_callback=report,
                                     read_workers=int(cfg.get('resume_read_workers', 64)),
                                     strict_enumeration=cfg.get('resume_strict_enumeration', False))
        live = pathlib.Path('/tmp') / (cfg['run_id'] + '-index.sqlite')
        merged = merge_history(candidate, live,
                               lambda counts: report(dict(summary, status='importing', **counts)))
        report(dict(summary, status='complete', **merged))
        write_json(result_path, {'ok': True, 'summary': summary, 'merge': merged, 'finished': now()})
    except BaseException:
        write_json(result_path, {'ok': False, 'error': traceback.format_exc(), 'finished': now()})
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run', 'seed'))
    parser.add_argument('--control', required=True)
    parser.add_argument('--warm-tasks')
    parser.add_argument('--candidate')
    parser.add_argument('--result')
    args = parser.parse_args()
    os.umask(0o077)
    if args.mode == 'seed':
        if not args.candidate or not args.result:
            parser.error('seed requires --candidate and --result')
        raise SystemExit(seed_child(args.control, args.candidate, args.result))
    if not args.warm_tasks:
        parser.error('run requires --warm-tasks')
    OverlapResume(args.control, args.warm_tasks).run()


if __name__ == '__main__':
    main()
