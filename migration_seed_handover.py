#!/usr/bin/env python3
"""One-time takeover of an interrupted, task-free resume-index bootstrap.

The predecessor bootstrap must have been stopped by exact process identity.
This entry never deletes an old temporary DB or reuses its partial contents.
All existing worker, broker, metrics and watchdog processes are retained.
"""
import argparse
import concurrent.futures
import fcntl
import os
import pathlib
import re
import sqlite3
import threading
import time
import traceback
import uuid

from migration_fresh_resume import FreshResumePipeline
from migration_io import read_json, write_json
from migration_parallel_pipeline import process_matches
from migration_pipeline import open_index
from migration_resume_index import _resume_controls
from migration_resume_index_parallel import build_resume_index
from migration_run import now, validate_mounts


class SeedHandoverPipeline(FreshResumePipeline):
    def __init__(self, control, handover):
        # Neither existing constructor is suitable: the original may delete a
        # seeded DB, while handover constructors require an old local index.
        self.control = pathlib.Path(control).resolve()
        self.cfg = read_json(self.control / 'config.json')
        run_id = self.cfg['run_id']
        if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', run_id):
            raise ValueError('invalid run_id')
        if (self.cfg.get('source'), self.cfg.get('destination')) != (
                '/share/project/eai_pwm', '/share-mixed/project/eai_pwm'):
            raise ValueError('this entrypoint is limited to priorities 1-3 of eai_pwm')
        self._lock = open('/tmp/' + run_id + '-coordinator.lock', 'a')
        fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.db = None
        self._require_isolation()
        validate_mounts(self.cfg)
        for root in (self.cfg['source'], self.cfg['destination']):
            resolved = os.path.realpath(root)
            if os.path.commonpath((resolved, str(self.control))) == resolved:
                raise ValueError('new control must be outside both data trees')
        index = pathlib.Path('/tmp') / (run_id + '-index.sqlite')
        if any(pathlib.Path(str(index) + suffix).exists() for suffix in ('', '-wal', '-shm')):
            raise FileExistsError('fresh bootstrap refuses any existing index: ' + str(index))
        self.handover_path = pathlib.Path(handover).resolve()
        self.handover = read_json(self.handover_path)
        self._validate_handover(run_id, index)
        predecessors = _resume_controls(self.cfg['resume_from'])
        for ancestor in predecessors:
            old = read_json(ancestor / 'config.json')
            if old.get('run_id') == run_id:
                raise ValueError('new run_id must differ from every predecessor')
            if (old.get('source'), old.get('destination')) != (
                    self.cfg['source'], self.cfg['destination']):
                raise ValueError('receipt scope differs in predecessor ' + str(ancestor))
        started = self.handover['fresh_bootstrap_snapshot']['started']
        self.handover_marker = self.control / 'seed-handovers' / (self.handover['id'] + '.json')
        if (self.handover_marker.exists() or
                (self.handover_marker.parent / '.snapshots' / self.handover_marker.name).exists()):
            raise RuntimeError('seed handover already consumed; explicit new recovery required')
        # The coordinator's local flock is held before this one-time marker.
        # NFS O_EXCL/rename is deliberately not used as distributed exclusion.
        write_json(self.handover_marker, {
            'status': 'seeding', 'id': self.handover['id'], 'run_id': run_id,
            'handover': str(self.handover_path), 'started': now(),
            'old_producer': self.handover['old_producer']})
        self.state = {'status': 'preparing_resume', 'started': started, 'run_id': run_id,
                      'stage': None, 'phases': {}, 'workers_expected': 12,
                      'threads_per_node': int(self.cfg.get('copy_threads_per_node', 128)),
                      'resumed_from': str(predecessors[0]),
                      'migration_scope': 'eai_pwm_priorities_1_3',
                      'seed_handover': str(self.handover_marker)}
        write_json(self.control / 'state.json', dict(self.state, updated=now()))
        write_json(self.control / 'fresh-bootstrap.json', {
            'status': 'seeding', 'started': started, 'run_id': run_id,
            'resume_from': str(predecessors[0]), 'predecessor_controls': [str(p) for p in predecessors],
            'seed_handover': str(self.handover_marker)})
        def seed_progress(progress):
            if (self.control / 'STOP').exists():
                raise RuntimeError('STOP requested during resume-index recovery')
            # These are counters from actually processed immutable receipts or
            # indexed records. Previous run progress is never reused as proof.
            snapshot = dict(progress, run_id=run_id, started=started, updated=now(),
                            resume_from=str(predecessors[0]))
            self.state.update(status='preparing_resume', stage=None, updated=snapshot['updated'],
                              seed_progress=dict(progress))
            write_json(self.control / 'seed-progress.json', snapshot)
            write_json(self.control / 'state.json', self.state)

        try:
            summary = build_resume_index(predecessors[0], index, progress_callback=seed_progress)
        except BaseException:
            self.state.update(status='stopped' if (self.control / 'STOP').exists() else 'failed',
                              stage=None, updated=now(), error=traceback.format_exc())
            write_json(self.control / 'state.json', self.state)
            write_json(self.handover_marker, dict(self.handover, status=self.state['status'], updated=now(),
                                                 error=self.state['error']))
            raise
        write_json(self.control / 'resume-index.json', summary)
        write_json(self.control / 'seed-progress.json', dict(
            summary, status='complete', run_id=run_id, started=started, updated=now(),
            resume_from=str(predecessors[0])))
        self.db = open_index(self.cfg)
        self.state.update(status='starting', stage=None, pipeline=True, resume_index=summary,
                          seed_progress=dict(summary, status='complete'))
        self.phase_progress = {str(p): {'discovered_files': 0, 'discovered_bytes': 0,
                                       'verified_files': 0, 'verified_bytes': 0,
                                       'inventory_complete': False} for p in (1, 2, 3)}
        self.current_scan = {}
        self.hardlinks = {}
        self.jobs = {}
        self.children = {}
        self.children_lock = threading.Lock()
        self.hardlink_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.directory_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.pipeline = {'version': 2, 'started': started, 'stages': {}, 'directory_jobs': {}}
        self.pending_adoption = None
        self.enqueued_batches = self.discovered_files = self.discovered_bytes = 0
        self.assigned = set()
        self.next_task_id = 0
        self.scan_workers = max(1, int(self.cfg.get('parallel_scan_threads', 64)))
        self.scan_batch_files = max(1, int(self.cfg.get('scan_batch_files', 64)))
        self.last_scan_save = time.monotonic()
        self._copy_extension(predecessors[0], summary['controls'])
        self.save(status='starting')
        write_json(self.control / 'fresh-bootstrap.json', {
            'status': 'seeded', 'started': started, 'seeded': now(), 'run_id': run_id,
            'resume_from': str(predecessors[0]), 'summary': summary,
            'seed_handover': str(self.handover_marker)})

        write_json(self.handover_marker, {
            'status': 'seeded', 'id': self.handover['id'], 'run_id': run_id,
            'handover': str(self.handover_path), 'seeded': now(), 'summary': summary})

    def _validate_handover(self, run_id, index):
        proof = self.handover
        if (not isinstance(proof, dict) or proof.get('version') != 1 or
                proof.get('purpose') != 'resume_index_seed_only' or
                proof.get('control') != str(self.control) or proof.get('run_id') != run_id):
            raise ValueError('handover scope/version does not match seed-only recovery')
        identity = proof.get('id')
        if not isinstance(identity, str):
            raise ValueError('handover requires a UUID identity')
        try:
            parsed = uuid.UUID(identity)
        except ValueError as exc:
            raise ValueError('handover requires a UUID identity') from exc
        if identity not in (str(parsed), parsed.hex):
            raise ValueError('handover UUID must be canonical')
        if os.path.commonpath((str(self.control), str(self.handover_path))) != str(self.control):
            raise ValueError('handover must remain inside this control directory')
        producer = proof.get('old_producer')
        if (not isinstance(producer, dict) or type(producer.get('pid')) is not int or
                producer['pid'] <= 0 or not str(producer.get('starttime_ticks', '')).isdigit() or
                not isinstance(producer.get('argv'), list) or not producer['argv'] or
                any(not isinstance(arg, str) for arg in producer['argv'])):
            raise ValueError('complete old producer PID/starttime/argv is required')
        if process_matches(producer):
            raise RuntimeError('old producer identity is still alive')
        state = read_json(self.control / 'state.json')
        bootstrap = read_json(self.control / 'fresh-bootstrap.json')
        if state != proof.get('state_snapshot') or bootstrap != proof.get('fresh_bootstrap_snapshot'):
            raise RuntimeError('seed handover evidence differs from current persistent state')
        if (state.get('status') != 'preparing_resume' or state.get('stage') is not None or
                state.get('run_id') != run_id or bootstrap.get('status') != 'seeding' or
                bootstrap.get('run_id') != run_id or
                bootstrap.get('resume_from') != self.cfg['resume_from'] or
                not isinstance(bootstrap.get('started'), str)):
            raise RuntimeError('handover is limited to an unstarted seeding bootstrap')
        for name in ('pipeline.json', 'resume-index.json'):
            if ((self.control / name).exists() or
                    (self.control / '.snapshots' / name).exists()):
                raise RuntimeError('task pipeline or index has already been published')
        for suffix in ('', '-wal', '-shm'):
            candidate = pathlib.Path(str(index) + suffix)
            if candidate.exists() or candidate.is_symlink():
                raise FileExistsError('published index must not exist: ' + str(candidate))
        for directory in ('stages', 'manifests'):
            path = self.control / directory
            if path.is_symlink():
                raise RuntimeError('stage/manifest root must not be a symlink')
            if not path.exists():
                continue
            for parent, directories, files in os.walk(path, followlinks=False):
                if files or any((pathlib.Path(parent) / name).is_symlink() for name in directories):
                    raise RuntimeError('seed handover refuses all existing stage/manifest files')
        broker = pathlib.Path('/tmp') / (run_id + '-broker.sqlite')
        if not broker.is_file() or broker.is_symlink():
            raise RuntimeError('existing broker database is required')
        db = sqlite3.connect(broker.as_uri() + '?mode=ro', uri=True, timeout=30)
        try:
            if db.execute('SELECT COUNT(*) FROM claims').fetchone()[0] != 0:
                raise RuntimeError('seed handover refuses any historical broker claim')
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--handover', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    SeedHandoverPipeline(args.control, args.handover).run()


if __name__ == '__main__':
    main()
