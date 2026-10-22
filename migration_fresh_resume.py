#!/usr/bin/env python3
"""Bootstrap a fresh run from predecessor receipts, then run all eight stages.

This entrypoint never adopts an old stage or handover and never deletes an
existing local index. The launcher must first isolate all predecessor writers.
Only new-control records and the new run's local SQLite index are initialized.
"""
import argparse
import concurrent.futures
import copy
import fcntl
import os
import pathlib
import re
import threading
import time
import traceback

from migration_io import read_json, write_json
from migration_parallel_pipeline import ParallelPipeline
from migration_pipeline import open_index
from migration_pipeline_v2 import PipelineCoordinator as AsyncPipeline
from migration_resume_index import build_resume_index, _resume_controls
from migration_run import now, validate_mounts


class FreshResumePipeline(ParallelPipeline):
    def __init__(self, control):
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
        for name in ('pipeline.json', 'fresh-bootstrap.json'):
            if ((self.control / name).exists() or
                    (self.control / '.snapshots' / name).exists()):
                raise RuntimeError('bootstrap already attempted; explicit recovery required: ' + name)
        for directory in ('stages', 'manifests'):
            path = self.control / directory
            if path.exists() and any(path.iterdir()):
                raise RuntimeError('fresh bootstrap refuses existing stage/manifest data')
        predecessors = _resume_controls(self.cfg['resume_from'])
        for ancestor in predecessors:
            old = read_json(ancestor / 'config.json')
            if old.get('run_id') == run_id:
                raise ValueError('new run_id must differ from every predecessor')
            if (old.get('source'), old.get('destination')) != (
                    self.cfg['source'], self.cfg['destination']):
                raise ValueError('receipt scope differs in predecessor ' + str(ancestor))
        started = now()
        self.state = {'status': 'preparing_resume', 'started': started, 'run_id': run_id,
                      'stage': None, 'phases': {}, 'workers_expected': 12,
                      'threads_per_node': int(self.cfg.get('copy_threads_per_node', 128)),
                      'resumed_from': str(predecessors[0]),
                      'migration_scope': 'eai_pwm_priorities_1_3'}
        write_json(self.control / 'state.json', dict(self.state, updated=now()))
        write_json(self.control / 'fresh-bootstrap.json', {
            'status': 'seeding', 'started': started, 'run_id': run_id,
            'resume_from': str(predecessors[0]), 'predecessor_controls': [str(p) for p in predecessors]})
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
            'resume_from': str(predecessors[0]), 'summary': summary})

    def _require_isolation(self):
        if (self.control / 'STOP').exists():
            raise RuntimeError('new STOP is present')
        previous = pathlib.Path(self.cfg['resume_from']).resolve()
        proof = self.cfg.get('predecessor_isolation', {})
        if (proof.get('confirmed') is not True or
                proof.get('old_control') != str(previous) or
                not isinstance(proof.get('method'), str) or not proof['method'].strip()):
            raise RuntimeError('launcher must confirm predecessor writer isolation')
        if not (previous / 'STOP').exists():
            raise RuntimeError('predecessor STOP must remain present')

    def _copy_extension(self, predecessor, ancestors):
        try:
            plan = copy.deepcopy(read_json(predecessor / 'extension-plan.json'))
        except FileNotFoundError:
            if self.cfg.get('require_priority4_plan', True):
                raise RuntimeError('priority4 extension plan is missing from predecessor')
            return
        if (plan.get('priority'), plan.get('source'), plan.get('destination'), plan.get('exclude')) != (
                4, '/share/project', '/share-mixed/project', ['eai_pwm']):
            raise ValueError('unexpected successor plan scope')
        if plan.get('status') != 'queued':
            raise RuntimeError('priority4 already started or requires explicit plan recovery')
        previous_dependency = plan.setdefault('depends_on', {}).get('control')
        plan['depends_on']['control'] = str(self.control)
        plan['dependency_predecessor'] = previous_dependency
        covered = list(dict.fromkeys([*ancestors, str(self.control)]))
        plan['covered_by'] = {'priorities': [1, 2, 3], 'controls': covered}
        plan['covered_by_controls'] = covered
        plan['updated'] = now()
        write_json(self.control / 'extension-plan.json', plan)
        self.state['extension_plan'] = str(self.control / 'extension-plan.json')

    def run(self):
        self._require_isolation()
        # Use V2's async collect/hardlink pipeline with our inherited parallel
        # scan(), not the partial-handover run() on the intermediate class.
        return AsyncPipeline.run(self)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run',))
    parser.add_argument('--control', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    FreshResumePipeline(args.control).run()


if __name__ == '__main__':
    main()
