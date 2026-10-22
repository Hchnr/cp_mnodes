#!/usr/bin/env python3
"""Continue the drained p1-r1 run without serial metadata barriers.

The existing coordinator must be stopped first. This module preserves its
local index, broker, workers and immutable manifests. Only this process writes
state.stage; background children never publish coordinator state.
"""
import argparse
import concurrent.futures
import fcntl
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid

from migration_io import read_json, write_json
from migration_run import Coordinator, GOOD, now, queued_count, validate_mounts
from migration_worker import configure_rate_limit


ORDER = ((1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (3, 2), (1, 3), (2, 3))
LAST_ROUND = {1: 3, 2: 3, 3: 2}


def open_index(cfg):
    path = pathlib.Path('/tmp') / (cfg['run_id'] + '-index.sqlite')
    if not path.is_file():
        raise FileNotFoundError('existing coordinator index required: ' + str(path))
    db = sqlite3.connect(str(path), timeout=120)
    db.execute('PRAGMA busy_timeout=120000')
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('SELECT rel FROM files LIMIT 1').fetchone()
    return db


def canonical_phase(rel):
    if rel == 'home/hcr' or rel.startswith('home/hcr/'):
        return 1
    if rel == 'repos/cosmos-framework-pwm' or rel.startswith('repos/cosmos-framework-pwm/'):
        return 2
    return 3


class PipelineCoordinator(Coordinator):
    def __init__(self, control):
        # Do not invoke Coordinator.__init__: resume_from would delete our DB.
        self.control = pathlib.Path(control).resolve()
        self.cfg = read_json(self.control / 'config.json')
        self._lock = open('/tmp/' + self.cfg['run_id'] + '-coordinator.lock', 'a')
        fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.state = read_json(self.control / 'state.json')
        if (self.control / 'STOP').exists():
            raise RuntimeError('STOP is present; pipeline cannot resume')
        self.db = open_index(self.cfg)
        progress = read_json(self.control / 'progress.json')
        self.phase_progress = progress.get('phases', {})
        for phase in (1, 2, 3):
            self.phase_progress.setdefault(str(phase), {
                'discovered_files': 0, 'discovered_bytes': 0,
                'verified_files': 0, 'verified_bytes': 0, 'inventory_complete': False})
        self.current_scan = progress.get('current_scan', {})
        self.hardlinks = {}
        self.jobs = {}
        self.children = {}
        self.children_lock = threading.Lock()
        self.hardlink_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.directory_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            self.pipeline = read_json(self.control / 'pipeline.json')
        except FileNotFoundError:
            self.pipeline = {'version': 1, 'started': now(), 'stages': {}, 'directory_jobs': {}}
            self._adopt_p1()
        self.state['pipeline'] = True
        self.state.setdefault('phases', {})
        for stage, record in self.pipeline['stages'].items():
            self._manifest_info(pathlib.Path(record['manifest']), restore_links=True)

    def _check_stop(self):
        if (self.control / 'STOP').exists():
            raise RuntimeError('STOP requested')

    def _manifest_info(self, manifest, restore_links=False):
        dependencies = set()
        with open(manifest, encoding='utf-8') as handle:
            for line in handle:
                entry = json.loads(line)
                if entry.get('kind') != 'file':
                    continue
                canonical = entry.get('hardlink_to')
                if canonical:
                    dependencies.add(canonical_phase(canonical))
                if restore_links and entry.get('nlink', 1) > 1:
                    self.hardlinks.setdefault((entry['dev'], entry['ino']), canonical or entry['rel'])
        return dependencies

    def _adopt_p1(self):
        if self.state.get('stage') != 'p1-r1':
            raise RuntimeError('first handover requires the drained p1-r1 stage')
        if any(queued_count(self.control / 'stages' / 'p1-r1')):
            raise RuntimeError('p1-r1 still has pending or running payload tasks')
        summary = scan = None
        with open(self.control / 'events.jsonl', encoding='utf-8') as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get('event') == 'round_complete' and event.get('stage') == 'p1-r1':
                    summary = event['summary']
                if (event.get('event') == 'scan_complete' and event.get('phase') == 1
                        and event.get('round') == 1):
                    scan = event['stats']
        if summary is None:
            raise RuntimeError('committed p1-r1 round_complete event required')
        scan = scan or self.state.get('scan')
        if not isinstance(scan, dict):
            raise RuntimeError('p1-r1 scan statistics unavailable')
        manifest = self.control / 'manifests' / 'p1-r1.jsonl'
        if not manifest.is_file():
            raise FileNotFoundError(str(manifest))
        self.pipeline['stages']['p1-r1'] = {
            'phase': 1, 'round': 1, 'stage': 'p1-r1', 'manifest': str(manifest),
            'scan': scan, 'summary': summary, 'adopted': now()}

    def _record_rounds(self):
        for phase in (1, 2, 3):
            records = [r for r in self.pipeline['stages'].values() if r['phase'] == phase]
            if records:
                self.state['phases'][str(phase)] = {
                    'rounds': sorted(records, key=lambda r: r['round'])}

    def _harvest(self):
        for name, job in self.jobs.items():
            if job.get('harvested') or not job['future'].done():
                continue
            try:
                result = job['future'].result()
            except BaseException as exc:
                result = {'errors': 1, 'detail': str(exc), 'fatal': True}
            if job['kind'] == 'hardlinks':
                record = self.pipeline['stages'][job['stage']]
                record['finalize'] = result
                counters = self.phase_progress[str(record['phase'])]
                if record['round'] == max(r['round'] for r in self.pipeline['stages'].values()
                                          if r['phase'] == record['phase']):
                    counters['verified_files'] = record['summary'].get('verified_files', 0) + result.get('hardlinks', 0)
                    counters['verified_bytes'] = record['summary'].get('verified_bytes', 0) + result.get('hardlink_bytes', 0)
            else:
                self.pipeline['directory_jobs'][job['stage']] = result
            job['harvested'] = True

    def save(self, **updates):
        self._harvest()
        self._record_rounds()
        self.state['background_metadata'] = [
            {'job': name, 'kind': job['kind'], 'stage': job['stage'],
             'status': 'done' if job.get('harvested') else 'running' if job['future'].running() else 'queued'}
            for name, job in self.jobs.items()]
        super().save(**updates)
        write_json(self.control / 'pipeline.json', self.pipeline)

    def _child(self, kind, stage, manifest, prerequisite=None):
        if prerequisite is not None:
            prerequisite.result()
        self._check_stop()
        result = self.control / 'pipeline-results' / (kind + '-' + stage + '-' + uuid.uuid4().hex + '.json')
        result.parent.mkdir(exist_ok=True)
        args = [sys.executable, '-u', str(pathlib.Path(__file__).resolve()), kind,
                '--control', str(self.control), '--manifest', str(manifest), '--result', str(result)]
        name = kind + ':' + stage
        with open(self.control / (kind + '-' + stage + '.log'), 'ab', buffering=0) as output:
            proc = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, close_fds=True)
            with self.children_lock:
                self.children[name] = proc
            try:
                status = proc.wait()
            finally:
                with self.children_lock:
                    self.children.pop(name, None)
        if status:
            raise RuntimeError(name + ' exited ' + str(status) + '; inspect its log')
        return read_json(result)

    def _queue_hardlinks(self, record):
        stage = record['stage']
        key = 'hardlinks:' + stage
        if key in self.jobs or 'finalize' in record:
            return
        dependencies = self._manifest_info(pathlib.Path(record['manifest']))
        future = self.hardlink_pool.submit(self._child, 'hardlinks', stage, record['manifest'])
        self.jobs[key] = {'future': future, 'kind': 'hardlinks', 'stage': stage,
                          'phase': record['phase'], 'dependencies': dependencies}

    def _queue_directories(self, record):
        phase, stage = record['phase'], record['stage']
        if record['round'] != LAST_ROUND[phase] or stage in self.pipeline['directory_jobs']:
            return
        key = 'directories:' + stage
        if key in self.jobs:
            return
        prerequisite = self.jobs.get('hardlinks:' + stage, {}).get('future')
        future = self.directory_pool.submit(self._child, 'directories', stage,
                                             record['manifest'], prerequisite)
        self.jobs[key] = {'future': future, 'kind': 'directories', 'stage': stage, 'phase': phase}

    def _wait(self, jobs, status='waiting_hardlinks'):
        while any(not job['future'].done() for job in jobs):
            self._check_stop()
            self.save(status=status)
            time.sleep(2)
        self._harvest()

    def _wait_before_catchup(self, phase):
        # Cross-phase hardlinks also depend on their canonical phase's inode.
        self._wait([j for j in self.jobs.values() if j['kind'] == 'hardlinks'
                    and (j['phase'] == phase or phase in j['dependencies'])])

    @staticmethod
    def _errors(value):
        errors = value.get('errors', 0)
        return len(errors) if isinstance(errors, list) else int(errors or 0)

    def run(self):
        validate_mounts(self.cfg)
        configure_rate_limit(32 * 1024**2)
        try:
            for record in list(self.pipeline['stages'].values()):
                self._queue_hardlinks(record)
                self._queue_directories(record)
            for phase, round_no in ORDER:
                self._check_stop()
                stage = 'p' + str(phase) + '-r' + str(round_no)
                if stage in self.pipeline['stages']:
                    continue
                if round_no > 1:
                    self._wait_before_catchup(phase)
                manifest = self.control / 'manifests' / (stage + '.jsonl')
                if manifest.exists():
                    raise RuntimeError('unfinished existing stage needs explicit recovery: ' + stage)
                stage, manifest, scan = self.scan(phase, round_no)
                self.drain(stage)
                summary = self.collect(stage)
                record = {'phase': phase, 'round': round_no, 'stage': stage,
                          'manifest': str(manifest), 'scan': scan, 'summary': summary}
                self.pipeline['stages'][stage] = record
                self._queue_hardlinks(record)
                self._queue_directories(record)
                self.save(status='payload_round_complete')
            self._wait(list(self.jobs.values()), status='finalizing')
            self._check_stop()
            from migration_metadata import finalize_shared_ancestors
            manifests = [self.pipeline['stages']['p' + str(p) + '-r' + str(LAST_ROUND[p])]['manifest']
                         for p in (1, 2, 3)]
            shared = finalize_shared_ancestors(self.control, manifests)
            self.pipeline['shared_ancestors'] = shared
            unresolved = self._errors(shared)
            for phase in (1, 2, 3):
                latest = self.pipeline['stages']['p' + str(phase) + '-r' + str(LAST_ROUND[phase])]
                summary = latest['summary']
                unresolved += sum(v for k, v in summary.items()
                                  if k not in GOOD and k not in ('bytes', 'metadata_warnings', 'verified_files', 'verified_bytes'))
                unresolved += summary.get('metadata_warnings', 0)
                unresolved += self._errors(latest.get('finalize', {}))
                unresolved += self._errors(latest['scan'])
                unresolved += self._errors(self.pipeline['directory_jobs'].get(latest['stage'], {}))
            self.save(status='complete_with_changes' if unresolved else 'complete',
                      unresolved=unresolved, finished=now(), stage=None)
            self.event('finished', pipeline=True, unresolved=unresolved, status=self.state['status'])
        except BaseException:
            with self.children_lock:
                for proc in self.children.values():
                    if proc.poll() is None:
                        proc.terminate()
            self.save(status='stopped' if (self.control / 'STOP').exists() else 'failed',
                      error=traceback.format_exc(), failed=now())
            raise
        finally:
            self.hardlink_pool.shutdown(wait=True, cancel_futures=True)
            self.directory_pool.shutdown(wait=True, cancel_futures=True)
            self.db.close()


def run_helper(args):
    control = pathlib.Path(args.control)
    cfg = read_json(control / 'config.json')
    if (control / 'STOP').exists():
        raise RuntimeError('STOP requested')
    if args.mode == 'hardlinks':
        coordinator = Coordinator.__new__(Coordinator)
        coordinator.control, coordinator.cfg = control, cfg
        coordinator.db = open_index(cfg)
        configure_rate_limit(32 * 1024**2)
        try:
            result = coordinator.finalize(pathlib.Path(args.manifest), include_directories=False)
        finally:
            coordinator.db.close()
    else:
        from migration_metadata import finalize_directories
        result = finalize_directories(control, args.manifest, threads=32, defer_roots=True)
    write_json(args.result, result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run', 'hardlinks', 'directories'))
    parser.add_argument('--control', required=True)
    parser.add_argument('--manifest')
    parser.add_argument('--result')
    args = parser.parse_args()
    os.umask(0o077)
    if args.mode == 'run':
        PipelineCoordinator(args.control).run()
    else:
        if not args.manifest or not args.result:
            parser.error('helper requires --manifest and --result')
        run_helper(args)


if __name__ == '__main__':
    main()
