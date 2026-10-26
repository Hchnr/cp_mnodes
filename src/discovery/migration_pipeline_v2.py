#!/usr/bin/env python3
"""Optional continuation: collect receipts in the background as well as links.

Requires the frozen migration_pipeline.py in the same immutable code bundle.
Only switch after the previous coordinator and its children have exited. A
closed latest scan can be adopted; partial scans are never renamed or rescanned.
"""
import argparse
import os
import pathlib
import subprocess
import sys
import time
import uuid

from migration_io import read_json, write_json
from migration_pipeline import (PipelineCoordinator as FirstPipeline, ORDER,
                                open_index, run_helper as first_run_helper)
from migration_run import Coordinator, now
from migration_worker import configure_rate_limit


PENDING = '_background_collect_pending'


class PipelineCoordinator(FirstPipeline):
    def __init__(self, control):
        super().__init__(control)
        self.pending_adoption = None
        self._restore_finished_results()
        stage = self.state.get('stage')
        if stage and stage not in self.pipeline['stages']:
            known = {'p%d-r%d' % pair: pair for pair in ORDER}
            manifest = self.control / 'manifests' / (stage + '.jsonl')
            if stage not in known or not manifest.is_file():
                raise RuntimeError('cannot adopt unknown or missing stage: ' + str(stage))
            phase, round_no = known[stage]
            stats = None
            with open(self.control / 'events.jsonl', encoding='utf-8') as handle:
                import json
                for line in handle:
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if (event.get('event') == 'scan_complete' and event.get('phase') == phase
                            and event.get('round') == round_no):
                        stats = event['stats']
            if stats is None and self.state.get('scan_complete'):
                stats = self.state.get('scan')
            if not isinstance(stats, dict):
                raise RuntimeError('partial scan requires recovery; refusing task-ID reuse: ' + stage)
            self._manifest_info(manifest, restore_links=True)
            self.pending_adoption = {
                'phase': phase, 'round': round_no, 'stage': stage,
                'manifest': str(manifest), 'scan': stats, 'summary': {PENDING: True},
                'adopted_closed_scan': now()}

    def _newest_result(self, kinds, stage):
        candidates = []
        root = self.control / 'pipeline-results'
        for kind in kinds:
            candidates.extend(root.glob(kind + '-' + stage + '-*.json'))
        for path in sorted(candidates, key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True):
            try:
                value = read_json(path)
            except (OSError, ValueError):
                continue
            if isinstance(value, dict):
                return value
        return None

    def _restore_finished_results(self):
        # Outputs are published only after DB work commits. A parent may have
        # died after publication but before harvesting the successful child.
        for stage, record in self.pipeline['stages'].items():
            if 'finalize' not in record:
                value = self._newest_result(('collect-hardlinks', 'hardlinks'), stage)
                if value is not None:
                    if 'summary' in value and 'finalize' in value:
                        record['summary'], record['finalize'] = value['summary'], value['finalize']
                    elif not record.get('summary', {}).get(PENDING):
                        record['finalize'] = value
            if stage not in self.pipeline['directory_jobs']:
                value = self._newest_result(('directories',), stage)
                if value is not None:
                    self.pipeline['directory_jobs'][stage] = value

    def collect(self, stage):
        # The stage is drained, but the next producer need not wait for its
        # thousands of immutable receipt reads and index updates.
        return {PENDING: True}

    def _queue_hardlinks(self, record):
        stage = record['stage']
        key = 'hardlinks:' + stage
        if key in self.jobs or 'finalize' in record:
            return
        dependencies = self._manifest_info(pathlib.Path(record['manifest']))
        mode = 'collect-hardlinks' if record.get('summary', {}).get(PENDING) else 'hardlinks'
        future = self.hardlink_pool.submit(self._child, mode, stage, record['manifest'])
        self.jobs[key] = {'future': future, 'kind': 'hardlinks', 'stage': stage,
                          'phase': record['phase'], 'dependencies': dependencies}

    def _child(self, kind, stage, manifest, prerequisite=None):
        if prerequisite is not None:
            prerequisite.result()
        self._check_stop()
        result = self.control / 'pipeline-results' / (kind + '-' + stage + '-' + uuid.uuid4().hex + '.json')
        result.parent.mkdir(exist_ok=True)
        args = [sys.executable, '-u', str(pathlib.Path(__file__).resolve()), kind,
                '--control', str(self.control), '--manifest', str(manifest),
                '--stage', stage, '--result', str(result)]
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

    def _harvest(self):
        for name, job in self.jobs.items():
            if job.get('harvested') or not job['future'].done():
                continue
            try:
                value = job['future'].result()
            except BaseException as exc:
                value = {'errors': 1, 'detail': str(exc), 'fatal': True}
            if job['kind'] == 'hardlinks':
                record = self.pipeline['stages'][job['stage']]
                if 'summary' in value and 'finalize' in value:
                    record['summary'], record['finalize'] = value['summary'], value['finalize']
                    self.event('round_complete', stage=job['stage'], summary=value['summary'], background=True)
                else:
                    record['finalize'] = value
                    if record.get('summary', {}).get(PENDING):
                        record['summary'] = {'error': 1, 'verified_files': 0, 'verified_bytes': 0}
                if record['round'] == max(r['round'] for r in self.pipeline['stages'].values()
                                          if r['phase'] == record['phase']):
                    counters = self.phase_progress[str(record['phase'])]
                    counters['verified_files'] = record['summary'].get('verified_files', 0) + record['finalize'].get('hardlinks', 0)
                    counters['verified_bytes'] = record['summary'].get('verified_bytes', 0) + record['finalize'].get('hardlink_bytes', 0)
            else:
                self.pipeline['directory_jobs'][job['stage']] = value
            job['harvested'] = True

    def run(self):
        if self.pending_adoption is not None:
            try:
                # Never change stage until original leases finish normally.
                for record in list(self.pipeline['stages'].values()):
                    self._queue_hardlinks(record)
                    self._queue_directories(record)
                record = self.pending_adoption
                self.save(status='copying', phase=record['phase'], round=record['round'],
                          stage=record['stage'], scan_complete=True, scan=record['scan'])
                self.drain(record['stage'])
                self.pipeline['stages'][record['stage']] = record
                self._queue_hardlinks(record)
                self._queue_directories(record)
                self.pending_adoption = None
                self.save(status='payload_round_complete')
            except BaseException:
                import traceback
                with self.children_lock:
                    for proc in self.children.values():
                        if proc.poll() is None:
                            proc.terminate()
                self.save(status='stopped' if (self.control / 'STOP').exists() else 'failed',
                          error=traceback.format_exc(), failed=now())
                self.hardlink_pool.shutdown(wait=True, cancel_futures=True)
                self.directory_pool.shutdown(wait=True, cancel_futures=True)
                self.db.close()
                raise
        return super().run()


def run_helper(args):
    if args.mode != 'collect-hardlinks':
        return first_run_helper(args)
    control = pathlib.Path(args.control)
    if (control / 'STOP').exists():
        raise RuntimeError('STOP requested')
    cfg = read_json(control / 'config.json')
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.control, coordinator.cfg = control, cfg
    coordinator.db = open_index(cfg)
    # Main coordinator owns the append-only event stream as well as state.
    coordinator.event = lambda *_args, **_kwargs: None
    configure_rate_limit(32 * 1024**2)
    try:
        summary = coordinator.collect(args.stage)
        result = coordinator.finalize(pathlib.Path(args.manifest), include_directories=False)
        write_json(args.result, {'summary': summary, 'finalize': result,
                                 'stage': args.stage, 'finished': now()})
    finally:
        coordinator.db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run', 'hardlinks', 'collect-hardlinks', 'directories'))
    parser.add_argument('--control', required=True)
    parser.add_argument('--manifest')
    parser.add_argument('--stage')
    parser.add_argument('--result')
    args = parser.parse_args()
    os.umask(0o077)
    if args.mode == 'run':
        PipelineCoordinator(args.control).run()
    else:
        if not args.manifest or not args.result or (args.mode == 'collect-hardlinks' and not args.stage):
            parser.error('helper requires --manifest/--result and collect helper needs --stage')
        run_helper(args)


if __name__ == '__main__':
    main()
