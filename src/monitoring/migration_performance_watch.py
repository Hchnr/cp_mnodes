#!/usr/bin/env python3
"""Sample migration metadata and publish performance evidence, without actions.

The sole publisher uses a master-local flock. Shared mutable JSON always goes
through migration_io. This daemon never reads source payloads, traverses task
queues, changes copy settings or starts/stops another process.
"""

import argparse
import copy
import datetime
import fcntl
import json
import math
import os
import pathlib
import re
import time

from migration_io import read_json, write_json
from migration_nodes import load_nodes
from migration_performance import TERMINAL, epoch, evaluate_performance, number


HISTORY_SECONDS = 1800
MAX_HISTORY_POINTS = 362
RATE_STATUSES = {'reading_receipts', 'finalizing', 'enumerating', 'publishing', 'importing'}
SUMMARY_COUNTS = ('verified_files', 'verified_symlinks', 'verified_bytes', 'receipts')


def timestamp(now):
    return datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat()


def run_id_for(control):
    value = pathlib.Path(control).name.removeprefix('.')
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', value):
        raise ValueError('invalid control basename for performance lock')
    return value


def acquire_lock(control):
    """Keep this handle open for the publisher lifetime; never unlink it."""
    path = '/tmp/' + run_id_for(control) + '-perf.lock'
    handle = open(path, 'a')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        handle.close()
        raise
    return handle


def _bounded(points, now):
    return [copy.deepcopy(p) for p in points if isinstance(p, dict)
            and epoch(p.get('sample_time')) is not None
            and now - HISTORY_SECONDS <= epoch(p['sample_time']) <= now + 5][-MAX_HISTORY_POINTS:]


def _compact_result(result, now):
    for key in ('observations', 'seed_samples'):
        if isinstance(result.get('next_state'), dict):
            result['next_state'][key] = _bounded(result['next_state'].get(key, []), now)
    return result


class PerformanceWatch:
    def __init__(self, control, *, reader=read_json, writer=write_json,
                 node_loader=load_nodes, clock=time.time, stop_reader=None,
                 event_writer=None, expected_nodes=12, expected_lanes=2):
        self.control = pathlib.Path(control)
        self.run_id = run_id_for(control)
        self.reader, self.writer, self.node_loader = reader, writer, node_loader
        self.clock = clock
        self.stop_reader = stop_reader or (lambda: (self.control / 'STOP').exists())
        self.event_writer = event_writer or self._append_event
        self.expected_lanes = expected_lanes
        self.policy = {'expected_nodes': expected_nodes, 'history_seconds': HISTORY_SECONDS}
        # Losing a saved active incident through an unreadable publication is
        # worse than this observer failing visibly. Never replace it with {}.
        try:
            previous = reader(self.control / 'performance.json')
        except FileNotFoundError:
            logical = self.control / 'performance.json'
            if logical.exists() or (logical.parent / '.snapshots' / logical.name).exists():
                raise
            previous = {}
        if not isinstance(previous, dict):
            raise ValueError('invalid saved performance snapshot')
        if previous and previous.get('run_id') != self.run_id:
            raise ValueError('saved performance snapshot belongs to another run')
        self.previous = previous

    def _append_event(self, event):
        with open(self.control / 'performance-events.jsonl', 'a', encoding='utf-8') as handle:
            handle.write(json.dumps(event, ensure_ascii=True, allow_nan=False) + '\n')

    def _load(self, name, errors, *, optional=False):
        try:
            value = self.reader(self.control / name)
            if not isinstance(value, dict):
                raise ValueError('expected JSON object')
            return value
        except FileNotFoundError:
            if not optional:
                errors.append({'file': name, 'error': 'FileNotFoundError'})
        except (OSError, ValueError) as exc:
            errors.append({'file': name, 'error': type(exc).__name__ + ': ' + str(exc)})
        return {}

    def _completion(self, seed, now, errors):
        if seed.get('status') != 'complete' or seed.get('run_id') != self.run_id:
            return None

        def agrees(summary):
            if not isinstance(summary, dict):
                return False
            if not isinstance(summary.get('controls'), list) or not summary['controls']:
                return False
            if not all(isinstance(p, str) and p.startswith('/') for p in summary['controls']):
                return False
            db_path = summary.get('db_path')
            if not isinstance(db_path, str) or not pathlib.Path(db_path).name.startswith(self.run_id + '-'):
                return False
            for key in SUMMARY_COUNTS:
                value, expected = summary.get(key, 0), seed.get(key, 0)
                if not number(value) or value < 0 or not float(value).is_integer() or value != expected:
                    return False
            return 'verified_files' in summary and 'verified_bytes' in summary

        result = self._load('overlap-seed-result.json', errors, optional=True)
        summary, merged = result.get('summary'), result.get('merge')
        imported = (merged.get('imported_files', merged.get('rows_processed'))
                    if isinstance(merged, dict) else None)
        aliases_agree = (not isinstance(merged, dict) or 'imported_files' not in merged
                         or 'rows_processed' not in merged or merged['imported_files'] == merged['rows_processed'])
        finished = epoch(result.get('finished'))
        started = epoch(seed.get('started'))
        if (result.get('ok') is True and agrees(summary) and isinstance(merged, dict)
                and aliases_agree and number(imported)
                and imported == summary.get('verified_files', 0) + summary.get('verified_symlinks', 0)
                and finished is not None and finished <= now + 5
                and (started is None or finished >= started)):
            return {'kind': 'overlap_seed_result', 'file': 'overlap-seed-result.json',
                    'finished': result['finished'], 'counts': {k: summary.get(k, 0) for k in SUMMARY_COUNTS},
                    'imported_files': imported, 'rows_processed': imported}
        # resume-index is only published after the parent has accepted the
        # child result. Checking its actual summary is independent evidence;
        # a state flag or the absence of a live child is not evidence.
        summary = self._load('resume-index.json', errors, optional=True)
        if agrees(summary):
            return {'kind': 'published_resume_index', 'file': 'resume-index.json',
                    'counts': {k: summary.get(k, 0) for k in SUMMARY_COUNTS}}
        return None

    def _events(self, lanes, now):
        events = []
        for lane, result in lanes.items():
            before = self.previous.get('lanes', {}).get(lane) or {}
            base = {'version': 1, 'run_id': self.run_id, 'sample_time': now,
                    'updated': timestamp(now), 'lane': lane}
            if result.get('trigger') and not before.get('trigger'):
                events.append(dict(base, event='trigger', mode=result.get('mode'),
                                   incident=result.get('incident'), rates=result.get('rates')))
            elif before.get('trigger') and not result.get('trigger'):
                incident = result.get('incident', {})
                if incident.get('recovered_at') or result.get('completion_evidence'):
                    events.append(dict(base, event='recovered', mode=result.get('mode'),
                                       resolution=incident.get('resolution', 'sustained_recovery_rate'),
                                       incident=incident, completion_evidence=result.get('completion_evidence')))
            deficient = 'telemetry_insufficient' in result.get('diagnostics', [])
            was_deficient = 'telemetry_insufficient' in before.get('diagnostics', [])
            if deficient != was_deficient:
                events.append(dict(base, event='telemetry_insufficient', active=deficient,
                                   reason_codes=result.get('reason_codes', [])))
        return events

    def sample(self, wall_time=None):
        stopped = bool(self.stop_reader())
        errors, warnings = [], []
        state = self._load('state.json', errors)
        metrics = self._load('metrics.json', errors)
        seed_errors = []
        seed = self._load('seed-progress.json', seed_errors, optional=True)
        try:
            nodes = self.node_loader(self.control, warnings=warnings)
        except (OSError, ValueError) as exc:
            nodes = []
            errors.append({'file': 'nodes', 'error': type(exc).__name__ + ': ' + str(exc)})
        now = self.clock() if wall_time is None else wall_time
        if not number(now):
            raise ValueError('sample time must be finite')
        stopped = stopped or bool(self.stop_reader())
        if state.get('run_id') != self.run_id:
            errors.append({'file': 'state.json', 'error': 'run_id_mismatch'})
        state = dict(state, run_id=self.run_id)
        incomplete = any(not isinstance(n, dict) or not isinstance(n.get('components'), list)
                         or len(n['components']) != self.expected_lanes for n in nodes)
        if incomplete:
            warnings.append('node_components_incomplete')
        if errors or warnings:
            state['measurement_uncertain'] = True
        old_lanes = self.previous.get('lanes', {})
        copy_view = dict(state)
        if copy_view.get('status') == 'preparing_resume':
            copy_view.update(status='waiting_for_copy', stage=None)
        lanes = {'copy': evaluate_performance(copy_view, metrics, nodes,
                    previous=old_lanes.get('copy'), policy=self.policy, now=now, stop=stopped)}
        if seed or state.get('resume_index_building') or state.get('status') == 'preparing_resume' or old_lanes.get('seed'):
            seed_view = dict(state, status='preparing_resume', stage=None,
                             updated=seed.get('updated', seed.get('sample_time')))
            if seed_errors or (seed and seed.get('run_id') != self.run_id):
                seed_view['measurement_uncertain'] = True
            completion = self._completion(seed, now, seed_errors) if not stopped else None
            if completion is not None:
                before = copy.deepcopy(old_lanes.get('seed') or {})
                incident = copy.deepcopy(before.get('incident', {'active': False}))
                incident.update(active=False, resolution='verified_seed_completion')
                incident.setdefault('resolved_at', now)
                previous_state = copy.deepcopy(before.get('next_state', {}))
                previous_state.update(run_id=self.run_id, incident=incident)
                lanes['seed'] = {'version': 1, 'run_id': self.run_id, 'stage': None,
                                 'mode': 'seed_complete', 'evaluated_at': now,
                                 'armed': False, 'trigger': False, 'optimize': False,
                                 'diagnostic_trigger': False, 'diagnostics': [], 'reason_codes': [],
                                 'rates': {}, 'windows': {}, 'unit': None,
                                 'window': {'valid': False, 'reason': 'seed_verified_complete'},
                                 'below_threshold': None, 'recovery_target_met': None,
                                 'low_threshold': None, 'recovery_target': None,
                                 'incident': incident, 'next_state': previous_state,
                                 'completion_evidence': completion}
            else:
                result = evaluate_performance(seed_view, metrics, nodes, seed_progress=seed,
                    previous=old_lanes.get('seed'), policy=self.policy, now=now,
                    stop=stopped or state.get('status') in TERMINAL)
                if seed.get('status') == 'complete' and not stopped:
                    result['diagnostics'] = sorted(set(result['diagnostics'] + ['seed_completion_unproven']))
                    result['diagnostic_trigger'] = True
                lanes['seed'] = result
        lanes = {name: _compact_result(value, now) for name, value in lanes.items()}
        counts = [n.get('counts', {}) for n in nodes if isinstance(n, dict)]
        point = {'sample_time': now, 'run_id': self.run_id, 'stage': state.get('stage'),
                 'nodes_seen': len(nodes), 'stop': stopped, 'seed_status': seed.get('status'),
                 'metrics_sample_time': metrics.get('sample_time'),
                 'measurement_uncertain': bool(errors or warnings)}
        for field in ('nodebytes', 'streamed_bytes'):
            if number(metrics.get(field)):
                point[field] = metrics[field]
        for out, fields in (('copied_files', ('copied',)), ('revalidated_files', ('verified', 'skipped'))):
            if len(counts) == self.policy['expected_nodes'] and all(
                    isinstance(c, dict) and all(number(c.get(k, 0)) for k in fields) for c in counts):
                point[out] = sum(c.get(k, 0) for c in counts for k in fields)
        for field in ('processed_receipts', 'verified_files', 'verified_symlinks', 'rows_processed', 'imported_files'):
            if number(seed.get(field)):
                point['seed_' + field] = seed[field]
        terminal = stopped or state.get('status') in TERMINAL
        events = self._events(lanes, now)
        result = {'version': 1, 'run_id': self.run_id, 'control': str(self.control),
                  'pid': os.getpid(), 'updated': timestamp(now), 'sample_time': now,
                  'status': 'stopped' if stopped else 'terminal' if terminal else 'watching',
                  'terminal': terminal, 'migration_status': state.get('status'), 'stop': stopped,
                  'metadata_only': True, 'lanes': lanes, 'warnings': warnings,
                  'read_errors': errors + seed_errors, 'samples': _bounded(self.previous.get('samples', []) + [point], now),
                  'events': events, 'optimize': any(v.get('optimize') for v in lanes.values()),
                  'diagnostic_trigger': any(v.get('diagnostic_trigger') for v in lanes.values()),
                  'constraints': {'stop_copy_allowed': False, 'restart_copy_allowed': False,
                                  'deploy_allowed': False, 'evaluation_only': True}}
        # Fail rather than publish NaN/Infinity or silently lose an incident.
        json.dumps(result, allow_nan=False)
        self.writer(self.control / 'performance.json', result)
        self.previous = result
        for event in events:
            self.event_writer(event)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--interval', type=float, default=10)
    parser.add_argument('--expected-nodes', type=int, default=12)
    parser.add_argument('--expected-lanes', type=int, default=2)
    args = parser.parse_args()
    if not math.isfinite(args.interval) or not 5 <= args.interval <= 30:
        parser.error('--interval must be between 5 and 30 seconds')
    if args.expected_nodes < 1 or args.expected_lanes < 1:
        parser.error('expected node/lane counts must be positive')
    os.umask(0o077)
    with acquire_lock(args.control):
        watch = PerformanceWatch(args.control, expected_nodes=args.expected_nodes,
                                 expected_lanes=args.expected_lanes)
        while True:
            started = time.monotonic()
            result = watch.sample()
            if result['terminal']:
                return
            time.sleep(max(0.1, args.interval - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
