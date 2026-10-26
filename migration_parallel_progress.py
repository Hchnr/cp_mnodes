#!/usr/bin/env python3
"""Read-only combined P1-3/P4 monitor, with separate counters and aligned rates.

The original active pointer and all copy daemons stay unchanged. Only a verified
independent P4 control explicitly registered in the predecessor plan is merged.
Historical evidence within P1-3 is never added twice. No queue/manifests read.
"""
import argparse
import collections
import copy
import json
import sys
import time
from pathlib import Path

import migration_progress as base
from migration_io import read_json
from migration_priority4_worker import ScopeGuard


def at(samples, timestamp, field):
    for index, sample in enumerate(samples):
        if sample['time'] < timestamp:
            continue
        value = base.number(sample.get(field))
        if sample['time'] == timestamp:
            return value
        if index == 0:
            return None
        previous = samples[index - 1]
        first = base.number(previous.get(field))
        if first is None or value is None or value < first:
            return None
        fraction = (timestamp - previous['time']) / (sample['time'] - previous['time'])
        return first + fraction * (value - first)
    return None


def aligned_rates(rates, tick):
    result = {}
    streams = [list(rate.samples) for rate in rates]
    for label, window in [('1m', 60), ('5m', 300)]:
        value = dict(seconds=0, copied_bytes_per_second=None, streamed_bytes_per_second=None)
        if streams and all(stream for stream in streams):
            start = max(tick - window, *(stream[0]['time'] for stream in streams))
            value['seconds'] = max(0, tick - start)
            for field in ('copied_bytes', 'streamed_bytes'):
                pairs = [(at(stream, start, field), at(stream, tick, field)) for stream in streams]
                if tick > start and all(a is not None and b is not None and b >= a for a, b in pairs):
                    value[field + '_per_second'] = sum(b - a for a, b in pairs) / (tick - start)
        result[label] = value
    return result


def p4_work(view):
    nodes = view.get('nodes', [])
    counts = collections.Counter()
    for node in nodes:
        counts.update(node.get('counts', {}))
    scan = view.get('current_scan', {})
    complete = view.get('status') in ('complete', 'complete_with_changes')
    verified = view.get('overall', {}) if complete else view.get('current_stage', {})
    pending = {}
    skew = False
    for field in ('files', 'bytes'):
        discovered = base.number(view.get('overall', {}).get('discovered_' + field) if complete else scan.get(field))
        done = base.number(verified.get('verified_' + field))
        pending[field] = discovered - done if discovered is not None and done is not None and discovered >= done else None
        skew |= discovered is not None and done is not None and discovered < done
    return dict(copied_files=counts.get('copied', 0) if nodes else None,
                copied_bytes=view.get('new_copied_bytes'),
                existing_checks=counts.get('skipped', 0) if nodes else None,
                existing_checks_lower_bound=False, pending_files=pending['files'], pending_bytes=pending['bytes'],
                snapshot_skew=skew)


class CombinedMonitor:
    def __init__(self):
        self.rates = {}

    def snapshot(self, control=None):
        first = base.snapshot(control)
        plan = first.get('extension_plan', {})
        parallel = plan.get('parallel_execution', {}) if isinstance(plan, dict) else {}
        if parallel.get('mode') != 'independent_concurrent_p4':
            self.rates.setdefault(first.get('control'), base.Rate()).add(first)
            return first
        try:
            target = Path(parallel['control'])
            cfg = read_json(target / 'config.json')
            ScopeGuard(str(target), cfg)
            proof = read_json(target / 'isolation.json')
            if (cfg.get('parallel_with_control') != first['control'] or
                    proof.get('predecessor_control') != first['control'] or
                    proof.get('verified') is not True or proof.get('control') != str(target) or
                    proof.get('run_id') != cfg.get('run_id')):
                raise ValueError('parallel scope binding differs')
            second = base.snapshot(target)
        except (OSError, ValueError, KeyError) as exc:
            first.setdefault('warnings', []).append('P4 并行监控不可用：' + str(exc))
            self.rates.setdefault(first.get('control'), base.Rate()).add(first)
            return first
        tick = time.time()
        parts = [first, second]
        trackers = []
        for part in parts:
            tracker = self.rates.setdefault(part['control'], base.Rate())
            tracker.add(part, monotonic=tick)
            trackers.append(tracker)
        result = copy.deepcopy(first)
        result['parallel_scopes'] = parts
        terminal = ('complete', 'complete_with_changes')
        result['status'] = (('complete_with_changes' if any(p.get('status') == 'complete_with_changes' for p in parts) else 'complete')
                            if all(p.get('status') in terminal for p in parts) else 'parallel_copy')
        result['extension_plan'] = None
        result['phases']['4'] = second.get('phases', {}).get('4', {})
        overall_parts = [first.get('current_run_overall', first['overall']), second['overall']]
        for field in ('discovered_files', 'discovered_bytes', 'verified_files', 'verified_bytes'):
            values = [base.number(part.get(field)) for part in overall_parts]
            result['overall'][field] = sum(values) if all(v is not None for v in values) else None
        result['overall']['inventory_complete'] = all(p.get('inventory_complete') is True for p in overall_parts)
        total, done = result['overall']['discovered_bytes'], result['overall']['verified_bytes']
        result['overall']['percent'] = (100 * done / total if result['overall']['inventory_complete']
                                        and total and done is not None and done <= total else None)
        result['rates'] = aligned_rates(trackers, tick)
        ages = [base.number(part.get('metrics_sample_age_seconds')) for part in parts]
        result['metrics_sample_age_seconds'] = max(ages) if all(a is not None for a in ages) else None
        result['eta_metrics_stale'] = any(part.get('eta_metrics_stale') for part in parts)
        result['updated'] = max((part.get('updated') for part in parts if part.get('updated')),
                                key=lambda v: base.sample_epoch(v) or 0, default=None)
        by_host = {}
        for part in parts:
            for node in part['nodes']:
                old = by_host.get(node['host'])
                if old is None:
                    by_host[node['host']] = copy.deepcopy(node)
                    continue
                for field in ('threads', 'active_threads', 'bytes', 'streamed_bytes', 'verified_files', 'verified_bytes'):
                    values = [base.number(old.get(field)), base.number(node.get(field))]
                    old[field] = sum(values) if all(v is not None for v in values) else None
                old['counts'] = dict(collections.Counter(old.get('counts', {})) + collections.Counter(node.get('counts', {})))
                old['heartbeat_age_seconds'] = max(old.get('heartbeat_age_seconds') or 0,
                                                   node.get('heartbeat_age_seconds') or 0)
                old['components'] = old.get('components', []) + node.get('components', [])
                old['last_error'] = '\n'.join(str(v) for v in (old.get('last_error'), node.get('last_error')) if v)
                old['ended'] = old.get('ended') or node.get('ended')
        result['nodes'] = list(by_host.values())
        works = [base.copy_work_progress(first), p4_work(second)]
        work = copy.deepcopy(works[0])
        for field in ('copied_files', 'copied_bytes', 'existing_checks', 'pending_files', 'pending_bytes'):
            values = [base.number(w.get(field)) for w in works]
            work[field] = sum(values) if all(v is not None for v in values) else None
        work['snapshot_skew'] = any(w.get('snapshot_skew') for w in works)
        work['existing_checks_lower_bound'] = any(w.get('existing_checks_lower_bound') for w in works)
        if work.get('historical_baseline_present'):
            for field in ('copied_files', 'copied_bytes'):
                a, b = base.number(works[0].get('cumulative_' + field)), base.number(works[1].get(field))
                work['cumulative_' + field] = a + b if a is not None and b is not None else None
        result['parallel_work_progress'] = work
        result['warnings'] = []
        for name, part in zip(('P1–3', 'P4'), parts):
            result['warnings'].extend(name + '：' + str(w) for w in part.get('warnings', []))
            if part.get('producer_error') and part.get('status') == 'producer_paused':
                result['warnings'].append(name + ' 生产端暂停：' + str(part['producer_error']).strip().splitlines()[-1])
            if (base.age(part.get('updated'), tick) or 0) > 60:
                result['warnings'].append(name + ' 状态更新超过 60 秒')
            if len(part['nodes']) != 12:
                result['warnings'].append(name + ' 可见节点心跳为 ' + str(len(part['nodes'])) + '/12')
            delayed = [n for n in part['nodes'] if (base.number(n.get('heartbeat_age_seconds')) or 0) > 30]
            if delayed:
                result['warnings'].append(name + ' 有 ' + str(len(delayed)) +
                                          ' 个节点统计延迟，速度仅反映已发布计数，活跃数也可能滞后')
        return result


def render(view, details=False):
    if not view.get('parallel_scopes'):
        return base.render(view, details=details)
    if details:
        return '\n\n'.join(base.render(part, details=True) for part in view['parallel_scopes'])
    # The base renderer takes its work summary from copy_work_progress. Patch
    # only this single-threaded display call, and restore even on formatting
    # errors. Neither the scanner nor a worker imports this monitor entrypoint.
    original = base.copy_work_progress
    try:
        base.copy_work_progress = lambda value: value['parallel_work_progress'] if value is view else original(value)
        text = base.render(view)
    finally:
        base.copy_work_progress = original
    lanes = []
    for name, part in zip(('P1–3 查漏', 'P4 搬运'), view['parallel_scopes']):
        active = sum(base.number(node.get('active_threads')) or 0 for node in part['nodes'])
        threads = sum(base.number(node.get('threads')) or 0 for node in part['nodes'])
        lanes.append(name + ' ' + base.count(active) + '/' + base.count(threads))
    text = text.replace('\n\n【整体进度】', '\n线程分工：' + '；'.join(lanes) + '（大表合并两套独立进程）\n\n【整体进度】')
    for node in view['nodes']:
        if not node.get('ended'):
            label = '-'.join(str(node.get('host', '-')).split('-')[-2:])
            text = text.replace(label + '：进程已结束或心跳延迟', label + '：心跳延迟，需另行核对实际进程')
    return text.replace('状态：parallel_copy', '状态：1–3 查漏与 P4 并行复制').replace(
        '范围：本次已探明部分，扫描未结束。',
        '范围：1–3 与 P4 已探明部分合并，精确排除重复 eai_pwm；全量扫描未结束。')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control')
    parser.add_argument('--watch', nargs='?', type=float, const=5)
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--details', action='store_true')
    args = parser.parse_args(argv)
    if args.watch is not None and (args.watch <= 0 or args.json):
        parser.error('watch must be positive and cannot be combined with --json')
    monitor = CombinedMonitor()
    try:
        while True:
            view = monitor.snapshot(args.control)
            if args.watch and sys.stdout.isatty():
                print('\033[2J\033[H', end='')
            print(json.dumps(view, ensure_ascii=False, indent=2) if args.json else render(view, args.details), flush=True)
            if args.watch is None:
                return 0 if view.get('control') else 1
            time.sleep(args.watch)
    except (KeyboardInterrupt, BrokenPipeError):
        return 0


if __name__ == '__main__':
    main()
