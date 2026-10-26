#!/usr/bin/env python3
"""Small-JSON progress producer. Never reads source data, queues or receipts.

Publishes metrics.json every five seconds, preserving fifteen minutes of samples
across restarts. Inventory aggregation is limited to once per thirty seconds.
All shared JSON reads/publications use immutable migration_io snapshots.
"""

import argparse
import collections
import datetime
import hashlib
import os
import pathlib
import threading
import time

from migration_io import read_json, write_json
from migration_nodes import load_nodes


WINDOW_SECONDS = 15 * 60


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def number(value, default=0):
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        return value
    return default


def load(path, warnings=None):
    try:
        value = read_json(path)
        if not isinstance(value, dict):
            raise ValueError('expected JSON object')
        return value
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        if warnings is not None:
            warnings.append(str(path) + ': ' + str(exc))
        return {}


def useful_progress(state, progress, nodes, previous=None):
    """Merge only the current stage into its phase; never sum repeated rounds."""
    previous = previous or {}
    phases = {str(k): dict(v) for k, v in progress.get('phases', {}).items()
              if isinstance(v, dict)}
    for phase in ('1', '2', '3'):
        phases.setdefault(phase, {})
    stage = state.get('stage', progress.get('stage'))
    phase = str(state.get('phase', progress.get('phase')))
    published = progress.get('current_stage')
    live = dict(published) if isinstance(published, dict) and published else {}
    if not live and stage:
        for node in nodes:
            stages = node.get('stages', {})
            current = stages.get(stage, {}) if isinstance(stages, dict) else {}
            if isinstance(current, dict):
                for key in ('verified_files', 'verified_bytes'):
                    live[key] = live.get(key, 0) + number(current.get(key))
    fields = ('verified_files', 'verified_bytes')
    original_sums = {key: sum(number(p.get(key)) for p in phases.values()) for key in fields}
    for key in fields:
        if phase in phases and key in live:
            phases[phase][key] = max(number(phases[phase].get(key)), number(live[key]))
    # Published useful progress is monotonic across stage changes and restarts;
    # node cumulative verification counters deliberately never enter this sum.
    old_phases = previous.get('phases', {})
    for name, counters in phases.items():
        old = old_phases.get(name, {}) if isinstance(old_phases, dict) else {}
        for key in fields:
            counters[key] = max(number(counters.get(key)), number(old.get(key)))
    totals = {}
    supplied = progress.get('totals', {})
    if not isinstance(supplied, dict):
        supplied = {}
    for key in fields:
        corrected_sum = sum(number(p.get(key)) for p in phases.values())
        base = number(supplied.get(key), original_sums[key])
        totals[key] = max(number(previous.get(key)), base + max(0, corrected_sum - original_sums[key]))
    return phases, totals


class Metrics:
    def __init__(self, control, start_inventory=True):
        self.control = pathlib.Path(control)
        self.previous = load(self.control / 'metrics.json')
        saved = self.previous.get('samples', self.previous.get('history', []))
        cutoff = time.time() - WINDOW_SECONDS
        self.samples = collections.deque(s for s in saved if isinstance(s, dict)
                                         and number(s.get('sample_time')) >= cutoff)
        self.inventory = load(self.control / 'inventory.json')
        self.inventory_last = float('-inf')
        stamp = self.inventory.get('updated_at', self.inventory.get('updated'))
        if stamp:
            try:
                published = datetime.datetime.fromisoformat(str(stamp).replace('Z', '+00:00')).timestamp()
                self.inventory_last = time.monotonic() - max(0, time.time() - published)
            except (ValueError, TypeError, OverflowError):
                pass
        self.inventory_fingerprint = None
        self.inventory_cache = {}
        self.inventory_lock = threading.Lock()
        self.inventory_stop = threading.Event()
        self.inventory_warnings = []
        self.inventory_busy = False
        self.inventory_thread = None
        if start_inventory:
            self.inventory_thread = threading.Thread(target=self.inventory_loop,
                                                      name='inventory-summary', daemon=True)
            self.inventory_thread.start()

    def inventory_loop(self):
        while not self.inventory_stop.is_set():
            warnings = []
            with self.inventory_lock:
                self.inventory_busy = True
            try:
                self.inventory_update(warnings, time.monotonic())
            except Exception as exc:
                if not self.inventory_stop.is_set():
                    warnings.append('inventory aggregation: ' + str(exc))
            finally:
                with self.inventory_lock:
                    self.inventory_warnings = warnings[-10:]
                    self.inventory_busy = False
            self.inventory_stop.wait(5)

    def close(self, timeout=1):
        self.inventory_stop.set()
        if self.inventory_thread is not None:
            # No sampling path joins this thread; shutdown wait is bounded even
            # if an underlying filesystem operation itself cannot be cancelled.
            self.inventory_thread.join(timeout=timeout)

    def inventory_update(self, warnings, tick):
        root = self.control / 'inventory'
        if tick - self.inventory_last < 30:
            return
        self.inventory_last = tick
        plan = load(root / 'plan.json', warnings)
        if not plan:
            return
        fingerprint = []
        for index in range(int(plan.get('nodes', 12))):
            node = load(root / 'nodes' / (str(index) + '.json'), warnings)
            current = node.get('current_unit') or {}
            stats = current.get('stats', {}) if isinstance(current, dict) else {}
            fingerprint.append((index, node.get('completed_units'), node.get('complete'),
                                current.get('id') if isinstance(current, dict) else None,
                                stats.get('files'), stats.get('bytes'), stats.get('errors')))
        fingerprint = tuple(fingerprint)
        if self.inventory_fingerprint == fingerprint:
            return
        try:
            from migration_inventory import aggregate_inventory
            aggregate = aggregate_inventory(self.control, cache=self.inventory_cache,
                                              cancel_event=self.inventory_stop)
            # Unit details already have immutable files. Keep the frequently
            # read inventory summary small for monitoring and metrics clients.
            summary = {key: value for key, value in aggregate.items() if key != 'units'}
            for phase in summary.get('phases', {}).values():
                phase['discovered_files'] = number(phase.get('files')) + number(phase.get('symlinks'))
                phase['discovered_bytes'] = number(phase.get('bytes'))
                phase['inventory_complete'] = bool(phase.get('complete') and phase.get('exact'))
            totals = summary.setdefault('totals', {})
            totals['discovered_files'] = number(totals.get('files')) + number(totals.get('symlinks'))
            totals['discovered_bytes'] = number(totals.get('bytes'))
            summary['inventory_complete'] = bool(summary.get('complete') and summary.get('exact'))
            summary['updated_at'] = summary.get('updated', utc())
            if self.inventory_stop.is_set():
                return
            write_json(self.control / 'inventory.json', summary)
            with self.inventory_lock:
                self.inventory = summary
            self.inventory_fingerprint = fingerprint
        except (OSError, ValueError, ImportError, KeyError) as exc:
            warnings.append('inventory aggregation: ' + str(exc))

    def sample(self, wall_time=None, monotonic=None):
        wall_time = time.time() if wall_time is None else wall_time
        tick = time.monotonic() if monotonic is None else monotonic
        warnings = []
        config = load(self.control / 'config.json', warnings)
        state = load(self.control / 'state.json', warnings)
        progress = load(self.control / 'progress.json', warnings)
        nodes = load_nodes(self.control, warnings)
        phases, useful = useful_progress(state, progress, nodes, self.previous)
        nodebytes = max(number(self.previous.get('nodebytes')), sum(number(n.get('bytes')) for n in nodes))
        streamed = [number(n.get('streamed_bytes'), None) for n in nodes]
        streamed_bytes = sum(streamed) if streamed and all(v is not None for v in streamed) else None
        if streamed_bytes is not None:
            streamed_bytes = max(streamed_bytes, number(self.previous.get('streamed_bytes')))
        point = {'sample_time': wall_time, 'nodebytes': nodebytes,
                 'copied_bytes': nodebytes, 'streamed_bytes': streamed_bytes, **useful}
        while self.samples and number(self.samples[0].get('sample_time')) < wall_time - WINDOW_SECONDS:
            self.samples.popleft()
        if not self.samples or wall_time > number(self.samples[-1].get('sample_time')):
            self.samples.append(point)
        stopped = (self.control / 'STOP').exists()
        if stopped:
            self.inventory_stop.set()
        with self.inventory_lock:
            inventory = self.inventory
            warnings.extend(self.inventory_warnings)
            inventory_busy = self.inventory_busy
        stamp = utc()
        result = {'version': 1, 'pid': os.getpid(), 'control': str(self.control),
                  'updated_at': stamp, 'updated': stamp, **point, 'phases': phases,
                  'current_stage': state.get('stage'), 'current_phase': state.get('phase'),
                  'migration_status': state.get('status'), 'status': 'stopped' if stopped else 'running',
                  'nodes_seen': len(nodes), 'nodes_expected': config.get('nodes', 12),
                  'inventory_complete': inventory.get('inventory_complete', False),
                  'inventory_updated_at': inventory.get('updated_at', inventory.get('updated')),
                  'inventory_aggregation_running': inventory_busy,
                  'samples': list(self.samples), 'history': list(self.samples), 'warnings': warnings}
        write_json(self.control / 'metrics.json', result)
        self.previous = result
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    if args.interval < 5:
        parser.error('--interval must be >= 5 seconds')
    # The daemon runs on one coordinator node; this is a local lock, not NFS.
    import fcntl
    ident = hashlib.sha256(os.path.abspath(args.control).encode()).hexdigest()[:20]
    lock = open('/tmp/hcr-metrics-' + ident + '.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    metrics = Metrics(args.control)
    try:
        while True:
            start = time.monotonic()
            result = metrics.sample()
            if result['status'] == 'stopped':
                return
            time.sleep(max(0.1, args.interval - (time.monotonic() - start)))
    finally:
        metrics.close()


if __name__ == '__main__':
    main()
