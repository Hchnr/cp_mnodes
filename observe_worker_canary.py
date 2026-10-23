#!/usr/bin/env python3
"""Bounded read-only per-lane throughput windows; never launch or stop services."""
import argparse
import datetime
import json
from pathlib import Path
import time

from migration_io import read_json, write_json


def snapshot(control):
    started = time.monotonic()
    state = read_json(control / 'state.json')
    nodes = {}
    for folder in ('nodes', 'nodes-extra'):
        for path in (control / folder).glob('*.json'):
            n = read_json(path)
            age = time.time() - datetime.datetime.fromisoformat(n['updated']).timestamp()
            if age < -5 or age > 15 or n.get('ended') or n.get('thread_error') or n.get('tuning_error'):
                raise RuntimeError('unhealthy copy heartbeat: ' + str(path))
            active = list(n.get('active', {}).values())
            nodes[folder + '/' + path.name] = {
                key: n.get(key) for key in ('pid', 'started', 'run_started', 'counts', 'bytes',
                'streamed_bytes', 'verified_files', 'verified_bytes', 'completed_batches',
                'broker_retries', 'stages')}
            nodes[folder + '/' + path.name].update(
                active=len(active), awaiting=sum(bool(x.get('awaiting_receipt')) for x in active))
    if len(nodes) != 24:
        raise RuntimeError('expected all twenty-four copy lanes')
    return {'epoch': time.time(), 'monotonic': time.monotonic(),
            'sample_seconds': time.monotonic() - started, 'nodes': nodes,
            'stage': state.get('stage'), 'status': state.get('status'),
            'gc_seconds': state.get('supply_metrics', {}).get('gc_seconds')}


def compare(a, b):
    seconds = b['monotonic'] - a['monotonic']
    if seconds <= 0 or (a['stage'], a['status']) != (b['stage'], b['status']):
        raise RuntimeError('invalid window or workload stage changed')
    rows = {}
    for key, n in b['nodes'].items():
        old = a['nodes'][key]
        if (n['pid'], n['started'], n['run_started']) != (old['pid'], old['started'], old['run_started']):
            raise RuntimeError('copy identity changed within window: ' + key)
        counts = {k: n['counts'].get(k, 0) - old['counts'].get(k, 0)
                  for k in n['counts'].keys() | old['counts'].keys()}
        values = {k: n[k] - old[k] for k in ('bytes', 'streamed_bytes', 'verified_files',
                  'verified_bytes', 'completed_batches', 'broker_retries')}
        if min([*counts.values(), *values.values()], default=0) < 0:
            raise RuntimeError('counter regressed within window: ' + key)
        files = counts.get('copied', 0)
        rows[key] = dict(values, counts=counts, files_per_second=files / seconds,
                         MiB_per_second=values['bytes'] / seconds / 1024**2,
                         mean_copied_bytes=values['bytes'] / files if files else None,
                         active=n['active'], awaiting=n['awaiting'])
    copied = sum(n['bytes'] for n in rows.values())
    files = sum(n['counts'].get('copied', 0) for n in rows.values())
    return {'seconds': seconds, 'GiB_per_second': copied / seconds / 1024**3,
            'files_per_second': files / seconds,
            'mean_copied_bytes': copied / files if files else None,
            'active': sum(n['active'] for n in rows.values()),
            'gc_seconds_delta': b['gc_seconds'] - a['gc_seconds']
                if b['gc_seconds'] is not None and a['gc_seconds'] is not None else None,
            'lanes': rows}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--control', required=True)
    p.add_argument('--seconds', type=float, default=20)
    p.add_argument('--windows', type=int, choices=(1, 2, 3), default=3)
    p.add_argument('--output', required=True)
    a = p.parse_args()
    if not 5 <= a.seconds <= 45:
        p.error('window must be within 5..45 seconds')
    control = Path(a.control).resolve()
    record = {'control': str(control), 'started': time.time(), 'windows': []}
    previous = snapshot(control)
    for _ in range(a.windows):
        time.sleep(a.seconds)
        current = snapshot(control)
        summary = compare(previous, current)
        record['windows'].append({'before': previous, 'after': current, 'summary': summary})
        write_json(a.output, record)
        print(json.dumps(summary), flush=True)
        previous = current


if __name__ == '__main__':
    main()
