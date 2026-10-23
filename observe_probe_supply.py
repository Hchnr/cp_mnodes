#!/usr/bin/env python3
"""Read-only, bounded before/after supply measurement on the actual master."""

import argparse
import collections
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import statistics
import time
import urllib.request

from migration_io import read_json, write_json


def snapshot(control, cfg, producer_pid):
    started = time.monotonic()
    state = read_json(control / 'state.json')
    nodes = {}
    sizes = []
    for folder in ('nodes', 'nodes-extra'):
        for path in (control / folder).glob('*.json'):
            n = read_json(path)
            age = time.time() - datetime.datetime.fromisoformat(n['updated']).timestamp()
            if age < -5 or age > 15 or n.get('ended') or n.get('thread_error') or n.get('tuning_error'):
                raise RuntimeError('unhealthy or stale copy heartbeat: ' + str(path))
            active = n.get('active', {})
            sizes.extend(x.get('size', 0) for x in active.values() if not x.get('awaiting_receipt'))
            nodes[folder + '/' + path.name] = {
                'pid': n['pid'], 'started': n['started'], 'counts': n.get('counts', {}),
                'bytes': n['bytes'], 'streamed_bytes': n['streamed_bytes'],
                'retries': n.get('broker_retries', 0), 'batches': n['completed_batches'],
                'active': len(active), 'awaiting': sum(bool(x.get('awaiting_receipt')) for x in active.values())}
    if len(nodes) != 24:
        raise RuntimeError('expected all twenty-four copy processes')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def health(endpoint):
        request = urllib.request.Request(endpoint + '/health',
                         headers={'Authorization': 'Bearer ' + cfg['broker_token']})
        with opener.open(request, timeout=10) as response:
            value = json.load(response)
        return {key: value.get(key) for key in
                ('pid', 'starttime_ticks', 'inflight', 'counts', 'small_probe_delegate', 'safe_open_backend')}

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        health_rows = dict(zip(cfg['target_probe_endpoints'], pool.map(health, cfg['target_probe_endpoints'])))
    pids = [producer_pid, *state.get('supply_metrics', {}).get('metadata_child_pids', [])]
    procs = {}
    for pid in pids:
        proc = Path('/proc') / str(pid)
        fields = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()
        procs[str(pid)] = {'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
                          'starttime_ticks': fields[19], 'wchan': (proc / 'wchan').read_text()}
    return {'epoch': time.time(), 'monotonic': time.monotonic(),
            'sample_seconds': time.monotonic() - started, 'nodes': nodes, 'probes': health_rows,
            'state': {k: state.get(k) for k in ('stage', 'status', 'updated', 'target_filter', 'task_publication')},
            'procs': procs, 'active_size_median': statistics.median(sizes) if sizes else None}


def compare(a, b):
    seconds = b['monotonic'] - a['monotonic']
    if seconds <= 0 or (a['state']['stage'], a['state']['status']) != (b['state']['stage'], b['state']['status']):
        raise ValueError('invalid interval or workload stage changed')
    totals = collections.Counter()
    stable = True
    for name, after in b['nodes'].items():
        before = a['nodes'][name]
        stable &= (before['pid'], before['started']) == (after['pid'], after['started'])
        for key in ('bytes', 'streamed_bytes', 'retries', 'batches'):
            delta = after[key] - before[key]
            if delta < 0:
                raise ValueError('copy counter reset')
            totals[key] += delta
        for key in before['counts'].keys() | after['counts'].keys():
            delta = after['counts'].get(key, 0) - before['counts'].get(key, 0)
            if delta < 0:
                raise ValueError('copy result counter reset')
            totals['count_' + key] += delta
    if not stable:
        raise RuntimeError('copy process identities changed during measurement')
    probes = []
    for endpoint, after in b['probes'].items():
        before = a['probes'][endpoint]
        if (before['pid'], before['starttime_ticks']) != (after['pid'], after['starttime_ticks']):
            raise RuntimeError('probe identity changed within measurement window')
        counts = {key: value - before['counts'].get(key, 0) for key, value in after['counts'].items()}
        if any(value < 0 for value in counts.values()):
            raise ValueError('probe counter reset')
        probes.append({'endpoint': endpoint, 'delta': counts, 'policy': after['small_probe_delegate']})
    tf = {key: value - a['state']['target_filter'].get(key, 0)
          for key, value in b['state']['target_filter'].items()}
    return {'seconds': seconds, 'GiB_per_second': totals['bytes'] / seconds / 1024**3,
            'files_per_second': totals['count_copied'] / seconds,
            'mean_copied_bytes': totals['bytes'] / totals['count_copied'] if totals['count_copied'] else None,
            'active': sum(n['active'] for n in b['nodes'].values()),
            'awaiting_receipt': sum(n['awaiting'] for n in b['nodes'].values()),
            'protected_copy_identities_stable': stable, 'totals_delta': dict(totals),
            'target_filter_delta': tf, 'probes': probes,
            'cpu_cores': {pid: (value['cpu_seconds'] - a['procs'][pid]['cpu_seconds']) / seconds
                          for pid, value in b['procs'].items()
                          if value['starttime_ticks'] == a['procs'][pid]['starttime_ticks']}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--producer-pid', type=int, required=True)
    parser.add_argument('--seconds', type=float, default=25)
    parser.add_argument('--output')
    args = parser.parse_args()
    if not 5 <= args.seconds <= 45:
        parser.error('measurement must be within 5..45 seconds')
    control = Path(args.control).resolve()
    cfg = read_json(control / 'config.json')
    a = snapshot(control, cfg, args.producer_pid)
    time.sleep(args.seconds)
    b = snapshot(control, cfg, args.producer_pid)
    result = {'control': str(control), 'before': a, 'after': b, 'summary': compare(a, b)}
    if args.output:
        write_json(args.output, result)
    print(json.dumps(result['summary']), flush=True)


if __name__ == '__main__':
    main()
