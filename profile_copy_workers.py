#!/usr/bin/env python3
"""Read-only /proc profile of the current primary and supplemental workers.

The latest local active_migration JSON supplies JOB/control. Twelve nodes are
sampled concurrently through migration_launch.ssh. Each node reads its two
immutable worker snapshots only at the beginning and end; the sampling loop
reads /proc only. It never opens configuration, source data or queue payloads.
"""

import argparse
import collections
import concurrent.futures
import datetime
import json
import math
import pathlib
import time

import migration_launch as transport
from migration_io import read_json, write_json


BASE = pathlib.Path(__file__).absolute().parent
LANES = ('primary', 'extra64')
IO_KEYS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes',
           'cancelled_write_bytes')
COUNTER_KEYS = ('bytes', 'streamed_bytes', 'verified_bytes', 'verified_files',
                'completed_batches', 'broker_retries')


REMOTE = r'''
import sys
sys.dont_write_bytecode=True
import collections,datetime,json,os,pathlib,time
control=pathlib.Path(CONTROL)
sys.path[:0]=[str(control),'/share-mixed/project/.hcr-copy-monitor']
from migration_io import read_json
host=os.uname().nodename
hz=os.sysconf('SC_CLK_TCK')
folders={'primary':'nodes','extra64':'nodes-extra'}
io_keys=('rchar','wchar','syscr','syscw','read_bytes','write_bytes','cancelled_write_bytes')
counter_keys=('bytes','streamed_bytes','verified_bytes','verified_files','completed_batches','broker_retries')
state_path=str(control/'state.json')
state_snapshots=str(control/'.snapshots'/'state.json')

def iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def error(exc):
    return type(exc).__name__+': '+str(exc)

def node_snapshot(lane):
    # Exactly one immutable JSON read per lane per endpoint. Never in the loop.
    try:
        n=read_json(control/folders[lane]/(host+'.json'))
        observed=time.time()
        active=n.get('active',{})
        stamp=n.get('updated',n.get('started'))
        try:
            age=observed-datetime.datetime.fromisoformat(stamp.replace('Z','+00:00')).timestamp()
        except (AttributeError,TypeError,ValueError):
            age=None
        result={k:n.get(k) for k in ('pid','host','started','run_started','updated','ended',
                                    'threads','io_bytes_per_second','thread_error','tuning_error')}
        broker_error=n.get('broker_error')
        if isinstance(broker_error,dict):
            result['broker_error']={k:broker_error.get(k) for k in ('operation','at','detail')}
        if n.get('last_error'):
            result['last_error']=str(n['last_error'])[-4000:]
        result.update(observed_at=observed,heartbeat_age_seconds=age,
                      active=len(active),
                      awaiting_receipt=sum(bool(v.get('awaiting_receipt')) for v in active.values()),
                      counters={k:n.get(k) for k in counter_keys},
                      counts={k:v for k,v in n.get('counts',{}).items()
                              if isinstance(v,(int,float)) and not isinstance(v,bool)},
                      last_error_present=bool(n.get('last_error')))
        return result
    except (OSError,ValueError,KeyError,TypeError) as exc:
        return {'snapshot_error':error(exc),'observed_at':time.time()}

def proc_identity(pid,lane):
    base=pathlib.Path('/proc')/str(pid)
    fields=(base/'stat').read_text().rsplit(') ',1)[1].split()
    args=(base/'cmdline').read_bytes().decode(errors='surrogateescape').rstrip('\0').split('\0')
    scripts=[pathlib.Path(a).name for a in args if a.endswith('.py')]
    same_control=(any(args[i]=='--control' and args[i+1]==str(control) for i in range(len(args)-1))
                  or '--control='+str(control) in args)
    cached='migration_cached_worker.py' in scripts
    if cached:
        kind='primary' if lane=='primary' else 'supplement'
        same_lane=any(pathlib.Path(args[i]).name=='migration_cached_worker.py'
                      and i+1<len(args) and args[i+1]==kind for i in range(len(args)))
    elif lane=='primary':
        same_lane='migration_run.py' in scripts and 'worker' in args
    else:
        same_lane='migration_worker128_run.py' in scripts
    return {'pid':pid,'start_ticks':int(fields[19]),'script_names':scripts,
            'cached_launcher':cached,'control_matches':same_control,'lane_matches':same_lane,
            'identity_matches':same_control and same_lane and fields[0]!='Z'}

def proc_sample(identity):
    pid=identity['pid'];base=pathlib.Path('/proc')/str(pid)
    result={'pid':pid,'sample_time':time.time(),'monotonic':time.monotonic()}
    try:
        fields=(base/'stat').read_text().rsplit(') ',1)[1].split()
        if int(fields[19])!=identity['start_ticks']:
            raise RuntimeError('PID start_ticks changed; refusing mixed-process counters')
        io={k:int(v) for k,v in (line.split(':',1) for line in (base/'io').read_text().splitlines())}
        result.update(start_ticks=int(fields[19]),state=fields[0],
                      cpu_ticks=int(fields[11])+int(fields[12]),kernel_threads=int(fields[17]),
                      io={k:io.get(k,0) for k in io_keys})
    except (OSError,ValueError,IndexError,RuntimeError) as exc:
        result['proc_error']=error(exc)
        return result
    waits=collections.Counter();wait_errors=0
    try:
        with os.scandir(base/'task') as tids:
            for tid in tids:
                try:
                    wait=pathlib.Path(tid.path,'wchan').read_text().strip() or 'empty'
                    waits[wait]+=1
                except OSError:
                    wait_errors+=1
    except OSError as exc:
        result['wchan_error']=error(exc)
    result.update(wchan=dict(waits),wchan_read_errors=wait_errors)
    state_fds=[];fd_errors=0;fd_count=0
    try:
        with os.scandir(base/'fd') as entries:
            for entry in entries:
                fd_count+=1
                try:
                    target=os.readlink(entry.path)
                    clean=target.removesuffix(' (deleted)')
                    if clean==state_path or clean==state_snapshots or clean.startswith(state_snapshots+'/'):
                        state_fds.append({'fd':entry.name,'path':target})
                except OSError:
                    fd_errors+=1
    except OSError as exc:
        result['fd_error']=error(exc)
    result.update(fd_count=fd_count,state_fd_count=len(state_fds),
                  state_fds=state_fds,fd_read_errors=fd_errors)
    return result

out={'host':host,'hz':hz,'requested_seconds':DURATION,'interval_seconds':INTERVAL,
     'started_at':iso(),'snapshots_before':{},'identities':{},'samples':[]}
out['STOP_before']=(control/'STOP').exists()
for lane in folders:
    n=node_snapshot(lane);out['snapshots_before'][lane]=n
    try:
        identity=proc_identity(int(n['pid']),lane)
        if n.get('host')!=host:
            identity['identity_matches']=False
            identity['host_mismatch']=True
        if identity['identity_matches']:
            out['identities'][lane]=identity
        else:
            out['identities'][lane]=dict(identity,identity_error='worker identity mismatch')
    except (OSError,ValueError,TypeError,KeyError,IndexError) as exc:
        out['identities'][lane]={'identity_error':error(exc),'pid':n.get('pid')}

start=time.monotonic();deadline=start+DURATION;next_tick=start
while True:
    if next_tick>start:
        time.sleep(max(0,next_tick-time.monotonic()))
    sample={'sample_time':time.time(),'monotonic':time.monotonic(),'lanes':{}}
    for lane,identity in out['identities'].items():
        if identity.get('identity_matches'):
            sample['lanes'][lane]=proc_sample(identity)
        else:
            sample['lanes'][lane]={'proc_error':'worker identity not validated'}
    out['samples'].append(sample)
    if sample['monotonic']>=deadline:
        break
    # Skip missed ticks instead of creating a burst of /proc reads afterwards.
    next_tick=min(deadline,max(next_tick+INTERVAL,time.monotonic()))
out['snapshots_after']={lane:node_snapshot(lane) for lane in folders}
out['identities_after']={}
for lane,n in out['snapshots_after'].items():
    try:
        out['identities_after'][lane]=proc_identity(int(n['pid']),lane)
    except (OSError,ValueError,TypeError,KeyError,IndexError) as exc:
        out['identities_after'][lane]={'identity_error':error(exc),'pid':n.get('pid')}
out['STOP_after']=(control/'STOP').exists()
out['finished_at']=iso()
print(json.dumps(out,separators=(',',':')))
'''


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def counter_delta(before, after):
    result = {}
    for key in before.keys() | after.keys():
        a, b = before.get(key), after.get(key)
        result[key] = b - a if numeric(a) and numeric(b) and b >= a else None
    return result


def summarize_node(node):
    summaries = {}
    for lane in LANES:
        before = node['snapshots_before'].get(lane, {})
        after = node['snapshots_after'].get(lane, {})
        identity = node['identities'].get(lane, {})
        after_identity = node.get('identities_after', {}).get(lane, {})
        samples = [s['lanes'].get(lane, {}) for s in node['samples']]
        good = [s for s in samples if 'proc_error' not in s and 'io' in s]
        summary = {k: after.get(k) for k in
                   ('pid', 'threads', 'active', 'awaiting_receipt',
                    'heartbeat_age_seconds', 'updated', 'ended', 'thread_error',
                    'tuning_error', 'last_error_present', 'last_error', 'broker_error')}
        summary['acknowledged_io_budget_bytes_per_second'] = after.get('io_bytes_per_second')
        summary.update(sample_count=len(samples), proc_sample_errors=len(samples) - len(good),
                       identity=identity, identity_after=after_identity,
                       pid_before=before.get('pid'), pid_after=after.get('pid'))
        summary['snapshot_errors'] = [s['snapshot_error'] for s in (before, after)
                                      if s.get('snapshot_error')]
        summary['process_stable'] = bool(
            identity.get('identity_matches') and after_identity.get('identity_matches')
            and identity.get('start_ticks') == after_identity.get('start_ticks')
            and before.get('pid') == after.get('pid')
            and good and not summary['proc_sample_errors'])
        summary['counter_delta'] = counter_delta(before.get('counters', {}), after.get('counters', {}))
        summary['broker_retries_delta'] = summary['counter_delta'].get('broker_retries')
        summary['broker_error_before'] = before.get('broker_error')
        summary['counts_delta'] = counter_delta(before.get('counts', {}), after.get('counts', {}))
        ages = [n.get('heartbeat_age_seconds') for n in (before, after)]
        summary['max_endpoint_heartbeat_age_seconds'] = max((a for a in ages if numeric(a)), default=None)
        if good:
            peak = max(good, key=lambda s: s.get('state_fd_count', 0))
            waits = collections.Counter()
            for sample in good:
                waits.update(sample.get('wchan', {}))
            summary.update(
                state_fd_peak_observed=peak.get('state_fd_count', 0),
                state_fds_at_peak=peak.get('state_fds', []),
                state_fd_mean_observed=sum(s.get('state_fd_count', 0) for s in good) / len(good),
                wchan_last=good[-1].get('wchan', {}),
                wchan_mean_threads={k: v / len(good) for k, v in waits.items()},
                kernel_threads_min=min(s['kernel_threads'] for s in good),
                kernel_threads_max=max(s['kernel_threads'] for s in good),
                kernel_threads_end=good[-1]['kernel_threads'])
        if len(good) >= 2 and summary['process_stable']:
            first, last = good[0], good[-1]
            elapsed = last['monotonic'] - first['monotonic']
            deltas = counter_delta(first['io'], last['io'])
            if elapsed > 0 and last['cpu_ticks'] >= first['cpu_ticks'] and all(v is not None for v in deltas.values()):
                summary.update(elapsed_seconds=elapsed,
                               cpu_cores=(last['cpu_ticks'] - first['cpu_ticks']) / node['hz'] / elapsed,
                               io_delta=deltas,
                               io_bytes_per_second={k: v / elapsed for k, v in deltas.items()})
                summary['logical_io_bytes_per_second'] = (deltas['rchar'] + deltas['wchar']) / elapsed
            else:
                summary['rate_error'] = 'invalid elapsed time or decreasing process counters'
        else:
            summary['rate_error'] = 'worker changed, identity failed, or /proc samples unavailable'
        summaries[lane] = summary

    endpoint = [node['snapshots_after'].get(lane, {}) for lane in LANES]
    totals = {key: sum(n.get(key, 0) or 0 for n in endpoint if numeric(n.get(key)))
              for key in ('threads', 'active', 'awaiting_receipt')}
    totals['acknowledged_io_budget_bytes_per_second'] = sum(
        n['io_bytes_per_second'] for n in endpoint if numeric(n.get('io_bytes_per_second')))
    totals['state_fd_peak_observed'] = max((sum(s['lanes'].get(lane, {}).get('state_fd_count', 0)
                                                for lane in LANES) for s in node['samples']), default=0)
    totals['healthy_endpoints'] = all(not n.get('snapshot_error') and not n.get('thread_error')
                                       and not n.get('ended') and not n.get('tuning_error')
                                       for n in endpoint)
    totals['cpu_cores'] = sum(s.get('cpu_cores', 0) for s in summaries.values())
    totals['logical_io_bytes_per_second'] = sum(s.get('logical_io_bytes_per_second', 0)
                                                for s in summaries.values())
    return {'lanes': summaries, 'total': totals}


def aggregate(nodes, requested_nodes):
    result = {'requested_physical_nodes': len(requested_nodes), 'sampled_physical_nodes': len(nodes),
              'requested_lanes': 2 * len(requested_nodes), 'lanes': {}}
    for lane in LANES:
        values = [n['summary']['lanes'][lane] for n in nodes]
        good = [s for s in values if 'rate_error' not in s]
        endpoint_keys = ('threads', 'active', 'awaiting_receipt')
        result['lanes'][lane] = {
            'sampled_processes': len(values), 'valid_rate_processes': len(good),
            **{k: sum(s.get(k, 0) or 0 for s in values if numeric(s.get(k))) for k in endpoint_keys},
            'acknowledged_io_budget_bytes_per_second': sum(s['acknowledged_io_budget_bytes_per_second']
                for s in values if numeric(s.get('acknowledged_io_budget_bytes_per_second'))),
            'cpu_cores': sum(s['cpu_cores'] for s in good),
            'logical_io_bytes_per_second': sum(s['logical_io_bytes_per_second'] for s in good),
            'io_bytes_per_second': {k: sum(s['io_bytes_per_second'][k] for s in good) for k in IO_KEYS},
            'counter_delta': {k: sum(s['counter_delta'].get(k, 0) or 0 for s in values
                                     if s['process_stable'] and numeric(s['counter_delta'].get(k))) for k in COUNTER_KEYS},
            'counts_delta': dict(sum((collections.Counter({k: v for k, v in s['counts_delta'].items()
                                                          if numeric(v)}) for s in values if s['process_stable']),
                                     collections.Counter())),
            'state_fd_peak_sum': sum(s.get('state_fd_peak_observed', 0) for s in values),
            'max_process_state_fd_peak': max((s.get('state_fd_peak_observed', 0) for s in values), default=0),
            'max_endpoint_heartbeat_age_seconds': max((s['max_endpoint_heartbeat_age_seconds'] for s in values
                                                       if numeric(s.get('max_endpoint_heartbeat_age_seconds'))), default=None),
            'wchan_mean_threads': dict(sum((collections.Counter(s.get('wchan_mean_threads', {})) for s in values),
                                           collections.Counter())),
        }
    values = list(result['lanes'].values())
    result['total'] = {
        **{k: sum(s[k] for s in values) for k in
           ('threads', 'active', 'awaiting_receipt', 'acknowledged_io_budget_bytes_per_second',
            'cpu_cores', 'logical_io_bytes_per_second', 'valid_rate_processes')},
        'io_bytes_per_second': {k: sum(s['io_bytes_per_second'][k] for s in values) for k in IO_KEYS},
        'counter_delta': {k: sum(s['counter_delta'][k] for s in values) for k in COUNTER_KEYS},
        'counts_delta': dict(sum((collections.Counter(s['counts_delta']) for s in values), collections.Counter())),
        'state_fd_peak_sum': sum(n['summary']['total']['state_fd_peak_observed'] for n in nodes),
        'max_node_state_fd_peak': max((n['summary']['total']['state_fd_peak_observed'] for n in nodes), default=0),
        'unhealthy_physical_nodes': sum(not n['summary']['total']['healthy_endpoints'] for n in nodes),
        'wchan_mean_threads': dict(sum((collections.Counter(s['wchan_mean_threads']) for s in values), collections.Counter())),
    }
    result['rate_complete'] = result['total']['valid_rate_processes'] == result['requested_lanes']
    return result


def collect(node, control, duration, interval, owner=None):
    prefix = 'CONTROL=' + repr(control) + '\nDURATION=' + repr(duration) + '\nINTERVAL=' + repr(interval) + '\n'
    _, output = transport.ssh(node, prefix + REMOTE, owner=owner)
    result = json.loads(output)
    result['node'] = node
    result['summary'] = summarize_node(result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--active', type=pathlib.Path, default=BASE / 'active_migration.json',
                        help='local active_migration logical JSON; latest immutable version is read')
    parser.add_argument('--control', help='optional control override; JOB still comes from latest active JSON')
    parser.add_argument('--duration', type=float, default=30.0,
                        help='sampling seconds, up to 60 within migration_launch SSH timeout (default: 30)')
    parser.add_argument('--interval', type=float, default=0.5,
                        help='local /proc sample interval in seconds (default: 0.5)')
    parser.add_argument('--nodes', nargs='+', choices=transport.HOSTS, default=list(transport.HOSTS))
    parser.add_argument('--output', type=pathlib.Path, default=BASE / 'state_cache_post_profile.json')
    args = parser.parse_args(argv)
    if (not math.isfinite(args.duration) or not 0 < args.duration <= 60
            or not math.isfinite(args.interval) or not 0 < args.interval <= args.duration):
        parser.error('require 0 < interval <= duration <= 60, all finite')
    if len(set(args.nodes)) != len(args.nodes):
        parser.error('nodes must be unique')
    active = read_json(args.active)
    transport.JOB = active['job']
    control = args.control or active['control']
    report = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'job': transport.JOB, 'control': control, 'duration_requested_seconds': args.duration,
              'interval_seconds': args.interval, 'nodes': [], 'errors': [],
              'notes': [
                  'Only start/end worker snapshots are read: four immutable JSON reads per physical node.',
                  'STOP is checked directly at both endpoints; it is never cached. Remote bytecode writes are disabled.',
                  'The interval loop reads /proc only; worker identity comes from current lane snapshots and /proc start_ticks.',
                  'State FD peaks are sampled observations. Summed node peaks are not a simultaneous cluster peak.',
                  'rchar+wchar measures process logical I/O, including source reads, destination writes/readback and metadata JSON.',
                  'Process logical I/O and kernel read_bytes/write_bytes do not measure NFS wire or backend bandwidth.',
                  'Payload counter deltas are endpoint snapshots; completion bytes may be bursty for large files.',
                  'Node windows start independently; rates are summed from each process own measured elapsed time.',
              ]}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(collect, node, control, args.duration, args.interval,
                               active.get('ssh_owner')): node for node in args.nodes}
        for future in concurrent.futures.as_completed(futures):
            try:
                report['nodes'].append(future.result())
            except Exception as exc:
                report['errors'].append({'node': futures[future], 'error': type(exc).__name__ + ': ' + str(exc)})
    report['nodes'].sort(key=lambda n: args.nodes.index(n['node']))
    report['aggregate'] = aggregate(report['nodes'], args.nodes)
    report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    write_json(args.output, report)
    print(json.dumps({'output': str(args.output), 'aggregate': report['aggregate'], 'errors': report['errors']},
                     ensure_ascii=False))
    return 1 if report['errors'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
