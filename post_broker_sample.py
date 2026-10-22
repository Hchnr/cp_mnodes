#!/usr/bin/env python3
"""Read-only, distributed observation of the live migration workers."""
import argparse
import concurrent.futures
import datetime as dt
import json
import pathlib
import shlex
import subprocess
import time

import migration_launch as transport

JOB = '0a326937-3eb4-4da4-8b70-3a50586e1976'
CONTROL = '/share-mixed/project/.hcr-copy-20261021T064717Z-resume-e25450'
NODES = ['master-0'] + ['worker-' + str(i) for i in range(11)]

REMOTE = r'''
import collections,datetime,json,os,pathlib,sys,time
sys.path[:0]=['/share-mixed/project/.hcr-copy-monitor','/share-mixed/project/.hcr-copy-20261021T054248Z-resume-0c6401']
from migration_io import read_json
control=pathlib.Path(CONTROL); host=os.uname().nodename
hz=os.sysconf('SC_CLK_TCK'); lanes={}; errors=[]
for lane,folder in [('primary','nodes'),('extra64','nodes-extra')]:
    n=read_json(control/folder/(host+'.json'))
    lanes[lane]={'pid':int(n['pid']),'folder':folder}

def proc(pid):
    base='/proc/'+str(pid)
    stat=open(base+'/stat').read().rsplit(')',1)[1].split()
    io={k:int(v) for k,v in (line.split(':',1) for line in open(base+'/io'))}
    return {'cpu_ticks':int(stat[11])+int(stat[12]),'start_ticks':int(stat[19]),
            'state':stat[0],'kernel_threads':int(stat[17]),
            'io':{k:io[k] for k in ('rchar','wchar','read_bytes','write_bytes')}}

def snap():
    out={'sample_time':time.time(),'monotonic':time.monotonic(),'lanes':{}}
    for lane,info in lanes.items():
        item={'pid':info['pid']}
        try: item.update(proc(info['pid']))
        except (OSError,ValueError) as exc:
            item['proc_error']=type(exc).__name__+': '+str(exc)
        try:
            n=read_json(control/info['folder']/(host+'.json'))
            active=n.get('active',{})
            stamp=datetime.datetime.fromisoformat(n['updated'].replace('Z','+00:00')).timestamp()
            item.update(snapshot_pid=n.get('pid'),threads=n.get('threads'),
                        updated=n['updated'],heartbeat_age=time.time()-stamp,
                        active=len(active),
                        awaiting_receipt=sum(bool(v.get('awaiting_receipt')) for v in active.values()),
                        unhealthy=bool(n.get('thread_error') or n.get('last_error')),
                        streamed_bytes=n.get('streamed_bytes'),copied_bytes=n.get('bytes'))
        except (OSError,ValueError,KeyError,TypeError) as exc:
            item['snapshot_error']=type(exc).__name__+': '+str(exc)
        out['lanes'][lane]=item
    return out

out={'host':host,'hz':hz,'duration_requested':DURATION,'interval':INTERVAL,
     'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'samples':[]}
if host.endswith('master-0'):
    try:
        p=proc(BROKER_PID)
        cmd=open('/proc/'+str(BROKER_PID)+'/cmdline','rb').read().split(b'\0')
        p['script_names']=[pathlib.Path(x.decode(errors='replace')).name for x in cmd
                           if x.endswith(b'.py')]
        out['broker']={'pid':BROKER_PID,**p}
    except (OSError,ValueError) as exc:out['broker']={'pid':BROKER_PID,'error':str(exc)}
    state=read_json(control/'state.json')
    out['state_before']={k:state.get(k) for k in ('status','phase','stage','updated','parallel_scan_threads','error')}
    out['STOP_before']=(control/'STOP').exists()
    drains=read_json(control/'drain-hosts.json')
    out['draining_hosts_before']=drains.get('draining_hosts')
start=time.monotonic(); out['samples'].append(snap())
deadline=start+DURATION; next_tick=start+INTERVAL
while True:
    target=min(next_tick,deadline)
    time.sleep(max(0,target-time.monotonic()))
    out['samples'].append(snap())
    if target>=deadline:break
    next_tick+=INTERVAL
out['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
if host.endswith('master-0'):
    state=read_json(control/'state.json'); health=read_json(control/'training-health.json')
    out['state_after']={k:state.get(k) for k in ('status','phase','stage','updated','parallel_scan_threads','error')}
    out['STOP_after']=(control/'STOP').exists()
    out['training_after']={k:health.get(k) for k in ('state','updated','reason','log_mtime')}
print(json.dumps(out,separators=(',',':')))
'''


def summarize(node):
    first, last = node['samples'][0], node['samples'][-1]
    elapsed = last['monotonic'] - first['monotonic']
    result = {'elapsed_seconds': elapsed, 'lanes': {}}
    for lane in ('primary', 'extra64'):
        a, b = first['lanes'][lane], last['lanes'][lane]
        if a.get('start_ticks') != b.get('start_ticks') or 'io' not in a or 'io' not in b:
            result['lanes'][lane] = {'error': 'process changed or counters unavailable', 'first': a, 'last': b}
            continue
        item = {key: b.get(key) for key in ('pid', 'threads', 'kernel_threads', 'start_ticks', 'active', 'awaiting_receipt', 'heartbeat_age', 'unhealthy')}
        item['cpu_cores'] = (b['cpu_ticks'] - a['cpu_ticks']) / node['hz'] / elapsed
        item['io_bytes_per_second'] = {k: (b['io'][k] - a['io'][k]) / elapsed for k in a['io']}
        samples = [s['lanes'][lane] for s in node['samples']]
        item['active_mean'] = sum(s.get('active', 0) for s in samples) / len(samples)
        item['awaiting_receipt_mean'] = sum(s.get('awaiting_receipt', 0) for s in samples) / len(samples)
        item['max_heartbeat_age'] = max(s.get('heartbeat_age', 0) for s in samples)
        item['snapshot_errors'] = sum('snapshot_error' in s for s in samples)
        result['lanes'][lane] = item
    return result


def aggregate(nodes):
    out = {'sampled_nodes': len(nodes), 'lanes': {}}
    for lane in ('primary', 'extra64'):
        good = [n['summary']['lanes'][lane] for n in nodes if 'error' not in n['summary']['lanes'][lane]]
        out['lanes'][lane] = {
            'sampled_nodes': len(good),
            'cpu_cores': sum(n['cpu_cores'] for n in good),
            'io_bytes_per_second': {k: sum(n['io_bytes_per_second'][k] for n in good) for k in ('rchar', 'wchar', 'read_bytes', 'write_bytes')},
            **{k: sum(n.get(k, 0) or 0 for n in good) for k in ('threads', 'active', 'awaiting_receipt', 'active_mean', 'awaiting_receipt_mean')},
            'max_heartbeat_age': max((n['max_heartbeat_age'] for n in good), default=None),
            'unhealthy_nodes': sum(bool(n.get('unhealthy') or n.get('snapshot_errors')) for n in good),
        }
    out['total'] = {
        'cpu_cores': sum(n['cpu_cores'] for n in out['lanes'].values()),
        'io_bytes_per_second': {k: sum(n['io_bytes_per_second'][k] for n in out['lanes'].values()) for k in ('rchar', 'wchar', 'read_bytes', 'write_bytes')},
        **{k: sum(n[k] for n in out['lanes'].values()) for k in ('threads', 'active', 'awaiting_receipt', 'active_mean', 'awaiting_receipt_mean')},
    }
    for numerator, denominator, key in (
        ('awaiting_receipt', 'active', 'awaiting_fraction_end'),
        ('awaiting_receipt_mean', 'active_mean', 'awaiting_fraction_mean')):
        out['total'][key] = out['total'][numerator] / out['total'][denominator] if out['total'][denominator] else None
    return out


def collect(name, args):
    destination = transport.ssh_destination(name, job=JOB)
    prefix = ('CONTROL=' + repr(args.control) + '\nDURATION=' + repr(args.duration)
              + '\nINTERVAL=' + repr(args.interval) + '\nBROKER_PID=' + repr(args.broker_pid) + '\n')
    command = 'python3 -c ' + shlex.quote(prefix + REMOTE)
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-p', '2222',
                             destination, command],
                            capture_output=True, text=True, timeout=args.duration + 120)
    if result.returncode:
        return {'node': name, 'error': 'ssh/remote process failed', 'returncode': result.returncode,
                'stderr': result.stderr[-1500:]}
    parsed = json.loads(result.stdout)
    parsed['node'] = name
    parsed['summary'] = summarize(parsed)
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--broker-pid', type=int, required=True)
    parser.add_argument('--control', default=CONTROL)
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--interval', type=float, default=10)
    parser.add_argument('--output', type=pathlib.Path, default=pathlib.Path('work/post_broker_assessment.json'))
    args = parser.parse_args()
    if args.duration <= 0 or args.interval <= 0:
        parser.error('duration and interval must be positive')
    report = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'broker_pid': args.broker_pid, 'control': args.control, 'nodes': [], 'errors': []}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(collect, node, args): node for node in NODES}
        for future in concurrent.futures.as_completed(futures):
            try:
                node = future.result()
            except Exception as exc:
                report['errors'].append({'node': futures[future], 'error': type(exc).__name__ + ': ' + str(exc)})
                continue
            if 'error' in node:
                report['errors'].append(node)
            else:
                report['nodes'].append(node)
    report['nodes'].sort(key=lambda n: NODES.index(n['node']))
    report['aggregate'] = aggregate(report['nodes'])
    report['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'output': str(args.output), 'aggregate': report['aggregate'], 'errors': report['errors']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
