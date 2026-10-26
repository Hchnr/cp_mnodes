#!/usr/bin/env python3
"""Exactly four idle master copy lanes, one 90-second shared-four-CPU trial.

Independent watchdog: new lease/payload FD, phase change, deadline or controller
EOF restores all owned masks. No copy restart/signal, thread or quota change.
"""
import argparse
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import sqlite3
import time
import urllib.parse

from migration_io import read_json, write_json
from experiment_idle_affinity import identity, same, restore, require_no_data_fds, cpu

CONTROL3=Path('/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a')
CONTROL4=Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053')
BOUND={5873:'73233439',5879:'73233459',3737241:'77411427',3741388:'77412116'}


def trial_mask(original,pid,separate_priority_groups=False):
    values=sorted(original)
    return values[4:8] if separate_priority_groups and pid in (3737241,3741388) else values[:4]


def restore_all(plans):
    results=[]
    for plan in plans:
        try:result=restore(plan)
        except BaseException as exc:result={'status':'restore_failed','error':repr(exc)}
        results.append(dict(result,pid=plan['expected']['pid']))
    return {'status':'restored' if all(r['status']=='restored' for r in results) else 'inspect_restoration',
            'workers':results}


def group_guard(entries):
    states={}
    for root in (CONTROL3,CONTROL4):
        state=read_json(root/'state.json');states[str(root)]=state
        if ((root/'STOP').exists() or state.get('stage')!=('p3-r3' if root==CONTROL3 else 'p4-r1')
                or state.get('status')!=('scanning' if root==CONTROL3 else 'copying')
                or (root==CONTROL3 and state.get('recovery_warm') is not True)):
            raise RuntimeError('bound source phase changed')
        cfg=read_json(root/'config.json')
        path='/tmp/'+cfg['run_id']+'-broker.sqlite'
        owners=[os.uname().nodename+'-'+str(i) for i in range(128)]
        with sqlite3.connect('file:'+urllib.parse.quote(path,safe='/')+'?mode=ro',uri=True,timeout=2) as db:
            count=db.execute("SELECT COUNT(*) FROM claims WHERE status='leased' AND owner IN ("+
                ','.join('?' for _ in owners)+')',owners).fetchone()[0]
        if count:raise RuntimeError('new master lease; restore all lanes immediately')
    for entry in entries:
        root=Path(entry['control']);expected=entry['expected']
        if not same(identity(expected['pid']),expected):raise RuntimeError('copy identity changed')
        node=read_json(root/entry['folder']/(os.uname().nodename+'.json'))
        age=time.time()-datetime.fromisoformat(node['updated']).timestamp()
        if (node.get('pid')!=expected['pid'] or node.get('active') or node.get('ended')
                or node.get('thread_error') or node.get('threads')!=64 or not 0<=age<15):
            raise RuntimeError('copy lane is no longer freshly idle')
        require_no_data_fds(expected['pid'],root,read_json(root/'config.json')['run_id'])
    return states[str(CONTROL3)]


def watchdog(plans,entries,read_fd,output,deadline):
    for sig in (signal.SIGHUP,signal.SIGINT,signal.SIGTERM):signal.signal(sig,signal.SIG_IGN)
    while True:
        remaining=max(0,deadline-time.monotonic())
        ready,_,_=select.select([read_fd],[],[],min(.5,remaining))
        reason=None
        if ready:
            if os.read(read_fd,1)==b'd':
                write_json(output,{'status':'controller_restored','at':time.time()});return
            reason='controller_pipe_closed'
        elif time.monotonic()>=deadline:reason='deadline'
        else:
            try:group_guard(entries)
            except BaseException as exc:reason='idle_guard_failed: '+repr(exc)
        if reason:
            write_json(output,dict(restore_all(plans),reason=reason,at=time.time()));return


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True,type=Path)
    parser.add_argument('--separate-priority-groups',action='store_true',
                        help='bounded retry: separate nice=15 P3 and nice=0 P4 CPU pools')
    args=parser.parse_args()
    directory=args.directory.resolve()
    entries=read_json(directory/'intent.json')['workers']
    if (directory.parent!=CONTROL3 or not directory.name.startswith('idle-group-affinity-')
            or os.uname().nodename!='job-2dadf47a-b549-47bb-a267-d56dc877aec9-master-0'
            or len(entries)!=4 or {e['expected']['pid']:e['expected']['start_ticks'] for e in entries}!=BOUND
            or {(e['control'],e['folder']) for e in entries}!=
               {(str(c),f) for c in (CONTROL3,CONTROL4) for f in ('nodes','nodes-extra')}):
        raise ValueError('only the four approved idle master lanes are in scope')
    group_guard(entries)
    plans=[]
    for entry in entries:
        expected=entry['expected'];masks=[os.sched_getaffinity(int(t.name)) for t in
                (Path('/proc')/str(expected['pid'])/'task').iterdir()]
        if len(masks)!=67 or len(masks[0])<8 or any(m!=masks[0] for m in masks):
            raise RuntimeError('require unchanged 67-thread lane and common broad mask')
        if args.separate_priority_groups and os.getpriority(os.PRIO_PROCESS,expected['pid'])!=(
                15 if expected['pid'] in (5873,5879) else 0):
            raise RuntimeError('priority-group retry requires the observed unchanged nice classes')
        plans.append(dict(expected=expected,original=sorted(masks[0]),
                          trial=trial_mask(masks[0],expected['pid'],args.separate_priority_groups)))
    if len({tuple(p['original']) for p in plans})!=1:raise RuntimeError('common original mask required')
    result={'status':'baseline','plans':plans,'points':[],'started':time.time(),
            'copy_restarts':0,'signals_to_copy_processes':0,'copy_thread_count_changes':0,'trial_seconds':90,
            'separate_priority_groups':args.separate_priority_groups}
    output=directory/'result.json';watch_output=directory/'watchdog-result.json'
    def sample(label):
        s=group_guard(entries);cg=Path('/sys/fs/cgroup')
        result['points'].append(dict(label=label,time=time.time(),monotonic=time.monotonic(),
            cpus={str(p):cpu(p) for p in (*BOUND,281673,2330276)},
            cgroup_cpu=dict(line.split() for line in (cg/'cpu.stat').read_text().splitlines()),
            cpu_pressure=(cg/'cpu.pressure').read_text(),state_updated=s['updated'],
            source=s['source_reader_progress'],target=s['target_filter'],next_task_id=s['next_task_id']))
        write_json(output,result)
    def observe(label,seconds):
        until=time.monotonic()+seconds
        sample(label)
        while time.monotonic()<until:time.sleep(5);sample(label)
    with open('/tmp/hcr-copy-20261023T141342Z-2dad-d7de938a-idle-group-affinity.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        observe('baseline',45)
        read_fd,write_fd=os.pipe();deadline=time.monotonic()+90
        pid=os.fork()
        if pid==0:
            os.close(write_fd);watchdog(plans,entries,read_fd,watch_output,deadline);os._exit(0)
        os.close(read_fd);result['watchdog_pid']=pid
        def interrupted(*_):raise InterruptedError('group affinity controller interrupted')
        for sig in (signal.SIGHUP,signal.SIGINT,signal.SIGTERM):signal.signal(sig,interrupted)
        try:
            result['status']='applying';write_json(output,result)
            for plan in plans:
                group_guard(entries)
                for thread in (Path('/proc')/str(plan['expected']['pid'])/'task').iterdir():
                    if time.monotonic()>=deadline or watch_output.exists():raise RuntimeError('trial guard already ended')
                    tid=int(thread.name)
                    if (not same(identity(plan['expected']['pid']),plan['expected'])
                            or os.sched_getaffinity(tid)!=set(plan['original'])):
                        raise RuntimeError('identity or affinity changed before applying trial')
                    os.sched_setaffinity(tid,set(plan['trial']))
            result['status']='trial';last_sample=0
            while time.monotonic()<deadline:
                group_guard(entries)
                if watch_output.exists():raise RuntimeError('independent guard restored trial')
                if time.monotonic()-last_sample>=5:sample('trial');last_sample=time.monotonic()
                time.sleep(.5)
            result['status']='sampled'
        except BaseException as exc:result.update(status='interrupted',error=repr(exc))
        finally:
            for sig in (signal.SIGHUP,signal.SIGINT,signal.SIGTERM):signal.signal(sig,signal.SIG_IGN)
            result['restoration']=restore_all(plans);result['restored_at']=time.time()
            write_json(output,result)
            if result['restoration']['status']=='restored':
                try:os.write(write_fd,b'd')
                except BrokenPipeError:pass  # Deadline/new-work guard already restored and exited.
            os.close(write_fd);os.waitpid(pid,0)
        observe('restored',45)
        result['finished']=time.time();write_json(output,result)
        print(json.dumps({k:v for k,v in result.items() if k not in ('plans','points')}),flush=True)


if __name__=='__main__':main()
