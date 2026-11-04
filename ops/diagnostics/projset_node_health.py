#!/usr/bin/env python3
"""Read-only worker identity, process and cgroup memory/CPU/OOM observation."""
import datetime
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def pairs(value):
    out = {}
    for line in (value or '').splitlines():
        fields = line.replace(':', '').split()
        if len(fields) > 1:
            try:
                out[fields[0]] = int(fields[1])
            except ValueError:
                out[fields[0]] = ' '.join(fields[1:])
    return out


def inspect(run, node):
    run = Path(run)
    folder=run/'nodes'/node
    launches=[folder/'launch.json']+list(folder.glob('attempt-*/launch.json'))
    launch=max((json.loads(p.read_text()) for p in launches),key=lambda x:x['started'])
    pid = launch['pid']
    proc = Path('/proc')/str(pid)
    stat = read(proc/'stat')
    argv = (proc/'cmdline').read_bytes().split(b'\0') if stat else []
    fields = stat.rsplit(') ', 1)[1].split() if stat else []
    process = dict(pid=pid, present=bool(stat), start_matches=bool(fields) and fields[19]==launch['start'],
        argv_matches=launch['argv'][0].encode() in argv,
        status=pairs(read(proc/'status')), io=pairs(read(proc/'io')),
        oom_score=read(proc/'oom_score'), oom_score_adj=read(proc/'oom_score_adj'),
        wchan=read(proc/'wchan'), fd_count=len(list((proc/'fd').iterdir())) if proc.exists() else None,
        cpu_ticks=int(fields[11])+int(fields[12]) if fields else None,
        started=launch['started'])
    cgtext=read(proc/'cgroup') or ''
    v2=next((x.split(':',2)[2] for x in cgtext.splitlines() if x.startswith('0::')),None)
    cgpaths=[]
    mountlines=(read('/proc/self/mountinfo') or '').splitlines()
    for line in mountlines:
        left,right=line.split(' - ',1);mount=left.split()[4];kind=right.split()[0]
        if kind=='cgroup2' and v2 is not None:
            cgpaths.extend([Path(mount),Path(mount)/v2.lstrip('/')])
        elif kind=='cgroup':
            controllers=right.split()[2].split(',')
            if any(x in controllers for x in ('memory','cpu','cpuacct')):
                cgpaths.append(Path(mount))
                for entry in cgtext.splitlines():
                    _,names,path=entry.split(':',2)
                    if set(names.split(',')) & set(controllers):cgpaths.append(Path(mount)/path.lstrip('/'))
    files=['memory.current','memory.peak','memory.max','memory.high','memory.events','memory.events.local',
        'memory.stat','memory.swap.current','memory.swap.max','memory.pressure','cpu.max','cpu.stat','cpu.pressure',
        'cpuset.cpus.effective','pids.current','pids.max','memory.usage_in_bytes','memory.max_usage_in_bytes',
        'memory.limit_in_bytes','memory.failcnt','memory.oom_control','memory.memsw.usage_in_bytes',
        'memory.memsw.limit_in_bytes','cpu.cfs_quota_us','cpu.cfs_period_us','cpuacct.usage']
    cgroups=[]
    for path in sorted(set(cgpaths)):
        data={name:v for name in files if (v:=read(path/name)) is not None}
        if data:cgroups.append(dict(path=str(path),data=data))
    since=datetime.datetime.fromtimestamp(launch['started'],datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    try:
        d=subprocess.run(['dmesg','--time-format','iso','--since',since],capture_output=True,text=True,timeout=10,env=dict(os.environ,TZ='UTC'))
        matches=[x for x in d.stdout.splitlines() if re.search(r'out of memory|oom[-_ ]|killed process|memory cgroup',x,re.I)]
        kernel=dict(available=d.returncode==0,returncode=d.returncode,oom_lines=matches[-40:],
                    error=d.stderr[-300:] if d.returncode else None)
    except (OSError,subprocess.TimeoutExpired) as e:
        kernel=dict(available=False,error=type(e).__name__)
    return dict(node=node,host=socket.gethostname(),boot=read('/proc/sys/kernel/random/boot_id'),
        boot_matches=read('/proc/sys/kernel/random/boot_id')==launch['boot'],updated=time.time(),
        process=process,cgroup_membership=cgtext,cgroups=cgroups,kernel=kernel,
        host_memory=pairs(read('/proc/meminfo')),host_load=read('/proc/loadavg'),
        effective_cpus=len(os.sched_getaffinity(0)),pressure={k:read('/proc/pressure/'+k) for k in ('memory','cpu','io')})


if __name__=='__main__':
    print(json.dumps(inspect(sys.argv[1],sys.argv[2]),separators=(',',':')))
