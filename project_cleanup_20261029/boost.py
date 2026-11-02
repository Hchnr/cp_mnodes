#!/usr/bin/env python3
"""Prepare a separately stoppable supplement; start is an operator action."""
import argparse
import collections
import concurrent.futures as futures
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import uuid

import rm24

HERE = Path(__file__).resolve().parent
POINTER = HERE/'runs/boost-current.json'

OBSERVE = r'''
import os,pathlib,json
run=pathlib.Path(RUN);root='/share/project'
actor=run/'workers'/(NODE+'.json');owner=None
if actor.exists():
 a=json.loads(actor.read_text())
 try:
  s=pathlib.Path('/proc/'+str(a['pid'])+'/stat').read_text().rsplit(')',1)[1].split()
  if s[19]==a['start'] and a['boot']==pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip():owner=a['pid']
 except FileNotFoundError:pass
roots=[];opened=[];count=0
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  args=[os.fsdecode(x) for x in p.joinpath('cmdline').read_bytes().split(b'\0') if x]
  if not args or os.path.basename(args[0])!='rm':continue
  cwd=os.readlink(p/'cwd')
  if cwd!=root and not cwd.startswith(root+'/'):continue
  count+=1
  fields=p.joinpath('stat').read_text().rsplit(')',1)[1].split()
  if int(fields[1])==owner and '--' in args:
   roots.extend(cwd+'/'+n for n in args[args.index('--')+1:] if '/' not in n and n not in ('.','..',''))
  for fd in p.joinpath('fd').iterdir():
   try:
    v=os.readlink(fd)
    if v.startswith(root+'/'):opened.append(v.removesuffix(' (deleted)'))
   except OSError:pass
 except (OSError,ValueError):pass
print(json.dumps(dict(node=NODE,rm_count=count,roots=sorted(set(roots)),opened=sorted(set(opened)))))
'''


def observe(run):
    def node(n):
        code='RUN='+repr(str(run))+'\nNODE='+repr(n)+'\n'+OBSERVE
        result=subprocess.run(['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=6',n,'python3 -S -B -'],
            input=code,text=True,capture_output=True,timeout=20)
        if result.returncode:
            raise RuntimeError(n+': '+result.stderr[-500:])
        return json.loads(result.stdout)
    with futures.ThreadPoolExecutor(max_workers=24) as pool:
        return list(pool.map(node,rm24.NODES))


def choose_tasks(root, roots, opened, limit=256):
    """Bounded read-only directory sampling; no sizes or file contents."""
    tasks=[];chosen=[];scans=0
    for absolute in roots:
        if not absolute.startswith(root.path+'/'):
            continue
        rel=absolute[len(root.path)+1:]
        parts=rel.split('/')
        for p in parts:
            rm24.component(p)
        fd=os.dup(root.fd)
        try:
            for part in parts:
                child=os.open(part,rm24.FLAGS,dir_fd=fd)
                os.close(fd);fd=child
                if rm24.mount_id(fd)!=root.mount:
                    raise ValueError('nested mount')
            inode=os.fstat(fd).st_ino
        except OSError:
            continue
        finally:
            os.close(fd)
        waiting=collections.deque([(rel,inode,0)])
        per_root=0
        while waiting and scans<64 and per_root<16 and len(tasks)<limit:
            parent,ino,depth=waiting.popleft();scans+=1
            try:
                with root.parent(parent,ino) as fd,os.scandir(fd) as entries:
                    for index,entry in enumerate(entries):
                        if index>=512:
                            break
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        child=parent+'/'+entry.name;absolute_child=root.path+'/'+child
                        # This excludes a currently opened descendant, but an
                        # original rm still owns the enclosing traversal. It is
                        # deliberately cooperative, not exclusive ownership.
                        if any(p==absolute_child or p.startswith(absolute_child+'/') for p in opened):
                            if depth<1:
                                waiting.append((child,entry.inode(),depth+1))
                            continue
                        if depth<1:
                            waiting.append((child,entry.inode(),depth+1));continue
                        if any(child==p or child.startswith(p+'/') or p.startswith(child+'/') for p in chosen):
                            continue
                        tasks.append(dict(kind='task',parent=parent,ino=ino,names=[entry.name]))
                        chosen.append(child);per_root+=1
                        if per_root>=16 or len(tasks)>=limit:
                            break
            except FileNotFoundError:
                continue
        if scans>=64 or len(tasks)>=limit:
            break
    return tasks


def prepare(per_node):
    original=rm24.RUN.resolve()
    cfg=rm24.load(original/'config.json')
    if (cfg['target'],cfg['root_inode'])!=(rm24.TARGET,rm24.ROOT_INODE):
        raise ValueError('original scope differs')
    status=rm24.load(original/'status.json')
    if status['state']!='rm_running' or time.time()-status['updated']>30:
        raise ValueError('original run is not freshly observed running')
    if POINTER.exists():
        previous=Path(rm24.load(POINTER)['run'])
        if (previous/'start-claim.json').exists():
            raise ValueError('a supplement was already started; inspect/stop it before preparing another')
    observations=observe(original)
    idle=[r['node'] for r in observations if r['rm_count']==0][:8]
    if not idle:
        raise ValueError('no idle nodes available')
    roots=sorted({p for row in observations for p in row['roots']},
        key=lambda p:(not p.startswith(rm24.TARGET+'/eai_pwm/datasets/'),p))
    opened=[p for row in observations for p in row['opened']]
    root=rm24.Root(rm24.TARGET,rm24.ROOT_INODE)
    try:
        tasks=choose_tasks(root,roots,opened)
    finally:
        root.close()
    if not tasks:
        raise ValueError('no suitable unopened child directories were found')
    run=HERE/'runs'/('boost-'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+uuid.uuid4().hex[:6])
    rm24.prepare(run,per_node,execution_nodes=idle,seed_tasks=tasks,supplement_of=original)
    rm24.atomic(run/'observations.json',observations)
    rm24.atomic(POINTER,dict(run=str(run),original=str(original)))
    print(json.dumps(dict(prepared=str(run),idle_nodes=idle,additional_rm_max=len(idle)*per_node,
        directory_tasks=len(tasks),started=False,cooperative_overlap=True)))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=('prepare','start','stop','status'))
    p.add_argument('--per-node',type=int,default=1)
    args=p.parse_args()
    if not 1<=args.per_node<=8:
        p.error('--per-node must be 1..8')
    if args.action=='prepare':
        prepare(args.per_node);return
    current=rm24.load(POINTER);run=Path(current['run'])
    if args.action=='status':
        for title,path in [('original',Path(current['original'])),('supplement',run)]:
            print(title,json.dumps(rm24.load(path/'status.json')))
        return
    command=[sys.executable,'-S','-B','-u',str(run/'rm24.py'),args.action,'--run',str(run)]
    raise SystemExit(subprocess.call(command))


if __name__=='__main__':
    main()
