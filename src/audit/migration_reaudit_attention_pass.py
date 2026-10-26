#!/usr/bin/env python3
"""Finite serial groups of completed source-change retries, never whole-tree proof.

One subprocess at a time, four copying threads at most. Preserve prior groups,
bind frozen helper digests, accumulate refreshed byte budgets, never auto-resume
an ambiguous spawn or repeatedly retry an unstable source in this pass.
"""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


def digest(path):
    h=hashlib.sha256()
    with open(path,'rb') as stream:
        for chunk in iter(lambda:stream.read(4*1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def read(path):return json.loads(Path(path).read_text())


def exclusive(path,value):
    with open(path,'x') as stream:
        json.dump(value,stream,separators=(',',':'));stream.flush();os.fsync(stream.fileno())


def prior_budget(control,snapshots):
    config_sha=digest(control/'config.json');total=0;proofs=[]
    for path in snapshots:
        summary=read(path);plan_path=path.parent/'retry-v1/plan.json';plan=read(plan_path)
        if (summary['control']!=str(control) or summary['config_sha256']!=config_sha
                or digest(summary['manifest'])!=summary['sha256']
                or plan['original_control']!=str(control) or plan['original_snapshot']!=str(path)
                or plan['original_snapshot_sha256']!=digest(path)
                or type(plan['accepted_bytes']) is not int or plan['accepted_bytes']<0):
            raise ValueError('prior accepted byte proof differs')
        total+=plan['accepted_bytes'];proofs.append(dict(snapshot=str(path),sha256=digest(path),
            retry_plan=str(plan_path),retry_plan_sha256=digest(plan_path),accepted_bytes=plan['accepted_bytes']))
    return total,proofs


def prepare(control,folder,snapshots,start_group,max_groups):
    control=control.resolve();folder=folder.resolve();cfg=read(control/'config.json')
    if (cfg['job_kind']!='files' or folder.parent!=Path(cfg['audit_root'])/'final'
            or not 1<=max_groups<=8 or start_group<3):raise ValueError('finite shared file-attention pass required')
    snapshots=[p.resolve() for p in snapshots];budget,proofs=prior_budget(control,snapshots)
    if cfg['bytes']+budget>=50_000_000_000_000:raise ValueError('prior scope exceeds byte budget')
    folder.mkdir();code=folder/'code';code.mkdir();repo=Path(__file__).resolve().parent;hashes={}
    for name in ('migration_reaudit_attention_pass.py','migration_reaudit_attention_snapshot.py',
                 'migration_reaudit_attention_retry.py'):
        shutil.copyfile(repo/name,code/name);hashes[name]=digest(code/name)
    exclusive(folder/'plan.json',dict(control=str(control),config_sha256=digest(control/'config.json'),
        previous=proofs,prior_refreshed_bytes=budget,start_group=start_group,max_groups=max_groups,
        code=hashes,created=time.time(),whole_tree_completion_proven=False))
    print(str(folder),flush=True)


def run(folder):
    folder=folder.resolve();plan=read(folder/'plan.json');control=Path(plan['control']);cfg=read(control/'config.json')
    sys.path.insert(0,str(control/'code'));import migration_reaudit_copy as entry
    base=entry.base;entry.validate(control,cfg,next(iter(cfg['shards'])))
    if digest(control/'config.json')!=plan['config_sha256']:raise ValueError('original config differs')
    for name,checksum in plan['code'].items():
        if Path(name).name!=name or digest(folder/'code'/name)!=checksum:raise ValueError('frozen pass helper differs')
    previous=[Path(p['snapshot']) for p in plan['previous']]
    budget,proofs=prior_budget(control,previous)
    if budget!=plan['prior_refreshed_bytes'] or proofs!=plan['previous']:raise ValueError('prior budget proof changed')
    who=dict(pid=os.getpid(),start=Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()[19])
    exclusive(folder/'driver-instance.json',who)
    counts=collections.Counter();groups=[];group=None
    def state(status,**extra):
        base.atomic(folder/'progress.json',dict(status=status,updated=time.time(),identity=who,group=group,
            completed_groups=len(groups),max_groups=plan['max_groups'],counts=dict(counts),groups=groups,
            conservative_combined_bytes=cfg['bytes']+budget,whole_tree_completion_proven=False,**extra))
    def child(role,command):
        label=str(group)+'-'+role;exclusive(folder/(label+'-spawn-intent.json'),dict(command=command,created=time.time()))
        with open(folder/(label+'.log'),'ab',buffering=0) as log:
            process=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=log,cwd='/',start_new_session=True)
            start=Path('/proc',str(process.pid),'stat').read_text().rsplit(') ',1)[1].split()[19]
            record=dict(pid=process.pid,start=start,command=command,created=time.time())
            exclusive(folder/(label+'-spawn.json'),record);state(role,child=record)
            if process.wait()!=0:raise RuntimeError(label+' subprocess failed; inspect log, never relaunch blindly')
    try:
        for group in range(plan['start_group'],plan['start_group']+plan['max_groups']):
            target=Path(cfg['audit_root'])/'final'/('main-copy-attention-'+str(group))
            if target.exists():raise ValueError('attention group already exists; inspect instead of relaunch')
            command=['/usr/bin/python3','-S','-B','-u',str(folder/'code/migration_reaudit_attention_snapshot.py'),
                '--control',str(control),'--output',str(target)]
            for path in previous:command+=['--exclude-snapshot',str(path)]
            child('collecting',command)
            if (target/'empty-snapshot.json').exists():
                state('no_new_completed_attention',empty_snapshot=str(target/'empty-snapshot.json'));return
            snapshot=target/'snapshot.json'
            child('copying',['/usr/bin/python3','-S','-B','-u',str(folder/'code/migration_reaudit_attention_retry.py'),
                '--snapshot',str(snapshot),'--output',str(target/'retry-v1'),'--prior-refreshed-bytes',str(budget)])
            result=read(target/'retry-v1/result.json');accepted=read(target/'retry-v1/plan.json')['accepted_bytes']
            budget+=accepted;counts.update(result['counts']);counts['original_attention']+=result['original_attention']
            groups.append(dict(group=group,snapshot=str(snapshot),
                result=str(target/'retry-v1/result.json'),result_sha256=digest(target/'retry-v1/result.json')))
            previous.append(snapshot);state('group_closed')
        state('bounded_pass_finished')
    except BaseException as error:
        state('inspection_required',error=repr(error));raise


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('role',choices=('prepare','run'))
    p.add_argument('--folder',type=Path,required=True);p.add_argument('--control',type=Path)
    p.add_argument('--previous',type=Path,action='append',default=[])
    p.add_argument('--start-group',type=int,default=3);p.add_argument('--max-groups',type=int,default=7)
    a=p.parse_args()
    if a.role=='prepare':prepare(a.control,a.folder,a.previous,a.start_group,a.max_groups)
    else:run(a.folder)
