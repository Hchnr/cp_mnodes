#!/usr/bin/env python3
"""Recheck closed namespace audit exceptions without copying or changing data."""
import argparse
import collections
import gzip
import json
import os
from pathlib import Path
import stat
import sys
import time


def inspect(base, cfg, task):
    item=task['entry'];rel=item['rel'];parents=[];reopened=[]
    row=dict(id=task['id'],rel=rel,kind=item['kind'])
    try:
        for root in (cfg['source'],cfg['target']):
            fd,name=base.native.open_parent_readonly(root,rel);parents.append(fd)
        bindings=[os.fstat(fd) for fd in parents]
        before=[os.stat(name,dir_fd=fd,follow_symlinks=False) for fd in parents]
        if item['kind']=='directory':
            valid=all(stat.S_ISDIR(s.st_mode) for s in before)
        elif item['kind']=='symlink':
            valid=all(stat.S_ISLNK(s.st_mode) for s in before)
            if valid:
                texts=[os.readlink(name,dir_fd=fd) for fd in parents]
                valid=texts[0]==texts[1]
        else:raise ValueError('not a namespace task')
        after=[os.stat(name,dir_fd=fd,follow_symlinks=False) for fd in parents]
        if any(not base.same_identity(a,base.engine._record(b)) for a,b in zip(after,before)):
            return dict(row,status='changed_during_check')
        for root,binding in zip((cfg['source'],cfg['target']),bindings):
            fd,_=base.native.open_parent_readonly(root,rel);reopened.append(fd);now=os.fstat(fd)
            if (now.st_dev,now.st_ino)!=(binding.st_dev,binding.st_ino):
                return dict(row,status='parent_binding_changed')
        return dict(row,status='namespace_path_present' if valid else 'namespace_type_or_link_conflict',
            source=base.engine._record(before[0]),target=base.engine._record(before[1]),
            source_matches_frozen_version=base.same_identity(before[0],item['identity']),
            metadata_differs=any(getattr(before[0],'st_'+key)!=getattr(before[1],'st_'+key)
                                for key in ('mode','uid','gid','mtime_ns')),
            subtree_completeness_proven=False)
    except OSError as error:
        return dict(row,status='path_unavailable',error=type(error).__name__+': '+str(error),errno=error.errno)
    finally:
        for fd in parents+reopened:os.close(fd)


def run(control,node,output):
    control=Path(control).resolve();output=Path(output).resolve()
    sys.path.insert(0,str(control/'code'))
    import migration_reaudit_copy as entry
    base=entry.base;cfg=base.read_json(control/'config.json')
    if cfg['job_kind']!='namespace':raise ValueError('namespace control required')
    entry.validate(control,cfg,node)
    handoff=control/'handoffs'/node;audit=handoff/'post-copy-audit'
    closed=base.read_json(handoff/'copy-closed-process-proof.json')
    proof=base.read_json(audit/'result.json')
    if (not closed['remote_actual_processes']['receiver']['actual_terminal']
            or not proof['actual_receiver_terminal'] or not proof['exact_receipt_coverage']
            or closed['progress']['active'] or not closed['progress']['status'].startswith('complete')
            or closed['progress']['counts']['processed']!=cfg['shards'][node]['files']
            or proof['original_config_sha256']!=base.digest(control/'config.json')
            or proof['shard_sha256']!=cfg['shards'][node]['sha256']):
        raise ValueError('closed shard evidence differs')
    for name,checksum in proof['receipts'].items():
        if Path(name).name!=name or base.digest(control/'nodes'/node/name)!=checksum:
            raise ValueError('closed receipts changed')
    path=audit/'issues.jsonl.gz';checksum=base.digest(path)
    with gzip.open(path,'rt') as stream:issues=[json.loads(line) for line in stream]
    expected=proof['counts']['processed']-proof['counts']['path_identity_verified']
    if not 1<=len(issues)<=64 or len(issues)!=expected:raise ValueError('bounded exact issue set required')
    wanted={i['id']:i for i in issues}
    if len(wanted)!=len(issues):raise ValueError('duplicate issue ID')
    tasks={}
    with gzip.open(cfg['shards'][node]['path'],'rt') as stream:
        for line in stream:
            task=json.loads(line)
            if task['id'] in wanted:
                if task['entry']['rel']!=wanted[task['id']]['rel']:raise ValueError('issue path differs from frozen task')
                tasks[task['id']]=task
    if set(tasks)!=set(wanted):raise ValueError('issue outside closed shard')
    rows=[inspect(base,cfg,tasks[i['id']]) for i in issues]
    if base.digest(path)!=checksum:raise ValueError('issue input changed')
    if output.exists():raise ValueError('fresh output folder required')
    output.mkdir()
    result=dict(status='namespace_attention_metadata_rechecked',updated=time.time(),control=str(control),node=node,
        original_audit=str(audit/'result.json'),original_audit_sha256=base.digest(audit/'result.json'),
        issues_sha256=checksum,counts=dict(collections.Counter(r['status'] for r in rows)),results=rows,
        payload_read=False,target_mutated=False,subtree_completeness_proven=False,
        metadata_equality_proven=False,whole_tree_completion_proven=False)
    base.atomic(output/'result.json',result)
    print(json.dumps(dict(status=result['status'],counts=result['counts'])),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True)
    p.add_argument('--node',required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();run(a.control,a.node,a.output)
