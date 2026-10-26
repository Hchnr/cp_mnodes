#!/usr/bin/env python3
"""Read-only path/identity audit of one actually closed namespace shard.

Run on the receiver host, using its frozen handoff validator. Metadata only:
directory ancestor opens, stat/fstat and readlink; no payload reads or copies.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os
from pathlib import Path
import stat
import socket
import sys
import time


def inspect_one(base,task,receipt,source_parent,target_parent):
    item=task['entry'];name=item['rel'].rpartition('/')[2]
    row=dict(id=task['id'],rel=item['rel'],kind=item['kind'])
    if receipt.get('status') not in ('copied','skipped'):return dict(row,status='copy_attention')
    try:
        src=os.stat(name,dir_fd=source_parent,follow_symlinks=False)
        dst=os.stat(name,dir_fd=target_parent,follow_symlinks=False)
        if dst.st_ino!=receipt['dst_stat']['ino']:return dict(row,status='target_inode_changed')
        expected=item['identity']
        if src.st_ino!=expected['ino']:return dict(row,status='source_inode_changed')
        if item['kind']=='directory':
            if not stat.S_ISDIR(src.st_mode) or not stat.S_ISDIR(dst.st_mode):return dict(row,status='directory_type_changed')
            if any(getattr(src,'st_'+k)!=expected[k] for k in ('mtime_ns','ctime_ns')):
                return dict(row,status='source_directory_version_changed',source=base.engine._record(src))
        elif item['kind']=='symlink':
            if not stat.S_ISLNK(src.st_mode) or not stat.S_ISLNK(dst.st_mode):return dict(row,status='symlink_type_changed')
            text=item['link_target']
            if os.readlink(name,dir_fd=source_parent)!=text or os.readlink(name,dir_fd=target_parent)!=text:
                return dict(row,status='symlink_target_changed')
            for parent,before in ((source_parent,src),(target_parent,dst)):
                after=os.stat(name,dir_fd=parent,follow_symlinks=False)
                if not base.same_identity(after,base.engine._record(before)):
                    return dict(row,status='symlink_changed_during_check')
            if not base.same_identity(src,expected):return dict(row,status='source_symlink_version_changed')
        else:raise ValueError('non-namespace task')
        metadata_diff=any(getattr(src,'st_'+k)!=getattr(dst,'st_'+k) for k in ('mode','uid','gid','mtime_ns'))
        return dict(row,status='path_identity_verified',metadata_differs=metadata_diff)
    except (OSError,KeyError) as exc:
        return dict(row,status='path_check_error',error=type(exc).__name__+': '+str(exc),errno=getattr(exc,'errno',None))


def inspect_group(base,cfg,tasks,receipts):
    source=target=None;reopened=[]
    try:
        rel=tasks[0]['entry']['rel'];parent=rel.rpartition('/')[0]
        if any(t['entry']['rel'].rpartition('/')[0]!=parent for t in tasks):raise ValueError('mixed-parent group')
        source,_=base.native.open_parent_readonly(cfg['source'],rel)
        target,_=base.native.open_parent_readonly(cfg['target'],rel)
        before=[os.fstat(source),os.fstat(target)]
        rows=[inspect_one(base,t,receipts[t['id']],source,target) for t in tasks]
        for root,old in zip((cfg['source'],cfg['target']),before):
            fd,_=base.native.open_parent_readonly(root,rel);reopened.append(fd);now=os.fstat(fd)
            if (old.st_dev,old.st_ino)!=(now.st_dev,now.st_ino):
                return [dict(id=t['id'],rel=t['entry']['rel'],status='parent_binding_changed') for t in tasks]
        return rows
    except OSError as exc:
        return [dict(id=t['id'],rel=t['entry']['rel'],status='parent_check_error',error=str(exc),errno=exc.errno) for t in tasks]
    finally:
        for fd in [source,target]+reopened:
            if fd is not None:os.close(fd)


def run(plan_path,bundle,output):
    sys.path.insert(0,str(Path(bundle).resolve()))
    import migration_reaudit_handoff_runtime as runtime
    from migration_reaudit_handoff import process_terminal,verify_prefix
    from migration_reaudit_shared import read
    plan,control,cfg=runtime.validate(plan_path,True);base=runtime.base;folder=Path(plan_path).parent
    output=Path(output).resolve()
    if output.parent!=folder.resolve():raise ValueError('dedicated handoff audit folder required')
    output.mkdir(exist_ok=False);who=runtime.identity()
    while True:
        state=read(control/'nodes'/plan['node']/'progress.json')
        actor=read(folder/'receiver-instance.json')
        if (state['pid'],state['start'],state['host'])!=(actor['pid'],actor['start'],actor['host']):
            raise ValueError('receiver identity differs')
        if process_terminal(actor):break
        base.atomic(output/'progress.json',dict(status='waiting_for_actual_receiver_exit',updated=time.time(),watcher=who))
        time.sleep(15)
    runtime.validate(plan_path,True)
    if state.get('active') or not state['status'].startswith('complete'):
        raise ValueError('receiver did not close full shard')
    return audit_closed(base,control,cfg,plan['node'],state,output,who)


def original_actor_state(state,actor,terminal):
    if any(state.get(k)!=actor[k] for k in ('host','pid','start')):
        return 'original_actor_replaced_or_handed_off'
    if not terminal:return 'waiting_for_actual_original_exit'
    if state.get('active') or not state['status'].startswith('complete'):
        return 'original_actor_exited_without_full_shard'
    return 'ready_for_audit'


def run_original(control,node,output):
    from migration_reaudit_handoff import process_terminal
    from migration_reaudit_shared import read
    control=Path(control).resolve();sys.path.insert(0,str(control/'code'))
    import migration_reaudit_copy as entry
    base=entry.base;cfg=base.read_json(control/'config.json');output=Path(output).resolve()
    if cfg['job_kind']!='namespace' or cfg['execution_mode']!='local':
        raise ValueError('original local namespace control required')
    entry.validate(control,cfg,node)
    actor=read(control/'nodes'/node/'launch.json')
    if actor['host']!=socket.gethostname():raise ValueError('not the original actor host')
    if output!=control/'nodes'/node/'post-copy-audit-original':
        raise ValueError('dedicated original namespace audit folder required')
    output.mkdir(exist_ok=False)
    who=dict(pid=os.getpid(),start=Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()[19],
             host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    base.atomic(output/'watcher-instance.json',who)
    while True:
        state=read(control/'nodes'/node/'progress.json')
        status=original_actor_state(state,actor,process_terminal(actor))
        if status=='ready_for_audit':break
        base.atomic(output/'progress.json',dict(status=status,updated=time.time(),watcher=who))
        if status!='waiting_for_actual_original_exit':
            base.atomic(output/'result.json',dict(status=status,updated=time.time(),
                watcher=who,target_mutated=False,whole_tree_completion_proven=False))
            return
        time.sleep(30)
    entry.validate(control,cfg,node)
    return audit_closed(base,control,cfg,node,state,output,who,actor_kind='original_local')


def audit_closed(base,control,cfg,node,state,output,who,actor_kind='receiver'):
    from migration_reaudit_handoff import verify_prefix
    shard=cfg['shards'][node];nodes=control/'nodes'/node;receipts=base.load_receipts(nodes)
    prefix=verify_prefix(shard['path'],receipts,state)
    if prefix['prefix_tasks']!=shard['files']:raise ValueError('closed shard is not complete')
    receipt_hashes={p.name:base.digest(p) for p in nodes.glob('receipts-*.jsonl')}
    counts=collections.Counter();started=time.time();seen=set()
    def groups():
        batch=[];parent=None
        with gzip.open(shard['path'],'rt') as stream:
            for line in stream:
                task=json.loads(line);key=task['entry']['rel'].rpartition('/')[0]
                if batch and (key!=parent or len(batch)>=128):yield batch;batch=[]
                batch.append(task);parent=key
            if batch:yield batch
    with gzip.open(output/'issues.jsonl.gz','wt',encoding='ascii',compresslevel=1) as issues,ThreadPoolExecutor(max_workers=8) as pool:
        # Bounded groups; Executor.map itself is eager on supported Python.
        pending=[]
        def consume(futures):
            for future in futures:
                for row in future.result():
                    if row['id'] in seen:raise ValueError('duplicate audited task')
                    seen.add(row['id']);counts['processed']+=1;counts[row['status']]+=1
                    if row.get('metadata_differs'):counts['metadata_differences']+=1
                    if row['status']!='path_identity_verified':issues.write(json.dumps(row,separators=(',',':'))+'\n')
            base.atomic(output/'progress.json',dict(status='checking',updated=time.time(),started=started,
                counts=dict(counts),expected=shard['files'],watcher=who))
        for batch in groups():
            pending.append(pool.submit(inspect_group,base,cfg,batch,receipts))
            if len(pending)>=16:consume(pending);pending=[]
        if pending:consume(pending)
    if seen!=set(receipts):raise ValueError('audit/shard/receipt coverage differs')
    for name,checksum in receipt_hashes.items():
        if base.digest(nodes/name)!=checksum:raise ValueError('closed receipts changed during postcheck')
    result=dict(status='closed_namespace_path_identities_verified' if counts['path_identity_verified']==shard['files']
                else 'closed_namespace_needs_reconciliation',updated=time.time(),started=started,counts=dict(counts),
        expected=shard['files'],original_config_sha256=base.digest(control/'config.json'),shard_sha256=shard['sha256'],
        receipts=receipt_hashes,actual_receiver_terminal=actor_kind=='receiver',actual_actor_terminal=True,
        actor_kind=actor_kind,actor_identity={k:state[k] for k in ('host','pid','start')},exact_receipt_coverage=True,
        metadata_equality_proven=False,payload_read=False,target_mutated=False,
        whole_tree_completion_proven=False)
    base.atomic(output/'result.json',result);print(json.dumps(result),flush=True)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);m=p.add_mutually_exclusive_group(required=True)
    m.add_argument('--plan',type=Path);m.add_argument('--control',type=Path)
    p.add_argument('--bundle',type=Path);p.add_argument('--node');p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.control:
        if not a.node:p.error('--control requires --node')
        run_original(a.control,a.node,a.output)
    else:
        if not a.bundle:p.error('--plan requires --bundle')
        run(a.plan,a.bundle,a.output)
