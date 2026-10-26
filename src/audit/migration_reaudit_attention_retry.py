#!/usr/bin/env python3
"""One bounded retry of completed source-changed tasks, never in-flight work.

Validate original authority, frozen code/shards and exact completed receipt
lines. Reobserve source metadata and require an absent target. Reuse unchanged
SHA/readback/fsync/no-replace primitives; keep original receipts and counters.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
import gzip
import json
import os
from pathlib import Path
import stat
import sys
import time


def refresh_candidate(base,cfg,task):
    old=task['entry'];base.validate_row(dict(rel=old['rel'],kind='file',source=old,reason='target_absent'))
    parent,name=base.native.open_parent_readonly(cfg['source'],old['rel'])
    try:source=os.stat(name,dir_fd=parent,follow_symlinks=False)
    finally:os.close(parent)
    if not stat.S_ISREG(source.st_mode):return None,'source_type_changed'
    target=None
    try:
        target,name=base.native.open_parent_readonly(cfg['target'],old['rel'])
        os.stat(name,dir_fd=target,follow_symlinks=False)
    except FileNotFoundError:pass
    else:return None,'existing_target_retained'
    finally:
        if target is not None:os.close(target)
    entry=base.validate_row(dict(rel=old['rel'],kind='file',source=base.engine._record(source),reason='target_absent'))
    return dict(id=task['id'],entry=entry),None


def run(snapshot,output,prior_refreshed_bytes=0):
    if type(prior_refreshed_bytes) is not int or prior_refreshed_bytes<0:
        raise ValueError('nonnegative prior refreshed byte budget required')
    snapshot=Path(snapshot).resolve();summary=json.loads(snapshot.read_text());control=Path(summary['control']).resolve()
    sys.path.insert(0,str(control/'code'))
    import migration_reaudit_copy as entry
    base=entry.base;cfg=base.read_json(control/'config.json')
    output=Path(output).resolve()
    if output.parent!=snapshot.parent or output.exists():raise ValueError('new adjacent retry folder required')
    if cfg['job_kind']!='files' or base.digest(control/'config.json')!=summary['config_sha256']:
        raise ValueError('original file configuration changed')
    manifest=Path(summary['manifest'])
    if manifest.parent!=snapshot.parent or base.digest(manifest)!=summary['sha256']:
        raise ValueError('completed attention evidence changed')
    with gzip.open(manifest,'rt') as stream:events=[json.loads(line) for line in stream]
    if not 1<=len(events)<=64 or len(events)!=summary['completed_attention']:
        raise ValueError('bounded exact attention set required')
    grouped=collections.defaultdict(dict)
    for event in events:
        node=event['node'];task=event['task'];receipt=event['receipt']
        if receipt['status']!='changed' or receipt['id']!=task['id'] or receipt['rel']!=task['entry']['rel']:
            raise ValueError('only completed source-changed tasks accepted')
        path=Path(event['original_receipt'])
        if path.parent!=control/'nodes'/node or not path.name.startswith('receipts-'):
            raise ValueError('foreign receipt')
        with path.open('rb') as stream:
            stream.seek(event['byte_offset']);line=stream.readline()
        import hashlib
        if not line.endswith(b'\n') or hashlib.sha256(line).hexdigest()!=event['line_sha256'] or json.loads(line)!=receipt:
            raise ValueError('completed original receipt line differs')
        if task['id'] in grouped[node]:raise ValueError('duplicate completed task')
        grouped[node][task['id']]=task
    for node,wanted in grouped.items():
        entry.validate(control,cfg,node)
        with gzip.open(cfg['shards'][node]['path'],'rt') as stream:
            remaining=set(wanted)
            for line in stream:
                original=json.loads(line)
                if original['id'] in remaining:
                    if original!=wanted[original['id']]:raise ValueError('task differs from original frozen shard')
                    remaining.remove(original['id'])
                if not remaining:break
        if remaining:raise ValueError('completed task outside original shard')
    output.mkdir();base.install_engine(cfg);new=[];observations=[]
    for event in events:
        try:task,reason=refresh_candidate(base,cfg,event['task'])
        except OSError as exc:task=None;reason=type(exc).__name__+': '+str(exc)
        observations.append(dict(node=event['node'],original_id=event['task']['id'],rel=event['receipt']['rel'],
            refreshed_task=task,not_dispatched_reason=reason))
        if task is not None:new.append(task)
    total=sum(t['entry']['size'] for t in new)
    # Conservatively count old scope plus refreshed versions, even though
    # these are the SAME paths, not additional unique files.
    if cfg['bytes']+prior_refreshed_bytes+total>=50_000_000_000_000:raise ValueError('conservative combined scope exceeds threshold')
    plan=dict(updated=time.time(),original_control=str(control),original_snapshot=str(snapshot),
        original_snapshot_sha256=base.digest(snapshot),observations=observations,accepted_files=len(new),accepted_bytes=total,
        conservative_combined_bytes=cfg['bytes']+prior_refreshed_bytes+total,
        prior_refreshed_bytes=prior_refreshed_bytes,same_main_paths_not_additional_unique_files=True,
        whole_tree_completion_proven=False)
    base.atomic(output/'plan.json',plan)
    retry_cfg=dict(cfg,run_id=cfg['run_id']+'-completed-attention-retry-v1')
    counts=collections.Counter();rows=[];started=time.time()
    with open(output/'receipts.jsonl','x') as log,ThreadPoolExecutor(max_workers=4) as pool:
        pending={pool.submit(base.copy_one,retry_cfg,t):t for t in new}
        while pending:
            done,_=wait(pending,timeout=3,return_when=FIRST_COMPLETED)
            for future in done:
                task=pending.pop(future);receipt=future.result()
                valid=receipt['status'] in ('copied','skipped') and base.resume_valid(retry_cfg,task['entry'],receipt)
                row=dict(task=task,receipt=receipt,postcopy_identity_valid=valid)
                rows.append(row);counts['processed']+=1;counts[receipt['status']]+=1
                if valid:counts['identity_verified']+=1;counts['verified_bytes']+=task['entry']['size']
                log.write(json.dumps(row,separators=(',',':'))+'\n');log.flush();os.fsync(log.fileno())
            base.atomic(output/'progress.json',dict(status='copying',updated=time.time(),counts=dict(counts),active=len(pending)))
    result=dict(status='all_refreshed_versions_copied_and_identity_verified' if counts['identity_verified']==len(events)
                else 'completed_attention_still_needs_reconciliation',started=started,updated=time.time(),counts=dict(counts),
        original_attention=len(events),accepted_files=len(new),accepted_bytes=total,results=rows,observations=observations,
        same_main_paths_not_additional_unique_files=True,whole_tree_completion_proven=False,
        original_receipts_unchanged=True,original_copy_workers_untouched=True)
    base.atomic(output/'result.json',result);print(json.dumps({k:result[k] for k in ('status','counts','accepted_files','accepted_bytes')}),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--snapshot',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--prior-refreshed-bytes',type=int,default=0)
    a=p.parse_args();run(a.snapshot,a.output,a.prior_refreshed_bytes)
