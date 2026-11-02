#!/usr/bin/env python3
"""Read-only file shard postcheck on the proven receiver host after actual exit."""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from pathlib import Path
import time

import migration_reaudit_handoff_runtime as runtime
from migration_reaudit_handoff import process_terminal, verify_prefix
from migration_reaudit_shared import read


def receipt_status(base,cfg,task,receipt):
    if not receipt or receipt.get('rel')!=task['entry']['rel']:return 'missing_receipt'
    if receipt.get('status') not in ('copied','skipped'):return 'copy_attention'
    sha=receipt.get('sha256')
    if not isinstance(sha,str) or len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):
        return 'missing_sha_evidence'
    return ('unchanged_verified_copy_identity' if base.resume_valid(cfg,task['entry'],receipt)
            else 'source_or_target_identity_changed')


def run(path):
    path=Path(path).resolve();plan,control,cfg=runtime.validate(path,True);base=runtime.base
    if cfg['job_kind']!='files':raise ValueError('file handoff required')
    folder=path.parent;output=folder/'post-copy-audit';output.mkdir()
    actor=read(folder/'receiver-instance.json');who=runtime.identity()
    runtime.exclusive(output/'watcher-instance.json',who)
    while True:
        state=read(control/'nodes'/plan['node']/'progress.json')
        if any(state.get(k)!=actor[k] for k in ('host','pid','start')):raise ValueError('receiver identity differs')
        if process_terminal(actor):break
        base.atomic(output/'progress.json',dict(status='waiting_for_actual_receiver_exit',updated=time.time(),watcher=who))
        time.sleep(20)
    runtime.validate(path,True)
    if state.get('active') or not state['status'].startswith('complete'):
        raise ValueError('receiver not fully closed')
    directory=control/'nodes'/plan['node'];receipts=base.load_receipts(directory)
    shard=cfg['shards'][plan['node']];prefix=verify_prefix(shard['path'],receipts,state)
    if prefix['prefix_tasks']!=shard['files']:raise ValueError('full shard receipt coverage differs')
    hashes={p.name:base.digest(p) for p in directory.glob('receipts-*.jsonl')}
    counts=collections.Counter();verified_bytes=0;started=time.time()
    with ThreadPoolExecutor(max_workers=16) as pool,gzip.open(output/'issues.jsonl.gz','wt') as issues:
        def check(task):return task,receipt_status(base,cfg,task,receipts.get(task['id']))
        def consume(tasks):
            nonlocal verified_bytes
            for task,status in pool.map(check,tasks):
                counts['processed']+=1;counts[status]+=1
                if status=='unchanged_verified_copy_identity':verified_bytes+=task['entry']['size']
                else:issues.write(json.dumps(dict(id=task['id'],rel=task['entry']['rel'],status=status))+'\n')
            base.atomic(output/'progress.json',dict(status='checking',counts=dict(counts),
                expected=shard['files'],verified_bytes=verified_bytes,updated=time.time()))
        pending=[]
        with gzip.open(shard['path'],'rt') as stream:
            for line in stream:
                pending.append(json.loads(line))
                if len(pending)>=1024:consume(pending);pending=[]
            if pending:consume(pending)
    if counts['processed']!=shard['files']:raise ValueError('postcheck coverage differs')
    for name,digest in hashes.items():
        if base.digest(directory/name)!=digest:raise ValueError('closed receipts changed during postcheck')
    result=dict(status='verified_closed_shard_identities' if counts['unchanged_verified_copy_identity']==shard['files']
                else 'closed_shard_needs_reconciliation',counts=dict(counts),expected=shard['files'],
        verified_bytes=verified_bytes,actual_receiver_terminal=True,receiver=actor,receipt_hashes=hashes,
        started=started,updated=time.time(),payload_read=False,target_mutated=False,whole_tree_completion_proven=False)
    base.atomic(output/'result.json',result)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--plan',type=Path,required=True)
    run(parser.parse_args().plan)
