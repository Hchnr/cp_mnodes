#!/usr/bin/env python3
"""Read-only postcheck of closed ordinary-file shards and durable SHA receipts.

Does not read file payloads again: unchanged source/target identities retain
the already-recorded copy+readback SHA evidence. Changed identities are reported
for reconciliation, never treated as verified and never overwritten.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from pathlib import Path
import sys
import time

from migration_reaudit import atomic


def terminal(record):
    try:f=Path('/proc',str(record['pid']),'stat').read_text().rsplit(') ',1)[1].split()
    except FileNotFoundError:return True
    return f[19]!=record['start'] or f[0] in ('Z','X')


def run(control):
    control=Path(control).resolve();sys.path.insert(0,str(control/'code'))
    import migration_reaudit_copy as entry
    base=entry.base;cfg=base.read_json(control/'config.json')
    if cfg['job_kind']!='files':raise ValueError('ordinary-file control required')
    folder=control/'post-copy-audit';folder.mkdir(exist_ok=False)
    counts=collections.Counter();bytes_checked=0;issues=[];shards=[];started=time.time()
    with ThreadPoolExecutor(max_workers=32) as pool:
        for node,item in cfg['shards'].items():
            entry.validate(control,cfg,node)
            directory=control/'nodes'/node;state=base.read_json(directory/'progress.json')
            if not terminal(state) or not state['status'].startswith('complete') or state.get('active'):
                raise ValueError('shard not fully closed: '+node)
            receipts=base.load_receipts(directory);seen=set();tasks=[]
            def check(task):
                receipt=receipts.get(task['id'])
                if not receipt or receipt.get('rel')!=task['entry']['rel']:return task,'missing_receipt'
                if receipt.get('status') not in ('copied','skipped'):return task,'copy_attention'
                sha=receipt.get('sha256')
                if not isinstance(sha,str) or len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):
                    return task,'missing_sha_evidence'
                return task,'unchanged_verified_copy_identity' if base.resume_valid(cfg,task['entry'],receipt) else 'source_or_target_identity_changed'
            def flush():
                nonlocal bytes_checked
                for task,status in pool.map(check,tasks):
                    counts[status]+=1;counts['processed']+=1
                    if status=='unchanged_verified_copy_identity':bytes_checked+=task['entry']['size']
                    else:issues.append(dict(node=node,id=task['id'],rel=task['entry']['rel'],status=status))
                tasks.clear()
            with gzip.open(item['path'],'rt',encoding='ascii') as stream:
                for line in stream:
                    task=json.loads(line)
                    if task['id'] in seen:raise ValueError('duplicate shard ID')
                    seen.add(task['id']);tasks.append(task)
                    if len(tasks)>=1024:flush()
                if tasks:flush()
            if len(seen)!=item['files'] or set(receipts)!=seen:raise ValueError('receipt/shard exact coverage differs')
            shards.append(dict(node=node,files=len(seen),shard_sha256=item['sha256'],actual_process_terminal=True))
            atomic(folder/'progress.json',dict(status='checking',updated=time.time(),processed=counts['processed'],
                expected=cfg['files'],closed_shards=len(shards),counts=dict(counts)))
    if counts['processed']!=cfg['files']:raise ValueError('global shard coverage differs')
    result=dict(status='verified_closed_batch_identities' if not issues else 'closed_batch_needs_reconciliation',
        started=started,updated=time.time(),control=str(control),counts=dict(counts),verified_logical_bytes=bytes_checked,
        expected_files=cfg['files'],expected_bytes=cfg['bytes'],issues=issues,shards=shards,
        payload_read=False,target_mutated=False,whole_tree_completion_proven=False,
        hash_evidence='Durable SHA256+destination-readback copy receipts, source and target inode/size/mtime/ctime unchanged at postcheck.')
    atomic(folder/'result.json',result);return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True);a=p.parse_args()
    r=run(a.control);print(json.dumps({k:r[k] for k in ['status','counts','verified_logical_bytes']}),flush=True)
