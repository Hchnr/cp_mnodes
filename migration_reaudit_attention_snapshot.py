#!/usr/bin/env python3
"""Extract a bounded exact set of completed changed receipts for safe retry.

Read only the newline-closed prefix observed at open; append-only worker logs
remain untouched. Match every selected ID/path/size against its frozen shard.
This is not a full final receipt inventory, and it never dispatches copies.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import time


def changed_lines(path, excluded, limit):
    selected=[]
    with open(path,'rb') as stream:
        end=os.fstat(stream.fileno()).st_size
        while stream.tell()<end and len(selected)<limit:
            offset=stream.tell();line=stream.readline()
            if stream.tell()>end or not line.endswith(b'\n'):break
            if b'"changed"' not in line:continue
            receipt=json.loads(line)
            if receipt.get('status')!='changed' or receipt['id'] in excluded:continue
            selected.append(dict(receipt=receipt,original_receipt=str(path),byte_offset=offset,
                                 line_sha256=hashlib.sha256(line).hexdigest()))
    return selected


def match_tasks(path, events):
    wanted={e['receipt']['id']:e for e in events}
    if len(wanted)!=len(events):raise ValueError('duplicate completed receipt ID')
    matched=set()
    with gzip.open(path,'rt') as stream:
        for line in stream:
            task=json.loads(line);key=task['id']
            if key not in wanted:continue
            event=wanted[key];receipt=event['receipt']
            if (key in matched or receipt['rel']!=task['entry']['rel']
                    or receipt['expected_bytes']!=task['entry']['size']):
                raise ValueError('completed receipt differs from frozen task')
            event['task']=task;matched.add(key)
            if len(matched)==len(wanted):break
    if matched!=set(wanted):raise ValueError('completed task outside original shard')


def run(control,output,exclusions,limit=64):
    control=Path(control).resolve();output=Path(output).resolve()
    if not 1<=limit<=64:raise ValueError('bounded 1..64 snapshot required')
    sys.path.insert(0,str(control/'code'))
    import migration_reaudit_copy as entry
    base=entry.base;cfg=base.read_json(control/'config.json')
    if cfg['job_kind']!='files':raise ValueError('ordinary files only')
    if output.parent!=Path(cfg['audit_root'])/'final' or output.exists():
        raise ValueError('fresh shared final subfolder required')
    excluded=set();exclude_proofs=[]
    for path in exclusions:
        path=Path(path).resolve();summary=base.read_json(path)
        if (summary['control']!=str(control) or summary['config_sha256']!=base.digest(control/'config.json')
                or base.digest(summary['manifest'])!=summary['sha256']):
            raise ValueError('foreign or changed previous snapshot')
        with gzip.open(summary['manifest'],'rt') as stream:
            excluded.update(json.loads(line)['task']['id'] for line in stream)
        exclude_proofs.append(dict(path=str(path),sha256=base.digest(path)))
    events=[];seen=set(excluded);sources=[]
    for node in cfg['shards']:
        entry.validate(control,cfg,node)
        group=[]
        for path in sorted((control/'nodes'/node).glob('receipts-*.jsonl')):
            found=changed_lines(path,seen,limit-len(events)-len(group))
            for event in found:
                event['node']=node;seen.add(event['receipt']['id'])
            group.extend(found)
            if len(events)+len(group)==limit:break
        if group:match_tasks(cfg['shards'][node]['path'],group)
        sources.append(node);events.extend(group)
        if len(events)==limit:break
    if not events:
        summary=dict(status='no_new_completed_attention',control=str(control),created=time.time(),
            visited_nodes=sources,previous_snapshots=exclude_proofs,excluded_completed_ids=len(excluded),
            whole_tree_completion_proven=False)
        output.mkdir();base.atomic(output/'empty-snapshot.json',summary)
        print(json.dumps(summary),flush=True);return
    output.mkdir();manifest=output/'completed-attention-tasks.jsonl.gz'
    with gzip.open(manifest,'wt',encoding='ascii',compresslevel=1) as stream:
        for event in events:stream.write(json.dumps(event,separators=(',',':'))+'\n')
    with open(manifest,'rb') as stream:os.fsync(stream.fileno())
    summary=dict(status='bounded_completed_attention_snapshot',created=time.time(),control=str(control),
        config_sha256=base.digest(control/'config.json'),manifest=str(manifest),sha256=base.digest(manifest),
        completed_attention=len(events),original_bytes=sum(e['task']['entry']['size'] for e in events),
        previous_snapshots=exclude_proofs,visited_nodes=sources,full_attention_inventory=False,
        original_workers_untouched=True,whole_tree_completion_proven=False)
    base.atomic(output/'snapshot.json',summary);print(json.dumps(summary),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--exclude-snapshot',type=Path,action='append',default=[])
    p.add_argument('--limit',type=int,default=64);a=p.parse_args();run(a.control,a.output,a.exclude_snapshot,a.limit)
