#!/usr/bin/env python3
"""Exact-prefix evidence for a separately approved, one-shard host handoff.

No signals, worker launches, payload operations, or immutable config edits.
An old worker must have returned, drained every submitted task and persisted
exact prefix receipts before a receiver may skip that prefix. Post-copy
identity revalidation remains necessary; this is not a completeness proof.
"""
import collections
import gzip
import json
from pathlib import Path


def verify_prefix(shard, receipts, state):
    if state.get('active') or state.get('status') not in ('stopped','complete','complete_with_attention'):
        raise ValueError('source worker is not drained and closed')
    count=state['counts']['processed']
    if type(count) is not int or count<0 or state.get('submitted')!=count:
        raise ValueError('submitted/processed prefix differs')
    if len(receipts)!=count:raise ValueError('receipt coverage differs from closed prefix')
    expected=collections.Counter();ids=set();logical=0
    with gzip.open(shard,'rt',encoding='ascii') as stream:
        for number,line in enumerate(stream):
            if number==count:break
            task=json.loads(line);identity=task['id'];entry=task['entry']
            if identity in ids:raise ValueError('duplicate prefix task ID')
            ids.add(identity)
            receipt=receipts.get(identity)
            if not receipt or receipt.get('rel')!=entry['rel'] or receipt.get('id')!=identity:
                raise ValueError('prefix receipt/task identity differs')
            status=receipt.get('status')
            if status not in ('copied','skipped','changed','error','conflict'):
                raise ValueError('unsupported receipt outcome')
            if receipt.get('expected_bytes')!=entry['size']:
                raise ValueError('receipt logical size differs')
            expected['processed']+=1;expected[status]+=1;logical+=entry['size']
            if status in ('copied','skipped'):
                expected['completed_bytes']+=entry['size'];expected[status+'_bytes']+=entry['size']
            else:expected['attention_bytes']+=entry['size']
            if receipt.get('resumed'):expected['resumed']+=1
            if receipt.get('metadata_warnings'):expected['metadata_warnings']+=1
    if ids!=set(receipts):raise ValueError('receipts are not exactly the submitted manifest prefix')
    normalize=lambda v:{k:n for k,n in v.items() if n!=0}
    if normalize(expected)!=normalize(state['counts']):raise ValueError('closed cumulative counters differ')
    return dict(prefix_tasks=count,prefix_logical_bytes=logical,counts=dict(state['counts']),
                exact_prefix_receipt_coverage=True,post_copy_revalidation_required=True)


def process_terminal(record, proc_root=Path('/proc')):
    try:fields=(Path(proc_root)/str(record['pid'])/'stat').read_text().rsplit(') ',1)[1].split()
    except FileNotFoundError:return True
    return fields[19]!=record['start'] or fields[0] in ('Z','X')
