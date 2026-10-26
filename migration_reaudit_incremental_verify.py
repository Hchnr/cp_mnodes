#!/usr/bin/env python3
"""Replay namespace proof decisions and identify non-overlapping late findings."""
import argparse
import collections
import gzip
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from migration_reaudit import atomic
from migration_reaudit_finish import digest,safe_rel
from migration_reaudit_incremental import VERSION,walk
from migration_reaudit_incremental_plan import inside
from migration_reaudit_merge import write_rows
from migration_reaudit_shared import read


def validate_capture(event,scope):
    rel=event['rel'];safe_rel(rel)
    if not inside(rel,scope) or event.get('payload_read') is not False:raise ValueError('invalid capture scope')
    if any(event['before'][k]!=event['after'][k] for k in VERSION):raise ValueError('unstable captured directory')
    views=event['views'];names=event['names'];expected={rel}|{rel+'/'+name for name in names}
    if len(set(names))!=len(names) or len(views)!=len(expected) or {r['rel'] for r in views}!=expected:
        raise ValueError('incomplete directory nameset')
    roots=[r for r in views if r['rel']==rel]
    if len(roots)!=1 or roots[0].get('kind')!='directory':raise ValueError('missing directory root observation')
    if any(roots[0]['source'][k]!=event['before'][k] for k in VERSION):raise ValueError('root source binding differs')
    if set(event['children'])!={r['rel'] for r in views if r['rel']!=rel and r.get('kind')=='directory'}:
        raise ValueError('child directory omitted from proof')
    for row in views:
        safe_rel(row['rel'])
        if row['status'] in ('error','blocked'):raise ValueError('capture retained unresolved access error')
        if row['status']=='missing':
            obs=row['observation'];kind=row['kind'];source=row['source']
            if obs['rel']!=row['rel'] or obs['kind']!=kind or obs['source']!=source or obs['reason']!='target_absent':
                raise ValueError('unproven missing observation')
            if kind=='file' and (not stat.S_ISREG(source['mode']) or type(source['size']) is not int or source['size']<0):
                raise ValueError('invalid ordinary file')
            if kind not in ('file','directory','symlink'):raise ValueError('invalid missing kind')
            if kind=='directory' and not stat.S_ISDIR(source['mode']):raise ValueError('invalid directory mode')
            if kind=='symlink' and (not stat.S_ISLNK(source['mode']) or not isinstance(obs.get('link_target'),str)):
                raise ValueError('invalid symlink')


def replay(baseline,seeds,events,scope):
    index=0;by_path=collections.defaultdict(list)
    for event in events:
        if event['event']=='stable_immediate_directory_capture':
            validate_capture(event,scope);by_path[event['rel']].append(event)
        elif event['event'] not in ('source_subtree_disappeared','source_old_directory_removed_or_retyped'):
            raise ValueError('unrecognized proof event')
    def capture(rel):
        if index>=len(events) or events[index]['rel']!=rel:raise ValueError('missing required directory capture: '+rel)
        event=events[index]
        if event['event']=='source_subtree_disappeared':raise FileNotFoundError(rel)
        if event['event']!='stable_immediate_directory_capture':raise ValueError('foreign directory event')
        return {k:v for k,v in event.items() if k!='event'}
    def absent(rel):
        if index<len(events) and events[index]['rel']==rel and events[index]['event']!='stable_immediate_directory_capture':
            row=events[index]['observation']
            if row['rel']!=rel:raise ValueError('absence proof path mismatch')
            if row['status']=='source_disappeared' and row.get('errno')!=2:raise ValueError('unproved source absence')
            return row
        # A directory that reappeared between the stable listing and its old
        # child check must have an actual later captured subtree, not a skip.
        if rel not in by_path:raise ValueError('unresolved old subtree: '+rel)
        return next(r for r in by_path[rel][0]['views'] if r['rel']==rel)
    def emit(event):
        nonlocal index
        if index>=len(events) or event!=events[index]:raise ValueError('proof order/coverage replay differs')
        index+=1
    result=walk(baseline,seeds,capture,absent,emit,lambda *args:None)
    if index!=len(events):raise ValueError('unconsumed/foreign proof event')
    return result


def verify(root):
    root=Path(root);folder=root/'final/incremental-namespace-closure';plan=read(folder/'plan.json');result=read(folder/'runtime/result.json')
    if result['status']!='changed_namespace_observations_closed_pending_overlay_review' or result['pending_directories']:
        raise ValueError('namespace walker did not close')
    launch=read(folder/'run-launch.json')
    try:fields=Path('/proc',str(launch['pid']),'stat').read_text().rsplit(') ',1)[1].split()
    except FileNotFoundError:fields=None
    if fields and fields[19]==launch['start'] and fields[0] not in ('Z','X'):raise ValueError('walker still live')
    source_hashes=read(root/'final/incremental-run-tools-v1/source-sha256.json')
    if digest(Path(__file__).with_name('migration_reaudit_incremental.py'))!=source_hashes['migration_reaudit_incremental.py']:
        raise ValueError('replay traversal implementation differs from actual walker')
    for name,sha in source_hashes.items():
        if digest(root/'final/incremental-run-tools-v1'/name)!=sha:raise ValueError('walker code changed')
    if digest(plan['baseline']['path'])!=plan['baseline']['sha256'] or digest(result['output'])!=result['output_sha256']:
        raise ValueError('proof/baseline input changed')
    baseline={}
    with gzip.open(plan['baseline']['path'],'rt',encoding='ascii') as stream:
        for row in map(json.loads,stream):
            rel=row['rel'];safe_rel(rel)
            if not inside(rel,plan['scope']) or rel in baseline or not stat.S_ISDIR(row['source']['mode']):
                raise ValueError('invalid baseline directory')
            baseline[rel]=row['source']
    if len(baseline)!=plan['expected_closed_directories']:raise ValueError('baseline coverage differs')
    with gzip.open(result['output'],'rt',encoding='ascii') as stream:events=list(map(json.loads,stream))
    proof=replay(baseline,[r['rel'] for r in plan['changed_directory_seeds']],events,plan['scope'])
    if any(proof[k]!=result[k] for k in proof):raise ValueError('closure counters differ from replay')
    current={};pruned=[]
    for event in events:
        rows=event['views'] if event['event']=='stable_immediate_directory_capture' else [event['observation']]
        if event['event']!='stable_immediate_directory_capture':pruned.append(event)
        for row in rows:
            prior=current.get(row['rel'])
            if prior is None or row['checked_at']>prior['checked_at']:current[row['rel']]=row
    output=folder/'verified';output.mkdir(exist_ok=False)
    observations=write_rows(output/'latest-observations.jsonl.gz',
        (json.dumps(current[k],ensure_ascii=True,separators=(',',':')) for k in sorted(current,key=os.fsencode)))
    oldroot=root/'final/namespace-closure-2';manifest=read(oldroot/'review-manifest.json')
    main_path=Path('/tmp')/('cp-mnodes-reaudit-index-'+root.name.removeprefix('.hcr-'))/'index.sqlite'
    main=sqlite3.connect('file:'+str(main_path)+'?mode=ro',uri=True)
    overlay=sqlite3.connect('file:'+manifest['cache']+'?mode=ro',uri=True)
    new=[];inflight=collections.Counter();deferred=[]
    try:
        for rel,row in current.items():
            if row['status']!='missing':continue
            key=safe_rel(rel);old=overlay.execute('SELECT status,kind,raw FROM reviewed WHERE rel=?',(key,)).fetchone()
            if old is None:old=main.execute('SELECT status,kind,raw FROM reviewed WHERE rel=?',(key,)).fetchone()
            if old and old[0]=='missing':
                # Both original fixed queues are already in flight. Defer type
                # changes too; never race a new file against an old namespace task.
                inflight[row['kind']]+=1
                if old[1]!=row['kind'] or json.loads(old[2]).get('source')!=row.get('source'):deferred.append(row)
            else:new.append(row['observation'])
    finally:main.close();overlay.close()
    outputs={};totals={}
    for kind,plural in [('file','files'),('directory','directories'),('symlink','symlinks')]:
        rows=[r for r in new if r['kind']==kind];rows.sort(key=lambda r:os.fsencode(r['rel']))
        outputs[plural]=write_rows(output/('new-missing-'+plural+'.jsonl.gz'),
            (json.dumps(r,ensure_ascii=True,separators=(',',':')) for r in rows))
        totals[kind]=dict(paths=len(rows),logical_bytes=sum(r['source']['size'] for r in rows) if kind=='file' else 0)
    outputs['deferred']=write_rows(output/'inflight-source-changes.jsonl.gz',
        (json.dumps(r,ensure_ascii=True,separators=(',',':')) for r in sorted(deferred,key=lambda r:os.fsencode(r['rel']))))
    report=dict(status='namespace_proof_replayed_new_findings_classified',updated=time.time(),scope=plan['scope'],proof=proof,
        baseline_directories=len(baseline),source_proof_sha256=result['output_sha256'],observations=observations,
        nonoverlapping_new_missing=totals,already_inflight=dict(inflight),inflight_source_changes=len(deferred),
        pruned_subtrees=pruned,outputs=outputs,whole_tree_completion_proven=False,data_copy_started=False,
        remaining='Resolve old candidates under pruned source branches; finish original copies and recheck deferred source changes; independently authorize the non-overlapping new manifests without changing the original frozen report.')
    atomic(output/'report.json',report);return report


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True);a=p.parse_args()
    r=verify(a.control);print(json.dumps({k:r[k] for k in ['status','nonoverlapping_new_missing','already_inflight','inflight_source_changes']}),flush=True)
