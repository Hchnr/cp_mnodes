#!/usr/bin/env python3
"""Freeze the completed-directory baseline for targeted namespace closure.

Preparation only: no payload opens, target mutation, or completion declaration.
Future reuse requires both closed-frontier membership and matching directory
version; unknown/replaced children must be traversed, never skipped.
"""
import argparse
import gzip
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from migration_reaudit import atomic
from migration_reaudit_finish import digest,safe_rel
from migration_reaudit_merge import write_rows
from migration_reaudit_shared import read

SCOPE='liyue/cosmos-framework/outputs/casa2500_jpeg26000_20261026_restart'


def inside(rel,scope):return rel==scope or rel.startswith(scope+'/')


def prepare(root):
    root=Path(root);prior=root/'final/namespace-closure-2';manifest=read(prior/'review-manifest.json')
    folder=root/'final/incremental-namespace-closure';folder.mkdir(exist_ok=False)
    seeds={};review_sources=[]
    for plan in manifest['plans']:
        output=prior/'reviews'/('slot-'+str(plan['slot']))/'report/result.json'
        result=read(output)
        if result['status']!='complete' or result['input_sha256']!=plan['input_sha256']:
            raise ValueError('unclosed/foreign review input')
        if digest(result['output'])!=result['output_sha256']:raise ValueError('review output changed')
        with gzip.open(result['output'],'rt',encoding='ascii') as stream:
            for line in stream:
                # The already-verified immutable review writer emits compact
                # JSON. Fast reject avoids parsing millions of unchanged rows.
                if '"directory_contents_changed":true' not in line:continue
                row=json.loads(line);rel=row['rel'];safe_rel(rel)
                if inside(rel,SCOPE):seeds[rel]=row['source']
        review_sources.append(dict(result=str(output),output_sha256=result['output_sha256']))
        atomic(folder/'progress.json',dict(status='collecting_changed_directory_seeds',updated=time.time(),
            reviews=len(review_sources),total_reviews=len(manifest['plans']),seeds=len(seeds)))
    if SCOPE not in seeds:raise ValueError('pending namespace root not present in reviewed changes')
    frontier=Path('/tmp/cp-mnodes-readonly-audit-20261026-full-0107-namespace-closure-2/slot-3/frontier.sqlite')
    fd=sqlite3.connect('file:'+str(frontier)+'?mode=ro',uri=True)
    try:
        coverage={os.fsencode(rel) for rel,status in fd.execute('SELECT rel,status FROM frontier')
                  if inside(rel,SCOPE) and status=='done'}
        unfinished=[rel for rel,status in fd.execute('SELECT rel,status FROM frontier')
                    if inside(rel,SCOPE) and status!='done']
    finally:fd.close()
    if unfinished or not coverage or os.fsencode(SCOPE) not in coverage:raise ValueError('recursive frontier not closed')
    expected=len(coverage);raw=safe_rel(SCOPE);missing=set(coverage);count=0
    db=sqlite3.connect('file:'+manifest['cache']+'?mode=ro',uri=True)
    def rows():
        nonlocal count
        for key,encoded in db.execute('SELECT rel,raw FROM reviewed WHERE kind=\'directory\' AND rel>=? AND rel<? ORDER BY rel',
                                      (raw,raw+b'0')):
            if key not in coverage:continue
            row=json.loads(encoded);source=row.get('source')
            if not source or not stat.S_ISDIR(source['mode']):raise ValueError('missing directory identity')
            rel=os.fsdecode(key)
            if not inside(rel,SCOPE):raise ValueError('foreign baseline path')
            missing.remove(key);count+=1
            yield json.dumps(dict(rel=rel,source=source,checked_at=row['checked_at']),ensure_ascii=True,separators=(',',':'))
    try:baseline=write_rows(folder/'directory-baseline.jsonl.gz',rows())
    finally:db.close()
    report=dict(status='prepared' if not missing else 'baseline_coverage_requires_review',updated=time.time(),
        scope=SCOPE,changed_directory_seeds=[dict(rel=k,source=seeds[k]) for k in sorted(seeds,key=os.fsencode)],
        baseline=baseline,expected_closed_directories=expected,baseline_directories=count,
        missing_baseline_directories=[os.fsdecode(k) for k in sorted(missing)],source_reviews=review_sources,
        original_frontier=str(frontier),data_copy_started=False,payload_read=False,
        whole_tree_completion_proven=False,
        next='Capture immediate entries for every seed (do not collapse to one coarse root). Reuse a child only with closed frontier membership and matching source version; recursively capture unknown or replaced child directories. Resolve removed branches explicitly. Keep the active copy report immutable.')
    atomic(folder/'plan.json',report);atomic(folder/'progress.json',dict(status=report['status'],updated=time.time(),
        directory_seeds=len(seeds),baseline_directories=count,missing_baseline_directories=len(missing)))
    return report


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True);a=p.parse_args()
    result=prepare(a.control);print(json.dumps({k:result[k] for k in ['status','expected_closed_directories','baseline_directories']}),flush=True)
