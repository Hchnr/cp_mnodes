#!/usr/bin/env python3
"""Close explicit changed namespaces using a proven per-directory baseline.

Metadata only. No source payload opens, data copies, or whole-tree completion
claims. Unchanged old subtrees retain their original observation windows;
every known changed seed is processed independently, never hidden by a parent.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os
from pathlib import Path
import sys
import time

from migration_reaudit import atomic
from migration_reaudit_capture import capture_directory
from migration_reaudit_finish import digest,safe_rel
from migration_reaudit_incremental_plan import inside
from migration_reaudit_shared import read

VERSION=('ino','mode','mtime_ns','ctime_ns')


def same_version(a,b):return a is not None and b is not None and all(a.get(k)==b.get(k) for k in VERSION)


def walk(baseline,seeds,capture,absent,emit,progress,*,limit=10000):
    old_children=collections.defaultdict(list)
    for rel in baseline:old_children[rel.rpartition('/')[0]].append(rel)
    queue=collections.deque(seeds);queued=set(seeds);versions={};visits=collections.Counter();reused=set();pruned={}
    captures=0;entries=0
    def enqueue(rel):
        if rel not in queued:queue.append(rel);queued.add(rel)
    while queue:
        if captures>=limit:raise RuntimeError('bounded closure limit reached; pending directories remain')
        rel=queue.popleft();queued.remove(rel);visits[rel]+=1
        if visits[rel]>3:raise RuntimeError('directory keeps changing across bounded closure: '+rel)
        try:value=capture(rel)
        except FileNotFoundError:
            row=absent(rel)
            if row['status']!='source_disappeared':raise
            pruned[rel]=row;emit(dict(event='source_subtree_disappeared',rel=rel,observation=row))
            captures+=1;progress(captures,entries,len(queue),len(reused),len(pruned));continue
        if value['rel']!=rel:raise ValueError('capture scope mismatch')
        views={row['rel']:row for row in value['views']}
        expected={rel}|{rel+'/'+name for name in value['names']}
        if set(views)!=expected:raise ValueError('capture names/observations differ')
        versions[rel]=value['after'];captures+=1;entries+=len(views)
        emit(dict(event='stable_immediate_directory_capture',**value))
        current_dirs=set()
        for child,row in views.items():
            if child==rel:continue
            if row.get('kind')=='directory':
                current_dirs.add(child)
                source=row['source'];previous=versions.get(child,baseline.get(child))
                if child in queued:continue  # Never suppress another explicit seed.
                if same_version(previous,source):reused.add(child)
                else:enqueue(child)
        # A source directory removed or replaced by a non-directory proves its
        # old descendants no longer belong to that source subtree. Keep a
        # concrete current observation rather than silently dropping a branch.
        for child in old_children.get(rel,[]):
            if child in current_dirs:continue
            row=views.get(child)
            if row is None:row=absent(child)
            if row['status']=='source_disappeared' or (row.get('kind') and row['kind']!='directory'):
                pruned[child]=row;emit(dict(event='source_old_directory_removed_or_retyped',rel=child,observation=row))
            elif row.get('kind')=='directory':enqueue(child)
            else:raise ValueError('unresolved old source subtree: '+child)
        progress(captures,entries,len(queue),len(reused),len(pruned))
    return dict(captured_directories=captures,observations=entries,reused_directories=len(reused),
                pruned_source_subtrees=len(pruned),pending_directories=0,seed_directories=len(seeds))


def run(root):
    root=Path(root);folder=root/'final/incremental-namespace-closure';plan=read(folder/'plan.json')
    if plan['status']!='prepared' or plan['missing_baseline_directories']:raise ValueError('baseline incomplete')
    item=plan['baseline']
    if digest(item['path'])!=item['sha256']:raise ValueError('directory baseline changed')
    baseline={}
    with gzip.open(item['path'],'rt',encoding='ascii') as stream:
        for line in stream:
            row=json.loads(line);rel=row['rel'];safe_rel(rel)
            if not inside(rel,plan['scope']) or rel in baseline:raise ValueError('foreign/duplicate baseline directory')
            baseline[rel]=row['source']
    if len(baseline)!=plan['expected_closed_directories']:raise ValueError('baseline count differs')
    sys.path.insert(0,str(root/'final/tools'));sys.path.insert(0,str(root/'tools'))
    import cp_mnodes_readonly_scan_20261024 as scan
    import revalidate_readonly_candidates_20261025 as review
    output=folder/'runtime';output.mkdir(exist_ok=False);started=time.time();last=0
    def progress(captured,entries,pending,reused,pruned):
        nonlocal last
        if time.monotonic()-last<3:return
        atomic(folder/'runtime-progress.json',dict(status='closing_changed_namespaces',updated=time.time(),started=started,
            pid=os.getpid(),captured_directories=captured,observations=entries,pending_directories=pending,
            reused_directories=reused,pruned_source_subtrees=pruned,payload_read=False,data_copy_started=False))
        last=time.monotonic()
    def capture(rel):
        safe_rel(rel)
        if not inside(rel,plan['scope']):raise ValueError('scope escape')
        return capture_directory(scan,review,'/share/project','/share-mixed/project',rel)
    def absent(rel):
        with ThreadPoolExecutor(max_workers=2) as attributes:
            return review.inspect_group(dict(parent=rel.rpartition('/')[0],rows=[dict(rel=rel)]),
                Path('/share/project'),Path('/share-mixed/project'),attributes)[0]
    partial=output/'observations-and-proofs.jsonl.gz.part';last_sync=0
    try:
        with open(partial,'wb') as raw,gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=1,mtime=0) as compressed:
            def emit(row):
                nonlocal last_sync
                compressed.write((json.dumps(row,ensure_ascii=True,separators=(',',':'))+'\n').encode('ascii'))
                if time.monotonic()-last_sync>5:
                    compressed.flush();raw.flush();os.fsync(raw.fileno());last_sync=time.monotonic()
            result=walk(baseline,[r['rel'] for r in plan['changed_directory_seeds']],capture,absent,emit,progress)
            compressed.close();raw.flush();os.fsync(raw.fileno())
        final=output/'observations-and-proofs.jsonl.gz';partial.replace(final)
        result.update(status='changed_namespace_observations_closed_pending_overlay_review',updated=time.time(),
            started=started,scope=plan['scope'],baseline_sha256=item['sha256'],output=str(final),output_sha256=digest(final),
            whole_tree_completion_proven=False,payload_read=False,data_copy_started=False,
            next='Validate proof graph and inherited per-directory windows, merge new observations, resolve old candidate paths below pruned branches, and compare against immutable in-flight copy manifests before scheduling any extra missing paths.')
        atomic(output/'result.json',result);atomic(folder/'runtime-progress.json',result);return result
    except BaseException as exc:
        atomic(output/'result.json',dict(status='failed',updated=time.time(),error=repr(exc),payload_read=False,data_copy_started=False))
        raise


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True);a=p.parse_args()
    print(json.dumps(run(a.control)),flush=True)
