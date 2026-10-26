#!/usr/bin/env python3
"""Validate every review output, export scope, and retain hard copy gates."""
import argparse
import collections
import fcntl
import gzip
import json
import os
from pathlib import Path
import sys
import time

from migration_reaudit import atomic
from migration_reaudit_shared import read,snapshot
from migration_reaudit_finish import connect,digest,safe_rel

STATUSES={'missing','present','metadata_only','conflict','unsupported','source_disappeared','error','blocked'}


def ingest(db,plan,result):
    slot=plan['slot'];output=Path(result['output'])
    if (result['status']!='complete' or result['input_sha256']!=plan['input_sha256']
            or result['counts']['processed']!=plan['records']
            or digest(output)!=result['output_sha256']):
        raise ValueError('review completion/integrity differs')
    prior=db.execute('SELECT input_sha256,output_sha256,rows FROM review_inputs WHERE slot=?',(slot,)).fetchone()
    if prior:
        if prior!=(plan['input_sha256'],result['output_sha256'],plan['records']):
            raise ValueError('immutable review input changed')
        return False
    counters=collections.Counter();batch=[]
    with db,gzip.open(output,'rt',encoding='ascii') as stream:
        for line in stream:
            if not line.endswith('\n'):raise ValueError('partial review output')
            row=json.loads(line);rel=row['rel'];raw=safe_rel(rel)
            status=row['status'];kind=row.get('kind')
            if status not in STATUSES:raise ValueError('unexpected review status')
            observation=row.get('observation')
            if status in ('missing','conflict','metadata_only'):
                if not observation or observation['rel']!=rel:raise ValueError('observation/path differs')
            size=0
            if status=='missing':
                if kind not in ('file','directory','symlink') or observation['reason']!='target_absent':
                    raise ValueError('unproven missing type')
                if kind=='file':
                    size=row['source']['size']
                    if type(size) is not int or size<0:raise ValueError('invalid logical size')
                    if observation['source']!=row['source']:raise ValueError('observation identity differs')
                counters['missing_'+{'file':'files','directory':'directories','symlink':'symlinks'}[kind]]+=1
                if kind=='file':counters['missing_logical_bytes']+=size
            counters['processed']+=1;counters[status]+=1
            changed=int(bool(row.get('directory_contents_changed')))
            if changed:counters['source_directory_changes']+=1
            batch.append((raw,status,kind,size,json.dumps(row,ensure_ascii=True,separators=(',',':')),
                json.dumps(observation,ensure_ascii=True,separators=(',',':')) if observation else None,
                row['checked_at'],changed))
            if len(batch)>=5000:
                db.executemany('INSERT INTO reviewed VALUES(?,?,?,?,?,?,?,?)',batch);batch.clear()
        if batch:db.executemany('INSERT INTO reviewed VALUES(?,?,?,?,?,?,?,?)',batch)
        if dict(counters)!=result['counts']:raise ValueError('review output counter mismatch')
        db.execute('INSERT INTO review_inputs VALUES(?,?,?,?)',
                   (slot,plan['input_sha256'],result['output_sha256'],counters['processed']))
    return True


def write_rows(path,rows):
    path=Path(path);tmp=path.with_suffix(path.suffix+'.part');count=0
    with open(tmp,'wb') as raw:
        with gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=1,mtime=0) as output:
            for row in rows:output.write((row+'\n').encode('ascii'));count+=1
        raw.flush();os.fsync(raw.fileno())
    tmp.replace(path)
    return dict(path=str(path),rows=count,bytes=path.stat().st_size,sha256=digest(path))


def minimal_scopes(paths):
    result=[]
    for rel in sorted(set(paths),key=os.fsencode):
        safe_rel(rel)
        if result and (rel==result[-1] or rel.startswith(result[-1]+'/')):continue
        result.append(rel)
    return result


def export(root,db,manifest):
    root=Path(root);folder=root/'final'
    total=db.execute('SELECT COUNT(*) FROM reviewed').fetchone()[0]
    missing=db.execute('SELECT COUNT(*) FROM candidates c WHERE NOT EXISTS(SELECT 1 FROM reviewed r WHERE r.rel=c.rel)').fetchone()[0]
    extra=db.execute('SELECT COUNT(*) FROM reviewed r WHERE NOT EXISTS(SELECT 1 FROM candidates c WHERE c.rel=r.rel)').fetchone()[0]
    if total!=manifest['records'] or missing or extra:raise ValueError('review coverage incomplete/foreign')
    expected=sum(p['records']>0 for p in manifest['plans'])
    if db.execute('SELECT COUNT(*) FROM review_inputs').fetchone()[0]!=expected:
        raise ValueError('not all review partitions merged')
    outputs={}
    for kind,plural in [('file','files'),('directory','directories'),('symlink','symlinks')]:
        outputs[plural]=write_rows(folder/('missing-'+plural+'.jsonl.gz'),
            (r[0] for r in db.execute("SELECT observation FROM reviewed WHERE status='missing' AND kind=? ORDER BY rel",(kind,))))
    outputs['attention']=write_rows(folder/'needs-attention.jsonl.gz',
        (r[0] for r in db.execute("SELECT raw FROM reviewed WHERE status IN ('conflict','error','blocked','unsupported') ORDER BY rel")))
    outputs['resolved']=write_rows(folder/'resolved-not-missing.jsonl.gz',
        (r[0] for r in db.execute("SELECT raw FROM reviewed WHERE status IN ('present','metadata_only','source_disappeared') ORDER BY rel")))
    scopes=minimal_scopes([os.fsdecode(r[0]) for r in db.execute('SELECT rel FROM reviewed WHERE directory_changed=1')])
    statuses=dict(db.execute('SELECT status,COUNT(*) FROM reviewed GROUP BY status'))
    absent={kind:dict(paths=n,logical_bytes=b) for kind,n,b in db.execute(
        "SELECT kind,COUNT(*),COALESCE(SUM(size),0) FROM reviewed WHERE status='missing' GROUP BY kind")}
    policy=read(root/'conditional-copy-policy.json')
    if policy['threshold_bytes_exclusive']!=50000000000000 or not policy['conditional_copy_authorized']:
        raise ValueError('conditional authorization differs')
    current=snapshot(root)
    if not current['enumeration_complete']:raise ValueError('primary scan no longer has closed coverage')
    size=absent.get('file',{}).get('logical_bytes',0)
    unresolved=statuses.get('error',0)+statuses.get('blocked',0)
    gates=dict(full_primary_enumeration=True,unique_candidate_review_complete=True,
        unresolved_access_paths=unresolved,changed_directory_scopes=len(scopes),
        below_threshold=size<policy['threshold_bytes_exclusive'],copy_started=False,
        final_frontier_and_ownership_audit_pending=True)
    result=dict(status='reviewed_manifest_needs_completion_audit',updated=time.time(),
        source='/share/project',target='/share-mixed/project',exclusions=['eai_pwm/datasets/raw'],
        coverage=dict(unique_reviewed_paths=total,missing_reviews=0,unexpected_paths=0,partitions=expected),
        missing=absent,statuses=statuses,outputs=outputs,copy_gates=gates,
        caveat='Online metadata observations, not an atomic snapshot or complete content-hash audit.')
    atomic(folder/'followup-required.json',dict(scopes=scopes,access_errors=unresolved))
    atomic(folder/'reviewed-manifest.json',result)
    atomic(root/'conditional-copy-state.json',dict(status='awaiting_completion_audit_or_scoped_followup',
        updated=time.time(),missing_bytes=size,threshold_bytes=policy['threshold_bytes_exclusive'],gates=gates))
    return result


def merge(root,watch=False):
    root=Path(root);db=None;lock=None
    try:
        while True:
            try:manifest=read(root/'final/revalidation/manifest.json')
            except FileNotFoundError:
                if not watch:raise
                time.sleep(20);continue
            if db is None:
                db,cache=connect(root)
                lock=open(cache/'merge.lock','a')
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            complete=True
            for plan in manifest['plans']:
                if not plan['records']:continue
                folder=root/'final/revalidation/nodes'/('dev-review-'+str(plan['slot']))
                try:result=read(folder/'report/result.json')
                except FileNotFoundError:complete=False;continue
                if result['status']!='complete':raise ValueError('candidate review failed: '+str(plan['slot']))
                if Path(result['output']).resolve()!=(folder/'report/observations.jsonl.gz').resolve():
                    raise ValueError('foreign review output')
                launch=read(folder/'launch.json')
                try:f=Path('/proc',str(launch['pid']),'stat').read_text().rsplit(') ',1)[1].split()
                except FileNotFoundError:f=None
                if f is not None and f[19]==launch['start'] and f[0] not in ('Z','X'):
                    complete=False;continue
                if ingest(db,plan,result):
                    atomic(root/'final/merge-state.json',dict(status='merging',updated=time.time(),
                        merged_partitions=db.execute('SELECT COUNT(*) FROM review_inputs').fetchone()[0],
                        unique_reviewed_paths=db.execute('SELECT COUNT(*) FROM reviewed').fetchone()[0]))
            if complete:
                result=export(root,db,manifest)
                atomic(root/'final/merge-state.json',dict(status='reviewed_manifest_needs_completion_audit',
                    updated=time.time(),coverage=result['coverage'],gates=result['copy_gates']))
                print(json.dumps(result),flush=True);return result
            if not watch:return
            time.sleep(20)
    finally:
        if db is not None:db.close()
        if lock is not None:lock.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control',type=Path,required=True);parser.add_argument('--watch',action='store_true')
    args=parser.parse_args();merge(args.control,args.watch)


if __name__=='__main__':main()
