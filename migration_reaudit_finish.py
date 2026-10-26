#!/usr/bin/env python3
"""Index immutable shared findings and review every unique candidate read-only.

The local SQLite is a rebuildable index, never the sole copy of evidence.
Authoritative inputs, SHA manifests, plans, review outputs and progress remain
on mixed-flash storage. This program does not start payload copying.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import fcntl
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import time

from migration_reaudit import atomic
from migration_reaudit_shared import read, snapshot, returned


def safe_rel(rel, allow_empty=False):
    if (not isinstance(rel,str) or rel.startswith('/') or '\x00' in rel
            or (not rel and not allow_empty)
            or any(x in ('','.','..') for x in rel.split('/') if rel)):
        raise ValueError('unsafe relative path')
    if rel=='eai_pwm/datasets/raw' or rel.startswith('eai_pwm/datasets/raw/'):
        raise ValueError('excluded raw path')
    return os.fsencode(rel)


def digest(path):
    with open(path,'rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def connect(root):
    cache=Path('/tmp')/('cp-mnodes-reaudit-index-'+Path(root).name.removeprefix('.hcr-'))
    cache.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(cache/'index.sqlite',timeout=60)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=NORMAL')
    db.execute('PRAGMA cache_size=-262144')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS inputs(instance TEXT,category TEXT,path TEXT,
        sha256 TEXT,bytes INTEGER,rows INTEGER,updated REAL,PRIMARY KEY(instance,category));
      CREATE TABLE IF NOT EXISTS candidates(rel BLOB PRIMARY KEY,parent BLOB,kind TEXT,
        raw TEXT,observed REAL,instance TEXT,category TEXT);
      CREATE TABLE IF NOT EXISTS exceptions(rel BLOB,operation TEXT,raw TEXT,
        PRIMARY KEY(rel,operation));
      CREATE TABLE IF NOT EXISTS reviewed(rel BLOB PRIMARY KEY,status TEXT,kind TEXT,size INTEGER,
        raw TEXT,observation TEXT,checked_at REAL,directory_changed INTEGER);
      CREATE INDEX IF NOT EXISTS reviewed_status_kind ON reviewed(status,kind);
      CREATE TABLE IF NOT EXISTS review_inputs(slot INTEGER PRIMARY KEY,input_sha256 TEXT,
        output_sha256 TEXT,rows INTEGER);
    ''')
    return db,cache


def ingest(db,record,category,result):
    instance=record['instance'];path=Path(record['report_directory'])/(category+'.jsonl')
    prior=db.execute('SELECT sha256,bytes,rows FROM inputs WHERE instance=? AND category=?',
                     (instance,category)).fetchone()
    if prior:
        if path.stat().st_size!=prior[1]:raise ValueError('closed input size changed')
        return False
    before=path.stat();hasher=hashlib.sha256();rows=0;batch=[];exception_rows=[]
    statement='''INSERT INTO candidates VALUES(?,?,?,?,?,?,?) ON CONFLICT(rel) DO UPDATE SET
      parent=excluded.parent,kind=excluded.kind,raw=excluded.raw,observed=excluded.observed,
      instance=excluded.instance,category=excluded.category WHERE excluded.observed>candidates.observed'''
    def flush():
        if batch:db.executemany(statement,batch);batch.clear()
        if exception_rows:db.executemany('INSERT OR REPLACE INTO exceptions VALUES(?,?,?)',exception_rows);exception_rows.clear()
    with db,open(path,'rb') as stream:
        for line in stream:
            if not line.endswith(b'\n'):raise ValueError('partial closed input')
            hasher.update(line);row=json.loads(line);rel=row.get('rel','')
            raw=safe_rel(rel,allow_empty=category=='errors')
            if not rel:raise ValueError('unresolved root/controller error requires inspection')
            encoded=json.dumps(row,ensure_ascii=True,separators=(',',':'))
            batch.append((raw,os.fsencode(rel.rpartition('/')[0]),row.get('kind'),encoded,
                          result['updated'],instance,category))
            if category=='errors':
                operation=row.get('operation',row.get('reason','unknown'))
                exception_rows.append((raw,operation,encoded))
            rows+=1
            if len(batch)>=5000:flush()
        flush()
        after=path.stat()
        if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
            raise ValueError('closed source changed during indexing')
        expected={'missing':sum(result['counts'].get('missing_'+name,0) for name in ('files','directories','symlinks')),
                  'conflicts':sum(result['counts'].get(name,0) for name in ('size_mismatches','type_conflicts','symlink_mismatches')),
                  'errors':result['counts'].get('errors',0)+result['counts'].get('changed_source_directories',0)+result['counts'].get('unsupported',0)}
        if rows!=expected[category]:raise ValueError('closed finding counter mismatch: '+instance+'/'+category)
        db.execute('INSERT INTO inputs VALUES(?,?,?,?,?,?,?)',
                   (instance,category,str(path),hasher.hexdigest(),after.st_size,rows,result['updated']))
    return True


def index_state(root,db,view,cache):
    inputs=[dict(instance=i,category=c,path=p,sha256=h,bytes=b,rows=n,updated=t)
            for i,c,p,h,b,n,t in db.execute('SELECT * FROM inputs ORDER BY instance,category')]
    state=dict(updated=time.time(),status='indexing_closed_reports',cache=str(cache),
        indexed_inputs=len(inputs),registered_scanners=len(view['nodes']),
        returned_scanners=sum(r.get('run_returned',False) for r in view['nodes']),
        enumeration_complete=view['enumeration_complete'],
        unique_candidates=db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0],
        distinct_exceptions=db.execute('SELECT COUNT(*) FROM exceptions').fetchone()[0],inputs=inputs)
    if view['enumeration_complete'] and len(inputs)==3*len(view['nodes']):
        state['status']='indexed_all_closed_reports'
    atomic(Path(root)/'final/index-state.json',state)
    return state


def index(root,watch=False):
    root=Path(root);(root/'final').mkdir(exist_ok=True)
    db,cache=connect(root)
    lock=open(cache/'index.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    try:
        while True:
            view=snapshot(root,closed_only=True)
            registry=read(root/'launch-results.json')
            records={r['instance']:r for r in registry['nodes']}
            for row in view['nodes']:
                if not row.get('run_returned'):continue
                for category in ('missing','conflicts','errors'):
                    if ingest(db,records[row['instance']],category,row['report']):
                        print(json.dumps(dict(indexed=row['instance'],category=category,at=time.time())),flush=True)
                        index_state(root,db,view,cache)
            state=index_state(root,db,view,cache)
            print(json.dumps({k:v for k,v in state.items() if k!='inputs'}),flush=True)
            if state['status']=='indexed_all_closed_reports':return state
            if not watch:return state
            time.sleep(20)
    finally:
        db.close();lock.close()


def plan_review(root,slots=24):
    root=Path(root);state=read(root/'final/index-state.json')
    view=snapshot(root,closed_only=True)
    if state['status']!='indexed_all_closed_reports' or not view['enumeration_complete']:
        raise ValueError('full scan and closed input coverage not established')
    db,cache=connect(root)
    if db.execute('SELECT COUNT(*) FROM inputs').fetchone()[0]!=3*len(view['nodes']):
        raise ValueError('not all input reports indexed')
    folder=root/'final/revalidation';inputs=folder/'inputs';inputs.mkdir(parents=True,exist_ok=False)
    streams=[gzip.open(inputs/('slot-'+str(i)+'.jsonl.gz'),'wt',encoding='ascii',compresslevel=1) for i in range(slots)]
    counts=[0]*slots;tasks=[0]*slots;parent=None;group=[]
    def emit():
        nonlocal group
        if not group:return
        slot=min(range(slots),key=lambda i:counts[i])
        streams[slot].write(json.dumps(dict(parent=parent,rows=group),ensure_ascii=True,separators=(',',':'))+'\n')
        counts[slot]+=len(group);tasks[slot]+=1;group=[]
    try:
        for raw,encoded in db.execute('SELECT rel,raw FROM candidates ORDER BY rel'):
            row=json.loads(encoded);current=row['rel'].rpartition('/')[0]
            if group and (current!=parent or len(group)>=256):emit()
            parent=current;group.append(row)
        emit()
    finally:
        for stream in streams:stream.close()
    plans=[]
    for slot in range(slots):
        path=inputs/('slot-'+str(slot)+'.jsonl.gz')
        with open(path,'rb') as stream:os.fsync(stream.fileno())
        plan=dict(source='/share/project',target='/share-mixed/project',exclusions=['eai_pwm/datasets/raw'],
                  host=socket.gethostname(),node='dev-review-'+str(slot),slot=slot,round=1,
                  input=str(path),input_sha256=digest(path),records=counts[slot],tasks=tasks[slot])
        atomic(inputs/('slot-'+str(slot)+'.json'),plan);plans.append(plan)
    expected=db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0];db.close()
    if sum(counts)!=expected:raise ValueError('review partition incomplete')
    manifest=dict(created=time.time(),records=expected,slots=slots,plans=plans,
                  closed_inputs=state['inputs'],source_scope='fresh full-tree scan, raw excluded')
    atomic(folder/'manifest.json',manifest)
    return manifest


def review_worker(root,slot):
    root=Path(root);folder=root/'final/revalidation'
    for name,expected in read(root/'final/tools/source-sha256.json').items():
        if Path(name).name!=name or digest(root/'final/tools'/name)!=expected:
            raise ValueError('frozen review bundle changed')
    plan=read(folder/'inputs'/('slot-'+str(slot)+'.json'))
    if plan['host']!=socket.gethostname():raise ValueError('review host changed')
    if (plan['source'],plan['target'],plan['exclusions'])!=('/share/project','/share-mixed/project',['eai_pwm/datasets/raw']):
        raise ValueError('review scope changed')
    if not Path(plan['input']).resolve().is_relative_to((folder/'inputs').resolve()):
        raise ValueError('foreign review input')
    sys.path.insert(0,str(root/'final/tools'));sys.path.insert(0,str(root/'tools'))
    # The frozen review template imports ONLY the run-local ROOT/atomic shim.
    import revalidate_readonly_candidates_20261025 as review
    output=folder/'nodes'/('dev-review-'+str(slot))/'report'
    result=review.run_review(plan,output,threads=32,stat_threads=64)
    print(json.dumps(result),flush=True)


def launch_reviews(root):
    root=Path(root);manifest=read(root/'final/revalidation/manifest.json')
    launches=[]
    for plan in manifest['plans']:
        if not plan['records']:continue
        slot=plan['slot'];folder=root/'final/revalidation/nodes'/('dev-review-'+str(slot));folder.mkdir(parents=True,exist_ok=True)
        if (folder/'launch.json').exists():raise ValueError('uncertain review launch; inspect before retry')
        command=[sys.executable,'-B','-u',str(root/'final/tools/migration_reaudit_finish.py'),
                 'review-worker','--control',str(root),'--slot',str(slot)]
        with open(folder/'review.log','ab',buffering=0) as log:
            child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        fields=Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()
        record=dict(slot=slot,pid=child.pid,start=fields[19],argv=command,at=time.time())
        atomic(folder/'launch.json',record);launches.append(record)
    atomic(root/'final/revalidation/launch-results.json',dict(launches=launches,at=time.time()))
    return launches


def pipeline(root):
    root=Path(root)
    index(root,watch=True)
    plan_review(root)
    launches=launch_reviews(root)
    atomic(root/'final/pipeline-state.json',dict(status='candidate_review_running',at=time.time(),launches=launches))
    print(json.dumps(dict(status='candidate_review_running',processes=len(launches))),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('index','plan-review','review-worker','launch-reviews','pipeline'))
    parser.add_argument('--control',type=Path,required=True)
    parser.add_argument('--watch',action='store_true');parser.add_argument('--slot',type=int)
    args=parser.parse_args()
    if args.command=='index':index(args.control,args.watch)
    elif args.command=='plan-review':plan_review(args.control)
    elif args.command=='review-worker':review_worker(args.control,args.slot)
    elif args.command=='launch-reviews':launch_reviews(args.control)
    else:pipeline(args.control)


if __name__=='__main__':main()
