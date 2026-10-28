#!/usr/bin/env python3
"""Split a drained metadata frontier, retaining its completed evidence.

The logical owner remains one of the original 48 shards. Its proxy reports
the drained baseline plus disjoint children; final publication requires every
child's post-close footer, a closed frontier and exact pending-unit coverage.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import heapq
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time

import migration_global_check as scan


CATEGORIES=('missing','conflicts','metadata','errors','source_links')


def digest(path):
    with Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def unit_digest(units):
    h=hashlib.sha256();previous=None;recursive=None
    for row in units:
        rel=row['rel'];leaves=row['leaves_only']
        if not isinstance(rel,str) or rel.startswith('/') or any(x in ('.','..','') for x in rel.split('/') if rel):
            raise ValueError('unsafe pending path')
        if type(leaves) is not bool or (previous is not None and rel<=previous):
            raise ValueError('pending units unordered/duplicated')
        if rel=='eai_pwm/datasets/raw' or rel.startswith('eai_pwm/datasets/raw/'):
            raise ValueError('raw exclusion violated')
        if recursive is not None and (recursive=='' or rel.startswith(recursive+'/')):
            raise ValueError('overlapping recursive pending units')
        recursive=rel if not leaves else None
        h.update((json.dumps([rel,leaves],ensure_ascii=True,separators=(',',':'))+'\n').encode())
        previous=rel
    return h.hexdigest()


def stopped(root,record):
    folder=Path(record['report']).parent
    request=scan.read(folder/'handoff-request-v1.json')
    assert request['old_pid']==record['pid'] and request['argv']==record['argv']
    result=scan.read(Path(record['report'])/'result.json')
    with (folder/'scan.log').open('rb') as stream:
        stream.seek(max(0,(folder/'scan.log').stat().st_size-1024*1024))
        footer=json.loads(stream.read().splitlines()[-1])
    if result!=footer or result['status']!='stopped' or result['active_directories']!=0 or result['pid']!=record['pid']:
        raise ValueError('owner not fully drained and fsynced')
    if any(result[k] for k in ('payload_bytes_read','payload_bytes_written','copy_tasks_published')):
        raise ValueError('non-readonly baseline')
    return result,request


def prepare(root,node,lane,destinations):
    scan.check_root(root)
    registry=scan.read(root/'launch-results.json')
    record=next(r for r in registry['records'] if (r['node'],r['lane'])==(node,lane))
    if 'balance' in record:raise ValueError('logical owner already balanced')
    if len(set(destinations))!=len(destinations) or not 2<=len(destinations)<=8:
        raise ValueError('2–8 distinct destination nodes required')
    if any(n not in scan.NODES or n==node for n in destinations):raise ValueError('invalid destination')
    baseline,request=stopped(root,record)
    if socket.gethostname()!='job-'+scan.CLUSTER+'-'+node:
        raise ValueError('prepare on the drained owner node')
    try:fields=Path('/proc',str(record['pid']),'stat').read_text().rsplit(') ',1)[1].split()
    except FileNotFoundError:fields=None
    if fields and fields[19]==request['old_start'] and fields[0] not in ('Z','X'):
        raise ValueError('old owner still alive')
    owner=node+'-'+str(lane);folder=root/'balance'/owner;folder.mkdir(parents=True,exist_ok=False)
    db=sqlite3.connect((Path(record['report'])/'frontier.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)
    try:
        if db.execute("SELECT 1 FROM frontier WHERE status NOT IN ('done','pending') LIMIT 1").fetchone():
            raise ValueError('unsettled old frontier')
        units=[dict(rel=r,leaves_only=bool(l)) for r,l in db.execute(
            "SELECT rel,leaves_only FROM frontier WHERE status='pending' ORDER BY rel")]
    finally:db.close()
    if len(units)!=baseline['pending_directories'] or not units:raise ValueError('pending checkpoint count differs/empty')
    signature=unit_digest(units)
    parent_plan=Path(record['report']).parent/'plan.json';original=scan.read(parent_plan)
    plans=[]
    for i,dest in enumerate(destinations):
        sub=folder/('part-'+str(i));sub.mkdir()
        own=dict(original,host='job-'+scan.CLUSTER+'-'+dest,units=units[i::len(destinations)],
            purpose='disjoint drained-frontier continuation',logical_owner=owner)
        path=sub/'plan.json';scan.atomic(path,own)
        plans.append(dict(slot=i,node=dest,path=str(path),sha256=digest(path),units=len(own['units']),
            report=str(root/'nodes'/('balance-'+owner)/str(i)/'report'),log_file=str(sub/'scan.log')))
    manifest=dict(owner=owner,node=node,lane=lane,original=record,baseline=baseline,
        parent_plan=str(parent_plan),parent_plan_sha256=digest(parent_plan),pending_units=len(units),
        pending_sha256=signature,plans=plans,created=time.time(),source='/share/project',target='/share-mixed/project',
        exclusions=['eai_pwm/datasets/raw'],data_copy_started=False)
    scan.atomic(folder/'manifest.json',manifest)
    return manifest


def child_plan(root,manifest,slot):
    spec=next(p for p in manifest['plans'] if p['slot']==slot)
    if digest(spec['path'])!=spec['sha256']:raise ValueError('child plan changed')
    plan=scan.read(Path(spec['path']))
    if (plan['source'],plan['target'],plan['exclusions'])!=('/share/project','/share-mixed/project',['eai_pwm/datasets/raw']):
        raise ValueError('child scope differs')
    return spec,plan


def worker(root,manifest_path,slot):
    manifest=scan.read(manifest_path);spec,plan=child_plan(root,manifest,slot)
    if plan['host']!=socket.gethostname():raise ValueError('wrong child node')
    for name,sha in scan.read(root/'tools-balance-v1-sha256.json').items():
        if digest(root/'tools-balance-v1'/name)!=sha:raise ValueError('child engine changed')
    import migration_global_resume as engine
    class Fresh(engine.ResumeAudit):
        def __init__(self):
            engine.original.Audit.__init__(self,plan['source'],plan['target'],Path(spec['report']),threads=32,stat_threads=64)
            self.pending=len(plan['units'])
            db=sqlite3.connect(self.frontier)
            try:
                db.execute('PRAGMA journal_mode=DELETE');db.execute('PRAGMA synchronous=FULL')
                db.execute('CREATE TABLE frontier(rel TEXT PRIMARY KEY,leaves_only INTEGER,status TEXT)')
                db.execute('CREATE INDEX frontier_status ON frontier(status)')
                db.executemany("INSERT INTO frontier VALUES(?,?,'pending')",[(r['rel'],int(r['leaves_only'])) for r in plan['units']])
                db.commit()
            finally:db.close()
    engine.original.scanner.COVERED_ROOTS.update(plan['coverage_roots'])
    os.nice(10);audit=Fresh()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:audit.stop.set())
    audit.progress('scanning',audit.pending,0)
    print(json.dumps(audit.run()),flush=True)


START='''
import hashlib,json,os,pathlib,socket,subprocess,sys,time
root=pathlib.Path(ROOT);tools=root/'tools-balance-v1';spec=SPEC
assert socket.gethostname()=='job-'+CLUSTER+'-'+spec['node']
assert os.stat('/share/project').st_ino==69783039777 and os.stat('/share-mixed/project').st_ino==3706574426
folder=pathlib.Path(spec['path']).parent
with (folder/'launch-claim.json').open('x') as f:
 json.dump(dict(at=time.time()),f);f.flush();os.fsync(f.fileno())
for name,sha in json.loads((root/'tools-balance-v1-sha256.json').read_text()).items():
 assert hashlib.sha256((tools/name).read_bytes()).hexdigest()==sha
argv=[sys.executable,'-S','-B','-u',str(tools/'migration_global_balance.py'),'worker',
 '--root',str(root),'--manifest',MANIFEST,'--slot',str(spec['slot'])]
with pathlib.Path(spec['log_file']).open('xb',buffering=0) as log:
 child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,cwd=str(tools))
record=dict(spec,pid=child.pid,argv=argv,at=time.time())
with (folder/'launch.json').open('x') as f:
 json.dump(record,f);f.flush();os.fsync(f.fileno())
print(json.dumps(record))
'''


def launch(root,manifest_path):
    manifest=scan.read(manifest_path);folder=manifest_path.parent
    with (folder/'launch-claim.json').open('x') as f:
        json.dump(dict(at=time.time()),f);f.flush();os.fsync(f.fileno())
    state=dict(records=[],errors=[])
    scan.atomic(folder/'launch-results.json',state)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(scan.remote,s['node'],'ROOT='+repr(str(root))+'\nSPEC='+repr(s)+'\nCLUSTER='+repr(scan.CLUSTER)+'\nMANIFEST='+repr(str(manifest_path))+'\n'+START):s for s in manifest['plans']}
        for future in as_completed(futures):
            try:state['records'].append(future.result())
            except Exception as exc:state['errors'].append(dict(slot=futures[future]['slot'],error=str(exc),uncertain=True))
            scan.atomic(folder/'launch-results.json',state)
    return state


def report(root,manifest,records,last_seen=None):
    if last_seen is None:last_seen={}
    counts=collections.Counter(manifest['baseline']['counts'])
    groups=collections.defaultdict(collections.Counter,{k:collections.Counter(v) for k,v in manifest['baseline']['groups'].items()})
    pending=active=0;closed=[];actors=[]
    for spec in manifest['plans']:
        reused=False
        record=records.get(spec['slot']);path=Path(spec['report'])
        try:r=scan.read(path/('result.json' if (path/'result.json').exists() else 'progress.json'))
        except FileNotFoundError:
            r=last_seen.get(spec['slot']);reused=True
            if r is None:
                pending+=spec['units'];actors.append(dict(node=spec['node'],status='initializing'));continue
        if not record or r['pid']!=record['pid'] or (r['source'],r['target'],r['excluded'])!=('/share/project','/share-mixed/project',['eai_pwm/datasets/raw']):
            raise ValueError('child report identity/scope differs')
        if any(r[k] for k in ('payload_bytes_read','payload_bytes_written','copy_tasks_published')):raise ValueError('non-readonly child')
        if r['status'] in ('failed','stopped'):raise RuntimeError('child stopped/failed: '+str(spec['slot']))
        prior=last_seen.get(spec['slot'])
        if prior and (r['updated']<prior['updated'] or any(r['counts'].get(k,0)<v for k,v in prior['counts'].items())):
            if r['status']=='complete':raise ValueError('closed child counters regressed')
            r=prior;reused=True
        last_seen[spec['slot']]=r
        counts.update(r['counts'])
        for k,v in r['groups'].items():groups[k].update(v)
        pending+=r['pending_directories'];active+=r['active_directories']
        actors.append(dict(node=spec['node'],pid=r['pid'],status=r['status'],entries=r['counts'].get('entries',0),
            updated=r['updated'],observation_reused=reused))
        if scan.returned(dict(log_file=spec['log_file'],identity=dict(pid=r['pid'])),r):closed.append((spec,record,r))
    result=dict(status='scanning',updated=time.time(),started=manifest['baseline']['started'],pid=os.getpid(),
        host=socket.gethostname(),source='/share/project',target='/share-mixed/project',excluded=['eai_pwm/datasets/raw'],
        counts=dict(counts),groups={k:dict(v) for k,v in groups.items()},pending_directories=pending,active_directories=active,
        payload_bytes_read=0,payload_bytes_written=0,copy_tasks_published=0,content_hashes_verified=False,
        hardlink_topology_verified=False,observation='drained baseline plus disjoint pending continuations',physical_actors=actors)
    return result,closed


def merge_frontiers(destination,baseline,children):
    db=sqlite3.connect(destination)
    try:
        db.execute('PRAGMA journal_mode=DELETE');db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE frontier(rel TEXT PRIMARY KEY,leaves_only INTEGER,status TEXT)')
        db.execute('CREATE INDEX frontier_status ON frontier(status)')
        db.execute('CREATE TABLE original_pending(rel TEXT PRIMARY KEY,leaves_only INTEGER)')
        db.execute('CREATE TABLE child_done(rel TEXT PRIMARY KEY,leaves_only INTEGER)')
        for source,is_base in [(baseline,True)]+[(p,False) for p in children]:
            origin=sqlite3.connect(source.as_uri()+'?mode=ro&immutable=1',uri=True)
            try:
                if not is_base and origin.execute("SELECT 1 FROM frontier WHERE status!='done' LIMIT 1").fetchone():
                    raise ValueError('child frontier unfinished')
                for rel,leaves,status in origin.execute('SELECT rel,leaves_only,status FROM frontier'):
                    if status not in (('pending','done') if is_base else ('done',)):raise ValueError('invalid frontier state')
                    previous=db.execute('SELECT leaves_only FROM frontier WHERE rel=?',(rel,)).fetchone()
                    if previous is not None and previous!=(leaves,):raise ValueError('frontier ownership width differs')
                    db.execute("INSERT OR IGNORE INTO frontier VALUES(?,?,'done')",(rel,leaves))
                    if is_base and status=='pending':db.execute('INSERT INTO original_pending VALUES(?,?)',(rel,leaves))
                    if not is_base:db.execute('INSERT OR IGNORE INTO child_done VALUES(?,?)',(rel,leaves))
            finally:origin.close()
        if db.execute('''SELECT 1 FROM original_pending p LEFT JOIN child_done c USING(rel)
                WHERE c.rel IS NULL OR p.leaves_only!=c.leaves_only LIMIT 1''').fetchone():
            raise ValueError('an original pending unit was not completed by any child')
        db.execute('DROP TABLE original_pending');db.execute('DROP TABLE child_done')
        db.commit()
    finally:db.close()


def aggregate(root,manifest_path,output=None):
    manifest=scan.read(manifest_path);folder=manifest_path.parent
    output=folder/'aggregate/report' if output is None else Path(output)
    if not output.is_relative_to(folder) or output==folder:raise ValueError('aggregate output outside owner control')
    output.mkdir(parents=True,exist_ok=False)
    state=scan.read(folder/'launch-results.json')
    if state['errors'] or len(state['records'])!=len(manifest['plans']):raise ValueError('child launch coverage incomplete')
    records={r['slot']:r for r in state['records']}
    if len(records)!=len(manifest['plans']):raise ValueError('duplicate child launch')
    last_seen={}
    while True:
        value,closed=report(root,manifest,records,last_seen)
        scan.atomic(output/'progress.json',value)
        if len(closed)==len(manifest['plans']):break
        time.sleep(5)
    unit_lists=[]
    for spec,_,_ in closed:
        _,plan=child_plan(root,manifest,spec['slot']);unit_lists.append(plan['units'])
    units=heapq.merge(*unit_lists,key=lambda x:x['rel'])
    if unit_digest(units)!=manifest['pending_sha256']:raise ValueError('pending ownership coverage differs')
    if digest(manifest['parent_plan'])!=manifest['parent_plan_sha256']:raise ValueError('original plan changed')
    baseline_path=Path(manifest['original']['report']);baseline_db=baseline_path/'frontier.sqlite'
    db=sqlite3.connect(baseline_db.as_uri()+'?mode=ro&immutable=1',uri=True)
    try:
        original=(dict(rel=r,leaves_only=bool(l)) for r,l in db.execute("SELECT rel,leaves_only FROM frontier WHERE status='pending' ORDER BY rel"))
        if unit_digest(original)!=manifest['pending_sha256']:raise ValueError('old pending frontier changed')
    finally:db.close()
    inputs=[]
    for category in CATEGORIES:
        dest=output/(category+'.jsonl')
        with dest.open('xb') as out:
            for src in [baseline_path/dest.name]+[Path(s['report'])/dest.name for s,_,_ in closed]:
                before=src.stat();h=hashlib.sha256()
                with src.open('rb') as stream:
                    while block:=stream.read(4*1024*1024):out.write(block);h.update(block)
                after=src.stat()
                if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
                    raise ValueError('closed finding changed while merging')
                inputs.append(dict(path=str(src),sha256=h.hexdigest(),bytes=before.st_size))
            out.flush();os.fsync(out.fileno())
    # Build the union on disposable local storage, then publish the durable DB.
    # All source frontiers and the ownership proof remain on mixed storage.
    import tempfile
    with tempfile.TemporaryDirectory(prefix='cp-mnodes-balance-union-') as tmp:
        local=Path(tmp)/'frontier.sqlite'
        merge_frontiers(local,baseline_db,[Path(s['report'])/'frontier.sqlite' for s,_,_ in closed])
        with local.open('rb') as src,(output/'frontier.sqlite').open('xb') as dst:
            shutil.copyfileobj(src,dst,4*1024*1024);dst.flush();os.fsync(dst.fileno())
    proof=dict(manifest=str(manifest_path),manifest_sha256=digest(manifest_path),pending_sha256=manifest['pending_sha256'],
        baseline_frontier_sha256=digest(baseline_db),
        inputs=inputs,outputs={p.name:digest(p) for p in output.iterdir() if p.suffix in ('.jsonl','.sqlite')},
        children=[dict(spec=s,record=r,result=z) for s,r,z in closed],updated=time.time())
    scan.atomic(output/'balance-proof.json',proof)
    value['status']='complete';value['enumeration_complete']=True;value['updated']=time.time()
    scan.atomic(output/'progress.json',value);scan.atomic(output/'result.json',value)
    print(json.dumps(value),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['prepare','launch','worker','aggregate']);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--manifest',type=Path);p.add_argument('--slot',type=int)
    p.add_argument('--output',type=Path)
    p.add_argument('--node');p.add_argument('--lane',type=int);p.add_argument('--destinations',nargs='+')
    a=p.parse_args()
    if a.action=='prepare':print(json.dumps(prepare(a.root,a.node,a.lane,a.destinations)),flush=True)
    elif a.action=='launch':print(json.dumps(launch(a.root,a.manifest)),flush=True)
    elif a.action=='worker':worker(a.root,a.manifest,a.slot)
    else:
        try:aggregate(a.root,a.manifest,a.output)
        except BaseException as exc:
            output=a.output if a.output is not None else a.manifest.parent/'aggregate/report'
            try:
                failure=scan.read(output/'progress.json');failure.update(status='failed',error=repr(exc),updated=time.time())
                scan.atomic(output/'progress.json',failure);scan.atomic(output/'result.json',failure)
            except OSError:pass
            raise
