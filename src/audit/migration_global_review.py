#!/usr/bin/env python3
"""Revalidate unique global findings on 24 nodes, retaining a strict 20 TB gate.

This stage publishes a reviewed manifest; changed namespaces and access errors
remain explicit closure obligations. No copying is launched by this module.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

import migration_global_check as scan
import migration_reaudit_finish as finish
from migration_reaudit_merge import ingest, write_rows, minimal_scopes

THRESHOLD=20_000_000_000_000
TEMPLATE_ROOT=Path('/share-mixed/project/.hcr-readonly-audit-20261026-full-0107/final/tools')


def policy(root):
    value=scan.read(root/'conditional-copy-policy.json')
    if (value.get('conditional_copy_authorized') is not True
            or type(value.get('threshold_bytes_exclusive')) is not int
            or value['threshold_bytes_exclusive']!=THRESHOLD
            or (value.get('source'),value.get('target'),value.get('exclusions'))!=
                ('/share/project','/share-mixed/project',scan.EXCLUDED)):
        raise ValueError('third-round 20 TB authority/scope differs')
    return value


def prepare(root):
    scan.check_root(root);policy(root)
    closure=scan.read(root/'final/global-closure.json')
    if not closure['enumeration_complete'] or len(closure['proofs'])!=48:
        raise ValueError('primary frontier/ownership audit incomplete')
    if not scan.snapshot(root)['enumeration_complete']:
        raise ValueError('primary scanners not fully returned')
    tools=root/'final/tools';tools.mkdir(exist_ok=False)
    hashes=scan.read(TEMPLATE_ROOT/'source-sha256.json')
    for name in ('persist_readonly_audit_20261025.py','revalidate_readonly_candidates_20261025.py',
            'migration_reaudit_finish.py','migration_reaudit.py','migration_reaudit_shared.py'):
        if finish.digest(TEMPLATE_ROOT/name)!=hashes[name]:raise ValueError('frozen review template changed')
        shutil.copyfile(TEMPLATE_ROOT/name,tools/name)
    # The template uses the original comparison and safe-directory operations.
    shutil.copyfile(root/'tools/readonly_scan_core.py',tools/'cp_mnodes_readonly_scan_20261024.py')
    scan.atomic(tools/'source-sha256.json',{f.name:finish.digest(f) for f in tools.iterdir()})
    previous=finish.snapshot
    try:
        finish.snapshot=lambda *_a,**_k:scan.snapshot(root)
        manifest=finish.plan_review(root,slots=24)
    finally:finish.snapshot=previous
    for slot,plan in enumerate(manifest['plans']):
        node=scan.NODES[slot]
        plan.update(host='job-'+scan.CLUSTER+'-'+node,node=node)
        scan.atomic(root/'final/revalidation/inputs'/('slot-'+str(slot)+'.json'),plan)
    manifest.update(cluster=scan.CLUSTER,threshold_bytes_exclusive=THRESHOLD)
    scan.atomic(root/'final/revalidation/manifest.json',manifest)
    return dict(status='review_planned',records=manifest['records'],nodes=24)


START='''
import hashlib,json,os,pathlib,socket,subprocess,sys,time
root=pathlib.Path(ROOT);tools=root/'final/tools'
assert socket.gethostname()=='job-'+CLUSTER+'-'+NODE
assert os.stat('/share/project').st_ino==69783039777
assert os.stat('/share-mixed/project').st_ino==3706574426
for name,digest in json.loads((tools/'source-sha256.json').read_text()).items():
 assert hashlib.sha256((tools/name).read_bytes()).hexdigest()==digest
plan=json.loads((root/'final/revalidation/inputs'/('slot-'+str(SLOT)+'.json')).read_text())
assert plan['host']==socket.gethostname() and plan['node']==NODE and plan['slot']==SLOT
assert plan['exclusions']==['eai_pwm/datasets/raw']
folder=root/'final/revalidation/nodes'/('dev-review-'+str(SLOT))
folder.mkdir(parents=True,exist_ok=False)
with (folder/'launch-claim.json').open('x') as f:
 json.dump(dict(node=NODE,slot=SLOT,at=time.time()),f);f.flush();os.fsync(f.fileno())
argv=[sys.executable,'-S','-B','-u',str(tools/'migration_reaudit_finish.py'),
 'review-worker','--control',str(root),'--slot',str(SLOT)]
with (folder/'review.log').open('xb',buffering=0) as log:
 child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
  start_new_session=True,cwd=str(tools))
record=dict(slot=SLOT,node=NODE,pid=child.pid,argv=argv,at=time.time())
with (folder/'launch.json').open('x') as f:
 json.dump(record,f);f.flush();os.fsync(f.fileno())
print(json.dumps(record))
'''


def launch(root):
    scan.check_root(root);policy(root)
    manifest=scan.read(root/'final/revalidation/manifest.json')
    if manifest['threshold_bytes_exclusive']!=THRESHOLD or manifest['cluster']!=scan.CLUSTER:
        raise ValueError('review manifest differs')
    folder=root/'final/revalidation'
    with (folder/'launch-claim.json').open('x') as f:
        json.dump(dict(at=time.time()),f);f.flush();os.fsync(f.fileno())
    state=dict(records=[],errors=[],status='launching')
    scan.atomic(folder/'launch-results.json',state)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures={}
        for plan in manifest['plans']:
            if not plan['records']:continue
            node=plan['node'];slot=plan['slot']
            code='ROOT='+repr(str(root))+'\nCLUSTER='+repr(scan.CLUSTER)+'\nNODE='+repr(node)+'\nSLOT='+str(slot)+'\n'+START
            futures[pool.submit(scan.remote,node,code)]=slot
        for future in as_completed(futures):
            try:state['records'].append(future.result())
            except Exception as exc:state['errors'].append(dict(slot=futures[future],error=str(exc),uncertain=True))
            scan.atomic(folder/'launch-results.json',state)
    state['status']='launched' if not state['errors'] else 'launch_incomplete'
    scan.atomic(folder/'launch-results.json',state)
    return state


def check_coverage(db,manifest):
    expected=manifest['records']
    total=db.execute('SELECT COUNT(*) FROM reviewed').fetchone()[0]
    if total!=expected or db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0]!=expected:
        raise ValueError('candidate/review count incomplete')
    absent=db.execute('SELECT 1 FROM candidates c LEFT JOIN reviewed r USING(rel) WHERE r.rel IS NULL LIMIT 1').fetchone()
    extra=db.execute('SELECT 1 FROM reviewed r LEFT JOIN candidates c USING(rel) WHERE c.rel IS NULL LIMIT 1').fetchone()
    if absent or extra:raise ValueError('review path coverage differs')
    if db.execute('SELECT COUNT(*) FROM review_inputs').fetchone()[0]!=sum(bool(p['records']) for p in manifest['plans']):
        raise ValueError('review partitions incomplete')


def merge(root):
    scan.check_root(root);policy(root)
    manifest=scan.read(root/'final/revalidation/manifest.json')
    registry=scan.read(root/'final/revalidation/launch-results.json')
    if registry['errors']:raise ValueError('uncertain review launch remains')
    expected={p['slot']:p for p in manifest['plans'] if p['records']}
    records={r['slot']:r for r in registry['records']}
    if len(records)!=len(registry['records']) or set(records)!=set(expected):raise ValueError('review launch coverage differs')
    db,cache=finish.connect(root)
    with (cache/'merge.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            for slot,plan in expected.items():
                folder=root/'final/revalidation/nodes'/('dev-review-'+str(slot))
                result=scan.read(folder/'report/result.json')
                if result['status']!='complete' or result['pid']!=records[slot]['pid'] or result['host']!=plan['host']:
                    raise ValueError('review result not closed or foreign')
                if result['data_copy_started'] or result['payload_read']:raise ValueError('non-readonly review')
                with (folder/'review.log').open('rb') as stream:
                    stream.seek(max(0,(folder/'review.log').stat().st_size-1024*1024))
                    footer=json.loads(stream.read().splitlines()[-1])
                if footer!=result:raise ValueError('post-close review footer missing')
                if Path(result['output'])!=folder/'report/observations.jsonl.gz':raise ValueError('foreign review output')
                ingest(db,plan,result)
            check_coverage(db,manifest)
            if not scan.snapshot(root)['enumeration_complete']:raise ValueError('primary scan closure differs')
            outputs={};missing={}
            for kind,plural in [('file','files'),('directory','directories'),('symlink','symlinks')]:
                outputs[plural]=write_rows(root/'final'/('reviewed-missing-'+plural+'.jsonl.gz'),
                    (r[0] for r in db.execute("SELECT observation FROM reviewed WHERE status='missing' AND kind=? ORDER BY rel",(kind,))))
                count,size=db.execute("SELECT COUNT(*),COALESCE(SUM(size),0) FROM reviewed WHERE status='missing' AND kind=?",(kind,)).fetchone()
                missing[kind]=dict(paths=count,logical_bytes=size)
            statuses=dict(db.execute('SELECT status,COUNT(*) FROM reviewed GROUP BY status'))
            scopes=[os.fsdecode(r[0]) for r in db.execute('SELECT rel FROM reviewed WHERE directory_changed=1')]
            scopes.extend(os.fsdecode(r[0]) for r in db.execute("SELECT rel FROM exceptions WHERE operation IN ('source_directory_changed_during_enumeration','source_directory_enumeration')"))
            scopes=minimal_scopes(scopes)
            unresolved=statuses.get('error',0)+statuses.get('blocked',0)
            report=dict(status='reviewed_manifest_needs_completion_audit',updated=time.time(),missing=missing,
                outputs=outputs,statuses=statuses,changed_scopes=scopes,unresolved_access_paths=unresolved,
                threshold_bytes_exclusive=THRESHOLD,below_threshold=missing['file']['logical_bytes']<THRESHOLD,
                content_hashes_verified=False,data_copy_started=False,whole_tree_completion_proven=False)
            scan.atomic(root/'final/global-reviewed-manifest.json',report)
            return report
        finally:db.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['prepare','launch','merge']);p.add_argument('--root',type=Path,required=True)
    a=p.parse_args();print(json.dumps(globals()[a.action](a.root)),flush=True)
