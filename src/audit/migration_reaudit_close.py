#!/usr/bin/env python3
"""Close scoped reviews, publish exact missing scope, then conditionally copy.

This does not assert an atomic snapshot of the live source. Every enumeration
obligation must have a closed recursive scan or bounded stable-directory proof.
Original reports are immutable; a separate overlay retains all newer evidence.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import fcntl
import gzip
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import uuid

from migration_reaudit import atomic,validate_partition
from migration_reaudit_capture import capture_tree
from migration_reaudit_finish import digest,safe_rel
from migration_reaudit_followup import closed_scans,minimal,NotReady
from migration_reaudit_merge import ingest,write_rows
from migration_reaudit_shared import read,snapshot

THRESHOLD=50000000000000
SCHEMA='''CREATE TABLE IF NOT EXISTS reviewed(rel BLOB PRIMARY KEY,status TEXT,kind TEXT,size INTEGER,
 raw TEXT,observation TEXT,checked_at REAL,directory_changed INTEGER);
 CREATE INDEX IF NOT EXISTS reviewed_status_kind ON reviewed(status,kind);
 CREATE TABLE IF NOT EXISTS review_inputs(slot INTEGER PRIMARY KEY,input_sha256 TEXT,output_sha256 TEXT,rows INTEGER);'''


def terminal(record):
    try:fields=Path('/proc',str(record['pid']),'stat').read_text().rsplit(') ',1)[1].split()
    except FileNotFoundError:return True
    return fields[19]!=record['start'] or fields[0] in ('Z','X')


def load_reviews(root,folder):
    folder=Path(folder);manifest=read(folder/'review-manifest.json')
    db=sqlite3.connect(manifest['cache'],timeout=60);db.executescript(SCHEMA)
    try:
        for plan in manifest['plans']:
            if not plan['records']:continue
            directory=folder/'reviews'/('slot-'+str(plan['slot']))
            if not terminal(read(directory/'launch.json')):raise NotReady('scoped review '+str(plan['slot'])+' still running')
            result=read(directory/'report/result.json')
            if Path(result['output']).resolve()!=(directory/'report/observations.jsonl.gz').resolve():
                raise ValueError('foreign scoped review output')
            ingest(db,plan,result)
            atomic(folder/'closure-state.json',dict(status='merging_scoped_reviews',updated=time.time(),
                merged=db.execute('SELECT COUNT(*) FROM review_inputs').fetchone()[0],expected=len(manifest['plans'])))
        total=db.execute('SELECT COUNT(*) FROM reviewed').fetchone()[0]
        absent=db.execute('SELECT COUNT(*) FROM candidates c WHERE NOT EXISTS(SELECT 1 FROM reviewed r WHERE r.rel=c.rel)').fetchone()[0]
        extra=db.execute('SELECT COUNT(*) FROM reviewed r WHERE NOT EXISTS(SELECT 1 FROM candidates c WHERE c.rel=r.rel)').fetchone()[0]
        captures=existing_captures(folder)
        if captures:
            # A previous controller may have closed some scopes before an
            # unrelated remaining scope failed. Reconstruct the exact expected
            # key set from immutable review inputs plus verified captures;
            # never accept an arbitrary larger cache as "resumed progress".
            expected=set()
            for plan in manifest['plans']:
                if digest(plan['input'])!=plan['input_sha256']:raise ValueError('scoped input changed')
                with gzip.open(plan['input'],'rt',encoding='ascii') as stream:
                    for line in stream:
                        for row in json.loads(line)['rows']:
                            key=safe_rel(row['rel'])
                            if key in expected:raise ValueError('duplicate original scoped input')
                            expected.add(key)
            if len(expected)!=manifest['records']:raise ValueError('original scoped input coverage differs')
            for result in captures:
                with gzip.open(result['output'],'rt',encoding='ascii') as stream:
                    for line in stream:expected.add(safe_rel(json.loads(line)['rel']))
            for table in ('reviewed','candidates'):
                remaining=expected.copy()
                for key, in db.execute('SELECT rel FROM '+table):
                    if key not in remaining:raise ValueError('foreign cached scoped path')
                    remaining.remove(key)
                if remaining:raise ValueError('cached scoped coverage incomplete')
        elif total!=manifest['records']:raise ValueError('scoped review count differs')
        if absent or extra:raise ValueError('scoped review coverage differs')
        return db,manifest
    except BaseException:db.close();raise


def pending_scopes(db,manifest):
    scopes=[os.fsdecode(r[0]) for r in db.execute('SELECT rel FROM reviewed WHERE directory_changed=1')]
    for scan in manifest['scans']:
        for item in scan['inputs']:
            if item['category']!='errors':continue
            if digest(item['path'])!=item['sha256']:raise ValueError('closed scan exception input changed')
            with open(item['path']) as stream:
                for line in stream:
                    row=json.loads(line)
                    if row.get('operation')=='source_directory_changed_during_enumeration':scopes.append(row['rel'])
                    elif row.get('operation')!='unsupported_source_type':
                        latest=db.execute('SELECT status FROM reviewed WHERE rel=?',(safe_rel(row['rel']),)).fetchone()
                        if latest is None or latest[0] in ('error','blocked'):
                            raise ValueError('unresolved source scan exception: '+row['rel'])
    return minimal(scopes)


def counters(rows):
    value=collections.Counter()
    for row in rows:
        value['processed']+=1;value[row['status']]+=1
        if row['status']=='missing':
            value['missing_'+{'file':'files','directory':'directories','symlink':'symlinks'}[row['kind']]]+=1
            if row['kind']=='file':value['missing_logical_bytes']+=row['source']['size']
        if row.get('directory_contents_changed'):value['source_directory_changes']+=1
    return dict(value)


def existing_captures(folder):
    results=[]
    for path in sorted((Path(folder)/'stable-captures').glob('*/result.json')):
        result=read(path);proof=Path(result['proof']);output=Path(result['output'])
        if proof.parent.resolve()!=path.parent.resolve() or output.parent.resolve()!=path.parent.resolve():
            raise ValueError('foreign prior stable capture')
        if digest(proof)!=result['input_sha256'] or digest(output)!=result['output_sha256']:
            raise ValueError('prior stable capture changed')
        if read(proof)['scope']!=result['scope']:raise ValueError('prior proof scope differs')
        # Re-run semantic/counter validation without changing canonical rows.
        scratch=sqlite3.connect(':memory:');scratch.executescript(SCHEMA)
        try:
            ingest(scratch,dict(slot=0,records=result['counts']['processed'],input_sha256=result['input_sha256']),result)
            for raw, in scratch.execute('SELECT rel FROM reviewed'):
                rel=os.fsdecode(raw)
                if not (rel==result['scope'] or rel.startswith(result['scope']+'/')):
                    raise ValueError('prior capture path outside proved namespace')
        finally:scratch.close()
        results.append(result)
    return results


def capture_scope(root,folder,scope,known):
    sys.path.insert(0,str(Path(root)/'final/tools'));sys.path.insert(0,str(Path(root)/'tools'))
    import cp_mnodes_readonly_scan_20261024 as scan
    import revalidate_readonly_candidates_20261025 as review
    values,proofs=capture_tree(scan,review,'/share/project','/share-mixed/project',scope)
    # Paths formerly found below this scope must also be resolved, not silently
    # dropped just because the current stable enumeration no longer lists them.
    missing=collections.defaultdict(list)
    for raw,encoded in known:
        rel=os.fsdecode(raw)
        if rel not in values:missing[rel.rpartition('/')[0]].append(json.loads(encoded))
    with ThreadPoolExecutor(max_workers=32) as attributes:
        for parent,rows in missing.items():
            for offset in range(0,len(rows),256):
                for row in review.inspect_group(dict(parent=parent,rows=rows[offset:offset+256]),
                                               Path('/share/project'),Path('/share-mixed/project'),attributes):
                    values[row['rel']]=row
    token=__import__('hashlib').sha256(os.fsencode(scope)).hexdigest()[:24]
    directory=Path(folder)/'stable-captures'/token;directory.mkdir(parents=True,exist_ok=False)
    proof_path=directory/'namespace-proof.json'
    atomic(proof_path,dict(scope=scope,directories=proofs,payload_read=False,at=time.time()))
    output=write_rows(directory/'observations.jsonl.gz',
                      (json.dumps(values[k],ensure_ascii=True,separators=(',',':')) for k in sorted(values,key=os.fsencode)))
    result=dict(status='complete',counts=counters(values.values()),input_sha256=digest(proof_path),
                output=output['path'],output_sha256=output['sha256'],scope=scope,proof=str(proof_path))
    atomic(directory/'result.json',result);return result


def incorporate_capture(db,result,slot):
    # Reuse the same semantic validation as the original review merger before
    # replacing any canonical overlay observation.
    scratch=sqlite3.connect(':memory:');scratch.executescript(SCHEMA)
    try:
        ingest(scratch,dict(slot=slot,records=result['counts']['processed'],input_sha256=result['input_sha256']),result)
        with db:
            for row in scratch.execute('SELECT * FROM reviewed'):
                raw,status,kind,size,encoded,obs,checked,changed=row
                rel=os.fsdecode(raw)
                if not (rel==result['scope'] or rel.startswith(result['scope']+'/')):raise ValueError('foreign stable scope output')
                db.execute('INSERT OR REPLACE INTO candidates VALUES(?,?,?)',
                           (raw,os.fsencode(rel.rpartition('/')[0]),encoded))
                db.execute('INSERT OR REPLACE INTO reviewed VALUES(?,?,?,?,?,?,?,?)',row)
    finally:scratch.close()


def stabilize(root,folder,db,manifest):
    captures=existing_captures(folder)
    prior=[r['scope'] for r in captures]
    scopes=[r for r in pending_scopes(db,manifest) if not any(r==p or r.startswith(p+'/') for p in prior)]
    total=len(scopes)+len(captures)
    for slot,scope in enumerate(scopes):
        raw=safe_rel(scope)
        known=list(db.execute('SELECT rel,raw FROM reviewed WHERE rel=? OR (rel>=? AND rel<?)',
                              (raw,raw+b'/',raw+b'0')))
        atomic(Path(folder)/'closure-state.json',dict(status='stable_namespace_capture',updated=time.time(),
            scopes=total,completed=len(captures),current=scope))
        result=capture_scope(root,folder,scope,known);incorporate_capture(db,result,1000+slot);captures.append(result)
    atomic(Path(folder)/'stable-capture-manifest.json',dict(scopes=prior+scopes,captures=captures,at=time.time()))
    return captures


def effective_rows(main,kind):
    return main.execute('''SELECT rel,size,observation FROM (
      SELECT r.rel,r.size,r.observation FROM main.reviewed r WHERE r.status='missing' AND r.kind=?
       AND NOT EXISTS(SELECT 1 FROM overlay.reviewed o WHERE o.rel=r.rel)
      UNION ALL SELECT rel,size,observation FROM overlay.reviewed WHERE status='missing' AND kind=?) ORDER BY rel''',(kind,kind))


def export(root,folder,db,manifest,captures,explicit_start=False):
    root=Path(root);folder=Path(folder)
    original=read(root/'final/reviewed-manifest.json')
    if original['coverage']['missing_reviews'] or original['coverage']['unexpected_paths']:
        raise ValueError('primary review coverage is incomplete')
    if original['copy_gates']['unresolved_access_paths']!=0:raise ValueError('primary access errors unresolved')
    unresolved=db.execute("SELECT COUNT(*) FROM reviewed WHERE status IN ('error','blocked')").fetchone()[0]
    if unresolved:raise ValueError('scoped access errors unresolved: '+str(unresolved))
    plan=read(root/'plan.json');validate_partition(plan['units'])
    if plan['planning_errors']:raise ValueError('primary namespace planning had errors')
    view=snapshot(root,closed_only=True)
    if not view['enumeration_complete']:raise ValueError('primary enumeration no longer closed')
    for name,sha in read(root/'source-sha256.json').items():
        if digest(root/'tools'/name)!=sha:raise ValueError('original scanner source changed')
    # Revalidate recursive scan, exact initial units, local frontiers, input
    # digests and process termination independently of the review's counters.
    closed_scans(folder)
    preflight=read(root/'preflight.json')['nodes']
    if any(os.stat('/share/project').st_ino!=p['source_ino'] or
           os.stat('/share-mixed/project').st_ino!=p['target_ino'] for p in preflight):
        raise ValueError('source/target root identity changed')
    main_path=Path('/tmp')/('cp-mnodes-reaudit-index-'+root.name.removeprefix('.hcr-'))/'index.sqlite'
    main=sqlite3.connect('file:'+str(main_path)+'?mode=ro',uri=True)
    main.execute('ATTACH DATABASE ? AS overlay',(manifest['cache'],))
    outputs={};absent={};closed=root/'final/closed';closed.mkdir(exist_ok=False)
    try:
        for kind,plural in [('file','files'),('directory','directories'),('symlink','symlinks')]:
            count=0;logical=0;previous=None
            def stream():
                nonlocal count,logical,previous
                for rel,size,encoded in effective_rows(main,kind):
                    safe_rel(os.fsdecode(rel))
                    if previous is not None and rel<=previous:raise ValueError('effective manifest not unique and sorted')
                    previous=rel;count+=1;logical+=size;yield encoded
            atomic(folder/'closure-state.json',dict(status='exporting_final_'+plural,updated=time.time()))
            outputs[plural]=write_rows(closed/('missing-'+plural+'.jsonl.gz'),stream())
            absent[kind]=dict(paths=count,logical_bytes=logical)
        attention=main.execute('''SELECT raw FROM (
          SELECT r.rel,r.raw FROM main.reviewed r WHERE r.status IN ('conflict','unsupported')
          AND NOT EXISTS(SELECT 1 FROM overlay.reviewed o WHERE o.rel=r.rel)
          UNION ALL SELECT rel,raw FROM overlay.reviewed WHERE status IN ('conflict','unsupported')) ORDER BY rel''')
        outputs['attention']=write_rows(closed/'needs-attention.jsonl.gz',(r[0] for r in attention))
    finally:main.close()
    policy=read(root/'conditional-copy-policy.json')
    if not policy['conditional_copy_authorized'] or policy['threshold_bytes_exclusive']!=THRESHOLD:
        raise ValueError('conditional authority differs')
    report=dict(status='verified_manifest_scope_only' if explicit_start else 'complete',updated=time.time(),source='/share/project',target='/share-mixed/project',
        exclusions=['eai_pwm/datasets/raw'],missing=absent,outputs=outputs,unresolved_access_errors=0,
        primary_review=str(root/'final/reviewed-manifest.json'),scoped_review=str(folder/'review-manifest.json'),
        primary_enumeration_closed=True,scoped_recursive_enumeration_closed=True,
        stable_namespace_captures=len(captures),data_copy_started=False,content_hashes_verified=False,
        whole_tree_completion_proven=not explicit_start,
        existing_metadata_and_content_equality_verified=False,
        caveat=('Verified known path manifest; whole-tree live-source completion remains pending. ' if explicit_start else
                'Completed online metadata/path/size audit, not a simultaneous source snapshot. ')+
                'New copying uses source-version guards and no-replace publication. Conflicts and special objects remain separately reported.')
    if (root/'final/final-report.json').exists():raise ValueError('final report already frozen; do not replace active authority')
    atomic(root/'final/final-report.json',report)
    size=absent['file']['logical_bytes']
    if size>=THRESHOLD:
        atomic(root/'conditional-copy-state.json',dict(status='above_authorized_threshold',updated=time.time(),missing_bytes=size));return report
    auth=dict(kind='explicit-user-verified-scope-copy-v1' if explicit_start else 'full-reaudit-conditional-copy-v1',threshold_bytes_exclusive=THRESHOLD,
              final_report_sha256=digest(root/'final/final-report.json'),all_completion_gates_verified=not explicit_start,
              unresolved_access_errors=0,ordinary_files=absent['file'],outputs={k:outputs[k] for k in ('files','directories','symlinks')})
    if explicit_start:
        request_path=root/'explicit-copy-request.json';request=read(request_path)
        if request.get('directive')!='开始拷贝吧，开大并发尽快完成':raise ValueError('explicit start request missing')
        auth.update(verified_path_manifest_only=True,explicit_user_request_sha256=digest(request_path))
    atomic(root/'final/conditional-copy-authorization.json',auth)
    atomic(root/'conditional-copy-state.json',dict(status='authorized_preparing_isolated_copy',updated=time.time(),missing_bytes=size))
    return report


def start_copy(root,file_threads=64,namespace_threads=64,copy_bundle=None):
    root=Path(root);controls={};bundle=Path(copy_bundle) if copy_bundle else root/'final/copy-tools';entry=bundle/'migration_reaudit_copy.py'
    for name,sha in read(bundle/'source-sha256.json').items():
        if digest(bundle/name)!=sha:raise ValueError('frozen copier code changed')
    token=time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+uuid.uuid4().hex[:8]
    for kind in ('files','namespace'):
        control=Path('/share-mixed/project')/('.hcr-copy-fixed-reaudit-'+token+'-'+kind)
        controls[kind]=str(control)
        atomic(root/'conditional-copy-state.json',dict(status='copy_preparing_'+kind,updated=time.time(),
            controls=controls,execution='local physical host; remote SSH bridge unavailable'))
        subprocess.run(['/usr/bin/python3','-S','-B','-u',str(entry),'prepare','--audit-root',str(root),
                        '--control',str(control),'--kind',kind,'--execution','local','--threads',
                        str(file_threads if kind=='files' else namespace_threads)],check=True)
        atomic(root/'conditional-copy-state.json',dict(status='copy_launch_in_progress',updated=time.time(),
            controls=controls,execution='local physical host, 12 logical shards per job; remote SSH bridge unavailable'))
        subprocess.run(['/usr/bin/python3','-S','-B','-u',str(control/'code/migration_reaudit_copy.py'),
                        'launch','--control',str(control)],check=True)
    atomic(root/'conditional-copy-state.json',dict(status='copy_running',updated=time.time(),controls=controls,
        execution='local physical host, 24 processes total; old queues remain untouched'))
    return controls


def run(root,folder,copy=False):
    folder=Path(folder);lock=open(folder/'close.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    while True:
        try:db,manifest=load_reviews(root,folder);break
        except NotReady as exc:
            atomic(folder/'closure-state.json',dict(status='waiting_for_scoped_reviews',updated=time.time(),detail=str(exc)))
            time.sleep(20)
    try:
        captures=stabilize(root,folder,db,manifest)
        report=export(root,folder,db,manifest,captures)
        if copy and report['missing']['file']['logical_bytes']<THRESHOLD:start_copy(root)
        atomic(folder/'closure-state.json',dict(status='final_scope_published',updated=time.time(),missing=report['missing']))
        return report
    finally:db.close();lock.close()


def run_explicit(root,folder):
    root=Path(root);folder=Path(folder)
    request=read(root/'explicit-copy-request.json')
    if request.get('directive')!='开始拷贝吧，开大并发尽快完成':raise ValueError('explicit start authority missing')
    lock=open(folder/'close.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    db,manifest=load_reviews(root,folder)
    try:
        captures=existing_captures(folder)
        report=export(root,folder,db,manifest,captures,explicit_start=True)
        if report['missing']['file']['logical_bytes']<THRESHOLD:
            start_copy(root,file_threads=128,namespace_threads=32,copy_bundle=root/'final/copy-tools-explicit-v1')
        atomic(folder/'closure-state.json',dict(status='verified_scope_copy_started_whole_tree_completion_pending',
            updated=time.time(),missing=report['missing']))
        return report
    finally:db.close();lock.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True)
    p.add_argument('--folder',type=Path,required=True);p.add_argument('--copy',action='store_true')
    p.add_argument('--explicit-start',action='store_true');a=p.parse_args()
    print(json.dumps(run_explicit(a.control,a.folder) if a.explicit_start else run(a.control,a.folder,a.copy)),flush=True)
