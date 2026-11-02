#!/usr/bin/env python3
"""Validate closed global scan shards and index their immutable findings.

The SQLite candidate index is disposable and rebuildable from shared reports;
authoritative plans, findings, input hashes and closure proofs remain shared.
This stage never launches copying or declares content equality.
"""
import argparse
import collections
import fcntl
import hashlib
import heapq
import json
from pathlib import Path
import sqlite3
import time

import migration_global_check as scan
import migration_reaudit_finish as existing


def identity_rows(units):
    rows = [(r['rel'], bool(r['leaves_only'])) for r in units]
    if len({r[0] for r in rows}) != len(rows):
        raise ValueError('duplicate unit')
    return sorted(rows)


def file_identity(path):
    s=path.stat()
    return dict(ino=s.st_ino,size=s.st_size,mtime_ns=s.st_mtime_ns,ctime_ns=s.st_ctime_ns)


def check_frontier(dbpath, expected):
    db=sqlite3.connect(dbpath.as_uri()+'?mode=ro&immutable=1',uri=True)
    try:
        if db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise ValueError('frontier integrity failure')
        if db.execute("SELECT COUNT(*) FROM frontier WHERE status != 'done'").fetchone()[0]:
            raise ValueError('frontier not closed')
        for rel,leaves in identity_rows(expected):
            if db.execute('SELECT leaves_only,status FROM frontier WHERE rel=?',(rel,)).fetchone() != (int(leaves),'done'):
                raise ValueError('initial unit absent or incomplete')
        return db.execute('SELECT COUNT(*) FROM frontier').fetchone()[0]
    finally:
        db.close()


def balance_evidence(root,rec):
    from migration_global_balance import unit_digest,CATEGORIES
    node,lane=rec['node'],rec['lane'];owner=node+'-'+str(lane)
    manifest_path=root/'balance'/owner/'manifest.json'
    if rec['balance']['manifest']!=str(manifest_path):raise ValueError('foreign balance manifest')
    original_report=manifest_path.parent/'aggregate/report'
    report=Path(rec['report'])
    if report==manifest_path.parent/'aggregate-v2/report':
        activation=scan.read(report.parent/'activation.json');old=activation['old']
        if (activation['new']!=rec or (old['node'],old['lane'])!=(node,lane)
                or Path(old['report'])!=original_report or old['balance']!=rec['balance']
                or rec.get('aggregate_upgrade')!='cached-whole-observation-v2'):
            raise ValueError('aggregate upgrade handoff differs')
    elif report!=original_report:raise ValueError('foreign aggregate report')
    manifest=scan.read(manifest_path);proof=scan.read(report/'balance-proof.json')
    if (proof['manifest']!=str(manifest_path) or proof['manifest_sha256']!=existing.digest(manifest_path)
            or rec['balance']['manifest_sha256']!=proof['manifest_sha256']):
        raise ValueError('balance manifest changed')
    original_folder=root/'nodes'/node/str(lane)
    if (manifest['node'],manifest['lane'],manifest['parent_plan'])!=(node,lane,str(original_folder/'plan.json')):
        raise ValueError('balance parent ownership differs')
    if existing.digest(manifest['parent_plan'])!=manifest['parent_plan_sha256']:raise ValueError('balance parent plan changed')
    baseline_path=original_folder/'report'
    if manifest['original']['report']!=str(baseline_path) or scan.read(baseline_path/'result.json')!=manifest['baseline']:
        raise ValueError('drained baseline differs')
    if existing.digest(baseline_path/'frontier.sqlite')!=proof['baseline_frontier_sha256']:
        raise ValueError('drained frontier changed')
    expected={p['slot']:p for p in manifest['plans']};children={p['spec']['slot']:p for p in proof['children']}
    if len(children)!=len(proof['children']) or set(children)!=set(expected):raise ValueError('balanced child coverage incomplete')
    units=[];counts=collections.Counter(manifest['baseline']['counts'])
    paths={str(baseline_path/(c+'.jsonl')) for c in CATEGORIES}
    for slot,spec in expected.items():
        child=children[slot]
        if child['spec']!=spec or existing.digest(spec['path'])!=spec['sha256']:raise ValueError('child plan changed')
        plan=scan.read(Path(spec['path']));units.append(plan['units'])
        result=scan.read(Path(spec['report'])/'result.json')
        if result!=child['result'] or not scan.returned(dict(log_file=spec['log_file'],identity=dict(pid=child['record']['pid'])),result):
            raise ValueError('child post-close result differs')
        counts.update(result['counts'])
        paths.update(str(Path(spec['report'])/(c+'.jsonl')) for c in CATEGORIES)
    if unit_digest(heapq.merge(*units,key=lambda u:u['rel']))!=manifest['pending_sha256'] or proof['pending_sha256']!=manifest['pending_sha256']:
        raise ValueError('balanced pending-unit coverage differs')
    if len(proof['inputs'])!=len(paths) or {p['path'] for p in proof['inputs']}!=paths:
        raise ValueError('balanced finding input coverage differs')
    for value in proof['inputs']:
        if existing.digest(value['path'])!=value['sha256']:raise ValueError('balanced finding input changed')
    if set(proof['outputs'])!={c+'.jsonl' for c in CATEGORIES}|{'frontier.sqlite'}:
        raise ValueError('aggregate output coverage differs')
    for name,sha in proof['outputs'].items():
        if existing.digest(report/name)!=sha:raise ValueError('aggregate output changed')
    if scan.read(report/'result.json')['counts']!=dict(counts):raise ValueError('aggregate counters differ')
    return report


def validate_shard(root, rec, global_plan):
    node, lane = rec['node'], rec['lane']
    if node not in scan.NODES or lane not in (0, 1):
        raise ValueError('foreign scanner')
    folder = root/'nodes'/node/str(lane)
    report=Path(rec['report'])
    if rec.get('balance'):
        balance_evidence(root,rec)
    elif report != folder/'report':
        raise ValueError('foreign report')
    plan = scan.read(folder/'plan.json')
    expected = global_plan['units'][scan.NODES.index(node)+lane*24::48]
    if identity_rows(plan['units']) != identity_rows(expected):
        raise ValueError('shard ownership differs')
    for key in ('source', 'target', 'exclusions', 'coverage_roots'):
        if plan[key] != global_plan[key]:
            raise ValueError('scope differs: '+key)
    result = scan.read(report/'result.json')
    record = dict(instance=node+'-'+str(lane),node=node,report_directory=str(report),
        directory=str(report.parent),log_file=str(report.parent/'scan.log'),identity=dict(pid=rec['pid']))
    if result['pid'] != rec['pid'] or not scan.returned(record,result):
        raise ValueError('scanner has not returned after fsync')
    if (result['source'],result['target'],result['excluded']) != (
            '/share/project','/share-mixed/project',scan.EXCLUDED):
        raise ValueError('result scope differs')
    if result['copy_tasks_published'] != 0:
        raise ValueError('non-readonly scan')
    # Immutable mode is valid ONLY after the post-close footer above proves
    # this single-owner database was closed. Never inspect an active frontier.
    dbpath=report/'frontier.sqlite'
    # A metadata-only identity can miss same-size writes in one timestamp tick.
    # Sequential hashing is much cheaper than repeating SQLite's random checks.
    identities=dict(frontier=file_identity(dbpath),frontier_sha256=existing.digest(dbpath),
        result_sha256=hashlib.sha256(json.dumps(result,sort_keys=True).encode()).hexdigest(),
        log=file_identity(report.parent/'scan.log'))
    plan_sha=hashlib.sha256((folder/'plan.json').read_bytes()).hexdigest()
    cachepath=report.parent/'closure-proof.json'
    try:cached=scan.read(cachepath)
    except FileNotFoundError:cached=None
    if (cached is not None and cached.get('identities')==identities
            and cached.get('plan_sha256')==plan_sha and cached.get('result_updated')==result['updated']
            and cached.get('node')==node and cached.get('lane')==lane):
        return record,result,cached
    total=check_frontier(dbpath,expected)
    if identities['frontier'] != file_identity(dbpath) or identities['log'] != file_identity(report.parent/'scan.log'):
        raise ValueError('closed evidence changed during verification')
    proof=dict(node=node,lane=lane,initial_units=len(expected),closed_directories=total,
        plan_sha256=hashlib.sha256((folder/'plan.json').read_bytes()).hexdigest(),
        identities=identities,result_updated=result['updated'],verified_at=time.time())
    scan.atomic(cachepath,proof)
    return record,result,proof


def index(root):
    scan.check_root(root)
    global_plan=scan.read(root/'plan.json')
    scan.validate_partition(global_plan['units'])
    registry=scan.read(root/'launch-results.json')
    slots=[(r['node'],r['lane']) for r in registry['records']]
    if len(slots)!=48 or set(slots)!={(n,l) for n in scan.NODES for l in (0,1)} or registry['errors']:
        raise ValueError('launch coverage incomplete or duplicated')
    final=root/'final';final.mkdir(exist_ok=True)
    db,cache=existing.connect(root)
    lock=open(cache/'index.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    proofs=[]
    try:
        for rec in registry['records']:
            report=Path(rec['report'])
            if not (report/'result.json').exists():continue
            # A result can appear before fsync/close; do not index it early.
            result=scan.read(report/'result.json')
            if not scan.returned(dict(log_file=str(report.parent/'scan.log'),identity=dict(pid=rec['pid'])),result):
                continue
            record,result,proof=validate_shard(root,rec,global_plan)
            for category in ('missing','conflicts','errors'):
                existing.ingest(db,record,category,result)
            proofs.append(proof)
        view=dict(nodes=[dict(run_returned=(r['node'],r['lane']) in {(p['node'],p['lane']) for p in proofs})
            for r in registry['records']],enumeration_complete=len(proofs)==48)
        state=existing.index_state(root,db,view,cache)
        scan.atomic(final/'global-closure.json',dict(proofs=proofs,enumeration_complete=len(proofs)==48,
            content_hashes_verified=False,copy_authorized_by_this_stage=False,updated=time.time()))
        return {k:v for k,v in state.items() if k!='inputs'}
    finally:
        db.close();lock.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(index(args.root)),flush=True)
