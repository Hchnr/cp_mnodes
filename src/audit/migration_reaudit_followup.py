#!/usr/bin/env python3
"""Read-only recursive closure of changed namespaces; no payload copy APIs."""
import argparse
import collections
import gzip
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time

from migration_reaudit import atomic
from migration_reaudit_finish import digest, safe_rel
from migration_reaudit_shared import read
from migration_reaudit_shared import returned


class NotReady(RuntimeError):
    pass


def minimal(paths):
    selected=set(paths)
    for rel in selected: safe_rel(rel)
    return sorted((rel for rel in selected if not any(
        '/'.join(rel.split('/')[:i]) in selected for i in range(1,len(rel.split('/'))))),key=os.fsencode)


def scopes_from_db(db):
    # All 24 immutable review partitions must be committed, not merely launched.
    if db.execute('SELECT COUNT(*) FROM review_inputs').fetchone()[0]!=24:
        raise ValueError('wait for all primary review partitions to be merged')
    paths=[os.fsdecode(r[0]) for r in db.execute('SELECT rel FROM reviewed WHERE directory_changed=1')]
    # Source enumeration exceptions also require recursive closure, even if a
    # later candidate stat no longer reports a directory timestamp change.
    for raw,operation in db.execute('SELECT rel,operation FROM exceptions'):
        if operation in ('source_directory_changed_during_enumeration','source_directory_enumeration'):
            paths.append(os.fsdecode(raw))
    return minimal(paths)


def prepare(root,folder,slots=16):
    root=Path(root);folder=Path(folder)
    if not folder.is_relative_to(root/'final'):raise ValueError('followup must live on shared audit storage')
    if not 1<=slots<=24:raise ValueError('invalid parallelism')
    cache=Path('/tmp')/('cp-mnodes-reaudit-index-'+root.name.removeprefix('.hcr-'))/'index.sqlite'
    db=sqlite3.connect('file:'+str(cache)+'?mode=ro',uri=True)
    try:scopes=scopes_from_db(db)
    finally:db.close()
    folder.mkdir(parents=True,exist_ok=False)
    hashes=read(root/'source-sha256.json')
    for name,sha in hashes.items():
        if digest(root/'tools'/name)!=sha:raise ValueError('scanner source integrity differs')
    plans=[]
    for slot in range(min(slots,len(scopes))):
        units=[dict(rel=r,leaves_only=False) for r in scopes[slot::slots]]
        plan=dict(source='/share/project',target='/share-mixed/project',exclusions=['eai_pwm/datasets/raw'],
                  host=socket.gethostname(),coverage_roots=[],units=units,purpose='changed namespace closure')
        directory=folder/('slot-'+str(slot));directory.mkdir()
        atomic(directory/'plan.json',plan)
        plans.append(dict(slot=slot,path=str(directory/'plan.json'),sha256=digest(directory/'plan.json'),
                          scopes=[u['rel'] for u in units],
                          report_directory=str(root/'nodes'/('dev-'+folder.name)/('slot-'+str(slot))/'report')))
    manifest=dict(at=time.time(),host=socket.gethostname(),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  source_hashes=hashes,scopes=scopes,plans=plans,data_copy_started=False)
    atomic(folder/'manifest.json',manifest)
    return manifest


def launch(root,folder):
    root=Path(root);folder=Path(folder);manifest=read(folder/'manifest.json')
    if manifest['host']!=socket.gethostname() or manifest['boot_id']!=Path('/proc/sys/kernel/random/boot_id').read_text().strip():
        raise ValueError('followup host changed')
    for name,sha in manifest['source_hashes'].items():
        if digest(root/'tools'/name)!=sha:raise ValueError('frozen scanner changed')
    # A persistent global guard precedes the first fork: never replay an
    # uncertain launch just because its observation handle expired.
    with open(folder/'launch-guard.json','x') as out:
        json.dump(dict(at=time.time(),pid=os.getpid()),out);out.flush();os.fsync(out.fileno())
    launches=[]
    for plan in manifest['plans']:
        directory=Path(plan['path']).parent
        if digest(plan['path'])!=plan['sha256']:raise ValueError('plan changed')
        local=Path('/tmp')/('cp-mnodes-'+root.name.removeprefix('.hcr-')+'-'+folder.name)/('slot-'+str(plan['slot']))
        if local.exists():raise ValueError('uncertain existing frontier')
        local.mkdir(parents=True)
        report=Path(plan['report_directory'])
        if not report.is_relative_to(root/'nodes'):raise ValueError('scanner shared report layout differs')
        command=['/usr/bin/python3','-S','-B','-u',str(root/'tools/cp_mnodes_readonly_scan_20261024.py'),
                 '--plan',plan['path'],'--output',str(report),'--frontier',str(local/'frontier.sqlite'),
                 '--threads','16','--stat-threads','32']
        with open(directory/'scan.log','ab',buffering=0) as log:
            child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        fields=Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()
        record=dict(slot=plan['slot'],pid=child.pid,start=fields[19],argv=command,at=time.time(),
                    directory=str(directory),report_directory=str(report),frontier=str(local/'frontier.sqlite'))
        atomic(directory/'launch.json',record);launches.append(record)
    atomic(folder/'launch-results.json',dict(launches=launches,at=time.time()))
    return launches


def selected_rows(db,scope):
    raw=safe_rel(scope)
    yield from db.execute('SELECT rel,raw FROM reviewed WHERE rel=?',(raw,))
    # '/' + any suffix, including non-UTF8 filenames. LIKE on decoded strings
    # and a naive prefix without a slash both give incorrect coverage.
    yield from db.execute('SELECT rel,raw FROM reviewed WHERE rel>=? AND rel<? ORDER BY rel',
                          (raw+b'/',raw+b'0'))


def closed_scans(folder):
    manifest=read(Path(folder)/'manifest.json');records=read(Path(folder)/'launch-results.json')['launches']
    if len(records)!=len(manifest['plans']):raise ValueError('incomplete launch registry')
    reports=[]
    for record,plan in zip(records,manifest['plans']):
        if record['slot']!=plan['slot']:raise ValueError('foreign scan slot')
        try:fields=Path('/proc',str(record['pid']),'stat').read_text().rsplit(') ',1)[1].split()
        except FileNotFoundError:fields=None
        if fields and fields[19]==record['start'] and fields[0] not in ('Z','X'):
            raise NotReady('scan is still running: '+str(plan['slot']))
        directory=Path(record['directory']);report_folder=Path(record['report_directory']);result=read(report_folder/'result.json')
        proof=dict(identity=dict(pid=record['pid']),log_file=str(directory/'scan.log'))
        if not returned(proof,result):raise ValueError('scan post-close proof absent')
        if result['excluded']!=['eai_pwm/datasets/raw']:raise ValueError('scan exclusion differs')
        db=sqlite3.connect('file:'+record['frontier']+'?mode=ro',uri=True)
        try:
            states=dict(db.execute('SELECT status,COUNT(*) FROM frontier GROUP BY status'))
            if set(states)-{'done'}:raise ValueError('unfinished namespace frontier')
            units=[(r['rel'],0) for r in read(plan['path'])['units']]
            if any(db.execute('SELECT status FROM frontier WHERE rel=? AND leaves_only=?',u).fetchone()!=('done',) for u in units):
                raise ValueError('scope root absent from completed frontier')
        finally:db.close()
        inputs=[]
        for name in ('missing','conflicts','errors','metadata'):
            path=report_folder/(name+'.jsonl')
            inputs.append(dict(category=name,path=str(path),sha256=digest(path),bytes=path.stat().st_size))
        reports.append(dict(slot=plan['slot'],result=result,frontier_states=states,inputs=inputs))
    return manifest,reports


def plan_review(root,folder,slots=16):
    root=Path(root);folder=Path(folder);manifest,reports=closed_scans(folder)
    cache=Path('/tmp')/('cp-mnodes-'+root.name.removeprefix('.hcr-')+'-'+folder.name)
    path=cache/'review-index.sqlite'
    if path.exists():raise ValueError('existing review index; inspect before retry')
    db=sqlite3.connect(path)
    db.execute('CREATE TABLE candidates(rel BLOB PRIMARY KEY,parent BLOB,raw TEXT)')
    main=Path('/tmp')/('cp-mnodes-reaudit-index-'+root.name.removeprefix('.hcr-'))/'index.sqlite'
    source=sqlite3.connect('file:'+str(main)+'?mode=ro',uri=True)
    baseline=0
    try:
        with db:
            for scope in manifest['scopes']:
                for raw,encoded in selected_rows(source,scope):
                    rel=os.fsdecode(raw);safe_rel(rel)
                    db.execute('INSERT INTO candidates VALUES(?,?,?)',(raw,os.fsencode(rel.rpartition('/')[0]),encoded));baseline+=1
            for report in reports:
                for entry in report['inputs']:
                    if digest(entry['path'])!=entry['sha256']:raise ValueError('scan findings changed')
                    with open(entry['path']) as stream:
                        for line in stream:
                            if not line.endswith('\n'):raise ValueError('partial scan row')
                            row=json.loads(line);rel=row['rel'];raw=safe_rel(rel)
                            if not any(rel==r or rel.startswith(r+'/') for r in manifest['scopes']):
                                raise ValueError('scan finding outside assigned namespaces')
                            if entry['category']=='metadata':
                                db.execute('UPDATE candidates SET raw=? WHERE rel=?',(line.strip(),raw))
                            else:
                                # An error marker must not erase a recent source
                                # identity from a missing/conflicting observation.
                                if entry['category']=='errors':
                                    db.execute('INSERT OR IGNORE INTO candidates VALUES(?,?,?)',
                                               (raw,os.fsencode(rel.rpartition('/')[0]),line.strip()))
                                else:
                                    db.execute('INSERT OR REPLACE INTO candidates VALUES(?,?,?)',
                                               (raw,os.fsencode(rel.rpartition('/')[0]),line.strip()))
        inputs=folder/'review-inputs';inputs.mkdir(exist_ok=False)
        streams=[gzip.open(inputs/('slot-'+str(i)+'.jsonl.gz'),'wt',encoding='ascii',compresslevel=1) for i in range(slots)]
        counts=[0]*slots;tasks=[0]*slots;parent=None;group=[]
        def emit():
            nonlocal group
            if not group:return
            slot=min(range(slots),key=lambda i:counts[i]);counts[slot]+=len(group);tasks[slot]+=1
            streams[slot].write(json.dumps(dict(parent=parent,rows=group),ensure_ascii=True,separators=(',',':'))+'\n');group=[]
        try:
            for raw,encoded in db.execute('SELECT rel,raw FROM candidates ORDER BY rel'):
                row=json.loads(encoded);current=row['rel'].rpartition('/')[0]
                if group and (current!=parent or len(group)>=256):emit()
                parent=current;group.append(row)
            emit()
        finally:
            for stream in streams:stream.close()
        total=db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0]
        if total!=sum(counts):raise ValueError('review partition coverage differs')
        plans=[]
        for slot in range(slots):
            file=inputs/('slot-'+str(slot)+'.jsonl.gz')
            plan=dict(source='/share/project',target='/share-mixed/project',exclusions=['eai_pwm/datasets/raw'],
                      host=socket.gethostname(),node='namespace-review-'+str(slot),slot=slot,
                      input=str(file),input_sha256=digest(file),records=counts[slot],tasks=tasks[slot])
            atomic(inputs/('slot-'+str(slot)+'.json'),plan);plans.append(plan)
        result=dict(at=time.time(),baseline_paths=baseline,records=total,plans=plans,scans=reports,
                    cache=str(path),scopes=manifest['scopes'],data_copy_started=False)
        atomic(folder/'review-manifest.json',result);return result
    finally:source.close();db.close()


def review_worker(root,folder,slot):
    root=Path(root);folder=Path(folder)
    for name,sha in read(root/'final/tools/source-sha256.json').items():
        if digest(root/'final/tools'/name)!=sha:raise ValueError('immutable review code differs')
    plan=read(folder/'review-inputs'/('slot-'+str(slot)+'.json'))
    if plan['host']!=socket.gethostname():raise ValueError('review host differs')
    sys.path.insert(0,str(root/'final/tools'));sys.path.insert(0,str(root/'tools'))
    import revalidate_readonly_candidates_20261025 as review
    return review.run_review(plan,folder/'reviews'/('slot-'+str(slot))/'report',threads=16,stat_threads=32)


def launch_reviews(root,folder):
    root=Path(root);folder=Path(folder);manifest=read(folder/'review-manifest.json')
    with open(folder/'review-launch-guard.json','x') as out:
        json.dump(dict(at=time.time(),pid=os.getpid()),out);out.flush();os.fsync(out.fileno())
    launches=[]
    for plan in manifest['plans']:
        if not plan['records']:continue
        directory=folder/'reviews'/('slot-'+str(plan['slot']));directory.mkdir(parents=True)
        command=['/usr/bin/python3','-S','-B','-u',str(Path(__file__).resolve()),'review-worker',
                 '--control',str(root),'--folder',str(folder),'--slot',str(plan['slot'])]
        with open(directory/'review.log','ab',buffering=0) as log:
            child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        fields=Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()
        record=dict(slot=plan['slot'],pid=child.pid,start=fields[19],argv=command,at=time.time())
        atomic(directory/'launch.json',record);launches.append(record)
    atomic(folder/'review-launch-results.json',dict(launches=launches,at=time.time()))
    return launches


def pipeline(root,folder):
    folder=Path(folder)
    while True:
        try:
            closed_scans(folder)
            break
        except NotReady as exc:
            atomic(folder/'pipeline-state.json',dict(status='waiting_for_namespace_scans',updated=time.time(),detail=str(exc)))
            time.sleep(20)
    plan=plan_review(root,folder)
    launches=launch_reviews(root,folder)
    result=dict(status='namespace_review_running',updated=time.time(),records=plan['records'],launches=launches)
    atomic(folder/'pipeline-state.json',result);return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('command',choices=['prepare','launch','plan-review','launch-reviews','review-worker','pipeline'])
    p.add_argument('--control',type=Path,required=True);p.add_argument('--folder',type=Path,required=True)
    p.add_argument('--slots',type=int,default=16);p.add_argument('--slot',type=int);a=p.parse_args()
    if a.command=='prepare':result=prepare(a.control,a.folder,a.slots)
    elif a.command=='launch':result=launch(a.control,a.folder)
    elif a.command=='plan-review':result=plan_review(a.control,a.folder,a.slots)
    elif a.command=='launch-reviews':result=launch_reviews(a.control,a.folder)
    elif a.command=='pipeline':result=pipeline(a.control,a.folder)
    else:result=review_worker(a.control,a.folder,a.slot)
    print(json.dumps(result),flush=True)


if __name__=='__main__':main()
