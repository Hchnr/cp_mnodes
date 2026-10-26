#!/usr/bin/env python3
"""Bounded speculative metadata replacement for unreachable tail scanners.

The original read-only actors are not signalled. Exact original unit ownership
is retained; a replacement becomes canonical ONLY after all its local pieces
return and their actual SQLite frontiers are closed/empty. If the original
finishes first it wins. Only the local metadata indexer is handed off.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import time

from migration_reaudit import atomic
from migration_reaudit_shared import read,returned


class NotReady(Exception):pass


def digest(path):
    with open(path,'rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def load_scan(root):
    sys.path.insert(0,str(Path(root)/'tools'))
    import cp_mnodes_readonly_scan_20261024 as scan
    return scan


def expand(scan,unit,max_depth=4):
    if scan.excluded(unit['rel']):raise ValueError('excluded original unit')
    if unit['leaves_only']:return [unit]
    pending=[(unit['rel'],0)];result=[]
    while pending:
        rel,depth=pending.pop()
        if depth>=max_depth:
            result.append(dict(rel=rel,leaves_only=False));continue
        fd=None
        try:
            fd=scan.directory('/share/project',rel)
            children=[]
            with os.scandir(fd) as stream:
                for entry in stream:
                    child=rel+'/'+entry.name if rel else entry.name
                    if scan.excluded(child):continue
                    if entry.is_dir(follow_symlinks=False):children.append(child)
        except OSError:
            # Keep the full original obligation. The actual scanner must
            # report/review access failure; preview never treats it as empty.
            result.append(dict(rel=rel,leaves_only=False));continue
        finally:
            if fd is not None:os.close(fd)
        if not children:
            result.append(dict(rel=rel,leaves_only=False));continue
        result.append(dict(rel=rel,leaves_only=True))
        if len(children)>=64:
            result.extend(dict(rel=c,leaves_only=False) for c in children)
        else:pending.extend((c,depth+1) for c in children)
    return result


def alive(record):
    try:
        process=Path('/proc',str(record['pid']))
        f=(process/'stat').read_text().rsplit(') ',1)[1].split()
        if f[19]!=record['start'] or f[0] in ('Z','X'):return False
        command=(process/'cmdline').read_bytes().rstrip(b'\0')
        if not command:
            # During exit Linux may clear cmdline before publishing Z/X.
            # Keep waiting on this exact PID/start; do not infer a new actor.
            return True
        if command.decode().split('\0')!=record['argv']:
            raise ValueError('process command differs')
        return True
    except FileNotFoundError:return False


def spawn(command,log,cwd):
    with open(log,'ab',buffering=0) as stream:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=stream,stderr=subprocess.STDOUT,
            start_new_session=True,cwd=cwd,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
    f=Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()
    return dict(pid=child.pid,start=f[19],argv=command,at=time.time())


def prepare(root,owners,slots=8,resume=False):
    root=Path(root);scan=load_scan(root)
    folder=root/'acceleration'
    if resume:
        if {p.name for p in folder.iterdir()}-{'scheduler-fence.json'}:
            raise ValueError('resume only the proven pre-expansion fence failure')
    else:folder.mkdir(exist_ok=False)
    supervisor=read(root/'supervisor-launch.json')
    if alive(supervisor):
        atomic(folder/'scheduler-fence.json',dict(at=time.time(),supervisor=supervisor,
            reason='fence remote metadata work stealing before isolated local ownership refinement'))
        os.kill(supervisor['pid'],signal.SIGTERM)
        while alive(supervisor):time.sleep(.1)
    lock=open(root/'rebalance.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    registry=read(root/'launch-results.json')
    if registry.get('transfers_pending') or registry.get('transfers'):
        raise ValueError('original scope already transferred; inspect ownership before replacement')
    original={r['instance']:r for r in registry['nodes']}
    selected={owner:original[owner] for owner in owners}
    old_global=set(read(root/'plan.json')['coverage_roots'])
    expanded={};proofs={}
    for owner,record in selected.items():
        path=Path(record['report_directory']).parent/'plan.json';plan=read(path)
        with ThreadPoolExecutor(max_workers=16) as pool:
            pieces=list(pool.map(lambda unit:expand(scan,unit),plan['units']))
        units=[unit for piece in pieces for unit in piece]
        if len(units)!=len({u['rel'] for u in units}):raise ValueError('duplicate replacement unit')
        # Every original task remains represented, even leaf-only tasks whose
        # children belong to another original node. Preserve the global mask.
        mapped={u['rel']:u for u in units}
        for unit in plan['units']:
            if unit['rel'] not in mapped:raise ValueError('original unit lost')
            if unit['leaves_only'] and not mapped[unit['rel']]['leaves_only']:
                raise ValueError('shallow ownership widened')
        expanded[owner]=units
        proofs[owner]=dict(original=record,original_plan=str(path),original_plan_sha256=digest(path),
            original_units=plan['units'],expanded_units=units)
        atomic(folder/(owner+'-ownership.json'),proofs[owner])
        print(json.dumps(dict(prepared_owner=owner,original_units=len(plan['units']),expanded_units=len(units))),flush=True)
    coverage=sorted(old_global|{u['rel'] for units in expanded.values() for u in units},key=os.fsencode)
    plans=[]
    for owner,units in expanded.items():
        for slot in range(slots):
            dest=folder/owner/('slot-'+str(slot));dest.mkdir(parents=True)
            plan=dict(source='/share/project',target='/share-mixed/project',exclusions=['eai_pwm/datasets/raw'],
                host=socket.gethostname(),coverage_roots=coverage,units=units[slot::slots],
                owner=owner,slot=slot,original_plan_sha256=proofs[owner]['original_plan_sha256'])
            atomic(dest/'plan.json',plan)
            plans.append(dict(owner=owner,slot=slot,plan=str(dest/'plan.json'),directory=str(dest),units=len(plan['units'])))
    atomic(folder/'manifest.json',dict(created=time.time(),owners=owners,slots=slots,plans=plans,
        global_initial_plan_sha256=digest(root/'plan.json'),policy='original or fully validated replacement, never sum both'))
    return plans


def worker(root,plan_path):
    root=Path(root);plan_path=Path(plan_path);plan=read(plan_path)
    if plan['host']!=socket.gethostname():raise ValueError('local accelerator host changed')
    if not plan_path.resolve().is_relative_to(root/'acceleration'):raise ValueError('foreign plan')
    validation=read(root/'acceleration/prelaunch-validation.json')
    manifest=read(root/'acceleration/manifest.json')
    if validation['status']!='passed' or digest(root/'acceleration/manifest.json')!=validation['manifest_sha256']:
        raise ValueError('replacement partition not validated')
    own=next(p for p in manifest['plans'] if p['plan']==str(plan_path))
    if digest(plan_path)!=own['plan_sha256']:raise ValueError('replacement plan changed')
    for name,expected in read(root/'source-sha256.json').items():
        if digest(root/'tools'/name)!=expected:raise ValueError('frozen source scanner changed')
    sys.path.insert(0,str(root/'tools'))
    import runpy
    local=Path('/tmp/cp-mnodes-readonly-audit-20261026-local-accel')/root.name/plan['owner']/str(plan['slot'])
    output=root/'nodes'/('local-accel-'+plan['owner'])/('slot-'+str(plan['slot']))/'report'
    argv=[str(root/'tools/cp_mnodes_readonly_scan_20261024.py'),'--plan',str(plan_path),
        '--output',str(output),'--frontier',str(local/'report/frontier.sqlite'),'--threads','32','--stat-threads','64']
    sys.argv=argv
    runpy.run_path(argv[0],run_name='__main__')


def piece(root,plan):
    directory=Path(plan['directory']);launch=read(directory/'launch.json')
    report=Path(root)/'nodes'/('local-accel-'+plan['owner'])/('slot-'+str(plan['slot']))/'report'
    result=read(report/'result.json')
    rec=dict(identity=launch,log_file=str(directory/'scan.log'))
    if result['status']!='complete':raise ValueError('local scan did not finish successfully')
    if alive(launch) or not returned(rec,result):raise NotReady('local scan has not actually returned')
    local=Path('/tmp/cp-mnodes-readonly-audit-20261026-local-accel')/Path(root).name/plan['owner']/str(plan['slot'])
    with sqlite3.connect((local/'report/frontier.sqlite').as_uri()+'?mode=ro',uri=True) as db:
        states=dict(db.execute('SELECT status,COUNT(*) FROM frontier GROUP BY status'))
    if states.get('pending',0) or states.get('running',0) or any(k!='done' for k in states):
        raise ValueError('replacement frontier is not closed')
    return dict(plan=plan,launch=launch,report=str(report),result=result,frontier=states)


def aggregate(root,owner):
    root=Path(root);folder=root/'acceleration';manifest=read(folder/'manifest.json')
    parts=[piece(root,p) for p in manifest['plans'] if p['owner']==owner]
    output=folder/owner/'aggregate/report';output.mkdir(parents=True,exist_ok=False)
    counts=collections.Counter();groups=collections.defaultdict(collections.Counter)
    for p in parts:
        counts.update(p['result']['counts'])
        for key,value in p['result']['groups'].items():groups[key].update(value)
    inputs=[]
    for category in ('missing','conflicts','metadata','errors'):
        with open(output/(category+'.jsonl'),'xb') as stream:
            for part in parts:
                path=Path(part['report'])/(category+'.jsonl');before=path.stat();h=hashlib.sha256()
                with open(path,'rb') as source:
                    while block:=source.read(4*1024**2):stream.write(block);h.update(block)
                after=path.stat()
                if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
                    raise ValueError('closed accelerator input changed')
                inputs.append(dict(path=str(path),sha256=h.hexdigest(),bytes=before.st_size))
            stream.flush();os.fsync(stream.fileno())
    result=dict(status='complete',updated=time.time(),started=min(p['result']['started'] for p in parts),
        source='/share/project',target='/share-mixed/project',excluded=['eai_pwm/datasets/raw'],
        pid=os.getpid(),host=socket.gethostname(),counts=dict(counts),groups={k:dict(v) for k,v in groups.items()},
        pending_directories=0,active_directories=0,payload_bytes_read=0,payload_bytes_written=0,
        implementation='verified complete local replacement of exact original scope',
        replaced_instance=owner,content_hashes_verified=False,enumeration_complete=True)
    atomic(output/'progress.json',result);atomic(output/'result.json',result)
    atomic(folder/owner/'aggregate/proof.json',dict(parts=parts,inputs=inputs,
        ownership=read(folder/(owner+'-ownership.json'))))
    print(json.dumps(result),flush=True)


def activate(root,owner,launch):
    root=Path(root);folder=root/'acceleration';registry=read(root/'launch-results.json')
    old=next(r for r in registry['nodes'] if r['instance']==owner)
    try:final=read(Path(old['report_directory'])/'result.json')
    except FileNotFoundError:final=None
    if final and returned(old,final):return 'original_finished_first'
    pipeline=read(root/'final/pipeline-launch.json')
    # Wait for the read-only indexer to be between immutable input commits.
    while alive(pipeline):
        channel=Path('/proc',str(pipeline['pid']),'wchan').read_text().strip()
        if 'nanosleep' in channel:break
        time.sleep(2)
    if not alive(pipeline):raise ValueError('indexer no longer awaiting primary scans; inspect before handoff')
    cache=Path('/tmp')/('cp-mnodes-reaudit-index-'+root.name.removeprefix('.hcr-'))/'index.sqlite'
    with sqlite3.connect(cache.as_uri()+'?mode=ro',uri=True) as db:
        if db.execute('SELECT COUNT(*) FROM inputs WHERE instance=?',(owner,)).fetchone()[0]:
            return 'original_already_indexed'
    operation=str(time.time_ns())
    atomic(folder/(owner+'-activation-intent.json'),dict(at=time.time(),old_record=old,indexer=pipeline,replacement=launch))
    os.kill(pipeline['pid'],signal.SIGTERM)
    while alive(pipeline):time.sleep(.2)
    registry=read(root/'launch-results.json')
    old=next(r for r in registry['nodes'] if r['instance']==owner)
    try:final=read(Path(old['report_directory'])/'result.json')
    except FileNotFoundError:final=None
    decision='original_finished_first' if final and returned(old,final) else 'activated'
    if decision=='activated':
        output=folder/owner/'aggregate/report';result=read(output/'result.json')
        replacement=dict(old,host=socket.gethostname(),identity=launch,directory=str(folder/owner/'aggregate'),
            report_directory=str(output),log_file=str(folder/owner/'aggregate/scan.log'),
            replacement_proof=str(folder/owner/'aggregate/proof.json'),retired_original=old,
            implementation='local metadata replacement, original actor unsignalled')
        if not returned(replacement,result):raise ValueError('aggregated post-close proof missing')
        registry['nodes']=[replacement if r['instance']==owner else r for r in registry['nodes']]
        registry.setdefault('local_replacements',[]).append(dict(owner=owner,proof=replacement['replacement_proof'],at=time.time()))
        atomic(root/'launch-results.json',registry)
    atomic(folder/('indexer-before-'+operation+'.json'),pipeline)
    command=pipeline['argv'];new=spawn(command,root/'final/pipeline.log',root/'final/tools')
    atomic(root/'final/pipeline-launch.json',new)
    atomic(folder/(owner+'-activation-result.json'),dict(status=decision,at=time.time(),indexer=new))
    return decision


def run(root):
    root=Path(root);folder=root/'acceleration';manifest=read(folder/'manifest.json')
    validation=read(folder/'prelaunch-validation.json')
    if validation['status']!='passed' or digest(folder/'manifest.json')!=validation['manifest_sha256']:
        raise ValueError('replacement ownership has not been independently validated')
    lock=open(folder/'controller.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    for plan in manifest['plans']:
        directory=Path(plan['directory'])
        if (directory/'launch.json').exists():raise ValueError('existing accelerator launch; inspect, never replay blindly')
        command=[sys.executable,'-B','-u',str(Path(__file__).resolve()),'worker','--control',str(root),'--plan',plan['plan']]
        launch=spawn(command,directory/'scan.log',Path(__file__).parent);atomic(directory/'launch.json',launch)
    finished={}
    while len(finished)<len(manifest['owners']):
        for owner in manifest['owners']:
            if owner in finished:continue
            registry=read(root/'launch-results.json');old=next(r for r in registry['nodes'] if r['instance']==owner)
            try:final=read(Path(old['report_directory'])/'result.json')
            except FileNotFoundError:final=None
            if final and returned(old,final):
                finished[owner]='original_finished_first'
                # Stop ONLY these newly launched local read-only actors.
                for plan in manifest['plans']:
                    if plan['owner']!=owner:continue
                    e=read(Path(plan['directory'])/'launch.json')
                    if alive(e):os.kill(e['pid'],signal.SIGTERM)
                continue
            own=[p for p in manifest['plans'] if p['owner']==owner]
            try:parts=[piece(root,p) for p in own]
            except (FileNotFoundError,NotReady):continue
            aggregate_dir=folder/owner/'aggregate';aggregate_dir.mkdir(exist_ok=False)
            command=[sys.executable,'-B','-u',str(Path(__file__).resolve()),'aggregate','--control',str(root),'--owner',owner]
            launch=spawn(command,aggregate_dir/'scan.log',Path(__file__).parent)
            atomic(aggregate_dir/'launch.json',launch)
            while alive(launch):time.sleep(1)
            finished[owner]=activate(root,owner,launch)
        atomic(folder/'state.json',dict(status='complete' if len(finished)==len(manifest['owners']) else 'running',
            updated=time.time(),finished=finished,workers=len(manifest['plans'])))
        if len(finished)<len(manifest['owners']):time.sleep(15)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('prepare','run','worker','aggregate'))
    parser.add_argument('--control',type=Path,required=True);parser.add_argument('--owners')
    parser.add_argument('--plan',type=Path);parser.add_argument('--owner')
    parser.add_argument('--resume-prepare',action='store_true')
    args=parser.parse_args()
    if args.command=='prepare':prepare(args.control,args.owners.split(','),resume=args.resume_prepare)
    elif args.command=='run':run(args.control)
    elif args.command=='worker':worker(args.control,args.plan)
    else:aggregate(args.control,args.owner)


if __name__=='__main__':main()
