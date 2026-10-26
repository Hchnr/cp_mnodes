#!/usr/bin/env python3
"""Condition-bound isolated supplement: ordinary files plus missing namespace.

Preparation REQUIRES a frozen, scope-bound authorization. Conditional startup
requires completion audit; a separately bound explicit user start may copy the
verified path manifest while whole-tree completion remains pending. No old
queue is resumed. A local execution fallback is explicit, host+boot-bound and
uses the same SHA/readback/fsync/no-replace file engine and shared receipts.
"""
import argparse
import collections
import fcntl
import gzip
import heapq
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import sys
import time

import migration_manifest_copy as base

ENTRY = 'migration_reaudit_copy.py'
CODE = tuple(base.FILES) + (ENTRY,)
THRESHOLD = 50000000000000


def authority(root):
    root=Path(root)
    path=root/'final/conditional-copy-authorization.json'
    auth=base.read_json(path)
    report_path=root/'final/final-report.json'
    explicit=auth.get('kind')=='explicit-user-verified-scope-copy-v1'
    if auth.get('kind') not in ('full-reaudit-conditional-copy-v1','explicit-user-verified-scope-copy-v1') or auth.get('threshold_bytes_exclusive')!=THRESHOLD:
        raise ValueError('missing exact conditional authorization')
    if base.digest(report_path)!=auth['final_report_sha256']:
        raise ValueError('completion report changed')
    report=base.read_json(report_path)
    expected_status='verified_manifest_scope_only' if explicit else 'complete'
    if (report['status']!=expected_status or report['source']!='/share/project'
            or report['target']!='/share-mixed/project' or report['exclusions']!=[base.EXCLUDED]):
        raise ValueError('final scope is not established')
    if explicit:
        request_path=root/'explicit-copy-request.json'
        request=base.read_json(request_path)
        if (base.digest(request_path)!=auth.get('explicit_user_request_sha256')
                or request.get('directive')!='开始拷贝吧，开大并发尽快完成'
                or not auth.get('verified_path_manifest_only')
                or report.get('whole_tree_completion_proven') is not False):
            raise ValueError('explicit verified-scope authority differs')
    elif not auth.get('all_completion_gates_verified'):
        raise ValueError('unresolved completeness gates')
    if auth.get('unresolved_access_errors')!=0:
        raise ValueError('unresolved access gates')
    files=report['missing'].get('file',dict(paths=0,logical_bytes=0))
    if type(files['logical_bytes']) is not int or not 0<=files['logical_bytes']<THRESHOLD:
        raise ValueError('missing total is not strictly below 50 TB')
    if files!=auth['ordinary_files']:
        raise ValueError('authorized missing count/size differs')
    for name in ('files','directories','symlinks'):
        if report['outputs'][name]['sha256']!=auth['outputs'][name]['sha256']:
            raise ValueError('authorized namespace/manifest differs')
    return path,auth,report


def namespace_entry(row):
    rel=row['rel'];base.engine._components(rel)
    if rel==base.EXCLUDED or rel.startswith(base.EXCLUDED+'/'):
        raise ValueError('excluded namespace')
    identity=row['source'];kind=row['kind']
    expected={'directory':stat.S_ISDIR,'symlink':stat.S_ISLNK}
    if (kind not in expected or not expected[kind](identity['mode'])
            or row.get('reason')!='target_absent'):
        raise ValueError('only audited missing directories and links are eligible')
    for key in ('ino','mode','size','uid','gid','mtime_ns','ctime_ns'):
        if type(identity[key]) is not int:raise ValueError('invalid namespace identity')
    if kind=='symlink' and not isinstance(row.get('link_target'),str):
        raise ValueError('missing audited symlink target')
    # Logical PAYLOAD accounting is zero. Real source inode size remains in
    # identity for _check_scanned; it is never advertised as directory capacity.
    return dict(rel=rel,kind=kind,size=0,identity=identity,
                link_target=row.get('link_target'))


def prepare_namespace(control,root,report,threads):
    control.mkdir(parents=True,exist_ok=False);(control/'code').mkdir();(control/'shards').mkdir()
    streams={n:gzip.open(control/'shards'/(n+'.jsonl.gz'),'wt',encoding='ascii',compresslevel=1) for n in base.NODES}
    counts=collections.Counter();totals=collections.Counter();previous=None;count=0
    def source(name):
        item=report['outputs'][name]
        if base.digest(item['path'])!=item['sha256']:raise ValueError('namespace input integrity differs')
        seen=0
        with gzip.open(item['path'],'rt',encoding='ascii') as stream:
            for line in stream:
                seen+=1;yield json.loads(line)
        if seen!=item['rows']:raise ValueError('namespace input count differs')
        if base.digest(item['path'])!=item['sha256']:raise ValueError('namespace input changed during preparation')
    try:
        for row in heapq.merge(source('directories'),source('symlinks'),key=lambda r:os.fsencode(r['rel'])):
            entry=namespace_entry(row);raw=os.fsencode(entry['rel'])
            if previous is not None and raw<=previous:raise ValueError('namespace is not unique and ordered')
            previous=raw
            node=base.NODES[(count//128)%len(base.NODES)]
            streams[node].write(json.dumps(dict(id=count,entry=entry),ensure_ascii=True,separators=(',',':'))+'\n')
            counts[node]+=1;totals[entry['kind']]+=1;count+=1
    finally:
        for stream in streams.values():stream.close()
    shards={}
    for node in base.NODES:
        path=control/'shards'/(node+'.jsonl.gz')
        with open(path,'rb') as stream:os.fsync(stream.fileno())
        shards[node]=dict(files=counts[node],bytes=0,path=str(path),sha256=base.digest(path))
    for name in base.FILES:shutil.copyfile(Path(base.__file__).parent/name,control/'code'/name)
    return dict(version=1,run_id=control.name,created=time.time(),source='/share/project',target='/share-mixed/project',
        excluded=[base.EXCLUDED],files=count,bytes=0,namespace_counts=dict(totals),shards=shards,
        source_root_ino=os.stat('/share/project').st_ino,target_root_ino=os.stat('/share-mixed/project').st_ino,
        manifest=str(root/'final/final-report.json'),manifest_sha256=base.digest(root/'final/final-report.json'),
        threads=threads,cluster=base.read_json(root/'plan.json')['cluster'],job_kind='namespace')


def prepare(root,control,kind,execution='local',threads=64):
    root,control=Path(root).resolve(),Path(control).absolute()
    if control.parent!=Path('/share-mixed/project') or not control.name.startswith('.hcr-copy-fixed-'):
        raise ValueError('independent shared fixed-manifest control required')
    auth_path,auth,report=authority(root)  # Must precede mkdir or any copy launch.
    if kind=='files':
        cfg=base.prepare(control,root/'final/final-report.json',base.read_json(root/'plan.json')['cluster'],threads,
                         verified_scope=auth['kind']=='explicit-user-verified-scope-copy-v1')
        cfg['job_kind']='files'
    else:
        cfg=prepare_namespace(control,root,report,threads)
    shutil.copyfile(__file__,control/'code'/ENTRY)
    cfg.update(execution_mode=execution,execution_host=socket.gethostname(),
        execution_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        audit_root=str(root),authorization=str(auth_path),authorization_sha256=base.digest(auth_path))
    cfg['code']={name:base.digest(control/'code'/name) for name in CODE}
    for name in CODE:
        with open(control/'code'/name,'rb') as stream:os.fsync(stream.fileno())
    base.atomic(control/'config.json',cfg)
    base.atomic(control/'ready.json',dict(config_sha256=base.digest(control/'config.json'),
        authorization_sha256=cfg['authorization_sha256'],job_kind=kind,files=cfg['files'],bytes=cfg['bytes']))
    print(json.dumps(dict(status='prepared',control=str(control),kind=kind,execution=execution,
                           files=cfg['files'],bytes=cfg['bytes'])),flush=True)
    return cfg


def validate(control,cfg,node):
    ready=base.read_json(control/'ready.json')
    if ready['config_sha256']!=base.digest(control/'config.json'):
        raise ValueError('prepared config changed')
    if cfg['execution_mode']=='local':
        if (socket.gethostname()!=cfg['execution_host']
                or Path('/proc/sys/kernel/random/boot_id').read_text().strip()!=cfg['execution_boot_id']):
            raise ValueError('local fallback host/boot differs; no automatic migration')
    elif cfg['execution_mode']!='remote' or socket.gethostname()!='job-'+cfg['cluster']+'-'+node:
        raise ValueError('remote cluster/node differs')
    path,auth,report=authority(cfg['audit_root'])
    if str(path)!=cfg['authorization'] or base.digest(path)!=cfg['authorization_sha256']:
        raise ValueError('completion authorization changed')
    if cfg['source']!='/share/project' or cfg['target']!='/share-mixed/project' or cfg['excluded']!=[base.EXCLUDED]:
        raise ValueError('copy roots/exclusion changed')
    if (os.stat(cfg['source']).st_ino!=cfg['source_root_ino']
            or os.stat(cfg['target']).st_ino!=cfg['target_root_ino']
            or not os.path.ismount(cfg['source']) or not os.path.ismount(cfg['target'])):
        raise ValueError('source/target mount differs')
    if set(cfg['code'])!=set(CODE):raise ValueError('incomplete frozen copy bundle')
    for name,checksum in cfg['code'].items():
        if base.digest(control/'code'/name)!=checksum:raise ValueError('frozen copy code changed')
    for module in (base,base.engine,base.native,sys.modules[base.FrontierParentOpener.__module__]):
        if Path(module.__file__).resolve().parent!=(control/'code').resolve():
            raise ValueError('foreign copy primitive')
    shard=cfg['shards'][node]
    if base.digest(shard['path'])!=shard['sha256']:raise ValueError('assigned shard changed')
    if cfg['job_kind']=='files':
        expected=report['missing'].get('file',dict(paths=0,logical_bytes=0))
        if cfg['files']!=expected['paths'] or cfg['bytes']!=expected['logical_bytes']:
            raise ValueError('ordinary-file scope differs')
        if cfg['manifest_sha256']!=report['outputs']['files']['sha256']:
            raise ValueError('ordinary-file manifest differs')
    else:
        expected=sum(report['outputs'][k]['rows'] for k in ('directories','symlinks'))
        if cfg['files']!=expected or cfg['bytes']!=0:raise ValueError('namespace scope differs')


def copy_directory(cfg,entry):
    engine=base.engine;source_parent=source_fd=target_parent=target_fd=None;warnings=[]
    result=dict(rel=entry['rel'],kind='directory',bytes=0)
    try:
        source_parent,name=engine._open_parent(cfg['source'],entry['rel'])
        source_fd=os.open(name,engine._directory_flags(),dir_fd=source_parent)
        before=os.fstat(source_fd);engine._check_scanned(before,entry)
        target_parent,target_name=engine._open_parent(cfg['target'],entry['rel'],create=True)
        created=False
        try:
            os.mkdir(target_name,stat.S_IMODE(before.st_mode),dir_fd=target_parent);created=True
        except FileExistsError:pass
        try:target_fd=os.open(target_name,engine._directory_flags(),dir_fd=target_parent)
        except (NotADirectoryError,OSError) as exc:
            if getattr(exc,'errno',None) in (20,40):raise engine.DestinationConflict('non-directory target/ancestor')
            raise
        if created:
            engine._metadata_fd(source_fd,target_fd,before,warnings);os.fsync(target_fd)
        if (engine._identity(os.fstat(source_fd))!=engine._identity(before)
                or engine._identity(os.stat(name,dir_fd=source_parent,follow_symlinks=False))!=engine._identity(before)):
            raise engine.SourceChanged('directory source changed during creation')
        result.update(status='copied' if created else 'skipped',source_identity=engine._record(before),
            dst_stat=engine._record(os.fstat(target_fd)),existing_metadata_untouched=not created)
    except engine.SourceChanged as exc:result.update(status='changed',detail=str(exc))
    except engine.DestinationConflict as exc:result.update(status='conflict',detail=str(exc))
    except Exception as exc:result.update(status='error',detail=type(exc).__name__+': '+str(exc))
    finally:
        if warnings:result['metadata_warnings']=warnings
        for fd in (source_parent,source_fd,target_parent,target_fd):
            if fd is not None:os.close(fd)
    return result


def validate_namespace_task(row):
    task=row['source'];identity=task['identity']
    if row['rel']!=task['rel'] or task['size']!=0:raise ValueError('namespace logical task identity differs')
    return namespace_entry(dict(rel=task['rel'],kind=task['kind'],source=identity,
                                reason='target_absent',link_target=task.get('link_target')))


def resume_namespace(cfg,record,prior):
    if not prior or prior.get('status') not in ('copied','skipped') or prior.get('rel')!=record['rel']:
        return False
    source_parent=target_parent=None
    try:
        source_parent,name=base.native.open_parent_readonly(cfg['source'],record['rel'])
        source=os.stat(name,dir_fd=source_parent,follow_symlinks=False)
        base.engine._check_scanned(source,record['identity'])
        target_parent,target_name=base.native.open_parent_readonly(cfg['target'],record['rel'])
        destination=os.stat(target_name,dir_fd=target_parent,follow_symlinks=False)
        if destination.st_ino!=prior['dst_stat']['ino']:return False
        if record['kind']=='directory':return stat.S_ISDIR(destination.st_mode) and stat.S_ISDIR(source.st_mode)
        if not stat.S_ISLNK(source.st_mode) or not stat.S_ISLNK(destination.st_mode):return False
        expected=record['link_target']
        if os.readlink(name,dir_fd=source_parent)!=expected or os.readlink(target_name,dir_fd=target_parent)!=expected:
            return False
        return (base.engine._identity(os.stat(name,dir_fd=source_parent,follow_symlinks=False))==base.engine._identity(source)
                and base.engine._identity(os.stat(target_name,dir_fd=target_parent,follow_symlinks=False))==base.engine._identity(destination))
    except (OSError,KeyError,base.engine.SourceChanged):return False
    finally:
        for fd in (source_parent,target_parent):
            if fd is not None:os.close(fd)


def namespace_one(cfg,task,prior=None):
    record=task['entry'];entry=dict(record['identity'],rel=record['rel'],kind=record['kind'])
    if record['kind']=='symlink':entry['link_target']=record['link_target']
    if resume_namespace(cfg,record,prior):return dict(prior,resumed=True,finished=time.time())
    result=None
    for attempt in range(3):
        result=(copy_directory(cfg,entry) if entry['kind']=='directory' else
                base.engine.copy_entry(cfg['source'],cfg['target'],entry,cfg['run_id'],verify=True))
        if result['status']!='error':break
        if attempt<2:time.sleep(.2*(attempt+1))
    return dict(result,id=task['id'],expected_bytes=0,finished=time.time(),attempts=attempt+1)


def worker(control,node):
    cfg=base.read_json(Path(control)/'config.json')
    base.validate_worker=validate
    if cfg['job_kind']=='namespace':
        base.validate_row=validate_namespace_task
        base.copy_one=namespace_one
    base.worker(control,node)


def spawn(control,node):
    control=Path(control);cfg=base.read_json(control/'config.json')
    validate(control,cfg,node)
    folder=control/'nodes'/node;folder.mkdir(parents=True,exist_ok=True)
    if (folder/'launch.json').exists() or list(folder.glob('spawn-*.json')):
        raise ValueError('existing launch evidence; inspect before retry/resume')
    command=[sys.executable,'-B','-u',str(control/'code'/ENTRY),'worker','--control',str(control),'--node',node]
    with open(folder/('worker-'+str(time.time_ns())+'.log'),'ab',buffering=0) as log:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
            start_new_session=True,cwd=control/'code',env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
    fields=Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()
    result=dict(node=node,pid=child.pid,start=fields[19],argv=command,at=time.time())
    base.atomic(folder/('spawn-'+str(child.pid)+'.json'),result)
    return result


def launch(control):
    control=Path(control).resolve();cfg=base.read_json(control/'config.json')
    marker=control/'launch-results.json'
    if marker.exists():raise ValueError('existing launch intent: do not restart blindly')
    base.atomic(marker,dict(status='launching',at=time.time(),nodes=[]))
    results=[]
    for node in base.NODES:
        if cfg['execution_mode']=='local':
            result=spawn(control,node)
        else:
            command='python3 -B '+str(control/'code'/ENTRY)+' launch-one --control '+str(control)+' --node '+node
            p=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',node,command],
                capture_output=True,text=True,timeout=60)
            if p.returncode:raise RuntimeError('remote launch result uncertain: '+p.stderr[-1000:])
            result=json.loads(p.stdout)
        results.append(result);base.atomic(marker,dict(status='launching',at=time.time(),nodes=results))
    base.atomic(marker,dict(status='launched',at=time.time(),nodes=results))
    print(json.dumps(dict(status='launched',control=str(control),processes=len(results),execution=cfg['execution_mode'])),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('prepare','launch','launch-one','worker'))
    parser.add_argument('--audit-root',type=Path);parser.add_argument('--control',type=Path,required=True)
    parser.add_argument('--kind',choices=('files','namespace'));parser.add_argument('--execution',choices=('local','remote'),default='local')
    parser.add_argument('--threads',type=int,default=64);parser.add_argument('--node',choices=base.NODES)
    args=parser.parse_args()
    if args.command=='prepare':prepare(args.audit_root,args.control,args.kind,args.execution,args.threads)
    elif args.command=='launch':launch(args.control)
    elif args.command=='launch-one':print(json.dumps(spawn(args.control,args.node)),flush=True)
    else:worker(args.control,args.node)


if __name__=='__main__':main()
