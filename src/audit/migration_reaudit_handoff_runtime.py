#!/usr/bin/env python3
"""One explicitly approved namespace shard, fenced old-exit/new-start handoff.

Original config, code, manifests and receipts stay immutable. New code only
adds exact-prefix carry-over; directory/link copying primitives are unchanged.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import sys
import time

import migration_manifest_copy as base
import migration_reaudit_copy as entry
from migration_reaudit_handoff import process_terminal, verify_prefix
from migration_reaudit_shared import read


def identity():
    return dict(pid=os.getpid(),start=Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()[19],
                host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def exclusive(path,value):
    with open(path,'x') as stream:
        json.dump(value,stream,separators=(',',':'));stream.flush();os.fsync(stream.fileno())


def validate_approval(approval, control, node):
    allowed = [dict(control=str(control), node=node)]
    if approval.get('answer') == '允许，先做一个分片对照':
        if approval.get('allowed') != allowed:
            raise ValueError('single-shard user approval differs')
        return
    nodes = ['master-0'] + ['worker-' + str(i) for i in range(1, 11)]
    expected = [dict(control=str(control), node=n) for n in nodes]
    if (approval.get('answer') != '允许，逐个交接剩余目录分片'
            or approval.get('allowed') != expected or node not in nodes
            or approval.get('sequential') is not True):
        raise ValueError('explicit sequential namespace approval differs')


def validate(plan_path,receiver):
    plan_path=Path(plan_path).resolve();plan=base.read_json(plan_path)
    control=Path(plan['control']).resolve();bundle=Path(__file__).resolve().parent
    if plan['kind']!='single-namespace-shard-host-handoff-v1':raise ValueError('wrong handoff kind')
    if plan_path.parent!=control/'handoffs'/plan['node']:raise ValueError('foreign handoff plan')
    cfg=base.read_json(control/'config.json');ready=base.read_json(control/'ready.json')
    if not (base.digest(control/'config.json')==ready['config_sha256']==plan['config_sha256']):
        raise ValueError('original configuration changed')
    if cfg['job_kind']!='namespace' or cfg['execution_mode']!='local':raise ValueError('only local namespace pilot allowed')
    if (plan['source_host'],plan['source_boot'])!=(cfg['execution_host'],cfg['execution_boot_id']):
        raise ValueError('source binding differs')
    actual=identity()
    expected=(plan['receiver_host'],plan['receiver_boot']) if receiver else (plan['source_host'],plan['source_boot'])
    if (actual['host'],actual['boot'])!=expected:raise ValueError('handoff host/boot differs')
    if plan['receiver_host']!='job-'+cfg['cluster']+'-'+plan['node']:raise ValueError('wrong remote cluster/node')
    approval=base.read_json(plan['approval'])
    if base.digest(plan['approval'])!=plan['approval_sha256']:
        raise ValueError('user approval digest differs')
    validate_approval(approval,control,plan['node'])
    if cfg['source']!='/share/project' or cfg['target']!='/share-mixed/project' or cfg['excluded']!=[base.EXCLUDED]:
        raise ValueError('source/target scope differs')
    for key in ('source','target'):
        if not os.path.ismount(cfg[key]) or os.stat(cfg[key]).st_ino!=cfg[key+'_root_ino']:
            raise ValueError('shared mount differs')
    path,auth,report=entry.authority(cfg['audit_root'])
    if str(path)!=cfg['authorization'] or base.digest(path)!=cfg['authorization_sha256']:
        raise ValueError('original authorization changed')
    if cfg['files']!=sum(report['outputs'][k]['rows'] for k in ('directories','symlinks')) or cfg['bytes']!=0:
        raise ValueError('namespace coverage differs')
    if set(cfg['code'])!=set(entry.CODE):raise ValueError('incomplete original bundle')
    for name,checksum in cfg['code'].items():
        if base.digest(control/'code'/name)!=checksum:raise ValueError('original code changed')
    for name,checksum in plan['receiver_code'].items():
        if Path(name).name!=name or base.digest(bundle/name)!=checksum:raise ValueError('receiver code changed')
    expected_names=set(entry.CODE)|{'migration_reaudit_handoff.py','migration_reaudit_handoff_runtime.py','migration_reaudit_shared.py'}
    if set(plan['receiver_code'])!=expected_names:raise ValueError('incomplete receiver bundle')
    for module in (base,entry,base.engine,base.native,sys.modules[base.FrontierParentOpener.__module__]):
        if Path(module.__file__).resolve().parent!=bundle:raise ValueError('foreign receiver primitive')
    # No copying primitive may differ; only manifest worker prefix scheduling
    # and this explicitly bound receiver/fencing code have changed.
    for name in ('migration_worker.py','migration_linux_open.py','migration_parent_frontier.py','migration_reaudit_copy.py'):
        if plan['receiver_code'][name]!=cfg['code'][name]:raise ValueError('copy primitive changed during host handoff')
    shard=cfg['shards'][plan['node']]
    if base.digest(shard['path'])!=shard['sha256'] or shard['sha256']!=plan['shard_sha256']:
        raise ValueError('assigned shard changed')
    return plan,control,cfg


def receiver(plan_path):
    plan,control,cfg=validate(plan_path,True);folder=Path(plan_path).parent;who=identity()
    who['plan_sha256']=base.digest(plan_path)
    exclusive(folder/'receiver-instance.json',who)
    started=time.monotonic()
    while True:
        base.atomic(folder/'receiver-state.json',dict(status='ready_waiting_for_source_exit',identity=who,updated=time.time()))
        try:seal=read(folder/'source-closed.json')
        except FileNotFoundError:
            if time.monotonic()-started>3600:raise TimeoutError('source-exit proof not received; no copying started')
            time.sleep(2);continue
        break
    validate(plan_path,True)
    if seal['plan_sha256']!=who['plan_sha256'] or seal['source_process']!=plan['source_process'] or not seal['actual_source_terminal']:
        raise ValueError('source exit proof differs')
    old=seal['closed_progress']
    if {k:old[k] for k in ('pid','start')}!=plan['source_process'] or old['host']!=plan['source_host']:
        raise ValueError('closed worker identity differs')
    node_folder=control/'nodes'/plan['node']
    if set(seal['receipts'])!={p.name for p in node_folder.glob('receipts-*.jsonl')}:
        raise ValueError('receipt input set changed before takeover')
    for name,checksum in seal['receipts'].items():
        if Path(name).name!=name or base.digest(node_folder/name)!=checksum:raise ValueError('closed receipt changed')
    prefix=verify_prefix(cfg['shards'][plan['node']]['path'],base.load_receipts(node_folder),old)
    if prefix!=seal['prefix']:raise ValueError('replayed exact-prefix proof differs')
    exclusive(folder/'receiver-copy-started.json',dict(identity=who,prefix=prefix,updated=time.time()))
    def validator(actual_control,actual_cfg,node):
        if Path(actual_control).resolve()!=control or node!=plan['node'] or actual_cfg!=cfg:
            raise ValueError('receiver assigned scope differs')
        validate(plan_path,True)
    base.validate_worker=validator
    base.validate_row=entry.validate_namespace_task;base.copy_one=entry.namespace_one
    base.atomic(folder/'receiver-state.json',dict(status='copying_verified_suffix',identity=who,updated=time.time(),prefix=prefix))
    base.worker(control,plan['node'],checkpoint=old)
    base.atomic(folder/'receiver-state.json',dict(status='receiver_returned',identity=who,updated=time.time()))


def drain(plan_path):
    plan,control,cfg=validate(plan_path,False);folder=Path(plan_path).parent;who=identity()
    exclusive(folder/'controller-instance.json',who)
    state=read(folder/'receiver-state.json')
    if (state['status']!='ready_waiting_for_source_exit' or time.time()-state['updated']>15
            or state['identity']['plan_sha256']!=base.digest(plan_path)):
        raise ValueError('receiver is not freshly staged')
    old=plan['source_process'];node_folder=control/'nodes'/plan['node']
    current=read(node_folder/'progress.json')
    if {k:current[k] for k in ('pid','start')}!=old or current['status']!='copying':
        raise ValueError('source actor changed before drain')
    if process_terminal(old):raise ValueError('source actor already exited; inspect instead of signaling')
    argv=Path('/proc',str(old['pid']),'cmdline').read_bytes().split(b'\0')
    required=[str(control/'code/migration_reaudit_copy.py').encode(),b'worker',b'--control',str(control).encode(),b'--node',plan['node'].encode()]
    if not all(x in argv for x in required):raise ValueError('source command differs')
    # pidfd pins the actual process, avoiding PID-reuse signal races.
    with os.fdopen(os.pidfd_open(old['pid']),'rb',buffering=0) as pidfd:
        if process_terminal(old):raise ValueError('source actor changed while opening pidfd')
        exclusive(folder/'drain-intent.json',dict(identity=who,source_process=old,updated=time.time(),signal='SIGTERM graceful drain'))
        signal.pidfd_send_signal(pidfd.fileno(),signal.SIGTERM,None,0)
    while not process_terminal(old):
        base.atomic(folder/'controller-state.json',dict(status='draining_one_source',identity=who,updated=time.time(),source_process=old))
        time.sleep(2)
    closed=read(node_folder/'progress.json')
    if {k:closed[k] for k in ('pid','start')}!=old:raise ValueError('source progress identity changed')
    receipts={p.name:base.digest(p) for p in node_folder.glob('receipts-*.jsonl')}
    prefix=verify_prefix(cfg['shards'][plan['node']]['path'],base.load_receipts(node_folder),closed)
    for name,checksum in receipts.items():
        if base.digest(node_folder/name)!=checksum:raise ValueError('receipt changed after actual exit')
    seal=dict(plan_sha256=base.digest(plan_path),source_process=old,actual_source_terminal=True,
              closed_progress=closed,receipts=receipts,prefix=prefix,updated=time.time(),controller=who)
    exclusive(folder/'source-closed.json',seal)
    base.atomic(folder/'controller-state.json',dict(status='source_closed_suffix_released',identity=who,updated=time.time(),prefix=prefix))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('role',choices=('receiver','drain'))
    p.add_argument('--plan',type=Path,required=True);a=p.parse_args()
    (receiver if a.role=='receiver' else drain)(a.plan)
