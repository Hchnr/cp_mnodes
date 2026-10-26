#!/usr/bin/env python3
"""Detached, identity-bound P3-only cutover; never signals copy/broker/seed.

Run stage/standby/apply on the actual master with an explicit private bundle.
Before the irreversible foreground interrupt every failure resumes original
readers. After it, only owner-requested SIGTERM cleanup may resume readers.
The controller itself is detached so a lost SSH bridge cannot strand STOPs.
"""
import argparse
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
import traceback

from migration_io import read_json, write_json
from migration_scan_boundary import (identity, require_identity, stopped_threads,
                                      observe_boundary, NotQuiescent)
from migration_unix_queues import unix_queues
from migration_warm_scan_checkpoint import validate_runtime
from migration_run import queued_count


def same(expected):
    return require_identity(expected)


def source_readers(parent, source, expected_count=8, expected_pids=None):
    result=[]
    children=(Path('/proc')/str(parent['pid'])/'task'/str(parent['pid'])/'children').read_text().split()
    if expected_pids is not None:
        if len(expected_pids)!=expected_count or len(set(expected_pids))!=expected_count:
            raise RuntimeError('source reader PID evidence is incomplete')
        if not set(map(str,expected_pids))<=set(children):
            raise RuntimeError('recorded source reader does not belong to this producer')
        children=list(map(str,expected_pids))
    for pid in children:
        value=identity(int(pid))
        if 'multiprocessing.spawn' not in ' '.join(value['argv']):continue
        for fd in (Path('/proc')/pid/'fd').iterdir():
            try:link=os.readlink(fd)
            except FileNotFoundError:continue
            if link==source:
                result.append(value);break
    if len(result)!=expected_count or any(v['ppid']!=parent['pid'] for v in result):
        raise RuntimeError('the exact expected anchored source reader pool is required')
    return result


def stage(directory):
    directory=Path(directory).resolve();control=directory.parent
    expected=read_json(directory/'expected-runtime.json')
    cfg,state,parent=validate_runtime(control,expected)
    readers=source_readers(parent,cfg['source'],expected.get('source_reader_count',8),
                          state.get('source_reader_progress',{}).get('pids')
                          if expected.get('entry_kind')=='fast-p3-scan' else None)
    children=(Path('/proc')/str(parent['pid'])/'task'/str(parent['pid'])/'children').read_text().split()
    seeds=[]
    for pid in children:
        value=identity(int(pid))
        if str(control/'migration_overlap_resume.py') in value['argv'] and 'seed' in value['argv']:
            seeds.append(value)
    if expected.get('entry_kind')=='fast-p3-scan':
        inherited=read_json(Path(expected['source_bundle'])/'handover.json')['seed_helper']
        helper=dict(pid=inherited['pid'],start_ticks=str(inherited['starttime_ticks']),argv=inherited['argv'])
        try:same(helper)
        except (FileNotFoundError,NotQuiescent):
            if read_json(Path(inherited['result_path'])).get('ok') is not True:
                raise RuntimeError('inherited original seed has no successful result')
        helper['result_path']=inherited['result_path']
    else:
        if len(seeds)!=1:raise RuntimeError('original live seed helper required at staging')
        helper=seeds[0];helper['result_path']=str(control/'overlap-seed-result.json')
    local_copies=[]
    roots=[control]
    plan=read_json(control/'extension-plan.json')
    parallel=plan.get('parallel_execution',{}).get('control')
    if parallel:roots.append(Path(parallel))
    for root in roots:
        for folder in ['nodes','nodes-extra']:
            node=read_json(root/folder/(socket.gethostname()+'.json'))
            local_copies.append(identity(node['pid']))
    record=dict(control=str(control),old_producer={k:parent[k] for k in ('pid','start_ticks','argv')},
                source_readers=readers,seed_helper=helper,local_copy_roles=local_copies,
                expected_runtime=expected,job=cfg['job'],created=time.time())
    write_json(directory/'stage.json',record)
    print(json.dumps(dict(staged=True,directory=str(directory),source_readers=len(readers),copy_roles_pinned=len(local_copies))))


def pending_term(pid):
    try:
        values=dict(line.split(':',1) for line in (Path('/proc')/str(pid)/'status').read_text().splitlines() if ':' in line)
    except FileNotFoundError:return False
    return bool((int(values.get('SigPnd','0'),16)|int(values.get('ShdPnd','0'),16))&(1<<(signal.SIGTERM-1)))


def queue_barrier(state,floor):
    target=state.get('target_filter',{});publication=state.get('task_publication',{})
    inputs=state.get('priority_inputs',{})
    feeds=state.get('distributed_feeds')
    return (state.get('recovery_warm') is True and state.get('stage')=='p3-r3'
            and state.get('next_task_id')==floor and target.get('inflight')==0
            and target.get('submitted')==target.get('checked')
            and publication.get('pending')==0 and not publication.get('failed')
            and inputs.get('open_groups')==0
            and isinstance(feeds,list) and len(feeds)==24
            and all(v.get('status')=='complete' for v in feeds))


def apply(directory):
    directory=Path(directory).resolve();record=read_json(directory/'stage.json')
    control=Path(record['control']);expected=record['expected_runtime']
    cfg,state,parent=validate_runtime(control,expected)
    if record['old_producer']!=expected['producer']:raise ValueError('staging producer changed')
    if (directory/'cutover.json').exists():raise RuntimeError('cutover was already issued; inspect successor')
    ready=read_json(directory/'standby.json');standby=read_json(directory/'standby-role.json')
    same(standby)
    if ready.get('status')!='ready' or ready.get('bytes_behind',2**64)>1024**2 or time.time()-ready['updated']>15:
        raise RuntimeError('successor cache is not freshly caught up; original scan untouched')
    floor=read_json(Path(read_json(directory/'options.json')['assignment_preparation']))['prefix_floor']
    if state['next_task_id']!=floor:raise RuntimeError('new assignments require a fresh assignment delta')
    stopped=[];foreground_stopped=False;interrupted=False
    events={'status':'checking','started':time.time(),'signals':[],'data_roles_signalled':0}
    def report(status,**values):
        events.update(status=status,updated=time.time(),**values);write_json(directory/'controller-progress.json',events)
    def send(value,sig):
        same(value);os.kill(value['pid'],sig)
        events['signals'].append(dict(pid=value['pid'],signal=sig.name,at=time.time()))
    try:
        for value in record['local_copy_roles']:same(value)
        write_json(directory/'quiesce-intent.json',dict(status='active',control=str(control),old_producer=record['old_producer']))
        for reader in record['source_readers']:
            send(reader,signal.SIGSTOP);stopped.append(reader)
        report('draining_source_ipc')
        deadline=time.monotonic()+75;boundary=None
        while time.monotonic()<deadline:
            try:
                for reader in stopped:stopped_threads(reader['pid'])
                state=read_json(control/'state.json')
                if queue_barrier(state,floor):
                    boundary=observe_boundary(parent,stopped,cfg['source']);break
            except NotQuiescent:pass
            time.sleep(.05)
        if boundary is None:raise RuntimeError('source IPC/main-loop/probe drain gate timed out; resuming old readers')
        send(parent,signal.SIGSTOP);foreground_stopped=True
        deadline=time.monotonic()+10
        while True:
            try:stopped_threads(parent['pid']);break
            except NotQuiescent:
                if time.monotonic()>deadline:raise RuntimeError('foreground did not freeze')
                time.sleep(.01)
        sockets=unix_queues({p[k] for p in boundary['ipc'] for k in ('child_inode','parent_inode')})
        for pair in boundary['ipc']:
            child=sockets[pair['child_inode']];peer=sockets[pair['parent_inode']]
            if child['send_bytes'] or peer['receive_bytes']:raise RuntimeError('source IPC refilled before freeze')
        state=read_json(control/'state.json')
        if not queue_barrier(state,floor):raise RuntimeError('drained counters changed before freeze')
        if queued_count(control/'stages/p3-r3')!=(0,0):raise RuntimeError('P3 payload queue is not empty')
        dbpath=Path('/tmp')/(cfg['run_id']+'-broker.sqlite')
        with sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True) as db:
            counts=dict(db.execute('SELECT status,COUNT(*) FROM claims WHERE stage=? GROUP BY status',('p3-r3',)))
        if counts!={'done':floor}:raise RuntimeError('P3 lease authority differs from prepared completed prefix')
        for value in record['local_copy_roles']:same(value)
        status=(Path('/proc')/str(parent['pid'])/'status').read_text()
        caught=dict(line.split(':',1) for line in status.splitlines() if ':' in line)
        if not int(caught['SigCgt'],16)&(1<<(signal.SIGINT-1)):raise RuntimeError('foreground has no SIGINT handler')
        write_json(directory/'frozen-state.json',state);write_json(directory/'boundary.json',boundary)
        report('foreground_graceful_unwind',boundary_verified=True)
        send(parent,signal.SIGINT);interrupted=True
        send(parent,signal.SIGCONT);foreground_stopped=False
        deadline=time.monotonic()+90
        while time.monotonic()<deadline:
            for reader in stopped:
                try:value=identity(reader['pid'])
                except FileNotFoundError:continue
                if value['state'] not in ('Z','X') and pending_term(reader['pid']):
                    try:send(reader,signal.SIGCONT)
                    except (FileNotFoundError,NotQuiescent):
                        try:retired=identity(reader['pid'])
                        except FileNotFoundError:continue
                        if retired['start_ticks']!=reader['start_ticks']:raise
                        # A dying task can clear cmdline before becoming Z.
                        # Do not signal an uncertain task; observe the next poll.
            try:old=identity(parent['pid'])
            except FileNotFoundError:old={'state':'exited'}
            if old['state'] in ('Z','X','exited'):break
            time.sleep(.05)
        else:raise RuntimeError('graceful producer exit is incomplete; preserve evidence, do not relaunch')
        manifest=control/'manifests/recovery-missing.jsonl'
        if not manifest.stat().st_size>=ready['offset']:raise RuntimeError('flushed manifest shrank')
        current=read_json(control/'state.json')
        if not queue_barrier(current,floor):raise RuntimeError('final drained counters changed')
        if record['seed_helper'].get('pid'):
            try:same(record['seed_helper'])
            except (FileNotFoundError,NotQuiescent):
                if not Path(record['seed_helper']['result_path']).exists():raise
        for value in record['local_copy_roles']:same(value)
        options=read_json(directory/'options.json')
        prepared=read_json(Path(options['assignment_preparation']))
        descriptor=Path(prepared['assignment_checkpoint']);meta=read_json(descriptor)
        delta=descriptor.parent/'assignment-delta.jsonl'
        empty=directory/'preserved-warm.jsonl'
        with open(empty,'xb') as stream:stream.flush();os.fsync(stream.fileno())
        def inherited(v):return dict(pid=v['pid'],starttime_ticks=v['start_ticks'],argv=v['argv'])
        helper=inherited(record['seed_helper']);helper['result_path']=record['seed_helper']['result_path']
        handover=dict(control=str(control),stage='p3-r3',old_producer=inherited(parent),seed_helper=helper,
          assigned_tasks_path=meta['base_ledger'],assignment_checkpoint=str(descriptor),
          assignment_prefix_sha256=meta['prefix_sha256'],assignment_delta_path=str(delta),
          assignment_delta_sha256=hashlib.sha256(delta.read_bytes()).hexdigest(),next_task_id=floor,
          warm_manifest=str(empty),missing_manifest=str(manifest),preserved_enqueued_batches=floor)
        write_json(directory/'handover.json',handover)
        active=sorted({f['rel'] for f in boundary['directory_fds']})
        write_json(directory/'cutover.json',dict(control=str(control),old_producer=record['old_producer'],
          same_pass_quiescence_and_flush_verified=True,next_task_id=floor,manifest_size=manifest.stat().st_size,
          active_directories=active,at=time.time()))
        report('cutover_issued',manifest_size=manifest.stat().st_size,standby_pid=standby['pid'])
    except BaseException:
        report('failed_after_interrupt' if interrupted else 'aborted_before_interrupt',error=traceback.format_exc())
        raise
    finally:
        if not interrupted:
            if foreground_stopped:send(parent,signal.SIGCONT)
            for reader in stopped:
                try:send(reader,signal.SIGCONT)
                except FileNotFoundError:pass
            write_json(directory/'quiesce-intent.json',dict(status='aborted',control=str(control),old_producer=record['old_producer']))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('mode',choices=['stage','apply'])
    parser.add_argument('--directory',required=True);args=parser.parse_args()
    directory=Path(args.directory).resolve();os.umask(0o077)
    with open('/tmp/'+directory.parent.name[1:]+'-fast-scan-control.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        (stage if args.mode=='stage' else apply)(directory)


if __name__=='__main__':main()
