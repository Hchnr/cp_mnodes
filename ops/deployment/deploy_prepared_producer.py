#!/usr/bin/env python3
"""One-shot handover from an active prepared producer to a prepared code bundle.

Only --apply signals the explicitly saved foreground producer.  Preflight
validates and rehearses the cache while it continues feeding copy workers.
The broker, copy workers and leases remain untouched.  There is no training
dependency.  An ambiguous apply attempt must be inspected, never auto-retried.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

import deploy_pipelined_producer as base
from deploy_interleaved_producer import reconcile_delta
from migration_io import read_json, write_json
from migration_pipelined_target_filter import ensure_producer_nofile
from migration_prepared_resume import digest_file


def unresolved_cancelled(previous, delta, control):
    """Carry only STOP-cancelled paths not reserved by the current producer.

The predecessor handover's retry evidence is immutable.  A task at or above
its starting next_task_id proves that producer has already retried/reserved a
path.  Blindly inheriting the old retry list would remove those valid new
reservations and could copy the same path twice after every handover.
"""
    if not previous.get('cancelled_retry_path'):
        return []
    path = Path(previous['cancelled_retry_path']).resolve()
    if (not path.is_relative_to(control)
            or digest_file(path) != previous['cancelled_retry_sha256']):
        raise RuntimeError('predecessor cancellation evidence changed')
    retry = {entry['rel']:entry for entry in read_json(path)['entries']}
    with open(delta, encoding='utf-8') as handle:
        for line in handle:
            task = json.loads(line)
            if task['id'] >= previous['next_task_id']:
                for entry in task['entries']:
                    retry.pop(entry['rel'], None)
    return sorted(retry.values(), key=lambda entry:entry.get('size',0), reverse=True)


def run(bundle, *, apply=False, entry='migration_interleaved_target_filter.py', producer_args=()):
    bundle = Path(bundle).resolve()
    operation_match = re.fullmatch(r'pipelined-producer-([0-9a-f]{32})', bundle.name)
    if operation_match is None or Path(entry).name != entry or not entry.endswith('.py'):
        raise ValueError('a unique producer bundle and local Python entrypoint are required')
    operation = operation_match.group(1)
    meta = read_json(bundle/'prepared.json')
    control = Path(meta['control']).resolve()
    if not bundle.is_relative_to(control):
        raise ValueError('bundle must belong to its control directory')
    cfg = read_json(control/'config.json')
    state = base.guarded_state(control, read_json)
    expected = read_json(bundle/'expected-producer.json')
    old = base.role_identity(expected)
    role = read_json(control/'pipeline-deployment.json')
    if (base.role_identity(role) != old or role.get('control') != str(control)
            or role.get('host') != socket.gethostname()
            or socket.gethostname() != 'job-'+cfg['job']+'-master-0'
            or state.get('foreground_handover') != meta['source_handover']
            or state.get('active_manifest') != meta['source_manifest']):
        raise RuntimeError('saved foreground identity or prepared input binding changed')
    base.require_identity(old)
    previous = read_json(meta['source_handover'])
    if (not previous.get('prepared_resume')
            or previous['assigned_tasks_path'] != meta['base_ledger']
            or previous['ledger_sha256'] != meta['base_ledger_sha256']):
        raise RuntimeError('handover must preserve the immutable historical assignment prefix')
    broker = base.process_identity(read_json(control/'broker.json')['pid'])
    if (not broker or broker['state'] in ('Z','X')
            or not any(arg.endswith('/migration_broker_parallel.py') for arg in broker['argv'])):
        raise RuntimeError('original broker is unavailable')
    base.require_identity(broker)
    base.broker_control(cfg,20)
    hashes = read_json(bundle/'source-sha256.json')['files']
    if entry not in hashes:
        raise RuntimeError('entrypoint is not included in the immutable code digest')
    for name, digest in hashes.items():
        if Path(name).name != name or digest_file(bundle/name) != digest:
            raise RuntimeError('code digest changed: '+name)
    if digest_file(meta['manifest']) != meta['manifest_sha256']:
        raise RuntimeError('prepared manifest prefix digest mismatch')
    rehearsal = dict(previous, prepared_resume=str(bundle/'prepared.json'),
                     predecessor_handover=meta['source_handover'])
    env = dict(os.environ, PYTHONPATH=str(bundle)+os.pathsep+str(control),
               PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-'+bundle.name)
    validation_run = subprocess.run([sys.executable,'-c',
        'import json,sys,os,time; from migration_prepared_resume import load_checkpoint; '
        't=time.monotonic(); m,v=load_checkpoint(json.loads(sys.argv[1]),sys.argv[2]); '
        'print(json.dumps({"loaded_paths":len(v["seeded_paths"]),"seconds":time.monotonic()-t}),flush=True); os._exit(0)',
        json.dumps(rehearsal),str(control)],env=env,check=True,capture_output=True,text=True,timeout=180)
    validation = json.loads(validation_run.stdout.strip())
    print(json.dumps({'status':'cache_rehearsed','validation':validation}),flush=True)
    if not apply:
        return validation
    locks = []
    for suffix in ('-parallel-target-dispatch.lock','-cache-age-dispatch.lock',
                   '-interleaved-dispatch.lock','-prepared-dispatch.lock'):
        lock = open('/tmp/'+cfg['run_id']+suffix,'a')
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        locks.append(lock)
    marker = bundle/'handover-attempt.json'
    with open(marker,'x') as handle:
        json.dump({'old':old,'created':base.now()},handle)
        handle.flush();os.fsync(handle.fileno())
    record = dict(version=1,operation=operation,status='prepared',control=str(control),
                  bundle=str(bundle),old_producer=old,validation=validation,started=base.now(),
                  workers_broker_training_signalled=False)
    base.durable_json(bundle/'deployment.json',record,write_json)
    base.guarded_state(control,read_json)
    base.require_identity(old);base.require_identity(broker);base.broker_control(cfg,20)
    limits = ensure_producer_nofile()
    record.update(status='terminating_old',termination_requested=base.now())
    base.durable_json(bundle/'deployment.json',record,write_json)
    os.kill(old['pid'],signal.SIGTERM)
    base.wait_exited(old,60)
    base.wait_lock_released('/tmp/'+cfg['run_id']+'-coordinator.lock',30)
    frozen = base.guarded_state(control,read_json)
    db = base.claims_reader(Path('/tmp')/(cfg['run_id']+'-broker.sqlite'))
    ledger = base.AssignmentLedger(bundle/'assignment-delta.jsonl')
    try:
        # ALL IDs after the immutable base, including holes behind MAX(id).
        # Pending first, then claims covers pending->leased transitions.
        reconcile_delta(control,db,ledger,meta['base_next_task_id'])
        delta = ledger.close()
    finally:
        db.close()
    next_id = max(meta['base_next_task_id'],frozen.get('next_task_id',0),delta['maximum_id']+1)
    handover = dict(previous,old_producer=old,created=base.now(),operation=operation,
                    predecessor_handover=meta['source_handover'],
                    prepared_resume=str(bundle/'prepared.json'),
                    assignment_delta_path=str(ledger.path), assignment_delta_sha256=delta['sha256'],
                    missing_manifest=meta['source_manifest'],next_task_id=next_id)
    handover.pop('cancelled_retry_path',None)
    handover.pop('cancelled_retry_sha256',None)
    retries = unresolved_cancelled(previous,ledger.path,control)
    if retries:
        retry_path = bundle/'cancelled-retry.json'
        base.durable_json(retry_path,{'entries':retries},write_json)
        handover.update(cancelled_retry_path=str(retry_path),
                        cancelled_retry_sha256=digest_file(retry_path))
    base.durable_json(bundle/'handover.json',handover,write_json)
    baseline = read_json(Path(meta['source_handover']).parent/'frozen-checkpoint.json')
    checkpoint_state = dict(frozen)
    probe = dict(frozen.get('distributed_target_probe',{}))
    counts = dict(probe.get('counts',{}))
    counts['remote_existing'] = counts.get('remote_existing',0)+baseline.get('frozen_state',{}).get('distributed_target_probe',{}).get('counts',{}).get('remote_existing',0)
    probe['counts']=counts;checkpoint_state['distributed_target_probe']=probe
    base.durable_json(bundle/'frozen-checkpoint.json',dict(record,status='frozen_checkpoint',
                       handover=handover,frozen_state=checkpoint_state,delta=delta,
                       base_prepared=meta,old_producer=old),write_json)
    record.update(status='launching_once',next_task_id=next_id,delta=delta,
                  old_exited=base.now(),producer_nofile=list(limits))
    base.durable_json(bundle/'deployment.json',record,write_json)
    argv=[sys.executable,'-u',str(bundle/entry),'run','--control',str(control),
          '--handover',str(bundle/'handover.json'),'--publish-workers','8','--publish-limit','64',
          *producer_args]
    with open(bundle/'producer.log','ab',buffering=0) as log:
        child=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,
                               stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
    for _ in range(200):
        actual=base.process_identity(child.pid)
        if actual and actual['argv']==argv and actual['state'] not in ('Z','X'):
            break
        if child.poll() is not None:
            raise RuntimeError('new producer exited; inspect the one-shot record, do not retry')
        time.sleep(.05)
    else:
        raise RuntimeError('new identity uncertain; inspect the one-shot record, do not retry')
    role=dict(control=str(control),host=socket.gethostname(),bundle=str(bundle),**actual)
    base.durable_json(control/'pipeline-deployment.json',role,write_json)
    record.update(status='loading_prepared_checkpoint',new_producer=actual,launched=base.now())
    base.durable_json(bundle/'deployment.json',record,write_json)
    base.require_identity(broker)
    print(json.dumps(record),flush=True)
    return record


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',required=True)
    parser.add_argument('--entry',default='migration_interleaved_target_filter.py')
    parser.add_argument('--producer-args-json',default='[]')
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    extra=json.loads(args.producer_args_json)
    if not isinstance(extra,list) or not all(isinstance(value,str) for value in extra):
        parser.error('--producer-args-json must contain a JSON string array')
    os.umask(0o077)
    run(args.bundle,apply=args.apply,entry=args.entry,producer_args=extra)
