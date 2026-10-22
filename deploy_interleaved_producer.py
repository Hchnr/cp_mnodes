#!/usr/bin/env python3
"""One-shot prepared handover; only the explicitly bound producer is replaced."""
import argparse
import concurrent.futures
import fcntl
import hashlib
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
from migration_io import read_json, write_json
from migration_pipelined_target_filter import ensure_producer_nofile
from migration_prepared_resume import digest_file, load_checkpoint

EXPECTED_PID = 3576614
EXPECTED_START = '507049137'
ENTRY = 'migration_interleaved_target_filter.py'


def reconcile_delta(control, db, ledger, lower):
    """Capture pending first, then durable claims (including moved pending)."""
    base.collect_pending(control, db, ledger, read_json)
    for task_id, payload in db.execute('SELECT id,payload FROM claims WHERE stage=? AND id>=? ORDER BY id',
                                     ('p3-r3', lower)):
        ledger.add(json.loads(payload), expected_id=task_id)
    if any(task_id < lower for task_id in ledger.hashes):
        raise RuntimeError('unexpected old pending task below prepared assignment prefix')


def cancelled_entries(control, db, lower):
    """Only retry explicit STOP cancellations in durable completed receipts."""
    rows = list(db.execute('SELECT id,status,payload FROM claims WHERE stage=? AND id>=?',
                           ('p3-r3', lower)))
    reserved = set()
    for task_id, status, payload in rows:
        if status != 'done':
            reserved.update(e['rel'] for e in json.loads(payload)['entries'])
    def inspect(row):
        task_id, status, payload = row
        if status != 'done':
            return []
        receipt = read_json(control/'stages/p3-r3/done'/str(task_id%64)/(str(task_id)+'.json'))
        cancelled = {r['rel'] for r in receipt['results']
                     if r.get('status') == 'paused' or r.get('cancelled') is True}
        return [e for e in json.loads(payload)['entries'] if e['rel'] in cancelled]
    retry = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        for entries in pool.map(inspect, rows):
            for entry in entries:
                if entry['rel'] in reserved:
                    raise RuntimeError('cancelled path still has a live reservation')
                retry[entry['rel']] = entry
    return sorted(retry.values(), key=lambda e:e.get('size',0), reverse=True)


def run(bundle, apply=False, resume_training_stop=False):
    bundle = Path(bundle).resolve()
    match = re.fullmatch(r'pipelined-producer-([0-9a-f]{32})', bundle.name)
    if match is None:
        raise ValueError('a unique producer deployment bundle is required')
    operation = match.group(1)
    meta = read_json(bundle/'prepared.json')
    control = Path(meta['control'])
    cfg = read_json(control/'config.json')
    stop_text = 'Training watchdog: a baseline training launcher has exited or restarted\n'
    def checked_state():
        if not resume_training_stop:
            return base.guarded_state(control, read_json)
        state = read_json(control/'state.json')
        if (state.get('stage') != 'p3-r3' or not state.get('recovery_warm')
                or state.get('scan_complete') or read_json(control/'pipeline.json').get('stages')
                or cfg.get('training_watchdog_enabled') is not False):
            raise RuntimeError('authorized stopped recovery prerequisites changed')
        if (control/'STOP').exists() and (control/'STOP').read_text() != stop_text:
            raise RuntimeError('unrecognized STOP; will not override')
        return state
    state = checked_state()
    role = read_json(control/'pipeline-deployment.json')
    old = base.role_identity(role)
    if (old['pid'] != EXPECTED_PID or old['starttime_ticks'] != EXPECTED_START
            or not any(x.endswith('/migration_priority_feed_target_filter.py') for x in old['argv'])
            or socket.gethostname() != 'job-'+cfg['job']+'-master-0'
            or role.get('control') != str(control)
            or state.get('foreground_handover') != meta['source_handover']
            or state.get('active_manifest') != meta['source_manifest']):
        raise RuntimeError('producer/control/prepared checkpoint identity changed')
    if resume_training_stop:
        actual = base.process_identity(old['pid'])
        if actual and actual['state'] not in ('Z', 'X'):
            raise RuntimeError('stopped producer is still live')
    else:
        base.require_identity(old)
    broker = base.process_identity(166974)
    if not broker or broker['starttime_ticks'] != '500376938':
        raise RuntimeError('protected broker identity changed')
    base.require_identity(broker)
    base.broker_control(cfg, 20)
    hashes = read_json(bundle/'source-sha256.json')['files']
    for name, digest in hashes.items():
        if digest_file(bundle/name) != digest:
            raise RuntimeError('code digest changed: '+name)
    previous = read_json(meta['source_handover'])
    if digest_file(meta['manifest']) != meta['manifest_sha256']:
        raise RuntimeError('prepared manifest prefix digest mismatch')
    rehearsal = dict(previous, prepared_resume=str(bundle/'prepared.json'),
                     predecessor_handover=meta['source_handover'])
    env = dict(os.environ, PYTHONPATH=str(bundle)+os.pathsep+str(control),
               PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-'+bundle.name)
    start = time.monotonic()
    # Rehearse cache validation/loading in a separate process while the old
    # producer still feeds workers. Fail before any producer signal.
    check = subprocess.run([sys.executable, '-c',
        'import json,sys,os,time; from migration_prepared_resume import load_checkpoint; '
        't=time.monotonic(); m,v=load_checkpoint(json.loads(sys.argv[1]),sys.argv[2]); '
        'print(json.dumps({"loaded_paths":len(v["seeded_paths"]),"seconds":time.monotonic()-t}),flush=True); os._exit(0)',
        json.dumps(rehearsal), str(control)], env=env, check=True,
        capture_output=True, text=True, timeout=180)
    validation = json.loads(check.stdout.strip())
    print(json.dumps({'status':'cache_rehearsed','validation':validation}), flush=True)
    if not apply:
        return
    locks = []
    for suffix in ('-parallel-target-dispatch.lock','-cache-age-dispatch.lock','-interleaved-dispatch.lock'):
        lock = open('/tmp/'+cfg['run_id']+suffix,'a')
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        locks.append(lock)
    marker = bundle/'handover-attempt.json'
    with open(marker,'x') as handle:
        json.dump({'old':old,'created':base.now()},handle)
        handle.flush();os.fsync(handle.fileno())
    record = {'version':1,'operation':operation,'status':'prepared','control':str(control),'bundle':str(bundle),
              'old_producer':old,'validation':validation,'started':base.now(),
              'workers_broker_training_signalled':False}
    base.durable_json(bundle/'deployment.json',record,write_json)
    checked_state()
    base.broker_control(cfg,20)
    if not resume_training_stop:
        base.require_identity(old)
    base.require_identity(broker)
    limits = ensure_producer_nofile()
    # SIGTERM ends only the foreground producer. Copy leases remain live.
    record.update(status='terminating_old',termination_requested=base.now())
    base.durable_json(bundle/'deployment.json',record,write_json)
    if not resume_training_stop:
        os.kill(old['pid'],signal.SIGTERM)
    base.wait_exited(old,60)
    base.wait_lock_released('/tmp/'+cfg['run_id']+'-coordinator.lock',30)
    frozen = checked_state()
    db = base.claims_reader(Path('/tmp')/(cfg['run_id']+'-broker.sqlite'))
    ledger = base.AssignmentLedger(bundle/'assignment-delta.jsonl')
    try:
        reconcile_delta(control,db,ledger,meta['base_next_task_id'])
        delta = ledger.close()
        retries = cancelled_entries(control,db,meta['base_next_task_id']) if resume_training_stop else []
    finally:
        db.close()
    next_id = max(meta['base_next_task_id'],frozen.get('next_task_id',0),delta['maximum_id']+1)
    handover = dict(previous,old_producer=old,created=base.now(),operation=operation,
                    predecessor_handover=meta['source_handover'],
                    prepared_resume=str(bundle/'prepared.json'),
                    assignment_delta_path=str(ledger.path),
                    assignment_delta_sha256=delta['sha256'],
                    missing_manifest=meta['source_manifest'],next_task_id=next_id)
    if retries:
        retry_path = bundle/'cancelled-retry.json'
        base.durable_json(retry_path, {'entries':retries}, write_json)
        handover.update(cancelled_retry_path=str(retry_path), cancelled_retry_sha256=digest_file(retry_path))
    base.durable_json(bundle/'handover.json',handover,write_json)
    # Carry cumulative target-exists observations through the normal monitor
    # baseline contract, without pretending observations are unique files.
    baseline = read_json(Path(meta['source_handover']).parent/'frozen-checkpoint.json')
    checkpoint_state = dict(frozen)
    probe = dict(frozen.get('distributed_target_probe',{}))
    counts = dict(probe.get('counts',{}))
    counts['remote_existing'] = counts.get('remote_existing',0)+baseline.get('frozen_state',{}).get('distributed_target_probe',{}).get('counts',{}).get('remote_existing',0)
    probe['counts']=counts;checkpoint_state['distributed_target_probe']=probe
    base.durable_json(bundle/'frozen-checkpoint.json',dict(record,status='frozen_checkpoint',
                       handover=handover,frozen_state=checkpoint_state,
                       delta=delta,base_prepared=meta,old_producer=old),write_json)
    record.update(status='launching_once',next_task_id=next_id,delta=delta,
                  old_exited=base.now(),producer_nofile=list(limits))
    base.durable_json(bundle/'deployment.json',record,write_json)
    if resume_training_stop:
        checked_state()
        if (control/'STOP').exists():
            os.rename(control/'STOP', bundle/'STOP.training-ended.archived')
        write_json(control/'state.json',dict(frozen,status='scanning'))
    argv=[sys.executable,'-u',str(bundle/ENTRY),'run','--control',str(control),
          '--handover',str(bundle/'handover.json'),'--publish-workers','8','--publish-limit','64']
    with open(bundle/'producer.log','ab',buffering=0) as log:
        child=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,
                               stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
    actual=None
    for _ in range(200):
        actual=base.process_identity(child.pid)
        if actual and actual['argv']==argv and actual['state'] not in ('Z','X'):break
        if child.poll() is not None:raise RuntimeError('new producer exited; inspect, do not retry')
        time.sleep(.05)
    else:raise RuntimeError('new launch identity uncertain; inspect, do not retry')
    role=dict(control=str(control),host=socket.gethostname(),bundle=str(bundle),**actual)
    base.durable_json(control/'pipeline-deployment.json',role,write_json)
    record.update(status='loading_prepared_checkpoint',new_producer=actual,launched=base.now())
    base.durable_json(bundle/'deployment.json',record,write_json)
    base.require_identity(broker)
    print(json.dumps(record),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle',required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--resume-training-stop',action='store_true')
    args=parser.parse_args()
    os.umask(0o077)
    run(args.bundle,args.apply,args.resume_training_stop)
