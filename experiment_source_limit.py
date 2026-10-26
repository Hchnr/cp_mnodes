#!/usr/bin/env python3
"""Detached source-dispatch-only trial; always restore its own initial budget.

Does not signal/restart any process or change copy/probe concurrency. Retiring
source readers finish their current directory, so busy may exceed the lowered
dispatch limit. Never call that state a pure lower-concurrency sample.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import sys
import time
import uuid


def restoration(current, old, operation, control, run_id, target=64):
    if current == old:
        return current
    if (current.get('operation_id')!=operation or current.get('processes')!=target
            or current.get('control')!=str(control) or current.get('run_id')!=run_id
            or current.get('scope')!='warm-source-reader-limit-v1'):
        return None
    result=dict(current,processes=old['processes']);result.pop('operation_id',None)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control',required=True,type=Path)
    parser.add_argument('--bundle',required=True,type=Path)
    parser.add_argument('--experiment',required=True,type=Path)
    parser.add_argument('--seconds',type=int,default=90)
    parser.add_argument('--target',type=int,default=64)
    args=parser.parse_args()
    control,bundle,directory=args.control.resolve(),args.bundle.resolve(),args.experiment.resolve()
    if bundle.parent!=control or directory.parent!=bundle or not directory.name.startswith('source-limit-experiment-'):
        raise ValueError('require explicit private experiment within the active source bundle')
    if not 30<=args.seconds<=180:raise ValueError('trial window must be bounded')
    if args.target not in (40,48,56,64):raise ValueError('trial target must be a prepared bounded increase')
    sys.path.insert(0,str(bundle))
    from migration_io import read_json,write_json
    from migration_scan_boundary import require_identity
    cfg=read_json(control/'config.json');expected=read_json(bundle/'standby-role.json')
    policy=bundle/'reader-limit.json';old=read_json(policy)
    if (old.get('control')!=str(control) or old.get('run_id')!=cfg['run_id']
            or old.get('scope')!='warm-source-reader-limit-v1' or old.get('processes')!=32
            or old.get('operation_id')):raise ValueError('expected unowned 32-reader baseline')
    def guard():
        require_identity(expected)
        state=read_json(control/'state.json')
        if (state.get('foreground_handover')!=str(bundle/'handover.json') or state.get('status')!='scanning'
                or state.get('source_reader_progress',{}).get('processes')!=64
                or state.get('recovery_warm') is not True):
            raise RuntimeError('source phase or process changed')
        return state
    operation=uuid.uuid4().hex;events=dict(operation_id=operation,control=str(control),bundle=str(bundle),
        initial=old,target=args.target,points=[],copy_processes_signalled=0,started=time.time())
    def save():write_json(directory/'result.json',events)
    def sample(label):
        state=guard();r=state['source_reader_progress']
        p4=Path(read_json(control/'extension-plan.json')['parallel_execution']['control'])
        metrics=read_json(p4/'metrics.json')
        value=dict(phase=label,time=time.time(),state_updated=state['updated'],source=r,
                   target=state['target_filter'],p4_sample_time=metrics.get('sample_time'),
                   p4_copied=metrics.get('nodebytes'),p4_streamed=metrics.get('streamed_bytes'))
        events['points'].append(value);save()
    def stop(*_):raise KeyboardInterrupt('source trial interrupted')
    for sig in (signal.SIGHUP,signal.SIGINT,signal.SIGTERM):signal.signal(sig,stop)
    lock_path='/tmp/'+cfg['run_id']+'-source-limit-experiment.lock'
    with open(lock_path,'a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        changed=False
        try:
            guard();sample('baseline32')
            for _ in range(6):time.sleep(10);sample('baseline32')
            changed=True  # A failed publication can have an uncertain outcome.
            events['intent_to_switch_at']=time.time();save()
            write_json(policy,dict(old,processes=args.target,operation_id=operation))
            events['switched_at']=time.time();save()
            until=time.monotonic()+45
            while guard()['source_reader_progress'].get('reader_limit')!=args.target:
                if time.monotonic()>=until:raise RuntimeError('source limit hot application timed out')
                time.sleep(1)
            until=time.monotonic()+args.seconds
            while time.monotonic()<until:
                sample('trial'+str(args.target));time.sleep(5)
            events['status']='sampled'
        except BaseException as exc:
            events.update(status='interrupted',error=type(exc).__name__+': '+str(exc))
            raise
        finally:
            for sig in (signal.SIGHUP,signal.SIGINT,signal.SIGTERM):signal.signal(sig,signal.SIG_IGN)
            if changed:
                current=read_json(policy)
                restored=restoration(current,old,operation,control,cfg['run_id'],args.target)
                if restored is None:
                    events.update(restored=False,restore_error='external source policy changed; refusing overwrite')
                else:
                    write_json(policy,restored);events['restored']=True
            else:events['restored']=True
            events['finished']=time.time();save()
    print(json.dumps({k:v for k,v in events.items() if k!='points'}),flush=True)


if __name__=='__main__':main()
