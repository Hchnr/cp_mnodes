#!/usr/bin/env python3
"""One-shot host-local replacement of an identified read-only probe only."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from deploy_pipelined_producer import process_identity, require_identity, wait_exited, wait_lock_released
from migration_io import read_json, write_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--control',required=True);p.add_argument('--bundle',required=True)
    a=p.parse_args();c=Path(a.control).resolve();b=Path(a.bundle).resolve()
    if not b.is_relative_to(c):raise ValueError('bundle outside control')
    cfg=read_json(c/'config.json');node=socket.gethostname().split(cfg['job']+'-',1)[1]
    folder=b/'node-restarts'/node;old=read_json(folder/'probe.json')
    if old.get('control')!=str(c) or old.get('slots')!=32 or (c/'STOP').exists():
        raise RuntimeError('unexpected probe or STOP')
    require_identity(old)
    if not any(x.endswith('/migration_target_probe.py') for x in old['argv']):
        raise RuntimeError('not a probe')
    script=b/'probe-capacity64.py'
    if hashlib.sha256(script.read_bytes()).hexdigest()!=read_json(b/'probe-capacity64-source.json')['sha256']:
        raise RuntimeError('probe code changed')
    lock=open('/tmp/'+cfg['run_id']+'-probe-rollout.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    with open(folder/'capacity64-attempt.json','x') as handle:json.dump({'old':old,'epoch':time.time()},handle)
    argv=list(old['argv']);argv[2]=str(script);argv[argv.index('--slots')+1]='64'
    argv.extend(['--stop-cache-seconds','0.1'])
    require_identity(old);os.kill(old['pid'],signal.SIGTERM)
    wait_exited(old,30);wait_lock_released('/tmp/'+cfg['run_id']+'-targetprobe.lock',30)
    with open(folder/'capacity64.log','ab',buffering=0) as log:
        child=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,
            stdout=log,stderr=subprocess.STDOUT,start_new_session=True,
            env=dict(os.environ,PYTHONPATH=str(b)+':'+str(c)))
    write_json(folder/'capacity64-launch.json',{'pid':child.pid,'argv':argv,'epoch':time.time()})
    for _ in range(100):
        if child.poll() is not None:raise RuntimeError('new probe exited; inspect log, do not retry')
        role=read_json(folder/'probe.json')
        if role.get('pid')==child.pid and role.get('slots')==64 and role.get('status')=='ready':
            print(json.dumps({'node':node,'pid':child.pid,'slots':64,'status':'ready'}),flush=True);return
        time.sleep(.2)
    raise RuntimeError('readiness unknown; inspect, do not retry')


if __name__=='__main__':main()
