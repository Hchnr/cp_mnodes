#!/usr/bin/env python3
"""Resume stopped copy lanes and probes; never manipulate training processes."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

from migration_io import read_json, write_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--control',required=True);p.add_argument('--bundle',required=True)
    args=p.parse_args();c=Path(args.control);b=Path(args.bundle);cfg=read_json(c/'config.json')
    host=socket.gethostname();node=host.split(cfg['job']+'-',1)[1]
    if cfg.get('training_watchdog_enabled') is not False:
        raise RuntimeError('training coupling has not been disabled')
    lock=open('/tmp/'+cfg['run_id']+'-resume-node.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    processes=[]
    for root in Path('/proc').iterdir():
        if not root.name.isdigit():continue
        try:
            argv=(root/'cmdline').read_bytes().decode().rstrip('\0').split('\0')
            state=(root/'stat').read_text().rsplit(') ',1)[1].split()[0]
            if str(c) in argv and state not in ('Z','X'):processes.append((int(root.name),argv))
        except (OSError,UnicodeError):pass
    for pid,argv in processes:
        if any(x.endswith('/migration_watchdog.py') for x in argv):
            os.kill(pid,signal.SIGTERM)
    if (c/'STOP').exists():raise RuntimeError('STOP still present')
    if any(any(x.endswith('/migration_cached_worker.py') or x.endswith('/migration_worker128_run.py')
               or (x.endswith('/migration_run.py') and 'worker' in argv) for x in argv)
           for pid,argv in processes):
        raise RuntimeError('copy worker already live; inspect, do not duplicate')
    folder=b/'node-restarts'/node;folder.mkdir(parents=True,exist_ok=True)
    with open(folder/'attempt.json','x') as handle:json.dump({'time':time.time(),'host':host},handle)
    soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(65536,hard))
    def launch(name,argv):
        env=dict(os.environ,PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-resume-'+name)
        with open(folder/(name+'.log'),'ab',buffering=0) as log:
            child=subprocess.Popen(['nice','-n','15','ionice','-c','3',sys.executable,'-u',*argv],
                stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
        write_json(folder/(name+'-launch.json'),{'pid':child.pid,'argv':argv,'time':time.time()})
        return child
    endpoints=cfg['target_probe_endpoints']
    if isinstance(endpoints,dict):
        endpoint=endpoints.get(host,endpoints.get(node))
    else:
        nodes=sorted(['master-0']+['worker-'+str(i) for i in range(11)])
        if len(endpoints)!=len(nodes):raise RuntimeError('unexpected endpoint count')
        endpoint=endpoints[nodes.index(node)]
    if isinstance(endpoint,dict):endpoint=endpoint['url']
    url=urlsplit(endpoint)
    if not any(any(x.endswith('/migration_target_probe.py') for x in argv) for pid,argv in processes):
        probe=launch('probe',[str(b/'migration_target_probe.py'),'--control',str(c),'--port',str(url.port),
            '--advertise-address',url.hostname,'--slots','32','--record',str(folder/'probe.json')])
        for _ in range(100):
            if probe.poll() is not None:raise RuntimeError('probe exited; inspect log')
            try:
                health=read_json(folder/'probe.json')
                if health.get('pid')==probe.pid and health.get('status')=='ready':break
            except FileNotFoundError:pass
            time.sleep(.2)
        else:raise RuntimeError('probe readiness unknown')
    primary=launch('primary',[str(c/'migration_cached_worker.py'),'primary','--control',str(c),
        '--threads','64','--cache-max-age','30'])
    for _ in range(120):
        if primary.poll() is not None:raise RuntimeError('primary exited; inspect log')
        value=read_json(c/'nodes'/(host+'.json'))
        if value.get('pid')==primary.pid and not value.get('ended'):break
        time.sleep(.5)
    else:raise RuntimeError('primary heartbeat not ready')
    extra=launch('supplement',[str(c/'migration_cached_worker.py'),'supplement','--control',str(c),
        '--threads','64','--cache-max-age','30','--nice','0'])
    time.sleep(2)
    if extra.poll() is not None:raise RuntimeError('supplement exited; inspect log')
    print(json.dumps({'node':node,'primary':primary.pid,'supplement':extra.pid,'status':'started'}),flush=True)


if __name__=='__main__':main()
