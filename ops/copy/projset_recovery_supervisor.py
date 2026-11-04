#!/usr/bin/env python3
"""Fixed-anchor recovery supervisor. Standby may restart the anchor, never steal it."""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import time
import uuid


def save(path, value):
    tmp = path.with_suffix('.tmp.'+uuid.uuid4().hex)
    with tmp.open('x') as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp,path)


def ssh(cfg, node, command):
    return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=4','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+cfg['known_hosts'],'-i',cfg['key'],cfg['hosts'][node],command]


FENCE = r'''
import json,os,signal,sys,time
from pathlib import Path
root=Path(sys.argv[1]);node=sys.argv[2];boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
for path in [root/'nodes'/node/'launch.json']+list((root/'nodes'/node).glob('attempt-*/launch.json')):
    state=json.loads(path.read_text());pid=state['pid']
    def matches():
        try:
            fields=Path('/proc/%d/stat'%pid).read_text().rsplit(') ',1)[1].split()
            argv=Path('/proc/%d/cmdline'%pid).read_bytes().split(b'\0')
            return state['boot']==boot and state['start']==fields[19] and fields[0]!='Z' and str(root).encode() in argv and b'worker' in argv and node.encode() in argv and state['argv'][0].encode() in argv
        except FileNotFoundError:return False
    if matches():
        os.kill(pid,signal.SIGTERM)
        for _ in range(100):
            if not matches():break
            time.sleep(.1)
        if matches():os.kill(pid,signal.SIGKILL)
        for _ in range(100):
            if not matches():break
            time.sleep(.1)
        if matches():raise RuntimeError('cannot fence worker')
print('fenced '+node)
'''


def detach(script, run, mode):
    return "import subprocess; subprocess.Popen("+repr([sys.executable,'-S','-B',str(script),mode,'--run',str(run)])+",stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True,cwd='/')"


def process_identity(pid):
    return dict(pid=pid,start=Path('/proc/%d/stat'%pid).read_text().rsplit(') ',1)[1].split()[19],host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def fence_local(previous, script, role=None):
    if previous.get('host') != socket.gethostname() or previous.get('boot') != Path('/proc/sys/kernel/random/boot_id').read_text().strip():
        return
    pid=previous['pid']
    def matches():
        try:
            fields=Path('/proc/%d/stat'%pid).read_text().rsplit(') ',1)[1].split()
            argv=Path('/proc/%d/cmdline'%pid).read_bytes().split(b'\0')
            return fields[0]!='Z' and fields[19]==previous.get('start') and str(script).encode() in argv and (role is None or role.encode() in argv)
        except FileNotFoundError:return False
    if matches():
        os.kill(pid,signal.SIGTERM)
        for _ in range(100):
            if not matches():return
            time.sleep(.1)
        if matches():os.kill(pid,signal.SIGKILL)
        for _ in range(100):
            if not matches():return
            time.sleep(.1)
        raise RuntimeError('old local process did not exit')


def supervise(run):
    root=run/'recovery'
    cfg=json.loads((root/'ssh.json').read_text())
    if socket.gethostname()!=cfg['hosts']['cpu-master-0']:
        raise RuntimeError('supervisor must run on the fixed anchor')
    lock=open('/var/tmp/'+run.name+'-supervisor.lock','a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:return
    save(root/'supervisor-launch.json',dict(pid=os.getpid(),host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),started=time.time()))
    child_path=root/'controller-child.json'
    previous = json.loads((child_path if child_path.exists() else run/'controller.json').read_text())
    fence_local(previous, root/'code/projset_recovery_copy.py', 'controller')
    for name in ('swanlab','outcomes'):
        path=root/(name+'-child.json')
        if path.exists():
            fence_local(json.loads(path.read_text()), root/'code'/('projset_'+name+'.py'))
    env=dict(os.environ,PROJSET_SSH_TOPOLOGY=str(root/'ssh.json'))
    sidecars={}
    reporter=[cfg['reporter_python'],'-B',str(root/'code/projset_swanlab.py'),'--run',str(run),'--interval','120']
    exporter=[sys.executable,'-S','-B',str(root/'code/projset_outcomes.py'),'--run',str(run),'--output',cfg['outcomes'],'--watch','--interval','120']
    controller=None
    retry_at=0
    attempt=None
    sidecar_next={}
    standby_next=0
    while True:
        if (run/'STOP').exists():
            if controller and controller.poll() is None:
                # Controller sees STOP itself and drains its leases.
                pass
            else:break
        now=time.time()
        if now>=standby_next:
            command='python3 -S -c '+shlex.quote(detach(Path(__file__).resolve(),run,'standby'))
            try:
                subprocess.run(ssh(cfg,'cpu-worker-0',command),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=40)
            except subprocess.TimeoutExpired:pass
            standby_next=now+120
        if not controller or controller.poll() is not None:
            if controller and controller.returncode==0:
                break
            if now>=retry_at:
                save(root/'supervisor.json',dict(state='fencing',updated=now,previous_returncode=controller.returncode if controller else None))
                try:
                    def fence(node):
                        result=subprocess.run(ssh(cfg,node,'python3 -S -c '+shlex.quote(FENCE)+' '+shlex.quote(str(run))+' '+shlex.quote(node)),capture_output=True,text=True,timeout=65)
                        if result.returncode:raise RuntimeError('fence failed '+node+': '+result.stderr[-300:])
                    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                        list(pool.map(fence,cfg['hosts']))
                    attempt=time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+uuid.uuid4().hex[:8]
                    status=json.loads((run/'status.json').read_text())
                    status.update(state='recovering',updated=time.time(),nodes_ready=0,active=0)
                    save(run/'status.json',status)
                    log=open(root/('controller.'+attempt+'.log'),'a')
                    controller=subprocess.Popen([sys.executable,'-S','-B',str(root/'code/projset_recovery_copy.py'),'controller','--run',str(run)],stdin=subprocess.DEVNULL,stdout=log,stderr=log,env=dict(env,PROJSET_ATTEMPT=attempt),start_new_session=True,cwd='/')
                    log.close()
                    try:
                        start=Path('/proc/%d/stat'%controller.pid).read_text().rsplit(') ',1)[1].split()[19]
                        save(root/'controller-child.json',dict(pid=controller.pid,start=start,host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),attempt=attempt))
                    except FileNotFoundError:pass
                    retry_at=now+30
                except Exception as error:
                    save(root/'last-error.json',dict(updated=time.time(),error=str(error)))
                    controller=None
                    retry_at=now+60
        if controller and controller.poll() is None:
            current=json.loads((run/'status.json').read_text())
            limit=1800 if current['state']=='recovering' else 600
            if time.time()-current['updated']>limit:
                controller.terminate()
        # Sidecars have their own locks and resume their existing run/checkpoints.
        for name,argv in [('swanlab',reporter),('outcomes',exporter)]:
            child=sidecars.get(name)
            if (not child or child.poll() is not None) and now>=sidecar_next.get(name,0):
                log=open(root/(name+'.log'),'a')
                sidecars[name]=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True,cwd='/')
                log.close()
                try:
                    save(root/(name+'-child.json'),process_identity(sidecars[name].pid))
                except FileNotFoundError:pass
                sidecar_next[name]=now+120
        save(root/'supervisor.json',dict(state='running' if controller and controller.poll() is None else 'retrying',updated=time.time(),pid=os.getpid(),attempt=attempt,controller_pid=controller.pid if controller else None,sidecars={n:p.pid for n,p in sidecars.items()}))
        time.sleep(10)
    save(root/'supervisor.json',dict(state='finished',updated=time.time(),pid=os.getpid()))


def standby(run):
    cfg=json.loads((run/'recovery/ssh.json').read_text())
    lock=open('/var/tmp/'+run.name+'-standby.lock','a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:return
    while not (run/'STOP').exists():
        status=json.loads((run/'status.json').read_text())
        if status['state'] in ('copy_pass_finished','copied_with_attention','stopped'):return
        # An unreachable anchor does not authorize a competing controller.
        command='python3 -S -c '+shlex.quote(detach(Path(__file__).resolve(),run,'supervise'))
        try:
            reachable=subprocess.run(ssh(cfg,'cpu-master-0',command),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=40).returncode==0
        except subprocess.TimeoutExpired:
            reachable=False
        save(run/'recovery/standby.json',dict(updated=time.time(),host=socket.gethostname(),pid=os.getpid(),anchor_reachable=reachable))
        time.sleep(30)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=['supervise','standby']);p.add_argument('--run',type=Path,required=True);a=p.parse_args()
    if a.mode=='supervise':supervise(a.run)
    else:standby(a.run)


if __name__=='__main__':main()
