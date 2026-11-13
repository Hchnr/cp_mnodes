#!/usr/bin/env python3
"""Fixed-anchor recovery supervisor. Standby may restart the anchor, never steal it."""
import argparse
import concurrent.futures
import fcntl
import json
import math
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


def stop_requested(run):
    """Fail closed on a missing or changed old-cluster fence."""
    generation = os.environ.get('PROJSET_RECOVERY_GENERATION')
    marker = run/'STOP'
    if not generation:
        return marker.exists()
    try:
        fence = json.loads((run/'recovery'/'legacy-cluster-fence.json').read_text())
        info = marker.stat()
        if (fence['generation'] != generation or
                (info.st_ino, info.st_size, info.st_mtime_ns) !=
                (fence['marker_ino'], fence['marker_size'], fence['marker_mtime_ns']) or
                marker.read_text() != fence['marker_content']):
            return True
        return (run/'STOP.requested').exists()
    except (OSError, ValueError, KeyError, TypeError):
        return True


def read_status(path):
    """Return a usable status or an error name, never a guessed state."""
    try:
        status = json.loads(path.read_text())
        if (not isinstance(status, dict) or not isinstance(status.get('state'), str) or
                isinstance(status.get('updated'), bool) or
                not isinstance(status.get('updated'), (int, float)) or
                not math.isfinite(status['updated'])):
            raise ValueError('invalid controller status')
        return status, None
    except (OSError, ValueError, TypeError, OverflowError) as error:
        return None, type(error).__name__


def check_controller_status(controller, status_path, now):
    """Treat a stale rebuild as unverified; it may be inside a long SQL query."""
    status, error = read_status(status_path)
    if status is not None:
        age = now - status['updated']
        if status['state'] == 'recovering':
            # Rebuilding the disposable index can spend over 30 minutes inside
            # one SQLite statement, where it cannot refresh the heartbeat.
            # Killing it would discard the current index, so flag the stale
            # status and let poll() recover only after a confirmed exit.
            if age > 1800:
                return 'RecoveringStatusStale'
        elif age > 600:
            controller.terminate()
    return error


def ssh(cfg, node, command):
    host=cfg['hosts'][node]
    addresses=cfg.get('addresses',{})
    pinned=['-o','HostKeyAlias='+host] if node in addresses else []
    return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=4','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+cfg['known_hosts'],'-i',cfg['key'],*pinned,addresses.get(node,host),command]


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


def lock_path(run, mode):
    name = 'supervisor' if mode == 'supervise' else 'standby'
    return Path('/var/tmp') / (run.name + '-' + name + '.lock')


def acquire_lock(run, mode, inherited_fd=None):
    """Acquire a role lock, or validate the same locked fd passed by a launcher."""
    path = lock_path(run, mode)
    lock = open(path, 'a') if inherited_fd is None else os.fdopen(inherited_fd, 'a')
    try:
        if inherited_fd is not None:
            fd_stat, path_stat = os.fstat(lock.fileno()), path.stat()
            if (fd_stat.st_dev, fd_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino):
                raise RuntimeError('inherited role lock does not match the lock file')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock
    except BaseException:
        lock.close()
        raise


def detach(script, run, mode):
    """Launch only when the role is absent, handing its lock to the child.

    The remote ssh command itself exits after this code runs.  Starting a
    redundant detached child on every health check leaks a zombie if the
    remote container's PID 1 does not reap orphans.
    """
    argv = [sys.executable, '-S', '-B', str(script), mode, '--run', str(run)]
    generation = os.environ.get('PROJSET_RECOVERY_GENERATION')
    generation_env = (',env=dict(os.environ,PROJSET_RECOVERY_GENERATION='+repr(generation)+')'
                      if generation else '')
    return (
        'import fcntl,os,subprocess,sys\n'
        'lock=open('+repr(str(lock_path(run, mode)))+',"a")\n'
        'try:\n'
        ' fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)\n'
        'except BlockingIOError:\n'
        ' sys.exit(0)\n'
        'subprocess.Popen('+repr(argv)+'+["--lock-fd",str(lock.fileno())],'
        'stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,'
        'start_new_session=True,cwd="/",pass_fds=(lock.fileno(),)'+generation_env+')\n'
    )


def process_identity(pid):
    return dict(pid=pid,start=Path('/proc/%d/stat'%pid).read_text().rsplit(') ',1)[1].split()[19],host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def identity_matches(previous, script, role=None):
    """Return True/False, or None when process identity cannot be read safely."""
    try:
        pid = int(previous['pid'])
        if previous.get('host') != socket.gethostname():
            return False
        if previous.get('boot') != Path('/proc/sys/kernel/random/boot_id').read_text().strip():
            return False
        fields = Path('/proc/%d/stat' % pid).read_text().rsplit(') ', 1)[1].split()
        argv = Path('/proc/%d/cmdline' % pid).read_bytes().split(b'\0')
        return (fields[0] != 'Z' and fields[19] == previous.get('start') and
                str(script).encode() in argv and
                (role is None or role.encode() in argv))
    except FileNotFoundError:
        return False
    except (OSError, KeyError, TypeError, ValueError, IndexError):
        return None


class AdoptedProcess:
    """Monitor an identity-checked process whose former supervisor exited."""
    def __init__(self, identity, script, role=None, status_path=None):
        self.identity, self.script, self.role = identity, script, role
        self.status_path = status_path
        self.pid = int(identity['pid'])
        self.returncode = None

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        match = identity_matches(self.identity, self.script, self.role)
        if match is not False:
            return None  # Unknown identity must not trigger a destructive recovery.
        if self.status_path is not None:
            status, error = read_status(self.status_path)
            if error is not None:
                return None  # Cannot distinguish a completed run from a failure yet.
            if (status.get('controller_pid') == self.pid and
                    status['state'] in ('copy_pass_finished', 'copied_with_attention',
                                        'workers_unavailable', 'stopped')):
                self.returncode = 0
                return self.returncode
        self.returncode = 1
        return self.returncode

    def terminate(self):
        if identity_matches(self.identity, self.script, self.role) is True:
            try:
                os.kill(self.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


def adopt(previous, script, role=None, status_path=None):
    if not isinstance(previous, dict) or not isinstance(previous.get('pid'), int):
        return None
    if identity_matches(previous, script, role) is False and status_path is None:
        return None
    return AdoptedProcess(previous, script, role, status_path)


def controller_lock_held(run):
    """Fail closed if an unidentified controller still owns its local lock."""
    run_id = json.loads((run/'config.json').read_text())['run_id']
    with open('/tmp/'+run_id+'-controller.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(lock, fcntl.LOCK_UN)
        return False


def supervise(run, inherited_fd=None):
    root=run/'recovery'
    cfg=json.loads((root/'ssh.json').read_text())
    if socket.gethostname()!=cfg['hosts']['cpu-master-0']:
        raise RuntimeError('supervisor must run on the fixed anchor')
    try:lock=acquire_lock(run, 'supervise', inherited_fd)
    except BlockingIOError:return
    save(root/'supervisor-launch.json',dict(pid=os.getpid(),host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),started=time.time()))
    child_path=root/'controller-child.json'
    previous = json.loads((child_path if child_path.exists() else run/'controller.json').read_text())
    controller = adopt(previous, root/'code/projset_recovery_copy.py', 'controller', run/'status.json')
    sidecars={}
    for name in ('swanlab','outcomes'):
        path=root/(name+'-child.json')
        if path.exists():
            child = adopt(json.loads(path.read_text()), root/'code'/('projset_'+name+'.py'))
            if child is not None:
                sidecars[name] = child
    env=dict(os.environ,PROJSET_SSH_TOPOLOGY=str(root/'ssh.json'))
    if env.get('PROJSET_RECOVERY_GENERATION'):
        env['PROJSET_FAST_REBUILD']='1'
        env['PROJSET_PARALLEL_RECEIPTS']='1'
    reporter=[cfg['reporter_python'],'-B',str(root/'code/projset_swanlab.py'),'--run',str(run),'--interval','120']
    exporter=[sys.executable,'-S','-B',str(root/'code/projset_outcomes.py'),'--run',str(run),'--output',cfg['outcomes'],'--watch','--interval','120']
    retry_at=0
    attempt=previous.get('attempt') if controller is not None else None
    sidecar_next={}
    standby_next=0
    while True:
        if stop_requested(run):
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
                # Never fence workers while any unrecorded controller holds
                # the controller lock; its identity or last status may lag.
                if controller_lock_held(run):
                    save(root/'supervisor.json',dict(state='awaiting_controller_lock',updated=now,pid=os.getpid()))
                    time.sleep(10)
                    continue
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
        status_error=None
        if controller and controller.poll() is None:
            status_error=check_controller_status(controller, run/'status.json', time.time())
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
        heartbeat=dict(state='running' if controller and controller.poll() is None else 'retrying',updated=time.time(),pid=os.getpid(),attempt=attempt,controller_pid=controller.pid if controller else None,sidecars={n:p.pid for n,p in sidecars.items()})
        if status_error is not None:
            heartbeat['status_error']=status_error
        save(root/'supervisor.json',heartbeat)
        time.sleep(10)
    save(root/'supervisor.json',dict(state='finished',updated=time.time(),pid=os.getpid()))


def standby(run, inherited_fd=None):
    cfg=json.loads((run/'recovery/ssh.json').read_text())
    try:lock=acquire_lock(run, 'standby', inherited_fd)
    except BlockingIOError:return
    while not stop_requested(run):
        status,status_error=read_status(run/'status.json')
        if status is not None and status['state'] in ('copy_pass_finished','copied_with_attention','stopped'):return
        # An unreachable anchor does not authorize a competing controller.
        command='python3 -S -c '+shlex.quote(detach(Path(__file__).resolve(),run,'supervise'))
        try:
            reachable=subprocess.run(ssh(cfg,'cpu-master-0',command),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=40).returncode==0
        except subprocess.TimeoutExpired:
            reachable=False
        heartbeat=dict(updated=time.time(),host=socket.gethostname(),pid=os.getpid(),anchor_reachable=reachable)
        if status_error is not None:
            heartbeat['status_error']=status_error
        save(run/'recovery/standby.json',heartbeat)
        time.sleep(30)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=['supervise','standby']);p.add_argument('--run',type=Path,required=True);p.add_argument('--lock-fd',type=int);a=p.parse_args()
    if a.mode=='supervise':supervise(a.run, a.lock_fd)
    else:standby(a.run, a.lock_fd)


if __name__=='__main__':main()
