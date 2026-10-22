#!/usr/bin/env python3
"""Deploy and detach the authorized copy job; never changes the source tree."""
import base64
import concurrent.futures
import datetime
import json
import pathlib
import re
import shlex
import subprocess
import sys
import time
import uuid

from migration_io import read_json

BASE = pathlib.Path(__file__).resolve().parent
JOB = '1626e143-de58-4098-ac78-51e927b01732'
OWNER = None  # Explicit preflight override; None selects the matching active run.
DEFAULT_OWNER = 'hechenrui'
HOSTS = ['master-0'] + ['worker-' + str(i) for i in range(11)]


def ssh_owner(job=None, owner=None):
    """Select explicit owner, global OWNER, matching active owner, then legacy."""
    job = JOB if job is None else job
    selected = owner if owner is not None else OWNER
    if selected is None:
        try:
            active = read_json(BASE / 'active_migration.json')
        except FileNotFoundError:
            active = {}
        if active.get('job') == job:
            selected = active.get('ssh_owner')
    if selected is None:
        selected = DEFAULT_OWNER
    if not isinstance(selected, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', selected):
        raise ValueError('invalid SSH owner')
    return selected


def ssh_destination(node, *, job=None, owner=None):
    """Build one gateway destination without changing the active run pointer."""
    job = JOB if job is None else job
    user = 'job-' + job + '-' + node + '.' + ssh_owner(job, owner) + '.baai-eai_pwm.ch1-calc1.job'
    return user + '@ssh.platform-multi.baai.ac.cn'


def ssh(node, code, *, owner=None):
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20',
               '-o', 'ServerAliveInterval=30', '-p', '2222',
               ssh_destination(node, owner=owner), 'python3 -']
    result = subprocess.run(command, input=code, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(node + ': ' + result.stderr + result.stdout)
    return node, result.stdout.strip()


def main():
    run_id = 'hcr-copy-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:6]
    control = '/share-mixed/project/.' + run_id
    files = {p.name: base64.b64encode(p.read_bytes()).decode() for p in
             [BASE / 'migration_scan.py', BASE / 'migration_worker.py', BASE / 'migration_run.py']}
    bundle = json.dumps({'control': control, 'run_id': run_id, 'files': files})
    bootstrap = '''import os,json,base64,pathlib,subprocess,datetime
b=json.loads(BUNDLE)
c=pathlib.Path(b['control'])
def mi(p):return subprocess.check_output(['findmnt','-T',p,'-rn','-o','TARGET,SOURCE,FSTYPE'],text=True).strip()
s=mi('/share/project');d=mi('/share-mixed/project')
assert s.split()[0]=='/share/project' and d.split()[0]=='/share-mixed/project'
assert os.stat('/share/project').st_dev != os.stat('/share-mixed/project').st_dev
assert not os.path.islink('/share-mixed/project') and not os.path.islink('/share-mixed/project/eai_pwm')
os.mkdir(c,0o700)
for n,v in b['files'].items():
 p=c/n;p.write_bytes(base64.b64decode(v));p.chmod(0o700)
cfg={'run_id':b['run_id'],'control':str(c),'source':'/share/project/eai_pwm','destination':'/share-mixed/project/eai_pwm','source_mount':s,'destination_mount':d,'threads':8,'job':'1626e143-de58-4098-ac78-51e927b01732','created':datetime.datetime.now(datetime.timezone.utc).isoformat()}
for n,v in [('config.json',cfg),('state.json',{'status':'starting','stage':None,'started':cfg['created'],'run_id':b['run_id']})]:
 p=c/n;p.write_text(json.dumps(v)+'\\n');p.chmod(0o600)
os.mkdir(c/'nodes',0o700)
print(json.dumps(cfg))
'''.replace('BUNDLE', repr(bundle), 1)
    _, output = ssh('master-0', bootstrap)
    cfg = json.loads(output)
    (BASE / 'active_migration.json').write_text(json.dumps(cfg, indent=2) + '\n')
    print('CONTROL ' + control, flush=True)
    verify_code = '''import sys,json,pathlib,socket
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_run import validate_mounts
cfg=json.loads((c/'config.json').read_text());validate_mounts(cfg)
assert cfg['run_id']==RUN_ID
print(socket.gethostname()+' shared-mount-ok')
'''.replace('CONTROL', repr(control)).replace('RUN_ID', repr(run_id))
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        for node, result in pool.map(lambda n: ssh(n, verify_code), HOSTS):
            print(node + ' ' + result, flush=True)

    def launch_code(mode):
        return '''import subprocess,sys,pathlib,socket,json
c=pathlib.Path(CONTROL)
name=MODE+'-'+socket.gethostname()
f=open(c/(name+'.log'),'ab',buffering=0)
p=subprocess.Popen([sys.executable,'-u',str(c/'migration_run.py'),MODE,'--control',str(c),'--threads','8'],stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
print(json.dumps({'pid':p.pid,'log':str(c/(name+'.log'))}))
'''.replace('CONTROL', repr(control)).replace('MODE', repr(mode))

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        for node, result in pool.map(lambda n: ssh(n, launch_code('worker')), HOSTS):
            print('WORKER ' + node + ' ' + result, flush=True)
    for _ in range(20):
        time.sleep(2)
        _, result = ssh('master-0', '''import pathlib,json
c=pathlib.Path(CONTROL)
ns=[json.loads(p.read_text()) for p in (c/'nodes').glob('*.json')]
print(json.dumps({'count':len(ns),'errors':[n['host'] for n in ns if n.get('thread_error') or n.get('ended')]}))
'''.replace('CONTROL', repr(control)))
        status = json.loads(result)
        if status['errors']:
            raise RuntimeError('worker startup errors: ' + repr(status))
        if status['count'] == 12:
            break
    else:
        raise RuntimeError('did not see all 12 worker heartbeats: ' + repr(status))
    _, result = ssh('master-0', launch_code('coordinator'))
    print('COORDINATOR ' + result, flush=True)
    print('LAUNCHED 12 nodes x 8 threads = 96 concurrent file workers', flush=True)


if __name__ == '__main__':
    main()
