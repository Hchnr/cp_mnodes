#!/usr/bin/env python3
"""Resume from durable receipts using a new control directory and broker."""
import base64
import concurrent.futures
import datetime
import json
import pathlib
import secrets
import subprocess
import time
import uuid
import migration_launch as transport

BASE=pathlib.Path(__file__).resolve().parent
JOB='0a326937-3eb4-4da4-8b70-3a50586e1976'
transport.JOB=JOB
HOSTS=['master-0']+['worker-'+str(i) for i in range(11)]
OLD='/share-mixed/project/.hcr-copy-20261021T033600Z-e1113e'


def main():
    run_id='hcr-copy-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-resume-'+uuid.uuid4().hex[:6]
    control='/share-mixed/project/.'+run_id
    names=['migration_run.py','migration_worker.py','migration_scan.py','migration_broker.py',
           'migration_resume_index.py','migration_progress.py','migration_watchdog.py']
    bundle={'control':control,'run_id':run_id,'old':OLD,'job':JOB,
            'token':secrets.token_urlsafe(32),
            'files':{n:base64.b64encode((BASE/n).read_bytes()).decode() for n in names}}
    bootstrap='''import os,pathlib,json,base64,subprocess,datetime
b=json.loads(BUNDLE);c=pathlib.Path(b['control']);os.umask(0o077)
def mi(p):return subprocess.check_output(['findmnt','-T',p,'-rn','-o','TARGET,SOURCE,FSTYPE'],text=True).strip()
s=mi('/share/project');d=mi('/share-mixed/project')
assert s.split()[0]=='/share/project' and d.split()[0]=='/share-mixed/project'
assert os.stat('/share/project').st_dev!=os.stat('/share-mixed/project').st_dev
assert not os.path.islink('/share-mixed/project/eai_pwm')
c.mkdir(mode=0o700);(c/'nodes').mkdir(mode=0o700)
for n,v in b['files'].items():(c/n).write_bytes(base64.b64decode(v));(c/n).chmod(0o700)
ip=subprocess.check_output(['hostname','-I'],text=True).split()[0]
cfg={'run_id':b['run_id'],'control':str(c),'source':'/share/project/eai_pwm','destination':'/share-mixed/project/eai_pwm','source_mount':s,'destination_mount':d,'threads':2,'io_bytes_per_second':300*1024**2,'scan_entries_per_second':1000,'job':b['job'],'created':datetime.datetime.now(datetime.timezone.utc).isoformat(),'resume_from':b['old'],'broker_url':'http://'+ip+':37651','broker_token':b['token'],'training_log':'/share/project/eai_pwm/repos/cosmos-framework-pwm/outputs/action128_wam_23src_20261015_warm_start_from22src_i8800_run01/logs/action128_node0.log'}
(c/'config.json').write_text(json.dumps(cfg)+'\\n')
(c/'state.json').write_text(json.dumps({'status':'preparing_resume','stage':None,'started':cfg['created'],'run_id':b['run_id'],'resumed_from':b['old']})+'\\n')
monitor=pathlib.Path('/share-mixed/project/.hcr-copy-monitor')
assert not monitor.is_symlink();monitor.mkdir(mode=0o700,exist_ok=True)
(monitor/'progress.py').write_bytes((c/'migration_progress.py').read_bytes());(monitor/'progress.py').chmod(0o700)
tmp=monitor/('active-'+b['run_id']+'.tmp');tmp.write_text(json.dumps({'control':str(c)})+'\\n');os.replace(tmp,monitor/'active.json')
print(json.dumps({k:v for k,v in cfg.items() if k!='broker_token'}))
'''.replace('BUNDLE',repr(json.dumps(bundle)),1)
    _,out=transport.ssh('master-0',bootstrap)
    cfg=json.loads(out);cfg['broker_token']=bundle['token']
    active=BASE/'active_migration.json';active.write_text(json.dumps(cfg,indent=2)+'\n');active.chmod(0o600)
    print('CONTROL '+control,flush=True)
    print('PROGRESS python3 /share-mixed/project/.hcr-copy-monitor/progress.py --watch',flush=True)

    launch='''import pathlib,subprocess,sys,shutil,json,socket
c=pathlib.Path(CONTROL);mode=MODE
if mode=='broker':args=[sys.executable,'-u',str(c/'migration_broker.py'),'--control',str(c),'--port','37651']
elif mode=='watchdog':args=[sys.executable,'-u',str(c/'migration_watchdog.py'),'--control',str(c)]
else:args=[sys.executable,'-u',str(c/'migration_run.py'),mode,'--control',str(c),'--threads','2']
args=['nice','-n','15']+args
if shutil.which('ionice'):args=['ionice','-c','3']+args
log=c/(mode+'-'+socket.gethostname()+'.log');f=open(log,'ab',buffering=0)
p=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
print(json.dumps({'pid':p.pid,'log':str(log)}))
'''.replace('CONTROL',repr(control))
    print('BROKER '+transport.ssh('master-0',launch.replace('MODE',repr('broker')))[1],flush=True)
    time.sleep(1)
    verify='''import pathlib,json,sys,urllib.request,socket
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_run import validate_mounts
cfg=json.loads((c/'config.json').read_text());validate_mounts(cfg)
req=urllib.request.Request(cfg['broker_url']+'/health',headers={'Authorization':'Bearer '+cfg['broker_token']})
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open(req,timeout=15) as r:assert json.load(r)['ok']
print(socket.gethostname()+' shared-mount-and-broker-ok')
'''.replace('CONTROL',repr(control))
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for node,result in pool.map(lambda n:transport.ssh(n,verify),HOSTS):
            print('VERIFY '+node+' '+result,flush=True)

    # Old daemons see STOP; old gateway is gone and all old heartbeats are stale.
    # Remove only their interrupted private temporary names, never source data.
    cleanup='''import pathlib,json,sys,os,re,stat,datetime,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_worker import _open_parent,_identity
cfg=json.loads((c/'config.json').read_text());old=pathlib.Path(cfg['resume_from'])
assert (old/'STOP').exists()
ns=[json.loads(p.read_text()) for p in (old/'nodes').glob('*.json')]
assert ns and all(time.time()-datetime.datetime.fromisoformat(n['updated']).timestamp()>600 for n in ns)
oldcfg=json.loads((old/'config.json').read_text())
prefix='.migration-'+re.sub(r'[^a-zA-Z0-9_-]','_',oldcfg['run_id'])[:24]+'-'
pattern=re.compile(re.escape(prefix)+r'[0-9a-f]{32}\\.tmp$')
parents=set()
for p in (old/'stages/p1-r1/running').glob('*.json'):
 for e in json.loads(p.read_text())['entries']:parents.add(os.path.dirname(e['rel']))
count=size=0
report=open(c/'old-temp-cleanup.jsonl','w')
for parent in sorted(parents):
 fd=None
 try:
  fd,_=_open_parent(cfg['destination'],parent+'/.probe')
  for name in os.listdir(fd):
   if not pattern.fullmatch(name):continue
   st=os.stat(name,dir_fd=fd,follow_symlinks=False)
   if not stat.S_ISREG(st.st_mode):continue
   if _identity(os.stat(name,dir_fd=fd,follow_symlinks=False))!=_identity(st):continue
   report.write(json.dumps({'rel':parent+'/'+name,'size':st.st_size,'nlink':st.st_nlink})+'\\n')
   os.unlink(name,dir_fd=fd);count+=1;size+=st.st_size
 except FileNotFoundError:pass
 finally:
  if fd is not None:os.close(fd)
report.close();summary={'temp_files':count,'apparent_bytes':size};(c/'old-temp-cleanup.json').write_text(json.dumps(summary)+'\\n');print(json.dumps(summary))
'''.replace('CONTROL',repr(control))
    print('OLD_TEMP_CLEANUP '+transport.ssh('master-0',cleanup)[1],flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for node,result in pool.map(lambda n:transport.ssh(n,launch.replace('MODE',repr('worker'))),HOSTS):
            print('WORKER '+node+' '+result,flush=True)
    for _ in range(15):
        time.sleep(2)
        _,out=transport.ssh('master-0',"import pathlib,json;c=pathlib.Path("+repr(control)+");ns=[json.loads(p.read_text()) for p in (c/'nodes').glob('*.json')];print(json.dumps({'count':len(ns),'errors':[n['host'] for n in ns if n.get('thread_error') or n.get('ended')]}))")
        status=json.loads(out)
        if status['errors']:raise RuntimeError(repr(status))
        if status['count']==12:break
    else:raise RuntimeError('worker startup incomplete '+repr(status))
    print('WATCHDOG '+transport.ssh('master-0',launch.replace('MODE',repr('watchdog')))[1],flush=True)
    print('COORDINATOR '+transport.ssh('master-0',launch.replace('MODE',repr('coordinator')))[1],flush=True)
    print('RUNNING 12 nodes x 2 threads; aggregate I/O cap 300 MiB/s/node; source preserved',flush=True)


if __name__=='__main__':main()
