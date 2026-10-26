#!/usr/bin/env python3
"""Controlled migration-only restart with durable receipt recovery."""
import base64, concurrent.futures, datetime, json, os, pathlib, secrets, time, uuid
from migration_io import read_json, write_json
import migration_launch as transport
BASE=pathlib.Path(__file__).resolve().parent
HOSTS=['master-0']+['worker-'+str(i) for i in range(11)]

def each(code):
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures={pool.submit(transport.ssh,n,code):n for n in HOSTS}
        results={}
        for f in concurrent.futures.as_completed(futures):
            n,out=f.result();results[n]=json.loads(out)
        return results

def main():
    os.umask(0o077)
    prior=read_json(BASE/'active_migration.json');transport.JOB=prior['job'];old=prior['control']
    # Immutable STOP is read by each copy thread before another file; cancellation
    # removes only that thread's own temporary file. Training is never signalled.
    _,out=transport.ssh('master-0',"import pathlib,json;p=pathlib.Path("+repr(old)+");(p/'STOP').touch(exist_ok=True);print(json.dumps({'stop':str(p)}))")
    print('STOP_OLD '+out,flush=True)
    process_code='''import pathlib,json,os
c=CONTROL;found=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  a=(p/'cmdline').read_bytes().decode(errors='replace').split('\\0')
  if any(x.startswith(c+'/migration_') and x.endswith('.py') for x in a):
   found.append({'pid':int(p.name),'args':a,'state':(p/'stat').read_text().split(') ')[1].split()[0]})
 except (OSError,IndexError):pass
print(json.dumps(found))
'''.replace('CONTROL',repr(old))
    # Cancel the frozen producer before any new queue may be created.
    signal_code=process_code.replace("print(json.dumps(found))",'''import signal
for p in found:
 if any(x.endswith('/migration_run.py') for x in p['args']) and 'coordinator' in p['args']:
  os.kill(p['pid'],signal.SIGTERM)
print(json.dumps(found))''')
    print('OLD_COORDINATOR '+transport.ssh('master-0',signal_code)[1],flush=True)
    for attempt in range(20):
        procs=each(process_code)
        workers={n:[p for p in ps if any(a.endswith('/migration_run.py') for a in p['args']) and 'worker' in p['args'] and p['state']!='Z'] for n,ps in procs.items()}
        active={n:ps for n,ps in workers.items() if ps}
        print('OLD_WORKERS '+json.dumps({n:[p['pid'] for p in ps] for n,ps in active.items()}),flush=True)
        if not active:break
        time.sleep(3)
    else:raise RuntimeError('Old copy workers have not exited; no new copy was started')
    # Exact argv matches only our old-control daemons. A pending fatal signal on
    # an uninterruptible old read prevents the process from executing more code.
    kill_code=process_code.replace("print(json.dumps(found))",'''import signal
for p in found:
 if p['state']!='Z':os.kill(p['pid'],signal.SIGKILL)
print(json.dumps(found))''')
    stopped=each(kill_code);time.sleep(1)
    remaining=each(process_code)
    dangerous={n:ps for n,ps in remaining.items() if any(p['state'] not in ('Z',) for p in ps)}
    if dangerous:raise RuntimeError('Old task processes still present after stop: '+json.dumps(dangerous))
    print('OLD_ALL_STOPPED',flush=True)

    run_id='hcr-copy-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-resume-'+uuid.uuid4().hex[:6]
    control='/share-mixed/project/.'+run_id
    names=['migration_run.py','migration_worker.py','migration_scan.py','migration_broker.py','migration_resume_index.py','migration_progress.py','migration_watchdog.py','migration_io.py','migration_inventory.py','migration_metrics.py']
    bundle={'control':control,'run_id':run_id,'old':old,'job':prior['job'],'token':secrets.token_urlsafe(32),'files':{n:base64.b64encode((BASE/n).read_bytes()).decode() for n in names}}
    bootstrap='''import pathlib,os,sys,json,base64,subprocess,datetime
b=json.loads(BUNDLE);c=pathlib.Path(b['control']);os.umask(0o077);c.mkdir(mode=0o700);(c/'nodes').mkdir()
for n,v in b['files'].items():(c/n).write_bytes(base64.b64decode(v));(c/n).chmod(0o700)
sys.path.insert(0,str(c));from migration_io import write_json
from migration_run import validate_mounts
oldcfg=json.loads((pathlib.Path(b['old'])/'config.json').read_text())
cfg={k:oldcfg[k] for k in ('source','destination','source_mount','destination_mount','training_log')}
ip=subprocess.check_output(['hostname','-I'],text=True).split()[0]
cfg.update(run_id=b['run_id'],control=str(c),threads=4,io_bytes_per_second=300*1024**2,scan_entries_per_second=1000,inventory_entries_per_second=100,job=b['job'],created=datetime.datetime.now(datetime.timezone.utc).isoformat(),resume_from=b['old'],broker_url='http://'+ip+':37651',broker_token=b['token'])
validate_mounts(cfg);write_json(c/'config.json',cfg)
write_json(c/'state.json',{'status':'preparing_resume','stage':None,'started':cfg['created'],'run_id':b['run_id'],'resumed_from':b['old']})
(pathlib.Path(b['old'])/'SUPERSEDED').write_text(str(c)+'\\n')
print(json.dumps({k:v for k,v in cfg.items() if k!='broker_token'}))
'''.replace('BUNDLE',repr(json.dumps(bundle)),1)
    _,out=transport.ssh('master-0',bootstrap);cfg=json.loads(out);cfg['broker_token']=bundle['token']
    p=BASE/'active_migration.json';write_json(p,cfg);p.chmod(0o600)
    print('CONTROL '+control,flush=True)
    launch='''import pathlib,subprocess,sys,shutil,json,socket
c=pathlib.Path(CONTROL);mode=MODE;args=[sys.executable,'-u']
if mode=='broker':args += [str(c/'migration_broker.py'),'--control',str(c),'--port','37651']
elif mode in ('watchdog','metrics'):args += [str(c/('migration_'+mode+'.py')),'--control',str(c)]
elif mode=='inventory-prepare':args += [str(c/'migration_inventory.py'),'--control',str(c),'--prepare','--rate','100','--nodes','12']
elif mode=='inventory':
 code="import sys,time,pathlib;sys.path.insert(0,"+repr(str(c))+");from migration_inventory import run_node;from migration_io import read_json;c=pathlib.Path("+repr(str(c))+");\\nwhile not (c/'inventory/plan.json').exists() and not (c/'STOP').exists():time.sleep(2)\\nif not (c/'STOP').exists():run_node(c,"+str(INDEX)+",rate=100)"
 args += ['-c',code]
else:args += [str(c/'migration_run.py'),mode,'--control',str(c),'--threads','4']
args=['nice','-n','15']+args
if shutil.which('ionice'):args=['ionice','-c','3']+args
log=c/(mode+'-'+socket.gethostname()+'.log');f=open(log,'ab',buffering=0)
p=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
print(json.dumps({'pid':p.pid,'log':str(log)}))
'''.replace('CONTROL',repr(control))
    def launch_on(node,mode,index=0):return transport.ssh(node,launch.replace('MODE',repr(mode)).replace('INDEX',str(index)))[1]
    print('BROKER '+launch_on('master-0','broker'),flush=True);time.sleep(1)
    verify='''import pathlib,sys,json,urllib.request,socket
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_run import validate_mounts
from migration_io import read_json
cfg=read_json(c/'config.json');validate_mounts(cfg)
r=urllib.request.Request(cfg['broker_url']+'/health',headers={'Authorization':'Bearer '+cfg['broker_token']})
o=urllib.request.build_opener(urllib.request.ProxyHandler({}))
with o.open(r,timeout=15) as x:assert json.load(x)['ok']
print(json.dumps({'host':socket.gethostname(),'ok':True}))
'''.replace('CONTROL',repr(control))
    print('VERIFY '+json.dumps(each(verify)),flush=True)
    # Do not remove any old partials here: graceful cancellation cleaned its own.
    # Receipts from every ancestor retain all completed copies for resume.
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for n,out in pool.map(lambda n:(n,launch_on(n,'worker')),HOSTS):print('WORKER '+n+' '+out,flush=True)
    for _ in range(15):
        time.sleep(2)
        _,out=transport.ssh('master-0',"import pathlib,sys,json;c=pathlib.Path("+repr(control)+");sys.path.insert(0,str(c));from migration_io import read_json;ns=[read_json(p) for p in (c/'nodes').glob('*.json')];print(json.dumps({'count':len(ns),'errors':[n['host'] for n in ns if n.get('thread_error') or n.get('ended')]}))")
        status=json.loads(out)
        if status['errors']:raise RuntimeError(repr(status))
        if status['count']==12:break
    else:raise RuntimeError('worker startup incomplete '+repr(status))
    for mode in ('watchdog','metrics','coordinator','inventory-prepare'):
        print(mode.upper()+' '+launch_on('master-0',mode),flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for n,out in pool.map(lambda x:(x[1],launch_on(x[1],'inventory',x[0])),enumerate(HOSTS)):print('INVENTORY '+n+' '+out,flush=True)
    publish='''import pathlib,sys,json,shutil
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import write_json
m=pathlib.Path('/share-mixed/project/.hcr-copy-monitor');m.mkdir(exist_ok=True)
for name in ('migration_io.py','migration_progress.py'):shutil.copyfile(c/name,m/name)
shutil.copyfile(c/'migration_progress.py',m/'progress.py')
write_json(m/'active.json',{'control':str(c)})
print(json.dumps({'control':str(c),'threads':4,'nodes':12,'io_cap_MiB_s':300}))
'''.replace('CONTROL',repr(control))
    print('PUBLISHED '+transport.ssh('master-0',publish)[1],flush=True)

if __name__=='__main__':main()
