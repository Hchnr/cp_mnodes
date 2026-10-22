#!/usr/bin/env python3
"""Roll node workers to new concurrency without restarting the source scanner."""
import base64,concurrent.futures,datetime,json,pathlib,time,uuid
from migration_io import read_json,write_json
import migration_launch as transport
BASE=pathlib.Path(__file__).resolve().parent
HOSTS=['master-0']+['worker-'+str(i) for i in range(11)]
THREADS=64
RATE=20_000_000_000

def main():
    import os
    os.umask(0o077)
    cfg=read_json(BASE/'active_migration.json');transport.JOB=cfg['job'];control=cfg['control']
    full={n:'job-'+cfg['job']+'-'+n for n in HOSTS}
    record=BASE/'scale_deploy.json'
    checkpoint=read_json(record) if record.exists() else {}
    if (checkpoint.get('control')==control and checkpoint.get('phase')=='complete'
            and checkpoint.get('threads')==THREADS and checkpoint.get('io_bytes_per_second')==RATE):
        print('ALREADY_COMPLETE '+json.dumps({'threads_each':THREADS,'rate_each':RATE}),flush=True)
        return
    resumed=(checkpoint.get('control')==control and checkpoint.get('phase')=='draining'
             and checkpoint.get('threads')==THREADS and checkpoint.get('io_bytes_per_second')==RATE)
    if not resumed:
        names=['migration_run.py','migration_broker.py','migration_metrics.py','migration_inventory.py','migration_progress.py']
        bundle={'control':control,'threads':THREADS,'rate':RATE,'hosts':list(full.values()),'version':datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6],
                'files':{n:base64.b64encode((BASE/n).read_bytes()).decode() for n in names}}
        code='''import pathlib,os,sys,json,base64,signal,time,subprocess,shutil,socket
b=json.loads(BUNDLE);os.umask(0o077);c=pathlib.Path(b['control']);sys.path.insert(0,str(c));from migration_io import read_json,write_json
assert not (c/'STOP').exists(),'STOP protects training; do not bypass'
s=read_json(c/'state.json');assert s['status'] not in ('stopped','failed','complete','complete_with_changes'),s['status']
v=c/'versions'/b['version'];v.mkdir(parents=True)
for name,data in b['files'].items():
 p=c/name;shutil.copyfile(p,v/name);payload=base64.b64decode(data);compile(payload,name,'exec');p.write_bytes(payload)
cfg=read_json(c/'config.json');cfg.update(threads=b['threads'],io_bytes_per_second=b['rate']);write_json(c/'config.json',cfg)
write_json(c/'drain-hosts.json',{'draining_hosts':b['hosts']})
def stop_owned(pid,script):
 p=pathlib.Path('/proc')/str(pid)
 if not p.exists():return
 a=(p/'cmdline').read_bytes().decode().split('\\0');assert str(c/script) in a,(pid,script)
 os.kill(pid,signal.SIGTERM)
 for _ in range(50):
  if not p.exists() or (p/'stat').read_text().split(') ')[1].split()[0]=='Z':return
  time.sleep(.1)
 a=(p/'cmdline').read_bytes().decode().split('\\0');assert str(c/script) in a;os.kill(pid,signal.SIGKILL)
 for _ in range(20):
  if not p.exists() or (p/'stat').read_text().split(') ')[1].split()[0]=='Z':return
  time.sleep(.1)
 raise RuntimeError('owned process did not exit: '+str(pid))
def launch(script,extra):
 args=[sys.executable,'-u',str(c/script),'--control',str(c)]+extra;args=['nice','-n','15']+args
 if shutil.which('ionice'):args=['ionice','-c','3']+args
 f=open(c/(script[:-3]+'-scaled-'+socket.gethostname()+'.log'),'ab',buffering=0)
 p=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True);return p.pid
oldbroker=read_json(c/'broker.json')['pid'];stop_owned(oldbroker,'migration_broker.py');broker=launch('migration_broker.py',['--port','37651'])
metricsold=read_json(c/'metrics.json')['pid'];stop_owned(metricsold,'migration_metrics.py');metrics=launch('migration_metrics.py',[])
m=pathlib.Path('/share-mixed/project/.hcr-copy-monitor');shutil.copyfile(c/'migration_progress.py',m/'progress.py');shutil.copyfile(c/'migration_progress.py',m/'migration_progress.py')
print(json.dumps({'broker':broker,'metrics':metrics,'threads':b['threads'],'io_bytes_per_second':b['rate'],'control':str(c)}))
'''.replace('BUNDLE',repr(json.dumps(bundle)),1)
        print('DEPLOY '+transport.ssh('master-0',code)[1],flush=True)
        cfg.update(threads=THREADS,io_bytes_per_second=RATE);write_json(BASE/'active_migration.json',cfg)
        checkpoint={'control':control,'phase':'draining','started':datetime.datetime.now(datetime.timezone.utc).isoformat(),'threads':THREADS,'io_bytes_per_second':RATE,'completed':{},'remaining':HOSTS}
        write_json(record,checkpoint)
    completed=checkpoint.get('completed',{})
    common='''import pathlib,sys,json,urllib.request,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json')
def request(method,route,data=None):
 body=None if data is None else json.dumps(data).encode();r=urllib.request.Request(cfg['broker_url']+route,data=body,method=method,headers={'Authorization':'Bearer '+cfg['broker_token'],'Content-Type':'application/json'})
 op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 with op.open(r,timeout=30) as f:return json.load(f)
'''.replace('CONTROL',repr(control))
    inspect=common+'''assert not (c/'STOP').exists(),'STOP protects training; do not bypass'
s=read_json(c/'state.json');assert s['status'] not in ('stopped','failed','complete','complete_with_changes'),s['status']
ns=[read_json(p) for p in (c/'nodes').glob('*.json')]
print(json.dumps({'control':request('GET','/control'),'nodes':[{k:n.get(k) for k in ('host','pid','active','ended','thread_error','updated','threads','io_bytes_per_second')} for n in ns],'state':{'stage':s.get('stage'),'status':s.get('status'),'updated':s.get('updated')}}))
'''
    # Restore the drain gate before resuming a partially checkpointed rollout.
    pending_hosts=[full[n] for n in HOSTS if n not in completed]
    reset=common+"print(json.dumps(request('POST','/control',{'draining_hosts':"+repr(pending_hosts)+"})))"
    print('RESTORE_DRAIN '+transport.ssh('master-0',reset)[1],flush=True)
    start=time.monotonic()
    while len(completed)<len(HOSTS):
        _,out=transport.ssh('master-0',inspect);status=json.loads(out)
        leases=status['control']['leased_by_host'];nodes={n['host']:n for n in status['nodes']}
        ready=[];waiting={}
        for label in HOSTS:
            if label in completed:continue
            host=full[label];node=nodes.get(host)
            if not node:raise RuntimeError('worker heartbeat missing '+label)
            if node.get('thread_error') or node.get('ended'):raise RuntimeError('worker unhealthy '+label)
            if not leases.get(host,0) and not node.get('active'):ready.append((label,node['pid']))
            else:waiting[label]={'leased':leases.get(host,0),'active':len(node.get('active') or {})}
        print('DRAIN '+json.dumps({'completed':len(completed),'ready':[n for n,_ in ready],'waiting':waiting}),flush=True)
        if ready:
            def replace(item):
                label,expected=item
                code='''import pathlib,sys,os,json,signal,time,subprocess,shutil,socket,resource
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
assert not (c/'STOP').exists(),'STOP protects training'
node=read_json(c/'nodes'/(socket.gethostname()+'.json'));assert node['pid']==EXPECTED and not node.get('active'),node.get('active')
p=pathlib.Path('/proc')/str(EXPECTED);a=(p/'cmdline').read_bytes().decode().split('\\0');assert str(c/'migration_run.py') in a and 'worker' in a
os.kill(EXPECTED,signal.SIGTERM)
for _ in range(50):
 if not p.exists() or (p/'stat').read_text().split(') ')[1].split()[0]=='Z':break
 time.sleep(.1)
else:raise RuntimeError('idle worker has not exited')
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);limit=65536 if hard==resource.RLIM_INFINITY else min(65536,hard);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,limit),hard))
args=['nice','-n','15',sys.executable,'-u',str(c/'migration_run.py'),'worker','--control',str(c),'--threads','64']
if shutil.which('ionice'):args=['ionice','-c','3']+args
f=open(c/('worker64-'+socket.gethostname()+'.log'),'ab',buffering=0);proc=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
for _ in range(50):
 n=read_json(c/'nodes'/(socket.gethostname()+'.json'))
 if n.get('pid')==proc.pid:
  assert n.get('threads')==64 and not n.get('ended') and not n.get('thread_error');break
 if proc.poll() is not None:raise RuntimeError('new worker exited '+str(proc.returncode))
 time.sleep(.2)
else:raise RuntimeError('new worker heartbeat missing')
print(json.dumps({'old_pid':EXPECTED,'pid':proc.pid,'threads':n['threads'],'io_bytes_per_second':n.get('io_bytes_per_second'),'nofile':max(soft,limit)}))
'''.replace('CONTROL',repr(control)).replace('EXPECTED',str(expected))
                return label,json.loads(transport.ssh(label,code)[1])
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                for label,result in pool.map(replace,ready):
                    completed[label]=result;print('REPLACED '+label+' '+json.dumps(result),flush=True)
            checkpoint.update(completed=completed,remaining=[n for n in HOSTS if n not in completed],updated=datetime.datetime.now(datetime.timezone.utc).isoformat());write_json(record,checkpoint)
            remaining=[full[n] for n in HOSTS if n not in completed]
            release=common+"print(json.dumps(request('POST','/control',{'draining_hosts':"+repr(remaining)+"})))"
            print('RELEASE '+transport.ssh('master-0',release)[1],flush=True)
            checkpoint.update(completed=completed,remaining=[n for n in HOSTS if n not in completed],updated=datetime.datetime.now(datetime.timezone.utc).isoformat());write_json(record,checkpoint)
        if len(completed)==len(HOSTS):break
        if time.monotonic()-start>1800:raise RuntimeError('Drain remains active; safe to resume from scale_deploy.json')
        time.sleep(5)
    checkpoint.update(phase='complete',finished=datetime.datetime.now(datetime.timezone.utc).isoformat());write_json(record,checkpoint)
    print('COMPLETE '+json.dumps({'nodes':12,'threads_each':THREADS,'io_bytes_per_second_each':RATE,'total_threads':12*THREADS}),flush=True)

if __name__=='__main__':main()
