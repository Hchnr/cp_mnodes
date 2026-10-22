#!/usr/bin/env python3
"""Replace only the foreground producer; retain all payload and seed PIDs."""
import base64,datetime,fcntl,json,pathlib,sys,uuid
from migration_io import read_json,write_json
import migration_launch as t
BASE=pathlib.Path(__file__).parent
lock=open(BASE/'parallel-target-filter-deploy.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
r=read_json(BASE/'overlap_resume_recovery.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
try:d=read_json(BASE/'parallel_target_filter_deployment.json')
except FileNotFoundError:
 uid=uuid.uuid4().hex;d={'id':uid,'status':'planned','control':r['control'],'bundle':r['control']+'/parallel-target-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uid[:6]};write_json(BASE/'parallel_target_filter_deployment.json',d)
if d['status']!='planned':raise RuntimeError('deployment already attempted; inspect actual identity before retry')
body={'record':r,'deployment':d,'code':base64.b64encode((BASE/'migration_overlap_parallel_filter.py').read_bytes()).decode()}
code=r'''import sys,os,pathlib,json,base64,fcntl,hashlib,urllib.request,sqlite3,socket,signal,time,subprocess,resource,datetime
b=json.loads(BODY);r=b['record'];d=b['deployment'];c=pathlib.Path(d['control']);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');guard=open('/tmp/'+cfg['run_id']+'-parallel-target-dispatch.lock','a');fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert not(c/'STOP').exists()
old=read_json(c/'pipeline-deployment.json');pid=old['pid'];p=pathlib.Path('/proc')/str(pid)
def identity(pid):
 try:
  q=pathlib.Path('/proc')/str(pid);f=(q/'stat').read_text().rsplit(')',1)[1].split();return {'pid':int(pid),'starttime_ticks':f[19],'state':f[0],'argv':(q/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
actual=identity(pid);assert actual and actual['state']!='Z' and actual['starttime_ticks']==old['startticks'] and actual['argv']==old['argv']
assert any(x.endswith('migration_overlap_resume.py') for x in actual['argv'])
seed=read_json(c/'overlap-seed-helper-identity.json');seedactual=identity(seed['pid'])
assert seedactual and seedactual['starttime_ticks']==seed['starttime_ticks'] and seedactual['argv']==seed['argv'] or (c/'overlap-seed-result.json').exists(),'original seed has no identity or result'
bundle=pathlib.Path(d['bundle']);bundle.mkdir(mode=0o700)
entry=bundle/'migration_overlap_parallel_filter.py';content=base64.b64decode(b['code']);entry.write_bytes(content);entry.chmod(0o700)
cache='/tmp/'+cfg['run_id']+'-parallel-target-pycache';env=dict(os.environ,PYTHONPATH=str(c),PYTHONPYCACHEPREFIX=cache)
subprocess.run([sys.executable,str(entry),'--help'],env=env,stdout=subprocess.DEVNULL,check=True)
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def request(method,data=None):
 body=None if data is None else json.dumps(data).encode()
 q=urllib.request.Request(cfg['broker_url']+'/control',data=body,method=method,headers={'Authorization':'Bearer '+cfg['broker_token'],'Content-Type':'application/json'})
 with op.open(q,timeout=30) as response:return json.load(response)
prior=request('GET')['draining_hosts'];added=sorted(set(prior)|{'job-'+cfg['job']+'-'+n for n in ['master-0']+['worker-'+str(i) for i in range(11)]})
restore=False;frozen=False;terminated=False;roles_before=[]
for folder in ('nodes','nodes-extra'):
 for q in(c/folder).glob('*.json'):
  row=read_json(q);roles_before.append({'host':row['host'],'pid':row['pid'],'started':row['started'],'folder':folder})
write_json(bundle/'drain-restore.json',{'original_drains':prior,'temporary_drains':added,'restore_required':True})
try:
 restore=True;request('POST',{'draining_hosts':added})
 os.kill(pid,signal.SIGSTOP);frozen=True
 for _ in range(200):
  live=identity(pid)
  if live and live['state'] in ('T','t'):break
  time.sleep(.05)
 else:raise RuntimeError('foreground did not freeze')
 assert live['starttime_ticks']==actual['starttime_ticks'] and live['argv']==actual['argv']
 state=read_json(c/'state.json');assert state.get('recovery_warm') and state.get('stage')=='p3-r3';assert not(c/'STOP').exists()
 db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=20)
 tasks={}
 for stage,task_id,payload in db.execute('SELECT stage,id,payload FROM claims'):
  assert stage=='p3-r3';task=json.loads(payload);assert task['id']==task_id;tasks[task_id]=task
 db.close()
 stage=c/'stages/p3-r3'
 for shard in range(64):
  for path in(stage/'pending'/str(shard)).glob('*.json'):
   task_id=int(path.stem)
   if task_id in tasks:continue
   try:task=read_json(path)
   except FileNotFoundError:
    db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True);row=db.execute('select payload from claims where stage=? and id=?',('p3-r3',task_id)).fetchone();db.close();assert row,'pending vanished outside broker';task=json.loads(row[0])
   assert task['id']==task_id;tasks[task_id]=task
 assigned=bundle/'assigned-tasks.jsonl'
 with open(assigned,'x') as out:
  for task_id,task in sorted(tasks.items()):out.write(json.dumps(task,ensure_ascii=True)+'\n')
 evidence={'control':str(c),'stage':'p3-r3','old_producer':{k:actual[k] for k in ('pid','starttime_ticks','argv')},'seed_helper':seed,'assigned_tasks_path':str(assigned),'warm_manifest':str(c/'manifests/recovery-warm.jsonl'),'missing_manifest':str(c/'manifests/recovery-missing.jsonl'),'next_task_id':max(tasks,default=-1)+1,'created':datetime.datetime.now(datetime.timezone.utc).isoformat(),'preserved_task_count':len(tasks),'worker_identities_before':roles_before,'source_code_sha256':hashlib.sha256(content).hexdigest()}
 handover=bundle/'handover.json';write_json(handover,evidence)
 os.kill(pid,signal.SIGTERM);os.kill(pid,signal.SIGCONT);frozen=False;terminated=True
 for _ in range(400):
  live=identity(pid)
  if not live or live['state']=='Z' or live['starttime_ticks']!=actual['starttime_ticks']:break
  time.sleep(.05)
 else:raise RuntimeError('old foreground still alive')
 coordinator=open('/tmp/'+cfg['run_id']+'-coordinator.lock','a')
 for _ in range(200):
  try:fcntl.flock(coordinator,fcntl.LOCK_EX|fcntl.LOCK_NB);break
  except BlockingIOError:time.sleep(.05)
 else:raise RuntimeError('old foreground lock has not released')
 fcntl.flock(coordinator,fcntl.LOCK_UN);coordinator.close()
 assert not(c/'STOP').exists()
 cfg.update(parallel_target_threads=64,target_filter_inflight=4096);write_json(c/'config.json',cfg)
 soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
 argv=[sys.executable,'-u',str(entry),'run','--control',str(c),'--handover',str(handover)]
 log=c/'parallel-target-producer.log'
 with open(log,'ab',buffering=0) as out:proc=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True,env=env)
 for _ in range(100):
  new=identity(proc.pid)
  if new and new['state']!='Z' and new['argv']==argv:break
  if proc.poll() is not None:raise RuntimeError('new foreground exited; inspect log without restarting copy')
  time.sleep(.05)
 else:raise RuntimeError('new foreground identity uncertain')
 role={'pid':new['pid'],'startticks':new['starttime_ticks'],'argv':new['argv'],'role':'producer','host':socket.gethostname(),'control':str(c),'log':str(log),'bundle':str(bundle),'entrypoint':entry.name,'handover':str(handover),'preserved_seed':seed,'parallel_target_threads':64,'target_filter_inflight':4096}
 write_json(c/'pipeline-deployment.json',role);write_json(c/'recovery-roles'/(socket.gethostname()+'-producer.json'),role)
 write_json(bundle/'deployment.json',{'status':'running','producer':role,'old_producer':old,'handover':evidence})
 print(json.dumps({'status':'running','producer':role,'preserved_tasks':len(tasks),'new_next_task_id':evidence['next_task_id']}))
finally:
 if frozen:
  live=identity(pid)
  if live and live['starttime_ticks']==actual['starttime_ticks']:os.kill(pid,signal.SIGCONT)
 if restore:
  result=request('POST',{'draining_hosts':prior});write_json(bundle/'drain-restore.json',{'original_drains':prior,'restore_required':False,'result':result})
'''.replace('BODY',repr(json.dumps(body)),1)
d['status']='deploying';write_json(BASE/'parallel_target_filter_deployment.json',d)
try:
 v=json.loads(t.ssh('master-0',code)[1]);d.update(v);write_json(BASE/'parallel_target_filter_deployment.json',d)
 r=read_json(BASE/'overlap_resume_recovery.json');r['roles']['master-0:producer']=v['producer'];r['status']='parallel_target_filter_running';write_json(BASE/'overlap_resume_recovery.json',r);write_json(BASE/'new_cluster_recovery.json',r);print(json.dumps(v))
except BaseException as exc:
 d.update(status='needs_inspection',error=str(exc));write_json(BASE/'parallel_target_filter_deployment.json',d);raise
