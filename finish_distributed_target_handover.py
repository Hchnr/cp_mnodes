import json,pathlib
from migration_io import read_json,write_json
import migration_launch as t
B=pathlib.Path(__file__).parent;r=read_json(B/'active_migration.json');d=read_json(B/'distributed_target_filter_deployment.json');services=read_json(B/'distributed_target_services_deployment.json');assert d['status']=='needs_inspection';t.JOB=r['job'];t.OWNER=r['ssh_owner']
body={'record':r,'deployment':d,'services':services}
code=r'''import sys,os,pathlib,json,fcntl,hashlib,sqlite3,subprocess,resource,socket,time
b=json.loads(BODY);c=pathlib.Path(b['record']['control']);bundle=pathlib.Path(b['deployment']['bundle']);sys.dont_write_bytecode=True;sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');assert not(c/'STOP').exists();s=read_json(c/'state.json');assert s.get('recovery_warm') and s.get('stage')=='p3-r3' and not read_json(c/'pipeline.json').get('stages')
guard=open('/tmp/'+cfg['run_id']+'-parallel-target-dispatch.lock','a');fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
handover=bundle/'handover.json';h=read_json(handover);old=h['old_producer']
def identity(pid):
 try:
  p=pathlib.Path('/proc')/str(pid);f=(p/'stat').read_text().rsplit(')',1)[1].split();return {'pid':int(pid),'starttime_ticks':f[19],'state':f[0],'argv':(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
live=identity(old['pid']);assert not live or live['starttime_ticks']!=old['starttime_ticks'] or live['state']=='Z','old producer must actually have exited'
lock=open('/tmp/'+cfg['run_id']+'-coordinator.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(lock,fcntl.LOCK_UN);lock.close()
assert not(bundle/'continuation-attempt.json').exists(),'continuation already attempted; inspect role before retry'
digest=hashlib.sha256(handover.read_bytes()).hexdigest()[:24];assert not(c/'parallel-filter-adoptions'/digest).exists(),'handover already consumed'
assigned={}
with open(h['assigned_tasks_path']) as f:
 for line in f:
  assert line.endswith('\n');task=json.loads(line);assigned[task['id']]=task
assert h['next_task_id']>max(assigned,default=-1)
db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True)
for stage,taskid,payload in db.execute('SELECT stage,id,payload FROM claims'):
 assert stage=='p3-r3' and taskid in assigned and json.loads(payload)==assigned[taskid],'current claims changed beyond the saved ledger'
db.close()
endpoints=[v['role']['endpoint'] for _,v in sorted(b['services']['nodes'].items())];assert len(endpoints)==12
entry=bundle/'migration_distributed_target_filter.py';assert hashlib.sha256(entry.read_bytes()).hexdigest()==b['services']['code']['files_sha256'][entry.name]
cfg.update(parallel_scan_threads=128,parallel_target_threads=256,distributed_target_threads=256,target_probe_endpoints=endpoints,target_probe_slots_per_node=32,target_filter_inflight=4096);write_json(c/'config.json',cfg)
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);assert hard==resource.RLIM_INFINITY or hard>=65536;resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
env=dict(os.environ,PYTHONPATH=str(c),PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-distributed-target-pycache')
argv=[sys.executable,'-u',str(entry),'run','--control',str(c),'--handover',str(handover)];log=c/'distributed-target-producer.log'
write_json(bundle/'continuation-attempt.json',{'status':'starting','old_producer':old,'assigned_task_count':len(assigned),'next_task_id':h['next_task_id'],'argv':argv})
with open(log,'ab',buffering=0) as out:child=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True,env=env)
for _ in range(100):
 new=identity(child.pid)
 if new and new['state']!='Z' and new['argv']==argv:break
 if child.poll() is not None:raise RuntimeError('new producer exited; inspect exact PID/log before any retry')
 time.sleep(.05)
else:raise RuntimeError('new producer outcome uncertain')
role={'pid':new['pid'],'startticks':new['starttime_ticks'],'argv':new['argv'],'role':'producer','host':socket.gethostname(),'control':str(c),'log':str(log),'bundle':str(bundle),'entrypoint':entry.name,'handover':str(handover),'preserved_seed':h['seed_helper'],'parallel_scan_threads':128,'parallel_target_threads':256,'distributed_target_threads':256,'target_probe_endpoints':endpoints,'target_filter_inflight':4096}
write_json(c/'pipeline-deployment.json',role);write_json(c/'recovery-roles'/(socket.gethostname()+'-producer.json'),role)
proof={'status':'running','producer':role,'old_producer':old,'preserved_tasks':len(assigned),'new_next_task_id':h['next_task_id'],'handover':h,'source_data_changed':False,'copy_workers_changed':False}
write_json(bundle/'continuation-attempt.json',proof);write_json(bundle/'deployment.json',proof);print(json.dumps({k:v for k,v in proof.items() if k!='handover'}))
'''.replace('BODY',repr(json.dumps(body)),1)
x=json.loads(t.ssh('master-0',code)[1]);d.update(x);write_json(B/'distributed_target_filter_deployment.json',d)
r=read_json(B/'overlap_resume_recovery.json');r['roles']['master-0:producer']=x['producer'];r['status']='distributed_target_filter_running';write_json(B/'overlap_resume_recovery.json',r);write_json(B/'new_cluster_recovery.json',r);print(json.dumps(x))
