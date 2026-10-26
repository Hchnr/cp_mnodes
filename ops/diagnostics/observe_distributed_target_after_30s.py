"""Read-only 30s control/daemon/queue observation, without worker/proc IO profiling."""
import json
from pathlib import Path
from migration_io import read_json, write_json
import migration_launch as t
base=Path(__file__).resolve().parent
active=read_json(base/'active_migration.json');services=read_json(base/'distributed_target_services_deployment.json')
roles={node:row['role'] for node,row in services['nodes'].items()}
t.JOB=active['job']
code=r'''import collections,concurrent.futures,json,os,pathlib,sqlite3,sys,time,urllib.request
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json');services=ROLES;op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def health(item):
 node,role=item;started=time.time()
 try:
  req=urllib.request.Request(role['endpoint']+'/health',headers={'Authorization':'Bearer '+cfg['broker_token']})
  with op.open(req,timeout=5) as response:body=json.load(response)
  return node,{'time':time.time(),'seconds':time.time()-started,'body':body,'identity_matches':body.get('pid')==role['pid'] and str(body.get('starttime_ticks'))==str(role['starttime_ticks']) and body.get('argv')==role['argv']}
 except Exception as exc:return node,{'time':time.time(),'seconds':time.time()-started,'error':type(exc).__name__+':'+str(exc)}
def proc(role):
 root=pathlib.Path('/proc')/str(role['pid'])
 try:
  f=(root/'stat').read_text().rsplit(') ',1)[1].split();argv=(root/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0');counts=collections.Counter()
  for task in(root/'task').iterdir():
   try:counts[(task/'wchan').read_text().strip()]+=1
   except OSError:pass
  return {'pid':role['pid'],'state':f[0],'starttime_ticks':f[19],'argv':argv,'identity_matches':str(f[19])==str(role['startticks']) and argv==role['argv'],'threads':int(f[17]),'main_wchan':(root/'wchan').read_text().strip(),'thread_wchan_counts':dict(counts)}
 except OSError as exc:return {'pid':role['pid'],'error':type(exc).__name__}
def sample():
 started=time.time();state=read_json(c/'state.json');deployment=read_json(c/'pipeline-deployment.json');stage=state['stage'];root=c/'stages'/stage
 dbpath=pathlib.Path('/tmp')/(cfg['run_id']+'-broker.sqlite');db=sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True,timeout=3)
 claims=db.execute('SELECT stage,status,COUNT(*) FROM claims GROUP BY stage,status').fetchall();duplicates=db.execute("SELECT stage,owner,COUNT(*) FROM claims WHERE status='leased' GROUP BY stage,owner HAVING COUNT(*)>1").fetchall();db.close()
 pending=0
 for shard in range(64):
  try:
   with os.scandir(root/'pending'/str(shard)) as items:pending+=sum(entry.name.endswith('.json') for entry in items)
  except FileNotFoundError:pass
 try:
  with os.scandir(root/'running') as items:running=sum(entry.name.endswith('.json') for entry in items)
 except FileNotFoundError:running=0
 workers=[]
 for folder in('nodes','nodes-extra'):
  for node in services:
   host=services[node]['host']
   try:
    row=read_json(c/folder/(host+'.json'));workers.append({'host':host,'lane':folder,'pid':row['pid'],'started':row['started'],'updated':row.get('updated'),'ended':row.get('ended'),'threads':row.get('threads')})
   except (OSError,ValueError) as exc:workers.append({'host':host,'lane':folder,'error':type(exc).__name__})
 try:
  req=urllib.request.Request(cfg['broker_url']+'/control',headers={'Authorization':'Bearer '+cfg['broker_token']})
  with op.open(req,timeout=5) as response:broker=json.load(response)
 except Exception as exc:broker={'error':type(exc).__name__+':'+str(exc)}
 with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:daemon=dict(pool.map(health,services.items()))
 return {'started':started,'finished':time.time(),'state':{key:state.get(key) for key in('updated','status','stage','recovery_warm','active_manifest','foreground_handover','target_filter','next_task_id','distributed_target_probe','parallel_target_threads','distributed_target_threads','distributed_target_nodes','resume_index_building','resume_index_error')},'producer':proc(deployment),'workers':workers,'health':daemon,'claims':claims,'duplicate_leased_owners':duplicates,'queue':{'pending':pending,'running':running},'broker_control':broker,'stop':(c/'STOP').exists(),'producer_handover':deployment.get('handover')}
a=sample();time.sleep(30);b=sample();baseline=[]
try:baseline=read_json(pathlib.Path(b['producer_handover'])).get('worker_identities_before',[])
except (OSError,ValueError):pass
print(json.dumps({'control':str(c),'first':a,'second':b,'worker_identities_before_handover':baseline}))
'''.replace('CONTROL',repr(active['control']),1).replace('ROLES',repr(roles),1)
_,raw=t.ssh('master-0',code,owner=active.get('ssh_owner'));v=json.loads(raw)
write_json(base/'distributed_target_after_30s.json',v)
a,b=v['first'],v['second'];da=a['state'].get('distributed_target_probe',{}).get('counts',{});db=b['state'].get('distributed_target_probe',{}).get('counts',{})
old={(x['host'],x['folder']):(x['pid'],x['started']) for x in v['worker_identities_before_handover']}
changes=[n for n in b['workers'] if old.get((n['host'],n['lane']))!=(n.get('pid'),n.get('started'))]
health={}
for node in roles:
 x=a['health'][node];y=b['health'][node];ca=x.get('body',{}).get('counts',{});cb=y.get('body',{}).get('counts',{})
 health[node]={'identity':y.get('identity_matches'),'inflight':y.get('body',{}).get('inflight'),'request_delta':cb.get('requests',0)-ca.get('requests',0),'missing_delta':cb.get('missing',0)-ca.get('missing',0),'error':y.get('error')}
print(json.dumps({'saved':str(base/'distributed_target_after_30s.json'),'seconds':b['started']-a['started'],'producer_first':a['producer'],'producer_second':b['producer'],'first_filter':a['state'].get('target_filter'),'second_filter':b['state'].get('target_filter'),'client_deltas':{key:db.get(key,0)-da.get(key,0) for key in set(da)|set(db)},'health_deltas':health,'queue':b['queue'],'stop':b['stop'],'drains':b['broker_control'].get('draining_hosts'),'worker_changes':changes,'baseline_count':len(old)},ensure_ascii=False))
