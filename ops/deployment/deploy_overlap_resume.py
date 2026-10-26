#!/usr/bin/env python3
"""One fresh namespace after explicit approval of the expected launcher exit."""
import argparse,base64,concurrent.futures,datetime,fcntl,json,pathlib,sys,uuid
from migration_io import read_json,write_json
import migration_launch as t
BASE=pathlib.Path(__file__).resolve().parent
RECORD=BASE/'overlap_resume_recovery.json'
def context(create=False):
 try:r=read_json(RECORD)
 except FileNotFoundError:
  if not create:raise
  old=read_json(BASE/'new_cluster_recovery.json');identity=uuid.uuid4().hex[:6]
  run_id='hcr-copy-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-overlap-'+identity
  r={'run_id':run_id,'control':'/share-mixed/project/.'+run_id,'job':old['job'],'ssh_owner':old['ssh_owner'],'old_control':old['control'],'origin_queue_control':'/share-mixed/project/.hcr-copy-20261021T064717Z-resume-e25450','created':datetime.datetime.now(datetime.timezone.utc).isoformat(),'roles':{},'status':'planned','authorization':{'statement':'这是预期退出，继续复制','kind':'expected_training_exit_user_authorized_new_baseline','confirmed':True}}
  write_json(RECORD,r)
 t.JOB,t.OWNER=r['job'],r['ssh_owner'];return r

def remote(node,code):return json.loads(t.ssh(node,code)[1])

def prepare():
 r=context(True)
 shutdown=read_json(BASE/'overlap_predecessor_shutdown.json');assert shutdown['control']==r['old_control']
 assert len(shutdown['results'])==12 and all(not x['original_process_live'] and not x['active'] for n in shutdown['results'] for x in n['lanes'])
 files={n:base64.b64encode((BASE/n).read_bytes()).decode() for n in ('migration_resume_index_parallel_memory.py',)}
 body={'record':r,'files':files,'shutdown':shutdown}
 code=r'''import sys,os,pathlib,json,base64,fcntl,hashlib,socket,secrets,datetime,copy,concurrent.futures,sqlite3,signal,time,subprocess
b=json.loads(BODY);r=b['record'];old=pathlib.Path(r['old_control']);c=pathlib.Path(r['control']);sys.path.insert(0,str(old));from migration_io import read_json,write_json
lock=open('/tmp/'+r['run_id']+'-prepare.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert r['authorization']['confirmed'] and (old/'STOP').exists();assert read_json(old/'state.json')['status']=='stopped'
assert not c.exists(),'namespace already exists; inspect instead of replaying prepare'
c.mkdir(mode=0o700)
origins={}
for p in old.glob('*.py'):
 data=p.read_bytes();(c/p.name).write_bytes(data);(c/p.name).chmod(0o700);origins[p.name]={'origin':str(p),'sha256':hashlib.sha256(data).hexdigest()}
for name,encoded in b['files'].items():
 data=base64.b64decode(encoded);(c/name).write_bytes(data);(c/name).chmod(0o700);origins[name]={'origin':'reviewed_local_parallel_memory_builder','sha256':hashlib.sha256(data).hexdigest()}
cfg=copy.deepcopy(read_json(old/'config.json'));stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
cfg.update(run_id=r['run_id'],control=str(c),resume_from=str(old),created=stamp,predecessor_isolation={'confirmed':True,'old_control':str(old),'method':r['authorization']['kind'],'statement':r['authorization']['statement']},broker_token=secrets.token_hex(32),warm_queue_control=r['origin_queue_control'],coordinator_entrypoint='migration_overlap_resume.py')
write_json(c/'config.json',cfg)
for d in ('nodes','nodes-extra','manifests','stages','recovery-roles'):(c/d).mkdir(mode=0o700)
write_json(c/'state.json',{'run_id':r['run_id'],'status':'starting','stage':None,'started':stamp,'updated':stamp,'workers_expected':12,'threads_per_node':128,'recovery_overlap_planned':True})
plan=copy.deepcopy(read_json(old/'extension-plan.json'));assert plan['status']=='queued' and plan['exclude']==['eai_pwm'];plan['dependency_predecessor']=plan['depends_on']['control'];plan['depends_on']['control']=str(c);covered=plan['covered_by_controls']+[str(c)];plan['covered_by_controls']=list(dict.fromkeys(covered));plan['covered_by']={'priorities':[1,2,3],'controls':plan['covered_by_controls']};plan['updated']=stamp;write_json(c/'extension-plan.json',plan)
write_json(c/'recovery-authorization.json',dict(r['authorization'],previous_stop=(old/'STOP').read_text(),old_control=str(old),recorded=stamp))
write_json(c/'predecessor-shutdown.json',b['shutdown'])
# Stable old task payloads are merely candidate work; old receipts are immutable evidence.
origin=pathlib.Path(r['origin_queue_control']);stage=origin/'stages/p3-r1';paths=[]
for d in ('pending','running'):
 for parent,dirs,names in os.walk(stage/d):
  dirs[:]=[n for n in dirs if n!='.snapshots']
  paths.extend(pathlib.Path(parent)/n for n in names if n.endswith('.json'))
def read_task(path):
 task=read_json(path);identity=task['id'];assert type(identity)==int and identity>=0 and isinstance(task['entries'],list)
 return path,task
by_id={};issues=[];already_done=0
with concurrent.futures.ThreadPoolExecutor(max_workers=64) as ex:
 for path,task in ex.map(read_task,sorted(paths)):
  identity=task['id'];done=stage/'done'/str(identity%64)/(str(identity)+'.json')
  if done.exists():already_done+=1;continue
  if identity in by_id:
   assert by_id[identity]['task']==task,'conflicting original task ID'
   by_id[identity]['origin']['paths'].append(str(path))
  else:by_id[identity]={'origin':{'control':str(origin),'stage':'p3-r1','task_id':identity,'paths':[str(path)]},'task':task}
seen=set();duplicates=0;entries=logical=0
with open(c/'warm-raw-tasks.jsonl','x') as raw,open(c/'warm-tasks.jsonl','x') as warm:
 for identity,item in sorted(by_id.items()):
  raw.write(json.dumps(item,ensure_ascii=True)+'\n');fresh=[]
  for entry in item['task']['entries']:
   rel=entry['rel'];assert isinstance(rel,str) and rel and not os.path.isabs(rel) and all(p not in ('','.','..') for p in rel.split('/'))
   if rel in seen:duplicates+=1;continue
   seen.add(rel);fresh.append(entry);entries+=1;logical+=entry.get('size',0)
  if fresh:
   filtered=copy.deepcopy(item);filtered['task']['entries']=fresh;warm.write(json.dumps(filtered,ensure_ascii=True)+'\n')
summary={'tasks':len(by_id),'entries':entries,'logical_bytes':logical,'duplicate_paths_removed':duplicates,'already_done_paths':already_done,'original_queue_paths':len(paths),'manifest_sha256':hashlib.sha256((c/'warm-tasks.jsonl').read_bytes()).hexdigest()}
write_json(c/'warm-queue-import.json',summary)
# Old broker owns the fixed private port but has no payload/leases; retire only it.
broker=read_json(old/'broker.json');pid=broker['pid'];p=pathlib.Path('/proc')/str(pid)
def identity():
 try:
  f=(p/'stat').read_text().rsplit(')',1)[1].split();return f[0],f[19],(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
 except OSError:return None
live=identity()
if live and live[0]!='Z':
 assert str(old) in live[2] and any(x.endswith('migration_broker_parallel.py') for x in live[2])
 db=sqlite3.connect('file:/tmp/'+read_json(old/'config.json')['run_id']+'-broker.sqlite?mode=ro',uri=True);assert db.execute("SELECT count(*) FROM claims WHERE status='leased'").fetchone()[0]==0;db.close()
 before=live;os.kill(pid,signal.SIGTERM)
 for _ in range(200):
  live=identity()
  if not live or live[0]=='Z' or live[1]!=before[1]:break
  time.sleep(.05)
 else:raise RuntimeError('old broker did not exit')
dep={'prepared':stamp,'control':str(c),'predecessor':str(old),'files':origins,'warm_queue':summary,'authorization':r['authorization'],'old_broker':broker,'old_stop_retained':True}
write_json(c/'overlap-deployment.json',dep)
print(json.dumps({'public_config':{k:v for k,v in cfg.items() if k!='broker_token'},'deployment':dep,'extension_plan':plan}))
'''.replace('BODY',repr(json.dumps(body)),1)
 v=remote('master-0',code);r.update(status='prepared',public_config=v['public_config'],deployment=v['deployment']);write_json(RECORD,r)
 print(json.dumps({'status':'prepared','control':r['control'],'warm_queue':v['deployment']['warm_queue']}),flush=True)

def launch_roles():
 r=context()
 def launch(item):
  node,role=item
  code='''import sys,pathlib,json
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_recovery_role import launch
print(json.dumps(launch(c,ROLE)))
'''.replace('CONTROL',repr(r['control'])).replace('ROLE',repr(role))
  return node,role,remote(node,code)
 for items in ([('master-0',x) for x in ('broker','metrics','watchdog')],[(n,'primary') for n in t.HOSTS],[(n,'supplement') for n in t.HOSTS]):
  errors=[]
  with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
   tasks={ex.submit(launch,item):item for item in items}
   for f in concurrent.futures.as_completed(tasks):
    try:
     n,role,v=f.result();r['roles'][n+':'+role]=v;write_json(RECORD,r);print(n,role,v['pid'],flush=True)
    except Exception as exc:errors.append({'item':tasks[f],'error':str(exc)})
  if errors:raise RuntimeError(repr(errors))
 r['status']='workers_ready';write_json(RECORD,r)

def activate():
 r=context();code='''import pathlib,sys,json
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
assert not(c/'STOP').exists();cfg=read_json(c/'config.json');write_json('/share-mixed/project/.hcr-copy-monitor/active.json',{'control':str(c),'run_id':cfg['run_id'],'job':cfg['job'],'ssh_owner':cfg['ssh_owner']});print(json.dumps(read_json(c/'extension-plan.json')))
'''.replace('CONTROL',repr(r['control']))
 plan=remote('master-0',code);write_json(BASE/'active_migration.json',r['public_config']);write_json(BASE/'extension-plan.json',plan);write_json(BASE/'new_cluster_recovery.json',r);print(json.dumps({'active':r['control']}))

def main():
 p=argparse.ArgumentParser();p.add_argument('action',choices=('prepare','roles','activate'));a=p.parse_args()
 lock=open(BASE/'overlap-deployment.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 {'prepare':prepare,'roles':launch_roles,'activate':activate}[a.action]()
if __name__=='__main__':main()
