"""Read-only snapshots of deployed probe services; never adds probe work."""
import concurrent.futures
import json
from pathlib import Path
from migration_io import read_json, write_json
import migration_launch as t

base=Path(__file__).resolve().parent
active=read_json(base/'active_migration.json');deployment=read_json(base/'distributed_target_services_deployment.json')
t.JOB=active['job'];t.OWNER=active.get('ssh_owner')
roles={node:row['role'] for node,row in deployment['nodes'].items()}
value={'control':active['control'],'bundle':deployment['bundle'],'roles':roles,'nodes':{},'health':{}}
nodecode=r'''import collections,json,os,pathlib,time
role=ROLE;pid=role['pid'];root=pathlib.Path('/proc')/str(pid)
def sample():
 now=time.time()
 try:
  f=(root/'stat').read_text().rsplit(') ',1)[1].split();argv=(root/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
  result={'time':now,'pid':pid,'starttime_ticks':f[19],'state':f[0],'argv':argv,'identity_matches':str(f[19])==str(role['starttime_ticks']) and argv==role['argv'],'threads':int(f[17]),'main_wchan':(root/'wchan').read_text().strip(),'utime':int(f[11]),'stime':int(f[12])}
  threads=[];counts=collections.Counter()
  for task in(root/'task').iterdir():
   try:w=(task/'wchan').read_text().strip();counts[w]+=1;threads.append({'tid':int(task.name),'wchan':w})
   except OSError:pass
  result['thread_wchan_counts']=dict(counts);result['thread_details']=threads
  descriptors=[]
  for fd in(root/'fd').iterdir():
   try:target=os.readlink(fd)
   except OSError:continue
   if target.startswith('/share'):
    descriptors.append({'fd':int(fd.name),'target':target})
  result['shared_fds']=descriptors
  result['io']=dict((key,int(n)) for key,n in(line.split(':',1) for line in(root/'io').read_text().splitlines()))
  return result
 except OSError as exc:return {'time':now,'pid':pid,'error':type(exc).__name__}
a=sample();time.sleep(20);b=sample();print(json.dumps({'first':a,'second':b}))
'''
mastercode=r'''import concurrent.futures,json,pathlib,sys,time,urllib.request
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json');roles=ROLES;op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def health(item):
 node,role=item;started=time.time()
 try:
  request=urllib.request.Request(role['endpoint']+'/health',headers={'Authorization':'Bearer '+cfg['broker_token']})
  with op.open(request,timeout=8) as response:body=json.load(response)
  return node,{'time':time.time(),'duration':time.time()-started,'body':body,'identity_matches':body.get('pid')==role['pid'] and str(body.get('starttime_ticks'))==str(role['starttime_ticks']) and body.get('argv')==role['argv']}
 except Exception as exc:return node,{'time':time.time(),'duration':time.time()-started,'error':type(exc).__name__+':'+str(exc)}
def sample():
 with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:return dict(pool.map(health,roles.items()))
a=sample();time.sleep(20);b=sample();print(json.dumps({'first':a,'second':b}))
'''.replace('CONTROL',repr(active['control']),1).replace('ROLES',repr(roles),1)
def run_node(item):
 node,role=item
 return node,json.loads(t.ssh(node,nodecode.replace('ROLE',repr(role),1),owner=active.get('ssh_owner'))[1])
with concurrent.futures.ThreadPoolExecutor(max_workers=13) as pool:
 futures={pool.submit(run_node,item):('node',item[0]) for item in roles.items()}
 futures[pool.submit(t.ssh,'master-0',mastercode,owner=active.get('ssh_owner'))]=('health','master-0')
 for future in concurrent.futures.as_completed(futures):
  kind,node=futures[future]
  try:
   if kind=='node':n,result=future.result();value['nodes'][n]=result
   else:value['health']=json.loads(future.result()[1])
  except Exception as exc:value.setdefault('errors',{})[kind+':'+node]=type(exc).__name__+':'+str(exc)
  write_json(base/'distributed_target_service_probe.json',value)
summary={}
for node,probes in value['nodes'].items():
 first,second=probes['first'],probes['second'];healtha=value['health'].get('first',{}).get(node,{});healthb=value['health'].get('second',{}).get(node,{})
 summary[node]={'identity_matches':second.get('identity_matches'),'threads':second.get('threads'),'wchan':second.get('thread_wchan_counts'),'main_wchan':second.get('main_wchan'),'health_first':healtha.get('body',{}).get('counts',healtha.get('error')),'health_second':healthb.get('body',{}).get('counts',healthb.get('error')),'health_inflight_first':healtha.get('body',{}).get('inflight'),'health_inflight_second':healthb.get('body',{}).get('inflight'),'fds':second.get('shared_fds')}
print(json.dumps({'saved':str(base/'distributed_target_service_probe.json'),'summary':summary,'errors':value.get('errors',{})},ensure_ascii=False))
