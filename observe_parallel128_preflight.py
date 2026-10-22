"""Read-only migration process/metadata observation for producer handover."""
import concurrent.futures
import json
import pathlib
import sys
import time
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json, write_json
import migration_launch as transport
base=pathlib.Path(__file__).parent
active=read_json(base/'active_migration.json')
transport.JOB=active['job'];transport.OWNER=active['ssh_owner']
control=active['control']
master=r'''import collections,json,os,pathlib,sqlite3,sys,time,urllib.request
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
warnings=[]
def get(name):
 try:return read_json(c/name)
 except (OSError,ValueError) as exc:warnings.append(name+': '+type(exc).__name__);return {}
def proc(pid):
 p=pathlib.Path('/proc')/str(pid)
 try:
  fields=(p/'stat').read_text().rsplit(') ',1)[1].split()
  argv=(p/'cmdline').read_bytes().decode().split('\0');argv=[x for x in argv if x]
  io={a:int(b) for a,b in (line.split(':',1) for line in (p/'io').read_text().splitlines())}
  return {'pid':int(pid),'state':fields[0],'start_ticks':fields[19],'threads':len(list((p/'task').iterdir())),'argv':argv,'io':io,'wchan':(p/'wchan').read_text().strip()}
 except (OSError,UnicodeError) as exc:return {'pid':pid,'error':type(exc).__name__}
s=get('state.json');m=get('metrics.json');p=get('performance.json');seed=get('seed-progress.json');b=get('broker.json');cfg=get('config.json')
processes=[]
for path in pathlib.Path('/proc').iterdir():
 if not path.name.isdigit():continue
 try:
  argv=(path/'cmdline').read_bytes().decode().split('\0')
  if str(c) not in argv or not any(x.startswith(str(c)+'/') and x.endswith('.py') for x in argv):continue
  if any(x.endswith('migration_cached_worker.py') for x in argv):continue
  processes.append(proc(path.name))
 except (OSError,UnicodeError):pass
samples=m.get('samples',m.get('history',[]));samples=sorted([x for x in samples if isinstance(x,dict) and isinstance(x.get('sample_time'),(int,float))],key=lambda x:x['sample_time'])
rates={}
for window in (60,300):
 if not samples:continue
 last=samples[-1];old=[x for x in samples if x['sample_time']<=last['sample_time']-window]
 if not old:rates[str(window)]={'valid':False,'reason':'insufficient_samples'};continue
 first=old[-1];delta=last['sample_time']-first['sample_time'];points=[x for x in samples if x['sample_time']>=first['sample_time']]
 item={'valid':True,'seconds':delta,'sample_time':last['sample_time'],'age':time.time()-last['sample_time'],'max_gap':max(z['sample_time']-a['sample_time'] for a,z in zip(points,points[1:]))}
 for k in ('nodebytes','streamed_bytes','verified_files','verified_bytes'):
  if isinstance(last.get(k),(int,float)) and isinstance(first.get(k),(int,float)):
   item[k+'_per_second']=(last[k]-first[k])/delta
 if item['age']>30 or item['max_gap']>30 or any(v<0 for k,v in item.items() if k.endswith('_per_second')):item['valid']=False
 rates[str(window)]=item
queue={'stage':s.get('stage'),'pending':0,'running':0}
if s.get('stage'):
 root=c/'stages'/s['stage']
 for shard in range(64):
  path=root/'pending'/str(shard)
  try:queue['pending']+=sum(x.name.endswith('.json') for x in path.iterdir())
  except FileNotFoundError:pass
 try:queue['running']=sum(x.name.endswith('.json') for x in (root/'running').iterdir())
 except FileNotFoundError:pass
claims=[];duplicates=[]
try:
 db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=3)
 db.execute('PRAGMA query_only=ON')
 claims=db.execute('SELECT stage,status,count(*) FROM claims GROUP BY stage,status').fetchall()
 duplicates=db.execute("SELECT stage,owner,count(*) FROM claims WHERE status='leased' GROUP BY stage,owner HAVING count(*)>1").fetchall();db.close()
except (sqlite3.Error,KeyError) as exc:warnings.append('claims: '+type(exc).__name__)
broker_control={}
try:
 op=urllib.request.build_opener(urllib.request.ProxyHandler({}));req=urllib.request.Request(cfg['broker_url']+'/control',headers={'Authorization':'Bearer '+cfg['broker_token']})
 with op.open(req,timeout=5) as response:broker_control=json.load(response)
except Exception as exc:warnings.append('broker_control: '+type(exc).__name__)
lanes={}
for name,value in p.get('lanes',{}).items():
 lanes[name]={k:value.get(k) for k in ('mode','armed','trigger','optimize','diagnostics','reason_codes','rates','low_threshold','recovery_target','window','completion_evidence')}
print(json.dumps({'checked_at':time.time(),'control':str(c),'stop':(c/'STOP').exists(),'state':{k:s.get(k) for k in ('run_id','status','updated','phase','round','stage','active_manifest','recovery_warm','resume_index_building','resume_index_error','scan_complete','enqueued_batches','discovered_files','discovered_bytes')},'seed':{k:seed.get(k) for k in ('run_id','status','updated','started','processed_receipts','receipt_paths_discovered','verified_files','verified_symlinks','verified_bytes','rows_processed','read_workers','results','receipts','elapsed_seconds')},'metrics':{'sample_time':m.get('sample_time'),'status':m.get('status'),'current_stage':m.get('current_stage'),'rates':rates},'performance':{'sample_time':p.get('sample_time'),'status':p.get('status'),'optimize':p.get('optimize'),'diagnostic_trigger':p.get('diagnostic_trigger'),'lanes':lanes},'training':get('training-health.json'),'queue':queue,'claim_counts':claims,'duplicate_owners':duplicates,'broker_control':broker_control,'core_processes':processes,'warnings':warnings},ensure_ascii=True))
'''.replace('CONTROL',repr(control),1)
worker=r'''import json,os,pathlib,socket,sys,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
host=socket.gethostname();lanes=[]
for name,folder in [('primary','nodes'),('extra64','nodes-extra')]:
 try:
  x=read_json(c/folder/(host+'.json'));p=pathlib.Path('/proc')/str(x.get('pid'));st=(p/'stat').read_text().rsplit(') ',1)[1].split();argv=[a for a in (p/'cmdline').read_bytes().decode().split('\0') if a]
  lanes.append({'lane':name,'pid':x.get('pid'),'alive':st[0] not in ('Z','X'),'state':st[0],'start_ticks':st[19],'threads_actual':len(list((p/'task').iterdir())),'argv':argv,'updated':x.get('updated'),'ended':x.get('ended'),'thread_error':x.get('thread_error'),'tuning_error':x.get('tuning_error'),'threads_configured':x.get('threads'),'io_bytes_per_second':x.get('io_bytes_per_second'),'active':len(x.get('active',{})),'awaiting_receipt':sum(bool(a.get('awaiting_receipt')) for a in x.get('active',{}).values()),'bytes':x.get('bytes'),'streamed_bytes':x.get('streamed_bytes'),'counts':x.get('counts',{}),'state_instance':x.get('instance')})
 except (OSError,ValueError) as exc:lanes.append({'lane':name,'alive':False,'error':type(exc).__name__+': '+str(exc)})
print(json.dumps({'checked_at':time.time(),'host':host,'stop':(c/'STOP').exists(),'lanes':lanes},ensure_ascii=True))
'''.replace('CONTROL',repr(control),1)
result={'started_at':time.time(),'job':active['job'],'ssh_owner':active['ssh_owner'],'control':control,'nodes':{},'errors':{}}
def call(node,code):
 try:return json.loads(transport.ssh(node,code,owner=active['ssh_owner'])[1])
 except Exception as exc:return {'error':type(exc).__name__+': '+str(exc)}
with concurrent.futures.ThreadPoolExecutor(max_workers=13) as pool:
 futures={pool.submit(call,node,worker):node for node in transport.HOSTS}
 core=pool.submit(call,'master-0',master)
 for f in concurrent.futures.as_completed(futures):
  node=futures[f];result['nodes'][node]=f.result()
 result['master_first']=core.result()
result['master_second']=call('master-0',master)
a=result['master_first'].get('seed',{});b=result['master_second'].get('seed',{})
elapsed=result['master_second'].get('checked_at',0)-result['master_first'].get('checked_at',0)
result['interval_seed']={'seconds':elapsed,'same_status':a.get('status')==b.get('status')}
if elapsed>0:
 for key in ('processed_receipts','verified_files','rows_processed'):
  if isinstance(a.get(key),(int,float)) and isinstance(b.get(key),(int,float)):
   result['interval_seed'][key+'_per_second']=(b[key]-a[key])/elapsed
result['finished_at']=time.time();target=base/'parallel128_preflight_observation.json';write_json(target,result)
lanes=[lane for x in result['nodes'].values() for lane in x.get('lanes',[])]
print(json.dumps({'saved':str(target),'control':control,'alive_lanes':sum(bool(x.get('alive')) for x in lanes),'configured_threads':sum(x.get('threads_configured',0) or 0 for x in lanes),'active':sum(x.get('active',0) or 0 for x in lanes),'awaiting_receipt':sum(x.get('awaiting_receipt',0) or 0 for x in lanes),'failed_nodes':{k:v for k,v in result['nodes'].items() if v.get('error')},'master':result['master_second'],'interval_seed':result['interval_seed']},ensure_ascii=False))
