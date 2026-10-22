"""Read-only compact inspection of the active copy run and its local broker."""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from migration_io import read_json, write_json
import migration_launch as transport

base = pathlib.Path(__file__).parent
active = read_json(base / 'active_migration.json')
transport.JOB = active['job']
code = r'''import datetime,json,os,pathlib,sqlite3,sys,time,urllib.request
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
cfg=read_json(c/'config.json');warnings=[]
sys.pycache_prefix='/tmp/'+cfg['run_id']+'-inspection-pycache'
observation={}
exec(compile(OBSERVATION_SOURCE,'<readonly-control-census>','exec'),observation)
from migration_nodes import load_nodes
def optional(name):
 try:return read_json(c/name)
 except (OSError,ValueError,TypeError) as exc:
  warnings.append(name+': '+type(exc).__name__);return {}
state=optional('state.json');progress=optional('progress.json');pipeline=optional('pipeline.json')
metrics=optional('metrics.json');inventory=optional('inventory.json');training=optional('training-health.json')
performance=optional('performance.json');seed=optional('seed-progress.json')
broker=optional('broker.json');deployment=optional('pipeline-deployment.json')
node_components=[component for node in load_nodes(c,warnings)
                 for component in node.get('components',[])]
if not deployment.get('pid'):
 import socket
 try:deployment=read_json(c/'recovery-roles'/(socket.gethostname()+'-producer.json'))
 except (OSError,ValueError):pass
def tail(path,limit=10000):
 try:
  with open(path,'rb') as handle:
   handle.seek(0,2);size=handle.tell();handle.seek(max(0,size-limit));data=handle.read().decode('utf-8','replace')
  return {'bytes':size,'mtime':pathlib.Path(path).stat().st_mtime,'tail':data}
 except OSError as exc:return {'error':type(exc).__name__}
def process(pid):
 p=pathlib.Path('/proc')/str(pid)
 try:
  fields=(p/'stat').read_text().rsplit(') ',1)[1].split();argv=(p/'cmdline').read_bytes().decode().split('\0')
  result={'pid':pid,'state':fields[0],'start_ticks':fields[19],'threads':len(list((p/'task').iterdir())),'argv':argv}
  log=pathlib.Path(os.readlink(p/'fd/1'))
  if log.is_relative_to(c):result['log']=tail(log)
  return result
 except OSError as exc:return {'pid':pid,'error':type(exc).__name__}
processes={'producer':process(deployment.get('pid')),'broker':process(broker.get('pid')),
           'metrics':process(metrics.get('pid'))}
if performance.get('pid'):processes['performance']=process(performance['pid'])
helpers=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  argv=(p/'cmdline').read_bytes().decode().split('\0')
  if any(x.startswith(str(c)+'/') and x.endswith('.py') for x in argv) and any(x in argv for x in ('helper','collect','finalize','metadata','hardlinks','collect-hardlinks','directories')):
   helpers.append(process(int(p.name)))
 except (OSError,UnicodeError):pass
db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=3)
counts=db.execute('SELECT stage,status,count(*) FROM claims GROUP BY stage,status').fetchall()
duplicates=db.execute("SELECT stage,owner,count(*) FROM claims WHERE status='leased' GROUP BY stage,owner HAVING count(*)>1").fetchall()
db.close()
op=urllib.request.build_opener(urllib.request.ProxyHandler({}));control={}
try:
 req=urllib.request.Request(cfg['broker_url']+'/control',headers={'Authorization':'Bearer '+cfg['broker_token']})
 with op.open(req,timeout=8) as response:control=json.load(response)
except Exception as exc:warnings.append('broker control: '+type(exc).__name__)
stage=state.get('stage')
queue_census=observation['observe_stage_queues'](c)
issue_census=observation['observe_issue_files'](c)
for label,value in (('queue census',queue_census),('issue census',issue_census)):
 for error in value['errors']:warnings.append(label+': '+error['path']+': '+error['error'])
if stage:
 current=queue_census['stages'].get(stage)
 if current is None:
  warnings.append('queue census: active stage missing: '+str(stage));pending=running=None
 else:pending,running=current['pending'],current['running']
elif queue_census['complete']:
 pending=sum(v['pending'] for v in queue_census['stages'].values())
 running=sum(v['running'] for v in queue_census['stages'].values())
else:pending=running=None
issues=issue_census['files']
samples=metrics.pop('samples',None)
history=metrics.pop('history',[])
if samples is None:samples=history
result={'checked_at':time.time(),'stop':(c/'STOP').exists(),'state':state,'progress':progress,'pipeline':pipeline,
 'node_components':node_components,
 'metrics':metrics,'metrics_sample_count':len(samples),'metrics_recent_samples':samples[-8:],
 'inventory_keys':list(inventory),'inventory_scalars':{k:v for k,v in inventory.items() if not isinstance(v,(dict,list))},
 'inventory_summary':inventory.get('summary',inventory.get('totals',{})),
 'training':training,'broker_control':control,'claim_counts':counts,'duplicate_leased_owners':duplicates,
 'queue':{'stage':stage,'pending':pending,'running':running},
 'all_stage_queues':queue_census,'issue_census':issue_census,
 'processes':processes,'helpers':helpers,
 'events':tail(c/'events.jsonl',6000),'issues':issues,'warnings':warnings,
 'performance':{k:performance.get(k) for k in ('updated','sample_time','status','stop','terminal')},
 'performance_lanes':{lane:{**{k:value.get(k) for k in ('mode','armed','diagnostic_trigger','diagnostics','reason_codes','rates','window','optimize',
              'below_threshold','recovery_target_met','low_threshold','recovery_target','incident','evidence')},
              **{key:value.get('next_state',{}).get(key) for key in ('last_disruption_at','last_stop_at','last_reset_at')}}
     for lane,value in performance.get('lanes',{}).items()},
 'seed':{k:seed.get(k) for k in ('status','updated','rows_processed','rows_inserted','receipts','results','verified_files','verified_bytes')},
 'extension_plan':optional('extension-plan.json')}
print(json.dumps(result,ensure_ascii=True))
'''.replace('CONTROL', repr(active['control']), 1).replace(
    'OBSERVATION_SOURCE', repr((base / 'migration_control_observation.py').read_text()), 1)
_, out = transport.ssh('master-0', code, owner=active.get('ssh_owner'))
result = json.loads(out)
path = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else base / 'heartbeat_inspection.json'
write_json(path, result)
print(json.dumps({'saved': str(path), 'stop': result['stop'], 'stage': result['state'].get('stage'),
    'status': result['state'].get('status'), 'queue': result['queue'],
    'warnings': result['warnings'], 'duplicate_leased_owners': result['duplicate_leased_owners']}, ensure_ascii=False))
