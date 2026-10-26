"""Read-only throughput, work-supply and backend-client diagnostics."""
import json
import pathlib

from migration_io import read_json
import migration_launch as transport

base = pathlib.Path(__file__).resolve().parent
cfg = read_json(base / 'active_migration.json')
transport.JOB = cfg['job']
code = r'''import pathlib,sys,json,sqlite3,time,datetime,collections
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
from migration_progress import snapshot,Rate
v=snapshot(str(c));Rate().add(v)
s=read_json(c/'state.json');stage=s['stage']
db=sqlite3.connect('file:/tmp/'+s['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=10)
counts=dict(db.execute('SELECT status,COUNT(*) FROM claims WHERE stage=? GROUP BY status',(stage,)))
leased=list(db.execute("SELECT owner,COUNT(*) FROM claims WHERE stage=? AND status='leased' GROUP BY owner",(stage,)))
db.close()
pending=sum(sum(1 for p in (c/'stages'/stage/'pending'/str(i)).iterdir() if p.name.endswith('.json')) for i in range(64))
metrics=read_json(c/'metrics.json');samples=metrics.get('samples',[])
long=[]
for window in (60,300,600,900):
 eligible=[p for p in samples if time.time()-p['sample_time']<=window]
 if len(eligible)>1:
  a,b=eligible[0],eligible[-1];elapsed=b['sample_time']-a['sample_time']
  long.append({'window':window,'elapsed':elapsed,'logical_streamed_B_s':(b['streamed_bytes']-a['streamed_bytes'])/elapsed,
               'confirmed_new_B_s':(b['nodebytes']-a['nodebytes'])/elapsed})
active_files=[]
nodes=[]
for p in (c/'nodes').glob('*.json'):
 n=read_json(p);active=list(n.get('active',{}).values());sizes=[x.get('size',0) for x in active]
 active_files.extend(sizes)
 nodes.append({'host':n['host'],'threads':n['threads'],'active':len(active),'rate_cap':n.get('io_bytes_per_second'),
               'heartbeat_age':time.time()-datetime.datetime.fromisoformat(n['updated']).timestamp(),
               'broker_retries':n.get('broker_retries'), 'broker_error':n.get('broker_error')})
active_files.sort()
def quantile(f):return active_files[min(len(active_files)-1,int(len(active_files)*f))] if active_files else None
out={'sampled':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':s['status'],'stage':stage,
     'pending_batches':pending,'broker_counts':counts,'nodes':nodes,'active_total':len(active_files),
     'logical_rates':long,'active_file_sizes':{'p10':quantile(.1),'p50':quantile(.5),'p90':quantile(.9),'max':quantile(.9999)},
     'current_scan':v.get('current_scan'),'training':v.get('training_health'), 'stop':(c/'STOP').exists(),
     'memory':{l.split(':')[0]:l.split(':',1)[1].strip() for l in pathlib.Path('/proc/meminfo').read_text().splitlines() if l.startswith(('MemAvailable:','MemTotal:'))}}
print(json.dumps(out))
'''.replace('CONTROL', repr(cfg['control']))
value = json.loads(transport.ssh('master-0', code)[1])
(base / 'scaling_assessment_master.json').write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
print(json.dumps(value, ensure_ascii=False))
