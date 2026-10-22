import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json,sqlite3,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json');nodes=[read_json(p) for p in (c/'nodes').glob('*.json')]
for n in nodes:
 expected=20000000000-(32*1024**2 if n['host'].endswith('master-0') else 0)
 assert n['threads']==64 and n.get('io_bytes_per_second')==expected,(n['host'],n['threads'],n.get('io_bytes_per_second'))
 assert not n.get('thread_error') and not n.get('ended')
db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=3)
owner='job-'+cfg['job']+'-worker-7-%';rows=db.execute("SELECT stage,id FROM claims WHERE owner LIKE ? AND status='done' ORDER BY rowid DESC LIMIT 400",(owner,)).fetchall()
errors=[]
for stage,taskid in rows:
 try:r=read_json(c/'stages'/stage/'done'/str(taskid%64)/(str(taskid)+'.json'))
 except FileNotFoundError:continue
 for item in r['results']:
  if item['status']=='error':errors.append(item)
 if len(errors)>=2:break
print(json.dumps({'verified_nodes':len(nodes),'threads_each':64,'io_bytes_per_second_each':20000000000,'master_reserved_MiB_s':32,'stop':(c/'STOP').exists(),'file_errors':errors,'receipts_examined_max':len(rows)}))
'''.replace('CONTROL',repr(cfg['control']))
print(t.ssh('master-0',code)[1])
