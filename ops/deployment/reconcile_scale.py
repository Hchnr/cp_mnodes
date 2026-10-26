import sys,pathlib,json,datetime
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json,write_json
import migration_launch as t
root=pathlib.Path(__file__).parent;cfg=read_json(root/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json,urllib.request,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');assert cfg['threads']==64 and cfg['io_bytes_per_second']==20000000000
assert not (c/'STOP').exists(),'STOP protects training'
ns=[read_json(p) for p in (c/'nodes').glob('*.json')];assert len(ns)==12
result={}
for n in ns:
 expected=20000000000-(32*1024**2 if n['host'].endswith('master-0') else 0)
 assert n['threads']==64 and n.get('io_bytes_per_second')==expected,(n['host'],n['threads'],n.get('io_bytes_per_second'))
 assert not n.get('thread_error') and not n.get('ended'),n['host']
 label=n['host'].split('job-'+cfg['job']+'-')[1];result[label]={'pid':n['pid'],'threads':n['threads'],'io_bytes_per_second':n['io_bytes_per_second']}
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
req=urllib.request.Request(cfg['broker_url']+'/control',data=json.dumps({'draining_hosts':[]}).encode(),method='POST',headers={'Authorization':'Bearer '+cfg['broker_token'],'Content-Type':'application/json'})
with op.open(req,timeout=60) as r:state=json.load(r)
assert state['ok'] and not state['draining_hosts']
summary={'phase':'complete','nodes':12,'threads_each':64,'io_bytes_per_second_each':20000000000,'completed':result,'draining_hosts':state['draining_hosts'],'training':read_json(c/'training-health.json').get('state'),'migration_state':{k:read_json(c/'state.json').get(k) for k in ('status','stage','updated')}}
write_json(c/'scale-upgrade.json',summary);print(json.dumps(summary))
'''.replace('CONTROL',repr(cfg['control']))
_,out=t.ssh('master-0',code);s=json.loads(out)
p=root/'scale_deploy.json';old=read_json(p);old.update(phase='complete',completed=s['completed'],remaining=[],finished=datetime.datetime.now(datetime.timezone.utc).isoformat());write_json(p,old)
print(json.dumps(s))
