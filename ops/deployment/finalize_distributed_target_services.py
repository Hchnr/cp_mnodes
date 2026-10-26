import base64,concurrent.futures,json,pathlib,sys
from migration_io import read_json,write_json
import migration_launch as t
B=pathlib.Path(__file__).parent;r=read_json(B/'active_migration.json');d=read_json(B/'distributed_target_services_deployment.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
x=read_json(B/'target_service_worker10_inspection.json');expected=x['nodes'];assert expected['pid']==x['launches']['pid'] and expected['argv']==x['launches']['argv']
code="""import pathlib,json
p=pathlib.Path('/proc')/str(PID);f=(p/'stat').read_text().rsplit(')',1)[1].split();a=(p/'cmdline').read_bytes().rstrip(b'\\0').decode().split('\\0');assert f[0]!='Z' and f[19]==START and a==ARGV;print(json.dumps({'pid':PID,'starttime_ticks':f[19],'argv':a,'state':f[0]}))
""".replace('PID',str(expected['pid'])).replace('START',repr(expected['starttime_ticks'])).replace('ARGV',repr(expected['argv']))
ident=json.loads(t.ssh('worker-10',code)[1]);expected['actual_identity']=ident;d['nodes']['worker-10']={'role':expected,'error':None,'adopted_after_ssh_timeout':True};write_json(B/'distributed_target_services_deployment.json',d)
body={'control':r['control'],'bundle':d['bundle'],'roles':[v['role'] for v in d['nodes'].values()],'files':{n:base64.b64encode((B/n).read_bytes()).decode() for n in ['migration_overlap_parallel_filter.py','migration_distributed_target_filter.py']}}
remote=r'''import sys,pathlib,json,base64,hashlib,concurrent.futures,urllib.request
b=json.loads(BODY);c=pathlib.Path(b['control']);sys.dont_write_bytecode=True;sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');assert not(c/'STOP').exists();p=pathlib.Path(b['bundle']);sha={}
for name,encoded in b['files'].items():
 raw=base64.b64decode(encoded);q=p/name
 if q.exists():assert q.read_bytes()==raw,'already published client bytes differ'
 else:
  with open(q,'xb') as f:f.write(raw)
 q.chmod(0o700);sha[name]=hashlib.sha256(raw).hexdigest()
def check(role):
 import time
 started=time.monotonic()
 op=urllib.request.build_opener(urllib.request.ProxyHandler({}));headers={'Authorization':'Bearer '+cfg['broker_token'],'Content-Type':'application/json'}
 req=urllib.request.Request(role['endpoint']+'/health',headers=headers)
 with op.open(req,timeout=30) as response:h=json.load(response)
 assert h['ok'] and h['pid']==role['pid'] and h['starttime_ticks']==role['starttime_ticks'] and h['argv']==role['argv'] and h['run_id']==cfg['run_id']
 req=urllib.request.Request(role['endpoint']+'/probe',headers=headers,data=json.dumps({'entry':{'rel':'__codex_readonly_target_probe_health_20261021','kind':'file'}}).encode())
 with op.open(req,timeout=30) as response:v=json.load(response)
 assert v['ok'] is True and v['missing'] is True and v['entry']['rel']=='__codex_readonly_target_probe_health_20261021'
 return {'host':role['host'],'endpoint':role['endpoint'],'identity_matches':True,'read_only_probe_passed':True,'elapsed_seconds':time.monotonic()-started}
with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:health=list(pool.map(check,b['roles']))
proof={'files_sha256':sha,'node_checks':health,'data_workers_changed':False,'source_writes':False};write_json(p/'client-code-proof.json',proof);print(json.dumps(proof))
'''.replace('BODY',repr(json.dumps(body)),1)
proof=json.loads(t.ssh('master-0',remote)[1]);d['code']['files_sha256'].update(proof['files_sha256']);d.update(status='services_ready',health_checks=proof['node_checks']);write_json(B/'distributed_target_services_deployment.json',d);print(json.dumps({'status':d['status'],'nodes':len(d['nodes']),'client_sha256':proof['files_sha256']['migration_distributed_target_filter.py']}))
