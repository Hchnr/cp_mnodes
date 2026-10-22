import sys,json,pathlib
from migration_io import read_json,write_json
import migration_launch as t
r=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
code=r'''import sys,pathlib,json,time
c=pathlib.Path(CONTROL);sys.dont_write_bytecode=True;sys.path.insert(0,str(c));from migration_io import read_json
s=read_json(c/'state.json');p=read_json(c/'pipeline-deployment.json');q=pathlib.Path('/proc')/str(p['pid']);m=read_json(c/'metrics.json');samples=m.get('samples',m.get('history',[]))
try:seed=read_json(c/'seed-progress.json')
except FileNotFoundError:seed={}
v={'now':time.time(),'STOP':(c/'STOP').exists(),'updated':s.get('updated'),'status':s.get('status'),'warm':s.get('recovery_warm'),'filter':s.get('target_filter'),'probe':s.get('distributed_target_probe'),'producer':{'pid':p['pid'],'threads':len(list((q/'task').iterdir())),'wchan':(q/'wchan').read_text()},'seed':seed,'metrics':{k:m.get(k) for k in ['sample_time','copied_bytes','streamed_bytes','verified_files','nodes_seen','warnings']},'samples':samples[-8:]};print(json.dumps(v))
'''.replace('CONTROL',repr(r['control']))
x=json.loads(t.ssh('master-0',code)[1]);write_json(pathlib.Path(__file__).parent/'distributed_target_current.json',x)
print(json.dumps({k:v for k,v in x.items() if k not in ['seed','probe','samples']},ensure_ascii=False));print('probe_counts',x.get('probe',{}).get('counts'));print('seed_status',x['seed'].get('status'),x['seed'].get('rows_processed',x['seed'].get('imported_files')))
