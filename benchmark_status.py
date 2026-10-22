import pathlib,sys,json
sys.path.insert(0,str(pathlib.Path(__file__).parent));from migration_io import read_json
import migration_launch as t
base=pathlib.Path(__file__).parent;cfg=read_json(base/'active_migration.json');t.JOB=cfg['job'];b=json.loads((base/'benchmark_active.json').read_text())
code='''import pathlib,sys,json,os
b=json.loads(INFO);sys.path.insert(0,b['control']);from migration_io import read_json
r=read_json(pathlib.Path(b['root'])/'results.json');p=pathlib.Path('/proc')/str(b['pid']);alive=p.exists() and (p/'stat').read_text().split(') ')[1].split()[0]!='Z'
print(json.dumps({'alive':alive,'wchan':(p/'wchan').read_text() if alive else None,'result':r,'log_tail':pathlib.Path(b['log']).read_text()[-3000:],'training':read_json(pathlib.Path(b['control'])/'training-health.json').get('state'),'stop':(pathlib.Path(b['control'])/'STOP').exists()}))
'''.replace('INFO',repr(json.dumps(b)))
s=json.loads(t.ssh(b['node'],code)[1]);(base/'benchmark_status_snapshot.json').write_text(json.dumps(s,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'alive':s['alive'],'status':s['result'].get('status'),'trials':s['result'].get('trials'),'workloads':s['result'].get('workloads'),'training':s['training'],'stop':s['stop'],'log_tail':s['log_tail'][-1800:]},ensure_ascii=False))
