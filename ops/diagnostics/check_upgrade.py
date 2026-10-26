import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json,os,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json');ps=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  a=(p/'cmdline').read_bytes().decode(errors='replace').split('\\0')
  if any(x.startswith(str(c)+'/migration_') for x in a):
   d={'pid':int(p.name),'args':a[1:],'state':(p/'stat').read_text().split(') ')[1].split()[0],'wchan':(p/'wchan').read_text(),'io':(p/'io').read_text()}
   ps.append(d)
 except OSError:pass
logs={p.name:p.read_text()[-1000:] for p in c.glob('*.log') if p.stat().st_size and not p.name.startswith('broker-')}
files={p.name:p.stat().st_size for p in pathlib.Path('/tmp').glob(cfg['run_id']+'*') if p.is_file()}
print(json.dumps({'processes':ps,'logs':logs,'tmp':files,'inventory_plan':(c/'inventory/plan.json').exists(),'stop':(c/'STOP').exists(),'state':read_json(c/'state.json')}))
'''.replace('CONTROL',repr(cfg['control']))
print(t.ssh('master-0',code)[1])
