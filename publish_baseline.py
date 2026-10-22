import sys,pathlib,json
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json
import migration_launch as t
cfg=read_json(pathlib.Path(__file__).parent/'active_migration.json');t.JOB=cfg['job']
code='''import pathlib,sys,json
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');old=pathlib.Path(cfg['resume_from']);p=read_json(old/'progress.json')
phases={str(k):{name:v.get(name,0) for name in ('discovered_files','discovered_bytes')} for k,v in p.get('phases',{}).items()}
b={'source_control':str(old),'source_updated':p.get('updated'),'phases':phases,'description':'Historical discovered lower bounds only; never verified or exact denominators.'}
write_json(c/'inventory-baseline.json',b)
print(json.dumps(b))
'''.replace('CONTROL',repr(cfg['control']))
print(t.ssh('master-0',code)[1])
