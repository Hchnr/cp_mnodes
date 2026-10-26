import sys,pathlib,base64,json
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json
import migration_launch as t
p=pathlib.Path(__file__).parent;c=read_json(p/'active_migration.json');t.JOB=c['job']
data=base64.b64encode((p/'migration_progress.py').read_bytes()).decode()
code='''import pathlib,base64,sys,json
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
payload=base64.b64decode(DATA);compile(payload,'progress.py','exec')
(c/'migration_progress.py').write_bytes(payload)
m=pathlib.Path('/share-mixed/project/.hcr-copy-monitor');(m/'progress.py').write_bytes(payload);(m/'migration_progress.py').write_bytes(payload)
print(json.dumps({'progress_updated':True,'control':str(c)}))
'''.replace('CONTROL',repr(c['control'])).replace('DATA',repr(data))
print(t.ssh('master-0',code)[1])
