import base64
import hashlib
import json
import pathlib

import migration_launch as transport
from migration_io import read_json

base = pathlib.Path(__file__).resolve().parent
cfg = read_json(base / 'active_migration.json')
transport.JOB = cfg['job']
raw = (base / 'migration_progress.py').read_bytes()
payload = {'control': cfg['control'], 'data': base64.b64encode(raw).decode(),
           'sha256': hashlib.sha256(raw).hexdigest()}
code = r'''import base64,hashlib,json,pathlib,py_compile,sys
b=json.loads(PAYLOAD);c=pathlib.Path(b['control']);m=pathlib.Path('/share-mixed/project/.hcr-copy-monitor')
sys.path.insert(0,str(c));from migration_io import read_json,write_json
deployment=read_json(c/'pipeline-deployment.json');bundle=pathlib.Path(deployment['bundle'])
raw=base64.b64decode(b['data']);assert hashlib.sha256(raw).hexdigest()==b['sha256']
saved=bundle/'migration_progress.py'
with open(saved,'xb') as f:f.write(raw)
py_compile.compile(str(saved),doraise=True)
for p in (c/'migration_progress.py',m/'migration_progress.py',m/'progress.py'):
 with open(p,'wb') as f:f.write(raw)
 p.chmod(0o700)
write_json(c/'pipeline-monitor-deployment.json',{'sha256':b['sha256'],'bundle':str(bundle)})
print(json.dumps({'monitor':str(m/'progress.py'),'sha256':b['sha256']}))
'''.replace('PAYLOAD', repr(json.dumps(payload)), 1)
print(transport.ssh('master-0', code)[1])
