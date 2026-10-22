import base64,datetime,json,pathlib,sys,uuid
from migration_io import read_json,write_json
import migration_launch as t
base=pathlib.Path(__file__).parent;r=read_json(base/'active_migration.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
bundle=r['control']+'/cache-age-20261021-'+uuid.uuid4().hex[:8]
b={'control':r['control'],'bundle':bundle,'files':{n:base64.b64encode((base/n).read_bytes()).decode() for n in ['rollout_cache_age.py','migration_io.py']}}
code=r'''import pathlib,json,base64,sys,hashlib,subprocess,os
b=json.loads(BODY);c=pathlib.Path(b['control']);assert not(c/'STOP').exists();p=pathlib.Path(b['bundle']);p.mkdir(mode=0o700)
sha={}
for name,encoded in b['files'].items():
 q=p/name;raw=base64.b64decode(encoded);q.write_bytes(raw);q.chmod(0o700);sha[name]=hashlib.sha256(raw).hexdigest()
env=dict(os.environ,PYTHONPYCACHEPREFIX='/tmp/hcr-cache-age-helper-pycache')
subprocess.run([sys.executable,str(p/'rollout_cache_age.py'),'--help'],env=env,check=True,stdout=subprocess.DEVNULL)
print(json.dumps({'control':str(c),'bundle':str(p),'script':str(p/'rollout_cache_age.py'),'sha256':sha}))
'''.replace('BODY',repr(json.dumps(b)),1)
x=json.loads(t.ssh('master-0',code)[1]);write_json(base/'cache_age_canary_deployment.json',x);print(x['script'])
