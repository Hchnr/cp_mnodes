"""Deploy only the monitor presentation and validate its current output."""
import base64
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from migration_io import read_json, write_json
import migration_launch as transport

base = pathlib.Path(__file__).parent
active = read_json(base / 'active_migration.json')
transport.JOB = active['job']
payload = base64.b64encode((base / 'migration_progress.py').read_bytes()).decode()
code = r'''import base64,datetime,json,os,pathlib,subprocess,sys,uuid
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
m=pathlib.Path('/share-mixed/project/.hcr-copy-monitor')
plan=read_json(c/'extension-plan.json');assert plan['priority']==4
data=base64.b64decode(PAYLOAD);compile(data,'migration_progress.py','exec')
for path in (c/'migration_progress.py',m/'migration_progress.py',m/'progress.py'):
 temp=path.with_name('.'+path.name+'.'+uuid.uuid4().hex+'.tmp')
 temp.write_bytes(data);temp.chmod(0o700);os.replace(temp,path)
def run(args):
 p=subprocess.run([sys.executable,str(m/'progress.py')]+args,capture_output=True,text=True,timeout=45)
 assert p.returncode==0,p.stderr[-1000:]
 return p.stdout
value=json.loads(run(['--json']))
assert set(('1','2','3','4')) <= set(value['phases'])
assert value['overall']['percent'] is None and value['eta_seconds'] is None
output=run([])
assert output.startswith('='*96+'\n复制进度开始  |  ')
assert '【后续复制目标】' not in output
assert output.count('4. /share/project → /share-mixed/project')==1
record={'updated':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'monitor_updated':True,'priorities':['1','2','3','4'],'separated_future_section':False,
        'start_separator':True,'phase':value.get('phase'),'stage':value.get('stage'),
        'extension_status':value['extension_plan']['status'],'global_percent':value['overall']['percent']}
write_json(c/'monitor-unified-deployment.json',record)
print(json.dumps({'deployment':record,'output':output},ensure_ascii=True))
'''.replace('CONTROL', repr(active['control']), 1).replace('PAYLOAD', repr(payload), 1)
_, output = transport.ssh('master-0', code)
result = json.loads(output)
write_json(base / 'monitor_unified_deployment.json', result['deployment'])
(base / 'monitor_unified_verified.txt').write_text(result['output'])
print(json.dumps(result['deployment'], ensure_ascii=False))
