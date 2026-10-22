import base64,json,pathlib
from migration_io import read_json,write_json
import migration_launch as t
B=pathlib.Path(__file__).parent;r=read_json(B/'active_migration.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
b={'control':r['control'],'monitor':base64.b64encode((B/'migration_progress.py').read_bytes()).decode()}
code=r'''import sys,pathlib,hashlib,json,base64
b=json.loads(BODY);c=pathlib.Path(b['control']);sys.dont_write_bytecode=True;sys.path.insert(0,str(c));from migration_io import write_json
p=pathlib.Path('/share-mixed/project/.hcr-copy-monitor/progress.py');old=p.read_bytes();new=base64.b64decode(b['monitor']);archive=c/'monitor-history';archive.mkdir(exist_ok=True)
for raw in (old,new):
 q=archive/(hashlib.sha256(raw).hexdigest()+'.py')
 if not q.exists():q.write_bytes(raw)
if old!=new:p.write_bytes(new)
v={'sha256':hashlib.sha256(new).hexdigest(),'previous_sha256':hashlib.sha256(old).hexdigest(),'distributed_target_nodes_display':True,'copy_processes_changed':False};write_json(c/'distributed-monitor-deployment.json',v);print(json.dumps(v))
'''.replace('BODY',repr(json.dumps(b)),1)
x=json.loads(t.ssh('master-0',code)[1]);write_json(B/'distributed_monitor_deployment.json',x);print(json.dumps(x))
