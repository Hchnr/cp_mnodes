"""Replace only the broker, retaining its SQLite leases and worker processes."""
import base64
import datetime
import json
import pathlib
import subprocess
import sys
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from migration_io import read_json, write_json
import migration_launch as transport

root = pathlib.Path(__file__).parent
active = read_json(root / 'active_migration.json')
transport.JOB = active['job']
name = 'broker-parallel-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:6]
bundle = {'control': active['control'], 'name': name, 'files': {
    n: base64.b64encode((root / n).read_bytes()).decode() for n in
    ('migration_broker_parallel.py', 'migration_broker.py', 'migration_io.py')}}
remote = r'''import base64,fcntl,json,os,pathlib,shutil,signal,sqlite3,subprocess,sys,time,urllib.request
b=json.loads(BUNDLE);c=pathlib.Path(b['control']);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
os.umask(0o077)
assert not (c/'STOP').exists(),'STOP protects training'
cfg=read_json(c/'config.json');old_record=read_json(c/'broker.json');old=int(old_record['pid'])
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def request(route):
 req=urllib.request.Request(cfg['broker_url']+route,headers={'Authorization':'Bearer '+cfg['broker_token']})
 with op.open(req,timeout=5) as response:return json.load(response)
baseline=request('/control');assert request('/health')['pid']==old
old_path=c/'migration_broker.py';proc_path=pathlib.Path('/proc')/str(old)
argv=(proc_path/'cmdline').read_bytes().decode().split('\0')
assert str(old_path) in argv and '--control' in argv and str(c) in argv, 'unexpected broker command'
start_ticks=(proc_path/'stat').read_text().rsplit(') ',1)[1].split()[19]
destination=c/b['name'];destination.mkdir(mode=0o700)
for filename,encoded in b['files'].items():
 payload=base64.b64decode(encoded);compile(payload,filename,'exec');(destination/filename).write_bytes(payload)
lock_path='/tmp/'+cfg['run_id']+'-broker.lock'
def alive(pid,ticks=None):
 p=pathlib.Path('/proc')/str(pid)
 try:
  fields=(p/'stat').read_text().rsplit(') ',1)[1].split()
  return fields[0]!='Z' and (ticks is None or fields[19]==ticks)
 except FileNotFoundError:return False
def wait_gone(pid,ticks=None):
 for _ in range(100):
  if not alive(pid,ticks):return
  time.sleep(.1)
 raise RuntimeError('broker did not exit')
def lock_free():
 # A process may reach Z while its last kernel-held descriptor is still
 # being released. Verify the flock separately rather than treating Z as
 # an immediate guarantee that the lock is already available.
 for _ in range(100):
  try:
   with open(lock_path,'a') as handle:
    fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
    fcntl.flock(handle,fcntl.LOCK_UN)
   return
  except BlockingIOError:time.sleep(.1)
 raise RuntimeError('broker flock remains held')
def launch(script,label):
 args=['nice','-n','15',sys.executable,'-u',str(script),'--control',str(c),'--port','37651']
 if shutil.which('ionice'):args=['ionice','-c','3']+args
 with open(c/(label+'.log'),'ab',buffering=0) as log:
  return subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
def wait_ready(process):
 for _ in range(35):
  if process.poll() is not None:raise RuntimeError('new broker process exited')
  try:
   health=request('/health')
   if health.get('pid')==process.pid:return request('/control')
  except Exception:pass
  time.sleep(.4)
 raise RuntimeError('broker health timed out')
assert alive(old,start_ticks),'broker identity changed'
write_json(destination/'deployment.json',{'status':'prepared','old_pid':old,'old_start_ticks':start_ticks,'baseline':baseline,'created':time.time()})
new=None
try:
 os.kill(old,signal.SIGTERM);wait_gone(old,start_ticks);lock_free()
 assert not (c/'STOP').exists(),'STOP appeared during broker handover'
 new=launch(destination/'migration_broker_parallel.py',b['name'])
 after=wait_ready(new)
 assert after['draining_hosts']==baseline['draining_hosts'],'drain state changed'
 result={'status':'ready','old_pid':old,'pid':new.pid,'bundle':str(destination),'baseline':baseline,'after':after,'updated':time.time()}
 write_json(destination/'deployment.json',result)
 write_json(c/'broker-parallel-deployment.json',result)
 print(json.dumps(result))
except BaseException as exc:
 if new and alive(new.pid):os.kill(new.pid,signal.SIGTERM);wait_gone(new.pid)
 lock_free()
 rollback=launch(old_path,b['name']+'-rollback')
 recovered=wait_ready(rollback)
 failure={'status':'rolled_back','reason':type(exc).__name__,'pid':rollback.pid,'bundle':str(destination),'control':recovered,'updated':time.time()}
 write_json(destination/'deployment.json',failure)
 print(json.dumps(failure))
 raise
'''.replace('BUNDLE', repr(json.dumps(bundle)), 1)
destination = transport.ssh_destination('master-0', owner=active.get('ssh_owner'))
result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20', '-o',
    'ServerAliveInterval=30', '-p', '2222', destination, 'python3 -'],
    input=remote, text=True, capture_output=True, timeout=180)
print(result.stdout, end='')
if result.returncode:
    print(result.stderr, file=sys.stderr)
    raise SystemExit(result.returncode)
record = json.loads(result.stdout)
write_json(root / 'parallel_broker_deployment.json', record)
