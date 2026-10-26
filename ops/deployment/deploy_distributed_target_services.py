#!/usr/bin/env python3
import base64,concurrent.futures,datetime,fcntl,json,pathlib,sys,uuid
from migration_io import read_json,write_json
import migration_launch as t
B=pathlib.Path(__file__).parent
lock=open(B/'distributed-target-services.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
r=read_json(B/'active_migration.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
recordpath=B/'distributed_target_services_deployment.json'
try:d=read_json(recordpath)
except FileNotFoundError:
 uid=uuid.uuid4().hex;d={'id':uid,'status':'planned','control':r['control'],'bundle':r['control']+'/distributed-target-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uid[:6],'nodes':{}};write_json(recordpath,d)
if d['status']!='planned':raise RuntimeError('dispatch already attempted; inspect persisted node identities before further action')
body={'control':d['control'],'bundle':d['bundle'],'files':{n:base64.b64encode((B/n).read_bytes()).decode() for n in ['migration_target_probe.py','migration_io.py']}}
prepare=r'''import sys,pathlib,json,base64,hashlib
b=json.loads(BODY);c=pathlib.Path(b['control']);sys.dont_write_bytecode=True;sys.path.insert(0,str(c));from migration_io import read_json,write_json
assert not(c/'STOP').exists();s=read_json(c/'state.json');assert s.get('recovery_warm') and s.get('stage')=='p3-r3'
p=pathlib.Path(b['bundle']);p.mkdir(mode=0o700);(p/'nodes').mkdir();(p/'launches').mkdir();sha={}
for name,encoded in b['files'].items():
 raw=base64.b64decode(encoded);q=p/name;q.write_bytes(raw);q.chmod(0o700);sha[name]=hashlib.sha256(raw).hexdigest()
write_json(p/'code-proof.json',{'files_sha256':sha,'copy_workers_changed':False,'payload_reads_and_writes':False})
print(json.dumps({'bundle':str(p),'files_sha256':sha}))
'''.replace('BODY',repr(json.dumps(body)),1)
d['status']='preparing';write_json(recordpath,d)
try:d['code']=json.loads(t.ssh('master-0',prepare)[1]);write_json(recordpath,d)
except BaseException as exc:
 d.update(status='needs_inspection',error=str(exc));write_json(recordpath,d);raise
code=r'''import sys,os,pathlib,json,fcntl,subprocess,time,resource,socket
sys.dont_write_bytecode=True
c=pathlib.Path(CONTROL);bundle=pathlib.Path(BUNDLE);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');assert not(c/'STOP').exists();host=socket.gethostname()
guard=open('/tmp/'+cfg['run_id']+'-target-probe-dispatch.lock','a');fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
rolepath=bundle/'nodes'/(host+'.json');launchpath=bundle/'launches'/(host+'.json')
def identity(pid):
 try:
  p=pathlib.Path('/proc')/str(pid);f=(p/'stat').read_text().rsplit(')',1)[1].split();return {'pid':int(pid),'starttime_ticks':f[19],'state':f[0],'argv':(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
assert not rolepath.exists() and not launchpath.exists(),'existing attempt requires actual-process adoption, not repeat launch'
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
assert hard==resource.RLIM_INFINITY or hard>=65536
resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
argv=[sys.executable,'-u',str(bundle/'migration_target_probe.py'),'--control',str(c),'--port','0','--slots','32','--record',str(rolepath)]
env=dict(os.environ,PYTHONPATH=str(c),PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-target-probe-pycache')
log=bundle/(host+'-probe.log')
with open(log,'ab',buffering=0) as out:child=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True,env=env)
write_json(launchpath,{'pid':child.pid,'argv':argv,'host':host,'log':str(log),'started':time.time()})
for _ in range(160):
 live=identity(child.pid)
 if not live or live['state']=='Z' or child.poll() is not None:raise RuntimeError('metadata probe exited; inspect exact log without repeating dispatch')
 try:role=read_json(rolepath)
 except FileNotFoundError:role={}
 if role.get('pid')==child.pid:
  assert live['argv']==argv;role['actual_identity']=live;print(json.dumps(role));break
 time.sleep(.25)
else:raise RuntimeError('metadata probe role did not become ready; inspect existing pid before retry')
'''.replace('CONTROL',repr(d['control'])).replace('BUNDLE',repr(d['bundle']))
def node(n):
 try:return n,json.loads(t.ssh(n,code)[1]),None
 except BaseException as exc:return n,None,str(exc)
with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
 for n,value,error in pool.map(node,t.HOSTS):
  d['nodes'][n]={'role':value,'error':error};write_json(recordpath,d);print(n, 'ready' if value else 'needs_inspection',flush=True)
d['status']='services_ready' if all(v['role'] for v in d['nodes'].values()) and len(d['nodes'])==12 else 'needs_inspection';write_json(recordpath,d)
print(json.dumps({'status':d['status'],'bundle':d['bundle'],'ready_nodes':sum(bool(v['role']) for v in d['nodes'].values())}))
