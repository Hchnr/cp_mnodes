import base64,datetime,json,pathlib,sys,uuid
from migration_io import read_json,write_json
import migration_launch as t
BASE=pathlib.Path(__file__).parent;r=read_json(BASE/'overlap_resume_recovery.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
bundle=r['control']+'/performance-watch-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6]
body={'control':r['control'],'bundle':bundle,'files':{n:base64.b64encode((BASE/n).read_bytes()).decode() for n in ('migration_performance.py','migration_performance_watch.py')},'monitor':base64.b64encode((BASE/'migration_progress.py').read_bytes()).decode()}
code=r'''import sys,os,pathlib,json,base64,hashlib,fcntl,subprocess,resource,time,socket
b=json.loads(BODY);c=pathlib.Path(b['control']);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');assert not(c/'STOP').exists()
lock=open('/tmp/'+cfg['run_id']+'-performance-dispatch.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
bundle=pathlib.Path(b['bundle']);bundle.mkdir(mode=0o700)
for name,value in b['files'].items():
 p=bundle/name;p.write_bytes(base64.b64decode(value));p.chmod(0o700)
rolepath=c/'recovery-roles'/(socket.gethostname()+'-performance.json')
def identity(pid):
 try:
  q=pathlib.Path('/proc')/str(pid);f=(q/'stat').read_text().rsplit(')',1)[1].split();return {'pid':int(pid),'startticks':f[19],'state':f[0],'argv':(q/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
try:prior=read_json(rolepath)
except FileNotFoundError:prior=None
if prior:
 live=identity(prior['pid']);assert live and live['state']!='Z' and live['startticks']==prior['startticks'] and live['argv']==prior['argv']
 role=prior
else:
 argv=[sys.executable,'-u',str(bundle/'migration_performance_watch.py'),'--control',str(c),'--interval','10']
 soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
 env=dict(os.environ,PYTHONPATH=str(c),PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-performance-pycache')
 log=c/'performance-watch.log'
 with open(log,'ab',buffering=0) as out:proc=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True,env=env)
 for _ in range(100):
  role=identity(proc.pid)
  if role and role['state']!='Z' and role['argv']==argv:break
  if proc.poll() is not None:raise RuntimeError('performance observer exited')
  time.sleep(.05)
 else:raise RuntimeError('observer identity uncertain; inspect before retry')
 role.update(role='performance',host=socket.gethostname(),control=str(c),log=str(log),bundle=str(bundle),interval=10)
 write_json(rolepath,role)
monitor=pathlib.Path('/share-mixed/project/.hcr-copy-monitor/progress.py')
old=monitor.read_bytes();new=base64.b64decode(b['monitor']);archive=c/'monitor-history';archive.mkdir(exist_ok=True)
for data in (old,new):
 digest=hashlib.sha256(data).hexdigest();p=archive/(digest+'.py')
 if not p.exists():p.write_bytes(data)
if old!=new:monitor.write_bytes(new)
proof={'performance':role,'monitor_sha256':hashlib.sha256(new).hexdigest(),'previous_monitor_sha256':hashlib.sha256(old).hexdigest(),'copy_and_seed_processes_changed':False}
write_json(c/'performance-watch-deployment.json',proof);print(json.dumps(proof))
'''.replace('BODY',repr(json.dumps(body)),1)
v=json.loads(t.ssh('master-0',code)[1]);write_json(BASE/'performance_watch_deployment.json',v);print(v)
