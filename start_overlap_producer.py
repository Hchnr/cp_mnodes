import base64,fcntl,json,pathlib,sys
from migration_io import read_json,write_json
import migration_launch as t
BASE=pathlib.Path(__file__).resolve().parent
lock=open(BASE/'overlap-producer-start.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
r=read_json(BASE/'overlap_resume_recovery.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
data=base64.b64encode((BASE/'migration_overlap_resume.py').read_bytes()).decode()
code=r'''import os,sys,pathlib,json,base64,fcntl,resource,subprocess,time,socket,hashlib
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');guard=open('/tmp/'+cfg['run_id']+'-overlap-dispatch.lock','a');fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert not(c/'STOP').exists()
entry=c/'migration_overlap_resume.py';content=base64.b64decode(DATA)
if entry.exists():assert entry.read_bytes()==content,'immutable entry mismatch'
else:
 with open(entry,'xb') as out:out.write(content)
 entry.chmod(0o700)
argv=[sys.executable,'-u',str(entry),'run','--control',str(c),'--warm-tasks',str(c/'warm-tasks.jsonl')]
rolepath=c/'recovery-roles'/(socket.gethostname()+'-producer.json')
def identity(pid):
 try:
  p=pathlib.Path('/proc')/str(pid);f=(p/'stat').read_text().rsplit(')',1)[1].split();return {'pid':int(pid),'startticks':f[19],'state':f[0],'argv':(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
try:prior=read_json(rolepath)
except FileNotFoundError:prior=None
if prior:
 live=identity(prior['pid']);assert live and live['state']!='Z' and live['startticks']==prior['startticks'] and live['argv']==argv,'prior role is not same live producer; inspect'
 print(json.dumps(dict(prior,adopted=True)));raise SystemExit
found=[]
for p in pathlib.Path('/proc').iterdir():
 if p.name.isdigit():
  live=identity(p.name)
  if live and live['state']!='Z' and live['argv']==argv:found.append(live)
assert len(found)<=1,'duplicate overlap producers'
if found:
 result=found[0]
else:
 assert not(c/'overlap-bootstrap.json').exists(),'bootstrap attempted; explicit recovery required'
 assert not pathlib.Path('/tmp/'+cfg['run_id']+'-index.sqlite').exists()
 assert not any((c/'stages').iterdir())
 soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
 log=c/'overlap-producer.log'
 with open(log,'ab',buffering=0) as out:
  proc=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
 for _ in range(100):
  result=identity(proc.pid)
  if result and result['state']!='Z' and result['argv']==argv:break
  if proc.poll() is not None:raise RuntimeError('producer exited; inspect overlap-producer.log')
  time.sleep(.05)
 else:raise RuntimeError('producer identity not established; inspect before retry')
result.update(role='producer',host=socket.gethostname(),control=str(c),entrypoint='migration_overlap_resume.py',log=str(c/'overlap-producer.log'),read_workers=64,index_temp_store='memory',copy_threads_per_node=128,warm_stage='p3-r3',warm_manifest=str(c/'warm-tasks.jsonl'),module_sha256=hashlib.sha256(content).hexdigest())
write_json(rolepath,result);write_json(c/'pipeline-deployment.json',result);print(json.dumps(result))
'''.replace('CONTROL',repr(r['control'])).replace('DATA',repr(data))
v=json.loads(t.ssh('master-0',code)[1]);r['roles']['master-0:producer']=v;r['status']='copying_with_background_index';write_json(BASE/'overlap_resume_recovery.json',r);write_json(BASE/'new_cluster_recovery.json',r);print(json.dumps(v))
