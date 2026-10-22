import sys,pathlib,json
from migration_io import read_json,write_json
import migration_launch as t
BASE=pathlib.Path(__file__).parent;r=read_json(BASE/'overlap_resume_recovery.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
code=r'''import sys
sys.pycache_prefix='/tmp/OVERLAP_CACHE'
import pathlib,json,os,fcntl,socket,signal,time,resource,subprocess
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
cfg=read_json(c/'config.json');assert not(c/'STOP').exists()
repair=open('/tmp/'+cfg['run_id']+'-extra-bootstrap-repair.lock','a');fcntl.flock(repair,fcntl.LOCK_EX|fcntl.LOCK_NB)
host=socket.gethostname();rolepath=c/'recovery-roles'/(host+'-supplement.json')
def ident(pid):
 try:
  p=pathlib.Path('/proc')/str(pid);f=(p/'stat').read_text().rsplit(')',1)[1].split();return {'pid':int(pid),'startticks':f[19],'state':f[0],'argv':(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
try:
 previous=read_json(rolepath);live=ident(previous['pid'])
except FileNotFoundError:previous=live=None
if live and live['state']!='Z' and live['startticks']==previous['startticks']:
 print(json.dumps({'already_live':previous}));raise SystemExit
# Prove no actual data-lane worker owns the node-local exclusion lock.
lane=open('/tmp/'+cfg['run_id']+'-extra64-worker.lock','a');fcntl.flock(lane,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(lane,fcntl.LOCK_UN);lane.close()
old=ident(159173);proof={'old_launcher':old,'no_active_extra_lane':True,'source_code_unchanged':True}
if old and old['state']!='Z':
 p=pathlib.Path('/proc')/'159173';assert old['startticks']=='738688118' and old['argv']==['python3','-']
 assert not(p/'task'/'159173'/'children').read_text().strip()
 fds={x.name:os.readlink(x) for x in(p/'fd').iterdir()};assert fds.get('3','').endswith(cfg['run_id']+'-recovery-launch-supplement.lock') and '__pycache__/migration_run.' in fds.get('4','')
 status=(p/'status').read_text();term_bit=1<<(signal.SIGTERM-1)
 vals={line.split(':')[0]:line.split(':')[1].strip() for line in status.splitlines() if ':' in line}
 assert not(int(vals['SigCgt'],16)&term_bit) and not(int(vals['SigIgn'],16)&term_bit)
 # This is an empty bootstrap process, not a data worker. If NFS keeps it
 # in D state, the fatal default signal prevents it resuming Python launch code.
 os.kill(159173,signal.SIGTERM);time.sleep(.2);after=ident(159173)
 proof.update(old_launcher_fds=fds,old_launcher_after=after,signal='SIGTERM to empty launcher only')
 if after and after['state']!='Z':
  vals={line.split(':')[0]:line.split(':')[1].strip() for line in(p/'status').read_text().splitlines() if ':' in line}
  assert (int(vals['SigPnd'],16)|int(vals['ShdPnd'],16))&term_bit,'fatal termination not pending'
write_json(c/'worker10-bootstrap-repair.json',proof)
# Private bytecode cache avoids the shared NFS .pyc inode which stalled launch.
prefix='/tmp/'+cfg['run_id']+'-worker10-pycache'
argv=[sys.executable,'-X','pycache_prefix='+prefix,'-u',str(c/'migration_cached_worker.py'),'supplement','--control',str(c),'--threads','64','--nice','0']
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
log=c/('supplement-privatecache-'+host+'.log')
with open(log,'ab',buffering=0) as out:
 proc=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
for _ in range(100):
 actual=ident(proc.pid)
 if actual and actual['state']!='Z' and actual['argv']==argv:break
 if proc.poll() is not None:raise RuntimeError('supplement startup failed; inspect privatecache log')
 time.sleep(.05)
else:raise RuntimeError('supplement launch identity uncertain; inspect exact PID, do not replay')
actual.update(role='supplement',host=host,control=str(c),log=str(log),private_pycache=prefix,repair_evidence=str(c/'worker10-bootstrap-repair.json'))
write_json(rolepath,actual);print(json.dumps(actual))
'''.replace('CONTROL',repr(r['control'])).replace('OVERLAP_CACHE',r['run_id']+'-repair-controller-pycache')
v=json.loads(t.ssh('worker-10',code)[1]);write_json(BASE/'overlap_worker10_repair_result.json',v)
r=read_json(BASE/'overlap_resume_recovery.json');r['roles']['worker-10:supplement']=v.get('already_live',v);write_json(BASE/'overlap_resume_recovery.json',r);write_json(BASE/'new_cluster_recovery.json',r);print(v)
