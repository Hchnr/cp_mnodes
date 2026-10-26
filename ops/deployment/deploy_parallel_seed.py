#!/usr/bin/env python3
"""A single seed-only handover; no payload worker, broker or lease is restarted."""
import base64
import datetime
import fcntl
import json
import pathlib
import uuid
import migration_launch as t
from migration_io import read_json,write_json

BASE=pathlib.Path(__file__).resolve().parent

def main():
    lock=open(BASE/'parallel-seed-deploy.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    r=read_json(BASE/'new_cluster_recovery.json');t.JOB=r['job'];t.OWNER=r['ssh_owner']
    try:d=read_json(BASE/'parallel_seed_recovery.json')
    except FileNotFoundError:
        identity=uuid.uuid4().hex
        d={'id':identity,'status':'planned','control':r['control'],
           'bundle':r['control']+'/parallel-seed-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+identity[:6],
           'old_producer':r['roles']['master-0:producer']}
        write_json(BASE/'parallel_seed_recovery.json',d)
    if d['status']!='planned':raise RuntimeError('deployment already attempted; inspect before any retry')
    files={n:base64.b64encode((BASE/n).read_bytes()).decode() for n in
           ('migration_seed_handover.py','migration_resume_index_parallel.py')}
    payload={'deployment':d,'files':files,'record':r}
    d['status']='deploying';write_json(BASE/'parallel_seed_recovery.json',d)
    code=r'''import sys,os,pathlib,json,base64,fcntl,hashlib,sqlite3,signal,time,subprocess,resource,socket,datetime
b=json.loads(PAYLOAD);d=b['deployment'];r=b['record'];c=pathlib.Path(d['control']);bundle=pathlib.Path(d['bundle']);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
cfg=read_json(c/'config.json');lock=open('/tmp/'+cfg['run_id']+'-parallel-seed-dispatch.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert not(c/'STOP').exists()
bundle.mkdir(mode=0o700,exist_ok=True)
proof=[]
for name,encoded in b['files'].items():
 data=base64.b64decode(encoded);p=bundle/name
 if p.exists():assert p.read_bytes()==data,'immutable module mismatch'
 else:
  with open(p,'xb') as f:f.write(data)
  p.chmod(0o700)
 proof.append({'name':name,'sha256':hashlib.sha256(data).hexdigest()})
subprocess.run([sys.executable,str(bundle/'migration_seed_handover.py'),'--help'],
               env=dict(os.environ,PYTHONPATH=str(c)),check=True,stdout=subprocess.DEVNULL)
old=d['old_producer'];pid=int(old['pid']);pr=pathlib.Path('/proc')/str(pid)
def identity():
 try:
  fields=(pr/'stat').read_text().rsplit(')',1)[1].split()
  return {'pid':pid,'starttime_ticks':fields[19],'state':fields[0],
          'argv':(pr/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')}
 except OSError:return None
actual=identity();assert actual and actual['starttime_ticks']==old['startticks'] and actual['argv']==old['argv']
before=read_json(c/'state.json');assert before['status']=='preparing_resume' and before.get('stage') is None
index=pathlib.Path('/tmp')/(cfg['run_id']+'-index.sqlite')
assert not any(pathlib.Path(str(index)+s).exists() for s in ('','-wal','-shm'))
frozen=False;terminated=False
try:
 os.kill(pid,signal.SIGSTOP);frozen=True
 for _ in range(200):
  actual=identity()
  if actual and actual['state'] in ('T','t'):break
  time.sleep(.05)
 assert actual and actual['state'] in ('T','t'),'producer did not freeze'
 assert actual['starttime_ticks']==old['startticks'] and actual['argv']==old['argv']
 s=read_json(c/'state.json');fresh=read_json(c/'fresh-bootstrap.json')
 assert s['status']=='preparing_resume' and s.get('stage') is None and fresh['status']=='seeding'
 assert not(c/'STOP').exists() and not index.exists()
 for name in ('stages','manifests'):
  assert not any((c/name).iterdir()),'payload namespace no longer empty'
 db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True)
 assert db.execute('SELECT count(*) FROM claims').fetchone()[0]==0,'broker has payload claims';db.close()
 children=(pr/'task'/str(pid)/'children').read_text().strip();assert not children,'producer already has children'
 evidence={'version':1,'purpose':'resume_index_seed_only','id':d['id'],'control':str(c),'run_id':cfg['run_id'],
           'old_producer':{k:actual[k] for k in ('pid','starttime_ticks','argv')},
           'state_snapshot':s,'fresh_bootstrap_snapshot':fresh,
           'created':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':proof}
 handover=bundle/'seed-handover.json'
 if handover.exists():raise RuntimeError('seed handover evidence already exists; inspect')
 write_json(handover,evidence)
 os.kill(pid,signal.SIGTERM);os.kill(pid,signal.SIGCONT);frozen=False;terminated=True
 for _ in range(400):
  live=identity()
  if live is None or live['starttime_ticks']!=old['startticks'] or live['state']=='Z':break
  time.sleep(.05)
 else:raise RuntimeError('old producer has not exited')
 coord=open('/tmp/'+cfg['run_id']+'-coordinator.lock','a')
 for _ in range(200):
  try:fcntl.flock(coord,fcntl.LOCK_EX|fcntl.LOCK_NB);break
  except BlockingIOError:time.sleep(.05)
 else:raise RuntimeError('old coordinator lock not released')
 fcntl.flock(coord,fcntl.LOCK_UN);coord.close()
 soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE);resource.setrlimit(resource.RLIMIT_NOFILE,(max(soft,65536),hard))
 argv=[sys.executable,'-u',str(bundle/'migration_seed_handover.py'),'run','--control',str(c),'--handover',str(handover)]
 log=c/'parallel-seed-producer.log'
 with open(log,'ab',buffering=0) as f:
  proc=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True,env=dict(os.environ,PYTHONPATH=str(c)))
 p=pathlib.Path('/proc')/str(proc.pid)
 for _ in range(100):
  try:
   cmd=(p/'cmdline').read_bytes().rstrip(b'\0').decode().split('\0');fields=(p/'stat').read_text().rsplit(')',1)[1].split()
   if cmd==argv and fields[0]!='Z':break
  except OSError:pass
  time.sleep(.05)
 else:raise RuntimeError('new producer startup failed; inspect log')
 role={'pid':proc.pid,'startticks':fields[19],'argv':argv,'host':socket.gethostname(),'role':'producer','control':str(c),
       'log':str(log),'bundle':str(bundle),'entrypoint':'migration_seed_handover.py','handover':str(handover),
       'previous_serial_producer':old,'published_index_preserved':False,'read_workers':64}
 write_json(c/'recovery-roles'/(socket.gethostname()+'-producer.json'),role)
 write_json(c/'pipeline-deployment.json',role)
 temps=[{'path':str(x),'bytes':x.stat().st_size} for x in pathlib.Path('/tmp').glob('.'+cfg['run_id']+'-index.sqlite.*.tmp*')]
 deployment=dict(d,status='launched',role=role,old_temp_indexes_preserved=temps,files=proof)
 write_json(c/'parallel-seed-deployment.json',deployment)
 print(json.dumps(deployment))
finally:
 if frozen and not terminated:
  live=identity()
  if live and live['starttime_ticks']==old['startticks']:os.kill(pid,signal.SIGCONT)
'''.replace('PAYLOAD',repr(json.dumps(payload)),1)
    try:
        result=json.loads(t.ssh('master-0',code)[1])
    except BaseException as exc:
        d['status']='attention';d['error']=str(exc);write_json(BASE/'parallel_seed_recovery.json',d);raise
    write_json(BASE/'parallel_seed_recovery.json',result)
    r=read_json(BASE/'new_cluster_recovery.json');r['roles']['master-0:producer']=result['role'];r['parallel_seed']=result
    write_json(BASE/'new_cluster_recovery.json',r)
    print(json.dumps({'status':result['status'],'pid':result['role']['pid'],'bundle':result['bundle'],'old_temp_indexes_preserved':len(result['old_temp_indexes_preserved'])}))

if __name__=='__main__':main()
