"""Replace only the drained legacy coordinator, preserving workers and receipts."""
import base64
import datetime
import json
import pathlib
import sys
import uuid

import migration_launch as transport
from migration_io import read_json, write_json


def main():
    base = pathlib.Path(__file__).resolve().parent
    cfg = read_json(base / 'active_migration.json')
    transport.JOB = cfg['job']
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    bundle_name = 'pipeline-' + stamp + '-' + uuid.uuid4().hex[:6]
    names = ['migration_pipeline.py', 'migration_metadata.py', 'migration_run.py',
             'migration_worker.py', 'migration_scan.py', 'migration_io.py']
    payload = {'control': cfg['control'], 'bundle': bundle_name,
               'files': {name: base64.b64encode((base / name).read_bytes()).decode()
                         for name in names}}
    code = r'''import base64,datetime,fcntl,hashlib,json,os,pathlib,signal,subprocess,sys,time
b=json.loads(PAYLOAD)
c=pathlib.Path(b['control']);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
from migration_run import queued_count
assert not (c/'STOP').exists(), 'STOP present; protect training'
s=read_json(c/'state.json');p=read_json(c/'progress.json')
assert s['status']=='finalizing' and s['stage']=='p1-r1' and s['scan_complete']
assert queued_count(c/'stages'/'p1-r1')==(0,0)
assert not (c/'manifests'/'p2-r1.jsonl').exists(), 'new stage already exists'
old=[]
for q in pathlib.Path('/proc').iterdir():
 if not q.name.isdigit():continue
 try:
  a=(q/'cmdline').read_bytes().decode().split('\0')
  if str(c/'migration_run.py') in a and 'coordinator' in a:
   assert '--control' in a and a[a.index('--control')+1]==str(c)
   start=(q/'stat').read_text().split(') ')[1].split()[19]
   old.append((int(q.name),start,a))
 except (FileNotFoundError,ProcessLookupError):pass
assert len(old)==1, 'expected one legacy coordinator'
bundle=c/b['bundle'];bundle.mkdir(mode=0o700)
digests={}
for name,data in b['files'].items():
 raw=base64.b64decode(data);path=bundle/name
 with open(path,'xb') as f:f.write(raw)
 path.chmod(0o700);digests[name]=hashlib.sha256(raw).hexdigest()
subprocess.run([sys.executable,'-m','py_compile',*[str(bundle/n) for n in b['files']]],check=True)
backup={'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'state':s,'progress':p,
        'old_pid':old[0][0],'old_starttime':old[0][1],'bundle':str(bundle),'code_sha256':digests}
write_json(bundle/'transition.json',backup)
pid,start,_=old[0]
q=pathlib.Path('/proc')/str(pid)
assert (q/'stat').read_text().split(') ')[1].split()[19]==start
s=read_json(c/'state.json')
assert s['status']=='finalizing' and s['stage']=='p1-r1' and s['scan_complete']
assert queued_count(c/'stages'/'p1-r1')==(0,0)
assert not (c/'manifests'/'p1-r2.jsonl').exists(), 'legacy coordinator advanced'
os.kill(pid,signal.SIGTERM)
def live():
 try:
  fields=(q/'stat').read_text().split(') ')[1].split()
  return fields[19]==start and fields[0]!='Z'
 except FileNotFoundError:return False
deadline=time.monotonic()+30
while live() and time.monotonic()<deadline:time.sleep(.2)
assert not live(), 'legacy coordinator has not exited; do not start another producer'
s=read_json(c/'state.json')
assert s['stage']=='p1-r1' and s['scan_complete']
assert queued_count(c/'stages'/'p1-r1')==(0,0)
assert not (c/'manifests'/'p1-r2.jsonl').exists(), 'legacy coordinator advanced at handover'
cfg=read_json(c/'config.json')
lock=open('/tmp/'+cfg['run_id']+'-coordinator.lock','a')
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
fcntl.flock(lock,fcntl.LOCK_UN);lock.close()
assert not (c/'STOP').exists(), 'STOP appeared during transition'
log=c/('coordinator-'+b['bundle']+'.log')
with open(log,'ab',buffering=0) as output:
 proc=subprocess.Popen(['nice','-n','15','ionice','-c','3',sys.executable,'-u',
                       str(bundle/'migration_pipeline.py'),'run','--control',str(c)],
                      stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,
                      start_new_session=True,close_fds=True)
time.sleep(.5)
assert proc.poll() is None, 'pipeline exited at startup: '+str(log)
deployment={'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'bundle':str(bundle),'pid':proc.pid,'log':str(log),'old_pid':pid,
            'workers_preserved':True,'broker_preserved':True,'control':str(c)}
write_json(c/'pipeline-deployment.json',deployment)
print(json.dumps(deployment))
'''.replace('PAYLOAD', repr(json.dumps(payload)), 1)
    result = json.loads(transport.ssh('master-0', code)[1])
    write_json(base / 'pipeline_deployment.json', result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
