"""Hand over only the producer; retain worker leases and running helpers."""
import base64
import datetime
import json
import pathlib
import uuid

import migration_launch as transport
from migration_io import read_json, write_json


def main():
    base = pathlib.Path(__file__).resolve().parent
    active = read_json(base / 'active_migration.json')
    transport.JOB = active['job']
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    names = ['migration_parallel_pipeline.py', 'migration_parallel_scan.py',
             'migration_pipeline_v2.py', 'migration_pipeline.py', 'migration_run.py',
             'migration_worker.py', 'migration_scan.py', 'migration_metadata.py', 'migration_io.py']
    payload = {'control': active['control'], 'bundle': 'parallel-pipeline-' + stamp + '-' + uuid.uuid4().hex[:6],
               'files': {n: base64.b64encode((base / n).read_bytes()).decode() for n in names}}
    code = r'''import base64,datetime,fcntl,hashlib,json,os,pathlib,signal,sqlite3,subprocess,sys,time,urllib.request
b=json.loads(PAYLOAD);c=pathlib.Path(b['control']);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
assert not (c/'STOP').exists(), 'STOP present: protect training'
cfg=read_json(c/'config.json');deployment=read_json(c/'pipeline-deployment.json')
old_script=str(pathlib.Path(deployment['bundle'])/'migration_pipeline.py')
def info(pid):
 q=pathlib.Path('/proc')/str(pid)
 try:
  fields=(q/'stat').read_text().split(') ')[1].split()
  return {'pid':int(pid),'starttime_ticks':fields[19],'state':fields[0],
          'argv':(q/'cmdline').read_bytes().decode().rstrip('\0').split('\0')}
 except (FileNotFoundError,ProcessLookupError):return None
old=info(deployment['pid'])
assert old and old_script in old['argv'] and 'run' in old['argv']
assert '--control' in old['argv'] and old['argv'][old['argv'].index('--control')+1]==str(c)
state=read_json(c/'state.json')
assert state['stage']=='p3-r1' and state['status']=='scanning' and not state['scan_complete']
bundle=c/b['bundle'];bundle.mkdir(mode=0o700);digests={}
for name,data in b['files'].items():
 raw=base64.b64decode(data);p=bundle/name
 with open(p,'xb') as f:f.write(raw)
 p.chmod(0o700);digests[name]=hashlib.sha256(raw).hexdigest()
subprocess.run([sys.executable,'-m','py_compile',*[str(bundle/n) for n in b['files']]],check=True)
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def request(method,body=None):
 raw=None if body is None else json.dumps(body).encode()
 req=urllib.request.Request(cfg['broker_url'].rstrip('/')+'/control',data=raw,method=method,
                           headers={'Authorization':'Bearer '+cfg['broker_token'],'Content-Type':'application/json'})
 with opener.open(req,timeout=30) as response:return json.load(response)
prior=request('GET')['draining_hosts'];assert not prior, 'another drain operation is active'
hosts=[]
for p in (c/'nodes').glob('*.json'):
 hosts.append(read_json(p)['host'])
assert len(hosts)==12 and len(set(hosts))==12
frozen=False;drained=False;terminated=False;newproc=None
try:
 # Drain is a broker-lock barrier: existing leases/done remain active, no new
 # claim insert can race the snapshot of task coverage below.
 drained=True
 request('POST',{'draining_hosts':hosts})
 latest=info(old['pid']);assert latest and latest['starttime_ticks']==old['starttime_ticks']
 os.kill(old['pid'],signal.SIGSTOP);frozen=True
 deadline=time.monotonic()+10
 while True:
  statuses=[]
  for p in (pathlib.Path('/proc')/str(old['pid'])/'task').iterdir():
   try:statuses.append((p/'stat').read_text().split(') ')[1].split()[0])
   except FileNotFoundError:pass
  if statuses and all(s in ('T','t') for s in statuses):break
  assert time.monotonic()<deadline, 'producer threads did not freeze'
  time.sleep(.05)
 state=read_json(c/'state.json')
 assert state['stage']=='p3-r1' and state['status']=='scanning' and not state['scan_complete']
 assert not (c/'STOP').exists(), 'STOP appeared during handover'
 helpers=[]
 for q in pathlib.Path('/proc').iterdir():
  if not q.name.isdigit():continue
  item=info(q.name)
  if not item or item['state']=='Z':continue
  a=item['argv']
  if old_script not in a or '--control' not in a or a[a.index('--control')+1]!=str(c):continue
  modes=[v for v in ('hardlinks','directories') if v in a]
  if not modes:continue
  assert len(modes)==1 and '--result' in a and '--manifest' in a
  item.update(kind=modes[0],result_path=a[a.index('--result')+1],
              manifest=a[a.index('--manifest')+1],stage=pathlib.Path(a[a.index('--manifest')+1]).stem)
  helpers.append(item)
 assert sum(v['kind']=='hardlinks' for v in helpers)<=1
 stage=state['stage'];root=c/'stages'/stage
 assigned=bundle/'assigned-tasks.jsonl'
 all_ids=set()
 with open(assigned,'x') as f:
  db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=10)
  try:
   for task_id,raw in db.execute('SELECT id,payload FROM claims WHERE stage=?',(stage,)):
    task=json.loads(raw);assert task['id']==task_id;all_ids.add(task_id)
    f.write(raw+'\n')
  finally:db.close()
  for shard in range(64):
   for p in (root/'pending'/str(shard)).glob('*.json'):
    task_id=int(p.stem)
    # Existing leases may recover/unlink stale pending paths during drain.
    # Their local SQLite payload is authoritative; never reopen that inode.
    if task_id in all_ids:continue
    try:task=read_json(p)
    except FileNotFoundError:
     reader=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=10)
     try:row=reader.execute('SELECT payload FROM claims WHERE stage=? AND id=?',(stage,task_id)).fetchone()
     finally:reader.close()
     assert row is not None, 'unclaimed pending task disappeared during stable handover'
     task=json.loads(row[0])
    assert task['id']==task_id;all_ids.add(task_id)
    f.write(json.dumps(task,ensure_ascii=True)+'\n')
 handover={'created':datetime.datetime.now(datetime.timezone.utc).isoformat(),
           'control':str(c),'stage':stage,'phase':3,'round':1,'bundle':str(bundle),
           'old_state':state,'old_progress':read_json(c/'progress.json'),
           'old_pipeline':read_json(c/'pipeline.json'),
           'old_manifest':str(c/'manifests'/(stage+'.jsonl')),
           'assigned_tasks_path':str(assigned),'assigned_task_count':len(all_ids),
           'next_task_id':max(all_ids,default=-1)+1,'live_helpers':helpers,
           'old_producer':old,'code_sha256':digests}
 handover_path=bundle/'handover.json';write_json(handover_path,handover)
 os.kill(old['pid'],signal.SIGTERM);os.kill(old['pid'],signal.SIGCONT);frozen=False
 deadline=time.monotonic()+20
 while True:
  item=info(old['pid'])
  if item is None or item['starttime_ticks']!=old['starttime_ticks'] or item['state']=='Z':break
  assert time.monotonic()<deadline, 'old producer did not exit'
  time.sleep(.1)
 terminated=True
 lock=open('/tmp/'+cfg['run_id']+'-coordinator.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 fcntl.flock(lock,fcntl.LOCK_UN);lock.close()
 cfg=read_json(c/'config.json')
 cfg.update(scan_entries_per_second=0,parallel_scan_threads=32,scan_batch_files=64)
 write_json(c/'config.json',cfg)
 log=c/('coordinator-'+b['bundle']+'.log')
 with open(log,'ab',buffering=0) as output:
  newproc=subprocess.Popen(['nice','-n','15','ionice','-c','3',sys.executable,'-u',
                           str(bundle/'migration_parallel_pipeline.py'),'run','--control',str(c),
                           '--handover',str(handover_path)],stdin=subprocess.DEVNULL,
                          stdout=output,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
 time.sleep(.5);assert newproc.poll() is None, 'new producer startup failed: '+str(log)
 result={'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'bundle':str(bundle),
         'pid':newproc.pid,'log':str(log),'old_pid':old['pid'],'control':str(c),
         'handover':str(handover_path),'helpers_adopted':helpers,
         'retained_tasks':len(all_ids),'next_task_id':handover['next_task_id']}
 write_json(c/'pipeline-deployment.json',result)
 print(json.dumps({k:v for k,v in result.items() if k!='helpers_adopted'}))
finally:
 if frozen and not terminated:
  try:
   item=info(old['pid'])
   if item and item['starttime_ticks']==old['starttime_ticks']:os.kill(old['pid'],signal.SIGCONT)
  except ProcessLookupError:pass
 if drained:request('POST',{'draining_hosts':prior})
'''.replace('PAYLOAD', repr(json.dumps(payload)), 1)
    result = json.loads(transport.ssh('master-0', code)[1])
    write_json(base / 'parallel_pipeline_deployment.json', result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
