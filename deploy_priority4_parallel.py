#!/usr/bin/env python3
"""Explicit concurrent P4 launch through current SSH aliases; no old-role edits.

prepare verifies all nodes before creating an isolated control. start requires
that exact control and never signals a process. It keeps the P1-3 active pointer
unchanged. A failed or uncertain launch is inspected by PID/start/argv, not
repeated blindly. Source code is frozen under the new shared control only.
"""
import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

from migration_io import read_json

ROOT = Path(__file__).resolve().parent
NODES = ['master-0'] + ['worker-' + str(i) for i in range(11)]


def ssh(node, code, timeout=55):
    result = subprocess.run(
        ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
         '-o', 'ForwardAgent=no', '-o', 'ForwardX11=no',
         '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2',
         node, 'python3 -'], input=code, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(node + ': ' + result.stderr[-3000:] + result.stdout[-3000:])
    return json.loads(result.stdout)


def sources():
    pending = ['migration_priority4_run', 'migration_priority4_worker',
               'migration_priority4_broker', 'migration_worker128_run', 'migration_metrics']
    found = set()
    while pending:
        name = pending.pop()
        if name in found:
            continue
        path = ROOT / (name + '.py')
        found.add(name)
        for node in ast.walk(ast.parse(path.read_text())):
            names = ([node.module] if isinstance(node, ast.ImportFrom) else
                     [alias.name for alias in node.names] if isinstance(node, ast.Import) else [])
            pending.extend(n for n in names if n and n.startswith('migration_') and
                           (ROOT / (n + '.py')).is_file())
    return {name + '.py': hashlib.sha256((ROOT / (name + '.py')).read_bytes()).hexdigest()
            for name in sorted(found)}


IDENTITY_CODE = '''
def identity(pid):
 p=Path('/proc')/str(pid)
 text=(p/'stat').read_text(); fields=text[text.rfind(')')+2:].split()
 argv=(p/'cmdline').read_bytes().rstrip(b'\\0').decode().split('\\0')
 if fields[0] in ('Z','X') or not argv: raise RuntimeError('role is not live')
 return dict(pid=pid,start_ticks=fields[19],argv=argv)
'''


def preflight(node, predecessor, job):
    code = '''import datetime,json,os,socket,subprocess,sys
from pathlib import Path
c=Path(PREDECESSOR);sys.path.insert(0,str(c))
from migration_io import read_json
IDENTITY
cfg=read_json(c/'config.json');state=read_json(c/'state.json')
host=socket.gethostname()
assert host=='job-'+JOB+'-'+NODE, ('wrong cluster/host',host)
assert cfg['job']==JOB and cfg['source']=='/share/project/eai_pwm' and cfg['destination']=='/share-mixed/project/eai_pwm'
assert not (c/'STOP').exists()
roles={}
for folder,lane in [('nodes','primary'),('nodes-extra','supplement')]:
 v=read_json(c/folder/(host+'.json'))
 age=datetime.datetime.now(datetime.timezone.utc).timestamp()-datetime.datetime.fromisoformat(v['updated']).timestamp()
 assert -5<=age<90 and not v.get('ended') and not v.get('thread_error'), (lane,'unhealthy')
 actual=identity(v['pid']);assert str(c) in actual['argv'], ('wrong worker control',actual)
 roles[lane]=actual
def mount(path):return subprocess.check_output(['findmnt','-T',path,'-rn','-o','TARGET,SOURCE,FSTYPE'],text=True).strip()
s=mount('/share/project');d=mount('/share-mixed/project')
assert s==cfg['source_mount'] and d==cfg['destination_mount']
assert not os.path.islink('/share/project') and not os.path.islink('/share-mixed/project')
assert os.stat('/share/project').st_dev != os.stat('/share-mixed/project').st_dev
protected=sorted(n for n in os.listdir('/share-mixed/project') if n.startswith('.hcr-copy-'))
top=sorted(os.listdir('/share/project'))
assert 'eai_pwm' in top and not set(top).intersection(protected), 'source/control name overlap'
for name in top:
 if name=='eai_pwm':continue
 dst=Path('/share-mixed/project')/name
 if dst.is_symlink():raise RuntimeError('P4 destination top-level symlink: '+name)
print(json.dumps(dict(node=NODE,host=host,job=JOB,roles=roles,source_mount=s,destination_mount=d,
 source_top=top,protected=protected,source_identity=[os.stat('/share/project').st_dev,os.stat('/share/project').st_ino],
 destination_identity=[os.stat('/share-mixed/project').st_dev,os.stat('/share-mixed/project').st_ino],
 predecessor_state={k:state.get(k) for k in ['status','stage','updated']})))
'''.replace('PREDECESSOR', repr(str(predecessor))).replace('IDENTITY', IDENTITY_CODE)
    code = code.replace('JOB', repr(job)).replace('NODE', repr(node))
    return ssh(node, code)


def prepare(args):
    if args.job != '2dadf47a-b549-47bb-a267-d56dc877aec9':
        raise ValueError('this explicit launch is bound to the verified 2dadf47a cluster')
    with ThreadPoolExecutor(max_workers=3) as pool:
        evidence = list(pool.map(lambda node: preflight(node, args.predecessor, args.job), NODES))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run_id = 'hcr-copy-p4-' + stamp + '-2dad-' + uuid.uuid4().hex[:6]
    control = '/share-mixed/project/.' + run_id
    hashes = sources()
    payload = dict(run_id=run_id, control=control, job=args.job, predecessor=str(args.predecessor),
                   repo=str(ROOT), hashes=hashes, evidence=evidence)
    code = '''import datetime,hashlib,json,os,secrets,shutil,socket,sys
from pathlib import Path
b=PAYLOAD;c=Path(b['control']);old=Path(b['predecessor']);sys.path.insert(0,str(old))
from migration_io import read_json,write_json
assert socket.gethostname()=='job-'+b['job']+'-master-0'
assert not c.exists() and not c.is_symlink()
oldcfg=read_json(old/'config.json');plan=read_json(old/'extension-plan.json')
assert oldcfg['job']==b['job'] and plan['exclude']==['eai_pwm']
assert (plan['source'],plan['destination'])==('/share/project','/share-mixed/project')
with socket.socket() as s:s.bind(('0.0.0.0',37654))
os.mkdir(c,0o700)
for name,digest in b['hashes'].items():
 src=Path(b['repo'])/name;dst=c/name
 assert hashlib.sha256(src.read_bytes()).hexdigest()==digest
 shutil.copyfile(src,dst);dst.chmod(0o700)
 assert hashlib.sha256(dst.read_bytes()).hexdigest()==digest
now=datetime.datetime.now(datetime.timezone.utc).isoformat()
protected=sorted(set().union(*(set(e['protected']) for e in b['evidence'])))
coverage=list(dict.fromkeys(plan.get('covered_by_controls',[])+[str(old)]))
cfg=dict(run_id=b['run_id'],control=str(c),job=b['job'],source='/share/project',destination='/share-mixed/project',
 migration_scope='project_priority4',exclude=['eai_pwm'],covered_by_controls=coverage,
 protected_destination_relatives=protected,parallel_with_control=str(old),resume_from=None,
 source_mount=b['evidence'][0]['source_mount'],destination_mount=b['evidence'][0]['destination_mount'],
 threads=64,supplement_threads=64,copy_threads_per_node=128,io_bytes_per_second=5000000000,
 supplement_io_bytes_per_second=5000000000,broker_fs_concurrency=64,poll_interval_seconds=3,
 broker_url='http://172.24.178.20:37654',broker_token=secrets.token_hex(32),training_watchdog=False,
 created=now,execution_mode='independent_concurrent_p4')
isolation=dict(version=1,mode='independent_concurrent_p4',control=str(c),run_id=cfg['run_id'],job=cfg['job'],
 source=cfg['source'],destination=cfg['destination'],exclude=['eai_pwm'],predecessor_control=str(old),
 existing_roles_unchanged=True,mount_root_metadata='preserve_existing',verified=True,
 user_authorization='允许先验证 P4 独立隔离，再并行搬运',verified_at=now,nodes=b['evidence'],
 root_namespace='independent P4 run_id, local databases, locks, broker port and P4-only stages',
 predecessor_acceptance_waived_for_concurrent_start=True,predecessor_final_acceptance_still_required=True)
for folder in ['nodes','nodes-extra','roles','logs','manifests']:(c/folder).mkdir(mode=0o700)
write_json(c/'config.json',cfg);write_json(c/'source-sha256.json',b['hashes']);write_json(c/'isolation.json',isolation)
write_json(c/'state.json',dict(status='starting',stage=None,run_id=cfg['run_id'],started=now,updated=now,phases={}))
write_json(c/'progress.json',dict(updated=now,phases={'4':dict(discovered_files=0,discovered_bytes=0,verified_files=0,verified_bytes=0,inventory_complete=False)},totals={}))
sys.path.insert(0,str(c))
from migration_priority4_run import validate_authorization
validate_authorization(c,cfg)
print(json.dumps(dict(control=str(c),run_id=cfg['run_id'],sources=len(b['hashes']),verified_nodes=len(b['evidence']),status='prepared_not_launched')))
'''.replace('PAYLOAD', repr(payload))
    print(json.dumps(ssh('master-0', code), ensure_ascii=False), flush=True)


def role(node, control, name):
    code = '''import fcntl,hashlib,json,os,socket,subprocess,sys,time
from pathlib import Path
c=Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
from migration_priority4_run import validate_authorization,ensure_nofile
from migration_run import validate_mounts
IDENTITY
cfg=read_json(c/'config.json');host=socket.gethostname();assert host=='job-'+cfg['job']+'-'+NODE
validate_authorization(c,cfg);validate_mounts(cfg);ensure_nofile()
assert not (c/'STOP').exists()
for filename,digest in read_json(c/'source-sha256.json').items():
 assert hashlib.sha256((c/filename).read_bytes()).hexdigest()==digest,filename
name=ROLE
if name=='broker':argv=['/usr/bin/python3','-u',str(c/'migration_priority4_broker.py'),'--control',str(c),'--port','37654']
elif name=='producer':argv=['/usr/bin/python3','-u',str(c/'migration_priority4_run.py'),'--control',str(c)]
elif name=='metrics':argv=['/usr/bin/python3','-u',str(c/'migration_metrics.py'),'--control',str(c)]
else:argv=['/usr/bin/python3','-u',str(c/'migration_priority4_worker.py'),name,'--control',str(c),'--threads','64']
record=c/'roles'/(host+'-'+name+'.json')
with open('/tmp/'+cfg['run_id']+'-launch-'+name+'.lock','a') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 try:previous=read_json(record)
 except FileNotFoundError:previous=None
 if previous:
  actual=identity(previous['pid'])
  assert actual['start_ticks']==previous['start_ticks'] and actual['argv']==argv, 'role record does not match a live process; inspect before recovery'
  print(json.dumps(dict(node=NODE,role=name,status='already_running',**actual)))
 else:
  # A prior SSH reply may have been lost after Popen but before publication.
  # Never start a second process merely because its role JSON is missing.
  for proc in Path('/proc').iterdir():
   if not proc.name.isdigit():continue
   try:other=(proc/'cmdline').read_bytes().rstrip(b'\\0').decode().split('\\0')
   except (OSError,UnicodeError):continue
   if other==argv:raise RuntimeError('unrecorded matching process exists; inspect PID '+proc.name)
  if name=='supplement':
   from migration_worker128_run import supplement_config
   supplement_config(c,require_primary=True)
  with open(c/'logs'/(host+'-'+name+'.log'),'ab',buffering=0) as output:
   child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,
    cwd='/tmp',start_new_session=True,close_fds=True)
  time.sleep(.3);assert child.poll() is None,'new role exited; inspect log before recovery'
  actual=identity(child.pid);assert actual['argv']==argv
  write_json(record,dict(node=NODE,role=name,started=time.time(),**actual))
  print(json.dumps(dict(node=NODE,role=name,status='launched',**actual)))
'''.replace('CONTROL', repr(str(control))).replace('IDENTITY', IDENTITY_CODE)
    return ssh(node, code.replace('NODE', repr(node)).replace('ROLE', repr(name)))


def register_parallel(control, before):
    cfg = read_json(Path(control) / 'config.json')
    with ThreadPoolExecutor(max_workers=3) as pool:
        after = list(pool.map(lambda node: preflight(node, Path(cfg['parallel_with_control']), cfg['job']), NODES))
    if {v['node']: v['roles'] for v in after} != before:
        raise RuntimeError('P1-3 identities changed during P4 launch; preserve evidence and inspect')
    payload = dict(control=str(control), predecessor=cfg['parallel_with_control'], evidence=after)
    code = '''import datetime,json,sys
from pathlib import Path
b=PAYLOAD;c=Path(b['control']);sys.path.insert(0,str(c))
from migration_io import read_json,write_json
from migration_nodes import load_nodes
cfg=read_json(c/'config.json');old=Path(b['predecessor'])
nodes=load_nodes(c);assert len(nodes)==12
assert all(len(n.get('components',[]))==2 and n['threads']==128 and not n.get('ended') and not n.get('thread_error') for n in nodes)
plan=read_json(old/'extension-plan.json');now=datetime.datetime.now(datetime.timezone.utc).isoformat()
if plan.get('parallel_execution'):
 assert plan['parallel_execution']['control']==str(c), 'another parallel P4 run is registered'
plan.setdefault('original_successor',plan.get('successor'))
plan.setdefault('original_depends_on',plan.get('depends_on'))
plan['parallel_execution']=dict(mode='independent_concurrent_p4',control=str(c),run_id=cfg['run_id'],
 user_authorization='允许先验证 P4 独立隔离，再并行搬运',isolation_record=str(c/'isolation.json'),
 started=now,status='running',predecessor_acceptance_still_required=True)
plan['successor']=dict(control=str(c),run_id=cfg['run_id'],state='running',phase=4,mode='independent_concurrent_p4')
plan['execution']['mode']='independent_concurrent_p4';plan['status']='running';plan['updated']=now
write_json(c/'launch-verification.json',dict(verified_at=now,predecessor_copy_identities_unchanged=True,
 nodes=b['evidence'],p4_nodes=12,p4_copy_processes=24,original_active_pointer_unchanged=True))
write_json(old/'extension-plan.json',plan)
print(json.dumps(dict(control=str(c),registered=True,predecessor_copy_identities_unchanged=True,p4_nodes=len(nodes))))
'''.replace('PAYLOAD', repr(payload))
    return ssh('master-0', code)


def start(args):
    control = str(args.control)
    cfg = read_json(Path(control) / 'config.json')
    if cfg['job'] != '2dadf47a-b549-47bb-a267-d56dc877aec9':
        raise ValueError('wrong cluster')
    isolation = read_json(Path(control) / 'isolation.json')
    expected = {item['node']: item['roles'] for item in isolation['nodes']}
    with ThreadPoolExecutor(max_workers=3) as pool:
        for value in pool.map(lambda node: preflight(node, Path(cfg['parallel_with_control']), cfg['job']), NODES):
            if value['roles'] != expected[value['node']]:
                raise RuntimeError('P1-3 process identities changed since isolation preflight')
    print(json.dumps(role('master-0', control, 'broker')), flush=True)
    # Authenticate the new port before starting any data consumers.
    code = '''import json,sys,urllib.request
from pathlib import Path
c=Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json
cfg=read_json(c/'config.json')
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
request=urllib.request.Request(cfg['broker_url']+'/health',headers={'Authorization':'Bearer '+cfg['broker_token']})
with opener.open(request,timeout=10) as r:value=json.load(r)
assert value['ok'];print(json.dumps(value))
'''.replace('CONTROL', repr(control))
    ssh('master-0', code)
    with ThreadPoolExecutor(max_workers=3) as pool:
        for value in pool.map(lambda node: role(node, control, 'primary'), NODES):
            print(json.dumps(value), flush=True)
    for attempt in range(12):
        from migration_nodes import load_nodes
        nodes = load_nodes(control)
        if len(nodes) == 12 and all(not n.get('ended') and not n.get('thread_error') for n in nodes):
            break
        time.sleep(1)
    else:
        raise RuntimeError('all12 P4 primary heartbeats not visible; inspect before continuing')
    with ThreadPoolExecutor(max_workers=3) as pool:
        for value in pool.map(lambda node: role(node, control, 'supplement'), NODES):
            print(json.dumps(value), flush=True)
    print(json.dumps(role('master-0', control, 'metrics')), flush=True)
    print(json.dumps(role('master-0', control, 'producer')), flush=True)
    print(json.dumps(register_parallel(control, expected)), flush=True)
    print(json.dumps(dict(control=control,status='launched',predecessor_untouched=cfg['parallel_with_control'])), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    prep = sub.add_parser('prepare')
    prep.add_argument('--predecessor', type=Path, required=True)
    prep.add_argument('--job', required=True)
    launch = sub.add_parser('start')
    launch.add_argument('--control', type=Path, required=True)
    args = parser.parse_args()
    (prepare if args.mode == 'prepare' else start)(args)


if __name__ == '__main__':
    main()
