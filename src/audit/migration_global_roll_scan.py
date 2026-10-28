#!/usr/bin/env python3
"""Drain and resume one metadata scanner, preserving its shared evidence."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import shutil

import migration_global_check as control


def bundle(root):
    path=root/'tools-resume-v1'
    if (root/'tools-resume-v1-sha256.json').exists():return path
    path.mkdir(exist_ok=False)
    shutil.copyfile(root/'tools/readonly_scan_core.py',path/'readonly_scan_core.py')
    shutil.copyfile(root/'tools/migration_reaudit_scan.py',path/'migration_reaudit_scan.py')
    for name in ('migration_global_scan.py','migration_global_resume.py','migration_reaudit_shared.py'):
        shutil.copyfile(Path(__file__).with_name(name),path/name)
    control.atomic(root/'tools-resume-v1-sha256.json',
        {f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in path.iterdir()})
    return path


REQUEST='''
import json,os,pathlib,signal,socket,time
root=pathlib.Path(ROOT);rec=REC
assert socket.gethostname()=='job-'+CLUSTER+'-'+rec['node']
folder=pathlib.Path(rec['report']).parent
assert folder==root/'nodes'/rec['node']/str(rec['lane'])
proc=pathlib.Path('/proc')/str(rec['pid'])
argv=[os.fsdecode(x) for x in (proc/'cmdline').read_bytes().split(b'\\0') if x]
assert argv==rec['argv'],'process identity changed'
fields=(proc/'stat').read_text().rsplit(') ',1)[1].split()
request=dict(old_pid=rec['pid'],old_start=fields[19],host=socket.gethostname(),argv=argv,at=time.time())
with (folder/'handoff-request-v1.json').open('x') as f:
 json.dump(request,f);f.flush();os.fsync(f.fileno())
assert (proc/'stat').read_text().rsplit(') ',1)[1].split()[19]==request['old_start']
os.kill(rec['pid'],signal.SIGTERM)
print(json.dumps(dict(status='drain_requested',request=request)))
'''

RESUME='''
import hashlib,json,os,pathlib,socket,subprocess,sys,time
root=pathlib.Path(ROOT);rec=REC;folder=pathlib.Path(rec['report']).parent
assert socket.gethostname()=='job-'+CLUSTER+'-'+rec['node']
assert folder==root/'nodes'/rec['node']/str(rec['lane'])
assert os.stat('/share/project').st_ino==69783039777
assert os.stat('/share-mixed/project').st_ino==3706574426
tools=root/'tools-resume-v1'
sys.path.insert(0,str(tools))
from migration_reaudit_shared import read
request=read(folder/'handoff-request-v1.json')
assert request['old_pid']==rec['pid'] and request['argv']==rec['argv']
try:state=(pathlib.Path('/proc')/str(request['old_pid'])/'stat').read_text().rsplit(') ',1)[1].split()
except FileNotFoundError:state=None
if state is not None and state[19]==request['old_start'] and state[0] not in ('Z','X'):
 print(json.dumps(dict(status='draining',pid=rec['pid'],process_state=state[0])));sys.exit(0)
if (folder/'resume-launched-v1.json').exists():
 response=read(folder/'resume-launched-v1.json')
 try:response['ready']=read(folder/'resume-ready-v1.json')
 except FileNotFoundError:pass
 print(json.dumps(response));sys.exit(0)
report=pathlib.Path(rec['report']);baseline=read(report/'result.json')
footer=json.loads((folder/'scan.log').read_text().splitlines()[-1])
assert baseline==footer and baseline['pid']==request['old_pid']
if baseline['status']=='complete':
 print(json.dumps(dict(status='completed_before_drain')));sys.exit(0)
assert baseline['status']=='stopped' and baseline['active_directories']==0
files={}
for name in ['frontier.sqlite','missing.jsonl','conflicts.jsonl','metadata.jsonl','errors.jsonl','source_links.jsonl']:
 s=(report/name).stat();files[name]=dict(ino=s.st_ino,size=s.st_size,mtime_ns=s.st_mtime_ns)
handoff=dict(request,baseline=baseline,files=files,closed_at=time.time())
hp=folder/'handoff-stopped-v1.json'
with hp.open('x') as f:
 json.dump(handoff,f);f.flush();os.fsync(f.fileno())
for name,digest in read(root/'tools-resume-v1-sha256.json').items():
 assert hashlib.sha256((tools/name).read_bytes()).hexdigest()==digest
argv=[sys.executable,'-S','-B','-u',str(tools/'migration_global_resume.py'),'--handoff',str(hp)]
with (folder/'scan.log').open('ab',buffering=0) as log:
 child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
  start_new_session=True,cwd=str(tools))
new=dict(rec,pid=child.pid,argv=argv,original_pid=rec['pid'],handoff=str(hp),resumed_at=time.time())
with (folder/'resume-launched-v1.json').open('x') as f:
 json.dump(dict(status='resume_launched',record=new),f);f.flush();os.fsync(f.fileno())
print(json.dumps(dict(status='resume_launched',record=new)))
'''


def run(root,node,lane,action):
    control.check_root(root)
    state=control.read(root/'launch-results.json')
    rec=next(r for r in state['records'] if (r['node'],r['lane'])==(node,lane))
    if action=='request':bundle(root)
    code=REQUEST if action=='request' else RESUME
    response=control.remote(node,'ROOT='+repr(str(root))+'\nREC='+repr(rec)+'\nCLUSTER='+repr(control.CLUSTER)+'\n'+code)
    if response['status']=='resume_launched':
        folder=Path(rec['report']).parent
        ready=response.get('ready')
        if ready is None:
            try:ready=control.read(folder/'resume-ready-v1.json')
            except FileNotFoundError:return dict(response,registration='awaiting_ready')
        new=response['record']
        if ready['pid']!=new['pid']:raise ValueError('ready process differs')
        with (root/'registry-update.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            state=control.read(root/'launch-results.json')
            prior=next(r for r in state['records'] if (r['node'],r['lane'])==(node,lane))
            if prior['pid']!=rec['pid']:raise ValueError('registry ownership changed')
            state['records']=[new if r is prior else r for r in state['records']]
            control.atomic(folder/'launch.json',new)
            control.atomic(root/'launch-results.json',state)
        response['registration']='committed'
    return response


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['request','resume']);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--node',choices=control.NODES,required=True);p.add_argument('--lane',type=int,choices=[0,1],required=True)
    a=p.parse_args();print(json.dumps(run(a.root,a.node,a.lane,a.action)),flush=True)
