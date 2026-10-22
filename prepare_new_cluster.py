#!/usr/bin/env python3
"""Prepare one fresh recovery namespace, preserving predecessor evidence."""
import base64
import json
import pathlib
import sys
from migration_io import read_json, write_json
import migration_launch as transport

BASE = pathlib.Path(__file__).resolve().parent

def main():
    record = read_json(BASE / 'new_cluster_recovery.json')
    transport.JOB, transport.OWNER = record['job'], record['ssh_owner']
    overrides = ['migration_resume_index.py', 'migration_fresh_resume.py',
                 'migration_cached_worker.py', 'migration_state_cache.py',
                 'migration_watchdog.py']
    # A role launcher can be added before any role is launched.
    if (BASE / 'migration_recovery_role.py').exists():
        overrides.append('migration_recovery_role.py')
    payload = {name: base64.b64encode((BASE / name).read_bytes()).decode() for name in overrides}
    data = {'record': record, 'files': payload}
    code = r'''import os,sys,json,pathlib,base64,fcntl,hashlib,socket,subprocess,secrets,datetime,copy
b=json.loads(PAYLOAD);r=b['record'];c=pathlib.Path(r['control']);old=pathlib.Path(r['old_control'])
sys.path.insert(0,str(old));from migration_io import read_json,write_json
lock=open('/tmp/'+r['run_id']+'-prepare.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert r['old_job_stopped_confirmed'] is True and (old/'STOP').exists()
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def mount(p):return subprocess.check_output(['findmnt','-T',p,'-rn','-o','TARGET,SOURCE,FSTYPE'],text=True).strip()
if not c.exists():c.mkdir(mode=0o700)
assert not c.is_symlink() and c.is_dir()
try:
 prior=read_json(c/'config.json')
 assert prior['run_id']==r['run_id'] and prior['resume_from']==str(old) and prior['job']==r['job']
except FileNotFoundError:prior=None
proofs=[]
producer=pathlib.Path(r['isolation']['producer_bundle']);broker=pathlib.Path(r['isolation']['broker_bundle'])
origins={p.name:p for p in producer.glob('*.py')}
for name in ('migration_broker.py','migration_broker_parallel.py'):origins[name]=broker/name
def install(name,data,origin):
 p=c/name
 if p.exists():
  if p.read_bytes()!=data:raise RuntimeError('immutable bundle mismatch: '+name)
 else:
  with open(p,'xb') as f:f.write(data)
  p.chmod(0o700)
 proofs.append({'name':name,'sha256':hashlib.sha256(data).hexdigest(),'origin':origin})
for name,p in origins.items():
 if name not in b['files']:install(name,p.read_bytes(),str(p))
for name,encoded in b['files'].items():install(name,base64.b64decode(encoded),'reviewed_local_recovery_bundle')
cfg=prior
if cfg is None:
 cfg=copy.deepcopy(read_json(old/'config.json'))
 cfg.update(run_id=r['run_id'],control=str(c),job=r['job'],ssh_owner=r['ssh_owner'],created=now(),
            source_mount=mount('/share/project'),destination_mount=mount('/share-mixed/project'),
            resume_from=str(old),threads=64,copy_threads_per_node=128,supplement_threads=64,
            io_bytes_per_second=10_000_000_000,supplement_io_bytes_per_second=10_000_000_000,
            parallel_scan_threads=64,scan_batch_files=64,scan_entries_per_second=0,
            broker_url='http://'+socket.gethostbyname(socket.gethostname())+':37651',
            broker_token=secrets.token_urlsafe(48),broker_fs_concurrency=64,
            predecessor_isolation={'confirmed':True,'old_control':str(old),'method':r['isolation_method']},
            require_priority4_plan=True)
 write_json(c/'config.json',cfg)
for directory in ('nodes','nodes-extra','stages','manifests','recovery-roles'): (c/directory).mkdir(exist_ok=True)
try:read_json(c/'state.json')
except FileNotFoundError:
 write_json(c/'state.json',{'status':'preparing_resume','stage':None,'started':now(),'run_id':r['run_id'],
                          'threads_per_node':128,'workers_expected':12,'resumed_from':str(old)})
plan=copy.deepcopy(read_json(old/'extension-plan.json'))
assert (plan['priority'],plan['source'],plan['destination'],plan['exclude'],plan['status'])==(4,'/share/project','/share-mixed/project',['eai_pwm'],'queued')
plan['dependency_predecessor']=plan['depends_on']['control'];plan['depends_on']['control']=str(c)
controls=[];p=old
while True:
 controls.append(str(p));a=read_json(p/'config.json').get('resume_from')
 if not a:break
 p=pathlib.Path(a)
plan['covered_by_controls']=list(dict.fromkeys([*reversed(controls),str(c)]))
plan['covered_by']={'priorities':[1,2,3],'controls':plan['covered_by_controls']};plan['updated']=now()
write_json(c/'extension-plan.json',plan)
deployment={'status':'prepared','created':now(),'host':socket.gethostname(),'control':str(c),
            'predecessor':str(old),'files':proofs,'isolation':cfg['predecessor_isolation']}
write_json(c/'recovery-deployment.json',deployment)
public={k:v for k,v in cfg.items() if k!='broker_token'}
print(json.dumps({'public_config':public,'deployment':deployment,'extension_plan':plan}))
'''.replace('PAYLOAD', repr(json.dumps(data)), 1)
    _, result = transport.ssh('master-0', code)
    result = json.loads(result)
    record.update(status='prepared', public_config=result['public_config'], deployment=result['deployment'])
    write_json(BASE/'new_cluster_recovery.json',record)
    write_json(BASE/'new_cluster_public_config.json',result['public_config'])
    write_json(BASE/'new_cluster_extension_plan.json',result['extension_plan'])
    print(json.dumps({'status':'prepared','control':record['control'],'modules':len(result['deployment']['files'])}))

if __name__=='__main__':main()
