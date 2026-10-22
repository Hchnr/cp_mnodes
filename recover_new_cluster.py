#!/usr/bin/env python3
"""Prepare a new isolated namespace and start/adopt explicitly recorded roles."""
import argparse
import base64
import concurrent.futures
import json
import pathlib
import sys

import migration_launch as transport
from migration_io import read_json, write_json

BASE = pathlib.Path(__file__).resolve().parent
RECORD = BASE / 'new_cluster_recovery.json'


def context():
    record = read_json(RECORD)
    assert record['old_job_stopped_confirmed'] is True
    transport.JOB, transport.OWNER = record['job'], record['ssh_owner']
    return record


def remote(code, node='master-0'):
    return json.loads(transport.ssh(node, code)[1])


def prepare():
    r = context()
    names = ['migration_resume_index.py', 'migration_fresh_resume.py',
             'migration_state_cache.py', 'migration_cached_worker.py',
             'migration_watchdog.py', 'migration_recovery_role.py',
             'migration_progress.py']
    files = {name: base64.b64encode((BASE / name).read_bytes()).decode() for name in names}
    body = {'record': r, 'files': files}
    code = '''import base64,datetime,fcntl,hashlib,json,os,pathlib,secrets,socket,subprocess,sys,py_compile
b=json.loads(BODY);r=b['record'];c=pathlib.Path(r['control']);old=pathlib.Path(r['old_control'])
sys.path.insert(0,str(old));from migration_io import read_json,write_json
lock=open('/tmp/'+r['run_id']+'-prepare.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
assert (old/'STOP').exists() and r['old_job_stopped_confirmed']
assert not c.is_symlink()
if c.exists():
 if (c/'deployment.json').exists():
  dep=read_json(c/'deployment.json');assert dep['run_id']==r['run_id']
  print(json.dumps({'status':'already_prepared','deployment':dep}));raise SystemExit
 raise RuntimeError('partial control requires inspection, not blind prepare replay')
os.mkdir(c,0o700)
origins={}
producer=pathlib.Path(r['isolation']['producer_bundle']);broker=pathlib.Path(r['isolation']['broker_bundle'])
for name in r['isolation']['producer_py']:
 p=producer/name;data=p.read_bytes();(c/name).write_bytes(data);(c/name).chmod(0o700);origins[name]={'origin':str(p),'sha256':hashlib.sha256(data).hexdigest()}
for name in ('migration_broker.py','migration_broker_parallel.py'):
 p=broker/name;data=p.read_bytes();(c/name).write_bytes(data);(c/name).chmod(0o700);origins[name]={'origin':str(p),'sha256':hashlib.sha256(data).hexdigest()}
for name,encoded in b['files'].items():
 data=base64.b64decode(encoded);(c/name).write_bytes(data);(c/name).chmod(0o700);origins[name]={'origin':'reviewed_local_recovery_bundle','sha256':hashlib.sha256(data).hexdigest()}
for p in c.glob('*.py'):py_compile.compile(str(p),doraise=True)
cfg=read_json(old/'config.json')
stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
def mi(path):return subprocess.check_output(['findmnt','-T',path,'-rn','-o','TARGET,SOURCE,FSTYPE'],text=True).strip()
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.connect(('8.8.8.8',80));address=s.getsockname()[0];s.close()
cfg.update(run_id=r['run_id'],control=str(c),job=r['job'],ssh_owner=r['ssh_owner'],created=stamp,resume_from=str(old),
 source_mount=mi('/share/project'),destination_mount=mi('/share-mixed/project'),
 threads=64,copy_threads_per_node=128,supplement_threads=64,
 io_bytes_per_second=10_000_000_000,supplement_io_bytes_per_second=10_000_000_000,
 parallel_scan_threads=64,scan_entries_per_second=0,scan_batch_files=64,
 inventory_entries_per_second=100,broker_fs_concurrency=64,
 broker_url='http://'+address+':37651',broker_token=secrets.token_hex(32),
 require_priority4_plan=True,predecessor_isolation={
 'confirmed':True,'old_control':str(old),'method':r['isolation_method'],
 'statement':'旧任务已停止或删除','recorded_at':r['created']})
for directory in ('nodes','nodes-extra','manifests','stages','recovery-roles'):(c/directory).mkdir(mode=0o700)
write_json(c/'config.json',cfg)
write_json(c/'state.json',{'status':'preparing_resume','stage':None,'run_id':r['run_id'],'started':stamp,'updated':stamp,'workers_expected':12,'threads_per_node':128,'resumed_from':str(old),'preparation':'waiting_for_index_bootstrap'})
plan=read_json(old/'extension-plan.json');assert plan['priority']==4 and plan['exclude']==['eai_pwm'] and plan['status']=='queued'
prior_dependency=plan['depends_on']['control'];plan['depends_on']['control']=str(c);plan['dependency_predecessor']=prior_dependency
from migration_resume_index import _resume_controls
ancestors=[str(p) for p in reversed(_resume_controls(old))]
covered=list(dict.fromkeys(ancestors+[str(c)]));plan['covered_by']={'priorities':[1,2,3],'controls':covered};plan['covered_by_controls']=covered;plan['updated']=stamp
write_json(c/'extension-plan.json',plan)
dep={'run_id':r['run_id'],'control':str(c),'prepared':stamp,'job':r['job'],'ssh_owner':r['ssh_owner'],'master_host':socket.gethostname(),'broker_url':cfg['broker_url'],'origins':origins,'old_control':str(old),'old_stop':True,'predecessor_isolation':cfg['predecessor_isolation'],'priority4_successor':plan['successor']}
write_json(c/'deployment.json',dep)
print(json.dumps({'status':'prepared','deployment':dep}))
'''.replace('BODY', repr(json.dumps(body)), 1)
    result = remote(code)
    r.update(status='prepared', deployment=result['deployment'])
    write_json(RECORD, r)
    print(json.dumps({'status': result['status'], 'control': r['control'],
                      'modules': len(result['deployment']['origins'])}), flush=True)


def launch_role(node, role, index=None):
    r = context()
    args = ['--control', r['control'], '--role', role]
    if index is not None:
        args += ['--node-index', str(index)]
    code = '''import pathlib,runpy,sys
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));sys.argv=[str(c/'migration_recovery_role.py')]+ARGS
runpy.run_path(sys.argv[0],run_name='__main__')
'''.replace('CONTROL', repr(r['control'])).replace('ARGS', repr(args))
    return node, remote(code, node)


def roles(role, nodes):
    r = context()
    indexes = {n:i for i,n in enumerate(transport.HOSTS)}
    errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(launch_role, n, role, indexes[n] if role == 'inventory' else None): n for n in nodes}
        for future in concurrent.futures.as_completed(futures):
            node = futures[future]
            try:
                _, value = future.result()
                r['roles'].setdefault(role, {})[node] = value
                print(json.dumps({'role': role, 'node': node, 'result': value}), flush=True)
            except Exception as exc:
                errors.append({'node': node, 'error': str(exc)})
                print(json.dumps({'role': role, 'node': node, 'error': str(exc)}), flush=True)
            write_json(RECORD, r)
    if errors:
        raise RuntimeError('launch failures: ' + repr(errors))


def activate():
    r = context()
    code = '''import pathlib,sys,json
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c));from migration_io import read_json,write_json
assert not (c/'STOP').exists();cfg=read_json(c/'config.json')
assert cfg['run_id']==RUNID
public={k:v for k,v in cfg.items() if k!='broker_token'}
write_json('/share-mixed/project/.hcr-copy-monitor/active.json',{'control':str(c),'run_id':cfg['run_id'],'job':cfg['job'],'ssh_owner':cfg['ssh_owner']})
print(json.dumps({'public':public,'extension':read_json(c/'extension-plan.json')}))
'''.replace('CONTROL', repr(r['control'])).replace('RUNID', repr(r['run_id']))
    result = remote(code)
    write_json(BASE / 'active_migration.json', result['public'])
    write_json(BASE / 'extension-plan.json', result['extension'])
    r['status'] = 'active_restore'
    write_json(RECORD, r)
    print(json.dumps({'status': r['status'], 'control': r['control']}), flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=('prepare', 'role', 'activate'))
    p.add_argument('--role')
    p.add_argument('--nodes', nargs='*')
    args = p.parse_args()
    if args.action == 'prepare':
        prepare()
    elif args.action == 'activate':
        activate()
    else:
        if not args.role:
            p.error('role required')
        roles(args.role, args.nodes or transport.HOSTS)


if __name__ == '__main__':
    main()
