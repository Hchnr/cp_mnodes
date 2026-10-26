#!/usr/bin/env python3
"""Explicit, user-authorized c3559 -> 2dad cold recovery; SSH aliases stay local."""
import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import uuid

from deploy_crash_cluster import unfinished_tasks
from migration_io import read_json, write_json
from migration_input_receipts import InputReceipts

JOB='2dadf47a-b549-47bb-a267-d56dc877aec9'
OLD=Path('/share-mixed/project/.hcr-copy-20261023T083418Z-c3559-4e81a5a7')
RECORD=Path('/share-mixed/project/.hcr-copy-monitor/recovery-2dad.json')
IPS={'master-0':'172.24.178.20','worker-0':'172.24.48.89','worker-1':'172.24.48.88',
     'worker-2':'172.24.219.69','worker-3':'172.24.219.70','worker-4':'172.24.102.224',
     'worker-5':'172.24.102.225','worker-6':'172.24.102.32','worker-7':'172.24.102.31',
     'worker-8':'172.24.13.230','worker-9':'172.24.13.231','worker-10':'172.24.6.134'}
MASTER='job-'+JOB+'-master-0'
OVERLAYS={'migration_cold_frontier.py','migration_hint_import.py','deploy_recovery_2dad.py',
    'migration_frontier_resume.py','migration_frontier_inputs.py','migration_input_receipts.py',
    'migration_missing_discovery.py','migration_task_publisher.py','migration_pipelined_target_filter.py',
    'migration_assigned_checkpoint.py','migration_overlap_parallel_filter.py','migration_recovery_role.py'}


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def inherited_baseline(work):
    """Flatten recorded ancestors once; never alter the new run's rate counters."""
    value=copy.deepcopy(work)
    for field in ('copied_files','copied_bytes'):
        cumulative='cumulative_'+field
        if cumulative in value:
            value['last_run_'+field]=value.get(field)
            value[field]=value[cumulative]  # Unknown ancestry must stay unknown.
    return value


def source_files(root):
    pending=['migration_cold_frontier','migration_hint_import','migration_missing_discovery','deploy_recovery_2dad',
             'migration_broker_sharded','migration_cached_worker','migration_probe_fast_open',
             'migration_metrics','migration_progress','migration_recovery_role',
             'migration_resume_index_parallel_memory','migration_worker128_run']
    found={}
    while pending:
        name=pending.pop()
        if name in found:continue
        filename=name+'.py'
        path=OLD/filename if filename not in OVERLAYS and (OLD/filename).is_file() else root/filename
        if not path.is_file():raise FileNotFoundError(str(path))
        found[name]=path
        for node in ast.walk(ast.parse(path.read_text())):
            names=[node.module] if isinstance(node,ast.ImportFrom) else [v.name for v in node.names] if isinstance(node,ast.Import) else []
            pending.extend(n for n in names if n and n.startswith(('migration_','deploy_')) and
                           ((OLD/(n+'.py')).is_file() or (root/(n+'.py')).is_file()))
    return {name+'.py':path for name,path in found.items()}


def prepare():
    if RECORD.exists():raise RuntimeError('recovery already prepared/attempted; inspect, do not replay')
    cfg=read_json(OLD/'config.json')
    if cfg.get('job')!='c3559adf-8c4e-4a14-afa4-b998ad8c0a1b':raise ValueError('wrong predecessor')
    if (cfg['source'],cfg['destination'])!=('/share/project/eai_pwm','/share-mixed/project/eai_pwm'):
        raise ValueError('wrong predecessor scope')
    now=datetime.datetime.now(datetime.timezone.utc)
    nodes=[read_json(p) for folder in ('nodes','nodes-extra') for p in (OLD/folder).glob('*.json')]
    if len(nodes)!=22 or min((now-datetime.datetime.fromisoformat(n['updated'])).total_seconds() for n in nodes)<180:
        raise RuntimeError('predecessor has recent/unknown writers')
    proof=dict(confirmed=True,old_control=str(OLD),recorded=now.isoformat(),
        method='user-confirmed-cluster-exit+stale-heartbeats+shared-STOP-fence',
        statement='用户确认旧集群已退出，按新12节点恢复复制',old_platform_deletion_independently_verified=False)
    try:fd=os.open(OLD/'STOP',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    except FileExistsError:
        if (OLD/'STOP').is_symlink() or not (OLD/'STOP').is_file():raise ValueError('unsafe old STOP')
    else:
        with os.fdopen(fd,'w') as f:json.dump(proof,f);f.flush();os.fsync(f.fileno())
    from migration_progress import snapshot,copy_work_progress
    baseline=inherited_baseline(copy_work_progress(snapshot(OLD)))
    run_id='hcr-copy-'+now.strftime('%Y%m%dT%H%M%SZ')+'-2dad-'+uuid.uuid4().hex[:8]
    control=OLD.parent/('.'+run_id);control.mkdir(mode=0o700)
    write_json(RECORD,dict(control=str(control),run_id=run_id,job=JOB,status='preparing',authorization=proof))
    for name,path in source_files(Path(__file__).resolve().parent).items():
        with open(control/name,'xb') as f:f.write(path.read_bytes());f.flush();os.fsync(f.fileno())
    hosts=['job-'+JOB+'-'+n for n in IPS]
    cfg.update(run_id=run_id,control=str(control),job=JOB,master_host=MASTER,copy_hosts=hosts,
        node_addresses=dict(zip(hosts,IPS.values())),created=now.isoformat(),resume_from=str(OLD),
        predecessor_isolation=proof,crash_recovery_v1=True,cold_frontier_v1=True,
        broker_url='http://'+IPS['master-0']+':37651',broker_token=secrets.token_hex(32),
        target_probe_endpoints=['http://'+ip+':40561' for ip in IPS.values()],
        metadata_client_processes=32,distributed_target_threads=2048,target_filter_inflight=4096,
        threads=64,supplement_threads=64,copy_threads_per_node=128,scan_batch_files=64,
        training_watchdog_enabled=False,warm_scan_missing=True)
    write_json(control/'config.json',cfg)
    for name in ('nodes','nodes-extra','stages','manifests','recovery-roles','probe-roles','feeds'):
        (control/name).mkdir(mode=0o700)
    for name in ('probe-policy.json','worker-directory-policy.json'):
        write_json(control/name,read_json(OLD/name))
    write_json(control/'state.json',dict(run_id=run_id,status='starting',stage=None,
        started=now.isoformat(),updated=now.isoformat(),workers_expected=12,threads_per_node=128,recovery_warm=True))
    write_json(control/'recovery-baseline.json',dict(control=str(control),run_id=run_id,
        resume_from=str(OLD),captured=now.isoformat(),work=baseline,
        scope='recorded-write-counters-and-probe-observations-not-unique-paths'))
    plan=copy.deepcopy(read_json(OLD/'extension-plan.json'))
    if plan.get('status')!='queued' or plan.get('exclude')!=['eai_pwm']:raise ValueError('unexpected P4 plan')
    plan['depends_on']['control']=str(control)
    plan['covered_by_controls']=list(dict.fromkeys(plan.get('covered_by_controls',[])+[str(OLD),str(control)]))
    plan['covered_by']={'priorities':[1,2,3],'controls':plan['covered_by_controls']}
    write_json(control/'extension-plan.json',plan)
    rows,summary=unfinished_tasks(OLD)
    with open(control/'warm-tasks.jsonl','x') as f:
        for row in rows:f.write(json.dumps(row,ensure_ascii=True)+'\n')
        f.flush();os.fsync(f.fileno())
    write_json(control/'warm-queue-import.json',summary)
    old_plan=read_json(OLD/'frontier-input/plan.json')
    if old_plan.get('control')!=str(OLD) or old_plan.get('run_id')!=read_json(OLD/'config.json')['run_id']:
        raise ValueError('old discovery plan differs')
    roots={h:[] for h in hosts}
    for i,row in enumerate(row for rows in old_plan['roots'].values() for row in rows):roots[hosts[i%12]].append(row)
    for h in hosts:
        for folder in ('ready','done'):(control/'frontier-input'/h/folder).mkdir(parents=True)
    write_json(control/'frontier-input/plan.json',dict(control=str(control),run_id=run_id,
        roots=roots,created=now.timestamp(),operation=uuid.uuid4().hex))
    # Scheduling acknowledgements only prioritize this optional fast feed;
    # they do not substitute for source discovery or data-copy receipts.
    ack=object.__new__(InputReceipts);ack.control=OLD;ack.run_id=read_json(OLD/'config.json')['run_id']
    ack.hosts=frozenset(read_json(OLD/'config.json')['copy_hosts'])
    deprioritized={h:[] for h in ack.hosts}
    for p in (OLD/'frontier-input/acknowledgements-v1/groups').glob('*.json'):
        rec=ack.read(p,'group')
        for path,count in ack._validate_group(rec):deprioritized[path.parent.parent.name].append(path.name)
    assignments={h:[] for h in hosts}
    for i,h in enumerate(sorted(ack.hosts)):assignments[hosts[i%12]].append(h)
    write_json(control/'hint-import-plan.json',dict(control=str(control),run_id=run_id,
        hosts=assignments,deprioritized=deprioritized,scope='fast-feed-only-full-discovery-and-scan-required'))
    write_json(control/'frontier-input-tuning.json',dict(control=str(control),run_id=run_id,
        max_files=1024,files_per_host=128,copy_batch_files=256))
    hashes={p.name:digest(p) for p in control.glob('*.py')}
    hashes.update({name:digest(control/name) for name in ('probe-policy.json','worker-directory-policy.json')})
    write_json(control/'source-sha256.json',hashes)
    result=dict(control=str(control),run_id=run_id,job=JOB,status='prepared',warm_queue=summary,authorization=proof)
    write_json(RECORD,result);return result


def verify(control):
    cfg=read_json(control/'config.json')
    if (cfg.get('job')!=JOB or set(cfg['copy_hosts'])!={'job-'+JOB+'-'+n for n in IPS}
            or cfg.get('resume_from')!=str(OLD) or not cfg.get('cold_frontier_v1')):raise ValueError('recovery identity differs')
    for name,expected in read_json(control/'source-sha256.json').items():
        if Path(name).name!=name or (control/name).is_symlink() or digest(control/name)!=expected:raise ValueError('source digest differs: '+name)
    if not (OLD/'STOP').is_file() or (control/'STOP').exists():raise RuntimeError('fence/STOP differs')
    return cfg


def launch_feed(control,kind):
    from migration_recovery_role import raise_nofile,process_info,wait_for_exec
    cfg=verify(control);host=socket.gethostname()
    if host not in cfg['copy_hosts']:raise ValueError('feed host excluded')
    from migration_run import validate_mounts
    validate_mounts(cfg);raise_nofile()
    script='migration_hint_import.py' if kind=='import' else 'migration_missing_discovery.py'
    argv=[sys.executable,'-u',str(control/script)]+([] if kind=='import' else ['run'])+['--control',str(control)]
    if kind!='import':argv+=['--threads','16']
    evidence=control/'feeds'/(host+'-'+kind+'.json')
    with open('/tmp/'+cfg['run_id']+'-launch-'+kind+'.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if evidence.exists():raise RuntimeError('feed already attempted; inspect instead of restarting')
        log=control/'feeds'/(host+'-'+kind+'.log')
        with open(log,'ab',buffering=0) as f:
            p=subprocess.Popen(['nice','-n','15','ionice','-c','3',*argv],stdin=subprocess.DEVNULL,
                stdout=f,stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
        info=wait_for_exec(p,argv)
        write_json(evidence,dict(info,control=str(control),run_id=cfg['run_id'],kind=kind,host=host))
        return info


def start(control):
    cfg=verify(control)
    if read_json(RECORD).get('control')!=str(control):raise ValueError('prepared control differs')
    if (control/'startup-attempt.json').exists():raise RuntimeError('startup already attempted; inspect first')
    write_json(control/'startup-attempt.json',{'control':str(control),'status':'starting'})
    def remote(node,roles=(),feeds=()):
        code=('from pathlib import Path;import sys,json;C=Path('+repr(str(control))+');sys.path.insert(0,str(C));'
              'from deploy_recovery_2dad import verify,launch_feed;verify(C);'
              'from migration_recovery_role import launch_role;'
              'print(json.dumps(dict(roles=[launch_role(C,r) for r in '+repr(list(roles))+'],feeds=[launch_feed(C,k) for k in '+repr(list(feeds))+'])))')
        r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8',node,'python3 -'],
            input=code,text=True,capture_output=True,timeout=100)
        if r.returncode:raise RuntimeError(node+' uncertain launch; inspect before retry: '+r.stderr[-1500:])
        return json.loads(r.stdout)
    outcomes={'master-services':remote('master-0',['broker','metrics'])}
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs={node:pool.submit(remote,node,['probe','primary','supplement'],['import','discovery']) for node in IPS}
        for node,f in jobs.items():
            outcomes[node]=f.result()
            write_json(control/'cluster-startup.json',{'status':'starting','outcomes':outcomes})
            print(json.dumps({'node':node,'roles':[r['role'] for r in outcomes[node]['roles']],
                'pids':[r['pid'] for r in outcomes[node]['roles']]}),flush=True)
    # Inspect actual authenticated listeners, not merely successful exec/PIDs.
    check=('from pathlib import Path;import sys,json,urllib.request,concurrent.futures;'
        'C=Path('+repr(str(control))+');sys.path.insert(0,str(C));from migration_io import read_json;'
        'cfg=read_json(C/"config.json");op=urllib.request.build_opener(urllib.request.ProxyHandler({}));'
        '\ndef health(url):\n'
        ' req=urllib.request.Request(url+"/health",headers={"Authorization":"Bearer "+cfg["broker_token"]})\n'
        ' with op.open(req,timeout=10) as f:v=json.load(f)\n'
        ' assert v.get("ok") is True and v.get("control")==str(C),url\n'
        ' return dict(endpoint=url,pid=v.get("pid"),control=v.get("control"))\n'
        'with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:rows=list(pool.map(health,cfg["target_probe_endpoints"]))\n'
        'print(json.dumps(rows))\n')
    health=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','master-0','python3 -'],
        input=check,text=True,capture_output=True,timeout=40)
    if health.returncode:raise RuntimeError('probe readiness uncertain; inspect, never replay startup: '+health.stderr[-1200:])
    write_json(control/'probe-startup-health.json',json.loads(health.stdout))
    outcomes['master-producer']=remote('master-0',['producer'])
    write_json(control/'cluster-startup.json',{'status':'launched','outcomes':outcomes})
    write_json('/share-mixed/project/.hcr-copy-monitor/active.json',dict(control=str(control),run_id=cfg['run_id'],job=JOB,ssh_owner='ybai'))
    return dict(control=str(control),status='launched',copy_processes=24,probe_processes=12)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=('prepare','start'))
    p.add_argument('--control',type=Path);a=p.parse_args()
    print(json.dumps(prepare() if a.action=='prepare' else start(a.control.resolve())),flush=True)


if __name__=='__main__':main()
