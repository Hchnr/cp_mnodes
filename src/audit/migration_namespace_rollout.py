#!/usr/bin/env python3
"""Sequential, explicitly approved namespace-only host handoff coordinator.

Never retries an ambiguous spawn or restarts an actor. All durable records live
on the shared control volume; ordinary file workers are identity-checked only.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import time

import migration_manifest_copy as base
from migration_reaudit_handoff import process_terminal
from migration_reaudit_handoff_runtime import exclusive, identity, validate_approval
from migration_reaudit_shared import read

NODES = ['master-0'] + ['worker-' + str(i) for i in range(1, 11)]
PYTHON = '/usr/bin/python3'


def remote(node, code, timeout=40):
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=7',
                             node, shlex.join([PYTHON, '-S', '-B', '-c', code])],
                            capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('SSH '+node+': '+result.stderr[-2000:])
    return json.loads(result.stdout)


def file_identities(control):
    cfg=read(control/'config.json')
    result={}
    for node in cfg['shards']:
        progress=read(control/'nodes'/node/'progress.json')
        result[node]={k:progress[k] for k in ('pid','start','host')}
    return result


def assert_files_unchanged(control, expected):
    if file_identities(control)!=expected:
        raise ValueError('ordinary file actor identities changed; inspect, do not restart')


def spawn(command, folder, role):
    exclusive(folder/(role+'-spawn-intent.json'),dict(command=command,created=time.time()))
    with open(folder/(role+'.log'),'ab',buffering=0) as log:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=log,
                               start_new_session=True,cwd='/')
    start=Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()[19]
    record=dict(pid=child.pid,start=start,command=command,created=time.time())
    exclusive(folder/(role+'-spawn.json'),record)
    return record


def prepare(control, files, root):
    folder=control/'namespace-rollout-v2'
    folder.mkdir()  # Existing run must be inspected, never overwritten/relaunched.
    cfg=read(control/'config.json')
    if cfg['job_kind']!='namespace' or cfg['execution_mode']!='local':
        raise ValueError('not original local namespace control')
    if (cfg['execution_host'],cfg['execution_boot_id'])!=(identity()['host'],identity()['boot']):
        raise ValueError('not the original source environment')
    approval=dict(answer='允许，逐个交接剩余目录分片',sequential=True,
        allowed=[dict(control=str(control),node=n) for n in NODES],
        unchanged='ordinary file actors, thread counts and copying primitives',created=time.time())
    validate_approval(approval,control,NODES[0])
    approval_path=folder/'user-approval.json';exclusive(approval_path,approval)
    pilot=read(control/'handoffs/worker-0/plan.json')
    bundle=control/'handoff-code-v2';bundle.mkdir()
    repo=Path(__file__).resolve().parent
    hashes={}
    for name in pilot['receiver_code']:
        source=(control/'code'/name if name in (
            'migration_worker.py','migration_linux_open.py','migration_parent_frontier.py',
            'migration_reaudit_copy.py') else repo/name)
        shutil.copyfile(source,bundle/name);hashes[name]=base.digest(bundle/name)
    shutil.copyfile(__file__,bundle/Path(__file__).name)
    exclusive(bundle/'source-sha256.json',hashes)
    plan=dict(control=str(control),files_control=str(files),audit_root=str(root),nodes=NODES,
        approval=str(approval_path),approval_sha256=base.digest(approval_path),receiver_code=hashes,
        file_actors=file_identities(files),created=time.time())
    exclusive(folder/'plan.json',plan)
    exclusive(root/'final/namespace-handoff-rollout.json',dict(control=str(control),folder=str(folder)))
    return folder


def run(folder):
    plan=read(folder/'plan.json');control=Path(plan['control']);files=Path(plan['files_control'])
    cfg=read(control/'config.json');bundle=control/'handoff-code-v2'
    exclusive(folder/'driver-instance.json',identity())
    done=[];node=None
    def state(status,**kw):
        base.atomic(folder/'progress.json',dict(status=status,node=node,completed=done,
                    expected=len(NODES),updated=time.time(),**kw))
    try:
        for node in plan['nodes']:
            assert_files_unchanged(files,plan['file_actors'])
            state('validating_remote')
            code="""import json,os,socket;from pathlib import Path
print(json.dumps(dict(host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
source=os.stat('/share/project').st_ino,target=os.stat('/share-mixed/project').st_ino,
mounts=all(os.path.ismount(p) for p in ('/share/project','/share-mixed/project')))))"""
            target=remote(node,code)
            if (target['host']!='job-'+cfg['cluster']+'-'+node or not target['mounts']
                    or target['source']!=cfg['source_root_ino'] or target['target']!=cfg['target_root_ino']):
                raise ValueError('receiver host or mounts differ')
            old=read(control/'nodes'/node/'progress.json')
            old_id={k:old[k] for k in ('pid','start')}
            if old['status']!='copying' or old['host']!=cfg['execution_host'] or process_terminal(old_id):
                raise ValueError('original namespace actor not live copying; inspect instead')
            handoff=control/'handoffs'/node;handoff.mkdir()
            item=dict(kind='single-namespace-shard-host-handoff-v1',control=str(control),node=node,
                config_sha256=base.digest(control/'config.json'),shard_sha256=cfg['shards'][node]['sha256'],
                source_host=cfg['execution_host'],source_boot=cfg['execution_boot_id'],source_process=old_id,
                receiver_host=target['host'],receiver_boot=target['boot'],receiver_code=plan['receiver_code'],
                approval=plan['approval'],approval_sha256=plan['approval_sha256'],created=time.time())
            path=handoff/'plan.json';exclusive(path,item)
            command=[PYTHON,'-S','-B','-u',str(bundle/'migration_reaudit_handoff_runtime.py'),
                     'receiver','--plan',str(path)]
            state('staging_receiver')
            # Remote helper is detached before returning. A lost SSH response is
            # ambiguous: leave source untouched and stop for inspection, never retry.
            remote_code=("import sys;sys.path.insert(0,"+repr(str(bundle))+");"
                "from migration_namespace_rollout import spawn;from pathlib import Path;import json;"
                "print(json.dumps(spawn("+repr(command)+",Path("+repr(str(handoff))+"),'receiver')))" )
            remote(node,remote_code)
            deadline=time.monotonic()+600
            while True:
                try:ready=read(handoff/'receiver-state.json')
                except FileNotFoundError:ready={}
                if ready.get('status')=='ready_waiting_for_source_exit' and time.time()-ready['updated']<15:
                    break
                if time.monotonic()>deadline:raise TimeoutError('staged receiver not ready; source untouched')
                time.sleep(2)
            state('draining_one_source')
            controller=spawn([PYTHON,'-S','-B','-u',str(bundle/'migration_reaudit_handoff_runtime.py'),
                              'drain','--plan',str(path)],handoff,'controller')
            deadline=time.monotonic()+1800
            while True:
                try:
                    closed=read(handoff/'source-closed.json')
                    started=read(handoff/'receiver-copy-started.json')
                    if (not process_terminal(old_id) or not closed['actual_source_terminal']
                            or started['prefix']!=closed['prefix']):
                        raise ValueError('source-exit / prefix proof inconsistent')
                    break
                except FileNotFoundError:pass
                if process_terminal(controller):
                    try:read(handoff/'source-closed.json')
                    except FileNotFoundError:raise RuntimeError('controller exited without seal; inspect logs')
                if time.monotonic()>deadline:raise TimeoutError('handoff not proven; inspect, never restart')
                time.sleep(2)
            state('verifying_receiver')
            probe=("import sys;sys.path.insert(0,"+repr(str(bundle))+");"
                "from migration_reaudit_shared import read;from pathlib import Path;import json;"
                "print(json.dumps(read(Path("+repr(str(control/'nodes'/node/'progress.json'))+"))))")
            deadline=time.monotonic()+600
            while True:
                current=remote(node,probe)
                if (current['host']==target['host'] and current['pid']==started['identity']['pid']
                        and current['start']==started['identity']['start']
                        and current['counts']['processed']>=closed['prefix']['prefix_tasks']):
                    break
                if time.monotonic()>deadline:raise TimeoutError('receiver counter continuity not proven')
                time.sleep(3)
            assert_files_unchanged(files,plan['file_actors'])
            exclusive(handoff/'rollout-ack.json',dict(source_terminal=True,prefix=closed['prefix'],
                receiver_progress=current,file_actors_unchanged=True,updated=time.time()))
            done.append(node);state('one_shard_handed_off')
        state('all_handed_off')
    except BaseException as error:
        state('inspection_required',error=repr(error))
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role',choices=('prepare','run'));parser.add_argument('--control',type=Path,required=True)
    parser.add_argument('--files',type=Path);parser.add_argument('--audit-root',type=Path)
    args=parser.parse_args()
    if args.role=='prepare':print(prepare(args.control,args.files,args.audit_root))
    else:run(args.control/'namespace-rollout-v2')
