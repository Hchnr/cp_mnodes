#!/usr/bin/env python3
"""Sequential host-only migration of twelve original file shards to workers 11–22.

No re-sharding, queue replay, thread changes, or copying primitive changes.
An ambiguous remote launch is never retried automatically.
"""
import argparse
from pathlib import Path
import shutil
import time

from migration_namespace_rollout import remote, spawn
from migration_reaudit_handoff_runtime import base, entry, exclusive, identity, validate_file_approval
from migration_reaudit_handoff import process_terminal
from migration_reaudit_shared import read

NODES=['master-0']+['worker-'+str(i) for i in range(11)]
PYTHON='/usr/bin/python3'


def prepare(control,namespace,source_bundle):
    control=Path(control).resolve();namespace=Path(namespace).resolve()
    cfg=read(control/'config.json');who=identity()
    if (cfg['job_kind']!='files' or cfg['execution_mode']!='local'
            or (who['host'],who['boot'])!=(cfg['execution_host'],cfg['execution_boot_id'])):
        raise ValueError('not original local file control')
    folder=control/'file-rollout-24-v1';folder.mkdir()
    mapping=[dict(control=str(control),node=n,target_node='worker-'+str(i+11)) for i,n in enumerate(NODES)]
    approval=dict(kind='explicit-24-node-host-migration-v1',
        directive='24节点已经可以连接，没问题的就把任务迁移到24个节点',
        original_user_message='24节点已经可以连接，你试试：ssh master-0 ... ssh worker-22，然后没问题的就把任务迁移到24 nodes上',
        allowed=mapping,sequential=True,created=time.time())
    validate_file_approval(approval,control,'master-0','worker-11')
    approval_path=folder/'user-approval.json';exclusive(approval_path,approval)
    bundle=control/'handoff-code-24-v1';bundle.mkdir()
    names=set(entry.CODE)|{'migration_reaudit_handoff.py','migration_reaudit_handoff_runtime.py','migration_reaudit_shared.py'}
    unchanged={'migration_worker.py','migration_linux_open.py','migration_parent_frontier.py','migration_reaudit_copy.py'}
    hashes={}
    for name in sorted(names):
        source=control/'code'/name if name in unchanged else Path(source_bundle)/name
        shutil.copyfile(source,bundle/name);hashes[name]=base.digest(bundle/name)
    for name in ('migration_file_rollout.py','migration_namespace_rollout.py','migration_file_handoff_postcheck.py'):
        shutil.copyfile(Path(source_bundle)/name,bundle/name)
    exclusive(bundle/'source-sha256.json',{p.name:base.digest(p) for p in bundle.glob('*.py')})
    actors={}
    for n in NODES:
        state=read(control/'nodes'/n/'progress.json')
        if state['host']!=who['host'] or process_terminal(state) or state['status']!='copying':
            raise ValueError('original file actor not live copying: '+n)
        actors[n]={k:state[k] for k in ('pid','start','host')}
    plan=dict(control=str(control),namespace=str(namespace),mapping=mapping,source_actors=actors,
        approval=str(approval_path),approval_sha256=base.digest(approval_path),
        config_sha256=base.digest(control/'config.json'),receiver_code=hashes,created=time.time())
    exclusive(folder/'plan.json',plan)
    return folder


def probe(node,control,cfg):
    code="""import json,os,socket;from pathlib import Path
actors=[]
for p in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=p.read_bytes().split(b'\\0')
  if any(a.startswith((CONTROL+'/').encode()) for a in args) and b'receiver' in args: actors.append(p.parent.name)
 except (FileNotFoundError,ProcessLookupError,PermissionError):pass
print(json.dumps(dict(host=socket.gethostname(),boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
source=os.stat('/share/project').st_ino,target=os.stat('/share-mixed/project').st_ino,
mounts=all(os.path.ismount(p) for p in ('/share/project','/share-mixed/project')),actors=actors)))"""
    result=remote(node,'CONTROL='+repr(str(control))+'\n'+code)
    if (result['host']!='job-'+cfg['cluster']+'-'+node or not result['mounts'] or result['actors']
            or any(result[k]!=cfg[k+'_root_ino'] for k in ('source','target'))):
        raise ValueError('target identity/mount/activity differs')
    return result


def start_remote(target,command,folder,role):
    bundle=folder.parents[1]/'handoff-code-24-v1'
    code=('import sys;sys.path.insert(0,'+repr(str(bundle))+');'
          'from migration_namespace_rollout import spawn;from pathlib import Path;import json;'
          'print(json.dumps(spawn('+repr(command)+',Path('+repr(str(folder))+'),'+repr(role)+')))')
    return remote(target,code)


def run(control):
    control=Path(control).resolve();folder=control/'file-rollout-24-v1';bundle=control/'handoff-code-24-v1'
    plan=read(folder/'plan.json');cfg=read(control/'config.json')
    if base.digest(control/'config.json')!=plan['config_sha256']:raise ValueError('configuration changed')
    exclusive(folder/'driver-instance.json',identity())
    done=[];node=target=None
    def state(status,**values):
        base.atomic(folder/'progress.json',dict(status=status,node=node,target_node=target,completed=done,
            expected=len(plan['mapping']),updated=time.time(),**values))
    try:
        # The old namespace coordinator explicitly freezes all file actor IDs.
        state('waiting_for_namespace_handoffs')
        while True:
            ns=read(Path(plan['namespace'])/'namespace-rollout-v2/progress.json')
            if ns['status']=='all_handed_off' and len(ns['completed'])==11:break
            if ns['status']=='inspection_required':raise ValueError('namespace rollout needs inspection; file actors untouched')
            state('waiting_for_namespace_handoffs');time.sleep(5)
        for item in plan['mapping']:
            node=item['node'];target=item['target_node'];state('validating_remote')
            receiver=probe(target,control,cfg)
            old=read(control/'nodes'/node/'progress.json');old_id={k:old[k] for k in ('pid','start')}
            if (any(old[k]!=plan['source_actors'][node][k] for k in ('host','pid','start'))
                    or process_terminal(old) or old['status']!='copying'):
                raise ValueError('original file actor changed; do not signal')
            handoff=control/'handoffs'/node;handoff.mkdir(parents=True)
            handoff_plan=dict(kind='single-file-shard-host-handoff-v1',control=str(control),node=node,target_node=target,
                config_sha256=plan['config_sha256'],shard_sha256=cfg['shards'][node]['sha256'],
                source_host=cfg['execution_host'],source_boot=cfg['execution_boot_id'],source_process=old_id,
                receiver_host=receiver['host'],receiver_boot=receiver['boot'],receiver_code=plan['receiver_code'],
                approval=plan['approval'],approval_sha256=plan['approval_sha256'],created=time.time())
            path=handoff/'plan.json';exclusive(path,handoff_plan)
            state('staging_receiver')
            start_remote(target,[PYTHON,'-S','-B','-u',str(bundle/'migration_reaudit_handoff_runtime.py'),
                                'receiver','--plan',str(path)],handoff,'receiver')
            deadline=time.monotonic()+900
            while True:
                try:ready=read(handoff/'receiver-state.json')
                except FileNotFoundError:ready={}
                if ready.get('status')=='ready_waiting_for_source_exit' and time.time()-ready['updated']<15:break
                if time.monotonic()>deadline:raise TimeoutError('receiver not ready; source untouched')
                time.sleep(2)
            state('draining_one_source')
            controller=spawn([PYTHON,'-S','-B','-u',str(bundle/'migration_reaudit_handoff_runtime.py'),
                              'drain','--plan',str(path)],handoff,'controller')
            deadline=time.monotonic()+1800
            while True:
                try:
                    closed=read(handoff/'source-closed.json');started=read(handoff/'receiver-copy-started.json')
                    if (not process_terminal(old_id) or not closed['actual_source_terminal']
                            or started['prefix']!=closed['prefix']):raise ValueError('source exit/prefix proof differs')
                    break
                except FileNotFoundError:pass
                if process_terminal(controller) and not (handoff/'source-closed.json').exists():
                    raise RuntimeError('controller exited without seal; inspect, never relaunch')
                if time.monotonic()>deadline:raise TimeoutError('handoff not proven; inspect')
                time.sleep(2)
            state('verifying_receiver')
            code=('import sys;sys.path.insert(0,'+repr(str(bundle))+');'
                  'from migration_reaudit_shared import read;from pathlib import Path;import json;'
                  'print(json.dumps(read(Path('+repr(str(control/'nodes'/node/'progress.json'))+'))))')
            deadline=time.monotonic()+600
            while True:
                current=remote(target,code)
                if (all(current[k]==started['identity'][k] for k in ('host','pid','start'))
                        and current['counts']['processed']>=closed['prefix']['prefix_tasks']):break
                if time.monotonic()>deadline:raise TimeoutError('counter continuity not proven')
                time.sleep(3)
            exclusive(handoff/'rollout-ack.json',dict(source_terminal=True,prefix=closed['prefix'],
                receiver_progress=current,target_node=target,updated=time.time()))
            # Audit after actual remote exit, never mistake a remote PID for a local one.
            start_remote(target,[PYTHON,'-S','-B','-u',str(bundle/'migration_file_handoff_postcheck.py'),
                                '--plan',str(path)],handoff,'postcheck')
            done.append(node);state('one_shard_handed_off')
        state('all_handed_off')
    except BaseException as error:
        state('inspection_required',error=repr(error));raise


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--control',type=Path,required=True)
    run(parser.parse_args().control)
