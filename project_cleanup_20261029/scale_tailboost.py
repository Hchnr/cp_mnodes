#!/usr/bin/env python3
"""Read-only scale preparation; apply is an explicit operator stop/start action."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import uuid

import boost
import rm24
import tailboost

HERE = Path(__file__).resolve().parent
POINTER = HERE/'runs/tail-scale-current.json'

INSPECT = r'''
import importlib.util,json,os,pathlib
spec=importlib.util.spec_from_file_location('scale_check',MODULE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
check=m.node_check(NODE)
boot=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
helpers=[];rms=[]
for path in pathlib.Path('/proc').iterdir():
 if not path.name.isdigit():continue
 try:
  args=[os.fsdecode(x) for x in (path/'cmdline').read_bytes().split(b'\0') if x]
  fields=(path/'stat').read_text().rsplit(')',1)[1].split()
  if not args or fields[0]=='Z':continue
  actor=dict(pid=int(path.name),start=fields[19],boot=boot)
  script=str(pathlib.Path(OLD)/'rm24.py')
  if (script in args and args[args.index(script)+1:args.index(script)+2]==['_worker']
      and '--run' in args and args[args.index('--run')+1]==OLD):
   helpers.append(actor)
  if os.path.basename(args[0])=='rm':
   cwd=os.readlink(path/'cwd')
   if cwd==m.TARGET or cwd.startswith(m.TARGET+'/'):
    rms.append(dict(actor,ppid=int(fields[1])))
 except (OSError,ValueError,IndexError):pass
owners={p['pid'] for p in helpers}
known={(a['pid'],a['start'],a['boot']) for a in KNOWN}
for actor in rms:
 actor['owned']=(actor['ppid'] in owners or
                 (actor['pid'],actor['start'],actor['boot']) in known)
print(json.dumps(dict(check=check,node=NODE,helpers=helpers,rms=rms)))
'''


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def alive(actor):
    try:
        fields = Path('/proc/'+str(actor['pid'])+'/stat').read_text().rsplit(')', 1)[1].split()
        return (fields[0] != 'Z' and rm24.identity(actor['pid']) == actor)
    except FileNotFoundError:
        return False


def inspect_nodes(old, nodes, known=()):
    known = {r['node']:r.get('rms', []) for r in known}
    def inspect(node):
        code = ('MODULE='+repr(str(HERE/'rm24.py'))+'\nOLD='+repr(str(old))+
                '\nNODE='+repr(node)+'\nKNOWN='+repr(known.get(node, []))+'\n'+INSPECT)
        result = subprocess.run(['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=6',
                                 node, 'python3 -S -B -'], input=code, text=True,
                                capture_output=True, timeout=22)
        if result.returncode:
            raise RuntimeError(node+': scale inspection failed: '+result.stderr[-1000:])
        return json.loads(result.stdout)
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        return list(pool.map(inspect, nodes))


def scopes_for(cfg, original):
    """Retain stopped/rc1 roots; looking only at live original rm loses them."""
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    candidates = list(cfg['tail_scopes'])
    try:
        failed = original/'failed-jobs.jsonl'
        if failed.exists():
            for line in failed.read_text().splitlines():
                task = json.loads(line)['task']
                try:
                    with root.parent(task['parent'], task['ino']) as fd:
                        for name in task['names']:
                            rm24.component(name)
                            try:
                                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                            except FileNotFoundError:
                                continue
                            if stat.S_ISDIR(info.st_mode):
                                candidates.append(dict(rel='/'.join(filter(None, (task['parent'], name))),
                                                       ino=info.st_ino))
                except FileNotFoundError:
                    pass
        selected = []
        for scope in sorted(candidates, key=lambda s:(s['rel'].count('/'), s['rel'])):
            with_scope = scope['rel']
            if any(with_scope == p['rel'] or with_scope.startswith(p['rel']+'/') for p in selected):
                continue
            try:
                with root.parent(with_scope, scope['ino']):
                    pass
            except FileNotFoundError:
                continue
            selected.append(scope)
        return selected
    finally:
        root.close()


def binding(plan):
    old = Path(plan['old']).resolve()
    original = rm24.RUN.resolve()
    cfg = rm24.load(old/'config.json')
    if (old.parent != (HERE/'runs').resolve() or not old.name.startswith('tail-boost-')
            or old == original or cfg.get('supplement_of') != str(original)
            or cfg.get('target') != rm24.TARGET or cfg.get('root_inode') != rm24.ROOT_INODE
            or 'tail_scopes' not in cfg):
        raise ValueError('scale may target only the bound streaming supplement')
    if digest(old/'config.json') != plan['old_config_sha'] or digest(old/'rm24.py') != cfg['sha256']:
        raise ValueError('old frozen configuration/code changed')
    for name, expected in plan['source_sha'].items():
        if digest(HERE/name) != expected:
            raise ValueError('prepared source changed; prepare a new scale plan')
    if rm24.load(tailboost.POINTER)['run'] != str(old):
        raise ValueError('current streaming run changed')
    if cfg.get('runtime_join'):
        # Frozen execution_nodes omit subsequently accepted members. A legacy
        # stop/start scale cannot prove that all dynamic members have exited.
        raise ValueError('runtime-enabled runs support online join only, not legacy stop/start scale')
    upgrade = plan.get('runtime_join', False)
    expected = cfg['per_node'] if upgrade else cfg['per_node']*2
    if (plan['nodes'] != cfg['execution_nodes'] or len(set(plan['nodes'])) != len(plan['nodes'])
            or any(n not in rm24.NODES for n in plan['nodes'])
            or plan['per_node'] != expected or not 1 <= plan['per_node'] <= 8):
        raise ValueError('scale plan is not the bound supplement configuration')
    if upgrade and (cfg.get('runtime_join') or plan.get('trial_node') not in rm24.NODES
                    or plan['trial_node'] in plan['nodes']):
        raise ValueError('runtime upgrade/trial binding differs')
    return old, original, cfg


def wait_ready(run, timeout=45):
    deadline = time.monotonic()+timeout
    while True:
        try:
            status = rm24.load(run/'status.json')
            controller_live = alive(rm24.load(run/'controller.json'))
        except (FileNotFoundError, json.JSONDecodeError):
            if time.monotonic() >= deadline:
                raise RuntimeError('replacement readiness metadata unavailable; inspect '+str(run))
            time.sleep(1)
            continue
        fresh = time.time()-status['updated'] < 15
        state = status['state']
        if fresh and state == 'supplement_complete':
            return status
        if fresh and controller_live and state in ('rm_running', 'retrying_failed_jobs'):
            return status
        if state in ('stopped_or_error', 'supplement_finished_with_errors') or not controller_live:
            raise RuntimeError('replacement failed readiness; inspect '+str(run))
        if time.monotonic() >= deadline:
            raise RuntimeError('replacement readiness timed out; inspect '+str(run))
        time.sleep(1)


def prepare(per_node=4, runtime_join=False, trial_node='worker-3'):
    pointer = rm24.load(tailboost.POINTER)
    old = Path(pointer['run']).resolve()
    cfg = rm24.load(old/'config.json')
    expected = cfg['per_node'] if runtime_join else cfg['per_node']*2
    if per_node != expected or not 1 <= per_node <= 8:
        raise ValueError('keep the current per-node cap for runtime upgrade; otherwise double it, up to 8/node')
    source_names = ['rm24.py', 'boost.py', 'tailboost.py', 'scale_tailboost.py']
    if runtime_join:
        source_names.append('join_tailboost.py')
    plan = dict(old=str(old), old_config_sha=digest(old/'config.json'),
                source_sha={n:digest(HERE/n) for n in source_names},
                old_controller=rm24.load(old/'controller.json'), nodes=cfg['execution_nodes'],
                old_per_node=cfg['per_node'], per_node=per_node, prepared_at=time.time())
    if runtime_join:
        plan.update(runtime_join=True, trial_node=trial_node)
    old, original, cfg = binding(plan)
    state = rm24.load(old/'status.json')
    if state['state'] != 'rm_running' or time.time()-state['updated'] > 30:
        raise ValueError('old streaming supplement is not freshly running')
    if not alive(plan['old_controller']):
        raise ValueError('old controller identity is not live')
    rows = inspect_nodes(old, plan['nodes'])
    if any(not p['owned'] for row in rows for p in row['rms']):
        raise ValueError('another rm is using selected nodes; no processes were stopped')
    if runtime_join:
        plan['trial_preflight'] = rm24.preflight(HERE/'rm24.py', [trial_node])
    scopes = scopes_for(cfg, original)
    if not scopes:
        raise ValueError('no remaining directory scopes')
    work = HERE/'runs'/('tail-scale-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
    work.mkdir(mode=0o700)
    plan.update(scopes=scopes, observations=rows)
    rm24.atomic(work/'plan.json', plan)
    rm24.atomic(POINTER, dict(work=str(work), plan_sha=digest(work/'plan.json')))
    print(json.dumps(dict(prepared=str(work), old_per_node=cfg['per_node'], new_per_node=per_node,
        nodes=len(plan['nodes']), old_max=len(plan['nodes'])*cfg['per_node'],
        new_max=len(plan['nodes'])*per_node, scopes=len(scopes),
        runtime_join=runtime_join, trial_node=trial_node if runtime_join else None,
        stop_start_required=True, strict_idle_preflight_deferred_until_apply=True, applied=False)))


def apply():
    pointer = rm24.load(POINTER)
    work = Path(pointer['work'])
    if digest(work/'plan.json') != pointer['plan_sha']:
        raise ValueError('scale plan changed')
    plan = rm24.load(work/'plan.json')
    old, original, cfg = binding(plan)
    current_controller = rm24.load(old/'controller.json')
    if current_controller != plan['old_controller']:
        raise ValueError('old controller identity changed')
    rows = inspect_nodes(old, plan['nodes'])
    if any(not p['owned'] for row in rows for p in row['rms']):
        raise ValueError('another rm is using selected nodes; no processes were stopped')
    with (work/'apply-claim.json').open('x') as stream:
        json.dump(dict(at=time.time()), stream)
    # Explicit operator action. Never signal/stop the original or first boost.
    rm24.atomic(old/'STOP', dict(reason='operator_scale_streaming_supplement', at=time.time()))
    deadline = time.monotonic()+150
    while True:
        observed = inspect_nodes(old, plan['nodes'], rows)
        rm24.atomic(work/'last-drain-observation.json', observed)
        remaining = sum(len(row['helpers'])+len(row['rms']) for row in observed)
        controller_live = alive(plan['old_controller'])
        print(json.dumps(dict(stage='waiting_for_old_processes_to_exit', remaining=remaining,
                              old_controller_live=controller_live)), flush=True)
        if not remaining and not controller_live:
            break
        # Retain identity proofs even if a live rm becomes orphaned while the
        # helper exits. Unknown survivors also block startup; no broad kill.
        by_node = {r['node']:r for r in rows}
        for row in observed:
            previous = by_node[row['node']]
            merged = {(a['pid'],a['start'],a['boot']):a for a in previous['rms']}
            merged.update({(a['pid'],a['start'],a['boot']):a for a in row['rms'] if a['owned']})
            previous['rms'] = list(merged.values())
        if time.monotonic() >= deadline:
            raise RuntimeError('old processes did not all exit; replacement NOT started')
        time.sleep(5)
    # After exit, strict preflight must reject any surviving or newly-arrived
    # rm, including unknown processes. prepare and controller both check it.
    scopes = scopes_for(cfg, original)
    if not scopes:
        raise RuntimeError('no remaining scopes; replacement NOT started')
    observations = boost.observe(original)
    busy = sorted({p for row in observations for p in row['opened']})
    run = HERE/'runs'/('tail-boost-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
    options = dict(execution_nodes=plan['nodes'], supplement_of=original,
                   tail_scopes=scopes, blocked_paths=cfg.get('blocked_paths', ()), busy_paths=busy)
    if plan.get('runtime_join'):
        options['runtime_join'] = True
    rm24.prepare(run, plan['per_node'], **options)
    rm24.atomic(work/'replacement.json', dict(run=str(run), scopes=scopes))
    subprocess.run([sys.executable, '-S', '-B', '-u', str(run/'rm24.py'), 'start', '--run', str(run)],
                   check=True)
    rm24.atomic(tailboost.POINTER, dict(run=str(run), original=str(original)))
    status = wait_ready(run)
    rm24.atomic(work/'ready.json', status)
    trial_result = None
    if plan.get('runtime_join'):
        # This is part of the explicit operator apply, never read-only prepare.
        # A rejected trial leaves the established baseline members running.
        import join_tailboost
        try:
            trial_result = join_tailboost.request_join(run, plan['trial_node'])
        except Exception as exc:
            rm24.atomic(work/'trial-join-error.json', dict(error=repr(exc), run=str(run), at=time.time()))
            raise RuntimeError('baseline replacement has already been launched and the current pointer has changed; '
                               'trial join was not confirmed; do NOT repeat apply. Inspect '+str(run)) from exc
        rm24.atomic(work/'trial-join.json', trial_result)
    base_max = len(plan['nodes'])*plan['per_node']
    print(json.dumps(dict(applied=True, old=str(old), new=str(run), per_node=plan['per_node'],
                          base_max=base_max, new_max=base_max+(plan['per_node'] if trial_result else 0),
                          trial_node=plan.get('trial_node'), trial_state=trial_result['state'] if trial_result else None,
                          state=status['state'])), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'apply'))
    parser.add_argument('--per-node', type=int, default=4)
    parser.add_argument('--runtime-join', action='store_true')
    parser.add_argument('--trial-node', choices=rm24.NODES, default='worker-3')
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(args.per_node, args.runtime_join, args.trial_node)
    else:
        apply()


if __name__ == '__main__':
    main()
