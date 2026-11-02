#!/usr/bin/env python3
"""Prepare a registry upgrade; only explicit operator apply stops/starts jobs.

The previous controller has no drain interface. Its supplementary rm children
are cancelled, positively accounted for, and then remaining scopes are
re-enumerated. The original run and its children are never signalled.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

import rm24
import scale_tailboost as scale
import tailboost
import verify_workers


HERE = Path(__file__).resolve().parent
POINTER = HERE/'runs/upgrade40-current.json'
PARENT_POINTER = HERE/'runs/parent-deploy-current.json'
PARENT_MODE = 'parent-policy-v1'
SOURCE_NAMES = ('rm24.py', 'upgrade40_tailboost.py', 'boost.py', 'tailboost.py',
                'scale_tailboost.py', 'join_tailboost.py', 'verify_workers.py')
PARENT_HELPERS = ('parent_scheduler.py', 'parent_tuning.py')

INSPECT = r'''
import hashlib,importlib.util,json,os,pathlib
if hashlib.sha256(pathlib.Path(MODULE).read_bytes()).hexdigest()!=MODULE_SHA:
 raise ValueError('inspection code changed')
spec=importlib.util.spec_from_file_location('upgrade40_check',MODULE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
check=m.node_check(NODE)
boot=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
helpers={'old':[],'original':[]};rms={'old':[],'original':[],'unknown':[]}
owners={};candidates=[]
for path in pathlib.Path('/proc').iterdir():
 if not path.name.isdigit():continue
 try:
  args=[os.fsdecode(x) for x in (path/'cmdline').read_bytes().split(b'\0') if x]
  fields=(path/'stat').read_text().rsplit(')',1)[1].split()
  if not args or fields[0]=='Z':continue
  actor=dict(pid=int(path.name),start=fields[19],boot=boot)
  for owner,run in [('old',OLD),('original',ORIGINAL)]:
   script=str(pathlib.Path(run)/'rm24.py')
   if (script in args and args[args.index(script)+1:args.index(script)+2]==['_worker']
       and '--run' in args and args[args.index('--run')+1]==run
       and '--node' in args and args[args.index('--node')+1]==NODE):
    helpers[owner].append(actor);owners[actor['pid']]=owner
  if os.path.basename(args[0])!='rm':continue
  cwd=os.readlink(path/'cwd').removesuffix(' (deleted)')
  if not m.rm_touches_target(args,cwd):continue
  opened=[]
  for fd in (path/'fd').iterdir():
   try:
    value=os.readlink(fd).removesuffix(' (deleted)')
    if value.startswith(m.TARGET+'/'):opened.append(value)
   except (FileNotFoundError,ProcessLookupError):pass
  candidates.append(dict(actor,ppid=int(fields[1]),opened=sorted(set(opened))))
 except (FileNotFoundError,ProcessLookupError):pass
for actor in candidates:
 key=(actor['pid'],actor['start'],actor['boot'])
 known_owners=[owner for owner in ('old','original')
               if key in {(x['pid'],x['start'],x['boot']) for x in KNOWN.get(owner,[])}]
 owner=owners.get(actor['ppid'])
 if len(known_owners)>1 or (known_owners and owner and known_owners[0]!=owner):
  raise ValueError('conflicting rm ownership evidence')
 owner=owner or (known_owners[0] if known_owners else 'unknown')
 rms[owner].append(actor)
print(json.dumps(dict(node=NODE,boot=boot,check=check,helpers=helpers,rms=rms)))
'''


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def root_check():
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    root.close()


def registry_for(cfg, original_cfg, parent_mode=False):
    old_nodes = cfg.get('nodes')
    original_nodes = original_cfg.get('nodes')
    if (not isinstance(original_nodes, list) or len(original_nodes) != 24
            or len(set(original_nodes)) != 24):
        raise ValueError('old registry must be the original 24-node registry')
    hosts = dict(rm24.NODE_HOSTS)
    if len(hosts) != 40 or len(set(hosts.values())) != 40 or list(hosts) != rm24.NODES:
        raise ValueError('new registry must bind exactly 40 distinct node identities')
    if any(hosts.get(node) != 'job-'+rm24.CLUSTER+'-'+node for node in original_nodes):
        raise ValueError('old cluster hostname binding differs')
    extras = [node for node in hosts if node not in original_nodes]
    if (len(extras) != 16 or any(not hosts[node].startswith('job-'+rm24.EXTRA_CLUSTER+'-')
                                 for node in extras)):
        raise ValueError('new cluster registry differs')
    if parent_mode:
        if old_nodes != rm24.NODES or cfg.get('node_hosts') != hosts:
            raise ValueError('parent deployment requires the bound complete forty-node registry')
        return list(rm24.NODES), []
    if old_nodes != original_nodes:
        raise ValueError('old registry must be the original 24-node registry')
    return old_nodes, extras


def parent_value(value):
    if value not in ('off', '1', '2'):
        raise ValueError('parent limit must be off, 1 or 2')
    return None if value == 'off' else int(value)


def is_parent(plan):
    mode = plan.get('mode', 'registry-24-to-40')
    if mode not in ('registry-24-to-40', PARENT_MODE):
        raise ValueError('unknown upgrade mode')
    return mode == PARENT_MODE


def source_names(plan):
    return SOURCE_NAMES+PARENT_HELPERS if is_parent(plan) else SOURCE_NAMES


def current_members(cfg, status):
    runtime = status.get('runtime_join', {})
    members = []
    for values in (cfg.get('execution_nodes', []), runtime.get('accepted', []), runtime.get('starting', [])):
        if not isinstance(values, list) or any(node not in cfg['nodes'] for node in values):
            raise ValueError('invalid dynamic membership evidence')
        members.extend(values)
    return sorted(set(members))


def inspect_nodes(old, original, nodes, known=None):
    known = {} if known is None else known
    module_sha = digest(HERE/'rm24.py')

    def inspect(node):
        code = ('MODULE='+repr(str(HERE/'rm24.py'))+'\nMODULE_SHA='+repr(module_sha)
                +'\nOLD='+repr(str(old))+'\nORIGINAL='+repr(str(original))
                +'\nNODE='+repr(node)+'\nKNOWN='+repr(known.get(node, {}))+'\n'+INSPECT)
        response = subprocess.run(['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=6',
            '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', node, 'python3 -S -B -'],
            input=code, text=True, capture_output=True, timeout=25)
        if response.returncode:
            raise RuntimeError(node+': ownership inspection failed: '+response.stderr[-1000:])
        return json.loads(response.stdout)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        rows = list(pool.map(inspect, nodes))
    validate_rows(rows, nodes)
    return rows


def validate_rows(rows, nodes, node_boots=None, require_boots=False):
    if len(rows) != len(nodes) or {row['node'] for row in rows} != set(nodes):
        raise ValueError('ownership inspection omitted/duplicated a node')
    for row in rows:
        if row['check'].get('node') != row['node'] or row['check'].get('host') != rm24.NODE_HOSTS[row['node']]:
            raise ValueError('inspection node identity differs')
        if row['rms'].get('unknown'):
            raise ValueError(row['node']+': unowned target rm; no broad kill is permitted')
        if require_boots or node_boots is not None:
            boot = row.get('boot')
            if not isinstance(boot, str) or not boot or (node_boots is not None and boot != node_boots[row['node']]):
                raise ValueError(row['node']+': boot identity changed or unavailable')
            actors = [actor for values in row['helpers'].values() for actor in values]
            actors += [actor for values in row['rms'].values() for actor in values]
            if any(actor.get('boot') != boot for actor in actors):
                raise ValueError(row['node']+': process boot differs from observed node boot')


def merge_known(known, rows):
    for row in rows:
        saved = known.setdefault(row['node'], {})
        for owner in ('old', 'original'):
            merged = {(a['pid'], a['start'], a['boot']): a for a in saved.get(owner, [])}
            merged.update({(a['pid'], a['start'], a['boot']): a for a in row['rms'][owner]})
            saved[owner] = list(merged.values())
    return known


def eligible_nodes(rows, extras):
    return [row['node'] for row in rows if not row['rms']['original']]+list(extras)


def static_binding(plan, require_running=True, allow_unobserved_boots=False):
    old, original = Path(plan['old']).resolve(), rm24.RUN.resolve()
    if (old.parent != (HERE/'runs').resolve() or not old.name.startswith('tail-boost-')
            or original == old or plan['original'] != str(original)):
        raise ValueError('upgrade may replace only the bound supplementary run')
    cfg, original_cfg = rm24.load(old/'config.json'), rm24.load(original/'config.json')
    if (cfg.get('target') != rm24.TARGET or cfg.get('root_inode') != rm24.ROOT_INODE
            or cfg.get('supplement_of') != str(original) or cfg.get('runtime_join') is not True
            or type(cfg.get('per_node')) is not int or cfg['per_node'] != 4
            or not cfg.get('tail_scopes') or original_cfg.get('target') != rm24.TARGET
            or original_cfg.get('root_inode') != rm24.ROOT_INODE):
        raise ValueError('scope, original binding, or fixed per-node cap differs')
    parent_mode = is_parent(plan)
    old_nodes, extras = registry_for(cfg, original_cfg, parent_mode)
    if (plan['old_nodes'] != old_nodes or plan['extras'] != extras
            or plan['node_hosts'] != rm24.NODE_HOSTS
            or plan['old_config_sha'] != digest(old/'config.json')
            or plan['original_config_sha'] != digest(original/'config.json')
            or digest(old/'rm24.py') != cfg['sha256']
            or digest(original/'rm24.py') != original_cfg['sha256']):
        raise ValueError('frozen registry/configuration/code changed')
    if set(plan['source_sha']) != set(source_names(plan)):
        raise ValueError('source hash manifest is incomplete')
    for name, expected in plan['source_sha'].items():
        if digest(HERE/name) != expected:
            raise ValueError('prepared source changed: '+name)
    if cfg.get('parent_helpers') is not None:
        if set(cfg['parent_helpers']) != set(PARENT_HELPERS):
            raise ValueError('old parent helper manifest differs')
        for name, expected in cfg['parent_helpers'].items():
            if digest(old/name) != expected:
                raise ValueError('old frozen parent helper changed: '+name)
    if parent_mode:
        if plan.get('parent_limit') is not None and (type(plan['parent_limit']) is not int or plan['parent_limit'] not in (1, 2)):
            raise ValueError('prepared parent limit differs')
        boots = plan.get('node_boots')
        if not (allow_unobserved_boots and boots is None):
            if (not isinstance(boots, dict) or set(boots) != set(rm24.NODES)
                    or any(not isinstance(boot, str) or not boot for boot in boots.values())):
                raise ValueError('parent deployment boot map is incomplete')
            if cfg.get('parent_policy') is not None and cfg['parent_policy'].get('node_boots') != boots:
                raise ValueError('old parent policy boot map changed')
    if (rm24.load(tailboost.POINTER)['run'] != str(old)
            or rm24.load(old/'controller.json') != plan['old_controller']
            or rm24.load(original/'controller.json') != plan['original_controller']):
        raise ValueError('run/controller binding changed')
    root_check()
    if require_running:
        status = rm24.load(old/'status.json')
        if ((old/'STOP').exists() or status['state'] != 'rm_running'
                or status.get('errors')
                or not -5 <= time.time()-status['updated'] <= 30
                or not scale.alive(plan['old_controller'])):
            raise ValueError('old supplement is stopped, stale, or no longer running')
        current_members(cfg, status)
    return old, original, cfg


def prepare(parent_limit=None):
    old = Path(rm24.load(tailboost.POINTER)['run']).resolve()
    if (old/'upgrade40-claim.json').exists():
        raise ValueError('this old run already has an upgrade claim; inspect it before proceeding')
    original = rm24.RUN.resolve()
    cfg, original_cfg = rm24.load(old/'config.json'), rm24.load(original/'config.json')
    parent_mode = parent_limit is not None
    limit = parent_value(parent_limit) if parent_mode else None
    old_nodes, extras = registry_for(cfg, original_cfg, parent_mode)
    plan = dict(version=1, old=str(old), original=str(original), prepared_at=time.time(),
        mode=PARENT_MODE if parent_mode else 'registry-24-to-40',
        old_nodes=old_nodes, extras=extras, node_hosts=dict(rm24.NODE_HOSTS),
        old_controller=rm24.load(old/'controller.json'),
        original_controller=rm24.load(original/'controller.json'),
        old_config_sha=digest(old/'config.json'), original_config_sha=digest(original/'config.json'),
        source_sha={name:digest(HERE/name) for name in SOURCE_NAMES+(PARENT_HELPERS if parent_mode else ())})
    if parent_mode:
        plan.update(parent_limit=limit, node_boots=None)
    old, original, cfg = static_binding(plan, allow_unobserved_boots=parent_mode)
    rows = inspect_nodes(old, original, old_nodes)
    validate_rows(rows, old_nodes, require_boots=parent_mode)
    if parent_mode:
        plan['node_boots'] = {row['node']: row['boot'] for row in rows}
    extra_checks = rm24.preflight(HERE/'rm24.py', extras) if extras else []
    scopes = scale.scopes_for(cfg, original)
    if not scopes:
        raise ValueError('no remaining supplementary scopes')
    plan.update(observations=rows, extra_preflight=extra_checks, scopes=scopes,
                dynamic_members=current_members(cfg, rm24.load(old/'status.json')),
                execution_nodes=eligible_nodes(rows, extras))
    if parent_mode:
        rows = inspect_nodes(old, original, old_nodes, merge_known({}, rows))
        validate_rows(rows, old_nodes, plan['node_boots'])
        plan.update(observations=rows, execution_nodes=eligible_nodes(rows, extras))
    static_binding(plan)  # Fence source/config changes during preflight.
    if (old/'upgrade40-claim.json').exists():
        raise ValueError('an upgrade claimed this run during preparation')
    work = HERE/'runs'/('upgrade40-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
    work.mkdir(mode=0o700)
    rm24.atomic(work/'plan.json', plan)
    rm24.atomic(work/'previous-status.json', rm24.load(old/'status.json'))
    rm24.atomic(PARENT_POINTER if parent_mode else POINTER, dict(work=str(work), plan_sha=digest(work/'plan.json')))
    result = dict(prepared=str(work), started=False, old_registry_nodes=len(old_nodes),
        dynamic_members=len(plan['dynamic_members']), registry_nodes=len(rm24.NODES),
        execution_nodes=plan['execution_nodes'], rm_max=len(plan['execution_nodes'])*4,
        per_node=4, mode=plan['mode'], parent_limit=limit,
        requires_supplement_cancellation_and_rescan=True)
    print(json.dumps(result), flush=True)
    return result


def claim_upgrade(old, work, plan_sha):
    # The lock belongs to the replaced run, not a particular plan. Two plans
    # must never both cancel the same controller and launch replacements.
    with (old/'upgrade40-claim.json').open('x') as stream:
        json.dump(dict(work=str(work), plan_sha=plan_sha, at=time.time()), stream)
        stream.flush()
        os.fsync(stream.fileno())


def replacement_parent_binding(run, plan, execution_nodes):
    cfg = rm24.load(run/'config.json')
    policy = dict(limit=plan['parent_limit'], node_boots=plan['node_boots'])
    expected_helpers = {name: plan['source_sha'][name] for name in PARENT_HELPERS}
    if (cfg.get('target') != rm24.TARGET or cfg.get('root_inode') != rm24.ROOT_INODE
            or cfg.get('supplement_of') != plan['original'] or cfg.get('runtime_join') is not True
            or cfg.get('nodes') != rm24.NODES or cfg.get('node_hosts') != plan['node_hosts']
            or cfg.get('execution_nodes') != execution_nodes or type(cfg.get('per_node')) is not int
            or cfg['per_node'] != 4 or cfg.get('parent_policy') != policy
            or cfg.get('parent_helpers') != expected_helpers
            or cfg.get('sha256') != plan['source_sha']['rm24.py']
            or digest(run/'rm24.py') != plan['source_sha']['rm24.py']):
        raise ValueError('replacement frozen policy/configuration/engine differs from plan')
    for name, expected in expected_helpers.items():
        if digest(run/name) != expected:
            raise ValueError('replacement frozen parent helper changed: '+name)
    return cfg


def parent_readiness(readiness, plan, execution_nodes):
    status = readiness['status']
    if readiness.get('state') == 'terminal_complete':
        if status.get('state') != 'supplement_complete':
            raise RuntimeError('replacement terminal readiness is not successful completion')
    else:
        proofs = readiness.get('proofs', [])
        if (readiness.get('state') != 'workers_ready'
                or readiness.get('verified_nodes') != len(execution_nodes)
                or len(proofs) != len(execution_nodes)
                or {proof.get('node') for proof in proofs} != set(execution_nodes)
                or any(proof.get('boot') != plan['node_boots'].get(proof.get('node')) for proof in proofs)):
            raise RuntimeError('replacement worker readiness omitted or changed a worker boot')
    scheduler, tuner = status.get('parent_scheduler'), status.get('parent_tuning')
    if (not isinstance(scheduler, dict) or not isinstance(tuner, dict)
            or 'limit' not in scheduler or 'limit' not in tuner
            or scheduler['limit'] != plan['parent_limit'] or tuner['limit'] != plan['parent_limit']
            or 'error' not in tuner or tuner['error'] is not None
            or type(tuner.get('generation')) is not int or tuner['generation'] < 0
            or any(type(scheduler.get(key)) is not int or scheduler[key] < 0
                   for key in ('pending', 'active_leases', 'claims', 'releases'))
            or scheduler['claims']-scheduler['releases'] != scheduler['active_leases']):
        raise RuntimeError('replacement scheduler/tuner readiness is unconfirmed')
    if readiness.get('state') == 'terminal_complete' and (scheduler['pending'] or scheduler['active_leases']):
        raise RuntimeError('replacement terminal readiness contradicts pending/in-flight work')


def apply(timeout=180, interval=3, parent_limit=None):
    parent_mode = parent_limit is not None
    limit = parent_value(parent_limit) if parent_mode else None
    pointer = rm24.load(PARENT_POINTER if parent_mode else POINTER)
    work = Path(pointer['work']).resolve()
    if work.parent != (HERE/'runs').resolve() or not work.name.startswith('upgrade40-'):
        raise ValueError('invalid upgrade plan directory')
    if digest(work/'plan.json') != pointer['plan_sha']:
        raise ValueError('upgrade plan changed')
    plan = rm24.load(work/'plan.json')
    if plan.get('version') != 1:
        raise ValueError('unsupported upgrade plan version')
    if is_parent(plan) != parent_mode or (parent_mode and plan['parent_limit'] != limit):
        raise ValueError('explicit apply mode/limit differs from prepared plan')
    old, original, cfg = static_binding(plan)
    known = merge_known({}, plan['observations'])
    rows = inspect_nodes(old, original, plan['old_nodes'], known)
    validate_rows(rows, plan['old_nodes'], plan.get('node_boots') if parent_mode else None)
    merge_known(known, rows)
    if plan['extras']:
        rm24.preflight(HERE/'rm24.py', plan['extras'])
    static_binding(plan)
    claim_upgrade(old, work, pointer['plan_sha'])
    with (work/'apply-claim.json').open('x') as stream:
        json.dump(dict(at=time.time(), old=str(old)), stream)
        stream.flush()
        os.fsync(stream.fileno())
    # Only an explicit operator apply reaches this cancellation. No signals,
    # pid-wide kills, original STOP writes, or config edits are ever performed.
    new_run = None
    try:
        static_binding(plan)
        rm24.atomic(old/'STOP', dict(reason='operator_parent_policy_supplement' if parent_mode
                                   else 'operator_upgrade40_supplement', at=time.time()))
        deadline, consecutive = time.monotonic()+timeout, 0
        while True:
            rows = inspect_nodes(old, original, plan['old_nodes'], known)
            validate_rows(rows, plan['old_nodes'], plan.get('node_boots') if parent_mode else None)
            merge_known(known, rows)
            remaining = sum(len(row['helpers']['old'])+len(row['rms']['old']) for row in rows)
            live = scale.alive(plan['old_controller'])
            consecutive = consecutive+1 if not remaining and not live else 0
            rm24.atomic(work/'last-drain-observation.json', dict(at=time.time(), rows=rows,
                known=known, old_controller_live=live, consecutive_empty=consecutive))
            print(json.dumps(dict(stage='confirming_old_supplement_exit',
                remaining=remaining, controller_live=live, consecutive_empty=consecutive)), flush=True)
            if consecutive >= 2:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('old supplement exit unconfirmed; replacement NOT started')
            time.sleep(interval)
        static_binding(plan, require_running=False)
        scopes = scale.scopes_for(cfg, original)
        if not scopes:
            raise RuntimeError('no remaining scopes; replacement NOT started')
        busy = sorted({path for row in rows for actor in row['rms']['original'] for path in actor['opened']})
        execution_nodes = eligible_nodes(rows, plan['extras'])
        new_run = HERE/'runs'/('tail-boost-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
        options = dict(execution_nodes=execution_nodes, supplement_of=original,
            tail_scopes=scopes, blocked_paths=cfg.get('blocked_paths', ()), busy_paths=busy,
            runtime_join=True)
        if parent_mode:
            options['parent_policy'] = dict(limit=limit, node_boots=plan['node_boots'])
        rm24.prepare(new_run, 4, **options)
        rm24.atomic(work/'replacement.json', dict(run=str(new_run), original=str(original),
            previous=str(old), execution_nodes=execution_nodes, per_node=4,
            mode=plan.get('mode', 'registry-24-to-40'), parent_limit=limit))
        static_binding(plan, require_running=False)
        if parent_mode:
            replacement_parent_binding(new_run, plan, execution_nodes)
            rows = inspect_nodes(old, original, plan['old_nodes'], known)
            validate_rows(rows, plan['old_nodes'], plan['node_boots'])
            if (scale.alive(plan['old_controller']) or any(row['helpers']['old'] or row['rms']['old'] for row in rows)
                    or any(row['node'] in execution_nodes and row['rms']['original'] for row in rows)):
                raise RuntimeError('process ownership changed before replacement start')
            static_binding(plan, require_running=False)
            replacement_parent_binding(new_run, plan, execution_nodes)
        subprocess.run([sys.executable, '-S', '-B', '-u', str(new_run/'rm24.py'),
                        'start', '--run', str(new_run)], check=True)
        status = scale.wait_ready(new_run)
        worker_readiness = verify_workers.wait_workers_ready(new_run, execution_nodes)
        rm24.atomic(work/'worker-readiness.json', worker_readiness)
        if parent_mode:
            static_binding(plan, require_running=False)
            replacement_parent_binding(new_run, plan, execution_nodes)
            parent_readiness(worker_readiness, plan, execution_nodes)
        # The verification may take several seconds; report its final phase,
        # not the earlier controller-only readiness snapshot.
        status = worker_readiness['status']
        if (status['state'] not in ('rm_running', 'retrying_failed_jobs', 'supplement_complete')
                or status.get('errors') or (new_run/'STOP').exists()
                or (parent_mode and not -5 <= time.time()-status['updated'] <= 15)):
            raise RuntimeError('replacement failed after worker readiness; inspect '+str(new_run))
        rm24.atomic(work/'ready.json', status)
        rm24.atomic(tailboost.POINTER, dict(run=str(new_run), original=str(original)))
        result = dict(ready=True, old=str(old), new=str(new_run), registry_nodes=len(rm24.NODES),
            execution_nodes=len(execution_nodes), per_node=4, rm_max=len(execution_nodes)*4,
            state=status['state'], mode=plan.get('mode', 'registry-24-to-40'), parent_limit=limit)
        print(json.dumps(result), flush=True)
        return result
    except BaseException as exc:
        rm24.atomic(work/'failure.json', dict(at=time.time(), error=repr(exc),
            replacement=str(new_run) if new_run else None,
            note='Original run untouched. Do not retry this claim or assume replacement is absent.'))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'apply'))
    parser.add_argument('--parent-limit', choices=('off', '1', '2'))
    args = parser.parse_args()
    prepare(args.parent_limit) if args.action == 'prepare' else apply(parent_limit=args.parent_limit)


if __name__ == '__main__':
    main()
