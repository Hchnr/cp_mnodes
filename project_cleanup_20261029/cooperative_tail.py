#!/usr/bin/env python3
"""Explicitly launched, inode-bound cooperative help for one known long rm.

The enclosing native rm is neither stopped nor transferred. Its open-directory
snapshot is avoidance, NOT a lock: concurrent disappearance and rc=1 are
possible. This helper never broadens its three child scopes to the pipeline or
source root, and never changes the main tail pointer or any existing STOP.
"""
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

import rm24
import scale_tailboost as scale
import tailboost
import upgrade40_tailboost as upgrade
import verify_workers


HERE = Path(__file__).resolve().parent
POINTER = HERE/'runs/cooperative-tail-current.json'
MODE = 'cooperative-pipeline-v1'
PIPELINE_REL = 'eai_pwm/home/yuanhai/ego_r2_runs/20261024-ego-r1-1024-pipeline-v3'
CHILDREN = ('tasks', 'label_work', 'mcap_cache')
ANCHOR_NODE = 'extra-worker-3'
ANCHOR_HELPER = dict(pid=1025267, start='1545706822', boot='4b574926-ebb0-4ac5-802a-ad5bcc1e45f3')
ANCHOR_RM = dict(pid=1025289, start='1545706850', boot=ANCHOR_HELPER['boot'])
PILOTS = ('extra-worker-2', 'extra-worker-12', 'extra-worker-13', 'extra-worker-14')
HELPERS = ('parent_scheduler.py', 'parent_tuning.py')
SOURCE_NAMES = ('cooperative_tail.py', 'rm24.py', 'upgrade40_tailboost.py',
                'scale_tailboost.py', 'tailboost.py', 'boost.py', 'join_tailboost.py',
                'verify_workers.py')+HELPERS
# Reuse the reviewed, read-only ownership probe; expose the rm operands as
# positive evidence instead of inferring scope merely from an open fd.
INSPECT = (upgrade.INSPECT
    .replace('import hashlib,importlib.util,json,os,pathlib',
             'import hashlib,importlib.util,json,os,pathlib,stat')
    .replace("  cwd=os.readlink(path/'cwd').removesuffix(' (deleted)')",
             "  cwd_raw=os.readlink(path/'cwd')\n"
             "  cwd_ino=os.stat(path/'cwd').st_ino\n"
             "  if os.readlink(path/'cwd')!=cwd_raw:raise ValueError('rm cwd changed during probe')\n"
             "  cwd=cwd_raw.removesuffix(' (deleted)')")
    .replace('  opened=[]', '  opened=[];dir_fds=[]')
    .replace("    value=os.readlink(fd).removesuffix(' (deleted)')\n"
             "    if value.startswith(m.TARGET+'/'):opened.append(value)",
             "    raw_path=os.readlink(fd)\n"
             "    value=raw_path.removesuffix(' (deleted)')\n"
             "    if value.startswith(m.TARGET+'/'):\n"
             "     opened.append(value)\n"
             "     info=os.stat(fd)\n"
             "     if raw_path==os.readlink(fd) and stat.S_ISDIR(info.st_mode):\n"
             "      dir_fds.append(dict(raw_path=raw_path,ino=info.st_ino))")
    .replace('opened=sorted(set(opened))))',
             'opened=sorted(set(opened)),cwd=cwd,argv=args,cwd_raw=cwd_raw,'
             'cwd_ino=cwd_ino,dir_fds=dir_fds))'))


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def actor(value):
    return {key: value.get(key) for key in ('pid', 'start', 'boot')}


def inspect_nodes(old, original):
    """Read-only all-registry proof. No pre-existing worker is contacted."""
    module_sha = digest(HERE/'rm24.py')
    def inspect(node):
        known = dict(old=[ANCHOR_RM], original=[])
        code = ('MODULE='+repr(str(HERE/'rm24.py'))+'\nMODULE_SHA='+repr(module_sha)
                +'\nOLD='+repr(str(old))+'\nORIGINAL='+repr(str(original))
                +'\nNODE='+repr(node)+'\nKNOWN='+repr(known)+'\n'+INSPECT)
        result = subprocess.run(['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=6',
            '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', node, 'python3 -S -B -'],
            input=code, text=True, capture_output=True, timeout=25)
        if result.returncode:
            raise RuntimeError(node+': read-only inspection failed: '+result.stderr[-1000:])
        return json.loads(result.stdout)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(rm24.NODES)) as pool:
        return list(pool.map(inspect, rm24.NODES))


def validate_observations(rows, plan=None):
    boots = None if plan is None else plan['node_boots']
    upgrade.validate_rows(rows, rm24.NODES, boots, require_boots=True)
    by_node = {row['node']: row for row in rows}
    anchor = by_node[ANCHOR_NODE]
    if (actor(ANCHOR_HELPER) != ANCHOR_HELPER or actor(ANCHOR_RM) != ANCHOR_RM
            or anchor['boot'] != ANCHOR_HELPER['boot']
            or [actor(x) for x in anchor['helpers']['old']] != [ANCHOR_HELPER]
            or [actor(x) for x in anchor['rms']['old']] != [ANCHOR_RM]):
        raise ValueError('the positively owned anchor helper/rm changed or exited')
    pipeline = str(Path(rm24.TARGET)/PIPELINE_REL)
    expected_args = ['/usr/bin/rm', '-rf', '--one-file-system', '--', Path(PIPELINE_REL).name]
    expected_cwd = str(Path(pipeline).parent)
    for row in rows:
        if row['helpers']['original'] or row['rms']['original']:
            raise ValueError('original process resumed; cooperative plan is stale')
        if any(actor(x) != ANCHOR_HELPER for x in row['helpers']['old']):
            raise ValueError('another supplementary helper is active')
        for process in row['rms']['old']:
            if (actor(process) != ANCHOR_RM or process.get('ppid') != ANCHOR_HELPER['pid']
                    or process.get('argv') != expected_args or process.get('cwd') != expected_cwd):
                raise ValueError('another rm or a changed anchor scope is active')
    opened = anchor['rms']['old'][0].get('opened')
    if not isinstance(opened, list) or any(not isinstance(path, str) for path in opened):
        raise ValueError('anchor open-directory evidence is missing')
    return {row['node']: row['boot'] for row in rows}, sorted(set(
        path for path in opened if path == pipeline or path.startswith(pipeline+'/')))


def choose_pilots(rows, requested=PILOTS):
    if len(requested) != 4 or len(set(requested)) != 4:
        raise ValueError('exactly four distinct pilot aliases are required')
    lookup = {row['node']: row for row in rows}
    seen = {ANCHOR_RM['boot']}
    for node in requested:
        row = lookup.get(node)
        if (row is None or row['boot'] in seen or any(row['helpers'].values())
                or any(row['rms'].values())):
            raise ValueError('pilot must be idle in a separate non-anchor boot domain: '+node)
        seen.add(row['boot'])
    return list(requested)


def frozen_helpers(run, cfg):
    manifest = cfg.get('parent_helpers')
    if not isinstance(manifest, dict) or set(manifest) != set(HELPERS):
        raise ValueError('frozen helper manifest is incomplete')
    for name, expected in manifest.items():
        if digest(run/name) != expected:
            raise ValueError('frozen helper changed: '+name)


def static_binding(plan, require_running=True):
    old, original = Path(plan['old']).resolve(), rm24.RUN.resolve()
    if (old.parent != (HERE/'runs').resolve() or not old.name.startswith('tail-boost-')
            or old == original or plan['original'] != str(original)
            or plan.get('version') != 1 or plan.get('mode') != MODE
            or plan.get('pipeline_rel') != PIPELINE_REL
            or plan.get('anchor_helper') != ANCHOR_HELPER or plan.get('anchor_rm') != ANCHOR_RM):
        raise ValueError('cooperative scope/run/anchor binding differs')
    if (rm24.load(tailboost.POINTER).get('run') != str(old)
            or rm24.load(old/'controller.json') != plan['old_controller']
            or rm24.load(original/'controller.json') != plan['original_controller']):
        raise ValueError('main run/controller binding changed')
    cfg, base = rm24.load(old/'config.json'), rm24.load(original/'config.json')
    if (cfg.get('nodes') != rm24.NODES or cfg.get('node_hosts') != rm24.NODE_HOSTS
            or len(rm24.NODES) != 40 or cfg.get('runtime_join') is not True
            or cfg.get('supplement_of') != str(original) or type(cfg.get('per_node')) is not int
            or cfg['per_node'] != 4 or cfg.get('target') != rm24.TARGET
            or cfg.get('root_inode') != rm24.ROOT_INODE or base.get('target') != rm24.TARGET
            or base.get('root_inode') != rm24.ROOT_INODE
            or plan['node_hosts'] != rm24.NODE_HOSTS
            or digest(old/'config.json') != plan['old_config_sha']
            or digest(original/'config.json') != plan['original_config_sha']
            or digest(old/'rm24.py') != cfg.get('sha256')
            or digest(original/'rm24.py') != base.get('sha256')
            or digest(original/'STOP') != plan['original_stop_sha']):
        raise ValueError('frozen scope/configuration/original STOP evidence changed')
    frozen_helpers(old, cfg)
    if set(plan['source_sha']) != set(SOURCE_NAMES):
        raise ValueError('source manifest is incomplete')
    for name, expected in plan['source_sha'].items():
        if digest(HERE/name) != expected:
            raise ValueError('prepared source changed: '+name)
    if (not any(PIPELINE_REL == s['rel'] or PIPELINE_REL.startswith(s['rel']+'/')
                for s in cfg.get('tail_scopes', []))
            or cfg.get('parent_policy', {}).get('node_boots') != plan['node_boots']):
        raise ValueError('pipeline is outside frozen scope or boot map changed')
    if require_running and ((old/'STOP').exists() or (old/'first-error.json').exists()):
        raise ValueError('main supplement has stopped or failed')
    status = rm24.load(old/'status.json')
    sched = status.get('parent_scheduler', {})
    if require_running and (status.get('state') != 'rm_running' or status.get('errors')
            or not -5 <= time.time()-status.get('updated', 0) <= 30
            or not scale.alive(plan['old_controller'])
            or sched.get('pending') != 0 or sched.get('active_leases') != 1
            or sched.get('claims', -1)-sched.get('releases', -1) != 1
            or sched.get('active_by_boot') != {ANCHOR_RM['boot']: 1}
            or sched.get('active_flat_groups') != 0):
        raise ValueError('main run is not a fresh, unique, non-flat tail lease')
    upgrade.current_members(cfg, status)
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    root.close()
    return old, original, cfg, status


def scope_snapshot(expected=None):
    """Only the known three direct directories, all components nofollow/mount-bound."""
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    try:
        parent_rel, name = PIPELINE_REL.rsplit('/', 1)
        # First walk pins the parent inode without following a component link.
        fd = os.dup(root.fd)
        try:
            for part in parent_rel.split('/'):
                child = os.open(rm24.component(part), rm24.FLAGS, dir_fd=fd)
                os.close(fd)
                fd = child
                if rm24.mount_id(fd) != root.mount:
                    raise ValueError('nested mount excluded')
            parent_ino = os.fstat(fd).st_ino
        finally:
            os.close(fd)
        with root.parent(parent_rel, parent_ino) as fd:
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError('pipeline is not an anchored directory')
        pipeline_ino = info.st_ino
        if expected is not None and (parent_ino != expected['parent_ino']
                                     or pipeline_ino != expected['pipeline_ino']):
            raise ValueError('pipeline or parent inode changed')
        scopes = []
        with root.parent(PIPELINE_REL, pipeline_ino) as fd, os.scandir(fd) as entries:
            for entry in entries:
                if entry.name not in CHILDREN or not entry.is_dir(follow_symlinks=False):
                    raise ValueError('unrecognized pipeline child/type; review is required')
                rel, ino = PIPELINE_REL+'/'+entry.name, entry.inode()
                try:
                    with root.parent(rel, ino):
                        pass
                except FileNotFoundError:
                    continue  # The enclosing rm is deliberately still running.
                scopes.append(dict(rel=rel, ino=ino))
        scopes.sort(key=lambda item:item['rel'])
        if not scopes:
            raise ValueError('all three pipeline child scopes have disappeared')
        if expected is not None:
            previous = {scope['rel']: scope['ino'] for scope in expected['scopes']}
            if any(previous.get(scope['rel']) != scope['ino'] for scope in scopes):
                raise ValueError('a pipeline child is new or its inode changed')
        return dict(parent_rel=parent_rel, parent_ino=parent_ino,
                    pipeline_ino=pipeline_ino, scopes=scopes)
    finally:
        root.close()


def verify_tree_anchor(rows, snapshot):
    """Tie the named tree to a live rm directory FD, not just an argv string.

    FTS may close a descriptor during inspection; try all bounded /proc proofs
    and require only one still-live, non-deleted descendant. A pipeline-root
    FD is useful but not required. No file contents or recursive sizes are read.
    """
    scope_snapshot(snapshot)
    process = next(row for row in rows if row['node'] == ANCHOR_NODE)['rms']['old'][0]
    expected_cwd = str((Path(rm24.TARGET)/PIPELINE_REL).parent)
    if (process.get('cwd_raw') != expected_cwd
            or type(process.get('cwd_ino')) is not int
            or process['cwd_ino'] != snapshot['parent_ino']):
        raise ValueError('live rm cwd inode does not anchor the pipeline parent')
    proofs = process.get('dir_fds')
    if not isinstance(proofs, list):
        raise ValueError('live rm directory FD evidence is missing')
    pipeline = str(Path(rm24.TARGET)/PIPELINE_REL)
    if any(isinstance(proof, dict) and proof.get('raw_path') == pipeline
           and proof.get('ino') != snapshot['pipeline_ino'] for proof in proofs):
        raise ValueError('live rm pipeline-root FD inode differs')
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    matched = None
    try:
        for proof in proofs:
            if not isinstance(proof, dict):
                continue
            raw, ino = proof.get('raw_path'), proof.get('ino')
            if (not isinstance(raw, str) or raw.endswith(' (deleted)')
                    or type(ino) is not int or ino <= 0
                    or not (raw == pipeline or raw.startswith(pipeline+'/'))):
                continue
            rel = raw[len(rm24.TARGET)+1:]
            try:
                with root.parent(rel, ino):
                    pass
            except (FileNotFoundError, ValueError):
                continue
            matched = dict(raw_path=raw, ino=ino)
            break
    finally:
        root.close()
    if matched is None:
        raise ValueError('no live rm directory FD anchors the current pipeline tree')
    scope_snapshot(snapshot)  # Reject same-path tree replacement during proof.
    return dict(cwd_raw=expected_cwd, cwd_ino=process['cwd_ino'], directory_fd=matched,
                pipeline_ino=snapshot['pipeline_ino'], at=time.time())


def preview(scopes, busy, max_items=96, seconds=20):
    start, count, ticks = time.monotonic(), 0, 0
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    stream = rm24.cooperative_plan(root, scopes, busy=busy)
    samples = []
    try:
        for item in stream:
            if item.get('kind') == 'task':
                count += 1
                if len(samples) < 6:
                    samples.append(item)
            else:
                ticks += 1
            if count >= max_items or ticks >= max_items or time.monotonic()-start >= seconds:
                break
    finally:
        stream.close()
        root.close()
    return dict(tasks=count, scan_ticks=ticks, seconds=time.monotonic()-start,
                bounded=True, samples=samples, snapshot_is_not_lock=True)


def prepare(nodes=PILOTS):
    old = Path(rm24.load(tailboost.POINTER)['run']).resolve()
    if (old/'cooperative-claim.json').exists():
        raise ValueError('this anchor run already has a cooperative claim; inspect before proceeding')
    original = rm24.RUN.resolve()
    cfg = rm24.load(old/'config.json')
    boots = cfg.get('parent_policy', {}).get('node_boots')
    if not isinstance(boots, dict) or set(boots) != set(rm24.NODES):
        raise ValueError('main run does not freeze all forty node boots')
    plan = dict(version=1, mode=MODE, old=str(old), original=str(original),
        prepared_at=time.time(), pipeline_rel=PIPELINE_REL,
        anchor_helper=ANCHOR_HELPER, anchor_rm=ANCHOR_RM,
        old_controller=rm24.load(old/'controller.json'),
        original_controller=rm24.load(original/'controller.json'),
        old_config_sha=digest(old/'config.json'), original_config_sha=digest(original/'config.json'),
        original_stop_sha=digest(original/'STOP'), node_hosts=dict(rm24.NODE_HOSTS),
        node_boots=dict(boots), parent_limit=2,
        source_sha={name:digest(HERE/name) for name in SOURCE_NAMES})
    _, _, _, status = static_binding(plan)
    rows = inspect_nodes(old, original)
    _, busy = validate_observations(rows, plan)
    nodes = choose_pilots(rows, nodes)
    snapshot = scope_snapshot()
    verify_tree_anchor(rows, snapshot)
    sample = preview(snapshot['scopes'], busy)
    if not sample['tasks']:
        raise ValueError('bounded preview found no schedulable tasks; no pilot prepared')
    plan.update(execution_nodes=nodes, observations=rows, busy_paths=busy,
                scope_snapshot=snapshot, preview=sample,
                dynamic_members=upgrade.current_members(cfg, status))
    # A second positive process proof fences natural exit during the preview.
    rows = inspect_nodes(old, original)
    validate_observations(rows, plan)
    choose_pilots(rows, nodes)
    scope_snapshot(snapshot)
    plan['anchor_tree_proof'] = verify_tree_anchor(rows, snapshot)
    static_binding(plan)
    if (old/'cooperative-claim.json').exists():
        raise ValueError('a cooperative launch claimed this anchor during preparation')
    work = HERE/'runs'/('cooperative-tail-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
    work.mkdir(mode=0o700)
    rm24.atomic(work/'plan.json', plan)
    rm24.atomic(work/'previous-status.json', status)
    rm24.atomic(POINTER, dict(work=str(work), plan_sha=digest(work/'plan.json')))
    result = dict(prepared=str(work), started=False, mode=MODE, execution_nodes=nodes,
        per_node=4, rm_max=16, scopes=snapshot['scopes'], preview=sample,
        cooperative_overlap=True, snapshot_is_not_lock=True)
    print(json.dumps(result), flush=True)
    return result


def replacement_binding(run, plan, scopes, busy):
    cfg = upgrade.replacement_parent_binding(run, plan, plan['execution_nodes'])
    if (cfg.get('tail_scopes') != scopes or cfg.get('blocked_paths') != []
            or cfg.get('busy_paths') != busy):
        raise ValueError('replacement cooperative frontier differs from the prepared scope')
    return cfg


def apply(timeout=90):
    if type(timeout) not in (int, float) or not 0 < timeout <= 600:
        raise ValueError('timeout must be in (0, 600] seconds')
    pointer = rm24.load(POINTER)
    work = Path(pointer['work']).resolve()
    if work.parent != (HERE/'runs').resolve() or not work.name.startswith('cooperative-tail-'):
        raise ValueError('invalid cooperative plan directory')
    if digest(work/'plan.json') != pointer['plan_sha']:
        raise ValueError('cooperative plan changed')
    plan = rm24.load(work/'plan.json')
    old, original, _, _ = static_binding(plan)
    rows = inspect_nodes(old, original)
    _, busy = validate_observations(rows, plan)
    nodes = choose_pilots(rows, plan['execution_nodes'])
    snapshot = scope_snapshot(plan['scope_snapshot'])
    tree_proof = verify_tree_anchor(rows, snapshot)
    static_binding(plan)
    # Exclusive per-anchor claim, shared by all separately prepared plans.
    with (old/'cooperative-claim.json').open('x') as stream:
        json.dump(dict(work=str(work), plan_sha=pointer['plan_sha'], at=time.time()), stream)
        stream.flush()
        os.fsync(stream.fileno())
    new_run = None
    try:
        rm24.atomic(work/'apply-claim.json', dict(at=time.time(), anchor=str(old)))
        rm24.atomic(work/'anchor-tree-proof.json', tree_proof)
        new_run = HERE/'runs'/('cooperative-run-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
        rm24.prepare(new_run, 4, execution_nodes=nodes, supplement_of=original,
            tail_scopes=snapshot['scopes'], blocked_paths=[], busy_paths=busy,
            runtime_join=True, parent_policy=dict(limit=2, node_boots=plan['node_boots']))
        rm24.atomic(work/'replacement.json', dict(run=str(new_run), anchor=str(old),
            original=str(original), execution_nodes=nodes, per_node=4,
            cooperative_overlap=True, snapshot_is_not_lock=True))
        replacement_binding(new_run, plan, snapshot['scopes'], busy)
        # Freeze-copy/source race, changed boots, unknown rm, vanished anchor,
        # and scope replacement all veto launch. No old process is signalled.
        static_binding(plan)
        rows = inspect_nodes(old, original)
        validate_observations(rows, plan)
        choose_pilots(rows, nodes)
        latest = scope_snapshot(snapshot)
        tree_proof = verify_tree_anchor(rows, snapshot)
        if latest['scopes'] != snapshot['scopes']:
            raise ValueError('frontier changed after freeze; inspect the unstarted pilot')
        static_binding(plan)
        replacement_binding(new_run, plan, snapshot['scopes'], busy)
        rm24.atomic(work/'anchor-tree-proof.json', tree_proof)
        subprocess.run([sys.executable, '-S', '-B', '-u', str(new_run/'rm24.py'),
                        'start', '--run', str(new_run)], check=True)
        ready = verify_workers.wait_workers_ready(new_run, nodes, timeout=timeout)
        rm24.atomic(work/'worker-readiness.json', ready)
        static_binding(plan, require_running=False)
        replacement_binding(new_run, plan, snapshot['scopes'], busy)
        upgrade.parent_readiness(ready, plan, nodes)
        status = ready['status']
        if ((new_run/'STOP').exists() or status.get('errors')
                or status.get('state') not in ('rm_running', 'retrying_failed_jobs', 'supplement_complete')
                or not -5 <= time.time()-status.get('updated', 0) <= 15):
            raise RuntimeError('pilot readiness is not a fresh successful state')
        rm24.atomic(work/'ready.json', ready)
        result = dict(ready=True, run=str(new_run), anchor=str(old), execution_nodes=nodes,
            rm_max=16, per_node=4, state=status['state'], cooperative_overlap=True,
            snapshot_is_not_lock=True, effective_speedup_not_yet_measured=True)
        print(json.dumps(result), flush=True)
        return result
    except BaseException as exc:
        rm24.atomic(work/'failure.json', dict(at=time.time(), error=repr(exc),
            replacement=str(new_run) if new_run else None,
            note='No old STOP/signals or main pointer changes. A pilot may be running; do not retry blindly.'))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'apply'), nargs='?', default='prepare')
    parser.add_argument('--nodes', nargs=4, metavar='ALIAS')
    parser.add_argument('--timeout', type=float, default=90)
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(PILOTS if args.nodes is None else args.nodes)
    else:
        if args.nodes is not None:
            parser.error('apply uses the exact prepared pilot aliases')
        apply(args.timeout)


if __name__ == '__main__':
    main()
