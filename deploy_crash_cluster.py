#!/usr/bin/env python3
"""Explicit c3559 recovery, excluding worker-9; no legacy launcher replay."""
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
import shlex
import socket
import stat
import subprocess
import sys
import time
import urllib.request
import uuid

from migration_io import read_json, write_json

JOB = 'c3559adf-8c4e-4a14-afa4-b998ad8c0a1b'
OLD = Path('/share-mixed/project/.hcr-copy-20261021T141055Z-overlap-2db42a')
IPS = {'master-0': '172.24.109.133', 'worker-0': '172.24.99.133',
       'worker-1': '172.24.229.197', 'worker-2': '172.24.76.197',
       'worker-3': '172.24.156.5', 'worker-4': '172.24.140.69',
       'worker-5': '172.24.236.70', 'worker-6': '172.24.27.69',
       'worker-7': '172.24.188.133', 'worker-8': '172.24.244.197',
       'worker-10': '172.24.140.133'}
MASTER = 'job-' + JOB + '-master-0'
RECORD = Path('/share-mixed/project/.hcr-copy-monitor/recovery-c3559.json')
GOOD = {'copied', 'skipped', 'verified', 'linked'}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def runtime_sources(root):
    pending = ['migration_crash_resume', 'migration_recovery_role', 'migration_broker_sharded',
               'migration_cached_worker', 'migration_probe_fast_open', 'migration_metrics',
               'migration_progress', 'migration_resume_index_parallel_memory', 'deploy_crash_cluster',
               'migration_run', 'migration_worker128_run', 'migration_linux_open']
    found = set()
    while pending:
        name = pending.pop()
        if name in found:
            continue
        path = root / (name + '.py')
        if not path.is_file():
            raise FileNotFoundError('missing recovery dependency: ' + str(path))
        found.add(name)
        for node in ast.walk(ast.parse(path.read_text())):
            names = ([node.module] if isinstance(node, ast.ImportFrom) else
                     [n.name for n in node.names] if isinstance(node, ast.Import) else [])
            pending.extend(n for n in names if n and n.startswith('migration_') and
                           (root / (n + '.py')).is_file())
    return sorted(n + '.py' for n in found)


def unfinished_tasks(old):
    stage = old / 'stages' / 'p3-r3'
    paths = []
    for kind in ('pending', 'running'):
        root = stage / kind
        for parent in [root, *(root / str(i) for i in range(64))]:
            try:
                with os.scandir(parent) as entries:
                    for item in entries:
                        if item.name.endswith('.json'):
                            if not item.is_file(follow_symlinks=False):
                                raise ValueError('non-regular queue record: ' + item.path)
                            paths.append(Path(item.path))
            except FileNotFoundError:
                if parent == root:
                    raise
    def consume(path):
        task = read_json(path)
        if (type(task.get('id')) is not int or task['id'] < 0 or
                (task.get('phase'), task.get('round')) != (3, 3) or
                not isinstance(task.get('entries'), list)):
            raise ValueError('invalid predecessor warm task')
        done = stage / 'done' / str(task['id'] % 64) / (str(task['id']) + '.json')
        try:
            receipt = read_json(done)
        except FileNotFoundError:
            return task
        if (receipt.get('id') != task['id'] or
                [r['rel'] for r in receipt['results']] != [e['rel'] for e in task['entries']]):
            raise ValueError('predecessor receipt does not cover task')
        bad = {r['rel'] for r in receipt['results'] if r['status'] not in GOOD}
        return dict(task, entries=[e for e in task['entries'] if e['rel'] in bad])
    tasks, seen = {}, set()
    with ThreadPoolExecutor(max_workers=16) as pool:
        for task in pool.map(consume, sorted(paths)):
            previous = tasks.setdefault(task['id'], task)
            if previous != task:
                raise ValueError('conflicting predecessor task ID')
    rows = []
    for identity, task in sorted(tasks.items()):
        entries = []
        for original in task['entries']:
            rel = original['rel']
            from migration_worker import _components
            _components(rel)
            if rel in seen:
                continue
            if rel == 'home/hcr' or rel.startswith('home/hcr/') or rel == 'repos/cosmos-framework-pwm' or rel.startswith('repos/cosmos-framework-pwm/'):
                raise ValueError('warm recovery task escaped priority3')
            seen.add(rel)
            entries.append({k: v for k, v in original.items() if k != 'known'})
        if entries:
            rows.append({'origin': {'control': str(old), 'stage': 'p3-r3', 'task_id': identity},
                         'task': dict(task, entries=entries)})
    return rows, {'queue_records': len(paths), 'unique_tasks': len(tasks),
                  'unfinished_tasks': len(rows), 'unfinished_paths': len(seen)}


def prepare():
    if RECORD.exists():
        raise RuntimeError('recovery already planned; inspect its record instead of preparing twice')
    cfg = read_json(OLD / 'config.json')
    if (cfg.get('job') != '1e56d871-526b-4036-950b-d11340c193ba' or
            (cfg.get('source'), cfg.get('destination')) !=
            ('/share/project/eai_pwm', '/share-mixed/project/eai_pwm')):
        raise ValueError('wrong predecessor scope')
    snapshots = [read_json(p) for folder in ('nodes', 'nodes-extra') for p in (OLD / folder).glob('*.json')]
    ages = [time.time() - datetime.datetime.fromisoformat(n['updated']).timestamp() for n in snapshots]
    if len(snapshots) != 24 or min(ages) < 180:
        raise RuntimeError('predecessor still has a recent/unknown writer; recovery refused')
    from migration_progress import snapshot, copy_work_progress
    baseline = copy_work_progress(snapshot(OLD))
    stamp = datetime.datetime.now(datetime.timezone.utc)
    run_id = 'hcr-copy-' + stamp.strftime('%Y%m%dT%H%M%SZ') + '-c3559-' + uuid.uuid4().hex[:8]
    control = OLD.parent / ('.' + run_id)
    proof = {'confirmed': True, 'old_control': str(OLD),
             'method': 'user-reported-cluster-failure+stale-heartbeats+shared-STOP-fence',
             'statement': '用户报告旧12节点挂掉，并授权新11节点恢复复制',
             'old_platform_deletion_independently_verified': False,
             'youngest_old_heartbeat_seconds': min(ages), 'recorded': stamp.isoformat()}
    # A recovered old process must see STOP; never delete/replace the old DB,
    # queue, receipts, hostname, active manifest or destination data.
    try:
        fd = os.open(OLD / 'STOP', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        if not stat.S_ISREG((OLD / 'STOP').lstat().st_mode):
            raise ValueError('predecessor STOP is not a regular file')
    else:
        with os.fdopen(fd, 'w') as handle:
            json.dump(proof, handle)
            handle.flush()
            os.fsync(handle.fileno())
    control.mkdir(mode=0o700)
    write_json(RECORD, {'control': str(control), 'run_id': run_id, 'job': JOB,
                       'status': 'preparing', 'authorization': proof})
    source = Path(__file__).resolve().parent
    for name in runtime_sources(source):
        import shutil
        shutil.copyfile(source / name, control / name)
    from migration_small_probe_delegate import SCOPE
    write_json(control / 'probe-policy.json', {'scope': SCOPE, 'max_file_bytes': 1048576,
        'min_missing': 16, 'recheck_every': 64, 'max_directories': 4096})
    write_json(control / 'worker-directory-policy.json', {'scope': 'native-safe-parent-open-v1'})
    hosts = ['job-' + JOB + '-' + n for n in IPS]
    cfg.update(run_id=run_id, control=str(control), job=JOB, ssh_owner='ybai',
        created=stamp.isoformat(), resume_from=str(OLD), predecessor_isolation=proof,
        crash_recovery_v1=True, copy_hosts=hosts, master_host=MASTER,
        node_addresses={h: ip for h, ip in zip(hosts, IPS.values())},
        threads=64, copy_threads_per_node=128, supplement_threads=64,
        io_bytes_per_second=10_000_000_000, supplement_io_bytes_per_second=10_000_000_000,
        broker_url='http://' + IPS['master-0'] + ':37651', broker_token=secrets.token_hex(32),
        broker_fs_concurrency=64, parallel_scan_threads=128, source_scan_processes=8,
        source_scan_threads_per_process=16, scan_entries_per_second=0, scan_batch_files=64,
        resume_read_workers=32, resume_strict_enumeration=True,
        distributed_target_threads=512, target_filter_inflight=4096,
        metadata_client_processes=8, metadata_client_batch_size=64, metadata_batch_http=True,
        target_probe_endpoints=['http://' + ip + ':40561' for ip in IPS.values()],
        training_watchdog_enabled=False, warm_scan_missing=True)
    write_json(control / 'config.json', cfg)
    for folder in ('nodes', 'nodes-extra', 'stages', 'manifests', 'recovery-roles', 'probe-roles'):
        (control / folder).mkdir(mode=0o700)
    write_json(control / 'state.json', {'run_id': run_id, 'status': 'starting', 'stage': None,
        'started': stamp.isoformat(), 'updated': stamp.isoformat(), 'workers_expected': 11,
        'threads_per_node': 128, 'recovery_warm': True, 'resumed_from': str(OLD)})
    write_json(control / 'recovery-baseline.json', {'control': str(control), 'run_id': run_id,
        'resume_from': str(OLD), 'captured': stamp.isoformat(), 'work': baseline,
        'scope': 'recorded-write-counters-and-probe-observations-not-unique-paths'})
    plan = copy.deepcopy(read_json(OLD / 'extension-plan.json'))
    if plan.get('status') != 'queued' or plan.get('exclude') != ['eai_pwm']:
        raise RuntimeError('priority4 plan requires explicit recovery')
    plan['depends_on']['control'] = str(control)
    plan['covered_by_controls'] = list(dict.fromkeys(plan.get('covered_by_controls', []) + [str(OLD), str(control)]))
    plan['covered_by'] = {'priorities': [1, 2, 3], 'controls': plan['covered_by_controls']}
    write_json(control / 'extension-plan.json', plan)
    rows, summary = unfinished_tasks(OLD)
    with open(control / 'warm-tasks.jsonl', 'x', encoding='utf-8') as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + '\n')
        handle.flush()
        os.fsync(handle.fileno())
    write_json(control / 'warm-queue-import.json', summary)
    names = [p.name for p in control.glob('*.py')] + ['probe-policy.json', 'worker-directory-policy.json']
    write_json(control / 'source-sha256.json', {n: digest(control / n) for n in names})
    result = {'control': str(control), 'run_id': run_id, 'job': JOB, 'status': 'prepared',
              'authorization': proof, 'warm_queue': summary, 'excluded_nodes': ['worker-9']}
    write_json(RECORD, result)
    return result


def verify(control):
    for name, expected in read_json(control / 'source-sha256.json').items():
        if Path(name).name != name or (control / name).is_symlink() or digest(control / name) != expected:
            raise ValueError('recovery source digest mismatch: ' + name)
    cfg = read_json(control / 'config.json')
    if cfg.get('job') != JOB or set(cfg.get('copy_hosts', [])) != {'job-' + JOB + '-' + n for n in IPS}:
        raise ValueError('recovery scope/host allowlist differs')
    if not (OLD / 'STOP').is_file() or (control / 'STOP').exists():
        raise RuntimeError('predecessor fence lost or new STOP present')
    return cfg


def internal_ssh(node, command):
    if node not in IPS or node == 'master-0':
        raise ValueError('only the ten explicitly allowed workers may use SSH')
    return ['ssh', '-F', '/dev/null', '-o', 'IdentityAgent=none', '-o', 'BatchMode=yes',
        '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=8', '-o', 'StrictHostKeyChecking=yes',
        '-o', 'HostKeyAlias=copy-c3559-' + node, '-o',
        'UserKnownHostsFile=/share-mixed/project/eai_pwm/home/hcr/.ssh/copy-node-bridge-20261023-jobc3559/known_hosts',
        '-i', '/share-mixed/project/eai_pwm/home/hcr/.ssh/id_rsa', 'root@' + IPS[node], command]


def start(control):
    cfg = verify(control)
    if socket.gethostname() != MASTER:
        raise RuntimeError('start must execute on the actual new master')
    from migration_recovery_role import launch_role
    with open('/tmp/' + cfg['run_id'] + '-deployment.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def launch(node, roles):
            if node == 'master-0':
                return [launch_role(control, role) for role in roles]
            code = ('import sys,pathlib,json;C=pathlib.Path(' + repr(str(control)) + ');'
                    'sys.path.insert(0,str(C));from deploy_crash_cluster import verify;verify(C);'
                    'from migration_recovery_role import launch_role;'
                    'print(json.dumps([launch_role(C,r) for r in ' + repr(roles) + ']))')
            result = subprocess.run(internal_ssh(node, shlex.join(['python3', '-c', code])),
                                    text=True, capture_output=True, timeout=75)
            if result.returncode:
                raise RuntimeError(node + ' role RPC failed; inspect before retry: ' + result.stderr[-1500:])
            return json.loads(result.stdout)
        outcomes = {}
        outcomes['master-services'] = launch('master-0', ['broker', 'metrics'])
        write_json(control / 'cluster-startup.json', {'status': 'starting', 'outcomes': outcomes})
        for roles in (['probe'], ['primary', 'supplement']):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {node: pool.submit(launch, node, roles) for node in IPS}
                for node, future in futures.items():
                    values = future.result()
                    for value in values:
                        outcomes[node + ':' + value['role']] = value
                    write_json(control / 'cluster-startup.json', {'status': 'starting', 'outcomes': outcomes})
                    print(json.dumps({'node': node, 'roles': roles, 'pids': [v['pid'] for v in values]}), flush=True)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for endpoint in cfg['target_probe_endpoints']:
            request = urllib.request.Request(endpoint + '/health', headers={'Authorization': 'Bearer ' + cfg['broker_token']})
            with opener.open(request, timeout=10) as stream:
                health = json.load(stream)
            if health.get('ok') is not True or health.get('control') != str(control):
                raise RuntimeError('new probe is not healthy')
        outcomes['master-producer'] = launch('master-0', ['producer'])
        write_json(control / 'cluster-startup.json', {'status': 'launched', 'outcomes': outcomes})
        write_json('/share-mixed/project/.hcr-copy-monitor/active.json',
                   {'control': str(control), 'run_id': cfg['run_id'], 'job': JOB, 'ssh_owner': 'ybai'})
        return {'control': str(control), 'status': 'launched', 'copy_processes': 22,
                'probe_processes': 11, 'excluded_nodes': ['worker-9']}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=('prepare', 'start'))
    p.add_argument('--control')
    a = p.parse_args()
    result = prepare() if a.action == 'prepare' else start(Path(a.control).resolve())
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
