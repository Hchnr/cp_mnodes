#!/usr/bin/env python3
"""Audited recovery of the confirmed-dead NOFILE producer, not a restart loop.

Reuse the immutable assignment prefix, append only proven complete new claims,
and preserve the failed full manifest. Workers, broker and training are never
signalled. A persistent attempt marker prevents blind duplicate launches.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import sys
import time
import uuid
import fcntl

import deploy_pipelined_producer as base

CONTROL = Path('/share-mixed/project/.hcr-copy-20261021T141055Z-overlap-2db42a')
FAILED_BUNDLE = 'pipelined-producer-4c9e3d4636794a9cb26f9e3a886793f4'
FAILED_PID = 859597
FAILED_START = '506760596'
ENTRY = 'migration_priority_feed_target_filter.py'
FILES = (*base.BUNDLE_FILES, ENTRY, 'deploy_pipelined_producer.py',
         'recover_paused_producer.py')


def claim_delta(db, start, stop):
    """Prove the whole integer suffix, not merely its maximum ID."""
    rows = db.execute('SELECT id,status,payload FROM claims WHERE stage=? AND id>=? ORDER BY id',
                      ('p3-r3', start))
    expected = start
    for task_id, status, payload in rows:
        if task_id != expected or task_id >= stop or status != 'done':
            raise RuntimeError('assignment suffix has a hole, new ID or unfinished lease')
        task = json.loads(payload)
        if task.get('id') != task_id:
            raise RuntimeError('claim ID/payload mismatch')
        yield base.task_bytes(task) + b'\n'
        expected += 1
    if expected != stop:
        raise RuntimeError('assignment suffix does not cover every reserved ID')


def prefix_copy(source, output, expected_sha, expected_bytes):
    digest = hashlib.sha256()
    length = 0
    with open(source, 'rb') as handle:
        while True:
            piece = handle.read(8 * 1024 * 1024)
            if not piece:
                break
            digest.update(piece)
            length += len(piece)
            output.write(piece)
    if digest.hexdigest() != expected_sha or length != expected_bytes:
        raise RuntimeError('immutable original assignment prefix changed')
    return digest, length


def checked(control, read_json):
    cfg = read_json(control / 'config.json')
    if (cfg.get('control') != str(control) or socket.gethostname() != 'job-' + cfg['job'] + '-master-0'
            or (cfg['source'], cfg['destination']) != ('/share/project/eai_pwm', '/share-mixed/project/eai_pwm')):
        raise RuntimeError('unexpected host/control/scope')
    if (control / 'STOP').exists() or read_json(control / 'pipeline.json').get('stages'):
        raise RuntimeError('STOP or formal pipeline blocks this recovery')
    state = read_json(control / 'state.json')
    role = read_json(control / 'pipeline-deployment.json')
    old = base.role_identity(role)
    if (old['pid'] != FAILED_PID or old['starttime_ticks'] != FAILED_START
            or role['bundle'] != str(control / FAILED_BUNDLE)):
        raise RuntimeError('failed producer role changed; do not recover another process')
    actual = base.process_identity(old['pid'])
    if actual and not (actual['starttime_ticks'] == old['starttime_ticks'] and actual['state'] in ('Z', 'X')):
        raise RuntimeError('producer is not positively confirmed dead')
    if (state.get('status') != 'producer_paused' or not state.get('recovery_warm')
            or state.get('stage') != 'p3-r3' or '[Errno 24]' not in state.get('producer_error', '')):
        raise RuntimeError('not the expected NOFILE failure')
    failure = read_json(Path(state['task_publication']['failure_record']))
    if failure.get('unconfirmed') or failure.get('failures') or state['task_publication'].get('failed'):
        raise RuntimeError('uncertain task publication requires a different recovery')
    for shard in range(64):
        with os.scandir(control / 'stages/p3-r3/pending' / str(shard)) as entries:
            if any(e.name.endswith('.json') for e in entries):
                raise RuntimeError('pending tasks are not drained')
    with os.scandir(control / 'stages/p3-r3/running') as entries:
        if any(e.name.endswith('.json') for e in entries):
            raise RuntimeError('running tasks remain')
    broker = base.process_identity(166974)
    training = base.process_identity(97245)
    if not broker or broker['starttime_ticks'] != '500376938' or broker['state'] in ('Z', 'X'):
        raise RuntimeError('original broker identity changed')
    if not training or training['starttime_ticks'] != '500250835' or training['state'] in ('Z', 'X'):
        raise RuntimeError('original training identity changed')
    base.broker_control(cfg, 30)
    handover = read_json(control / FAILED_BUNDLE / 'handover.json')
    checkpoint = read_json(control / FAILED_BUNDLE / 'frozen-checkpoint.json')
    start, stop = handover['next_task_id'], state['next_task_id']
    if stop - start != state['task_publication']['published_since_handover']:
        raise RuntimeError('new task reservation/publication counts disagree')
    if checkpoint['ledger']['tasks'] != start or checkpoint['ledger']['maximum_id'] != start - 1:
        raise RuntimeError('original immutable ledger is not a proven complete prefix')
    manifest = base.within_control(state['active_manifest'], control)
    if state.get('recovery_replay', {}).get('phase') != 'fresh_scan':
        raise RuntimeError('failed manifest was not a completed inventory replay')
    return cfg, state, role, old, handover, checkpoint, broker, training


def remote(bundle):
    control = CONTROL
    sys.path.append(str(control))
    from migration_io import read_json, write_json
    from migration_pipelined_target_filter import ensure_producer_nofile
    cfg, state, role, old, prior, checkpoint, broker, training = checked(control, read_json)
    locks = []
    for suffix in ('-parallel-target-dispatch.lock', '-cache-age-dispatch.lock', '-nofile-recovery.lock'):
        lock = open('/tmp/' + cfg['run_id'] + suffix, 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    base.wait_lock_released('/tmp/' + cfg['run_id'] + '-coordinator.lock', 1)
    limits = ensure_producer_nofile()
    expected_hashes = read_json(bundle / 'source-sha256.json')['files']
    for name, digest in expected_hashes.items():
        if hashlib.sha256((bundle / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError('recovery source changed: ' + name)
    record = {'version': 1, 'operation': bundle.name.split('-')[-1], 'status': 'preparing',
              'control': str(control), 'bundle': str(bundle), 'old_producer': old,
              'producer_nofile': list(limits), 'started': base.now(),
              'workers_broker_training_changed': False}
    base.durable_json(bundle / 'deployment.json', record, write_json)
    db = base.claims_reader(Path('/tmp') / (cfg['run_id'] + '-broker.sqlite'))
    start, stop = prior['next_task_id'], state['next_task_id']
    count, maximum = db.execute('SELECT COUNT(*),MAX(id) FROM claims WHERE stage=?', ('p3-r3',)).fetchone()
    if count != stop or maximum != stop - 1:
        raise RuntimeError('broker task set differs from the proven complete range')
    ledger = bundle / 'assigned-tasks.jsonl'
    with open(ledger, 'xb') as out:
        digest, length = prefix_copy(prior['assigned_tasks_path'], out,
                                     prior['ledger_sha256'], checkpoint['ledger']['bytes'])
        for line in claim_delta(db, start, stop):
            out.write(line)
            digest.update(line)
            length += len(line)
        out.flush()
        os.fsync(out.fileno())
    db.close()
    # Add the preceding run's observations to its historical baseline without
    # modifying current endpoint rates or pretending observations are unique.
    frozen_state = dict(state)
    historical = checkpoint['frozen_state']['distributed_target_probe']['counts'].get('remote_existing', 0)
    probe = dict(state['distributed_target_probe'])
    probe['counts'] = dict(probe['counts'])
    probe['counts']['remote_existing'] = historical + probe['counts'].get('remote_existing', 0)
    frozen_state['distributed_target_probe'] = probe
    handover = dict(prior)
    handover.update(old_producer=old, operation=record['operation'], created=base.now(),
                    assigned_tasks_path=str(ledger), ledger_sha256=digest.hexdigest(),
                    missing_manifest=state['active_manifest'], next_task_id=stop,
                    source_code_sha256=expected_hashes[ENTRY], preserved_task_count=stop,
                    recovery_cause='NOFILE startup limit omission', predecessor_bundle=role['bundle'])
    handover_path = bundle / 'handover.json'
    base.durable_json(handover_path, handover, write_json)
    record.update(status='frozen_checkpoint', handover=handover, frozen_state=frozen_state,
                  ledger={'tasks': stop, 'maximum_id': stop - 1, 'bytes': length, 'sha256': digest.hexdigest()},
                  preserved_prefix_tasks=start, appended_tasks=stop-start)
    base.durable_json(bundle / 'frozen-checkpoint.json', record, write_json)
    checked(control, read_json)
    base.require_identity(broker)
    base.require_identity(training)
    base.wait_lock_released('/tmp/' + cfg['run_id'] + '-coordinator.lock', 1)
    marker = control / FAILED_BUNDLE / 'nofile-recovery-start-once.json'
    with open(marker, 'xb') as out:
        out.write((json.dumps({'bundle': str(bundle), 'created': base.now()}) + '\n').encode())
        out.flush()
        os.fsync(out.fileno())
    argv = [sys.executable, '-u', str(bundle / ENTRY), 'run', '--control', str(control),
            '--handover', str(handover_path), '--publish-workers', '8', '--publish-limit', '64']
    record.update(status='launching_once', launch_argv=argv)
    base.durable_json(bundle / 'deployment.json', record, write_json)
    env = dict(os.environ, PYTHONPATH=str(bundle)+os.pathsep+str(control),
               PYTHONPYCACHEPREFIX='/tmp/'+cfg['run_id']+'-nofile-'+record['operation'])
    with open(bundle / 'producer.log', 'ab', buffering=0) as out:
        child = subprocess.Popen(['nice', '-n', '15', 'ionice', '-c', '3', *argv],
                                 stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                                 env=env, start_new_session=True, close_fds=True)
    for _ in range(200):
        actual = base.process_identity(child.pid)
        if actual and actual['state'] not in ('Z', 'X') and actual['argv'] == argv:
            break
        if child.poll() is not None:
            raise RuntimeError('recovery producer exited; do not retry launch')
        time.sleep(.05)
    else:
        raise RuntimeError('launch outcome uncertain; do not retry')
    limit_lines = (Path('/proc') / str(child.pid) / 'limits').read_text().splitlines()
    limit_line = next(line for line in limit_lines if line.startswith('Max open files'))
    if int(limit_line.split()[3]) < 65536:
        raise RuntimeError('actual child NOFILE was not raised')
    new_role = dict(role)
    new_role.update(pid=actual['pid'], startticks=actual['starttime_ticks'], argv=argv,
                    bundle=str(bundle), log=str(bundle/'producer.log'), handover=str(handover_path),
                    deployment_operation=record['operation'], producer_nofile=list(limits))
    write_json(control / 'pipeline-deployment.json', new_role)
    write_json(control / 'recovery-roles' / (new_role['host']+'-producer.json'), new_role)
    record.update(status='starting_recovery', producer=new_role, actual_nofile_line=limit_line)
    base.durable_json(bundle / 'deployment.json', record, write_json)
    print(json.dumps({'status':record['status'],'producer':new_role,'preserved_tasks':stop,
                      'appended_tasks':stop-start,'nofile':limit_line}),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--remote', type=Path)
    args=parser.parse_args()
    if args.remote:
        remote(args.remote.resolve())
        return
    if not args.apply:
        parser.error('--apply is required; this tool never automatically retries')
    from migration_io import write_json
    bundle=CONTROL/('pipelined-producer-'+uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    hashes={}
    for name in FILES:
        raw=(Path(__file__).resolve().parent/name).read_bytes()
        with open(bundle/name,'xb') as out:
            out.write(raw);out.flush();os.fsync(out.fileno())
        (bundle/name).chmod(0o444)
        hashes[name]=hashlib.sha256(raw).hexdigest()
    write_json(bundle/'source-sha256.json',{'files':hashes})
    print(json.dumps({'bundle':str(bundle),'status':'prepared'}),flush=True)
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15',
                           '-o','ServerAliveInterval=15','copy-master-0','python3',
                           str(bundle/'recover_paused_producer.py'),'--remote',str(bundle)])
    if result.returncode:
        raise RuntimeError('recovery attempt failed/uncertain; inspect bundle, never blindly retry')


if __name__ == '__main__':
    main()
