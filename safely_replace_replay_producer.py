#!/usr/bin/env python3
"""One-shot correction of the fixed, still-unproductive replay producer.

Only predecessor pipelined-producer-9e35845dfd4f492f93cb961a0503b1a1 is
accepted. Its complete immutable ledger/manifests are reused, never rebuilt.
Any new task, any changed process identity or any evidence of new source scan
aborts. Default is read-only inspection; only --apply sends producer signals.
No worker, broker or training process is ever signalled or drained.
"""
import argparse
import base64
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import stat
import subprocess
import sys
import time
import traceback
import uuid

import deploy_pipelined_producer as base


PREDECESSOR = 'pipelined-producer-9e35845dfd4f492f93cb961a0503b1a1'
PREDECESSOR_PID = 3660953
PREDECESSOR_START = '506620646'
ENTRY = 'migration_priority_feed_target_filter.py'
FILES = (*base.BUNDLE_FILES, ENTRY, 'deploy_pipelined_producer.py',
         'safely_replace_replay_producer.py')


def file_sha256(path):
    digest = hashlib.sha256()
    length = 0
    with open(path, 'rb', buffering=0) as handle:
        while True:
            piece = handle.read(8 * 1024 * 1024)
            if not piece:
                break
            digest.update(piece)
            length += len(piece)
    return digest.hexdigest(), length


def zero_publication(state, next_id):
    if (state.get('stage') != 'p3-r3' or state.get('recovery_warm') is not True
            or state.get('status') != 'scanning' or state.get('scan_complete')
            or state.get('next_task_id') != next_id):
        raise RuntimeError('predecessor no longer has the original unfinished replay reservation')
    publication = state.get('task_publication', {})
    if (publication.get('published_since_handover') != 0 or publication.get('pending') != 0
            or publication.get('next_task_id') != next_id or publication.get('failed') is not False
            or publication.get('workers') != 8 or publication.get('limit') != 64):
        raise RuntimeError('publication occurred, failed, or is in flight; correction is forbidden')
    target_filter = state.get('target_filter', {})
    if type(target_filter.get('missing')) is not int or target_filter['missing'] != 0:
        raise RuntimeError('predecessor found missing work; correction is forbidden')
    probe = state.get('distributed_target_probe', {}).get('counts', {})
    if probe.get('remote_missing', 0) != 0 or probe.get('local_missing', 0) != 0:
        raise RuntimeError('predecessor has missing probe results; correction is forbidden')


def wait_all_threads_frozen(identity, timeout=30):
    """Do not abandon a possible task writer while a syscall is still live."""
    deadline = time.monotonic() + timeout
    previous = None
    while time.monotonic() < deadline:
        base.require_identity(identity, states={'T', 't'})
        tasks = Path('/proc') / str(identity['pid']) / 'task'
        observed = []
        try:
            for path in tasks.iterdir():
                fields = (path / 'stat').read_text().rsplit(') ', 1)[1].split()
                observed.append((int(path.name), fields[0]))
        except FileNotFoundError:
            continue
        if observed and all(state in ('T', 't') for _, state in observed):
            current = tuple(sorted(tid for tid, _ in observed))
            if current == previous:
                return {'threads': len(observed), 'all_stopped': True,
                        'matching_samples': 2}
            previous = current
        else:
            previous = None
        time.sleep(.1)
    raise RuntimeError('not every replay thread stopped; no replacement attempted')


def descriptor_proof(identity, *, old_missing, active_manifest, source):
    """Prove it is still streaming old records, not walking the source tree."""
    base.require_identity(identity)
    old_missing, active_manifest, source = map(lambda p: str(Path(p).resolve()),
                                              (old_missing, active_manifest, source))
    old_reads, replay_writes, source_fds, source_anchors = [], [], [], []
    root = Path('/proc') / str(identity['pid'])
    for descriptor in (root / 'fd').iterdir():
        try:
            target = os.readlink(descriptor)
            info = dict(line.split(':', 1) for line in
                        (root / 'fdinfo' / descriptor.name).read_text().splitlines() if ':' in line)
            flags = int(info['flags'].strip(), 8)
        except (FileNotFoundError, ProcessLookupError):
            continue
        row = {'fd': int(descriptor.name), 'path': target, 'flags': flags,
               'position': int(info.get('pos', '0').strip())}
        if target == old_missing:
            if flags & os.O_ACCMODE != os.O_RDONLY:
                raise RuntimeError('old full manifest is not open read-only')
            old_reads.append(row)
        if target == active_manifest:
            if flags & os.O_ACCMODE not in (os.O_WRONLY, os.O_RDWR):
                raise RuntimeError('replay manifest is not open for output')
            replay_writes.append(row)
        if target == source or target.startswith(source + '/'):
            row['is_directory'] = stat.S_ISDIR(descriptor.stat().st_mode)
            if (target == source and row['is_directory']
                    and flags & os.O_ACCMODE == os.O_RDONLY):
                # scan_phase opens its root anchor BEFORE iterating
                # resume_records; this is not evidence of a directory walk.
                source_anchors.append(row)
            else:
                source_fds.append(row)
    threads = len(list((root / 'task').iterdir()))
    if (not old_reads or not replay_writes or source_fds or len(source_anchors) > 2
            or threads > 257):
        raise RuntimeError('cannot prove old-manifest-only replay (source scan may have started)')
    return {'old_manifest_reads': old_reads, 'replay_manifest_writes': replay_writes,
            'source_fds': source_fds, 'source_root_anchors': source_anchors,
            'thread_count': threads}


def task_fence(control, cfg, next_id):
    db = base.claims_reader(Path('/tmp') / (cfg['run_id'] + '-broker.sqlite'))
    try:
        stages = [row[0] for row in db.execute('SELECT DISTINCT stage FROM claims')]
        if any(stage != 'p3-r3' for stage in stages):
            raise RuntimeError('unexpected formal-stage claim')
        maximum = db.execute('SELECT MAX(id) FROM claims WHERE stage=?', ('p3-r3',)).fetchone()[0]
    finally:
        db.close()
    if maximum is not None and maximum >= next_id:
        raise RuntimeError('broker has a new task beyond the reused ledger')
    pending_seen = 0
    root = control / 'stages/p3-r3/pending'
    for shard in range(64):
        directory = root / str(shard)
        if not directory.is_dir():
            raise RuntimeError('missing pending shard')
        with os.scandir(directory) as entries:
            for entry in entries:
                if re.fullmatch(r'[0-9]+\.json', entry.name):
                    task_id = int(entry.name[:-5])
                    if task_id % 64 != shard or task_id >= next_id:
                        raise RuntimeError('new or malformed pending task beyond ledger fence')
                    pending_seen += 1
    # All new tasks remain pending through durable claims commit. Check claims
    # again AFTER enumeration, covering a task which moved during the scan.
    db = base.claims_reader(Path('/tmp') / (cfg['run_id'] + '-broker.sqlite'))
    try:
        after = db.execute('SELECT MAX(id) FROM claims WHERE stage=?', ('p3-r3',)).fetchone()[0]
    finally:
        db.close()
    if after is not None and after >= next_id:
        raise RuntimeError('new task was claimed while scanning pending fence')
    return {'next_task_id': next_id, 'broker_maximum_id': after, 'pending_seen': pending_seen}


def preflight(control, read_json, timeout):
    cfg = read_json(control / 'config.json')
    if (cfg.get('control') != str(control)
            or (cfg.get('source'), cfg.get('destination')) != (
                '/share/project/eai_pwm', '/share-mixed/project/eai_pwm')
            or not re.fullmatch(r'[A-Za-z0-9_.-]{1,180}', cfg['run_id'])):
        raise RuntimeError('unexpected control, source, destination or run ID')
    bundle = control / PREDECESSOR
    role = read_json(control / 'pipeline-deployment.json')
    old = base.role_identity(role)
    if (old['pid'] != PREDECESSOR_PID or old['starttime_ticks'] != PREDECESSOR_START
            or role.get('bundle') != str(bundle)
            or role.get('entrypoint') != 'migration_pipelined_target_filter.py'
            or role.get('handover') != str(bundle / 'handover.json')
            or role.get('host') != socket.gethostname()
            or socket.gethostname() != 'job-' + cfg['job'] + '-master-0'):
        raise RuntimeError('only the fixed saved replay predecessor may be corrected')
    argv = [sys.executable, '-u', str(bundle / 'migration_pipelined_target_filter.py'),
            'run', '--control', str(control), '--handover', str(bundle / 'handover.json'),
            '--publish-workers', '8', '--publish-limit', '64']
    if old['argv'] != argv:
        raise RuntimeError('predecessor full argv differs from the approved replay command')
    base.require_identity(old)
    handover = read_json(bundle / 'handover.json')
    checkpoint = read_json(bundle / 'frozen-checkpoint.json')
    deployment = read_json(bundle / 'deployment.json')
    source_hashes = read_json(bundle / 'source-sha256.json')['files']
    if (handover.get('control') != str(control) or handover.get('stage') != 'p3-r3'
            or checkpoint.get('handover') != handover
            or deployment.get('handover') != handover
            or deployment.get('status') != 'starting_replay'
            or base.role_identity(deployment['producer']) != old):
        raise RuntimeError('predecessor original handover/checkpoint/deployment disagree')
    for name in base.BUNDLE_FILES:
        actual, _ = file_sha256(bundle / name)
        if actual != source_hashes.get(name):
            raise RuntimeError('predecessor runtime code changed: ' + name)
    if source_hashes['migration_pipelined_target_filter.py'] != handover['source_code_sha256']:
        raise RuntimeError('predecessor entry SHA differs from original handover')
    ledger = base.within_control(handover['assigned_tasks_path'], control)
    if (ledger != bundle / 'assigned-tasks.jsonl'
            or handover['ledger_sha256'] != checkpoint['ledger']['sha256']
            or ledger.stat().st_size != checkpoint['ledger']['bytes']):
        raise RuntimeError('predecessor immutable ledger metadata changed')
    warm = base.within_control(handover['warm_manifest'], control)
    missing = base.within_control(handover['missing_manifest'], control)
    digest, _ = file_sha256(bundle / 'handover.json')
    active = control / 'manifests' / ('recovery-missing-parallel-' + digest[:24] + '.jsonl')
    state = base.guarded_state(control, read_json)
    zero_publication(state, handover['next_task_id'])
    if state.get('active_manifest') != str(active) or state.get('foreground_handover') != str(bundle / 'handover.json'):
        raise RuntimeError('active manifest is not the fixed predecessor replay adoption')
    seed = handover['seed_helper']
    if read_json(control / 'overlap-seed-helper-identity.json') != seed:
        raise RuntimeError('seed helper record changed')
    if not base.same_identity(base.process_identity(seed['pid']), base.role_identity(seed)):
        base.within_control(seed['result_path'], control)
    broker_record = read_json(control / 'broker.json')
    broker = base.process_identity(broker_record['pid'])
    if (not broker or broker['state'] in ('Z', 'X')
            or not any(arg.endswith('/migration_broker_parallel.py') for arg in broker['argv'])):
        raise RuntimeError('original parallel broker is not alive')
    health = read_json(control / 'training-health.json')
    if health.get('state') != 'watching' or not health.get('training_pids'):
        raise RuntimeError('training watchdog no longer watching')
    training = []
    for pid in health['training_pids']:
        identity = base.process_identity(int(pid))
        if not identity or identity['state'] in ('Z', 'X'):
            raise RuntimeError('baseline training launcher exited')
        training.append(identity)
    proof = descriptor_proof(old, old_missing=missing, active_manifest=active, source=cfg['source'])
    return {'captured': base.now(), 'control': str(control), 'old_producer': old,
            'old_role': role, 'original_handover': handover, 'original_checkpoint': checkpoint,
            'ledger': str(ledger), 'ledger_expected_sha256': handover['ledger_sha256'],
            'ledger_bytes': checkpoint['ledger']['bytes'], 'broker': broker,
            'training': training, 'state': state, 'descriptor_proof': proof,
            'broker_control': base.broker_control(cfg, timeout),
            'task_fence': task_fence(control, cfg, handover['next_task_id'])}, cfg


def cumulative_state(original, replay):
    """Preserve original totals and add the discarded replay's check work."""
    result = copy.deepcopy(original)
    if (original.get('stage'), original.get('run_id')) != (replay.get('stage'), replay.get('run_id')):
        raise RuntimeError('counter baseline belongs to another run/stage')
    old_probe = result.setdefault('distributed_target_probe', {})
    new_probe = replay.get('distributed_target_probe', {})
    for group in ('counts',):
        target = old_probe.setdefault(group, {})
        for key, value in new_probe.get(group, {}).items():
            if type(value) is not int or value < 0:
                raise ValueError('invalid replay probe counter')
            target[key] = int(target.get(key, 0)) + value
    endpoints = old_probe.setdefault('endpoints', {})
    for endpoint, values in new_probe.get('endpoints', {}).items():
        target = endpoints.setdefault(endpoint, {})
        for key, value in values.items():
            if type(value) is not int or value < 0:
                raise ValueError('invalid replay endpoint counter')
            target[key] = int(target.get(key, 0)) + value
    target_filter = result.setdefault('target_filter', {})
    for key in ('submitted', 'checked', 'missing'):
        target_filter[key] = int(target_filter.get(key, 0)) + int(replay.get('target_filter', {}).get(key, 0))
    target_filter['peak_inflight'] = max(target_filter.get('peak_inflight', 0),
                                         replay.get('target_filter', {}).get('peak_inflight', 0))
    target_filter['inflight'] = replay.get('target_filter', {}).get('inflight', 0)
    result['counter_baseline_updated'] = base.now()
    return result


def make_bundle(control, payload, write_json):
    bundle = control / ('pipelined-producer-' + payload['operation'])
    bundle.mkdir(mode=0o700)
    if set(payload['files']) != set(FILES):
        raise RuntimeError('correction source allowlist differs')
    hashes = {}
    for name, record in payload['files'].items():
        raw = base64.b64decode(record['base64'], validate=True)
        if len(raw) > 4 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != record['sha256']:
            raise RuntimeError('invalid correction source SHA')
        path = bundle / name
        with open(path, 'xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        path.chmod(0o444)
        if file_sha256(path)[0] != record['sha256']:
            raise RuntimeError('deployed correction source SHA differs')
        hashes[name] = record['sha256']
    base.durable_json(bundle / 'source-sha256.json', {'files': hashes}, write_json)
    return bundle, hashes


def remote_operation(payload):
    control = Path(payload['control']).resolve()
    sys.path.insert(0, str(control))
    from migration_io import read_json, write_json
    evidence, cfg = preflight(control, read_json, payload['control_timeout'])
    if payload['mode'] == 'inspect':
        return {'status': 'inspected', 'evidence': evidence}
    locks = []
    for suffix in ('-parallel-target-dispatch.lock', '-cache-age-dispatch.lock', '-replay-correction.lock'):
        lock = open('/tmp/' + cfg['run_id'] + suffix, 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    bundle, hashes = make_bundle(control, payload, write_json)
    record = {'version': 1, 'operation': payload['operation'], 'status': 'preparing',
              'predecessor_bundle': str(control / PREDECESSOR), 'created': base.now(),
              'bundle': str(bundle), 'control': str(control), 'preflight': evidence,
              'code_sha256': hashes, 'termination_attempted': False,
              'old_producer_terminated': False, 'worker_broker_training_signalled': False,
              'drains_modified': False}
    record_path = bundle / 'deployment.json'
    write_json(record_path, record)
    old = evidence['old_producer']
    frozen, term_attempted, old_exited = False, False, False
    previous_handlers = {}

    def interrupted(signum, frame):
        raise RuntimeError('correction interrupted by signal ' + str(signum))

    for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, interrupted)
    try:
        entry = bundle / ENTRY
        env = dict(os.environ, PYTHONPATH=str(bundle) + os.pathsep + str(control),
                   PYTHONPYCACHEPREFIX='/tmp/' + cfg['run_id'] + '-priority-' + payload['operation'])
        with open(bundle / 'entry-help.log', 'xb') as handle:
            subprocess.run([sys.executable, str(entry), '--help'], env=env,
                           stdout=handle, stderr=subprocess.STDOUT, check=True, timeout=120)
        # Expensive hash verification happens while the predecessor remains
        # alive. Reuse the original ledger verbatim; no huge in-memory rebuild.
        ledger_sha, ledger_bytes = file_sha256(evidence['ledger'])
        if ledger_sha != evidence['ledger_expected_sha256'] or ledger_bytes != evidence['ledger_bytes']:
            raise RuntimeError('original complete immutable ledger SHA mismatch')
        record['verified_ledger'] = {'path': evidence['ledger'], 'bytes': ledger_bytes,
                                     'sha256': ledger_sha, 'verified': base.now()}
        latest, _ = preflight(control, read_json, payload['control_timeout'])
        if not base.same_identity(latest['old_producer'], old):
            raise RuntimeError('predecessor identity changed while hashing ledger')
        if base.require_identity(old)['state'] in ('T', 't'):
            raise RuntimeError('predecessor was already stopped by another operator')
        base.assert_protected_identities(evidence)
        record.update(status='freezing', freeze_requested=base.now())
        base.durable_json(record_path, record, write_json)
        # O_EXCL persistent attempt marker: a second apply is never a retry.
        marker = control / PREDECESSOR / 'replay-correction-attempt.json'
        with open(marker, 'xb') as handle:
            handle.write(json.dumps({'operation': payload['operation'], 'bundle': str(bundle),
                                     'created': base.now()}).encode('ascii') + b'\n')
            handle.flush()
            os.fsync(handle.fileno())
        base.require_identity(old)
        frozen = True
        os.kill(old['pid'], signal.SIGSTOP)
        base.wait_state(old, {'T', 't'}, 30)
        frozen_threads = wait_all_threads_frozen(old)
        frozen_evidence, _ = preflight(control, read_json, payload['control_timeout'])
        base.require_identity(old, states={'T', 't'})
        base.assert_protected_identities(evidence)
        original = evidence['original_handover']
        next_id = original['next_task_id']
        combined = cumulative_state(evidence['original_checkpoint']['frozen_state'],
                                    frozen_evidence['state'])
        handover = dict(original)
        handover.update(old_producer=old, created=base.now(), operation=payload['operation'],
                        source_code_sha256=hashes[ENTRY],
                        predecessor_bundle=str(control / PREDECESSOR),
                        discarded_replay_manifest=frozen_evidence['state']['active_manifest'],
                        counter_baseline_path=str(bundle / 'frozen-checkpoint.json'))
        handover_path = bundle / 'handover.json'
        base.durable_json(handover_path, handover, write_json)
        record.update(status='frozen_checkpoint', handover=handover, frozen_state=combined,
                      discarded_replay_state=frozen_evidence['state'],
                      frozen_proof=frozen_evidence['descriptor_proof'],
                      frozen_threads=frozen_threads,
                      task_fence=frozen_evidence['task_fence'],
                      ledger=evidence['original_checkpoint']['ledger'])
        base.durable_json(bundle / 'frozen-checkpoint.json', record, write_json)
        base.durable_json(record_path, record, write_json)
        # Last no-new-work barrier, immediately before the durable TERM intent.
        final, _ = preflight(control, read_json, payload['control_timeout'])
        base.require_identity(old, states={'T', 't'})
        base.assert_protected_identities(evidence)
        record.update(status='terminating_old', termination_attempted=True,
                      termination_requested=base.now())
        base.durable_json(record_path, record, write_json)
        term_attempted = True
        os.kill(old['pid'], signal.SIGTERM)
        os.kill(old['pid'], signal.SIGCONT)
        frozen = False
        base.wait_exited(old, 60)
        old_exited = True
        base.wait_lock_released('/tmp/' + cfg['run_id'] + '-coordinator.lock', 60)
        base.guarded_state(control, read_json)
        base.assert_protected_identities(evidence)
        base.broker_control(cfg, payload['control_timeout'])
        task_fence(control, cfg, next_id)
        argv = [sys.executable, '-u', str(entry), 'run', '--control', str(control),
                '--handover', str(handover_path), '--publish-workers', '8', '--publish-limit', '64']
        record.update(status='launching', launch_argv=argv, launch_requested=base.now())
        base.durable_json(record_path, record, write_json)
        log = bundle / 'producer.log'
        with open(log, 'ab', buffering=0) as handle:
            proc = subprocess.Popen(['nice', '-n', '15', 'ionice', '-c', '3', *argv],
                                    stdin=subprocess.DEVNULL, stdout=handle,
                                    stderr=subprocess.STDOUT, env=env,
                                    start_new_session=True, close_fds=True)
        for _ in range(200):
            identity = base.process_identity(proc.pid)
            if identity and identity['state'] not in ('Z', 'X') and identity['argv'] == argv:
                break
            if proc.poll() is not None:
                raise RuntimeError('priority producer exited; inspect without retrying')
            time.sleep(.05)
        else:
            raise RuntimeError('priority producer identity uncertain; inspect without retrying')
        role = dict(evidence['old_role'])
        role.update(pid=identity['pid'], startticks=identity['starttime_ticks'], argv=argv,
                    bundle=str(bundle), log=str(log), entrypoint=ENTRY, handover=str(handover_path),
                    deployment_operation=payload['operation'], publication_workers=8, publication_limit=64)
        write_json(control / 'pipeline-deployment.json', role)
        write_json(control / 'recovery-roles' / (role['host'] + '-producer.json'), role)
        record.update(status='starting_priority_replay', producer=role,
                      old_producer_terminated=True, started=base.now(),
                      readiness='Not ready until new source scan publishes tasks; PID alive is insufficient')
        base.durable_json(record_path, record, write_json)
        return {'status': record['status'], 'operation': payload['operation'],
                'bundle': str(bundle), 'producer': role, 'next_task_id': next_id}
    except BaseException as error:
        if frozen and not term_attempted:
            actual = base.process_identity(old['pid'])
            if base.same_identity(actual, old) and actual['state'] in ('T', 't'):
                os.kill(old['pid'], signal.SIGCONT)
                frozen = False
        record.update(status='needs_inspection' if term_attempted else 'aborted_old_preserved',
                      termination_attempted=term_attempted, old_producer_terminated=old_exited,
                      failed=base.now(), error=type(error).__name__ + ': ' + str(error),
                      traceback=traceback.format_exc()[-32000:])
        try:
            write_json(record_path, record)
        except BaseException:
            pass
        raise
    finally:
        if frozen:
            actual = base.process_identity(old['pid'])
            if base.same_identity(actual, old) and actual['state'] in ('T', 't'):
                os.kill(old['pid'], signal.SIGCONT)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        for lock in locks:
            lock.close()


def source_payload():
    root = Path(__file__).resolve().parent
    result = {}
    for name in FILES:
        raw = (root / name).read_bytes()
        result[name] = {'base64': base64.b64encode(raw).decode('ascii'),
                        'sha256': hashlib.sha256(raw).hexdigest()}
    return result


def run_ssh(payload, alias, output, timeout):
    root = Path(__file__).resolve().parent
    base_code = base64.b64encode((root / 'deploy_pipelined_producer.py').read_bytes()).decode('ascii')
    own_code = base64.b64encode(Path(__file__).read_bytes()).decode('ascii')
    bootstrap = ('import base64,sys,types; m=types.ModuleType("deploy_pipelined_producer");'
                 'sys.modules[m.__name__]=m;exec(compile(base64.b64decode(' + repr(base_code)
                 + '),"deploy_pipelined_producer.py","exec"),m.__dict__);'
                 'n={"__name__":"safe_replay_remote"};exec(compile(base64.b64decode('
                 + repr(own_code) + '),"safely_replace_replay_producer.py","exec"),n);'
                 'n["remote_main"]()')
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
               '-o', 'ServerAliveInterval=15', alias,
               shlex.join(['python3', '-u', '-c', bootstrap])]
    with open(output, 'xb') as handle:
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=handle, stderr=subprocess.STDOUT)
        try:
            proc.communicate(json.dumps(payload).encode('ascii'), timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.terminate()
            proc.wait(timeout=15)
            raise RuntimeError('SSH timeout: inspect remote attempt; never retry mutations')
    with open(output, 'rb') as handle:
        handle.seek(max(0, output.stat().st_size - base.MAX_RECORD))
        tail = handle.read(base.MAX_RECORD).decode(errors='replace')
    results = [json.loads(line[7:]) for line in tail.splitlines() if line.startswith('RESULT ')]
    if proc.returncode or len(results) != 1:
        raise RuntimeError('remote correction failed/uncertain; inspect ' + str(output))
    return results[0]


def remote_main():
    raw = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024:
        raise RuntimeError('correction request exceeds bound')
    print('RESULT ' + json.dumps(remote_operation(json.loads(raw))), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--control')
    parser.add_argument('--active-pointer', type=Path, default=base.DEFAULT_POINTER)
    parser.add_argument('--ssh-alias', default='copy-master-0')
    parser.add_argument('--control-timeout', type=float, default=30)
    parser.add_argument('--ssh-timeout', type=float, default=7200)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not 30 <= args.control_timeout <= 120 or not 300 <= args.ssh_timeout <= 21600:
        parser.error('control timeout 30..120s; SSH timeout 300..21600s')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', args.ssh_alias):
        parser.error('SSH alias must be a configured plain host name')
    from migration_io import read_json, write_json
    control = Path(args.control or read_json(args.active_pointer)['control']).resolve()
    operation = uuid.uuid4().hex
    output = args.output or Path.cwd() / ('replay-correction-controller-' + operation + '.json')
    if output.exists() or output.with_name(output.name + '.ssh.log').exists():
        raise FileExistsError('controller output must be a fresh path')
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {'control': str(control), 'operation': operation,
               'mode': 'apply' if args.apply else 'inspect', 'control_timeout': args.control_timeout}
    if args.apply:
        payload['files'] = source_payload()
    record = {'status': 'attempting_once', 'created': base.now(), 'operation': operation,
              'mode': payload['mode'], 'control': str(control),
              'bundle': str(control / ('pipelined-producer-' + operation))}
    write_json(output, record)
    try:
        result = run_ssh(payload, args.ssh_alias, output.with_name(output.name + '.ssh.log'), args.ssh_timeout)
        record.update(status=result['status'], result=result, completed=base.now())
        write_json(output, record)
        print(json.dumps(result))
    except BaseException as error:
        record.update(status='needs_inspection' if args.apply else 'inspection_failed',
                      error=type(error).__name__ + ': ' + str(error), failed=base.now())
        write_json(output, record)
        raise


if __name__ == '__main__':
    main()
