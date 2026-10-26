#!/usr/bin/env python3
"""Explicit prepared P4 producer-only handover. NEVER signals copy/broker PIDs."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import time
import uuid

from migration_io import read_json, write_json
from migration_priority4_run import validate_authorization
from migration_scan_boundary import identity, require_identity


CONTROL = Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053')
PREDECESSOR = Path('/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a')
SOURCE = CONTROL / 'source-preparation-5a32110da4504ff6823da6bf0ec6c7a4/preparation.json'
ASSIGNMENTS = CONTROL / 'assignment-preparation-69fc220c6e984caba0452bfec9655237/preparation.json'
P3_SOURCE = PREDECESSOR / 'fast-p3-scan-466d08d7ea874d91a73534fb5caabac5'
P4_BROKER = CONTROL / 'broker-sharded-16b7938341ae4f669ce2413ff8dc77eb'
AUTHORIZATION = '允许，验证后交接 P4 生产端'
PRODUCER = dict(pid=3745648, start_ticks='77412873', argv=[
    '/usr/bin/python3', '-u', str(CONTROL / 'migration_priority4_run.py'), '--control', str(CONTROL)])


def frozen(directory, manifest_sha=None):
    raw = (directory / 'source-sha256.json').read_bytes()
    if manifest_sha and hashlib.sha256(raw).hexdigest() != manifest_sha:
        raise ValueError('frozen runtime manifest SHA differs')
    values = json.loads(raw)
    for name, digest in values.items():
        path = directory / name
        if (Path(name).name != name or path.is_symlink()
                or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
            raise ValueError('immutable runtime source differs: ' + str(path))
    return hashlib.sha256(raw).hexdigest()


def validate_original():
    cfg = read_json(CONTROL / 'config.json')
    validate_authorization(CONTROL, cfg)
    if (socket.gethostname() != 'job-' + cfg['job'] + '-master-0'
            or cfg['job'] != '2dadf47a-b549-47bb-a267-d56dc877aec9'
            or cfg['parallel_with_control'] != str(PREDECESSOR)):
        raise ValueError('actual bound P4 cluster master required')
    require_identity(PRODUCER)
    lock_path = Path('/tmp/' + cfg['run_id'] + '-coordinator.lock')
    lock_info = lock_path.stat()
    if not any(len(parts) >= 6 and parts[1] == 'FLOCK' and parts[4] == str(PRODUCER['pid'])
               and parts[5].endswith(':' + str(lock_info.st_ino))
               for parts in (line.split() for line in Path('/proc/locks').read_text().splitlines())):
        raise ValueError('original P4 producer does not hold the expected coordinator flock')
    state = read_json(CONTROL / 'state.json')
    if (state.get('status'), state.get('stage'), state.get('scan_complete')) != ('scanning', 'p4-r1', False):
        raise ValueError('original P4 scanning stage changed')
    if (CONTROL / 'STOP').exists() or (PREDECESSOR / 'STOP').exists():
        raise RuntimeError('STOP exists; no handover')
    return cfg, state


def prepare():
    cfg, state = validate_original()
    root_sha = frozen(CONTROL)
    preserved_hashes = {str(path): frozen(path) for path in (PREDECESSOR, P3_SOURCE, P4_BROKER)}
    preparation_hashes = {str(path): frozen(path) for path in (SOURCE.parent, ASSIGNMENTS.parent)}
    source = read_json(SOURCE)
    assignments = read_json(ASSIGNMENTS) if ASSIGNMENTS.exists() else None
    if (source['status'] != 'prepared_prefix_and_fifo_only'
            or source['producer'] != PRODUCER
            or source['runtime_manifest_sha256'] != root_sha
            or assignments is not None and (assignments['status'] != 'prepared_assignment_prefix_only'
                or assignments['producer'] != PRODUCER or assignments['runtime_manifest_sha256'] != root_sha)):
        raise ValueError('bound source and available assignment preparations required')
    bundle = CONTROL / ('fast-p4-source-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parent
    # Unchanged engine modules come from the frozen live bundle, not arbitrary
    # working-tree versions. Only the explicitly named new paths are overlaid.
    names = {p.name for p in repository.glob('migration_*.py')}
    names.update(p.name for p in repository.glob('test_*.py'))
    names.update(('prepare_priority4_assignments.py', 'prepare_priority4_source.py',
                  'deploy_fast_priority4_source.py'))
    overlay = {'migration_priority4_scan.py', 'migration_process_scan.py', 'migration_linux_open.py',
               'migration_task_publisher.py', 'migration_source_tuning.py', 'migration_priority4_fast_run.py'}
    original = read_json(CONTROL / 'source-sha256.json')
    hashes = {}
    for name in sorted(names):
        origin = CONTROL / name if name in original and name not in overlay else repository / name
        data = origin.read_bytes()
        with open(bundle / name, 'xb') as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        (bundle / name).chmod(0o444)
        hashes[name] = hashlib.sha256(data).hexdigest()
    write_json(bundle / 'source-sha256.json', hashes)
    write_json(bundle / 'intent.json', dict(control=str(CONTROL), producer=PRODUCER,
        source_preparation=str(SOURCE), assignment_preparation=str(ASSIGNMENTS),
        runtime_manifest_sha256=root_sha, operation=uuid.uuid4().hex,
        preserved_runtime_manifests=preserved_hashes,
        preparation_runtime_manifests=preparation_hashes,
        preserved_actors=[identity(pid) for pid in (5834, 1856078, 281673, 5837, 3744900)],
        created=time.time(), copy_processes_signalled=0, brokers_signalled=0))
    write_json(bundle / 'source-limit.json', dict(scope='priority4-source-reader-limit-v1',
        control=str(CONTROL), run_id=cfg['run_id'], processes=8))
    tests = ['test_migration_priority4_fast_run', 'test_migration_priority4_frontier',
             'test_prepare_priority4_assignments', 'test_migration_priority4_scan',
             'test_migration_process_scan', 'test_migration_task_publisher', 'test_migration_source_tuning',
             'test_deploy_fast_priority4_source', 'test_priority4_fifo_live_resume']
    with open(bundle / 'native-tests.log', 'xb') as output:
        subprocess.run(['/usr/bin/python3', '-m', 'unittest'] + tests, cwd=bundle,
                       stdout=output, stderr=subprocess.STDOUT, timeout=180, check=True)
    validate_original()
    frozen(bundle)
    with open(bundle / 'producer.log', 'xb') as output:
        process = subprocess.Popen(['/usr/bin/python3', '-u', str(bundle / 'migration_priority4_fast_run.py'),
            '--directory', str(bundle)], cwd=bundle, stdin=subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    write_json(bundle / 'producer-role.json', identity(process.pid))
    print(json.dumps(dict(status='standby_launched_readonly', directory=str(bundle), pid=process.pid)), flush=True)


def source_preload_lag(meta, state, standby, info):
    offset = standby.get('manifest_offset')
    if (state.get('active_manifest') != meta['manifest'] or not stat.S_ISREG(info.st_mode)
            or (info.st_dev, info.st_ino) != (meta['manifest_dev'], meta['manifest_ino'])
            or type(offset) is not int or offset < meta['offset']
            or not 0 <= info.st_size - offset <= 16 * 1024**2):
        raise ValueError('standby has not caught the actual current source EOF closely enough')
    return info.st_size - offset


def handover(directory, audit):
    if (directory.parent != CONTROL or directory.is_symlink()
            or not directory.name.startswith('fast-p4-source-') or audit.parent != directory):
        raise ValueError('exact prepared P4 successor and its fresh audit required')
    cfg, state = validate_original()
    intent = read_json(directory / 'intent.json')
    if intent['producer'] != PRODUCER or intent['control'] != str(CONTROL):
        raise ValueError('P4 successor intent differs')
    frozen(CONTROL, intent['runtime_manifest_sha256'])
    for path, expected_sha in intent.get('preserved_runtime_manifests', {}).items():
        if Path(path) not in (PREDECESSOR, P3_SOURCE, P4_BROKER):
            raise ValueError('unexpected preserved runtime binding')
        frozen(Path(path), expected_sha)
    for path, expected_sha in intent.get('preparation_runtime_manifests', {}).items():
        if Path(path) not in (SOURCE.parent, ASSIGNMENTS.parent):
            raise ValueError('unexpected preparation runtime binding')
        frozen(Path(path), expected_sha)
    frozen(directory)
    proof = read_json(audit)
    if (proof.get('all_unchanged') is not True or proof.get('signals_sent') != 0
            or proof.get('copy_process_count') != 48 or len(proof.get('nodes', [])) != 12
            or not 0 <= time.time() - proof['verified_at'] <= 60
            or any(time.time() - n['verified_at'] > 60 for n in proof['nodes'])):
        raise ValueError('fresh 12-node/48-copy native identity proof required')
    expected_nodes = {'master-0'} | {'worker-' + str(i) for i in range(11)}
    if {n['node'] for n in proof['nodes']} != expected_nodes:
        raise ValueError('native proof has missing/duplicate nodes')
    standby = read_json(directory / 'standby.json')
    successor = read_json(directory / 'producer-role.json')
    require_identity(successor)
    if (standby.get('status') != 'standby_readonly_caught_up' or standby.get('manifest_lag') != 0
            or time.time() - standby['updated'] > 15
            or type(standby.get('tasks')) is not int or standby['tasks'] <= 0
            or abs(state['next_task_id'] - standby['tasks']) > 512):
        raise ValueError('successor has not caught up closely enough; original remains running')
    # Native identities of both brokers and non-P4 control actors are captured
    # now and must survive unchanged. No sockets or payloads are consumed.
    actors = intent['preserved_actors']
    if [actor['pid'] for actor in actors] != [5834, 1856078, 281673, 5837, 3744900]:
        raise ValueError('preserved P3/P4 actor identities differ')
    if (str(PREDECESSOR) not in actors[0]['argv'] or str(CONTROL) not in actors[1]['argv']
            or str(PREDECESSOR) not in actors[2]['argv']):
        raise ValueError('P3/P4 broker or P3 producer binding differs')
    for actor in actors:
        require_identity(actor)
    source_meta = read_json(SOURCE)
    lag = source_preload_lag(source_meta, state, standby, Path(source_meta['manifest']).lstat())
    with open(directory / 'handover-controller.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (directory / 'handover-request.json').exists():
            raise RuntimeError('handover already requested; inspect, never retry blindly')
        record = dict(control=str(CONTROL), producer=PRODUCER, successor=successor,
            operation=intent['operation'], user_authorization=AUTHORIZATION,
            fresh_copy_audit=str(audit), preserved_actors=actors, requested=time.time(),
            actual_source_preload_lag_bytes=lag,
            controller_entry=str(Path(__file__).resolve()),
            controller_entry_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            copy_processes_signalled=0, brokers_signalled=0)
        write_json(directory / 'handover-request.json', record)
        require_identity(PRODUCER)
        require_identity(successor)
        # This is the ONLY signal issued by this controller.
        os.kill(PRODUCER['pid'], signal.SIGINT)
        record['sigint_sent_at'] = time.time()
        # The standby reads the request during exit. Keep that file immutable:
        # upgrading it to snapshot mode here could expose a transient empty
        # snapshot directory at exactly the handover boundary.
        write_json(directory / 'sigint-sent.json', dict(producer=PRODUCER,
            operation=intent['operation'], sent_at=record['sigint_sent_at']))
        began, deadline = time.monotonic(), time.monotonic() + 180
        while True:
            require_identity(successor)
            for actor in actors:
                require_identity(actor)
            try:
                old = identity(PRODUCER['pid'])
            except FileNotFoundError:
                old = None
            if old is None or old['state'] in ('Z', 'X'):
                break
            if any(old[k] != PRODUCER[k] for k in ('pid', 'start_ticks', 'argv')):
                raise RuntimeError('old producer identity changed during graceful exit')
            if time.monotonic() > deadline:
                raise TimeoutError('graceful P4 producer exit not confirmed; no escalation or actor restarts')
            time.sleep(.2)
        saved = read_json(CONTROL / 'state.json')
        if (saved.get('status'), saved.get('stage')) != ('producer_paused', 'p4-r1') or 'KeyboardInterrupt' not in saved.get('producer_error', ''):
            raise RuntimeError('old producer did not record expected graceful pause')
        with open('/tmp/' + cfg['run_id'] + '-coordinator.lock', 'a') as coordinator_lock:
            fcntl.flock(coordinator_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record.update(graceful_sigint_exit=True, all_copy_and_broker_identities_verified=True,
                      graceful_exit_seconds=time.monotonic() - began, released=time.time())
        write_json(directory / 'release.json', record)
        print(json.dumps(dict(status='released_to_prepared_successor', directory=str(directory),
            graceful_exit_seconds=record['graceful_exit_seconds'], copy_processes_signalled=0,
            brokers_signalled=0)), flush=True)


def start_handover(directory, audit):
    if (directory.parent != CONTROL or directory.is_symlink()
            or not directory.name.startswith('fast-p4-source-') or audit.parent != directory
            or (directory / 'apply-role.json').exists()):
        raise ValueError('new exact P4 producer-only handover controller required')
    entry = Path(__file__).resolve()
    if entry.parent != directory and (entry.parent.parent != directory or not entry.parent.name.startswith('controller-')):
        raise ValueError('controller entry must be frozen inside this successor bundle')
    frozen(directory)
    with open(directory / 'apply.log', 'xb') as output:
        process = subprocess.Popen(['/usr/bin/python3', '-u', str(entry), 'handover',
            '--directory', str(directory), '--audit', str(audit)], cwd=directory,
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    actual = identity(process.pid)
    write_json(directory / 'apply-role.json', actual)
    print(json.dumps(dict(status='validated_handover_controller_started', controller=actual)), flush=True)


def stage_controller(directory):
    if directory.parent != CONTROL or not directory.name.startswith('fast-p4-source-') or directory.is_symlink():
        raise ValueError('exact immutable successor bundle required')
    frozen(directory)
    result = directory / ('controller-' + uuid.uuid4().hex)
    result.mkdir(mode=0o700)
    hashes = {}
    for name in read_json(directory / 'source-sha256.json'):
        if name.startswith('test_') or name.startswith('prepare_'):
            continue
        origin = Path(__file__) if name == Path(__file__).name else directory / name
        data = origin.read_bytes()
        with open(result / name, 'xb') as output:
            output.write(data)
        (result / name).chmod(0o444)
        hashes[name] = hashlib.sha256(data).hexdigest()
    name = 'test_deploy_fast_priority4_source.py'
    data = (Path(__file__).parent / name).read_bytes()
    with open(result / name, 'xb') as output:
        output.write(data)
    (result / name).chmod(0o444)
    hashes[name] = hashlib.sha256(data).hexdigest()
    write_json(result / 'source-sha256.json', hashes)
    subprocess.run(['/usr/bin/python3', '-m', 'unittest', 'test_deploy_fast_priority4_source'],
                   cwd=result, check=True, timeout=30)
    print(json.dumps(dict(status='controller_staged_only', controller=str(result))), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('prepare', 'handover', 'start-handover', 'stage-controller'))
    parser.add_argument('--directory', type=Path)
    parser.add_argument('--audit', type=Path)
    args = parser.parse_args()
    if args.operation == 'prepare':
        prepare()
    elif args.operation == 'stage-controller':
        if args.directory is None:
            parser.error('stage-controller requires --directory')
        stage_controller(args.directory.resolve())
    else:
        if args.directory is None or args.audit is None:
            parser.error('handover requires exact --directory and --audit')
        function = handover if args.operation == 'handover' else start_handover
        function(args.directory.resolve(), args.audit.resolve())


if __name__ == '__main__':
    main()
