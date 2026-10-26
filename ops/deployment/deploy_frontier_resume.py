#!/usr/bin/env python3
"""One-shot producer-only c3559 handover. Workers/broker/seed stay running."""
import argparse
import fcntl
import hashlib
import json
import os
import pickle
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

import deploy_pipelined_producer as base
from migration_io import read_json, write_json


CONTROL = Path('/share-mixed/project/.hcr-copy-20261023T083418Z-c3559-4e81a5a7')
ENTRY = 'migration_frontier_resume.py'
FILES = (ENTRY, 'migration_frontier_inputs.py', 'migration_missing_discovery.py',
         'migration_input_receipts.py',
         'migration_task_publisher.py', 'migration_pipelined_target_filter.py',
         'migration_assigned_checkpoint.py', 'migration_overlap_parallel_filter.py',
         'deploy_frontier_resume.py', 'deploy_pipelined_producer.py')


class SnapshotLedger(base.AssignmentLedger):
    def __init__(self, path):
        super().__init__(path)
        self.assigned = set()
        self.observed_pending = None
        self.delta = None
        self.delta_hash = hashlib.sha256()

    def add(self, task, *, expected_id=None):
        added = super().add(task, expected_id=expected_id)
        if self.observed_pending is not None:
            self.observed_pending.add(task['id'])
        if added:
            self.assigned.update(e['rel'] for e in task['entries'])
            if self.delta is not None:
                line = base.task_bytes(task) + b'\n'
                self.delta.write(line)
                self.delta_hash.update(line)
        return added

    def prepare_cache(self, control, bundle, cfg):
        from migration_assigned_checkpoint import digest_file
        scratch = Path('/tmp') / (cfg['run_id'] + '-assigned-' + bundle.name.rsplit('-', 1)[-1])
        scratch.mkdir(mode=0o700)
        cache = scratch / 'assigned.pickle'
        with open(cache, 'xb') as stream:
            os.chmod(cache, 0o600)
            pickle.dump({'paths':self.assigned, 'task_ids':set(self.hashes)}, stream, protocol=5)
            stream.flush(); os.fsync(stream.fileno())
        meta = {'version':2,'control':str(control),'run_id':cfg['run_id'],
            'base_ledger':str(self.path),'prefix_sha256':self.digest.hexdigest(),
            'cache':str(cache),'cache_sha256':digest_file(cache),
            'assigned_paths':len(self.assigned),'maximum_id':max(self.hashes,default=-1),
            'prefix_tasks':len(self.hashes),'created':base.now()}
        descriptor = bundle / 'assignment-checkpoint.json'
        base.durable_json(descriptor, meta, write_json)
        self.delta = open(bundle / 'assignment-delta.jsonl', 'xb')
        return descriptor, meta

    def close(self):
        result = super().close()
        if self.delta is not None and not self.delta.closed:
            self.delta.flush(); os.fsync(self.delta.fileno()); self.delta.close()
        return result


def stage():
    bundle = CONTROL / ('pipelined-producer-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    hashes = {}
    for name in FILES:
        raw = (Path(__file__).resolve().parent / name).read_bytes()
        with open(bundle / name, 'xb') as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        (bundle / name).chmod(0o444)
        hashes[name] = hashlib.sha256(raw).hexdigest()
    write_json(bundle / 'source-sha256.json', {'files': hashes})
    # An empty preserved warm manifest satisfies the existing adoption
    # contract. Warm queue payloads come from the complete assignment ledger.
    with open(bundle / 'preserved-warm.jsonl', 'xb') as handle:
        handle.flush(); os.fsync(handle.fileno())
    return bundle


def identities_of_children(pid):
    result = []
    for item in Path('/proc').iterdir():
        if not item.name.isdigit():
            continue
        try:
            fields = (item / 'stat').read_text().rsplit(') ', 1)[1].split()
            if int(fields[1]) == pid:
                value = base.process_identity(int(item.name))
                if value and value['state'] not in ('Z', 'X'):
                    result.append(value)
        except (FileNotFoundError, ProcessLookupError):
            pass
    return result


def request_publication_barrier(control, cfg, old, state, bundle, record):
    """Use the already running entrypoint's drained pool replacement hook.

    Do not freeze a busy writer pool or infer quiescence from an old sample.
    This changes bounded control-file writers only, never copy workers.
    """
    publication = state['task_publication']
    workers = publication['workers']
    workers = 16 if workers < 16 else workers + 1 if workers < 64 else 63
    operation = uuid.uuid4().hex
    tuning = {'scope':'warm-publication-v1','control':str(control),'run_id':cfg['run_id'],
        'operation_id':operation,'publish_workers':workers,
        'publish_limit':max(workers,publication['limit'])}
    record.update(status='requesting_publication_barrier', publication_tuning=tuning)
    base.durable_json(bundle / 'deployment.json', record, write_json)
    write_json(control / 'producer-tuning.json', tuning)
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        base.require_identity(old)
        current = base.guarded_state(control, read_json)
        seen = current.get('task_publication', {})
        if seen.get('tuning_operation') == operation and seen.get('pending') == 0:
            return current
        time.sleep(.1)
    raise RuntimeError('publication barrier not observed; original producer preserved')


def thread_states(pid):
    states = []
    for path in (Path('/proc') / str(pid) / 'task').iterdir():
        try:
            states.append((path / 'stat').read_text().rsplit(') ', 1)[1].split()[0])
        except FileNotFoundError:
            pass
    return states


def wait_frozen_threads(expected, seconds=10):
    # A stopped group leader alone does not prove a background thread in a
    # metadata syscall has stopped. Let every such syscall settle before taking
    # the canonical pending/claim snapshot, never after starting a successor.
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        base.require_identity(expected)
        states = thread_states(expected['pid'])
        if states and all(s in ('T', 't', 'Z', 'X') for s in states):
            return len(states)
        time.sleep(.01)
    raise RuntimeError('producer threads not fully frozen; original producer preserved')


def require_assignment_coverage(ledger, db):
    present = ledger.observed_pending | {row[0] for row in db.execute(
        "SELECT id FROM claims WHERE stage='p3-r3'")}
    if present != set(ledger.hashes):
        raise RuntimeError('canonical assignment set differs from checkpoint; original producer preserved')
    return len(present)


def catch_up_live(control, db, ledger):
    """Absorb the long prefix-read tail while copying/production still run."""
    rounds = []
    for _ in range(2):
        before = len(ledger.hashes)
        base.collect_pending(control, db, ledger, read_json)
        base.collect_claims(db, ledger, missing_only=True)
        added = len(ledger.hashes) - before
        rounds.append(added)
        if added < 256:
            break
    return rounds


def preflight(control):
    cfg = read_json(control / 'config.json')
    if (control != CONTROL or cfg.get('job') != 'c3559adf-8c4e-4a14-afa4-b998ad8c0a1b'
            or socket.gethostname() != cfg.get('master_host')
            or cfg.get('crash_recovery_v1') is not True
            or len(cfg.get('copy_hosts', [])) != 11
            or any(h.endswith('-worker-9') for h in cfg['copy_hosts'])):
        raise RuntimeError('wrong host/control or node allowlist')
    state = base.guarded_state(control, read_json)
    if state.get('task_publication', {}).get('failed'):
        raise RuntimeError('publication failure requires explicit reconciliation')
    role_path = control / 'recovery-roles' / (socket.gethostname() + '-producer.json')
    role = read_json(role_path)
    old = base.role_identity(dict(role, starttime_ticks=role.get('starttime_ticks',
        role.get('startticks', role.get('start_ticks')))))
    base.require_identity(old)
    if not any(arg == str(control / 'migration_crash_resume.py')
               or (Path(arg).name == ENTRY and Path(arg).parent.parent == control)
               for arg in old['argv']):
        raise RuntimeError('only the original fresh crash producer may be replaced')
    children = identities_of_children(old['pid'])
    seeds = [v for v in children if str(control / 'migration_overlap_resume.py') in v['argv']
             and 'seed' in v['argv']]
    result = control / 'overlap-seed-result.json'
    if len(seeds) == 1:
        helper = dict(seeds[0], result_path=str(result))
    elif not seeds and role.get('handover'):
        helper = read_json(Path(role['handover']))['seed_helper']
    else:
        raise RuntimeError('exactly one original live seed helper is required')
    base.require_identity(helper)
    frontier = Path(read_json(Path(cfg['resume_from']) / 'state.json')['active_manifest']).resolve()
    if (not frontier.is_relative_to(Path(cfg['resume_from']).resolve())
            or not frontier.is_file() or not (Path(cfg['resume_from']) / 'STOP').is_file()):
        raise RuntimeError('old inventory/fence changed')
    broker = read_json(control / 'broker.json')
    broker_id = base.process_identity(broker['pid'])
    if not broker_id or not any(a.endswith('/migration_broker_sharded.py') for a in broker_id['argv']):
        raise RuntimeError('original sharded broker is not live')
    base.require_identity(broker_id)
    base.broker_control(cfg, 15)
    return cfg, state, role, role_path, old, helper, children, frontier, broker_id


def apply(bundle):
    control = CONTROL
    cfg, state, role, role_path, old, helper, children, frontier, broker_id = preflight(control)
    if bundle.parent != control:
        raise ValueError('bundle outside current control')
    hashes = read_json(bundle / 'source-sha256.json')['files']
    if set(hashes) != set(FILES):
        raise ValueError('unexpected bundle files')
    for name, digest in hashes.items():
        if hashlib.sha256((bundle / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError('bundle source changed')
    locks = []
    for suffix in ('-parallel-target-dispatch.lock', '-cache-age-dispatch.lock', '-frontier-dispatch.lock'):
        lock = open('/tmp/' + cfg['run_id'] + suffix, 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    # One-shot intent is durable before any producer signal. Reruns fail here.
    with open(bundle / 'apply-once.json', 'xb') as handle:
        handle.write((json.dumps({'created': base.now(), 'old': old}) + '\n').encode())
        handle.flush(); os.fsync(handle.fileno())
    record = {'version': 1, 'operation': bundle.name.rsplit('-', 1)[-1],
        'control': str(control), 'bundle': str(bundle), 'status': 'preparing',
        'started': base.now(), 'old_producer': old, 'seed_helper': helper,
        'workers_broker_seed_signalled': False}
    frozen = terminated = False
    ledger = SnapshotLedger(bundle / 'assigned-tasks.jsonl')
    db = base.claims_reader(Path('/tmp') / (cfg['run_id'] + '-broker.sqlite'))
    handlers = {}
    def interrupted(signum, frame):
        raise RuntimeError('handover interrupted by signal ' + str(signum))
    for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        handlers[sig] = signal.signal(sig, interrupted)
    try:
        base.collect_pending(control, db, ledger, read_json)
        base.collect_claims(db, ledger)
        descriptor, assignment_cache = ledger.prepare_cache(control, bundle, cfg)
        record.update(status='prepared_while_old_running',
                      assignment_checkpoint=str(descriptor), assigned_paths=len(ledger.assigned))
        base.durable_json(bundle / 'deployment.json', record, write_json)
        # No signal until the new entrypoint successfully imports and parses.
        env = dict(os.environ, PYTHONPATH=str(bundle) + os.pathsep + str(control),
            PYTHONPYCACHEPREFIX='/tmp/' + cfg['run_id'] + '-frontier-' + record['operation'])
        with open(bundle / 'help.log', 'xb') as out:
            subprocess.run([sys.executable, str(bundle / ENTRY), '--help'], env=env,
                stdout=out, stderr=subprocess.STDOUT, check=True, timeout=30)
        record['live_catchup_tasks'] = catch_up_live(control, db, ledger)
        record.update(status='caught_up_while_old_running')
        base.durable_json(bundle / 'deployment.json', record, write_json)
        base.require_identity(old); base.require_identity(helper); base.require_identity(broker_id)
        state = base.guarded_state(control, read_json)
        request_publication_barrier(control, cfg, old, state, bundle, record)
        record.update(status='freezing')
        base.durable_json(bundle / 'deployment.json', record, write_json)
        for attempt in range(20):
            base.require_identity(old)
            frozen = True
            os.kill(old['pid'], signal.SIGSTOP)
            base.wait_state(old, {'T', 't'}, 10)
            record['frozen_threads'] = wait_frozen_threads(old)
            state = base.guarded_state(control, read_json)
            if state.get('task_publication', {}).get('pending') == 0:
                break
            os.kill(old['pid'], signal.SIGCONT)
            frozen = False
            time.sleep(.25)
        else:
            raise RuntimeError('publisher not quiescent; original producer preserved')
        ledger.observed_pending = set()
        base.collect_pending(control, db, ledger, read_json)
        base.collect_claims(db, ledger, missing_only=True)
        record['canonical_assignments_reconciled'] = require_assignment_coverage(ledger, db)
        info = ledger.close()
        # Leave a small explicit reservation gap for a frozen unobserved
        # publication. Never reuse even an uncertain old-process task ID.
        next_id = max(info['maximum_id'] + 1, state['next_task_id']) + 128
        handover = {'control': str(control), 'stage': 'p3-r3', 'old_producer': old,
            'seed_helper': helper, 'assigned_tasks_path': str(ledger.path),
            'ledger_sha256': info['sha256'], 'next_task_id': next_id,
            'warm_manifest': str(bundle / 'preserved-warm.jsonl'),
            'missing_manifest': str(control / 'manifests/recovery-missing.jsonl'),
            'frontier_manifest': str(frontier), 'frontier_size': frontier.stat().st_size,
            'operation': record['operation'], 'created': base.now(),
            'preserved_task_count': info['tasks'], 'reserved_id_gap': 128,
            'preserved_enqueued_batches': info['tasks'],
            'assignment_checkpoint':str(descriptor),
            'assignment_prefix_sha256':assignment_cache['prefix_sha256'],
            'assignment_delta_path':str(bundle / 'assignment-delta.jsonl'),
            'assignment_delta_sha256':ledger.delta_hash.hexdigest()}
        handover_path = bundle / 'handover.json'
        base.durable_json(handover_path, handover, write_json)
        # Carry the preceding producer's frozen observations across multiple
        # handovers, without contaminating process-local endpoint rates.
        from migration_progress import publication_probe_baseline
        previous = publication_probe_baseline(control, cfg, state, [])
        frozen_state = json.loads(json.dumps(state))
        if previous and previous.get('complete'):
            counts = frozen_state['distributed_target_probe']['counts']
            counts['remote_existing'] = counts.get('remote_existing', 0) + previous['counts']['remote_existing']
        checkpoint = dict(record, status='frozen_checkpoint', handover=handover,
            frozen_state=frozen_state, ledger=info)
        base.durable_json(bundle / 'frozen-checkpoint.json', checkpoint, write_json)
        base.require_identity(helper); base.require_identity(broker_id)
        base.require_identity(old, states={'T', 't'})
        record.update(status='terminating_old', termination_requested=base.now())
        base.durable_json(bundle / 'deployment.json', record, write_json)
        terminated = True
        os.kill(old['pid'], signal.SIGTERM)
        os.kill(old['pid'], signal.SIGCONT)
        frozen = False
        base.wait_exited(old, 15)
        base.wait_lock_released('/tmp/' + cfg['run_id'] + '-coordinator.lock', 15)
        # The old state writer has exited and the successor has not launched:
        # publish one truthful handover heartbeat, without changing progress,
        # task IDs or stage. This avoids a 30 s cached-state expiry during load.
        heartbeat = dict(state, updated=base.now(),
            producer_handover={'operation':record['operation'],'status':'loading_successor'})
        base.durable_json(control / 'state.json', heartbeat, write_json)
        # Stop only the exact obsolete, read-only multiprocessing clients.
        # The seed helper, broker, metrics and all copy workers are untouched.
        for child in children:
            if child['pid'] == helper['pid']:
                continue
            if (any('multiprocessing.spawn' in a for a in child['argv'])
                    and base.same_identity(base.process_identity(child['pid']), child)):
                os.kill(child['pid'], signal.SIGTERM)
        base.require_identity(helper); base.require_identity(broker_id)
        argv = [sys.executable, '-u', str(bundle / ENTRY), 'run', '--control', str(control),
                '--handover', str(handover_path)]
        record.update(status='launching_once', argv=argv)
        base.durable_json(bundle / 'deployment.json', record, write_json)
        with open(bundle / 'producer.log', 'ab', buffering=0) as out:
            proc = subprocess.Popen(['nice', '-n', '15', 'ionice', '-c', '3', *argv],
                env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                start_new_session=True, close_fds=True)
        for _ in range(200):
            actual = base.process_identity(proc.pid)
            if actual and actual['argv'] == argv and actual['state'] not in ('Z', 'X'):
                break
            if proc.poll() is not None:
                raise RuntimeError('new producer exited; inspect, never blindly retry')
            time.sleep(.05)
        else:
            raise RuntimeError('new launch identity uncertain')
        new_role = dict(role, pid=actual['pid'], startticks=actual['starttime_ticks'],
            start_ticks=actual['starttime_ticks'], starttime_ticks=actual['starttime_ticks'],
            argv=argv, expected_argv=argv, bundle=str(bundle),
            handover=str(handover_path), log=str(bundle / 'producer.log'),
            status='launched', started=base.now(), updated=base.now(),
            health_verified=False, health_verified_at=None, verification=None)
        write_json(role_path, new_role)
        write_json(control / 'pipeline-deployment.json', new_role)
        record.update(status='starting_frontier_feed', producer=new_role,
            next_task_id=next_id, preserved_tasks=info['tasks'], finished=base.now())
        base.durable_json(bundle / 'deployment.json', record, write_json)
        print(json.dumps({'status': record['status'], 'pid': actual['pid'],
            'bundle': str(bundle), 'preserved_tasks': info['tasks'],
            'next_task_id': next_id, 'seed_pid_unchanged': helper['pid']}), flush=True)
    except BaseException as exc:
        # Resume immediately on pre-termination failure, before slow audit I/O.
        if frozen and not terminated and base.same_identity(base.process_identity(old['pid']), old):
            os.kill(old['pid'], signal.SIGCONT)
            frozen = False
        record.update(status='needs_inspection' if terminated else 'aborted_old_preserved',
                      error=repr(exc), failed=base.now())
        base.durable_json(bundle / 'deployment.json', record, write_json)
        raise
    finally:
        if frozen and base.same_identity(base.process_identity(old['pid']), old):
            os.kill(old['pid'], signal.SIGCONT)
        ledger.close(); db.close()
        for sig, handler in handlers.items(): signal.signal(sig, handler)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage', action='store_true')
    p.add_argument('--apply', type=Path)
    args = p.parse_args()
    if args.stage:
        print(json.dumps({'bundle': str(stage()), 'status': 'staged'}), flush=True)
    elif args.apply:
        apply(args.apply.resolve())
    else:
        p.error('--stage or --apply BUNDLE is required')


if __name__ == '__main__':
    main()
