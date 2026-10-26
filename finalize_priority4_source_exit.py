#!/usr/bin/env python3
"""Continue ONLY the already-signalled P4 handover after proven terminal exit.

No signals, process launches, queue writes or broker changes are performed.
The original immutable successor validates final archives and inventory itself.
"""
import argparse
import fcntl
import hashlib
from pathlib import Path
import time

from deploy_fast_priority4_source import CONTROL, PRODUCER, AUTHORIZATION, frozen
from migration_io import read_json, write_json
from migration_scan_boundary import identity, require_identity


def terminal(actual, expected):
    if actual is not None and (actual.get('state') not in ('Z', 'X') or
            any(actual.get(key) != expected[key] for key in ('pid', 'start_ticks'))):
        raise RuntimeError('original P4 producer terminal exit not proven')


def finalize(directory, audit):
    if (directory.parent != CONTROL or not directory.name.startswith('fast-p4-source-')
            or directory.is_symlink() or audit.parent != directory):
        raise ValueError('exact previously requested P4 source operation required')
    intent, request = read_json(directory / 'intent.json'), read_json(directory / 'handover-request.json')
    sent = read_json(directory / 'sigint-sent.json')
    if (intent['producer'] != PRODUCER or request['producer'] != PRODUCER or sent['producer'] != PRODUCER
            or request['user_authorization'] != AUTHORIZATION
            or not intent['operation'] == request['operation'] == sent['operation']
            or (directory / 'release.json').exists() or (directory / 'failure.json').exists()):
        raise ValueError('original one-signal transaction binding differs')
    frozen(CONTROL, intent['runtime_manifest_sha256'])
    frozen(directory)
    for path, checksum in intent['preserved_runtime_manifests'].items():
        frozen(Path(path), checksum)
    try:
        actual = identity(PRODUCER['pid'])
    except FileNotFoundError:
        actual = None
    terminal(actual, PRODUCER)
    for actor in request['preserved_actors']:
        require_identity(actor)
    successor = read_json(directory / 'producer-role.json')
    require_identity(successor)
    proof = read_json(audit)
    expected_nodes = {'master-0'} | {'worker-' + str(i) for i in range(11)}
    if (proof.get('all_unchanged') is not True or proof.get('copy_process_count') != 48
            or proof.get('signals_sent') != 0 or len(proof.get('nodes', [])) != 12
            or {n['node'] for n in proof['nodes']} != expected_nodes
            or not 0 <= time.time() - proof['verified_at'] <= 60
            or any(time.time() - n['verified_at'] > 60 for n in proof['nodes'])):
        raise ValueError('fresh 48-process native proof required')
    state = read_json(CONTROL / 'state.json')
    if (state.get('status') != 'producer_paused' or state.get('stage') != 'p4-r1'
            or 'KeyboardInterrupt' not in state.get('producer_error', '')):
        raise ValueError('original producer did not record its expected graceful exit')
    cfg = read_json(CONTROL / 'config.json')
    with open('/tmp/' + cfg['run_id'] + '-coordinator.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    record = dict(request, graceful_sigint_exit=True, all_copy_and_broker_identities_verified=True,
        graceful_exit_seconds=time.time() - sent['sent_at'], released=time.time(),
        post_exit_copy_audit=str(audit), terminal_predecessor=actual,
        continuation_reason='first controller rejected argv clearing during exit_mm; terminal exit independently verified',
        continuation_entry=str(Path(__file__).resolve()),
        continuation_entry_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        additional_signals_sent=0)
    write_json(directory / 'post-exit-finalization.json', record)
    # Lock is released before the standby is allowed to acquire it.
    write_json(directory / 'release.json', record)
    print(dict(status='released_after_verified_terminal_exit', elapsed=record['graceful_exit_seconds'],
               additional_signals_sent=0), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--audit', type=Path, required=True)
    args = parser.parse_args()
    finalize(args.directory.resolve(), args.audit.resolve())
