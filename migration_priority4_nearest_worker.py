#!/usr/bin/env python3
"""UNACTIVATED, second-canary P4 O_RDONLY missing-parent entry.

No drain, signal, broker change or process launch exists here. A separately
authorized controller must first prove the exact current lane idle/lease-free,
stop it gracefully, and provide a fresh checkpoint. All copy/SHA code and the
original P4 scope wrapper remain byte-identical to their frozen predecessors.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket

import migration_priority4_path_worker as proof
import migration_parent_frontier as frontier


SCOPE = 'priority4-nearest-readonly-parent-checkpoint-v1'
STRATEGY = 'nearest_native_readonly'
AUTHORIZATION = '允许，验证后做第二次单进程对照'
PREVIOUS_NAME = 'p4-copy-path-0f78c471bb9e4a99afdd2260b4ac4fae'
PREVIOUS_CONTROLLER = 'controller-8d15cd7be4964331aa7a5ae164259bfd'
PREVIOUS_OPERATION = 'bad956b1286340f4bb7d2ea5eddb3f14'
PREVIOUS_PID = 918041
PREVIOUS_START = '1071970035'
ENTRY = 'migration_priority4_nearest_worker.py'
POLICY = 'nearest-parent-policy.json'
ADDITIONS = frozenset((ENTRY, 'migration_parent_frontier.py', POLICY))
StartupProgressError = proof.StartupProgressError


def expected_predecessor(control, job):
    previous = control / PREVIOUS_NAME
    host = 'job-' + job + '-worker-10'
    return dict(pid=PREVIOUS_PID, starttime_ticks=PREVIOUS_START, argv=[
        '/usr/bin/python3', '-u', str(previous / 'migration_priority4_path_worker.py'),
        'supplement', '--control', str(control), '--threads', '64', '--nice', '0',
        '--cache-max-age', '30.0', '--progress-checkpoint',
        str(previous / 'cache-age-rollout' / (host + '-supplement.json')),
        '--progress-operation', PREVIOUS_OPERATION])


def policy_for(control, cfg):
    previous = control / PREVIOUS_NAME
    return dict(scope=SCOPE, control=str(control), job=cfg['job'], run_id=cfg['run_id'],
        node='worker-10', lane='supplement', directory_strategy=STRATEGY,
        previous_bundle=str(previous),
        predecessor_stdout_log=str(previous / PREVIOUS_CONTROLLER / 'worker.log'),
        previous_manifest_sha256=proof.digest_file(previous / 'source-sha256.json'),
        original_manifest_sha256=proof.digest_file(control / 'source-sha256.json'),
        config_sha256=hashlib.sha256(json.dumps(cfg, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        predecessor=expected_predecessor(control, cfg['job']))


def validate_bundle(control, bundle, lane):
    if (bundle.parent != control or not bundle.name.startswith('p4-copy-nearest-')
            or bundle.is_symlink() or control.is_symlink()):
        raise StartupProgressError('independent immutable nearest-parent bundle required')
    cfg = proof.io.read_json(control / 'config.json')
    proof.original.ScopeGuard(str(control), cfg)
    original = proof.static_json(control / 'source-sha256.json')
    previous_path = control / PREVIOUS_NAME
    previous = proof.static_json(previous_path / 'source-sha256.json')
    current = proof.static_json(bundle / 'source-sha256.json')
    if (not isinstance(original, dict) or not isinstance(previous, dict)
            or set(previous) != set(original) | proof.ADDITIONS
            or not isinstance(current, dict) or set(current) != set(previous) | ADDITIONS):
        raise StartupProgressError('complete original, first-canary and second-canary manifests required')
    for name, digest in current.items():
        if (Path(name).name != name or name in ('.', '..')
                or proof.digest_file(bundle / name) != digest):
            raise StartupProgressError('second-canary source differs: ' + name)
        if name in previous and (digest != previous[name]
                or proof.digest_file(previous_path / name) != digest):
            raise StartupProgressError('first-canary source changed: ' + name)
        if name in original and (digest != original[name]
                or proof.digest_file(control / name) != digest):
            raise StartupProgressError('original P4 source changed: ' + name)
    # Reuse the original immutable-policy checks against the first bundle,
    # while the new policy explicitly binds this exact live predecessor.
    proof.validate_bundle(control, previous_path, lane)
    policy = proof.static_json(bundle / POLICY)
    if (policy != policy_for(control, cfg) or lane != 'supplement'
            or socket.gethostname() != 'job-' + cfg['job'] + '-worker-10'):
        raise StartupProgressError('nearest-parent policy/node/lane/predecessor differs')
    return cfg, policy


def validate_imports(bundle):
    proof.validate_imports(bundle)
    for module in (proof, frontier):
        if Path(os.path.abspath(module.__file__)).parent != bundle:
            raise StartupProgressError('nearest-parent entry imported a foreign helper')


def checkpoint_identity(control, bundle, checkpoint, operation, lane, reader, policy):
    record = reader(checkpoint)
    if (lane != 'supplement' or record.get('control') != str(control) or record.get('migration_scope') != SCOPE
            or record.get('directory_strategy') != STRATEGY
            or record.get('user_authorization') != AUTHORIZATION
            or record.get('expected') != policy['predecessor']):
        raise StartupProgressError('fresh scope-bound second-canary authorization/checkpoint required')
    guard = proof.StartupProgressGuard(control, lane, checkpoint, operation, reader, bundle)
    guard.preflight()
    return guard, record['expected']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('supplement',))
    parser.add_argument('--control', required=True)
    parser.add_argument('--threads', type=int, choices=(64,), default=64)
    parser.add_argument('--nice', type=int, choices=(0,), default=0)
    parser.add_argument('--cache-max-age', type=float, choices=(30.0,), default=30.0)
    parser.add_argument('--progress-checkpoint', type=Path, required=True)
    parser.add_argument('--progress-operation', required=True)
    args = parser.parse_args(argv)
    control, bundle = Path(os.path.abspath(args.control)), Path(os.path.abspath(__file__)).parent
    cfg, policy = validate_bundle(control, bundle, args.kind)
    validate_imports(bundle)
    reader, opener, parent = proof.io.read_json, proof.engine._directory_opener, proof.engine._open_parent
    guard, predecessor = checkpoint_identity(control, bundle, args.progress_checkpoint,
        args.progress_operation, args.kind, reader, policy)
    proof.require_predecessor_exited(predecessor)
    try:
        proof.io.read_json = guard.read_json
        proof.engine.configure_directory_opener(proof.native.open_parent_readonly)
        proof.engine._open_parent = frontier.FrontierParentOpener(cfg['destination'], parent,
            check=proof.engine._check_cancel, opener=proof.native.open_parent_readonly,
            anchor_opener=proof.native.open_parent_readonly)
        return proof.original.main([args.kind, '--control', str(control), '--threads', '64',
                                    '--nice', '0', '--cache-max-age', '30.0'])
    finally:
        proof.engine._open_parent = parent
        proof.engine.configure_directory_opener(opener)
        proof.io.read_json = reader


if __name__ == '__main__':
    main()
