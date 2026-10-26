#!/usr/bin/env python3
"""UNDEPLOYED, checkpoint-only P4 native-readonly parent-open canary.

This entry never drains, signals, restarts or attaches to another process. A
separate, explicitly authorized controller must first prove the predecessor is
idle and lease-free, stop only that idle lane, and publish its checkpoint. The
unchanged P4 worker retains its own singleton flock and all copy/scope logic.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket

import migration_io as io
import migration_linux_open as native
import migration_priority4_worker as original
import migration_worker as engine
from migration_cached_worker import StartupProgressError, StartupProgressGuard


SCOPE = 'priority4-native-parent-open-checkpoint-v1'
AUTHORIZATION = '允许，先做一个 P4 复制进程的滚动对照'
ADDITIONS = frozenset(('migration_priority4_path_worker.py', 'migration_linux_open.py',
                       'migration_cached_worker.py', 'path-open-policy.json'))


def identity(pid):
    fields = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(') ', 1)[1].split()
    return dict(pid=pid, state=fields[0], start_ticks=fields[19])


def static_json(path):
    if path.is_symlink() or (path.parent / '.snapshots' / path.name).exists():
        raise StartupProgressError('static canary proof must not be linked or versioned')
    return json.loads(path.read_bytes())


def digest_file(path):
    if path.is_symlink() or not path.is_file():
        raise StartupProgressError('canary source must be a regular local bundle file')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_bundle(control, bundle, lane):
    """Bind the complete original P4 source manifest, not just six engines."""
    if (bundle.parent != control or not bundle.name.startswith('p4-copy-path-')
            or bundle.is_symlink() or control.is_symlink()):
        raise StartupProgressError('an independent immutable P4 canary bundle is required')
    cfg = io.read_json(control / 'config.json')
    original.ScopeGuard(str(control), cfg)
    original_manifest = static_json(control / 'source-sha256.json')
    manifest = static_json(bundle / 'source-sha256.json')
    required = {'migration_priority4_worker.py', 'migration_run.py', 'migration_worker.py',
                'migration_worker128_run.py', 'migration_io.py', 'migration_state_cache.py'}
    if (not isinstance(original_manifest, dict) or not required.issubset(original_manifest)
            or not isinstance(manifest, dict) or set(manifest) != set(original_manifest) | ADDITIONS):
        raise StartupProgressError('complete frozen P4 source manifest required')
    for name, expected in manifest.items():
        if (Path(name).name != name or name in ('.', '..')
                or digest_file(bundle / name) != expected):
            raise StartupProgressError('P4 canary source digest differs: ' + name)
        if name in original_manifest and (expected != original_manifest[name]
                or digest_file(control / name) != original_manifest[name]):
            raise StartupProgressError('original P4 engine/scope/cache source changed: ' + name)
    policy = static_json(bundle / 'path-open-policy.json')
    expected_policy = dict(scope=SCOPE, control=str(control), run_id=cfg['run_id'],
        job=cfg['job'], node='worker-10', lane='supplement', directory_strategy='native_readonly',
        predecessor_manifest_sha256=digest_file(control / 'source-sha256.json'))
    if (policy != expected_policy or lane != policy['lane']
            or socket.gethostname() != 'job-' + cfg['job'] + '-' + policy['node']):
        raise StartupProgressError('P4 path canary policy/node/lane differs')
    return cfg, policy


def validate_imports(bundle):
    # The executable must import its frozen engines, not repository modules or
    # modules from an earlier deployment that happen to have matching names.
    import migration_cached_worker as checkpoint
    import migration_run as core
    import migration_worker128_run as auxiliary
    import migration_state_cache as state_cache
    for module in (io, original, engine, native, checkpoint, core, auxiliary, state_cache):
        if Path(os.path.abspath(module.__file__)).parent != bundle:
            raise StartupProgressError('P4 canary imported a foreign runtime module')


def checkpoint_identity(control, bundle, checkpoint, operation, lane, reader):
    record = reader(checkpoint)
    if (record.get('control') != str(control) or record.get('migration_scope') != SCOPE
            or record.get('directory_strategy') != 'native_readonly'
            or record.get('user_authorization') != AUTHORIZATION):
        raise StartupProgressError('explicit P4 copy-canary checkpoint/authorization required')
    guard = StartupProgressGuard(control, lane, checkpoint, operation, reader, bundle)
    guard.preflight()
    expected = record.get('expected', {})
    argv = expected.get('argv', [])
    if (type(expected.get('pid')) is not int or expected['pid'] <= 0
            or not str(expected.get('starttime_ticks', '')).isdigit()
            or not isinstance(argv, list)
            or str(control / 'migration_priority4_worker.py') not in argv
            or argv.count('--control') != 1
            or argv.index('--control') + 1 >= len(argv)
            or argv[argv.index('--control') + 1] != str(control)
            or lane not in argv):
        raise StartupProgressError('exact original P4 lane identity required')
    return guard, expected


def require_predecessor_exited(expected):
    try:
        current = identity(expected['pid'])
    except (FileNotFoundError, ProcessLookupError):
        return
    # Empty argv during exit_mm is not proof of exit. Conversely, a terminal
    # process need not retain argv. Never treat a reused PID as our predecessor.
    if (current['start_ticks'] != str(expected['starttime_ticks'])
            or current['state'] not in ('Z', 'X')):
        raise StartupProgressError('original P4 lane exit is not proven')


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
    control = Path(os.path.abspath(args.control))
    bundle = Path(os.path.abspath(__file__)).parent
    validate_bundle(control, bundle, args.kind)
    validate_imports(bundle)
    reader, opener = io.read_json, engine._directory_opener
    guard, expected = checkpoint_identity(control, bundle, args.progress_checkpoint,
                                          args.progress_operation, args.kind, reader)
    require_predecessor_exited(expected)
    try:
        # The legacy P4 wrapper captures this dispatcher, then its worker reads
        # its actual first node checkpoint under the unchanged singleton flock.
        # RuntimeError-derived failures cannot enter its zero-counter fallback.
        io.read_json = guard.read_json
        engine.configure_directory_opener(native.open_parent_readonly)
        return original.main([args.kind, '--control', str(control), '--threads', '64',
                              '--nice', '0', '--cache-max-age', '30.0'])
    finally:
        engine.configure_directory_opener(opener)
        io.read_json = reader


if __name__ == '__main__':
    main()
