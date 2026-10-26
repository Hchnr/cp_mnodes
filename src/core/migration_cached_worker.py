#!/usr/bin/env python3
"""Launch an unchanged worker with a process-local state.json read cache.

Examples (same control, lanes, ownership, I/O budgets and copy verification):
  migration_cached_worker.py primary --control CONTROL --threads 64
  migration_cached_worker.py supplement --control CONTROL --threads 64 --nice 10

Only CONTROL/state.json is cached. STOP checks and all configuration, node,
receipt and other JSON reads retain their original behavior. This launcher is
for a fresh process after operator-controlled drain, never a live patch.
"""

import argparse
import copy
import importlib
import math
import os
import pathlib
import runpy
import socket
import sys

from migration_state_cache import StateCache


PATH_POLICY = {'scope': 'destination-opath-checkpoint-v1'}
PROGRESS_COUNTERS = ('bytes', 'streamed_bytes', 'verified_bytes', 'verified_files',
                     'completed_batches', 'broker_retries')


class StartupProgressError(RuntimeError):
    """Not OSError/ValueError: the legacy worker must not catch and zero it."""


def _check_progress(before, after):
    def compare(old, new, prefix=''):
        if not isinstance(new, dict):
            raise StartupProgressError('progress mapping missing: ' + prefix)
        for key, value in old.items():
            if isinstance(value, dict):
                compare(value, new.get(key), prefix + key + '.')
            elif (not isinstance(value, (int, float)) or isinstance(value, bool)
                  or not math.isfinite(value) or value < 0
                  or not isinstance(new.get(key), (int, float))
                  or isinstance(new[key], bool) or not math.isfinite(new[key])
                  or new[key] < value):
                raise StartupProgressError('progress counter invalid or regressed: ' + prefix + key)
    for field in PROGRESS_COUNTERS:
        if field not in before:
            raise StartupProgressError('checkpoint counter missing: ' + field)
    compare({key: before[key] for key in PROGRESS_COUNTERS}, after)
    for field in ('counts', 'stages'):
        if not isinstance(before.get(field), dict):
            raise StartupProgressError('checkpoint mapping missing: ' + field)
        compare(before[field], after.get(field), field + '.')
    if (not before.get('run_started') or
            after.get('run_started') != before['run_started']):
        raise StartupProgressError('progress run_started changed or missing')


class StartupProgressGuard:
    """Validate the drained lane's actual first read; never fabricate history."""
    def __init__(self, control, kind, checkpoint, operation, reader, bundle=None):
        self.reader = reader
        self.control = pathlib.Path(os.path.abspath(control))
        self.host = socket.gethostname()
        self.kind = kind
        self.node_path = self.control / ('nodes' if kind == 'primary' else 'nodes-extra') / (self.host + '.json')
        self.consumed = False
        try:
            checkpoint = pathlib.Path(checkpoint)
            if (not checkpoint.is_absolute() or not operation or
                    checkpoint.name != self.host + '-' + kind + '.json' or
                    (bundle is not None and checkpoint.parent != pathlib.Path(bundle) / 'cache-age-rollout')):
                raise StartupProgressError('startup checkpoint path/operation differs')
            record = reader(checkpoint)
            if (record.get('operation') != operation or record.get('host') != self.host
                    or record.get('lane') != kind
                    or record.get('status') not in ('idle_verified', 'replacing', 'launched')
                    or record.get('lane_leases') != {}):
                raise StartupProgressError('startup checkpoint is not this drained lane')
            self.before = copy.deepcopy(record['idle'])
            self.expected_pid = record['expected']['pid']
            if (self.before.get('pid') != self.expected_pid or self.before.get('active')
                    or self.before.get('host') != self.host):
                raise StartupProgressError('startup idle identity differs')
            _check_progress(self.before, self.before)
        except StartupProgressError:
            raise
        except Exception as exc:
            raise StartupProgressError('cannot read startup progress checkpoint') from exc

    def preflight(self):
        try:
            value = self.reader(self.node_path)
            if (value.get('host') != self.host or value.get('pid') != self.expected_pid
                    or value.get('active') or value.get('thread_error')):
                raise StartupProgressError('old lane identity/idle state changed')
            _check_progress(self.before, value)
            return value
        except StartupProgressError:
            raise
        except Exception as exc:
            raise StartupProgressError('cannot read old lane progress; refusing zero initialization') from exc

    def read_json(self, path):
        if not self.consumed and pathlib.Path(os.path.abspath(path)) == self.node_path:
            value = self.preflight()
            self.consumed = True
            return value
        return self.reader(path)


def _directory_opener_for_bundle(bundle, reader, control=None):
    """Read an immutable opt-in policy; reject malformed policy before start."""
    policy_path = bundle / 'worker-directory-policy.json'
    try:
        policy_path.lstat()
    except FileNotFoundError:
        return None
    policy = reader(policy_path)
    if policy == PATH_POLICY:
        if control is None:
            raise ValueError('destination O_PATH policy requires an explicit control')
        cfg = reader(pathlib.Path(control) / 'config.json')
        destination = os.path.abspath(cfg['destination'])
        native = importlib.import_module('migration_linux_open')
        def opener(root, rel):
            fn = (native.open_parent_path if rel != '__directory_open_root__' and
                  os.path.abspath(os.fsdecode(root)) == destination
                  else native.open_parent_readonly)
            return fn(root, rel)
        return opener
    if policy != {'scope': 'native-safe-parent-open-v1'}:
        raise ValueError('invalid worker directory-open policy')
    return importlib.import_module('migration_linux_open').open_parent_readonly


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Remaining arguments are forwarded to the selected original worker CLI.')
    parser.add_argument('kind', choices=('primary', 'supplement'),
                        help='primary: migration_run.py worker; supplement: migration_worker128_run.py')
    parser.add_argument('--control', required=True, help='existing migration control directory')
    parser.add_argument('--cache-refresh-interval', type=float, default=0.5,
                        help='seconds between state refreshes (default: 0.5)')
    parser.add_argument('--cache-max-age', type=float, default=3.0,
                        help='fail state reads when refresh age exceeds this many seconds (default: 3)')
    parser.add_argument('--progress-checkpoint', help='drained lane checkpoint, required for O_PATH rollout')
    parser.add_argument('--progress-operation', help='exact checkpoint operation identity')
    args, worker_args = parser.parse_known_args(argv)

    bundle = pathlib.Path(os.path.abspath(__file__)).parent
    script = bundle / ('migration_run.py' if args.kind == 'primary'
                       else 'migration_worker128_run.py')
    original_argv = sys.argv
    original_path = list(sys.path)
    io_module = None
    original_read_json = None
    worker_module = None
    original_directory_opener = None
    cache = None
    try:
        sys.path.insert(0, str(bundle))
        io_module = importlib.import_module('migration_io')
        original_read_json = io_module.read_json
        opener = _directory_opener_for_bundle(bundle, original_read_json, args.control)
        guarded = (bundle / 'worker-directory-policy.json').exists() and original_read_json(
            bundle / 'worker-directory-policy.json') == PATH_POLICY
        guard = None
        if guarded or args.progress_checkpoint or args.progress_operation:
            if not args.progress_checkpoint or not args.progress_operation:
                raise StartupProgressError('required startup progress checkpoint is missing')
            guard = StartupProgressGuard(args.control, args.kind, args.progress_checkpoint,
                                         args.progress_operation, original_read_json, bundle)
            guard.preflight()
        worker_module = importlib.import_module('migration_worker')
        original_directory_opener = worker_module._directory_opener
        worker_module.configure_directory_opener(opener)
        cache = StateCache(pathlib.Path(args.control) / 'state.json', original_read_json,
                           refresh_interval=args.cache_refresh_interval,
                           max_age=args.cache_max_age)
        # The refresher retains original_read_json, avoiding recursion through
        # this dispatcher. run_path imports the patched function afterwards.
        if guard is None:
            io_module.read_json = cache.read_json
        else:
            # Preserve the same fail-closed check on the worker's actual first
            # node read under its singleton flock, not merely a preflight.
            guard.reader = cache.read_json
            io_module.read_json = guard.read_json
        cache.start()
        sys.argv = [str(script)]
        if args.kind == 'primary':
            sys.argv.append('worker')
        sys.argv.extend(['--control', args.control])
        sys.argv.extend(worker_args)
        runpy.run_path(str(script), run_name='__main__')
    finally:
        if cache is not None:
            cache.close()
        if io_module is not None and original_read_json is not None:
            io_module.read_json = original_read_json
        if worker_module is not None:
            worker_module.configure_directory_opener(original_directory_opener)
        sys.argv = original_argv
        sys.path[:] = original_path
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
