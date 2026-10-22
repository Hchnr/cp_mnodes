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
import importlib
import os
import pathlib
import runpy
import sys

from migration_state_cache import StateCache


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
    args, worker_args = parser.parse_known_args(argv)

    bundle = pathlib.Path(os.path.abspath(__file__)).parent
    script = bundle / ('migration_run.py' if args.kind == 'primary'
                       else 'migration_worker128_run.py')
    original_argv = sys.argv
    original_path = list(sys.path)
    io_module = None
    original_read_json = None
    cache = None
    try:
        sys.path.insert(0, str(bundle))
        io_module = importlib.import_module('migration_io')
        original_read_json = io_module.read_json
        cache = StateCache(pathlib.Path(args.control) / 'state.json', original_read_json,
                           refresh_interval=args.cache_refresh_interval,
                           max_age=args.cache_max_age)
        # The refresher retains original_read_json, avoiding recursion through
        # this dispatcher. run_path imports the patched function afterwards.
        io_module.read_json = cache.read_json
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
        sys.argv = original_argv
        sys.path[:] = original_path
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
