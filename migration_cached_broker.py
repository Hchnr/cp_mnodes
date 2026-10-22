#!/usr/bin/env python3
"""Launch the unchanged parallel broker with a process-local state cache.

Example:
  migration_cached_broker.py --control CONTROL --port 37651

Only CONTROL/state.json is cached. All other JSON, SQLite, HTTP, locks and the
64-slot filesystem concurrency behavior remain in migration_broker_parallel.
This is a fresh-process launcher, never a patch to a running broker.
"""

import argparse
import errno
import importlib
import math
import os
import pathlib
import runpy
import sys
import time

from migration_state_cache import StateCache


def wait_until_ready(cache, control, timeout=10.0):
    """Wait a bounded time for a usable first state; check STOP directly."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('cache startup timeout must be positive and finite')
    stop_path = pathlib.Path(control) / 'STOP'
    deadline = time.monotonic() + timeout
    last_error = None
    while True:
        if stop_path.exists():
            raise RuntimeError('STOP is present; cached broker must not start')
        try:
            cache.read()
            return
        except OSError as exc:
            last_error = exc
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OSError(errno.ETIMEDOUT, 'state cache did not become ready before startup timeout',
                          cache.state_path) from last_error
        time.sleep(min(0.05, remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Other arguments, including --port, are forwarded to the original broker CLI.')
    parser.add_argument('--control', required=True)
    parser.add_argument('--cache-refresh-interval', type=float, default=0.5,
                        help='seconds between state refreshes (default: 0.5)')
    parser.add_argument('--cache-max-age', type=float, default=3.0,
                        help='fail state reads past this age in seconds (default: 3)')
    parser.add_argument('--cache-startup-timeout', type=float, default=10.0,
                        help='bounded wait for a fresh first state before HTTP startup (default: 10)')
    args, broker_args = parser.parse_known_args(argv)
    if not math.isfinite(args.cache_startup_timeout) or args.cache_startup_timeout <= 0:
        parser.error('cache startup timeout must be positive and finite')
    if (pathlib.Path(args.control) / 'STOP').exists():
        raise RuntimeError('STOP is present; cached broker must not start')

    bundle = pathlib.Path(os.path.abspath(__file__)).parent
    script = bundle / 'migration_broker_parallel.py'
    original_argv = sys.argv
    original_path = list(sys.path)
    io_module = broker_module = None
    original_read_json = original_broker_read = original_broker_class = None
    cache = None
    try:
        sys.path.insert(0, str(bundle))
        io_module = importlib.import_module('migration_io')
        original_read_json = io_module.read_json
        cache = StateCache(pathlib.Path(args.control) / 'state.json', original_read_json,
                           refresh_interval=args.cache_refresh_interval, max_age=args.cache_max_age)
        # Keep the original reader in the refresher, then patch before either
        # broker import. The unchanged parallel script imports this alias.
        io_module.read_json = cache.read_json
        cache.start()
        wait_until_ready(cache, args.control, args.cache_startup_timeout)
        broker_module = importlib.import_module('migration_broker')
        original_broker_read = broker_module.read_json
        if original_broker_read == cache.read_json:
            # Newly imported legacy module captured our temporary dispatcher.
            original_broker_read = original_read_json
        original_broker_class = broker_module.Broker
        # A new process normally imports the patched alias automatically.
        # Explicit rebinding also makes repeated local fixtures independent.
        broker_module.read_json = cache.read_json
        if (pathlib.Path(args.control) / 'STOP').exists():
            raise RuntimeError('STOP is present; cached broker must not start')
        sys.argv = [str(script), '--control', args.control] + broker_args
        runpy.run_path(str(script), run_name='__main__')
    finally:
        if cache is not None:
            cache.close()
        if broker_module is not None:
            broker_module.read_json = original_broker_read
            broker_module.Broker = original_broker_class
        if io_module is not None and original_read_json is not None:
            io_module.read_json = original_read_json
        sys.argv = original_argv
        sys.path[:] = original_path
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
