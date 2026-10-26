#!/usr/bin/env python3
"""UNDEPLOYED P4 worker component, not a complete P4 copy engine.

Launch only after the independent P4 dependency/dispatch checks succeed. The
existing process_entry and copy/SHA implementation are delegated unchanged.
This wrapper adds exact scope guards and a process-local state cache; it never
changes an already running worker. Directory/hardlink finalizers must also use
the guards below, but are not implemented or launched by this component.
"""
import argparse
import importlib
import os
from pathlib import Path
import re

from migration_state_cache import StateCache, lexical_absolute


SOURCE = '/share/project'
DESTINATION = '/share-mixed/project'
SCOPE = 'project_priority4'
RUN_ID = re.compile(r'hcr-copy-p4-[A-Za-z0-9-]+\Z')
STAGE = re.compile(r'p4-r([123])\Z')


def components(rel, allow_root=False):
    if not isinstance(rel, str) or '\x00' in rel or rel.startswith('/'):
        raise ValueError('P4 requires a safe relative path')
    if allow_root and rel in ('', '.'):
        return ()
    parts = tuple(rel.split('/'))
    if any(part in ('', '.', '..') for part in parts):
        raise ValueError('P4 requires a safe relative path')
    return parts


def control_name(control):
    """Validate one immediate destination child without resolving any links."""
    if not isinstance(control, str) or not control.startswith(DESTINATION + '/'):
        raise ValueError('P4 control must be under the destination mount')
    name = control[len(DESTINATION) + 1:]
    if len(components(name)) != 1 or not name.startswith('.hcr-copy-'):
        raise ValueError('P4 control must be a dedicated immediate child')
    return name


class ScopeGuard:
    def __init__(self, control, cfg):
        self.control = lexical_absolute(control)
        self.control_name = control_name(self.control)
        run_id = cfg.get('run_id')
        if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
            raise ValueError('P4 requires a dedicated P4 run_id')
        if self.control != DESTINATION + '/.' + run_id:
            raise ValueError('P4 run_id and control directory differ')
        self.run_id = run_id
        covered_by = cfg.get('covered_by_controls')
        if not isinstance(covered_by, list) or not covered_by:
            raise ValueError('P4 requires predecessor coverage controls')
        self.covered_by = tuple(covered_by)
        if self.control in self.covered_by:
            raise ValueError('P4 control cannot cover itself')
        protected = cfg.get('protected_destination_relatives')
        if not isinstance(protected, list):
            raise ValueError('P4 requires an explicit protected destination list')
        for rel in protected:
            if len(components(rel)) != 1:
                raise ValueError('protected destinations must be immediate children')
        self.explicit_protected = tuple(protected)
        self.protected = frozenset(
            ['eai_pwm', '.hcr-copy-monitor', self.control_name] + protected +
            [control_name(path) for path in self.covered_by])
        self.assert_config(cfg)

    def assert_config(self, cfg):
        if (cfg.get('source'), cfg.get('destination'), cfg.get('migration_scope'),
                cfg.get('exclude'), cfg.get('run_id')) != (
                SOURCE, DESTINATION, SCOPE, ['eai_pwm'], self.run_id):
            raise ValueError('P4 configuration scope changed or is not exact')
        if (cfg.get('covered_by_controls') != list(self.covered_by) or
                cfg.get('protected_destination_relatives') != list(self.explicit_protected)):
            raise ValueError('P4 coverage/protection configuration changed')
        for key, expected in [('threads', 64), ('copy_threads_per_node', 128),
                              ('supplement_threads', 64)]:
            if type(cfg.get(key)) is not int or cfg[key] != expected:
                raise ValueError('P4 copy lanes must remain 64 + 64 threads')
        rates = [cfg.get('io_bytes_per_second'), cfg.get('supplement_io_bytes_per_second')]
        if any(type(rate) is not int or not 0 < rate <= 10_000_000_000 for rate in rates):
            raise ValueError('P4 primary/supplement I/O budgets must be positive and <=10GB/s each')
        return cfg

    def assert_state(self, state):
        stage = state.get('stage')
        if stage is not None and (not isinstance(stage, str) or not STAGE.fullmatch(stage)):
            raise ValueError('P4 state has a non-P4 stage')
        if state.get('recovery_warm'):
            raise ValueError('P4 cannot adopt the eai_pwm warm recovery stage')
        if state.get('phase') is not None and (type(state['phase']) is not int or state['phase'] != 4):
            raise ValueError('P4 state has a non-P4 phase')
        if state.get('round') is not None:
            round_number = state['round']
            if type(round_number) is not int or round_number not in (1, 2, 3):
                raise ValueError('P4 state has an invalid round')
            if stage is not None and stage != 'p4-r' + str(round_number):
                raise ValueError('P4 state round and stage differ')
        return state

    def assert_path(self, rel):
        parts = components(rel)
        if parts[0] in self.protected:
            raise ValueError('P4 destination is covered or reserved: ' + parts[0])
        return rel

    def destination_metadata_policy(self, rel):
        if components(rel, allow_root=True) == ():
            return 'preserve_mount_root'
        self.assert_path(rel)
        return 'copy_child_metadata'

    def assert_task(self, task, entry):
        if (type(task.get('id')) is not int or task['id'] < 0 or
                type(task.get('phase')) is not int or task['phase'] != 4 or
                type(task.get('round')) is not int or task['round'] not in (1, 2, 3) or
                task.get('op') not in ('copy', 'sync')):
            raise ValueError('P4 worker received an invalid task')
        if entry.get('kind') not in ('file', 'symlink'):
            raise ValueError('P4 payload worker accepts only files and symlinks')
        self.assert_path(entry.get('rel'))

    def wrap(self, process_entry):
        def guarded(cfg, task, entry):
            try:
                self.assert_config(cfg)
                self.assert_task(task, entry)
            except (ValueError, TypeError, KeyError) as exc:
                # Return a durable issue; do not strand the entire batch or
                # let an unsafe entry reach source/destination filesystem I/O.
                return {'rel': entry.get('rel'), 'kind': entry.get('kind'),
                        'status': 'error', 'bytes': 0, 'scope_violation': True,
                        'detail': 'Priority4Scope: ' + str(exc)}
            return process_entry(cfg, task, entry)
        return guarded


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('primary', 'supplement'))
    parser.add_argument('--control', required=True)
    parser.add_argument('--threads', type=int, choices=(64,), default=64)
    parser.add_argument('--nice', type=int, choices=range(20), default=0)
    parser.add_argument('--cache-refresh-interval', type=float, default=0.5)
    parser.add_argument('--cache-max-age', type=float, default=30.0)
    args = parser.parse_args(argv)
    io = importlib.import_module('migration_io')
    saved_reader = io.read_json
    cfg = saved_reader(Path(args.control) / 'config.json')
    guard = ScopeGuard(args.control, cfg)
    config_path = lexical_absolute(Path(args.control) / 'config.json')
    state_path = lexical_absolute(Path(args.control) / 'state.json')

    def scoped_reader(path):
        value = saved_reader(path)
        absolute = lexical_absolute(path)
        if absolute == config_path:
            guard.assert_config(value)
        elif absolute == state_path:
            guard.assert_state(value)
        return value

    cache = StateCache(state_path, scoped_reader,
                       refresh_interval=args.cache_refresh_interval,
                       max_age=args.cache_max_age)
    saved = []
    def patch(module, name, value):
        saved.append((module, name, getattr(module, name)))
        setattr(module, name, value)
    try:
        # Import both lanes before changing any aliases. Module imports only
        # define functions; this also permits exact restoration in fixtures.
        core = importlib.import_module('migration_run')
        module = (core if args.kind == 'primary' else
                  importlib.import_module('migration_worker128_run'))
        guarded = guard.wrap(core.process_entry)
        patch(io, 'read_json', cache.read_json)
        patch(core, 'process_entry', guarded)
        patch(core, 'read_json', cache.read_json)
        if args.kind == 'supplement':
            patch(module, 'process_entry', guarded)
            patch(module, 'read_json', cache.read_json)
        if args.nice:
            os.nice(args.nice)
        cache.start()
        # Call the imported module, not run_path('__main__'): both lanes must
        # use these guarded module globals, including the known-copy fast path.
        module.worker(args.control, args.threads)
    finally:
        cache.close()
        for module, name, previous in reversed(saved):
            setattr(module, name, previous)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
