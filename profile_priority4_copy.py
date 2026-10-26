#!/usr/bin/env python3
"""Bounded scratch-only phase profiling of the unchanged live P4 copy engine.

Reuses the existing benchmark's exact control, source-identity, byte, time and
process guards. Unlike that benchmark, parent scaffolding is inside the timed
interval. No production files, claims, services, tuning or counters change.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import sys
import threading
import time

import benchmark_priority4_copy_paths as benchmark
from migration_parent_frontier import FrontierParentOpener


SYSCALLS = frozenset(('open', 'close', 'stat', 'fstat', 'mkdir', 'read', 'write',
                     'lseek', 'ftruncate', 'fsync', 'link', 'unlink', 'replace',
                     'listxattr', 'getxattr', 'setxattr', 'fchmod', 'fchown', 'utime'))


def add_measurement(record, key, seconds, failed=False):
    value = record.setdefault(key, dict(calls=0, seconds=0.0, max_seconds=0.0, failures=0))
    value['calls'] += 1
    value['seconds'] += seconds
    value['max_seconds'] = max(value['max_seconds'], seconds)
    value['failures'] += bool(failed)


class OSProxy:
    """Instrument only the private engine's os reference, never global os."""
    def __init__(self, original, profiler):
        self.original = original
        self.profiler = profiler
        self.cache = {}

    def __getattr__(self, name):
        value = getattr(self.original, name)
        if name not in SYSCALLS or not callable(value):
            return value
        if name not in self.cache:
            def measured(*args, **kwargs):
                record = getattr(self.profiler.local, 'record', None)
                if record is None:
                    return value(*args, **kwargs)
                phase = self.profiler.local.phase
                started, failed = time.perf_counter(), True
                try:
                    result = value(*args, **kwargs)
                    failed = False
                    return result
                finally:
                    add_measurement(record['syscalls'], phase + ':' + name,
                                    time.perf_counter() - started, failed)
            self.cache[name] = measured
        return self.cache[name]


class CopyProfiler:
    def __init__(self, engine, source, destination):
        self.engine = engine
        self.source = os.path.abspath(source)
        self.destination = os.path.abspath(destination)
        self.local = threading.local()
        self.original = {}

    def phase(self, name, function):
        def measured(*args, **kwargs):
            record = getattr(self.local, 'record', None)
            if record is None:
                return function(*args, **kwargs)
            label = name(*args, **kwargs) if callable(name) else name
            previous = self.local.phase
            self.local.phase = label
            started, failed = time.perf_counter(), True
            try:
                result = function(*args, **kwargs)
                failed = False
                return result
            finally:
                add_measurement(record['phases'], label, time.perf_counter() - started, failed)
                self.local.phase = previous
        return measured

    def __enter__(self):
        self.original = {key: getattr(self.engine, key)
                         for key in ('os', '_open_parent', '_metadata_fd', '_hash_fd')}
        def parent_label(root, rel, create=False):
            path = os.path.abspath(os.fsdecode(root))
            if path == self.source:
                return 'source_parent'
            if path == self.destination:
                return 'destination_parent'
            raise ValueError('unexpected benchmark root')
        self.engine.os = OSProxy(self.original['os'], self)
        self.engine._open_parent = self.phase(parent_label, self.original['_open_parent'])
        self.engine._metadata_fd = self.phase('metadata', self.original['_metadata_fd'])
        self.engine._hash_fd = self.phase('readback_sha', self.original['_hash_fd'])
        return self

    def __exit__(self, *unused):
        for key, value in self.original.items():
            setattr(self.engine, key, value)

    def copy(self, function, *args, **kwargs):
        if getattr(self.local, 'record', None) is not None:
            raise RuntimeError('nested copy profile')
        record = dict(phases={}, syscalls={})
        self.local.record, self.local.phase = record, 'body'
        started = time.perf_counter()
        try:
            result = function(*args, **kwargs)
            record['copy_seconds'] = time.perf_counter() - started
            return result, record
        finally:
            self.local.record = None


def summarize(records):
    result = dict(files=len(records), copy_thread_seconds=sum(r['copy_seconds'] for r in records),
                  phases={}, syscalls={})
    for record in records:
        for group in ('phases', 'syscalls'):
            for key, value in record[group].items():
                target = result[group].setdefault(key, dict(calls=0, seconds=0.0,
                                                          max_seconds=0.0, failures=0))
                for field in ('calls', 'seconds', 'failures'):
                    target[field] += value[field]
                target['max_seconds'] = max(target['max_seconds'], value['max_seconds'])
    for group in ('phases', 'syscalls'):
        for value in result[group].values():
            value['mean_ms'] = value['seconds'] / value['calls'] * 1000
            value['fraction_of_copy_thread_time'] = value['seconds'] / result['copy_thread_seconds']
    return result


def observe_frontier(helper, destination, entries, check):
    """Read only real directory ancestors; never stat/open a payload leaf."""
    if not 1 <= len(entries) <= 512:
        raise ValueError('bounded entry sample required')
    began = time.time()
    boundaries = {}
    for entry in entries:
        check()
        rel = entry['rel']
        parent = rel.rsplit('/', 1)[0] if '/' in rel else ''
        if parent in boundaries:
            continue
        parts = parent.split('/') if parent else []
        if len(parts) > 128:
            raise ValueError('directory depth exceeds diagnostic bound')
        for depth in range(len(parts), -1, -1):
            try:
                fd, _ = helper.open_parent_path(destination,
                    '/'.join(parts[:depth] + ['__readonly_frontier_probe__']))
            except FileNotFoundError:
                continue
            else:
                os.close(fd)
                boundaries[parent] = dict(existing_prefix='/'.join(parts[:depth]),
                                          missing_depth=len(parts) - depth)
                break
        else:
            raise RuntimeError('production destination root does not exist')
    return dict(started=began, sampled_at=time.time(), destination=str(destination),
                boundaries=boundaries, distinct_parents=len(boundaries),
                missing_depth_histogram=dict(collections.Counter(
                    x['missing_depth'] for x in boundaries.values())),
                caveat='Live parents may be created during observation; this is not an atomic filesystem snapshot.')


def run_trial(engine, helper, source, destination, entries, hashes, mode, run_id, check,
              *, threads=64, scaffolding=None):
    if (not entries or destination.is_symlink() or not destination.is_dir()
            or any(destination.iterdir()) or not 1 <= threads <= 64):
        raise ValueError('an empty exclusive trial directory and bounded threads are required')
    engine.configure_directory_opener(None)
    if scaffolding is not None:
        expected = {e['rel'].rsplit('/', 1)[0] if '/' in e['rel'] else '' for e in entries}
        if set(scaffolding['boundaries']) != expected:
            raise ValueError('frontier snapshot differs from trial parents')
        prefixes = {value['existing_prefix'] for value in scaffolding['boundaries'].values()}
        for prefix in sorted(prefixes):
            check()
            engine.ensure_destination_directory(str(destination), prefix)
    for entry in entries:
        benchmark.stable_source(engine, source, entry)
    candidate = mode in ('frontier_readonly', 'frontier_path_anchor')
    strategy = 'native_readonly' if candidate else mode
    engine.configure_directory_opener(benchmark.directory_opener(strategy, destination, helper))
    original_parent = engine._open_parent
    adapter = None
    if candidate:
        adapter = FrontierParentOpener(destination, original_parent, check=engine._check_cancel,
            opener=helper.open_parent_readonly,
            anchor_opener=helper.open_parent_path if mode == 'frontier_path_anchor' else helper.open_parent_readonly)
        engine._open_parent = adapter
    try:
        with CopyProfiler(engine, source, destination) as profiler:
            if adapter is not None:
                adapter.os = engine.os
            def copy(entry):
                check()
                result, profile = profiler.copy(engine.copy_entry, source, str(destination), entry,
                                                run_id, verify=True)
                if (result.get('status') != 'copied' or result.get('metadata_warnings')
                        or result.get('sha256') != hashes[entry['rel']]):
                    raise RuntimeError('profile copy validation failed: ' + str(result))
                actual = result['dst_stat']
                for key in ('size', 'uid', 'gid', 'mtime_ns'):
                    if actual[key] != entry[key]:
                        raise RuntimeError('profile target metadata differs: ' + key)
                if actual['mode'] & 0o7777 != entry['mode'] & 0o7777:
                    raise RuntimeError('profile target mode differs')
                return result['bytes'], profile
            started, cpu = time.monotonic(), time.process_time()
            with ThreadPoolExecutor(max_workers=threads) as pool:
                values = list(pool.map(copy, entries))
            elapsed = time.monotonic() - started
    finally:
        engine._open_parent = original_parent
        engine.configure_directory_opener(None)
    check()
    return dict(mode=mode, seconds=elapsed, cpu_seconds=time.process_time() - cpu,
                files=len(entries), bytes=sum(value[0] for value in values), threads=threads,
                files_s=len(entries) / elapsed,
                logical_GB_s=sum(value[0] for value in values) / elapsed / 1e9,
                parent_scaffolding_timed=True,
                scaffolding_mode='production_frontier_snapshot' if scaffolding else 'completely_empty_tree',
                frontier_missing_depth_histogram=scaffolding['missing_depth_histogram'] if scaffolding else None,
                distinct_parents=len({e['rel'].rsplit('/', 1)[0] for e in entries}),
                profile=summarize([value[1] for value in values]),
                source_identity_and_destination_sha_verified=True,
                destination=str(destination),
                parent_candidate_sha256=hashlib.sha256(
                    Path(sys.modules[FrontierParentOpener.__module__].__file__).read_bytes()).hexdigest(),
                profile_driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, required=True)
    parser.add_argument('--frontier', action='store_true',
                        help='prebuild only the directory prefixes currently present in production')
    parser.add_argument('--candidate', action='store_true',
                        help='scratch-only nearest-ancestor comparison; never installs a worker')
    args = parser.parse_args(argv)
    previous_argv, previous_trial, previous_note, previous_modes = (
        sys.argv, benchmark.run_trial, benchmark.SCAFFOLDING_NOTE, benchmark.MODES)
    frontier = None
    def trial(engine, helper, source, destination, entries, hashes, mode, run_id, check, **kwargs):
        nonlocal frontier
        if args.frontier and frontier is None:
            cfg = benchmark.read_json(args.control / 'config.json')
            frontier = observe_frontier(helper, cfg['destination'], entries, check)
            benchmark.write_json(destination.parent / 'frontier-scaffolding.json', frontier)
        return run_trial(engine, helper, source, destination, entries, hashes, mode, run_id, check,
                         scaffolding=frontier, **kwargs)
    # Retain the existing exact-control/actor/scope/byte/deadline/owner gates.
    benchmark.run_trial = trial
    if args.candidate:
        benchmark.MODES = ('native_readonly', 'frontier_readonly', 'frontier_path_anchor',
                           'frontier_path_anchor', 'frontier_readonly', 'native_readonly')
    benchmark.SCAFFOLDING_NOTE = ('Only production-existing prefixes are prebuilt; missing parents are timed. '
                                 if args.frontier else 'All parent creation is timed. ') + 'Source SHA baseline is outside timing.'
    sys.argv = [previous_argv[0], '--control', str(args.control)]
    try:
        benchmark.main()
    finally:
        sys.argv, benchmark.run_trial, benchmark.SCAFFOLDING_NOTE, benchmark.MODES = (
            previous_argv, previous_trial, previous_note, previous_modes)


if __name__ == '__main__':
    main()
