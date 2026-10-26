#!/usr/bin/env python3
"""Preloaded, same-pass P3 scan continuation; copy/broker/seed roles untouched.

Standby only reads metadata until an independently verified cutover arrives.
It never signals a process. The controller must stop only the old source
readers, drain their IPC/probes/publications, and verify graceful manifest
flush before this entry may acquire the existing coordinator lock.
"""
import argparse
from collections import deque
import collections
import copy
import gc
import hashlib
import json
import os
from pathlib import Path
import stat
import threading
import time

import migration_assigned_checkpoint as assignments
from migration_frontier_resume import FrontierResume
from migration_io import read_json, write_json
from migration_overlap_resume import OverlapResume, WARM_STAGE
from migration_parallel_scan import scan_phase
from migration_pipelined_target_filter import PipelinedTargetFilterResume
from migration_warm_scan_checkpoint import (Prefix, read_prefix, validate_runtime,
                                            resource_budget, process_identity)
from migration_source_tuning import SourceLimit
from migration_assigned_source_guard import AssignedSourceGuard


class RetiredInputs:
    def submitted(self, *args): pass
    def harvested(self, *args): pass
    def close(self): pass
    def finish(self): pass


def restore_prefix(values):
    if (not isinstance(values, dict) or type(values.get('paths')) is not set or
            type(values.get('directories')) is not dict or type(values.get('parents')) is not set or
            type(values.get('error_parents')) is not set or type(values.get('stats')) is not dict or
            type(values.get('hardlinks')) is not dict or type(values.get('tail')) is not list):
        raise ValueError('invalid primitive scan checkpoint')
    prefix = Prefix()
    prefix.paths, prefix.directories = values['paths'], values['directories']
    prefix.parents, prefix.error_parents = values['parents'], values['error_parents']
    prefix.tail = deque(values['tail'], maxlen=512)
    prefix.scanner.stats, prefix.scanner.hardlinks = values['stats'], values['hardlinks']
    prefix.root_identity = tuple(values['root_identity'])
    prefix.records = values['records']
    return prefix


def validate_cache_lineage(control, expected, scan_meta, assignment_meta):
    """Follow hash-bound same-pass handovers back to the prepared cache owner."""
    if expected.get('cache_predecessor') is None:
        return
    if expected.get('entry_kind') != 'fast-p3-scan':
        raise ValueError('prepared cache same-pass lineage is not proven')
    bundle = Path(expected['source_bundle'])
    visited = set()
    for _ in range(16):
        if (bundle in visited or bundle.parent != control or bundle.is_symlink()
                or not bundle.name.startswith('fast-p3-scan-')):
            raise ValueError('invalid or cyclic cache handover chain')
        visited.add(bundle)
        handover = read_json(bundle / 'handover.json')
        cutover = read_json(bundle / 'cutover.json')
        old = handover['old_producer']
        predecessor = dict(pid=old['pid'], start_ticks=str(old['starttime_ticks']), argv=old['argv'])
        if (handover.get('control') != str(control)
                or cutover.get('same_pass_quiescence_and_flush_verified') is not True
                or handover.get('missing_manifest') != scan_meta['source_manifest']
                or handover.get('assignment_checkpoint') != assignment_meta['assignment_checkpoint']
                or handover.get('next_task_id') != assignment_meta['prefix_floor']
                or cutover.get('manifest_size', 0) < scan_meta['offset']):
            raise ValueError('prepared cache same-pass lineage is not proven')
        if predecessor == expected['cache_predecessor']:
            return
        try:
            previous_intent = read_json(bundle / 'expected-runtime.json')
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise ValueError('cache chain producer binding is missing') from exc
        if (previous_intent.get('producer') != predecessor
                or previous_intent.get('entry_kind') != 'fast-p3-scan'):
            raise ValueError('cache chain producer binding differs')
        previous = Path(previous_intent['source_bundle'])
        if (previous.parent != control or previous.is_symlink()
                or not previous.name.startswith('fast-p3-scan-')
                or len(predecessor['argv']) < 3
                or predecessor['argv'][2] != str(previous / 'migration_fast_cold_resume.py')):
            raise ValueError('cache chain source entry differs')
        manifest = previous / 'source-sha256.json'
        raw = manifest.read_bytes()
        if manifest.is_symlink() or hashlib.sha256(raw).hexdigest() != previous_intent.get('frozen_active_manifest_sha256'):
            raise ValueError('cache chain source manifest differs')
        for name, checksum in json.loads(raw).items():
            path = previous / name
            if Path(name).name != name or path.is_symlink() or assignments.digest_file(path) != checksum:
                raise ValueError('cache chain frozen source differs')
        bundle = previous
    raise ValueError('cache handover chain exceeds bounded depth')


def record_sha256(value):
    """Bind the logical immutable JSON record, not an obsolete canonical inode."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def inherited_guard_owner(control, expected, path, value):
    """Reuse only the ready guard actually adopted by the exact live predecessor.

    Do not relabel a preparation as belonging to a different producer. Its
    original owner, assignment prefix and private cache digest stay intact.
    A fresh intent must hash-bind both records and prove the intervening
    same-pass handover. Normal preparation ownership remains the default.
    """
    if (expected.get('reuse_assigned_guard_from_active') is not True
            or expected.get('entry_kind') != 'fast-p3-scan'
            or value.get('status') != 'prepared_assignment_metadata_only'
            or record_sha256(value) != expected.get('inherited_assigned_guard_record_sha256')):
        return False
    _, state, _ = validate_runtime(control, expected)
    prior = Path(expected['source_bundle'])
    intent = read_json(prior / 'expected-runtime.json')
    handover = read_json(prior / 'handover.json')
    cutover = read_json(prior / 'cutover.json')
    old = handover.get('old_producer', {})
    owner = dict(pid=old.get('pid'), start_ticks=str(old.get('starttime_ticks')), argv=old.get('argv'))
    recorded_owner = {k: value.get('producer', {}).get(k) for k in ('pid', 'start_ticks', 'argv')}
    return (
        record_sha256(intent) == expected.get('active_assigned_guard_intent_sha256')
        and intent.get('producer') == owner == recorded_owner
        and intent.get('skip_assigned_source_metadata') is True
        and intent.get('assigned_source_guard_preparation') == str(path)
        and handover.get('control') == str(control)
        and handover.get('assignment_checkpoint') == value.get('assignment_checkpoint')
        and handover.get('next_task_id') == value.get('prefix_floor')
        and cutover.get('control') == str(control)
        and cutover.get('old_producer') == owner
        and cutover.get('same_pass_quiescence_and_flush_verified') is True
        and cutover.get('next_task_id') == value.get('prefix_floor')
        and state.get('source_scan_acceleration', {}).get('assigned_source_metadata_guard') is True
        and state.get('next_task_id') == value.get('prefix_floor')
    )


def guard_preparation(control, expected, require_ready=True):
    enabled = expected.get('skip_assigned_source_metadata', False)
    if type(enabled) is not bool:
        raise ValueError('invalid assigned source metadata option')
    if not enabled:
        return None
    path = Path(expected['assigned_source_guard_preparation'])
    if (path.name != 'preparation.json' or path.parent.parent != control
            or not path.parent.name.startswith('warm-assigned-source-guard-')
            or path.is_symlink() or path.parent.is_symlink()):
        raise ValueError('assigned source guard preparation escapes control')
    value = read_json(path)
    statuses = {'prepared_assignment_metadata_only'}
    if not require_ready:
        statuses.update(('reading_assignment_metadata_only', 'saving_private_guard'))
    if (value.get('status') not in statuses
            or value.get('control') != str(control) or value.get('run_id') != expected['run_id']):
        raise ValueError('assigned source guard owner/scope differs')
    owner = {k: value.get('producer', {}).get(k) for k in ('pid', 'start_ticks', 'argv')}
    if owner != expected['producer'] and not inherited_guard_owner(control, expected, path, value):
        raise ValueError('assigned source guard owner/scope differs')
    return value


def load_source_guard(control, expected, descriptor_path, descriptor, assigned, floor):
    meta = guard_preparation(control, expected)
    if meta is None:
        return None
    if (meta.get('assignment_checkpoint') != str(descriptor_path)
            or meta.get('assignment_descriptor_sha256') != assignments.digest_file(descriptor_path)
            or meta.get('prefix_floor') != floor or meta.get('tasks') != floor
            or meta.get('base_ledger') != descriptor['base_ledger']
            or meta.get('prefix_sha256') != descriptor['prefix_sha256']):
        raise ValueError('assigned source guard assignment prefix differs')
    cache = Path(meta['cache'])
    info = cache.lstat()
    if (not cache.is_absolute() or cache.is_symlink() or info.st_uid != os.getuid()
            or info.st_mode & 0o077 or assignments.digest_file(cache) != meta['cache_sha256']):
        raise ValueError('assigned source guard cache owner/mode/digest differs')
    enabled = gc.isenabled()
    gc.disable()
    try:
        with cache.open('rb', buffering=8 * 1024**2) as stream:
            value = assignments.PrimitiveUnpickler(stream).load()
        if (type(value) is not dict or set(value) != {'inode_paths', 'excluded'}
                or type(value['inode_paths']) is not dict or type(value['excluded']) is not set
                or len(value['inode_paths']) != meta.get('eligible_inodes')
                or len(value['excluded']) != meta.get('ambiguous_or_multilink_inodes')):
            raise ValueError('assigned source guard cache container/count differs')
        result = AssignedSourceGuard(value['inode_paths'], value['excluded'], assigned)
        gc.freeze()
        return result
    finally:
        if enabled:
            gc.enable()


class Standby:
    def __init__(self, control, directory, expected, scan_preparation, assignment_preparation):
        self.control, self.directory = Path(control).resolve(), Path(directory).resolve()
        if self.directory.parent != self.control or not self.directory.name.startswith('fast-p3-scan-'):
            raise ValueError('standby requires its own immediate control child')
        self.expected = expected
        self.cfg, _, _ = validate_runtime(self.control, expected)
        self.scan_meta = read_json(Path(scan_preparation))
        self.assignment_meta = read_json(Path(assignment_preparation))
        if (self.scan_meta.get('status') != 'prepared_prefix_only' or
                self.assignment_meta.get('status') != 'prepared_assignment_prefix_only' or
                any(m.get('control') != str(self.control) or m.get('run_id') != self.cfg['run_id']
                    for m in (self.scan_meta, self.assignment_meta)) or
                any({k:m['producer'][k] for k in ('pid','start_ticks','argv')} != expected.get('cache_predecessor',expected['producer'])
                    for m in (self.scan_meta,self.assignment_meta))):
            raise ValueError('prepared caches do not belong to this producer')
        validate_cache_lineage(self.control, expected, self.scan_meta, self.assignment_meta)
        self.descriptor = Path(self.assignment_meta['assignment_checkpoint'])
        self.assignment_descriptor_hash = assignments.digest_file(self.descriptor)
        self.assignment_descriptor = read_json(self.descriptor)
        self.assignment_delta = self.descriptor.parent / 'assignment-delta.jsonl'
        self.assignment_fields = dict(
            assignment_checkpoint=str(self.descriptor),
            assigned_tasks_path=self.assignment_descriptor['base_ledger'],
            assignment_prefix_sha256=self.assignment_descriptor['prefix_sha256'],
            assignment_delta_path=str(self.assignment_delta),
            assignment_delta_sha256=assignments.digest_file(self.assignment_delta))
        self.started, self.last_publish = time.monotonic(), 0
        self.prefix = None
        self.status = 'loading_caches'
        self.offset = self.scan_meta['offset']
        self.source = Path(self.scan_meta['source_manifest'])
        self.report(force=True)
        defer_guard = expected.get('defer_assigned_guard_load', False)
        if type(defer_guard) is not bool or (defer_guard and not expected.get('skip_assigned_source_metadata')):
            raise ValueError('invalid deferred guard preparation option')
        extra_guard = guard_preparation(self.control, expected, require_ready=not defer_guard)
        extra_bytes = Path(extra_guard['cache']).stat().st_size if extra_guard is not None and not defer_guard else 0
        self._guard_pending = bool(extra_guard is not None and defer_guard)
        if self._guard_pending:
            extra_bytes = expected.get('assigned_guard_cache_max_bytes')
            if (type(extra_bytes) is not int or extra_bytes <= 0
                    or extra_bytes > Path(self.assignment_descriptor['base_ledger']).stat().st_size):
                raise ValueError('deferred guard cache needs a conservative assignment-ledger byte bound')
        resource_budget(Path(self.scan_meta['cache']).stat().st_size +
                        Path(self.assignment_descriptor['cache']).stat().st_size +
                        extra_bytes)
        self.assigned, maximum, _, self.task_ids = assignments.load_checkpoint(
            self.control, self.cfg, self.assignment_fields)
        self.floor = maximum + 1
        self.assigned_source_guard = (None if self._guard_pending else load_source_guard(
            self.control, expected, self.descriptor, self.assignment_descriptor, self.assigned, self.floor))
        cache = Path(self.scan_meta['cache'])
        info = cache.lstat()
        if (not cache.is_absolute() or cache.is_symlink() or info.st_uid != os.getuid() or
                info.st_mode & 0o077 or assignments.digest_file(cache) != self.scan_meta['cache_sha256']):
            raise ValueError('scan cache ownership/mode/digest differs')
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            with open(cache, 'rb', buffering=8*1024**2) as stream:
                values = assignments.PrimitiveUnpickler(stream).load()
            self.prefix = restore_prefix(values)
            if (len(self.prefix.paths) != self.scan_meta['paths'] or
                    len(self.prefix.directories) != self.scan_meta['directories'] or
                    self.prefix.records != self.scan_meta['records']):
                raise ValueError('scan cache counts differ')
            gc.freeze()  # Only startup/permanent containers exist at this point.
        finally:
            if was_enabled:
                gc.enable()
        self.digest = hashlib.sha256()  # Digest of the tail, not the earlier prefix.
        self.status = 'catching_up'

    def report(self, force=False, **values):
        if not force and time.monotonic() - self.last_publish < 3:
            return
        value = dict(status=self.status, pid=os.getpid(), control=str(self.control),
                     old_producer=self.expected['producer'], offset=self.offset,
                     elapsed_seconds=time.monotonic()-self.started, updated=time.time(),
                     authorizes_cutover=False, signals_sent=0, **values)
        if self.prefix is not None:
            value.update(records=self.prefix.records, paths=len(self.prefix.paths),
                         directories=len(self.prefix.directories), assigned_paths=len(self.assigned))
        write_json(self.directory / 'standby.json', value)
        self.last_publish = time.monotonic()

    def catch_up(self, final_limit=None):
        with open(self.source, 'rb', buffering=8*1024**2) as stream:
            current = os.fstat(stream.fileno())
            if ((current.st_dev,current.st_ino) != (self.scan_meta['source_manifest_dev'],self.scan_meta['source_manifest_ino'])
                    or current.st_size < self.offset):
                raise RuntimeError('source inventory identity or append boundary changed')
            limit = current.st_size if final_limit is None else final_limit
            if limit > current.st_size:
                raise RuntimeError('final manifest length is not visible')
            stream.seek(self.offset)
            def progress(**values):
                self.offset = values['offset']
                self.report(manifest_bytes=limit, bytes_behind=limit-self.offset)
            self.offset = read_prefix(stream, limit, self.prefix, self.digest, report=progress)
            return limit - self.offset

    def cached_assignments(self, control, cfg, handover):
        if (Path(control).resolve() != self.control or cfg['run_id'] != self.cfg['run_id'] or
                assignments.digest_file(self.descriptor) != self.assignment_descriptor_hash or
                any(handover.get(k) != v for k,v in self.assignment_fields.items()) or
                handover.get('next_task_id') != self.floor):
            raise ValueError('preloaded assignment binding changed')
        return self.assigned, self.floor-1, self.assignment_delta, self.task_ids

    def prepare_guard(self):
        if not self._guard_pending:
            return True
        meta = guard_preparation(self.control, self.expected, require_ready=False)
        if meta['status'] != 'prepared_assignment_metadata_only':
            return False
        if Path(meta['cache']).stat().st_size > self.expected['assigned_guard_cache_max_bytes']:
            raise ValueError('prepared guard exceeds its conservative startup budget')
        self.status = 'loading_assigned_metadata'
        self.report(force=True)
        self.assigned_source_guard = load_source_guard(self.control, self.expected, self.descriptor,
                                                       self.assignment_descriptor, self.assigned, self.floor)
        self._guard_pending = False
        return True

    def run(self):
        checked_runtime = 0
        while not (self.directory / 'cutover.json').exists():
            intent_path = self.directory / 'quiesce-intent.json'
            intent = read_json(intent_path) if intent_path.exists() else {}
            if intent.get('status') == 'active':
                if (intent.get('control') != str(self.control) or
                        intent.get('old_producer') != self.expected['producer']):
                    raise ValueError('quiescence intent belongs to another producer')
            else:
                current=process_identity(self.expected['producer']['pid'])
                if (current['state'] in ('Z','X') or any(current[k]!=self.expected['producer'][k]
                        for k in ('pid','start_ticks','argv'))):
                    raise RuntimeError('prepared predecessor identity changed')
                if time.monotonic()-checked_runtime>=10:
                    # Full source binding is rechecked by the controller
                    # immediately before cutover; do not reread 79 modules
                    # every second while a healthy predecessor is scanning.
                    validate_runtime(self.control, self.expected)
                    checked_runtime=time.monotonic()
            lag = self.catch_up()
            guard_ready = self.prepare_guard()
            # A long prefix/cache load must not report the previous EOF as
            # fresh readiness after the live predecessor has appended again.
            lag = max(lag, self.source.stat().st_size - self.offset)
            self.status = ('waiting_assigned_metadata' if not guard_ready else
                           'ready' if lag < 1024**2 else 'catching_up')
            self.report(force=True, bytes_behind=lag, assigned_guard_ready=guard_ready)
            time.sleep(1)
        cutover = read_json(self.directory / 'cutover.json')
        if self._guard_pending:
            raise ValueError('assigned source guard is not prepared; cutover forbidden')
        if (cutover.get('control') != str(self.control) or cutover.get('old_producer') != self.expected['producer']
                or cutover.get('same_pass_quiescence_and_flush_verified') is not True
                or cutover.get('next_task_id') != self.floor):
            raise ValueError('verified same-pass cutover record required')
        try:
            current = process_identity(self.expected['producer']['pid'])
        except FileNotFoundError:
            current = {'state':'exited'}
        if current['state'] not in ('Z','X','exited'):
            raise RuntimeError('original producer is still live')
        if self.catch_up(cutover['manifest_size']) or self.source.stat().st_size != self.offset:
            raise RuntimeError('graceful exit did not leave a complete immutable manifest tail')
        self.completed = self.prefix.pruning_candidates(cutover['active_directories'], quiescent_and_flushed=True)
        self.cutover = cutover
        self.status = 'adopting'
        self.report(force=True, completed_subtree_candidates=len(self.completed))
        return FastColdResume(self).run()


class FastColdResume(FrontierResume):
    def __init__(self, standby):
        self.standby = standby
        self.source_processes = getattr(standby,'expected',{}).get('new_source_processes',32)
        self.assigned_source_guard = getattr(standby, 'assigned_source_guard', None)
        self.parallel_directories = getattr(standby,'expected',{}).get('parallel_directory_metadata',False)
        if type(self.source_processes) is not int or not 1<=self.source_processes<=64 or type(self.parallel_directories) is not bool:
            raise ValueError('invalid prepared source concurrency')
        self._inputs = RetiredInputs()
        self._stop_seen, self._stop_until, self._stop_lock = False, 0, threading.Lock()
        state = read_json(standby.control / 'state.json')
        self.filter_counts = {k:state['target_filter'][k] for k in ('submitted','checked','missing','peak_inflight')}
        self.pending_probes, self.offered, self.selected, self.inflight = {}, [], [], 0
        self.last_filter_save = time.monotonic()
        self._metadata_batching = copy.deepcopy(state['metadata_batching'])
        self._save_timing = {'scope':'since-fast-scan-adoption',
            **{k:dict(calls=0,failures=0,seconds=0.0,last_seconds=0.0,max_seconds=0.0) for k in ('save','progress')}}
        self._task_batch_policy = copy.deepcopy(state['task_batch_policy'])
        self._probe_restore = state.get('distributed_target_probe', {})
        self._probe_restored = False
        handover = standby.directory / 'handover.json'
        original_loader = assignments.load_checkpoint
        try:
            assignments.load_checkpoint = standby.cached_assignments
            PipelinedTargetFilterResume.__init__(self, standby.control, handover,
                                               publish_workers=16,publish_limit=64,check_on_submit=False)
        finally:
            assignments.load_checkpoint = original_loader
        self.cfg = dict(self.cfg,metadata_client_processes=32,distributed_target_threads=2048)
        self._distributed_workers = self.filter_workers = 2048
        self.scan_batch_files = self._task_batch_policy['max_files']
        self._formal_scan_batch_files = int(self.cfg.get('scan_batch_files',64))
        self.probe_pool = None
        self.state['priority_inputs'] = dict(self.state.get('priority_inputs',{}),closed=True,open_groups=0)
        self.state['distributed_input_retired'] = dict(reason='verified-complete-before-same-pass-adoption',
                                                      current_source_full_scan_required=True)
        self.state.pop('producer_error',None)
        self.save(source_scan_acceleration=dict(processes=self.source_processes,threads_per_process=8,process_batch_size=1024,
                  parallel_directory_metadata=self.parallel_directories,
                  fast_directory_open=True,checkpoint_seconds=15,retired_completed_input_pool=True,
                  assigned_source_metadata_guard=self.assigned_source_guard is not None,
                  warm_metadata_inventory_complete=False if self.assigned_source_guard is not None else None,
                  previous_paths_preserved=len(standby.prefix.paths),handover=str(handover)))

    def _select_adoption_manifest(self, proposed):
        s = self.standby
        if (s.source != s.control / 'manifests/recovery-missing.jsonl' or
                s.source.is_symlink() or s.source.stat().st_size != s.offset):
            raise ValueError('same-pass manifest binding changed')
        return s.source

    def save(self, **updates):
        updates['target_probe_counts_scope']='cumulative-across-producer-handover'
        if hasattr(self, '_probe_counts') and not self._probe_restored:
            self._probe_counts.update(self._probe_restore.get('counts',{}))
            for endpoint, counts in self._probe_restore.get('endpoints',{}).items():
                if endpoint in self._endpoint_counts:
                    self._endpoint_counts[endpoint].update(counts)
            self._probe_restored = True
        return super().save(**updates)

    def _checkpoint(self, force=False):
        self._check_stop()
        if force or time.monotonic()-self.last_filter_save >= 15:
            self.save(target_filter=dict(self.filter_counts,inflight=self.inflight),next_task_id=self.next_task_id)

    def pump_inputs(self, force=False):
        pass  # No completed-input set construction or old ready-tree polling.

    def supplement_missing(self):
        s = self.standby
        initial=getattr(s,'expected',{}).get('initial_source_limit',self.source_processes)
        limit=SourceLimit(s.directory/'reader-limit.json',self.control,self.cfg['run_id'],self.source_processes,initial=initial).start()
        try:
            self.save(status='scanning',recovery_label='P3 同轮断点扫描：'+str(self.source_processes)+' 进程，已完成子树跳过')
            # The final scanner batch might be recorded but not yet offered.
            # Reprobe only this bounded tail; assigned paths still deduplicate.
            self._offer(list(s.prefix.tail))
            def reader_progress(metrics):
                self.state['source_reader_progress'] = copy.deepcopy(metrics)
                self.state['source_reader_tuning_error'] = limit.error
            stats = scan_phase(self.cfg['source'],3,self._offer,self.combined_manifest,
                batch_files=self.scan_batch_files,batch_bytes=1024**3,workers=self.scan_workers,
                control=str(self.control),hardlink_index=s.prefix.scanner.hardlinks,
                initial_seeded_paths=s.prefix.paths,seed_stats=s.prefix.scanner.stats,
                append_manifest=True,skip_seeded_nondirectories=True,
                completed_directories=s.completed,expected_root_identity=s.prefix.root_identity,
                processes=self.source_processes,threads_per_process=8,process_batch_size=1024,fast_directory_open=True,
                parallel_directory_metadata=self.parallel_directories,concurrency_limit=limit,
                assigned_source_guard=self.assigned_source_guard,
                on_reader_progress=reader_progress,
                on_tick=lambda:(self._harvest_probes(),self._flush_selected()))
            self._drain_probes()
            self._collect_publications(finish=True)
            self.state['warm_missing_scan'] = stats
            self.save(status='copying',scan_complete=True)
        finally:
            limit.close()
            self._collect_publications(finish=True)
            self._task_publisher = None
            self.scan_batch_files = self._formal_scan_batch_files


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control',required=True);parser.add_argument('--directory',required=True)
    parser.add_argument('--expected-runtime',required=True)
    parser.add_argument('--scan-preparation',required=True);parser.add_argument('--assignment-preparation',required=True)
    args=parser.parse_args();os.umask(0o077)
    standby=Standby(args.control,args.directory,read_json(Path(args.expected_runtime)),
                    args.scan_preparation,args.assignment_preparation)
    standby.run()


if __name__=='__main__':main()
