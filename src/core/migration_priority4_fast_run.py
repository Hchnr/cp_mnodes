#!/usr/bin/env python3
"""Prepared same-pass P4 producer successor; explicit validated release required.

Before release this process is read-only with respect to queues, state, source
and destination. It never signals another process. Both brokers and every copy
process retain their identities, databases, leases and accumulated counters.
"""
import argparse
from contextlib import closing
import fcntl
import gc
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import traceback
import uuid

from migration_assigned_checkpoint import PrimitiveUnpickler
from migration_io import read_json, write_json
from migration_priority4_frontier import Priority4Prefix, ancestors
from migration_priority4_run import Priority4Coordinator, ensure_nofile, validate_authorization
from migration_priority4_scan import scan_phase
from migration_scan_boundary import identity, require_identity
from migration_source_tuning import SourceLimit
from migration_state_cache import StateCache
from migration_task_publisher import BoundedTaskPublisher
from migration_warm_scan_checkpoint import read_prefix
from migration_worker import configure_rate_limit
from prepare_priority4_assignments import digest_file, task_digest


AUTHORIZATION = '允许，验证后交接 P4 生产端'


class ContinuingPrefix(Priority4Prefix):
    def __init__(self, values):
        super().__init__()
        for field in ('paths', 'directories', 'parents', 'error_parents', 'root_identity', 'records'):
            setattr(self, field, values[field])
        self.tail.extend(values['tail'])
        self.scanner.hardlinks = values['hardlinks']
        self.scanner.stats = values['stats']
        self.candidates = values['completed_candidates']

    def add(self, entry):
        super().add(entry)
        if entry.get('kind') in ('directory', 'error'):
            for rel in ancestors(entry['rel']):
                self.candidates.pop(rel, None)


def private_cache(meta):
    path = Path(meta['cache'])
    info, parent = path.lstat(), path.parent.lstat()
    if (not path.is_absolute() or path.parent.parent != Path('/tmp')
            or not stat.S_ISREG(info.st_mode) or not stat.S_ISDIR(parent.st_mode)
            or info.st_uid != os.getuid() or parent.st_uid != os.getuid()
            or info.st_mode & 0o077 or parent.st_mode & 0o077
            or digest_file(path) != meta['cache_sha256']):
        raise ValueError('private preparation cache mode/owner/SHA differs')
    with open(path, 'rb', buffering=8 * 1024**2) as stream:
        return PrimitiveUnpickler(stream).load()


def prefix_digest(path, length):
    value = hashlib.sha256()
    with open(path, 'rb', buffering=8 * 1024**2) as stream:
        remaining = length
        while remaining:
            block = stream.read(min(8 * 1024**2, remaining))
            if not block:
                raise ValueError('P4 prepared source prefix was truncated')
            value.update(block)
            remaining -= len(block)
    return value


def unoffered_tail(prefix, assigned):
    """A full counter equality proves no older orphan lies outside the tail."""
    if not assigned.issubset(prefix.paths):
        raise ValueError('P4 assignments include paths absent from combined source inventory')
    entries = [entry for entry in prefix.tail if entry['rel'] not in assigned]
    if (len({entry['rel'] for entry in entries}) != len(entries)
            or prefix.scanner.stats['copy_entries'] - len(assigned) != len(entries)):
        raise ValueError('P4 unpublished canonical tail cannot be completely accounted for')
    return entries


class PreparedContinuation:
    def __init__(self, directory, intent, guard):
        self.directory, self.intent, self.guard = directory, intent, guard
        self.control = Path(intent['control'])
        self.source_meta = read_json(Path(intent['source_preparation']))
        for meta in (self.source_meta,):
            if (meta['control'] != str(self.control) or meta['run_id'] != guard.run_id
                    or meta['producer'] != intent['producer']
                    or meta['runtime_manifest_sha256'] != intent['runtime_manifest_sha256']):
                raise ValueError('P4 preparation identity/runtime bindings differ')
        if (self.source_meta['status'] != 'prepared_prefix_and_fifo_only'
                or self.source_meta['fifo']['status'] != 'prepared_fifo_frontier_only'
                or self.source_meta['fifo']['adoption_authorized'] is not False):
            raise ValueError('P4 prepared FIFO proof required')
        self.manifest = Path(self.source_meta['manifest'])
        if self.manifest.parent != self.control / 'manifests' or not self.manifest.name.startswith('p4-r1-'):
            raise ValueError('P4 current combined manifest required')
        self.verify_manifest()
        self.digest = prefix_digest(self.manifest, self.source_meta['offset'])
        if self.digest.hexdigest() != self.source_meta['prefix_sha256']:
            raise ValueError('P4 source prefix SHA differs')
        values = private_cache(self.source_meta)
        self.prefix = ContinuingPrefix(values)
        if (len(self.prefix.paths) != self.source_meta['paths']
                or self.prefix.records != self.source_meta['records']
                or self.prefix.root_identity != tuple(self.source_meta['root_identity'])
                or len(self.prefix.candidates) != self.source_meta['candidate_directories']):
            raise ValueError('P4 prepared source cache counts differ')
        self.stream = open(self.manifest, 'rb', buffering=8 * 1024**2)
        self.stream.seek(self.source_meta['offset'])
        self.last_report = 0
        deadline = time.monotonic() + 1200
        while not Path(intent['assignment_preparation']).exists():
            require_identity(intent['producer'])
            self.verify_manifest()
            read_prefix(self.stream, self.manifest.stat().st_size, self.prefix, self.digest)
            self.report(status='standby_source_loaded_waiting_assignment_cache',
                        source_records=self.prefix.records, manifest_offset=self.stream.tell())
            if time.monotonic() > deadline or (self.control / 'STOP').exists():
                raise RuntimeError('readonly assignment wait expired or STOP; original producer untouched')
            time.sleep(2)
        self.assignment_meta = read_json(Path(intent['assignment_preparation']))
        meta = self.assignment_meta
        if (meta['control'] != str(self.control) or meta['run_id'] != guard.run_id
                or meta['producer'] != intent['producer']
                or meta['runtime_manifest_sha256'] != intent['runtime_manifest_sha256']
                or meta['status'] != 'prepared_assignment_prefix_only'):
            raise ValueError('P4 prepared assignment proof required')
        values = private_cache(meta)
        self.assigned, self.hashes = values['paths'], values['hashes']
        self.assignment_hardlinks = values['hardlinks']
        if (type(self.assigned) is not set or len(self.assigned) != self.assignment_meta['assigned_paths']
                or type(self.hashes) is not list or len(self.hashes) != self.assignment_meta['tasks']):
            raise ValueError('P4 prepared assignment cache counts differ')
        self.broker = Path(self.assignment_meta['broker'])
        info = self.broker.stat()
        if (info.st_dev, info.st_ino) != (self.assignment_meta['broker_dev'], self.assignment_meta['broker_ino']):
            raise ValueError('original P4 broker database identity changed')

    def verify_manifest(self):
        info = self.manifest.lstat()
        if (not stat.S_ISREG(info.st_mode) or
                (info.st_dev, info.st_ino) != (self.source_meta['manifest_dev'], self.source_meta['manifest_ino'])):
            raise ValueError('P4 combined manifest identity changed')

    def catch_up(self, *, final=False):
        self.verify_manifest()
        size = self.manifest.stat().st_size
        read_prefix(self.stream, size, self.prefix, self.digest)
        if final and self.stream.tell() != size:
            raise ValueError('gracefully closed P4 manifest has an incomplete final record')
        with closing(sqlite3.connect(self.broker.as_uri() + '?mode=ro', uri=True, timeout=10)) as db:
            for number, payload in db.execute('SELECT id,payload FROM claims WHERE stage=? AND id>=? ORDER BY id',
                                             ('p4-r1', len(self.hashes))):
                if number != len(self.hashes):
                    break
                task = json.loads(payload)
                self.add_task(task)
        self.report(status='standby_readonly_caught_up', manifest_offset=self.stream.tell(),
                    manifest_lag=size - self.stream.tell(), source_records=self.prefix.records,
                    tasks=len(self.hashes), assigned_paths=len(self.assigned),
                    candidate_directories=len(self.prefix.candidates))

    def add_task(self, task):
        digest = task_digest(task, self.guard, len(self.hashes))
        for entry in task['entries']:
            if entry['rel'] in self.assigned:
                raise ValueError('P4 successor observed duplicate immutable assignments')
            self.assigned.add(entry['rel'])
            if entry['kind'] == 'file' and entry.get('nlink', 1) > 1:
                key = (entry['dev'], entry['ino'])
                prior = self.assignment_hardlinks.setdefault(key, entry['rel'])
                if prior != entry['rel']:
                    raise ValueError('P4 hardlink canonical assignment changed')
        self.hashes.append(digest)

    def report(self, **updates):
        if time.monotonic() - self.last_report >= 2 or updates.get('force'):
            updates.pop('force', None)
            write_json(self.directory / 'standby.json', dict(control=str(self.control),
                producer_predecessor=self.intent['producer'], copy_processes_signalled=0,
                brokers_signalled=0, updated=time.time(), **updates))
            self.last_report = time.monotonic()

    def verify_final_tasks(self):
        """All archive names, cached broker prefix, and every small final delta."""
        from concurrent.futures import ThreadPoolExecutor
        root = self.control / 'stages/p4-r1/tasks'
        def list_shard(shard):
            numbers = []
            with os.scandir(root / str(shard)) as entries:
                for entry in entries:
                    if entry.name == '.snapshots':
                        # An immutable archive may never have a snapshot.
                        if any(os.scandir(entry.path)):
                            raise ValueError('P4 immutable task archive has unexpected snapshots')
                        continue
                    if entry.name.startswith('.') and entry.name.endswith('.tmp'):
                        raise ValueError('P4 publisher left an incomplete archive')
                    if not entry.name.endswith('.json') or not entry.name[:-5].isdigit():
                        raise ValueError('unrecognized P4 immutable archive name')
                    number = int(entry.name[:-5])
                    if number % 64 != shard or not entry.is_file(follow_symlinks=False):
                        raise ValueError('P4 archive shard or type differs')
                    numbers.append(number)
            return numbers
        with ThreadPoolExecutor(max_workers=8) as pool:
            numbers = [n for group in pool.map(list_shard, range(64)) for n in group]
        numbers.sort()
        if not numbers or numbers != list(range(len(numbers))) or len(self.hashes) > len(numbers):
            raise ValueError('P4 original archive IDs are not a complete contiguous prefix')
        cached = len(self.hashes)
        root_pending = self.control / 'stages/p4-r1/pending'
        with closing(sqlite3.connect(self.broker.as_uri() + '?mode=ro', uri=True, timeout=10)) as db:
            for number in range(cached, len(numbers)):
                task = read_json(root / str(number % 64) / (str(number) + '.json'))
                claim = db.execute('SELECT payload FROM claims WHERE stage=? AND id=?', ('p4-r1', number)).fetchone()
                if claim:
                    if task_digest(json.loads(claim[0]), self.guard, number) != task_digest(task, self.guard, number):
                        raise ValueError('P4 broker/archive delta payload differs')
                else:
                    pending = root_pending / str(number % 64) / (str(number) + '.json')
                    # No task is republished by the verifier. A missing claim
                    # MUST have its original accepted publication still present.
                    if read_json(pending) != task:
                        raise ValueError('unclaimed P4 archive lacks its exact pending payload')
                self.add_task(task)
            maximum = db.execute('SELECT MAX(id) FROM claims WHERE stage=?', ('p4-r1',)).fetchone()[0]
            if maximum is not None and maximum >= len(numbers):
                raise ValueError('P4 broker has a claim beyond archived authority')
        state = read_json(self.control / 'state.json')
        if not len(numbers) - 1 <= state['next_task_id'] <= len(numbers):
            raise ValueError('P4 saved task counter differs beyond one interrupted submission')
        for key, canonical in self.assignment_hardlinks.items():
            if self.prefix.scanner.hardlinks.get(key) != canonical:
                raise ValueError('P4 source and assignment hardlink canonicals differ')
        orphan = unoffered_tail(self.prefix, self.assigned)
        self.stream.close()
        return dict(tasks=len(numbers), cached_broker_tasks=cached, final_archive_delta=len(numbers) - cached,
                    orphan_entries=len(orphan), manifest_offset=self.manifest.stat().st_size,
                    manifest_sha256=self.digest.hexdigest(), source_records=self.prefix.records,
                    source_files=self.prefix.scanner.stats['files'], source_bytes=self.prefix.scanner.stats['bytes'],
                    prune_candidates=len(self.prefix.candidates), leases_deleted=0, tasks_republished=0), orphan


class FastPriority4Coordinator(Priority4Coordinator):
    def __init__(self, control, prepared, directory, proof, orphans):
        super().__init__(control)
        self.prepared, self.directory, self.proof, self.orphans = prepared, directory, proof, orphans
        self.config_cache = StateCache(self.control / 'config.json', read_json, refresh_interval=1, max_age=30)
        self.config_cache.refresh_once()
        self.config_cache.start()
        self.reader_limit = SourceLimit(directory / 'source-limit.json', self.control, self.cfg['run_id'],
                                       16, initial=8, scope='priority4-source-reader-limit-v1')
        self.reader_limit.start()

    def check(self):
        if (self.control / 'STOP').exists():
            raise RuntimeError('P4 STOP requested')
        cache = getattr(self, 'config_cache', None)
        self.guard.assert_config(cache.read() if cache is not None else read_json(self.control / 'config.json'))

    def scan(self, phase, round_no):
        if phase != 4 or round_no not in (1, 2):
            raise ValueError('P4 scanner scope/round differs')
        self.check()
        stage = 'p4-r' + str(round_no)
        continuation = self.prepared if round_no == 1 else None
        if continuation:
            assigned, task_id = continuation.assigned, len(continuation.hashes)
            self.hardlinks = continuation.prefix.scanner.hardlinks
            manifest = continuation.manifest
            seed = continuation.prefix.scanner.stats
            self.current_scan = dict(phase=4, stage=stage,
                files=seed['files'] + seed['symlinks'] - seed['hardlinks'], bytes=seed['copy_bytes'])
        else:
            self.hardlinks = {}
            assigned, task_id = self.recover_assignments(stage, round_no)
            manifest = self.control / 'manifests' / (stage + '-' + uuid.uuid4().hex[:12] + '.jsonl')
            self.current_scan = dict(phase=4, stage=stage, files=0, bytes=0)
        root = self.control / 'stages' / stage
        for kind in ('tasks', 'pending'):
            for shard in range(64):
                path = root / kind / str(shard)
                path.mkdir(parents=True, exist_ok=True)
                if path.is_symlink() or not path.is_dir():
                    raise ValueError('P4 prepared queue parent is unsafe')
        self.phase_progress['4']['inventory_complete'] = False
        self.save(status='scanning', stage=stage, phase=4, round=round_no, scan_complete=False,
            active_manifest=str(manifest), next_task_id=task_id, producer_error=None,
            producer_handover=str(self.directory / 'handover.json'),
            source_scan_acceleration=dict(kind='spawn-source-readers', processes=16, initial_processes=8,
                                          threads_per_process=8, same_pass_append=bool(continuation),
                                          batch_files=256, queued_tasks_limit=2048),
            task_publication=dict(workers=32, limit=256, pending=0))
        def publish(target, task):
            archive = root / 'tasks' / str(task['id'] % 64) / target.name
            if os.path.lexists(archive) or os.path.lexists(archive.parent / '.snapshots' / archive.name):
                raise FileExistsError('P4 immutable archive already exists')
            write_json(archive, task, ensure_parent=False)
            return write_json(target, task, ensure_parent=False)
        publisher = BoundedTaskPublisher(workers=32, max_pending=256, writer=publish, check_on_submit=False)
        submitted = task_id
        # The coordinator exclusively owns this index; collect() writes it
        # only after scan/drain. Do not issue millions of guaranteed-empty
        # point queries in the first inventory pass.
        have_known_files = self.db.execute('SELECT 1 FROM files LIMIT 1').fetchone() is not None
        def emit(entries, *, counted=False):
            nonlocal task_id, submitted
            self.check()
            if not counted:
                self.current_scan['files'] += len(entries)
                self.current_scan['bytes'] += sum(e.get('size', 0) for e in entries if e['kind'] == 'file')
            for key in ('files', 'bytes'):
                name = 'discovered_' + key
                self.phase_progress['4'][name] = max(self.phase_progress['4'][name], self.current_scan[key])
            fresh = [dict(e) for e in entries if e['rel'] not in assigned]
            if not fresh:
                self.maybe_save()
                return
            for entry in fresh:
                self.guard.assert_path(entry['rel'])
                row = (self.db.execute('SELECT source,destination,owned,sha256 FROM files WHERE rel=?',
                                       (entry['rel'],)).fetchone() if have_known_files else None)
                if row:
                    entry['known'] = dict(source=json.loads(row[0]), destination=json.loads(row[1]),
                                          owned=bool(row[2]), sha256=row[3])
            task = dict(id=task_id, phase=4, round=round_no, op='copy' if round_no == 1 else 'sync', entries=fresh)
            target = root / 'pending' / str(task_id % 64) / (str(task_id) + '.json')
            while not publisher.submit(task_id, task, target):
                publisher.harvest(wait=True)
                self.maybe_save()
            assigned.update(e['rel'] for e in fresh)
            task_id += 1
            submitted += 1
            publisher.harvest()
            self.state['task_publication']['pending'] = publisher.pending_count
            self.maybe_save(enqueued_batches=submitted, next_task_id=task_id)
            if submitted % 128 == 0:
                with closing(sqlite3.connect('/tmp/' + self.cfg['run_id'] + '-broker.sqlite', timeout=10)) as db:
                    while submitted - db.execute("SELECT COUNT(*) FROM claims WHERE stage=? AND status='done'", (stage,)).fetchone()[0] >= 2048:
                        self.check()
                        publisher.harvest()
                        self.maybe_save(supply_backpressure=True)
                        time.sleep(1)
                self.state['supply_backpressure'] = False
        def on_tick():
            publisher.harvest()
            self.maybe_save()
        def on_reader_progress(value):
            self.state['source_reader_progress'] = dict(value)
        def error(entry):
            with open(self.control / (manifest.stem + '-scan-issues.jsonl'), 'a') as output:
                output.write(json.dumps(entry, ensure_ascii=True) + '\n')
        kwargs = dict(processes=16, threads_per_process=8, process_batch_size=512,
            batch_bytes=256 * 1024**2, batch_files=256, workers=128, queue_size=8192,
            hardlink_index=self.hardlinks, on_error=error, control=self.control,
            on_tick=on_tick, fast_directory_open=True, parallel_directory_metadata=True,
            on_reader_progress=on_reader_progress, concurrency_limit=self.reader_limit)
        if continuation:
            kwargs.update(initial_seeded_paths=continuation.prefix.paths, seed_stats=continuation.prefix.scanner.stats,
                append_manifest=True, skip_seeded_nondirectories=True,
                completed_directories=continuation.prefix.candidates,
                expected_root_identity=continuation.prefix.root_identity)
        try:
            if continuation and self.orphans:
                emit(self.orphans, counted=True)
            stats = scan_phase(self.cfg['source'], 4, emit, manifest, **kwargs)
        finally:
            publisher.finish()
        self.phase_progress['4'].update(discovered_files=stats['files'] + stats['symlinks'],
            discovered_bytes=stats['bytes'], inventory_complete=stats['scan_complete'])
        self.current_scan.update(files=stats['files'] + stats['symlinks'], bytes=stats['bytes'])
        self.state['task_publication']['pending'] = 0
        self.save(status='copying', scan_complete=True, scan=stats, enqueued_batches=task_id, next_task_id=task_id)
        return stage, manifest, stats

    def close(self):
        self.config_cache.close()
        self.reader_limit.close()
        self.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    directory = parser.parse_args().directory.resolve()
    intent = read_json(directory / 'intent.json')
    control = Path(intent['control'])
    if directory.parent != control or not directory.name.startswith('fast-p4-source-'):
        raise ValueError('dedicated P4 successor bundle required')
    cfg = read_json(control / 'config.json')
    guard = validate_authorization(control, cfg)
    producer = intent['producer']
    ensure_nofile()
    require_identity(producer)
    enabled = gc.isenabled()
    gc.disable()
    prepared, coordinator = None, None
    try:
        prepared = PreparedContinuation(directory, intent, guard)
        prepared.report(status='standby_cache_loaded', force=True)
        while not (directory / 'release.json').exists():
            requested = (directory / 'handover-request.json').exists()
            if requested:
                request = read_json(directory / 'handover-request.json')
                if (request.get('producer') != producer or request.get('operation') != intent['operation']
                        or request.get('user_authorization') != AUTHORIZATION):
                    raise ValueError('P4 handover request binding differs')
            else:
                require_identity(producer)
            if (control / 'STOP').exists():
                raise RuntimeError('STOP during readonly standby')
            prepared.catch_up()
            time.sleep(1)
        release = read_json(directory / 'release.json')
        if (release.get('user_authorization') != AUTHORIZATION or release.get('producer') != producer
                or release.get('operation') != intent['operation'] or release.get('graceful_sigint_exit') is not True
                or release.get('all_copy_and_broker_identities_verified') is not True):
            raise ValueError('explicit validated P4 producer-only release required')
        try:
            actual = identity(producer['pid'])
        except FileNotFoundError:
            actual = None
        if actual is not None and (actual['state'] not in ('Z', 'X') or actual['start_ticks'] != producer['start_ticks']):
            raise RuntimeError('original P4 producer has not exited')
        state = read_json(control / 'state.json')
        if (state.get('status') != 'producer_paused' or state.get('stage') != 'p4-r1'
                or 'KeyboardInterrupt' not in state.get('producer_error', '')):
            raise RuntimeError('original P4 producer did not record its graceful SIGINT exit')
        with open('/tmp/' + cfg['run_id'] + '-coordinator.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            prepared.catch_up(final=True)
            proof, orphans = prepared.verify_final_tasks()
            gc.collect()
            gc.freeze()
            if enabled:
                gc.enable()
            write_json(directory / 'handover.json', dict(status='validated_same_pass_adoption',
                predecessor=producer, successor=identity(os.getpid()), proof=proof,
                copy_processes_signalled=0, brokers_signalled=0, created=time.time()))
            import socket
            role_path = control / 'roles' / (socket.gethostname() + '-producer.json')
            old_role = read_json(role_path)
            if any(old_role.get(key) != producer[key] for key in ('pid', 'start_ticks', 'argv')):
                raise ValueError('original P4 producer role no longer matches predecessor')
            write_json(directory / 'predecessor-role.json', old_role)
            actual = identity(os.getpid())
            write_json(role_path, dict(node='master-0', role='producer', started=time.time(),
                pid=actual['pid'], start_ticks=actual['start_ticks'], argv=actual['argv'],
                predecessor=producer, source_bundle=str(directory),
                handover=str(directory / 'handover.json')))
            coordinator = FastPriority4Coordinator(control, prepared, directory, proof, orphans)
            prepared.report(status='adopted_and_starting_scan', proof=proof, force=True)
            configure_rate_limit(32 * 1024**2)
            coordinator.run()
    except BaseException as exc:
        write_json(directory / 'failure.json', dict(error=repr(exc), traceback=traceback.format_exc(),
            producer_predecessor=producer, copy_processes_signalled=0, brokers_signalled=0, failed=time.time()))
        if coordinator is not None:
            coordinator.save(status='producer_paused', producer_error=traceback.format_exc())
        raise
    finally:
        if enabled:
            gc.enable()
        if coordinator is not None:
            coordinator.close()
        if prepared is not None and not prepared.stream.closed:
            prepared.stream.close()


if __name__ == '__main__':
    main()
