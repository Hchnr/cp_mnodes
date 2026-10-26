#!/usr/bin/env python3
"""Independent P4 coordinator; never adopts or restarts the eai_pwm engine.

The concurrent mode requires an explicit, scope-bound isolation record. Task
archives precede publication, so a producer restart can recover publication
holes without inventing IDs or recopying assigned paths. Local broker loss is
deliberately fail-closed: it requires separate lease recovery, not a fresh run.
"""
import argparse
import collections
import concurrent.futures
import datetime
import fcntl
import itertools
import json
import os
from pathlib import Path
import resource
import socket
import sqlite3
import time
import traceback
import uuid

from migration_io import read_json, write_json
from migration_priority4_worker import ScopeGuard
from migration_priority4_scan import scan_phase
from migration_priority4_metadata import finalize_directories
from migration_run import (Coordinator, GOOD, now, queue_dirs, queued_count,
                           validate_mounts)
from migration_task_publisher import BoundedTaskPublisher
from migration_worker import configure_rate_limit


def ensure_nofile(minimum=65536):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < minimum:
        raise RuntimeError('P4 RLIMIT_NOFILE hard limit is too low')
    if soft != resource.RLIM_INFINITY and soft < minimum:
        resource.setrlimit(resource.RLIMIT_NOFILE, (minimum, hard))
    actual = resource.getrlimit(resource.RLIMIT_NOFILE)
    if actual[0] != resource.RLIM_INFINITY and actual[0] < minimum:
        raise RuntimeError('P4 RLIMIT_NOFILE did not reach minimum')
    return actual


def validate_authorization(control, cfg):
    guard = ScopeGuard(str(control), cfg)
    record = read_json(Path(control) / 'isolation.json')
    expected = {
        'version': 1, 'mode': 'independent_concurrent_p4',
        'control': str(control), 'run_id': cfg['run_id'], 'job': cfg['job'],
        'source': cfg['source'], 'destination': cfg['destination'],
        'exclude': ['eai_pwm'], 'predecessor_control': cfg['parallel_with_control'],
        'existing_roles_unchanged': True, 'mount_root_metadata': 'preserve_existing',
        'verified': True,
    }
    if any(record.get(key) != value for key, value in expected.items()):
        raise ValueError('P4 isolation/authorization record does not match this run')
    if record.get('user_authorization') != '允许先验证 P4 独立隔离，再并行搬运':
        raise ValueError('explicit concurrent P4 authorization is absent')
    if cfg.get('resume_from'):
        raise ValueError('this launcher does not import a different control or scope')
    if cfg['parallel_with_control'] not in guard.covered_by:
        raise ValueError('parallel predecessor is not protected')
    if cfg.get('training_watchdog') is not False:
        raise ValueError('P4 must not start a training watchdog')
    guard.assert_state(read_json(Path(control) / 'state.json'))
    return guard


class Priority4Coordinator(Coordinator):
    """Caller must hold the run's local coordinator flock before construction."""
    def __init__(self, control):
        self.control = Path(os.path.abspath(control))
        self.cfg = read_json(self.control / 'config.json')
        self.guard = validate_authorization(self.control, self.cfg)
        validate_mounts(self.cfg)
        self.state = read_json(self.control / 'state.json')
        self.state.setdefault('started', now())
        self.state.setdefault('phases', {})
        self.state.update(run_id=self.cfg['run_id'], workers_expected=12,
                          threads_per_node=128, migration_scope='project_priority4')
        self.db = sqlite3.connect('/tmp/' + self.cfg['run_id'] + '-index.sqlite')
        self.db.execute('CREATE TABLE IF NOT EXISTS files '
                        '(rel TEXT PRIMARY KEY, source TEXT, destination TEXT, owned INTEGER, sha256 TEXT)')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.commit()
        self.hardlinks = {}
        self.phase_progress = {'4': dict(discovered_files=0, discovered_bytes=0,
                                        verified_files=0, verified_bytes=0,
                                        inventory_complete=False)}
        try:
            self.phase_progress['4'].update(read_json(self.control / 'progress.json')['phases']['4'])
        except FileNotFoundError:
            pass
        self.current_scan = {}
        self.last_save = 0.0

    def check(self):
        if (self.control / 'STOP').exists():
            raise RuntimeError('P4 STOP requested; unrelated P1-3 processes are untouched')
        self.guard.assert_config(read_json(self.control / 'config.json'))

    def save(self, **updates):
        super().save(**updates)
        self.last_save = time.monotonic()

    def maybe_save(self, **updates):
        self.state.update(updates)
        if time.monotonic() - self.last_save >= 10:
            self.check()
            self.save()

    def archives(self, stage):
        root = self.control / 'stages' / stage / 'tasks'
        return sorted(root.glob('[0-9]*/*.json'), key=lambda p: int(p.stem))

    def recover_assignments(self, stage, round_no):
        """Same-broker producer recovery; no claims are deleted or reassigned."""
        root = queue_dirs(self.control, stage)
        for shard in range(64):
            (root / 'tasks' / str(shard)).mkdir(parents=True, exist_ok=True)
        paths, count = set(), 0
        broker_path = Path('/tmp/' + self.cfg['run_id'] + '-broker.sqlite')
        if not broker_path.is_file() or broker_path.is_symlink():
            raise RuntimeError('P4 broker database is absent; explicit lease recovery required')
        with sqlite3.connect(broker_path.as_uri() + '?mode=ro', uri=True) as broker:
            for archive in self.archives(stage):
                task = read_json(archive)
                if task.get('id') != count or (task.get('phase'), task.get('round')) != (4, round_no):
                    raise ValueError('P4 task archive is not a contiguous stage prefix')
                for entry in task['entries']:
                    self.guard.assert_task(task, entry)
                    if entry['rel'] in paths:
                        raise ValueError('duplicate P4 assignment path')
                    paths.add(entry['rel'])
                    if entry.get('nlink', 1) > 1:
                        self.hardlinks[(entry['dev'], entry['ino'])] = entry['rel']
                claim = broker.execute('SELECT status,payload FROM claims WHERE stage=? AND id=?',
                                       (stage, count)).fetchone()
                if claim and json.loads(claim[1]) != task:
                    raise ValueError('P4 broker/archive payload differs')
                pending = root / 'pending' / str(count % 64) / (str(count) + '.json')
                if not claim:
                    done = root / 'done' / str(count % 64) / (str(count) + '.json')
                    if done.exists() or (done.parent / '.snapshots' / done.name).exists():
                        raise RuntimeError('P4 receipt exists without broker authority; lease recovery required')
                    if pending.exists():
                        if read_json(pending) != task:
                            raise ValueError('P4 pending/archive payload differs')
                    else:
                        write_json(pending, task)
                count += 1
        return paths, count

    def scan(self, phase, round_no):
        if phase != 4:
            raise ValueError('independent coordinator only accepts phase4')
        self.check()
        stage = 'p4-r' + str(round_no)
        self.hardlinks = {}
        assigned, task_id = self.recover_assignments(stage, round_no)
        root = self.control / 'stages' / stage
        manifest = self.control / 'manifests' / (stage + '-' + uuid.uuid4().hex[:12] + '.jsonl')
        manifest.parent.mkdir(exist_ok=True)
        self.current_scan = dict(phase=4, stage=stage, files=0, bytes=0)
        self.phase_progress['4']['inventory_complete'] = False
        self.save(status='scanning', stage=stage, phase=4, round=round_no,
                  scan_complete=False, active_manifest=str(manifest), next_task_id=task_id,
                  task_publication={'workers': 32, 'limit': 256, 'pending': 0})

        def publish(target, task):
            archive = root / 'tasks' / str(task['id'] % 64) / target.name
            if archive.exists() or (archive.parent / '.snapshots' / archive.name).exists():
                raise FileExistsError('P4 immutable archive already exists: ' + str(archive))
            write_json(archive, task)
            return write_json(target, task)

        publisher = BoundedTaskPublisher(workers=32, max_pending=256, writer=publish)
        submitted = task_id

        def emit(entries):
            nonlocal task_id, submitted
            self.check()
            self.current_scan['files'] += len(entries)
            self.current_scan['bytes'] += sum(e.get('size', 0) for e in entries if e['kind'] == 'file')
            for key in ('files', 'bytes'):
                counter = 'discovered_' + key
                self.phase_progress['4'][counter] = max(self.phase_progress['4'][counter], self.current_scan[key])
            fresh = [dict(entry) for entry in entries if entry['rel'] not in assigned]
            if not fresh:
                self.maybe_save()
                return
            for entry in fresh:
                self.guard.assert_path(entry['rel'])
                row = self.db.execute('SELECT source,destination,owned,sha256 FROM files WHERE rel=?',
                                      (entry['rel'],)).fetchone()
                if row:
                    entry['known'] = dict(source=json.loads(row[0]), destination=json.loads(row[1]),
                                          owned=bool(row[2]), sha256=row[3])
            task = dict(id=task_id, phase=4, round=round_no,
                        op='sync' if round_no > 1 else 'copy', entries=fresh)
            target = root / 'pending' / str(task_id % 64) / (str(task_id) + '.json')
            while not publisher.submit(task_id, task, target):
                publisher.harvest(wait=True)
                self.maybe_save()
            assigned.update(entry['rel'] for entry in fresh)
            task_id += 1
            submitted += 1
            publisher.harvest()
            self.state['task_publication']['pending'] = publisher.pending_count
            self.maybe_save(enqueued_batches=submitted, next_task_id=task_id)
            if submitted % 256 == 0:
                # Bound total queued work, not merely the publisher's futures.
                # Slow consumers must not leave millions of pending NFS files.
                broker_file = Path('/tmp/' + self.cfg['run_id'] + '-broker.sqlite')
                with sqlite3.connect(broker_file.as_uri() + '?mode=ro', uri=True) as broker:
                    while True:
                        done = broker.execute("SELECT COUNT(*) FROM claims WHERE stage=? AND status='done'",
                                              (stage,)).fetchone()[0]
                        if submitted - done < 4096:
                            break
                        self.check()
                        publisher.harvest()
                        self.maybe_save(supply_backpressure=True)
                        time.sleep(1)
                self.state['supply_backpressure'] = False

        def error(entry):
            with open(self.control / (manifest.stem + '-scan-issues.jsonl'), 'a') as handle:
                handle.write(json.dumps(entry, ensure_ascii=True) + '\n')

        try:
            stats = scan_phase(self.cfg['source'], 4, emit, manifest,
                               batch_bytes=256 * 1024**2, batch_files=64,
                               workers=64, queue_size=8192, hardlink_index=self.hardlinks,
                               on_error=error, control=self.control)
        finally:
            publisher.finish()
        self.phase_progress['4'].update(discovered_files=stats['files'] + stats['symlinks'],
                                       discovered_bytes=stats['bytes'],
                                       inventory_complete=stats['scan_complete'])
        self.current_scan.update(files=stats['files'] + stats['symlinks'], bytes=stats['bytes'])
        self.state['task_publication']['pending'] = 0
        self.save(status='copying', scan_complete=True, scan=stats,
                  enqueued_batches=task_id, next_task_id=task_id)
        return stage, manifest, stats

    def collect(self, stage):
        # Read receipts once, with bounded parallel NFS reads. SQLite ownership
        # and summary updates remain serialized in the coordinator thread.
        summary = collections.Counter()
        paths = itertools.chain.from_iterable(
            (self.control / 'stages' / stage / 'done' / str(shard)).glob('*.json')
            for shard in range(64))
        with open(self.control / (stage + '-issues.jsonl'), 'w') as issues, \
                concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            while True:
                batch = list(itertools.islice(paths, 128))
                if not batch:
                    break
                self.check()
                for receipt in pool.map(read_json, batch):
                    for result in receipt['results']:
                        self.guard.assert_path(result['rel'])
                        status = result['status']
                        summary[status] += 1
                        summary['bytes'] += result.get('bytes', 0)
                        if status in GOOD:
                            summary['verified_files'] += 1
                            if result.get('kind') == 'file':
                                summary['verified_bytes'] += result.get('source_identity', {}).get('size', 0)
                        warnings = result.get('metadata_warnings')
                        if warnings:
                            summary['metadata_warnings'] += 1
                        if status not in GOOD or warnings:
                            issues.write(json.dumps(result, ensure_ascii=True) + '\n')
                        src, dst = result.get('source_identity'), result.get('dst_stat')
                        if status in GOOD and src and dst and not warnings:
                            prior = self.db.execute('SELECT destination,owned,sha256 FROM files WHERE rel=?',
                                                    (result['rel'],)).fetchone()
                            owned = status == 'copied' or bool(
                                prior and prior[1] and json.loads(prior[0]).get('ino') == dst.get('ino')
                                and prior[2] and prior[2] == result.get('sha256'))
                            self.db.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?,?)',
                                            (result['rel'], json.dumps(src), json.dumps(dst), owned,
                                             result.get('sha256')))
                self.db.commit()
        self.event('round_complete', stage=stage, summary=dict(summary))
        return dict(summary)

    def finalize(self, manifest, include_directories=True):
        # Both ends of every hardlink must stay in this isolated scope. The
        # inherited finalizer cannot touch root directory metadata here.
        with open(manifest) as handle:
            for line in handle:
                entry = json.loads(line)
                if entry.get('rel') == '' and entry.get('kind') == 'directory':
                    continue
                self.guard.assert_path(entry['rel'])
                if entry.get('hardlink_to'):
                    self.guard.assert_path(entry['hardlink_to'])
        result = super().finalize(manifest, include_directories=False)
        directories = finalize_directories(self.control, manifest, threads=32, defer_roots=True)
        result['directory_metadata'] = directories
        result['errors'] = result.get('errors', 0) + directories.get('errors', 0)
        result['directories'] = directories.get('directories', 0)
        result['mount_root_metadata'] = 'preserved_existing'
        return result

    def run(self):
        configure_rate_limit(32 * 1024**2)
        self.check()
        completed = list(self.state.get('phases', {}).get('4', {}).get('rounds', []))
        # Rebuild local ownership from durable same-scope receipts when needed;
        # do not import an eai_pwm-relative receipt or replace another DB.
        for number in range(1, len(completed) + 1):
            self.collect('p4-r' + str(number))
        for number in range(len(completed) + 1, 3):
            stage = 'p4-r' + str(number)
            if (self.state.get('stage') == stage and self.state.get('scan_complete') and
                    self.state.get('active_manifest') and self.state.get('scan')):
                manifest = Path(self.state['active_manifest'])
                if manifest.parent != self.control / 'manifests' or not manifest.stem.startswith(stage + '-'):
                    raise ValueError('P4 saved manifest is outside this stage')
                scan = self.state['scan']
            else:
                stage, manifest, scan = self.scan(4, number)
            self.drain(stage)
            summary = self.collect(stage)
            self.save(status='finalizing')
            final = self.finalize(manifest)
            self.phase_progress['4'].update(
                verified_files=summary.get('verified_files', 0) + final.get('hardlinks', 0),
                verified_bytes=summary.get('verified_bytes', 0) + final.get('hardlink_bytes', 0))
            completed.append(dict(stage=stage, manifest=str(manifest), scan=scan,
                                  summary=summary, finalize=final))
            self.state['phases']['4'] = dict(rounds=completed, finished=now())
            self.save(status='phase_complete')
        latest = completed[-1]
        unresolved = sum(value for key, value in latest['summary'].items()
                         if key not in GOOD | {'bytes', 'verified_files', 'verified_bytes', 'metadata_warnings'})
        unresolved += latest['summary'].get('metadata_warnings', 0)
        unresolved += latest['finalize'].get('errors', 0) + latest['scan'].get('errors', 0)
        unresolved += latest['scan'].get('unsupported', 0)
        # Independent completion is never a claim that P1-3 are complete.
        self.save(status='complete_with_changes' if unresolved else 'complete',
                  unresolved=unresolved, stage=None, finished=now(),
                  completion_scope='priority4_only', predecessor_acceptance_still_required=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    control = Path(os.path.abspath(args.control))
    cfg = read_json(control / 'config.json')
    validate_authorization(control, cfg)
    ensure_nofile()
    with open('/tmp/' + cfg['run_id'] + '-coordinator.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        coordinator = Priority4Coordinator(control)
        try:
            coordinator.run()
        except BaseException:
            coordinator.save(status='producer_paused', producer_error=traceback.format_exc())
            raise
        finally:
            coordinator.db.close()


if __name__ == '__main__':
    main()
