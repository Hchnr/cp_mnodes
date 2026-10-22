#!/usr/bin/env python3
"""Persistent shared-filesystem copy coordinator and node worker launcher.

Only destination/control trees are mutated. Queue claims use a single-node
HTTP broker; SQLite indexes live on the coordinator's local disk.
"""
import argparse
import collections
import concurrent.futures
import datetime
import hashlib
import json
import os
import pathlib
import random
import shutil
import socket
import sqlite3
import stat
import subprocess
import threading
import time
import traceback
import uuid

from migration_scan import scan_phase
from migration_worker import (copy_entry, ensure_destination_directory,
                              _open_parent, _open_root, _record, _metadata_fd,
                              _identity, _hash_fd, configure_rate_limit, configure_cancel_event,
                              configure_progress_callback)

SHARDS = 64
GOOD = {'copied', 'skipped', 'verified', 'linked'}
TERMINAL = {'complete', 'complete_with_changes', 'failed', 'stopped'}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


from migration_io import read_json, write_json as atomic_json


def mount_info(path):
    return subprocess.check_output(
        ['findmnt', '-T', path, '-rn', '-o', 'TARGET,SOURCE,FSTYPE'], text=True).strip()


def validate_mounts(cfg):
    for key, mount in [('source', '/share/project'), ('destination', '/share-mixed/project')]:
        path = cfg[key]
        info = mount_info(mount)
        if info != cfg[key + '_mount']:
            raise RuntimeError('mount mismatch for ' + mount + ': ' + info)
        if not os.path.isdir(mount) or os.path.islink(mount):
            raise RuntimeError('missing or symlink mount: ' + mount)
    if os.stat('/share/project').st_dev == os.stat('/share-mixed/project').st_dev:
        raise RuntimeError('source and destination are on same device')


def queue_dirs(control, stage):
    root = pathlib.Path(control) / 'stages' / stage
    for name in ('pending', 'running', 'done'):
        (root / name).mkdir(parents=True, exist_ok=True)
    for i in range(SHARDS):
        (root / 'pending' / str(i)).mkdir(exist_ok=True)
        (root / 'done' / str(i)).mkdir(exist_ok=True)
    return root


def queued_count(root):
    pending = sum(sum(1 for p in (root / 'pending' / str(i)).iterdir()
                      if p.name.endswith('.json')) for i in range(SHARDS))
    running = sum(1 for p in (root / 'running').iterdir() if p.name.endswith('.json'))
    return pending, running


def same_snapshot(st, rec):
    return rec is not None and all(getattr(st, 'st_' + k) == rec[k]
                                  for k in ('ino', 'size', 'mtime_ns', 'ctime_ns'))


def process_entry(cfg, task, entry):
    if task['op'] == 'sync' and entry.get('known'):
        known = entry['known']
        try:
            srcfd, srcname = _open_parent(cfg['source'], entry['rel'])
            try:
                srcst = os.stat(srcname, dir_fd=srcfd, follow_symlinks=False)
            finally:
                os.close(srcfd)
            dstfd, dstname = _open_parent(cfg['destination'], entry['rel'])
            try:
                dstst = os.stat(dstname, dir_fd=dstfd, follow_symlinks=False)
            finally:
                os.close(dstfd)
            if (same_snapshot(srcst, entry) and same_snapshot(srcst, known['source'])
                    and same_snapshot(dstst, known['destination'])
                    and (stat.S_IMODE(srcst.st_mode), srcst.st_uid, srcst.st_gid,
                         srcst.st_size, srcst.st_mtime_ns) ==
                        (stat.S_IMODE(dstst.st_mode), dstst.st_uid, dstst.st_gid,
                         dstst.st_size, dstst.st_mtime_ns)):
                return {'rel': entry['rel'], 'kind': entry['kind'], 'status': 'verified',
                        'bytes': 0, 'source_identity': _record(srcst), 'dst_stat': _record(dstst),
                        'sha256': known.get('sha256')}
        except (OSError, ValueError):
            pass
    known = entry.get('known')
    replacement = None
    if known and known.get('owned'):
        try:
            fd, name = _open_parent(cfg['destination'], entry['rel'])
            try:
                current = os.stat(name, dir_fd=fd, follow_symlinks=False)
            finally:
                os.close(fd)
            if same_snapshot(current, known['destination']):
                replacement = known['destination']
        except OSError:
            pass
    result = copy_entry(cfg['source'], cfg['destination'], entry, cfg['run_id'],
                        verify=True, allow_replace_snapshot=replacement)
    return result


def worker(control, threads):
    # All queue ownership and completion writes belong to the broker. Shared
    # filesystem rename/O_EXCL did not provide exclusive claims on this mount.
    import urllib.parse
    import urllib.request

    control = pathlib.Path(control)
    cfg = read_json(control / 'config.json')
    validate_mounts(cfg)
    worker_rate = cfg.get('io_bytes_per_second', 300 * 1024**2)
    if socket.gethostname().endswith('master-0'):
        worker_rate = max(1, worker_rate - 32 * 1024**2)
    configure_rate_limit(worker_rate)
    host = socket.gethostname()
    # No second worker daemon may operate on this node for the same run.
    import fcntl
    lock = open('/tmp/' + cfg['run_id'] + '-worker.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    node_path = control / 'nodes' / (host + '.json')
    try:
        previous = read_json(node_path)
    except (OSError, ValueError):
        previous = {}
    started = now()
    progress = {'host': host, 'pid': os.getpid(), 'started': started,
                'run_started': previous.get('run_started', previous.get('started', started)),
                'threads': threads, 'io_bytes_per_second': worker_rate, 'counts': dict(previous.get('counts', {})),
                'bytes': previous.get('bytes', 0), 'streamed_bytes': previous.get('streamed_bytes', 0), 'active': {},
                'completed_batches': previous.get('completed_batches', 0),
                'verified_bytes': previous.get('verified_bytes', 0),
                'verified_files': previous.get('verified_files', 0),
                'stages': dict(previous.get('stages', {})),
                'broker_retries': previous.get('broker_retries', 0)}
    broker_url = cfg['broker_url'].rstrip('/')
    broker_token = cfg['broker_token']
    mutex = threading.Lock()
    stop = threading.Event()
    configure_cancel_event(stop)
    errors = []
    tuning = {'desired': threads, 'launched': 0, 'pool': None, 'rate': worker_rate}

    def streamed(amount):
        with mutex:
            progress['streamed_bytes'] += amount
    configure_progress_callback(streamed)

    def broker_request(method, route, payload=None):
        body = None if payload is None else json.dumps(payload, ensure_ascii=True).encode('utf-8')
        request = urllib.request.Request(
            broker_url + route, data=body, method=method,
            headers={'Authorization': 'Bearer ' + broker_token,
                     'Content-Type': 'application/json'})
        # The broker is on the private node network, never an HTTP proxy.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=30) as response:
            result = json.load(response)
        if not isinstance(result, dict) or result.get('ok') is False:
            raise ValueError('broker returned an invalid or rejected response')
        return result

    def record_broker_error(operation, exc):
        with mutex:
            progress['broker_retries'] += 1
            progress['broker_error'] = {'operation': operation, 'at': now(),
                                        'detail': type(exc).__name__ + ': ' + str(exc)}

    def heartbeat():
        while not stop.wait(3):
            if (control / 'STOP').exists():
                stop.set()
            if not stop.is_set():
                try:
                    current_cfg = read_json(control / 'config.json')
                    desired = int(current_cfg.get('threads', threads))
                    if not 1 <= desired <= 64:
                        raise ValueError('worker thread tuning must be between 1 and 64')
                    rate = int(current_cfg.get('io_bytes_per_second', worker_rate))
                    if rate <= 0:
                        raise ValueError('worker I/O tuning must be positive')
                    if host.endswith('master-0'):
                        rate = max(1, rate - 32 * 1024**2)
                    if rate != tuning['rate']:
                        configure_rate_limit(rate)
                        tuning['rate'] = rate
                    tuning['desired'] = desired
                    pool = tuning['pool']
                    if pool is not None:
                        while tuning['launched'] < desired:
                            index = tuning['launched']
                            pool.submit(thread_main, index)
                            tuning['launched'] += 1
                    with mutex:
                        progress.update(threads=desired, io_bytes_per_second=rate)
                        progress.pop('tuning_error', None)
                except (OSError, ValueError, RuntimeError) as exc:
                    with mutex:
                        progress['tuning_error'] = str(exc)
            with mutex:
                progress['updated'] = now()
                atomic_json(node_path, progress)

    def thread_main(index):
        owner = host + '-' + str(index)
        while not stop.is_set():
            if index >= tuning['desired']:
                stop.wait(1)
                continue
            if (control / 'STOP').exists():
                return
            try:
                state = read_json(control / 'state.json')
            except (OSError, ValueError) as exc:
                record_broker_error('read_state', exc)
                stop.wait(3)
                continue
            if state['status'] in TERMINAL:
                return
            stage = state.get('stage')
            if not stage:
                stop.wait(1)
                continue
            try:
                claim = broker_request('GET', '/claim?' + urllib.parse.urlencode(
                    {'stage': stage, 'owner': owner}))
                task = claim.get('task')
                if task is not None and (not isinstance(task, dict)
                                         or not isinstance(claim.get('lease'), str)
                                         or not claim['lease']):
                    raise ValueError('broker claim lacks a task or lease')
            except Exception as exc:
                record_broker_error('claim', exc)
                stop.wait(3)
                continue
            if task is None:
                stop.wait(1.5)
                continue
            lease = claim['lease']
            try:
                receipt = {'id': task['id'], 'host': host, 'thread': index,
                           'started': now(), 'op': task['op'], 'results': []}
                for entry in task['entries']:
                    with mutex:
                        progress['active'][str(index)] = {'stage': stage, 'task': task['id'],
                                                         'rel': entry['rel'], 'size': entry.get('size', 0)}
                    if stop.is_set() or (control / 'STOP').exists():
                        stop.set()
                        result = {'rel': entry['rel'], 'kind': entry['kind'], 'status': 'paused',
                                  'bytes': 0, 'detail': 'copy paused by STOP marker'}
                    else:
                        result = process_entry(cfg, task, entry)
                    receipt['results'].append(result)
                    with mutex:
                        status = result['status']
                        progress['counts'][status] = progress['counts'].get(status, 0) + 1
                        progress['bytes'] += result.get('bytes', 0)
                        stage_progress = progress['stages'].setdefault(stage, {
                            'verified_files': 0, 'verified_bytes': 0, 'copied_bytes': 0, 'counts': {}})
                        stage_progress['counts'][status] = stage_progress['counts'].get(status, 0) + 1
                        stage_progress['copied_bytes'] += result.get('bytes', 0)
                        if status in GOOD:
                            verified_bytes = entry.get('size', 0) if entry['kind'] == 'file' else 0
                            progress['verified_files'] += 1
                            progress['verified_bytes'] += verified_bytes
                            stage_progress['verified_files'] += 1
                            stage_progress['verified_bytes'] += verified_bytes
                receipt['finished'] = now()
                payload = {'stage': stage, 'id': task['id'], 'lease': lease,
                           'receipt': receipt}
                with mutex:
                    progress['active'].setdefault(str(index), {}).update(
                        stage=stage, task=task['id'], awaiting_receipt=True)
                delay = 1
                while True:
                    try:
                        broker_request('POST', '/done', payload)
                        break
                    except Exception as exc:
                        # A lost reply may mean the broker already committed.
                        # Retry the same receipt/lease forever; never recopy or
                        # claim new work after an uncertain completion.
                        record_broker_error('done', exc)
                        time.sleep(delay)
                        delay = min(30, delay * 2)
                with mutex:
                    progress['completed_batches'] += 1
                    progress['active'].pop(str(index), None)
            except Exception:
                errors.append(traceback.format_exc())
                with mutex:
                    progress['last_error'] = errors[-1]
                    progress['thread_error'] = True
                # Broker retains this lease for explicit operator recovery.
                return

    progress['updated'] = now()
    atomic_json(node_path, progress)
    hb = threading.Thread(target=heartbeat, daemon=True)
    hb.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
        initial = [pool.submit(thread_main, index) for index in range(threads)]
        tuning.update(pool=pool, launched=threads)
        for future in initial:
            future.result()
        # Additional threads submitted by heartbeat also stop on the terminal
        # state/STOP before executor shutdown waits for them.
        tuning['pool'] = None
    stop.set()
    with mutex:
        progress['updated'] = now()
        progress['ended'] = now()
        atomic_json(node_path, progress)
    if errors:
        raise RuntimeError(errors[-1])


class Coordinator:
    def __init__(self, control):
        self.control = pathlib.Path(control)
        self.cfg = read_json(self.control / 'config.json')
        self.state = {'status': 'starting', 'started': now(), 'run_id': self.cfg['run_id'],
                      'phases': {}, 'workers_expected': 12, 'threads_per_node': self.cfg['threads']}
        self.db = sqlite3.connect('/tmp/' + self.cfg['run_id'] + '-index.sqlite')
        self.db.execute('CREATE TABLE IF NOT EXISTS files '
                        '(rel TEXT PRIMARY KEY, source TEXT, destination TEXT, owned INTEGER, sha256 TEXT)')
        self.db.commit()
        self.hardlinks = {}
        self.phase_progress = {str(p): {'discovered_files': 0, 'discovered_bytes': 0,
                                      'verified_files': 0, 'verified_bytes': 0,
                                      'inventory_complete': False} for p in (1, 2, 3)}
        self.current_scan = {}
        if self.cfg.get('resume_from'):
            from migration_resume_index import build_resume_index
            self.db.close()
            db_path = '/tmp/' + self.cfg['run_id'] + '-index.sqlite'
            pathlib.Path(db_path).unlink()
            summary = build_resume_index(self.cfg['resume_from'], db_path)
            self.db = sqlite3.connect(db_path)
            self.state['resume_index'] = summary
            self.state['resumed_from'] = self.cfg['resume_from']

    def save(self, **updates):
        self.state.update(updates)
        self.state['updated'] = now()
        atomic_json(self.control / 'state.json', self.state)
        self.save_progress()

    def save_progress(self):
        from migration_nodes import load_nodes
        stage = self.state.get('stage')
        phase = str(self.state.get('phase', ''))
        copied = 0
        verified_files = verified_bytes = 0
        for node in load_nodes(self.control):
            copied += node.get('bytes', 0)
            current = node.get('stages', {}).get(stage, {})
            verified_files += current.get('verified_files', 0)
            verified_bytes += current.get('verified_bytes', 0)
        if phase in self.phase_progress and stage:
            self.phase_progress[phase]['verified_files'] = max(verified_files, self.phase_progress[phase]['verified_files'])
            self.phase_progress[phase]['verified_bytes'] = max(verified_bytes, self.phase_progress[phase]['verified_bytes'])
        inventory_complete = all(p['inventory_complete'] for p in self.phase_progress.values())
        atomic_json(self.control / 'progress.json', {
            'updated': now(), 'inventory_complete': inventory_complete,
            'phases': self.phase_progress,
            'current_scan': self.current_scan,
            'totals': {'discovered_files': sum(p['discovered_files'] for p in self.phase_progress.values()),
                       'discovered_bytes': sum(p['discovered_bytes'] for p in self.phase_progress.values()),
                       'verified_files': sum(p['verified_files'] for p in self.phase_progress.values()),
                       'verified_bytes': sum(p['verified_bytes'] for p in self.phase_progress.values()),
                       'new_copied_bytes': copied}})

    def event(self, name, **data):
        with open(self.control / 'events.jsonl', 'a') as f:
            f.write(json.dumps({'time': now(), 'event': name, **data}, ensure_ascii=True) + '\n')

    def scan(self, phase, round_no):
        stage = 'p' + str(phase) + '-r' + str(round_no)
        root = queue_dirs(self.control, stage)
        manifest = self.control / 'manifests' / (stage + '.jsonl')
        manifest.parent.mkdir(exist_ok=True)
        task_id = 0
        discovered_files = discovered_bytes = 0
        if round_no == 1:
            self.phase_progress[str(phase)].update(discovered_files=0, discovered_bytes=0,
                                                   verified_files=0, verified_bytes=0,
                                                   inventory_complete=False)
        self.current_scan = {'phase': phase, 'stage': stage, 'files': 0, 'bytes': 0}
        self.save(status='scanning', phase=phase, round=round_no, stage=stage,
                  phase_started=now(), scan_complete=False)

        def emit(entries):
            nonlocal task_id, discovered_files, discovered_bytes
            if (self.control / 'STOP').exists():
                raise RuntimeError('STOP requested')
            if round_no > 1 or self.cfg.get('resume_from'):
                for entry in entries:
                    row = self.db.execute('SELECT source,destination,owned,sha256 FROM files WHERE rel=?',
                                          (entry['rel'],)).fetchone()
                    if row:
                        entry['known'] = {'source': json.loads(row[0]), 'destination': json.loads(row[1]),
                                          'owned': bool(row[2]), 'sha256': row[3]}
            task = {'id': task_id, 'phase': phase, 'round': round_no,
                    'op': 'sync' if round_no > 1 or self.cfg.get('resume_from') else 'copy', 'entries': entries}
            atomic_json(root / 'pending' / str(task_id % SHARDS) / (str(task_id) + '.json'), task)
            task_id += 1
            discovered_files += len(entries)
            discovered_bytes += sum(e.get('size', 0) for e in entries if e['kind'] == 'file')
            pp = self.phase_progress[str(phase)]
            pp.update(discovered_files=max(discovered_files, pp['discovered_files']),
                      discovered_bytes=max(discovered_bytes, pp['discovered_bytes']))
            self.current_scan.update(files=discovered_files, bytes=discovered_bytes)
            if task_id % 10 == 0:
                self.save(enqueued_batches=task_id)

        stats = scan_phase(self.cfg['source'], phase, emit, str(manifest),
                           batch_bytes=1024**3, batch_files=256,
                           hardlink_index=self.hardlinks,
                           entries_per_second=self.cfg.get('scan_entries_per_second', 1000))
        self.phase_progress[str(phase)].update(discovered_files=stats['files'] + stats['symlinks'],
                                               discovered_bytes=stats['bytes'], inventory_complete=True)
        self.save(status='copying', scan_complete=True, enqueued_batches=task_id, scan=stats)
        self.event('scan_complete', phase=phase, round=round_no, stats=stats, batches=task_id)
        return stage, manifest, stats

    def drain(self, stage):
        from migration_nodes import load_nodes
        root = self.control / 'stages' / stage
        while True:
            if (self.control / 'STOP').exists():
                raise RuntimeError('STOP requested; workers finish current batches then exit')
            pending, running = queued_count(root)
            nodes = load_nodes(self.control)
            unhealthy = [n['host'] for n in nodes if n.get('thread_error') or n.get('ended')]
            stale = [n['host'] for n in nodes if
                     time.time() - datetime.datetime.fromisoformat(n.get('updated', n['started'])).timestamp() > 180]
            if unhealthy or stale:
                raise RuntimeError('workers need recovery; thread errors=' + repr(unhealthy)
                                   + '; stale heartbeats=' + repr(stale))
            if not nodes and time.time() - datetime.datetime.fromisoformat(self.state['started']).timestamp() > 180:
                raise RuntimeError('no worker heartbeat within 180 seconds')
            self.save(pending_batches=pending, running_batches=running,
                      total_copied_bytes=sum(n.get('bytes', 0) for n in nodes),
                      node_counts={n['host']: n.get('counts', {}) for n in nodes},
                      nodes_seen=len(nodes))
            if pending == 0 and running == 0:
                # A task's done receipt is committed before its claim is removed.
                return
            time.sleep(self.cfg.get('poll_interval_seconds', 5))

    def collect(self, stage):
        summary = collections.Counter()
        issues = self.control / (stage + '-issues.jsonl')
        with open(issues, 'w') as issue_file:
            for shard in range(SHARDS):
                for path in (self.control / 'stages' / stage / 'done' / str(shard)).glob('*.json'):
                    for result in read_json(path)['results']:
                        summary[result['status']] += 1
                        summary['bytes'] += result.get('bytes', 0)
                        if result['status'] in GOOD:
                            summary['verified_files'] += 1
                            if result.get('kind') == 'file':
                                summary['verified_bytes'] += result.get('source_identity', {}).get('size', 0)
                        if result.get('metadata_warnings'):
                            summary['metadata_warnings'] += 1
                        if result['status'] not in GOOD or result.get('metadata_warnings'):
                            issue_file.write(json.dumps(result, ensure_ascii=True) + '\n')
                        src, dst = result.get('source_identity'), result.get('dst_stat')
                        if result['status'] in GOOD and src and dst and not result.get('metadata_warnings'):
                            prior = self.db.execute('SELECT destination,owned,sha256 FROM files WHERE rel=?', (result['rel'],)).fetchone()
                            owned = result['status'] == 'copied' or bool(
                                prior and prior[1] and json.loads(prior[0]).get('ino') == dst.get('ino')
                                and prior[2] and prior[2] == result.get('sha256'))
                            self.db.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?,?)',
                                            (result['rel'], json.dumps(src), json.dumps(dst), owned, result.get('sha256')))
            self.db.commit()
        self.event('round_complete', stage=stage, summary=dict(summary))
        return dict(summary)

    def finalize(self, manifest, include_directories=True):
        counts = collections.Counter()
        dirs = []
        touched = {}
        inode_snapshots = {}

        def stable_hash(parent, name, expected):
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or _identity(before) != _identity(expected):
                    raise RuntimeError('hardlink destination changed before checksum')
                digest = _hash_fd(fd)
                if (_identity(os.fstat(fd)) != _identity(before) or
                        _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != _identity(before)):
                    raise RuntimeError('hardlink destination changed during checksum')
                return digest
            finally:
                os.close(fd)

        with open(self.control / (manifest.stem + '-finalize-issues.jsonl'), 'w') as errors:
            def issue(entry, exc):
                counts['errors'] += 1
                errors.write(json.dumps({'rel': entry['rel'], 'detail': str(exc)}, ensure_ascii=True) + '\n')
            with open(manifest) as f:
                for line in f:
                    if (self.control / 'STOP').exists():
                        raise RuntimeError('STOP requested during finalization')
                    entry = json.loads(line)
                    kind = entry['kind']
                    if kind == 'directory':
                        if include_directories:
                            dirs.append(entry)
                    elif entry.get('hardlink_to'):
                        srcfd = dstfd = canonicalfd = canonical_srcfd = None
                        temp = None
                        try:
                            canonical_rel = entry['hardlink_to']
                            srcfd, name = _open_parent(self.cfg['source'], entry['rel'])
                            source_st = os.stat(name, dir_fd=srcfd, follow_symlinks=False)
                            if not stat.S_ISREG(source_st.st_mode) or not same_snapshot(source_st, entry):
                                raise RuntimeError('source hardlink changed since inventory')
                            canonical_srcfd, canonical_srcname = _open_parent(self.cfg['source'], canonical_rel)
                            canonical_src = os.stat(canonical_srcname, dir_fd=canonical_srcfd, follow_symlinks=False)
                            if _identity(source_st) != _identity(canonical_src):
                                raise RuntimeError('source hardlink group changed')
                            ensure_destination_directory(self.cfg['destination'], os.path.dirname(entry['rel']))
                            dstfd, name = _open_parent(self.cfg['destination'], entry['rel'])
                            canonicalfd, canonical_name = _open_parent(self.cfg['destination'], canonical_rel)
                            canonical_dst = os.stat(canonical_name, dir_fd=canonicalfd, follow_symlinks=False)
                            if not stat.S_ISREG(canonical_dst.st_mode) or canonical_dst.st_size != source_st.st_size:
                                raise RuntimeError('canonical hardlink copy missing or changed')
                            row = self.db.execute('SELECT source,destination,owned,sha256 FROM files WHERE rel=?',
                                                  (canonical_rel,)).fetchone()
                            if not row or not same_snapshot(canonical_src, json.loads(row[0])):
                                raise RuntimeError('canonical hardlink source not verified at its current version')
                            canonical_key = (canonical_dst.st_dev, canonical_dst.st_ino)
                            known_dst = inode_snapshots.get(canonical_key, json.loads(row[1]))
                            if not same_snapshot(canonical_dst, known_dst):
                                if not row[3] or stable_hash(canonicalfd, canonical_name, canonical_dst) != row[3]:
                                    raise RuntimeError('canonical hardlink destination changed after verification')
                            try:
                                existing = os.stat(name, dir_fd=dstfd, follow_symlinks=False)
                            except FileNotFoundError:
                                existing = None
                            alias_row = self.db.execute('SELECT destination,owned,sha256 FROM files WHERE rel=?',
                                                        (entry['rel'],)).fetchone()
                            owned = bool(alias_row and alias_row[1])
                            if existing is not None:
                                if (existing.st_dev, existing.st_ino) == (canonical_dst.st_dev, canonical_dst.st_ino):
                                    self.db.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?,?)',
                                                    (entry['rel'], json.dumps(_record(source_st)),
                                                     json.dumps(_record(existing)), owned, row[3]))
                                    touched[canonical_rel] = canonical_key
                                    touched[entry['rel']] = canonical_key
                                    inode_snapshots[canonical_key] = _record(canonical_dst)
                                    counts['hardlinks'] += 1
                                    counts['hardlink_bytes'] += source_st.st_size
                                    self.db.commit()
                                    continue
                                if not owned or not stat.S_ISREG(existing.st_mode):
                                    raise RuntimeError('existing unowned destination differs from canonical hardlink')
                                if not same_snapshot(existing, json.loads(alias_row[0])):
                                    if not alias_row[2] or stable_hash(dstfd, name, existing) != alias_row[2]:
                                        raise RuntimeError('owned hardlink alias changed after its earlier copy')
                            if (_identity(os.stat(canonical_srcname, dir_fd=canonical_srcfd,
                                                  follow_symlinks=False)) != _identity(canonical_src) or
                                    _identity(os.stat(entry['rel'].rsplit('/', 1)[-1], dir_fd=srcfd,
                                                      follow_symlinks=False)) != _identity(source_st)):
                                raise RuntimeError('source hardlink changed before publication')
                            temp = '.migration-link-' + uuid.uuid4().hex + '.tmp'
                            os.link(canonical_name, temp, src_dir_fd=canonicalfd, dst_dir_fd=dstfd,
                                    follow_symlinks=False)
                            linked = os.stat(temp, dir_fd=dstfd, follow_symlinks=False)
                            if ((linked.st_dev, linked.st_ino, linked.st_size, linked.st_mtime_ns) !=
                                    (canonical_dst.st_dev, canonical_dst.st_ino,
                                     canonical_dst.st_size, canonical_dst.st_mtime_ns)):
                                raise RuntimeError('canonical hardlink changed during publication')
                            if existing is None:
                                os.link(temp, name, src_dir_fd=dstfd, dst_dir_fd=dstfd,
                                        follow_symlinks=False)
                                os.unlink(temp, dir_fd=dstfd)
                                temp = None
                            else:
                                if _identity(os.stat(name, dir_fd=dstfd, follow_symlinks=False)) != _identity(existing):
                                    raise RuntimeError('owned hardlink alias changed before replacement')
                                os.replace(temp, name, src_dir_fd=dstfd, dst_dir_fd=dstfd)
                                temp = None
                            current = os.stat(name, dir_fd=dstfd, follow_symlinks=False)
                            self.db.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?,?)',
                                            (entry['rel'], json.dumps(_record(source_st)),
                                             json.dumps(_record(current)), True, row[3]))
                            touched[canonical_rel] = canonical_key
                            touched[entry['rel']] = canonical_key
                            inode_snapshots[canonical_key] = _record(current)
                            counts['hardlinks'] += 1
                            counts['hardlink_bytes'] += source_st.st_size
                            # Background finalization must not retain a write
                            # transaction while the next payload phase scans.
                            self.db.commit()
                        except Exception as exc:
                            issue(entry, exc)
                        finally:
                            if temp is not None and dstfd is not None:
                                try:
                                    os.unlink(temp, dir_fd=dstfd)
                                except FileNotFoundError:
                                    pass
                            for fd in (srcfd, dstfd, canonicalfd, canonical_srcfd):
                                if fd is not None:
                                    os.close(fd)
                    elif kind not in ('file', 'symlink'):
                        issue(entry, RuntimeError('unsupported source object: ' + kind))
            # Restore directory metadata only after children have been published.
            dirs.sort(key=lambda e: e['rel'].count('/') + bool(e['rel']), reverse=True)
            for entry in dirs:
                if (self.control / 'STOP').exists():
                    raise RuntimeError('STOP requested during directory metadata')
                srcfd = dstfd = None
                try:
                    ensure_destination_directory(self.cfg['destination'], entry['rel'])
                    srcfd = _open_root(os.path.join(self.cfg['source'], entry['rel']))
                    dstfd = _open_root(os.path.join(self.cfg['destination'], entry['rel']))
                    warnings = []
                    _metadata_fd(srcfd, dstfd, os.fstat(srcfd), warnings)
                    if warnings:
                        issue(entry, RuntimeError('; '.join(warnings)))
                    counts['directories'] += 1
                except Exception as exc:
                    issue(entry, exc)
                finally:
                    for fd in (srcfd, dstfd):
                        if fd is not None:
                            os.close(fd)
            # link/unlink changes ctime on every alias of an inode. Record all
            # final snapshots together, after the last link operation.
            for rel, key in touched.items():
                if (self.control / 'STOP').exists():
                    raise RuntimeError('STOP requested during hardlink snapshots')
                parent = None
                try:
                    parent, name = _open_parent(self.cfg['destination'], rel)
                    current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if not same_snapshot(current, inode_snapshots[key]):
                        raise RuntimeError('hardlink destination changed before final snapshot')
                    self.db.execute('UPDATE files SET destination=? WHERE rel=?',
                                    (json.dumps(_record(current)), rel))
                    self.db.commit()
                except Exception as exc:
                    issue({'rel': rel}, exc)
                finally:
                    if parent is not None:
                        os.close(parent)
            self.db.commit()
        return dict(counts)

    def run(self):
        import fcntl
        lock = open('/tmp/' + self.cfg['run_id'] + '-coordinator.lock', 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        validate_mounts(self.cfg)
        configure_rate_limit(32 * 1024**2)
        (self.control / 'nodes').mkdir(exist_ok=True)
        try:
            for phase in (1, 2, 3):
                rounds = []
                for round_no in (1, 2):
                    stage, manifest, scan = self.scan(phase, round_no)
                    self.drain(stage)
                    summary = self.collect(stage)
                    self.save(status='finalizing')
                    final = self.finalize(manifest)
                    self.phase_progress[str(phase)].update(
                        verified_files=summary.get('verified_files', 0) + final.get('hardlinks', 0),
                        verified_bytes=summary.get('verified_bytes', 0) + final.get('hardlink_bytes', 0))
                    rounds.append({'stage': stage, 'scan': scan, 'summary': summary, 'finalize': final})
                self.state['phases'][str(phase)] = {'finished': now(), 'rounds': rounds}
                self.save(status='phase_complete')
                self.event('phase_complete', phase=phase, rounds=rounds)
            # Repeat an incremental catch-up for the higher-priority trees after
            # the large remainder, which may have taken hours to copy.
            for phase in (1, 2):
                stage, manifest, scan = self.scan(phase, 3)
                self.drain(stage)
                summary = self.collect(stage)
                self.save(status='finalizing')
                final = self.finalize(manifest)
                self.phase_progress[str(phase)].update(
                    verified_files=summary.get('verified_files', 0) + final.get('hardlinks', 0),
                    verified_bytes=summary.get('verified_bytes', 0) + final.get('hardlink_bytes', 0))
                self.state['phases'][str(phase)]['rounds'].append(
                    {'stage': stage, 'scan': scan, 'summary': summary, 'finalize': final})
            unresolved = 0
            for phase in self.state['phases'].values():
                latest = phase['rounds'][-1]
                unresolved += sum(v for k, v in latest['summary'].items()
                                  if k not in GOOD and k not in ('bytes', 'metadata_warnings', 'verified_files', 'verified_bytes'))
                unresolved += latest['summary'].get('metadata_warnings', 0)
                unresolved += latest['finalize'].get('errors', 0)
                unresolved += latest['scan'].get('errors', 0)
            self.save(status='complete_with_changes' if unresolved else 'complete',
                      unresolved=unresolved, finished=now(), stage=None)
            self.event('finished', status=self.state['status'], unresolved=unresolved)
        except BaseException:
            self.save(status='stopped' if (self.control / 'STOP').exists() else 'failed',
                      error=traceback.format_exc(), failed=now())
            self.event('failed', error=self.state['error'])
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['coordinator', 'worker'])
    parser.add_argument('--control', required=True)
    parser.add_argument('--threads', type=int, default=8)
    args = parser.parse_args()
    os.umask(0o077)
    if args.mode == 'worker':
        worker(args.control, args.threads)
    else:
        Coordinator(args.control).run()


if __name__ == '__main__':
    main()
