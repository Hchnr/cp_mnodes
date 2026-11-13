#!/usr/bin/env python3
"""Resumable, distributed, read-only source-to-target namespace audit.

Directory discovery and acknowledgements are committed together to a shared
SQLite journal. Only the elected controller writes it. Workers never modify
either tree. Same-size matches are not content hashes or point-in-time proof.
"""
import argparse
import collections
import concurrent.futures
import csv
import errno
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import queue
import shlex
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import uuid


RUN = Path('/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae')
SOURCE = '/share-mixed/projset'
TARGET = '/share-mixed/project'
EXCLUDE = ('eai_pwm/datasets/Sekai',)
THREADS = 4


def encoded(value):
    # SQLite cannot encode surrogate-escaped POSIX filenames as UTF-8.
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'))


def excluded(rel):
    return any(rel == x or rel.startswith(x + '/') for x in EXCLUDE)


def atomic(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, ensure_ascii=True, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def kind(mode):
    if stat.S_ISDIR(mode):
        return 'directory'
    if stat.S_ISREG(mode):
        return 'file'
    if stat.S_ISLNK(mode):
        return 'symlink'
    return 'special'


def unchanged(before, after):
    return all(getattr(before, 'st_' + name) == getattr(after, 'st_' + name)
               for name in ('dev', 'ino', 'mtime_ns', 'ctime_ns'))


def scan_directory(native, source_fd, target_fd, rel, emit, batch_size=512, final_emit=None):
    """Stream source children and target differences; never follow symlinks.

    Return counts for this directory only. Any failure or changed directory
    makes the caller retry the entire directory; partial children are safe to
    rediscover and provisional issues are replaced on the successful attempt.
    """
    if excluded(rel):
        raise ValueError('excluded path was scheduled')
    children, issues = [], []
    counts = collections.Counter()

    def flush(final=False):
        nonlocal children, issues
        if children or issues:
            callback = final_emit if final and final_emit is not None else emit
            callback(children, issues)
            children, issues = [], []

    try:
        src = native.open_directory_beneath(source_fd, rel)
    except FileNotFoundError:
        if not rel:
            raise
        callback = final_emit if final_emit is not None else emit
        callback([], [{'rel': rel, 'issue': 'source_disappeared',
                       'source_type': 'directory', 'target_type': 'unknown'}])
        return {'source_disappeared': 1}
    dst = None
    try:
        before = os.fstat(src)
        target_state = 'directory'
        try:
            dst = native.open_directory_beneath(target_fd, rel)
            target_before = os.fstat(dst)
        except FileNotFoundError:
            target_state = 'missing'
        except OSError as error:
            if error.errno in (errno.ENOTDIR, errno.ELOOP):
                target_state = 'blocked'
            else:
                raise
        with os.scandir(src) as entries:
            for entry in entries:
                path = rel + '/' + entry.name if rel else entry.name
                if excluded(path):
                    counts['excluded_entries'] += 1
                    continue
                info = entry.stat(follow_symlinks=False)
                source_kind = kind(info.st_mode)
                counts['entries'] += 1
                counts[source_kind + 's'] += 1
                if source_kind == 'file':
                    counts['file_bytes'] += info.st_size
                if info.st_dev != before.st_dev:
                    issues.append({'rel': path, 'issue': 'nested_source_mount',
                                   'source_type': source_kind, 'target_type': 'unknown'})
                    counts['nested_source_mount'] += 1
                    continue
                if source_kind == 'directory':
                    children.append(path)
                if dst is None:
                    target_kind = 'missing' if target_state == 'missing' else 'blocked'
                    target_info = None
                else:
                    try:
                        target_info = os.stat(entry.name, dir_fd=dst, follow_symlinks=False)
                        target_kind = kind(target_info.st_mode)
                    except FileNotFoundError:
                        target_info, target_kind = None, 'missing'
                issue, detail = None, {}
                if target_kind == 'missing':
                    issue = 'missing_target'
                elif target_kind == 'blocked':
                    issue = 'target_ancestor_not_directory'
                elif source_kind != target_kind:
                    issue = 'different_type'
                elif source_kind == 'file' and info.st_size != target_info.st_size:
                    issue = 'different_size'
                elif source_kind == 'file' and info.st_mtime_ns != target_info.st_mtime_ns:
                    issue = 'different_mtime_same_size'
                elif source_kind == 'symlink':
                    source_link = os.readlink(entry.name, dir_fd=src)
                    target_link = os.readlink(entry.name, dir_fd=dst)
                    if source_link != target_link:
                        issue = 'different_link'
                        detail = {'source_link': source_link, 'target_link': target_link}
                elif source_kind == 'special':
                    issue = 'special_type_unverified'
                if issue:
                    counts[issue] += 1
                    if issue == 'missing_target' and source_kind == 'file':
                        counts['missing_file_bytes'] += info.st_size
                    issues.append(dict(rel=path, issue=issue, source_type=source_kind,
                                       target_type=target_kind, source_bytes=info.st_size,
                                       target_bytes=target_info.st_size if target_info else None,
                                       **detail))
                else:
                    counts['same_type_and_metadata'] += 1
                if len(children) + len(issues) >= batch_size:
                    flush()
        if not unchanged(before, os.fstat(src)):
            raise RuntimeError('source directory changed during scan')
        if dst is not None and not unchanged(target_before, os.fstat(dst)):
            raise RuntimeError('target directory changed during scan')
        flush(final=True)
        return dict(counts)
    finally:
        os.close(src)
        if dst is not None:
            os.close(dst)


def database(path, root_rel=''):
    db = sqlite3.connect(str(path), timeout=60)
    db.execute('pragma journal_mode=TRUNCATE')
    db.execute('pragma synchronous=FULL')
    db.execute('pragma temp_store=MEMORY')
    db.execute('pragma cache_size=-32768')
    db.executescript('''
      create table if not exists dirs(
        id integer primary key, rel text not null unique, state integer not null default 0,
        node text, attempt integer not null default 0, next real not null default 0,
        updated real not null default 0);
      create index if not exists dir_queue on dirs(state,next,id);
      create table if not exists issues(
        parent integer not null, rel text not null, issue text not null,
        attempt integer not null, data text not null,
        primary key(parent,rel,issue));
      create table if not exists failures(
        dir integer primary key, attempts integer not null, detail text not null,
        last_time real not null, resolved integer not null default 0);
      create table if not exists meta(key text primary key, value text not null);
    ''')
    if db.execute('select 1 from meta where key="stats"').fetchone() is None:
        if excluded(root_rel):
            raise ValueError('excluded root')
        db.execute('insert into dirs(rel) values(?)', (encoded(root_rel),))
        db.execute('insert into meta values(?,?)', ('stats', encoded({
            'discovered_dirs': 1, 'done_dirs': 0, 'counts': {}, 'nodes': {}, 'retries': 0})))
    # A controller death may interrupt any lease. Chunk commits remain durable;
    # rescanning is read-only and exact completion counters are transactional.
    db.execute('update dirs set state=0,node=null where state=1')
    db.commit()
    return db


def consume(db, stats, node, msg):
    """Apply a worker message inside the controller's transaction."""
    ident, attempt = msg['id'], msg['attempt']
    row = db.execute('select state,node,attempt from dirs where id=?', (ident,)).fetchone()
    if row != (1, node, attempt):
        return False
    event = msg['event']
    if event == 'chunk' or (event == 'done' and
                            ('final_children' in msg or 'final_issues' in msg)):
        children = msg['children'] if event == 'chunk' else msg.get('final_children', [])
        issues = msg['issues'] if event == 'chunk' else msg.get('final_issues', [])
        if not isinstance(children, list) or not isinstance(issues, list):
            raise ValueError('invalid directory chunk')
        for rel in children:
            if excluded(rel):
                raise ValueError('worker attempted to enqueue excluded path')
            cur = db.execute('insert or ignore into dirs(rel) values(?)', (encoded(rel),))
            stats['discovered_dirs'] += cur.rowcount
        for issue in issues:
            db.execute('insert or replace into issues values(?,?,?,?,?)',
                       (ident, encoded(issue['rel']), issue['issue'], attempt, encoded(issue)))
    if event == 'chunk':
        pass
    elif event == 'done':
        db.execute('delete from issues where parent=? and attempt<>?', (ident, attempt))
        db.execute('update dirs set state=2,node=null,updated=? where id=?', (time.time(), ident))
        db.execute('update failures set resolved=1 where dir=?', (ident,))
        stats['done_dirs'] += 1
        ns = stats['nodes'].setdefault(node, {})
        ns['done_dirs'] = ns.get('done_dirs', 0) + 1
        for key, value in msg['counts'].items():
            stats['counts'][key] = stats['counts'].get(key, 0) + value
            ns[key] = ns.get(key, 0) + value
    elif event == 'retry':
        delay = min(300, 2 ** min(attempt, 8))
        db.execute('update dirs set state=3,node=null,next=?,updated=? where id=?',
                   (time.time() + delay, time.time(), ident))
        db.execute('insert into failures values(?,?,?,?,0) on conflict(dir) do update set '
                   'attempts=excluded.attempts,detail=excluded.detail,last_time=excluded.last_time,resolved=0',
                   (ident, attempt, msg['detail'][:1000], time.time()))
        stats['retries'] += 1
    else:
        raise ValueError('invalid worker event')
    return True


def worker(run, work, node):
    os.environ['PROJSET_SSH_TOPOLOGY'] = str(run / 'recovery/ssh.json')
    recovery = module('audit_recovery', run / 'recovery/code/projset_recovery_copy.py')
    cfg = json.loads((run / 'config.json').read_text())
    recovery.validate(run, cfg, node)
    manifest = json.loads((work / 'manifest.json').read_text())
    if hashlib.sha256((work / 'code/projset_full_audit.py').read_bytes()).hexdigest() != manifest['code_sha256']:
        raise ValueError('audit code changed')
    native = recovery.native
    source_fd = native._open_absolute_directory(SOURCE)
    target_fd = native._open_absolute_directory(TARGET)
    lock = threading.Lock()
    waiting = {}
    stopped = threading.Event()

    def send(value):
        with lock:
            print(encoded(value), flush=True)

    def execute(task):
        ident, attempt = task['id'], task['attempt']
        sequence = 0
        pending = None

        def send_chunk(children, issues):
            nonlocal sequence
            sequence += 1
            token = (ident, attempt, sequence)
            gate = threading.Event()
            with lock:
                waiting[token] = gate
            send({'event': 'chunk', 'id': ident, 'attempt': attempt, 'sequence': sequence,
                  'children': children, 'issues': issues})
            while not gate.wait(1):
                if stopped.is_set():
                    raise RuntimeError('controller connection closed')

        def emit_final(children, issues):
            nonlocal pending
            pending = (children, issues)

        try:
            # Full chunks are ACKed immediately, preserving discovery and
            # backpressure even if the remainder takes hours to enumerate.
            # Older controllers omit this capability and retain the original
            # final-chunk ACK protocol during a mixed-version handover.
            if task.get('commit_final_chunk_v1'):
                counts = scan_directory(native, source_fd, target_fd, task['rel'], send_chunk,
                                        final_emit=emit_final)
            else:
                counts = scan_directory(native, source_fd, target_fd, task['rel'], send_chunk)
            done = {'event': 'done', 'id': ident, 'attempt': attempt, 'counts': counts}
            if pending is not None:
                done.update(final_children=pending[0], final_issues=pending[1])
            send(done)
        except Exception as error:
            send({'event': 'retry', 'id': ident, 'attempt': attempt,
                  'detail': type(error).__name__ + ': ' + str(error)})

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)
    send({'event': 'ready', 'node': node, 'host': socket.gethostname(), 'pid': os.getpid()})
    try:
        for line in sys.stdin:
            msg = json.loads(line)
            if msg['event'] == 'task':
                pool.submit(execute, msg['task'])
            elif msg['event'] == 'ack':
                token = (msg['id'], msg['attempt'], msg['sequence'])
                with lock:
                    gate = waiting.pop(token, None)
                if gate:
                    gate.set()
            elif msg['event'] == 'stop':
                break
    finally:
        stopped.set()
        pool.shutdown(wait=False)
        os.close(source_fd)
        os.close(target_fd)
    return 0


def export(db, work):
    tmp = work / ('issues.' + uuid.uuid4().hex + '.tmp')
    columns = ['relative_path', 'source_path', 'target_path', 'issue', 'source_type',
               'target_type', 'source_bytes', 'target_bytes', 'source_link', 'target_link']
    with tmp.open('w', encoding='utf-8', errors='backslashreplace', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for raw, in db.execute('select data from issues order by parent,rel,issue'):
            item = json.loads(raw)
            rel = item.pop('rel')
            writer.writerow(dict(item, relative_path=rel, source_path=SOURCE + '/' + rel,
                                 target_path=TARGET + '/' + rel))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, work / 'issues.csv')


def controller(run, work):
    os.environ['PROJSET_SSH_TOPOLOGY'] = str(run / 'recovery/ssh.json')
    recovery = module('audit_recovery_controller', run / 'recovery/code/projset_recovery_copy.py')
    topology = json.loads((run / 'recovery/ssh.json').read_text())
    manifest = json.loads((work / 'manifest.json').read_text())
    nodes = list(topology['hosts'])
    with (work / 'controller.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            existing = json.loads((work / 'status.json').read_text())
            if existing.get('state') == 'finished':
                return 0
        except (OSError, ValueError):
            pass
        db = database(work / 'audit.sqlite3', manifest.get('root_rel', ''))
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        events = queue.Queue(maxsize=512)
        clients = {}
        active = {}
        retry_at = {}
        start = time.time()
        tick = 0
        reporter = None
        reporter_retry_at = 0

        def reader(node, generation, proc):
            try:
                for line in proc.stdout:
                    events.put((node, generation, json.loads(line)))
            except Exception as error:
                events.put((node, generation, {'event': 'reader_error', 'detail': repr(error)}))
            finally:
                events.put((node, generation, {'event': 'eof'}))

        def send(node, msg):
            client = clients[node]
            try:
                client['process'].stdin.write(encoded(msg) + '\n')
                client['process'].stdin.flush()
                return True
            except (OSError, ValueError, BrokenPipeError):
                return False

        def launch(node):
            # The process may exit before its reader's EOF event is delivered.
            # Requeue its leases before replacing the generation, otherwise the
            # stale EOF would be ignored and those directories would be lost.
            db.execute('begin immediate')
            for ident, owner in list(active.items()):
                if owner == node:
                    db.execute('update dirs set state=0,node=null where id=? and state=1', (ident,))
                    active.pop(ident, None)
            db.commit()
            generation = uuid.uuid4().hex
            command = ('python3 -S -B ' + shlex.quote(str(work / 'code/projset_full_audit.py')) +
                       ' worker --run ' + shlex.quote(str(run)) + ' --work ' + shlex.quote(str(work)) +
                       ' --node ' + shlex.quote(node))
            with (work / 'logs' / (node + '.log')).open('ab') as log:
                proc = subprocess.Popen(recovery.ssh(node, command), stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)
            clients[node] = {'process': proc, 'generation': generation, 'ready': False,
                             'host': '', 'last': time.time()}
            threading.Thread(target=reader, args=(node, generation, proc), daemon=True).start()
            retry_at[node] = time.time() + 15

        def snapshot(state):
            waiting = db.execute('select count(*) from dirs where state=3').fetchone()[0]
            counts = stats['counts']
            status = dict(state=state, updated=time.time(), started=start, controller_host=socket.gethostname(),
                          controller_pid=os.getpid(), nodes_total=len(nodes),
                          nodes_ready=sum(bool(c['ready']) for c in clients.values()),
                          discovered_dirs=stats['discovered_dirs'], done_dirs=stats['done_dirs'],
                          active_dirs=len(active), retry_dirs=waiting,
                          pending_dirs=stats['discovered_dirs']-stats['done_dirs']-len(active)-waiting,
                          counts=counts, retries=stats['retries'], node_counts=stats['nodes'],
                          scope_root=manifest.get('root_rel', ''), excluded=list(EXCLUDE),
                          verification='namespace, type, size, mtime and link text; no full content hash',
                          online_source_not_atomic=True)
            atomic(work / 'status.json', status)
            return status

        try:
            while True:
                for node in nodes:
                    client = clients.get(node)
                    if (client is None or client['process'].poll() is not None) and time.time() >= retry_at.get(node, 0):
                        launch(node)
                if (manifest.get('reporter', True) and time.time() >= reporter_retry_at and
                        (reporter is None or reporter.poll() is not None)):
                    cfg = json.loads((run / 'recovery/ssh.json').read_text())
                    with (work / 'swanlab.log').open('ab') as log:
                        reporter = subprocess.Popen([cfg['reporter_python'], '-B',
                            str(work / 'code/projset_full_audit.py'), 'reporter', '--work', str(work)],
                            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                    # A surviving reporter on another host owns the lock.
                    # Avoid rapidly relaunching candidates during failover.
                    reporter_retry_at = time.time() + 30
                batch = []
                try:
                    batch.append(events.get(timeout=0.2))
                    while len(batch) < 512:
                        batch.append(events.get_nowait())
                except queue.Empty:
                    pass
                acks = []
                if batch:
                    db.execute('begin immediate')
                    for node, generation, msg in batch:
                        client = clients.get(node)
                        if not client or client['generation'] != generation:
                            continue
                        client['last'] = time.time()
                        event = msg['event']
                        if event == 'ready':
                            client.update(ready=True, host=msg['host'])
                        elif event in ('chunk', 'done', 'retry'):
                            if consume(db, stats, node, msg):
                                if event == 'chunk':
                                    acks.append((node, {'event': 'ack', 'id': msg['id'],
                                                       'attempt': msg['attempt'], 'sequence': msg['sequence']}))
                                else:
                                    active.pop(msg['id'], None)
                        elif event in ('eof', 'reader_error'):
                            client['ready'] = False
                            for ident, owner in list(active.items()):
                                if owner == node:
                                    db.execute('update dirs set state=0,node=null where id=? and state=1', (ident,))
                                    active.pop(ident, None)
                    db.execute('update meta set value=? where key="stats"', (encoded(stats),))
                assignments = []
                accepting = not (work / 'STOP').exists()
                if accepting:
                    # Complete messages and the next leases atomically. This
                    # avoids a second durability sync for the same event loop.
                    if not batch:
                        db.execute('begin immediate')
                    for node in nodes:
                        client = clients.get(node)
                        if not client or not client['ready']:
                            continue
                        free = THREADS - sum(owner == node for owner in active.values())
                        for _ in range(free):
                            row = db.execute('select id,rel,attempt from dirs where state=3 and next<=? order by next,id limit 1', (time.time(),)).fetchone()
                            if row is None:
                                row = db.execute('select id,rel,attempt from dirs where state=0 order by next,id limit 1').fetchone()
                            if row is None:
                                break
                            ident, rel, attempt = row
                            attempt += 1
                            db.execute('update dirs set state=1,node=?,attempt=?,updated=? where id=?',
                                       (node, attempt, time.time(), ident))
                            active[ident] = node
                            assignments.append((node, {'event': 'task', 'task':
                                {'id': ident, 'attempt': attempt, 'rel': json.loads(rel),
                                 'commit_final_chunk_v1': True}}))
                if batch or accepting:
                    db.commit()
                # Workers may only observe chunks and leases after all of the
                # corresponding durable queue updates have committed.
                for node, ack in acks:
                    send(node, ack)
                for node, task in assignments:
                    if not send(node, task):
                        clients[node]['process'].terminate()
                if time.time() - tick >= 10:
                    snapshot('running')
                    tick = time.time()
                if stats['done_dirs'] == stats['discovered_dirs'] and not active:
                    snapshot('exporting')
                    export(db, work)
                    snapshot('finished')
                    break
                if (work / 'STOP').exists() and not active:
                    snapshot('stopped')
                    break
        finally:
            for node, client in clients.items():
                if client['ready']:
                    send(node, {'event': 'stop'})
                try:
                    client['process'].stdin.close()
                except (OSError, ValueError):
                    pass
            db.close()
    return 0


def repair_values(state):
    """Expose repair outcomes separately from historical audit observations."""
    outcomes = state.get('outcomes', {})
    return {'repair/已接入路径': state.get('total', 0),
            'repair/已处理路径': state.get('done', 0),
            'repair/待处理路径': state.get('pending', 0) + state.get('active', 0) + state.get('retry', 0),
            'repair/补传操作累计': state.get('copied_operations', 0),
            'repair/补传_TB': state.get('copied_bytes', 0) / 1e12,
            'repair/已确认相同': outcomes.get('already_verified', 0),
            'repair/目录已就绪': outcomes.get('directory_ready', 0),
            'repair/人工关注': sum(v for k, v in outcomes.items() if k.startswith('manual_')),
            'repair/待重试路径': state.get('retry', 0),
            'repair/历史重试次数': state.get('retries', 0),
            'repair/在线节点': state.get('nodes_ready', 0)}


def speed_values(scan, repair, previous):
    """Calculate rates from committed counters; metadata TB is logical size."""
    counts = scan.get('counts', {})
    sample = {'scan_time': scan.get('updated', 0),
              'files_links': counts.get('files', 0) + counts.get('symlinks', 0),
              'file_bytes': counts.get('file_bytes', 0),
              'repair_time': repair.get('updated', 0),
              'copied_operations': repair.get('copied_operations', 0),
              'copied_bytes': repair.get('copied_bytes', 0)}
    values = {'audit/已核对普通文件': counts.get('files', 0),
              'audit/已核对文件及链接': sample['files_links']}
    if previous:
        for time_key, metrics in (
            ('scan_time', (('files_links', 'speed/复核文件及链接_项每小时', 1),
                           ('file_bytes', 'speed/复核逻辑容量_TB每小时', 1e12))),
            ('repair_time', (('copied_operations', 'speed/补传文件及链接_项每小时', 1),
                             ('copied_bytes', 'speed/补传数据_TB每小时', 1e12)))):
            elapsed = sample[time_key] - previous.get(time_key, 0)
            if elapsed <= 0 or not previous.get(time_key):
                continue
            for key, metric, divisor in metrics:
                delta = sample[key] - previous.get(key, sample[key])
                if delta >= 0:
                    values[metric] = delta * 3600 / elapsed / divisor
    return values, sample


def reporter_values(status, repair_state, previous):
    """Build one reporter payload from a single scan and repair observation."""
    counts = status['counts']
    values = {'audit/已核对项': counts.get('entries', 0),
              'audit/已核对文件_TB': counts.get('file_bytes', 0) / 1e12,
              'audit/目标缺失项': counts.get('missing_target', 0),
              'audit/目标缺失文件_TB': counts.get('missing_file_bytes', 0) / 1e12,
              'audit/大小不同项': counts.get('different_size', 0),
              'audit/类型不同项': counts.get('different_type', 0),
              'audit/链接不同项': counts.get('different_link', 0),
              'audit/同大小时间不同项': counts.get('different_mtime_same_size', 0),
              'audit/目标父路径受阻项': counts.get('target_ancestor_not_directory', 0),
              'audit/源路径已消失项': counts.get('source_disappeared', 0),
              'queue/已发现目录': status['discovered_dirs'],
              'queue/已完成目录': status['done_dirs'],
              'queue/待处理目录': status['pending_dirs'],
              'queue/待重试目录': status['retry_dirs'],
              'nodes/在线': status['nodes_ready']}
    if repair_state is not None:
        values.update(repair_values(repair_state))
    rates, sample = speed_values(status, repair_state or {}, previous)
    values.update(rates)
    return values, sample


def monitor_values(scan, monitor, now, repair=None):
    """Expose fresh 20-minute measurements and explicitly rough ETA scenarios."""
    values = {'estimate20/文件外推数据有效': 0, 'estimate20/容量外推数据有效': 0,
              'estimate20/已发现目录外推数据有效': 0, 'nfs20/主控目标挂载数据有效': 0}

    def age_minutes(state):
        if not isinstance(state, dict):
            return None
        updated = state.get('updated', 0)
        if not isinstance(updated, (int, float)) or not math.isfinite(updated) or updated <= 0:
            return None
        age = now - updated
        return max(age, 0) / 60 if age >= -60 else None

    scan_age, repair_age, monitor_age = (age_minutes(state) for state in (scan, repair, monitor))
    for age, label in ((scan_age, 'monitor/复核心跳距今_分钟'),
                       (repair_age, 'monitor/补传心跳距今_分钟'),
                       (monitor_age, 'monitor/二十分钟采样距今_分钟')):
        if age is not None:
            values[label] = age
    # A stale monitor or controller is not evidence of throughput or completion.
    if (scan_age is None or scan_age > 5 or not isinstance(scan, dict) or
            scan.get('state') != 'running' or monitor_age is None or monitor_age > 45 or
            not isinstance(monitor, dict) or monitor.get('audit_state') != 'running'):
        return values
    counts = scan.get('counts', {})
    rates, estimates = monitor.get('rates', {}), monitor.get('estimates', {})
    if not all(isinstance(item, dict) for item in (counts, rates, estimates)):
        return values
    # A 20-minute sample that began before this controller started remains a
    # valid historical rate, but cannot describe its current operating regime.
    sample_time = monitor.get('sample', {}).get('audit_updated')
    interval = rates.get('audit_updated_seconds')
    started = scan.get('started')
    stable_interval = (all(isinstance(x, (int, float)) and math.isfinite(x)
                           for x in (sample_time, interval, started)) and
                       interval > 0 and started <= sample_time - interval)
    fields = (
        ('audit_files_links_per_hour', 'speed20/复核文件及链接_项每小时',
         'files_baseline', counts.get('files', 0) + counts.get('symlinks', 0),
         'estimate20/按首轮累计项外推_剩余小时', 'estimate20/文件外推数据有效'),
        ('audit_logical_TB_per_hour', 'speed20/复核逻辑容量_TB每小时',
         'logical_TB_baseline', counts.get('file_bytes', 0) / 1e12,
         'estimate20/按首轮累计TB外推_剩余小时', 'estimate20/容量外推数据有效'))
    for rate_key, rate_label, base_key, current, eta_label, valid_label in fields:
        rate, baseline = rates.get(rate_key), estimates.get(base_key)
        if not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate < 0:
            continue
        values[rate_label] = rate
        if (stable_interval and rate > 0 and isinstance(baseline, (int, float)) and math.isfinite(baseline)
                and baseline > current):
            values[eta_label] = (baseline - current) / rate
            values[valid_label] = 1
    directory_rate = rates.get('audit_directories_per_hour')
    if isinstance(directory_rate, (int, float)) and math.isfinite(directory_rate) and directory_rate >= 0:
        values['speed20/完成目录_项每小时'] = directory_rate
        backlog = sum(scan.get(key, 0) for key in ('pending_dirs', 'active_dirs', 'retry_dirs'))
        if stable_interval and directory_rate > 0 and backlog > 0:
            # This excludes directories that have not yet been discovered.
            values['estimate20/仅已发现目录外推_剩余小时'] = backlog / directory_rate
            values['estimate20/已发现目录外推数据有效'] = 1
    # Mount counters include other tasks. Require the sampled controller PID
    # and host to still match, and an entire interval under that controller.
    for probe in monitor.get('probes', []) if stable_interval else []:
        if not (probe.get('identity_ok') and probe.get('host') == scan.get('controller_host')):
            continue
        controllers = probe.get('roles', {}).get('audit', {}).get('controllers', [])
        if not any(p.get('pid') == scan.get('controller_pid') for p in controllers):
            continue
        write = (monitor.get('nfs_rates', {}).get(probe.get('node'), {})
                 .get('target', {}).get('WRITE', {}))
        rtt, ops = write.get('mean_rtt_ms'), write.get('requests_per_second')
        if (write.get('requests', 0) > 0 and
                all(isinstance(x, (int, float)) and math.isfinite(x) and x >= 0 for x in (rtt, ops))):
            values['nfs20/主控目标挂载WRITE平均RTT_秒'] = rtt / 1000
            values['nfs20/主控目标挂载WRITE_次每秒'] = ops
            values['nfs20/主控目标挂载数据有效'] = 1
        break
    return values


def reporter(work):
    with (work / 'reporter.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'all_proxy'):
            os.environ.pop(key, None)
        secrets = Path('/share-mixed/project/eai_pwm/.secrets')
        proxy = (secrets / 'swanlab_proxy').read_text().strip()
        os.environ.update({key: proxy for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY')})
        os.environ['SWANLAB_API_KEY'] = (secrets / 'swanlab_api_key').read_text().strip()
        import swanlab
        swanlab.merge_settings(swanlab.Settings(interactive=False, probe=swanlab.Settings.Probe(
            hardware=False, runtime=False, requirements=False, conda=False, git=False, swanlab=False, monitor=False)))
        path = work / 'swanlab.json'
        try:
            state = json.loads(path.read_text())
        except (OSError, ValueError):
            state = {}
        run = None
        while True:
            try:
                time.sleep(max(0, state.get('enqueued_at', 0) + 120 - time.time()))
                status = json.loads((work / 'status.json').read_text())
                if run is None:
                    swanlab.login(api_key=os.environ['SWANLAB_API_KEY'], save=False, timeout=10)
                    (work / 'swanlab-sdk').mkdir(exist_ok=True)
                    run = swanlab.init(mode='online', project='data-migration',
                        name='projset-全量遗漏复核-' + work.name[-8:],
                        id=state.get('run_id'), resume='allow' if state.get('run_id') else 'never',
                        public=True, log_dir=str(work / 'swanlab-sdk'),
                        config={'nodes': 16, 'upload_interval_seconds': 120, 'mode': 'read_only_metadata',
                                'content_hash': False, 'excluded': list(EXCLUDE)})
                    state.update(run_id=run.id, url=run.url, run_path=run.path)
                    atomic(path, state)
                repair_work = work / 'repair'
                repair_required = (repair_work / 'manifest.json').exists()
                repair_state = {}
                payload_repair = None
                if repair_required:
                    try:
                        repair_state = json.loads((repair_work / 'status.json').read_text())
                        if not isinstance(repair_state, dict):
                            raise TypeError('repair status must be a mapping')
                        payload_repair = repair_state
                    except (OSError, ValueError):
                        # The repair may still be starting; its absence must not
                        # cause the combined run to finish prematurely.
                        pass
                values, sample = reporter_values(status, payload_repair, state.get('last_sample'))
                try:
                    monitored = json.loads((work / 'monitor20/status.json').read_text())
                except (OSError, ValueError, TypeError):
                    monitored = None
                # Share the cadence gate with historical attention reporters.
                with (work.parent / 'attention-swanlab-cadence.lock').open('a') as gate:
                    fcntl.flock(gate, fcntl.LOCK_EX)
                    cadence = work.parent / 'attention-swanlab-cadence.json'
                    try:
                        last = json.loads(cadence.read_text()).get('enqueued_at', 0)
                    except (OSError, ValueError):
                        last = 0
                    time.sleep(max(0, last + 120 - time.time()))
                    now = time.time()
                    # The cadence lock can delay this upload. Judge heartbeat
                    # and ETA freshness at sending time, not before the wait.
                    try:
                        values.update(monitor_values(status, monitored, now, repair_state))
                    except (ValueError, TypeError, KeyError, AttributeError):
                        # Malformed monitor data must not interrupt the other
                        # metrics or leave an old ETA marked valid.
                        values.update({'estimate20/文件外推数据有效': 0,
                                       'estimate20/容量外推数据有效': 0,
                                       'estimate20/已发现目录外推数据有效': 0,
                                       'nfs20/主控目标挂载数据有效': 0})
                    step = int(state.get('step', -1)) + 1
                    state.update(state='sending', step=step, enqueued_at=now, interval_seconds=120)
                    atomic(path, state)
                    atomic(cadence, {'enqueued_at': now, 'interval_seconds': 120})
                    run.log(values, step=step)
                    atomic(cadence, {'enqueued_at': time.time(), 'interval_seconds': 120})
                state.update(state='streaming', updated=time.time(), last_sample=sample)
                atomic(path, state)
                if (status['state'] == 'finished' and
                        (not repair_required or repair_state.get('state') == 'finished')):
                    run.finish()
                    state.update(state='finished', finished_at=time.time())
                    atomic(path, state)
                    return 0
            except Exception as error:
                state.update(state='retrying', error_type=type(error).__name__, updated=time.time())
                atomic(path, state)
                time.sleep(120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('controller', 'worker', 'reporter', 'export'))
    parser.add_argument('--run', type=Path, default=RUN)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--node')
    args = parser.parse_args()
    if args.mode == 'worker':
        return worker(args.run.resolve(), args.work.resolve(), args.node)
    if args.mode == 'controller':
        return controller(args.run.resolve(), args.work.resolve())
    if args.mode == 'reporter':
        return reporter(args.work.resolve())
    # The operator may export current, provisional issues without affecting the queue.
    db = sqlite3.connect('file:' + str(args.work.resolve() / 'audit.sqlite3') + '?mode=ro', uri=True, timeout=60)
    try:
        export(db, args.work.resolve())
    finally:
        db.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
