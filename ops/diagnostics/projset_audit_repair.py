#!/usr/bin/env python3
"""Repair missing paths discovered by the live projset namespace audit.

An append-only SQLite outbox connects the existing scan transaction to a
separate resumable copy queue. The scan does not need to restart, and an
unfinished or repeated event can safely be retried without replacing targets.
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
THREADS = 2


def encoded(value):
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'))


def excluded(rel):
    return any(rel == p or rel.startswith(p + '/') for p in EXCLUDE)


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


def install_outbox(audit_db):
    """Atomically capture old issues and every later missing-path event."""
    db = sqlite3.connect(str(audit_db), timeout=60)
    try:
        db.execute('begin immediate')
        exists = db.execute("select 1 from sqlite_master where type='table' and name='repair_outbox'").fetchone()
        if not exists:
            db.execute('create table repair_outbox(id integer primary key autoincrement, rel text not null, data text not null)')
            db.execute('create trigger repair_missing_outbox after insert on issues '
                       "when new.issue='missing_target' begin "
                       'insert into repair_outbox(rel,data) values(new.rel,new.data); end')
            db.execute("insert into repair_outbox(rel,data) select rel,data from issues where issue='missing_target'")
        elif not db.execute("select 1 from sqlite_master where type='trigger' and name='repair_missing_outbox'").fetchone():
            raise RuntimeError('outbox exists without its required capture trigger')
        total = db.execute('select count(*) from repair_outbox').fetchone()[0]
        db.commit()
        return total
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def read_outbox(source, cursor):
    """Return None on a transient SQLite lock; never mistake it for empty."""
    try:
        return source.execute('select id,rel,data from repair_outbox where id>? order by id limit 512',
                              (cursor,)).fetchall()
    except sqlite3.OperationalError as error:
        code = getattr(error, 'sqlite_errorcode', None)
        message = str(error).lower()
        if ((isinstance(code, int) and code & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)) or
                (code is None and message in ('database is locked', 'database table is locked',
                                               'database schema is locked'))):
            return None
        raise


def configure_no_replace(engine):
    """Retain existing targets and their metadata, including race winners."""
    def existing(src_fd, dst_parent, dst_name, source_stat, warnings):
        try:
            target = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(target.st_mode):
            raise engine.DestinationConflict('destination exists with a different type')
        if target.st_size != source_stat.st_size:
            raise engine.DestinationConflict('destination exists with different size')
        fd = os.open(dst_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dst_parent)
        try:
            before = os.fstat(fd)
            if engine._identity(before) != engine._identity(target):
                raise engine.DestinationConflict('destination changed during inspection')
            a = engine._hash_fd(src_fd)
            if engine._identity(os.fstat(src_fd)) != engine._identity(source_stat):
                raise engine.SourceChanged('source changed during checksum')
            b = engine._hash_fd(fd)
            if (engine._identity(os.fstat(fd)) != engine._identity(before) or
                engine._identity(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)) != engine._identity(before)):
                raise engine.DestinationConflict('destination changed during checksum')
            if a != b:
                raise engine.DestinationConflict('destination exists with different checksum')
            return a
        finally:
            os.close(fd)

    def symlink(src_root, dst_root, entry, src_parent, src_name, dst_parent, dst_name,
                temp_name, warnings, created):
        source = os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)
        engine._check_scanned(source, entry)
        if not stat.S_ISLNK(source.st_mode):
            raise engine.SourceChanged('source is no longer a symlink')
        text = os.readlink(src_name, dir_fd=src_parent)
        if text != entry.get('target', text) or engine._identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != engine._identity(source):
            raise engine.SourceChanged('source symlink changed')
        def check_existing():
            try:
                before = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if not stat.S_ISLNK(before.st_mode) or os.readlink(dst_name, dir_fd=dst_parent) != text:
                raise engine.DestinationConflict('destination exists with different symlink target or type')
            if engine._identity(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)) != engine._identity(before):
                raise engine.DestinationConflict('destination symlink changed during inspection')
            return True
        if check_existing():
            return {'status': 'skipped', 'bytes': 0, 'target': text, 'source_identity': engine._record(source)}
        os.symlink(text, temp_name, dir_fd=dst_parent)
        created.append(temp_name)
        engine._metadata_symlink(src_parent, src_name, dst_parent, temp_name, src_root, dst_root,
                                 entry['rel'], source, warnings)
        if engine._identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != engine._identity(source):
            raise engine.SourceChanged('source symlink changed before publication')
        try:
            os.link(temp_name, dst_name, src_dir_fd=dst_parent, dst_dir_fd=dst_parent, follow_symlinks=False)
        except FileExistsError:
            if not check_existing():
                raise engine.DestinationConflict('destination appeared and disappeared during publication')
            return {'status': 'skipped', 'bytes': 0, 'target': text, 'source_identity': engine._record(source)}
        return {'status': 'copied', 'bytes': 0, 'target': text, 'source_identity': engine._record(source)}

    engine._existing_file = existing
    engine._copy_symlink = symlink


class RetryWork(Exception):
    pass


def repair_one(recovery, cfg, rel):
    """Revalidate the source and publish missing entries without replacement."""
    if excluded(rel):
        return {'status': 'excluded', 'rel': rel}
    engine, native = recovery.engine, recovery.native
    parent, name = native.open_parent_readonly(cfg['source'], rel)
    try:
        source = os.stat(name, dir_fd=parent, follow_symlinks=False)
        source_kind = ('directory' if stat.S_ISDIR(source.st_mode) else
                       'file' if stat.S_ISREG(source.st_mode) else
                       'symlink' if stat.S_ISLNK(source.st_mode) else 'special')
        if source_kind == 'special':
            return {'status': 'manual_special', 'rel': rel}
        if source_kind == 'directory':
            try:
                engine.ensure_destination_directory(cfg['target'], rel)
            except OSError as error:
                if error.errno in (errno.ENOTDIR, errno.ELOOP, errno.EEXIST):
                    return {'status': 'manual_conflict', 'rel': rel, 'detail': str(error)}
                raise
            if engine._identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != engine._identity(source):
                raise RetryWork('source directory changed while ensuring destination')
            return {'status': 'directory_ready', 'rel': rel}
        entry = dict(engine._record(source), rel=rel, kind=source_kind)
        if source_kind == 'symlink':
            entry['target'] = os.readlink(name, dir_fd=parent)
        receipt = recovery.copy_entry_with_estale_retry(cfg['source'], cfg['target'], entry,
                                                        cfg['run_id'] + '-audit-repair')
        if receipt.get('status') in ('error', 'changed'):
            # An existing non-directory ancestor is a conflict, not a transient
            # error that can be resolved by replacing the target.
            detail = receipt.get('detail', '')
            if 'NotADirectoryError:' in detail or 'Too many levels of symbolic links' in detail:
                return {'status': 'manual_conflict', 'rel': rel, 'detail': detail, 'receipt': receipt}
            raise RetryWork(detail or receipt.get('status'))
        if receipt.get('status') == 'conflict':
            return {'status': 'manual_conflict', 'rel': rel, 'receipt': receipt}
        if receipt.get('status') not in ('copied', 'skipped'):
            raise RetryWork('unexpected result ' + repr(receipt.get('status')))
        result = {'status': 'copied' if receipt['status'] == 'copied' else 'already_verified',
                  'rel': rel, 'logical_bytes': source.st_size if source_kind == 'file' else 0,
                  'kind': source_kind, 'receipt': receipt}
        if receipt.get('metadata_warnings'):
            result['status'] = 'manual_metadata_warning'
        return result
    finally:
        os.close(parent)


def database(path):
    db = sqlite3.connect(str(path), timeout=60)
    db.execute('pragma journal_mode=TRUNCATE')
    db.execute('pragma synchronous=FULL')
    db.execute('pragma cache_size=-16384')
    db.executescript('''
      create table if not exists tasks(
        id integer primary key, rel text not null unique, latest_seq integer not null,
        leased_seq integer not null default 0, state integer not null default 0,
        node text, attempt integer not null default 0, next real not null default 0,
        last_result text, updated real not null default 0);
      create index if not exists task_queue on tasks(state,next,id);
      create table if not exists meta(key text primary key,value text not null);
    ''')
    db.execute('insert or ignore into meta values(?,?)', ('stats', encoded({
        'cursor': 0, 'total': 0, 'done': 0, 'outcomes': {}, 'retries': 0,
        'copied_operations': 0, 'copied_bytes': 0, 'nodes': {}})))
    db.execute('update tasks set state=0,node=null where state=1')
    db.commit()
    return db


def ingest(db, stats, events):
    for sequence, rel, raw in events:
        # Validation at both queue ingestion and the worker makes exclusions
        # independent of the scan's state or event history.
        path = json.loads(rel)
        if excluded(path):
            stats['cursor'] = sequence
            continue
        row = db.execute('select id,state,last_result from tasks where rel=?', (rel,)).fetchone()
        if row is None:
            db.execute('insert into tasks(rel,latest_seq) values(?,?)', (rel, sequence))
            stats['total'] += 1
        else:
            ident, state, last_result = row
            if state == 2:
                prior = json.loads(last_result)['status']
                stats['done'] -= 1
                stats['outcomes'][prior] -= 1
                db.execute('update tasks set latest_seq=?,state=0,last_result=null,next=0 where id=?', (sequence, ident))
            else:
                db.execute('update tasks set latest_seq=? where id=?', (sequence, ident))
        stats['cursor'] = sequence


def consume(db, stats, node, msg):
    row = db.execute('select state,node,attempt,latest_seq,leased_seq from tasks where id=?', (msg['id'],)).fetchone()
    if not row or row[:3] != (1, node, msg['attempt']):
        return False
    if msg['event'] == 'retry':
        delay = min(300, 2 ** min(msg['attempt'], 8))
        db.execute('update tasks set state=3,node=null,next=?,updated=?,last_result=? where id=?',
                   (time.time()+delay, time.time(), encoded({'status': 'retry', 'detail': msg['detail'][:1000]}), msg['id']))
        stats['retries'] += 1
    elif msg['event'] == 'done':
        result = msg['result']
        result['node'] = node
        result['finished_at'] = time.time()
        receipt = result.get('receipt', {})
        if receipt.get('status') == 'copied':
            stats['copied_operations'] += 1
            stats['copied_bytes'] += receipt.get('bytes', 0)
            ns = stats['nodes'].setdefault(node, {})
            ns['copied_operations'] = ns.get('copied_operations', 0) + 1
            ns['copied_bytes'] = ns.get('copied_bytes', 0) + receipt.get('bytes', 0)
        if row[3] > row[4]:
            db.execute('update tasks set state=0,node=null,next=0,updated=?,last_result=? where id=?',
                       (time.time(), encoded(result), msg['id']))
        else:
            db.execute('update tasks set state=2,node=null,updated=?,last_result=? where id=?',
                       (time.time(), encoded(result), msg['id']))
            stats['done'] += 1
            key = result['status']
            stats['outcomes'][key] = stats['outcomes'].get(key, 0) + 1
            ns = stats['nodes'].setdefault(node, {})
            ns['done'] = ns.get('done', 0) + 1
    else:
        raise ValueError('invalid event')
    return True


def worker(run, work, node):
    os.environ['PROJSET_SSH_TOPOLOGY'] = str(run / 'recovery/ssh.json')
    recovery = module('repair_recovery', run / 'recovery/code/projset_recovery_copy.py')
    cfg = json.loads((run / 'config.json').read_text())
    recovery.validate(run, cfg, node)
    manifest = json.loads((work / 'manifest.json').read_text())
    if hashlib.sha256((work / 'code/projset_audit_repair.py').read_bytes()).hexdigest() != manifest['code_sha256']:
        raise ValueError('repair code changed')
    recovery.engine.configure_directory_opener(recovery.native.open_parent_readonly)
    configure_no_replace(recovery.engine)
    mutex = threading.Lock()
    def send(value):
        with mutex:
            print(encoded(value), flush=True)
    def execute(task):
        try:
            result = repair_one(recovery, cfg, task['rel'])
            send({'event': 'done', 'id': task['id'], 'attempt': task['attempt'], 'result': result})
        except Exception as error:
            send({'event': 'retry', 'id': task['id'], 'attempt': task['attempt'],
                  'detail': type(error).__name__ + ': ' + str(error)})
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)
    send({'event': 'ready', 'node': node, 'host': socket.gethostname(), 'pid': os.getpid()})
    try:
        for line in sys.stdin:
            msg = json.loads(line)
            if msg['event'] == 'task':
                pool.submit(execute, msg['task'])
            elif msg['event'] == 'stop':
                break
    finally:
        pool.shutdown(wait=False)
    return 0


def export(db, work):
    tmp = work / ('repairs.' + uuid.uuid4().hex + '.tmp')
    fields = ['relative_path', 'source_path', 'target_path', 'status', 'node',
              'logical_TB', 'sha256', 'detail', 'result_json']
    with tmp.open('w', encoding='utf-8', errors='backslashreplace', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for rel, state, raw in db.execute('select rel,state,last_result from tasks order by id'):
            path = json.loads(rel)
            result = json.loads(raw) if raw else {'status': 'pending'}
            receipt = result.get('receipt', {})
            writer.writerow({'relative_path': path, 'source_path': SOURCE + '/' + path,
                'target_path': TARGET + '/' + path, 'status': result['status'],
                'node': result.get('node', ''), 'logical_TB': result.get('logical_bytes', 0)/1e12,
                'sha256': receipt.get('sha256', ''), 'detail': result.get('detail', receipt.get('detail', '')),
                'result_json': encoded(result)})
        stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, work / 'repairs.csv')


def controller(run, work, audit):
    os.environ['PROJSET_SSH_TOPOLOGY'] = str(run / 'recovery/ssh.json')
    recovery = module('repair_recovery_controller', run / 'recovery/code/projset_recovery_copy.py')
    topology = json.loads((run / 'recovery/ssh.json').read_text())
    nodes = list(topology['hosts'])
    with (work / 'controller.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            if json.loads((work / 'status.json').read_text()).get('state') == 'finished':
                return 0
        except (OSError, ValueError):
            pass
        db = database(work / 'repair.sqlite3')
        stats = json.loads(db.execute('select value from meta where key="stats"').fetchone()[0])
        # A short read-lock timeout keeps the repair heartbeat and existing
        # tasks alive when the audit controller briefly holds an NFS lock.
        source = sqlite3.connect('file:' + str(audit / 'audit.sqlite3') + '?mode=ro', uri=True, timeout=2)
        source.execute('pragma query_only=ON')
        events = queue.Queue(maxsize=512)
        clients, active, retry_at = {}, {}, {}
        last_ingest = 0
        tick = 0
        source_empty = False
        outbox_busy_retries = 0
        outbox_last_busy_at = None

        def reader(node, generation, proc):
            try:
                for line in proc.stdout:
                    events.put((node, generation, json.loads(line)))
            except Exception as error:
                events.put((node, generation, {'event': 'reader_error', 'detail': repr(error)}))
            finally:
                events.put((node, generation, {'event': 'eof'}))

        def send(node, msg):
            try:
                clients[node]['process'].stdin.write(encoded(msg) + '\n')
                clients[node]['process'].stdin.flush()
                return True
            except (OSError, ValueError, BrokenPipeError):
                return False

        def launch(node):
            db.execute('begin immediate')
            for ident, owner in list(active.items()):
                if owner == node:
                    db.execute('update tasks set state=0,node=null where id=? and state=1', (ident,))
                    active.pop(ident, None)
            db.commit()
            generation = uuid.uuid4().hex
            command = ('python3 -S -B ' + shlex.quote(str(work / 'code/projset_audit_repair.py')) +
                       ' worker --run ' + shlex.quote(str(run)) + ' --work ' + shlex.quote(str(work)) +
                       ' --node ' + shlex.quote(node))
            with (work / 'logs' / (node + '.log')).open('ab') as log:
                proc = subprocess.Popen(recovery.ssh(node, command), stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)
            clients[node] = {'process': proc, 'generation': generation, 'ready': False}
            threading.Thread(target=reader, args=(node, generation, proc), daemon=True).start()
            retry_at[node] = time.time() + 15

        def snapshot(state, scan_state):
            waiting = db.execute('select count(*) from tasks where state=3').fetchone()[0]
            out = dict(state=state, updated=time.time(), scan_state=scan_state,
                controller_host=socket.gethostname(), controller_pid=os.getpid(),
                nodes_total=len(nodes), nodes_ready=sum(c['ready'] for c in clients.values()),
                total=stats['total'], done=stats['done'], active=len(active), retry=waiting,
                pending=stats['total']-stats['done']-len(active)-waiting,
                outbox_cursor=stats['cursor'], outcomes=stats['outcomes'], retries=stats['retries'],
                outbox_busy_retries=outbox_busy_retries, outbox_last_busy_at=outbox_last_busy_at,
                copied_operations=stats['copied_operations'], copied_bytes=stats['copied_bytes'],
                nodes=stats['nodes'])
            atomic(work / 'status.json', out)
            return out

        try:
            while True:
                for node in nodes:
                    client = clients.get(node)
                    if (client is None or client['process'].poll() is not None) and time.time() >= retry_at.get(node, 0):
                        launch(node)
                if time.time() - last_ingest >= 2:
                    # Fetch a small bounded batch and release the audit DB read
                    # lock before writing this controller's separate ledger.
                    rows = read_outbox(source, stats['cursor'])
                    source_empty = rows == []
                    if rows is None:
                        outbox_busy_retries += 1
                        outbox_last_busy_at = time.time()
                    if rows:
                        db.execute('begin immediate')
                        ingest(db, stats, rows)
                        db.execute('update meta set value=? where key="stats"', (encoded(stats),))
                        db.commit()
                    last_ingest = time.time()
                batch = []
                try:
                    batch.append(events.get(timeout=.2))
                    while len(batch) < 512:
                        batch.append(events.get_nowait())
                except queue.Empty:
                    pass
                if batch:
                    db.execute('begin immediate')
                    for node, generation, msg in batch:
                        client = clients.get(node)
                        if not client or client['generation'] != generation:
                            continue
                        event = msg['event']
                        if event == 'ready':
                            client['ready'] = True
                        elif event in ('done', 'retry'):
                            if consume(db, stats, node, msg):
                                active.pop(msg['id'], None)
                        elif event in ('eof', 'reader_error'):
                            client['ready'] = False
                            for ident, owner in list(active.items()):
                                if owner == node:
                                    db.execute('update tasks set state=0,node=null where id=? and state=1', (ident,))
                                    active.pop(ident, None)
                    db.execute('update meta set value=? where key="stats"', (encoded(stats),))
                    db.commit()
                assignments = []
                if not (work / 'STOP').exists():
                    db.execute('begin immediate')
                    for node in nodes:
                        client = clients.get(node)
                        if not client or not client['ready']:
                            continue
                        for _ in range(THREADS - sum(owner == node for owner in active.values())):
                            row = db.execute('select id,rel,attempt,latest_seq from tasks where state=3 and next<=? order by next,id limit 1', (time.time(),)).fetchone()
                            if row is None:
                                row = db.execute('select id,rel,attempt,latest_seq from tasks where state=0 order by next,id limit 1').fetchone()
                            if row is None:
                                break
                            ident, rel, attempt, sequence = row
                            attempt += 1
                            db.execute('update tasks set state=1,node=?,attempt=?,leased_seq=?,updated=? where id=?',
                                       (node, attempt, sequence, time.time(), ident))
                            active[ident] = node
                            assignments.append((node, {'event': 'task', 'task':
                                {'id': ident, 'attempt': attempt, 'rel': json.loads(rel)}}))
                    db.commit()
                    for node, task in assignments:
                        if not send(node, task):
                            clients[node]['process'].terminate()
                try:
                    scan = json.loads((audit / 'status.json').read_text())['state']
                except (OSError, ValueError, KeyError):
                    scan = 'unknown'
                if time.time() - tick >= 10:
                    snapshot('running', scan)
                    tick = time.time()
                if scan == 'finished' and source_empty and stats['done'] == stats['total'] and not active:
                    # The scan may have appended after our last empty poll but
                    # before publishing finished. Check the immutable final
                    # outbox once more before declaring repair complete.
                    final_rows = read_outbox(source, stats['cursor'])
                    if final_rows is None:
                        outbox_busy_retries += 1
                        outbox_last_busy_at = time.time()
                        source_empty = False
                        continue
                    if final_rows:
                        db.execute('begin immediate')
                        ingest(db, stats, final_rows)
                        db.execute('update meta set value=? where key="stats"', (encoded(stats),))
                        db.commit()
                        source_empty = False
                        continue
                    snapshot('exporting', scan)
                    export(db, work)
                    snapshot('finished', scan)
                    break
                if (work / 'STOP').exists() and not active:
                    snapshot('stopped', scan)
                    break
        finally:
            for node, client in clients.items():
                if client['ready']:
                    send(node, {'event': 'stop'})
                try:
                    client['process'].stdin.close()
                except (OSError, ValueError):
                    pass
            source.close(); db.close()
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('install-outbox', 'controller', 'worker', 'export'))
    parser.add_argument('--run', type=Path, default=RUN)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--audit', type=Path)
    parser.add_argument('--node')
    args = parser.parse_args()
    if args.mode == 'install-outbox':
        if args.audit is None:
            parser.error('--audit is required')
        print(encoded({'initial_events': install_outbox(args.audit / 'audit.sqlite3')}))
    elif args.mode == 'controller':
        if args.audit is None:
            parser.error('--audit is required')
        return controller(args.run.resolve(), args.work.resolve(), args.audit.resolve())
    elif args.mode == 'worker':
        return worker(args.run.resolve(), args.work.resolve(), args.node)
    else:
        db = sqlite3.connect('file:' + str(args.work.resolve() / 'repair.sqlite3') + '?mode=ro', uri=True, timeout=60)
        try:
            export(db, args.work.resolve())
        finally:
            db.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
