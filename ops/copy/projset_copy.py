#!/usr/bin/env python3
"""Independent distributed, no-replace projset copier over existing SSH aliases.

Shared append-only task/completion journals precede dispatch/acknowledgement.
SQLite is ONLY a local scheduling cache; receipts live on the shared target.
No old migration queues, active pointers, signals, or training hooks are used.
"""
import argparse
import collections
import concurrent.futures as cf
import fcntl
import hashlib
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

CORE = Path(__file__).resolve().parents[2] / 'src/core'
if CORE.is_dir():
    sys.path.insert(0, str(CORE))
import migration_worker as engine
import migration_linux_open as native

SOURCE = '/share-mixed/projset'
TARGET = '/share-mixed/project'


def atomic(path, data):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with open(tmp, 'x') as f:
        json.dump(data, f, ensure_ascii=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read(path):
    return json.loads(Path(path).read_text())


def durable(stream):
    stream.flush()
    os.fsync(stream.fileno())


def append(stream, value):
    stream.write(json.dumps(value, ensure_ascii=True, separators=(',', ':')) + '\n')


def ssh(node, command):
    return ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=4', node, command]


def identity():
    mounts = Path('/proc/self/mountinfo').read_text().splitlines()
    result = dict(host=socket.gethostname(), boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    for key, root in [('source', SOURCE), ('target', TARGET)]:
        fd = native._open_absolute_directory(root)
        try:
            result[key + '_ino'] = os.fstat(fd).st_ino
        finally:
            os.close(fd)
        rows = [x for x in mounts if x.split()[4] == root]
        if len(rows) != 1:
            raise ValueError('missing mount: ' + root)
        fields = rows[0].split(' - ', 1)[1].split()
        result[key + '_mount'] = fields[:2]
        nested = [x for x in mounts if x.split()[4].startswith(root + '/')]
        for row in nested:
            sub = row.split(' - ', 1)[1].split()
            suffix = row.split()[4][len(root):]
            # Some CPU nodes bind the same target export's _admin directory.
            # Permit only an exact export/path correspondence, never another tree.
            if key != 'target' or sub[:2] != [fields[0], fields[1] + suffix]:
                raise ValueError('foreign nested mount: ' + row.split()[4])
        result[key + '_submounts'] = sorted(x.split()[4] for x in nested)
    if result['source_mount'] == result['target_mount']:
        raise ValueError('source and destination are the same mount')
    return result


def validate(run, cfg, node):
    expected = cfg['nodes'][node]
    actual = identity()
    if actual != expected:
        raise ValueError('node or mount identity changed')
    if cfg['source'] != SOURCE or cfg['target'] != TARGET:
        raise ValueError('unexpected scope')
    for name, digest in cfg['code'].items():
        if hashlib.sha256((run/'code'/name).read_bytes()).hexdigest() != digest:
            raise ValueError('frozen code changed: ' + name)
    for module in (engine, native):
        if Path(module.__file__).resolve().parent != run/'code':
            raise ValueError('engine is not the frozen copy')


def scan_directory(source, target, rel, emit, issue, expected_ino=None):
    """Emit bounded, disjoint children/file tasks; never follow directory links."""
    engine._components(rel, allow_empty=True)
    fd = native._open_absolute_directory(source) if not rel else native.open_parent_readonly(source, rel + '/_')[0]
    children, entries, size = [], [], 0
    try:
        before = os.fstat(fd)
        if expected_ino is not None and before.st_ino != expected_ino:
            raise engine.SourceChanged('source directory replaced since discovery')
        # Source enumeration remains useful even if this destination conflicts.
        try:
            engine.ensure_destination_directory(target, rel)
        except Exception as error:
            issue(dict(rel=rel, status='conflict', kind='directory', detail=str(error)))
        def flush():
            nonlocal children, entries, size
            if children or entries:
                emit(children, entries)
                children, entries, size = [], [], 0
        with os.scandir(fd) as iterator:
            for child in iterator:
                path = rel + '/' + child.name if rel else child.name
                try:
                    info = child.stat(follow_symlinks=False)
                    if info.st_dev != before.st_dev:
                        raise ValueError('nested filesystem: ' + path)
                    if stat.S_ISDIR(info.st_mode):
                        children.append(dict(rel=path, ino=info.st_ino))
                    elif stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                        entry = dict(engine._record(info), rel=path,
                                     kind='file' if stat.S_ISREG(info.st_mode) else 'symlink')
                        if entry['kind'] == 'symlink':
                            entry['target'] = os.readlink(child.name, dir_fd=fd)
                        entries.append(entry)
                        size += info.st_size
                    else:
                        issue(dict(rel=path, kind='special', status='unsupported', mode=info.st_mode))
                except Exception as error:
                    issue(dict(rel=path, status='error', detail=type(error).__name__ + ': ' + str(error)))
                if len(children) + len(entries) >= 128 or size >= 256 * 1024**2:
                    flush()
        flush()
        after = os.fstat(fd)
        if engine._identity(before) != engine._identity(after):
            issue(dict(rel=rel, kind='directory', status='changed', detail='directory changed during enumeration'))
        return engine._record(before)
    finally:
        os.close(fd)


def worker(run, node):
    cfg = read(run/'config.json')
    validate(run, cfg, node)
    lock = open('/tmp/' + cfg['run_id'] + '-' + node + '.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    folder = run/'nodes'/node
    folder.mkdir(parents=True, exist_ok=True)
    if (folder/'launch.json').exists():
        raise ValueError('worker already launched; inspect receipts before explicit recovery')
    state = dict(node=node, pid=os.getpid(), start=Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()[19],
                 **identity(), argv=sys.argv, started=time.time())
    atomic(folder/'launch.json', state)
    engine.configure_directory_opener(native.open_parent_readonly)
    outlock, reclock, countlock = threading.Lock(), threading.Lock(), threading.Lock()
    counts = collections.Counter()
    active, acknowledgements = {}, {}
    stop = threading.Event()
    receipts = open(folder/'receipts.jsonl', 'x', buffering=1048576)
    issues = open(folder/'issues.jsonl', 'x', buffering=65536)
    def send(value):
        with outlock:
            print(json.dumps(value, ensure_ascii=True, separators=(',', ':')), flush=True)
    def record(result):
        with reclock:
            append(receipts, result)
            if result['status'] not in ('copied', 'skipped') or result.get('metadata_warnings'):
                append(issues, result)
        with countlock:
            counts[result['status']] += 1
            counts['processed'] += 1
            if result['status'] in ('copied', 'skipped'):
                counts[result['status'] + '_bytes'] += result.get('logical_bytes', 0)
            if result.get('metadata_warnings'):
                counts['metadata_warnings'] += 1
            if result.get('nlink', 1) > 1:
                counts['hardlink_paths'] += 1
    def progress(delta):
        with countlock:
            counts['payload_written'] += delta
    engine.configure_progress_callback(progress)
    files = cf.ThreadPoolExecutor(max_workers=cfg['threads'])
    jobs = cf.ThreadPoolExecutor(max_workers=4)
    def execute(task):
        key = task['id']
        try:
            if task['type'] == 'scan':
                ack = threading.Event()
                acknowledgements[key] = ack
                def emit(children, entries):
                    ack.clear()
                    send(dict(kind='spawn', id=key, children=children, entries=entries))
                    while not ack.wait(1):
                        if stop.is_set():
                            raise RuntimeError('control connection ended during scan')
                info = scan_directory(cfg['source'], cfg['target'], task['rel'], emit, record, task.get('ino'))
                send(dict(kind='directory', id=key, rel=task['rel'], source=info))
            else:
                pending = set()
                def one(entry):
                    result = engine.copy_entry(cfg['source'], cfg['target'], entry, cfg['run_id'], verify=True)
                    record(dict(result, job=key, logical_bytes=entry['size'], nlink=entry['nlink'], finished=time.time()))
                for entry in task['entries']:
                    pending.add(files.submit(one, entry))
                    if len(pending) >= cfg['threads'] * 2:
                        done, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
                        for future in done:
                            future.result()
                for future in cf.as_completed(pending):
                    future.result()
        except Exception as error:
            record(dict(rel=task.get('rel'), job=key, status='error', detail=type(error).__name__ + ': ' + str(error)))
        finally:
            with reclock:
                durable(receipts)
                durable(issues)
            active.pop(key, None)
            acknowledgements.pop(key, None)
            with countlock:
                final_counts = dict(counts)
            send(dict(kind='done', id=key, counts=final_counts))
    def heartbeat():
        while not stop.wait(5):
            with reclock:
                durable(receipts)
                durable(issues)
            with countlock:
                snapshot = dict(state, counts=dict(counts), active=list(active), updated=time.time())
            atomic(folder/'progress.json', snapshot)
            send(dict(kind='progress', **snapshot))
    threading.Thread(target=heartbeat, daemon=True).start()
    send(dict(kind='ready', **state))
    try:
        for line in sys.stdin:
            msg = json.loads(line)
            if msg['kind'] == 'job':
                task = msg['task']
                active[task['id']] = task['type']
                jobs.submit(execute, task)
            elif msg['kind'] == 'ack':
                acknowledgements[msg['id']].set()
            elif msg['kind'] == 'finish':
                break
    finally:
        # EOF drains accepted copies, releases blocked enumerators, takes no new work.
        stop.set()
        jobs.shutdown(wait=True)
        files.shutdown(wait=True)
        with reclock:
            durable(receipts)
            durable(issues)
        with countlock:
            atomic(folder/'progress.json', dict(state, counts=dict(counts), active=[], updated=time.time(), state='exited'))


def controller(run):
    cfg = read(run/'config.json')
    lock = open('/tmp/' + cfg['run_id'] + '-controller.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    # A fresh controller never destroys a predecessor's queue or lease evidence.
    journal = open(run/'tasks.jsonl', 'x', buffering=1048576)
    completed = open(run/'completed.jsonl', 'x', buffering=65536)
    directories = open(run/'directories.jsonl', 'x', buffering=65536)
    dbpath = '/tmp/' + cfg['run_id'] + '.sqlite'
    db = sqlite3.connect(dbpath)
    db.execute('create table jobs(id integer primary key, type text, state text, payload text, node text)')
    db.execute('create index pending on jobs(state,type,id)')
    db.execute('create table scanned(rel text primary key)')
    atomic(run/'controller.json', dict(pid=os.getpid(), host=socket.gethostname(), local_cache=dbpath,
                                     started=time.time(), argv=sys.argv))
    uid = 0
    pending = collections.Counter()
    def issue(kind, **data):
        nonlocal uid
        if kind == 'scan':
            if db.execute('select 1 from scanned where rel=?', (data['rel'],)).fetchone():
                return
            db.execute('insert into scanned values(?)', (data['rel'],))
        uid += 1
        task = dict(id=uid, type=kind, **data)
        append(journal, task)
        db.execute('insert into jobs values(?,?,?,?,?)', (uid, kind, 'pending', json.dumps(task), None))
        pending[kind] += 1
    issue('scan', rel='')
    events = queue.Queue(maxsize=256)
    clients, leases, blocked = {}, {}, set()
    def reader(node, process):
        try:
            for line in process.stdout:
                events.put((node, json.loads(line)))
        except Exception as error:
            events.put((node, dict(kind='reader_error', detail=type(error).__name__)))
        finally:
            events.put((node, dict(kind='eof', rc=process.wait())))
    def send(node, msg):
        stream = clients[node]['p'].stdin
        try:
            stream.write(json.dumps(msg, ensure_ascii=True, separators=(',', ':')) + '\n')
            stream.flush()
        except (OSError, ValueError):
            clients[node]['alive'] = clients[node]['ready'] = False
            failures.append(dict(node=node, event='connection_lost', active=list(clients[node]['active'])))
    for node in cfg['nodes']:
        log = open(run/'logs'/(node+'.stderr.log'), 'x')
        command = 'python3 -S -B ' + shlex.quote(str(run/'code/projset_copy.py')) + ' worker --run ' + shlex.quote(str(run)) + ' --node ' + shlex.quote(node)
        p = subprocess.Popen(ssh(node, command), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                             text=True, bufsize=1)
        clients[node] = dict(p=p, ready=False, alive=True, active={}, last=time.time(), counts={})
        threading.Thread(target=reader, args=(node, p), daemon=True).start()
    tick, started, done, failures = 0, time.time(), 0, []
    history = collections.deque(maxlen=400)
    phase = 'copying'
    try:
        while True:
            try:
                node, msg = events.get(timeout=.1)
                client = clients[node]
                client['last'] = time.time()
                kind = msg['kind']
                if kind == 'ready':
                    if any(msg[k] != v for k, v in cfg['nodes'][node].items()):
                        raise ValueError('unexpected worker identity: ' + node)
                    client['ready'] = True
                elif kind == 'spawn':
                    if leases.get(msg['id']) != node:
                        raise ValueError('scan owner differs')
                    for child in msg['children']:
                        issue('scan', **child)
                    if msg['entries']:
                        issue('copy', entries=msg['entries'])
                    durable(journal)
                    db.commit()
                    # Bound pending file manifests even for giant flat directories.
                    if pending['copy'] < 8192:
                        send(node, dict(kind='ack', id=msg['id']))
                    else:
                        blocked.add((node, msg['id']))
                elif kind == 'directory':
                    append(directories, dict(msg, node=node))
                elif kind == 'done':
                    # Several job threads may finish together; retain monotonic totals.
                    for k, v in msg['counts'].items():
                        client['counts'][k] = max(client['counts'].get(k, 0), v)
                    if leases.pop(msg['id']) != node:
                        raise ValueError('completion owner differs')
                    client['active'].pop(msg['id'])
                    append(completed, dict(msg, node=node, time=time.time()))
                    durable(completed)
                    db.execute('delete from jobs where id=?', (msg['id'],))
                    db.commit()
                    done += 1
                elif kind == 'progress':
                    for k, v in msg['counts'].items():
                        client['counts'][k] = max(client['counts'].get(k, 0), v)
                elif kind in ('eof', 'reader_error'):
                    client['alive'] = client['ready'] = False
                    failures.append(dict(node=node, event=msg, active=list(client['active'])))
                    # Unknown in-flight copies are never reassigned to another actor.
            except queue.Empty:
                pass
            if pending['copy'] < 4096 and blocked:
                for node, key in list(blocked):
                    if clients[node]['alive']:
                        send(node, dict(kind='ack', id=key))
                    blocked.remove((node, key))
            stopping = (run/'STOP').exists()
            for node, client in clients.items():
                if not client['alive'] or not client['ready'] or stopping:
                    continue
                for kind, slots in [('copy', 3), ('scan', 1)]:
                    if kind == 'scan' and pending['copy'] >= 8192:
                        continue
                    available = slots - list(client['active'].values()).count(kind)
                    for _ in range(available):
                        row = db.execute('select id,payload from jobs where state="pending" and type=? order by id limit 1', (kind,)).fetchone()
                        if row is None:
                            break
                        key, payload = row
                        durable(journal)
                        db.execute('update jobs set state="leased",node=? where id=?', (node, key))
                        db.commit()
                        leases[key] = node
                        client['active'][key] = kind
                        pending[kind] -= 1
                        send(node, dict(kind='job', task=json.loads(payload)))
            now = time.time()
            if not leases and not sum(pending.values()) and events.empty():
                phase = 'copied_with_attention' if failures else 'copy_pass_finished'
            elif not any(c['alive'] for c in clients.values()):
                phase = 'workers_unavailable'
            elif stopping and not leases:
                phase = 'stopped'
            if now - tick >= 5 or phase != 'copying':
                counts = collections.Counter()
                for client in clients.values():
                    counts.update(client['counts'])
                history.append((now, counts['payload_written'], counts['copied_bytes'], counts['processed']))
                rates = {}
                for seconds in (60, 300):
                    old = next((v for v in history if v[0] >= now-seconds), history[0])
                    dt = now-old[0]
                    rates[str(seconds)] = dict(seconds=dt, write_Bps=(counts['payload_written']-old[1])/dt if dt>1 else 0,
                        copied_Bps=(counts['copied_bytes']-old[2])/dt if dt>1 else 0, files_ps=(counts['processed']-old[3])/dt if dt>1 else 0)
                if phase == 'copy_pass_finished' and any(counts[k] for k in ('error','conflict','changed','unsupported','metadata_warnings')):
                    phase = 'copied_with_attention'
                status = dict(state=phase, source=SOURCE, target=TARGET, started=started, updated=now,
                    controller_pid=os.getpid(), discovered_jobs=uid, finished_jobs=done, pending=dict(pending),
                    active=len(leases), nodes_ready=sum(c['ready'] for c in clients.values()), nodes_total=len(clients),
                    counts=dict(counts), rates=rates, failures=failures,
                    workers={n:dict(ready=c['ready'], alive=c['alive'], age=now-c['last'], active=c['active'], counts=c['counts']) for n,c in clients.items()},
                    scope_complete=False, note='Live source; this is a copy pass, not an atomic snapshot. Hardlink topology and directory metadata are not finalized.')
                atomic(run/'status.json', status)
                with open(run/'samples.jsonl', 'a') as f:
                    append(f, status)
                durable(journal)
                durable(directories)
                tick = now
            if phase != 'copying':
                break
    finally:
        for node, client in clients.items():
            if client['alive']:
                try:
                    send(node, dict(kind='finish'))
                    client['p'].stdin.close()
                except (OSError, ValueError):
                    pass
        durable(journal)
        durable(completed)
        durable(directories)
        db.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=['probe', 'controller', 'worker', 'status'])
    p.add_argument('--run', type=Path)
    p.add_argument('--node')
    args = p.parse_args()
    if args.mode == 'probe':
        print(json.dumps(identity()))
    elif args.mode == 'worker':
        worker(args.run.resolve(), args.node)
    elif args.mode == 'controller':
        controller(args.run.resolve())
    else:
        print(json.dumps(read(args.run/'status.json'), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
