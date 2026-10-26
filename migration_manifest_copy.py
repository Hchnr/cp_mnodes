#!/usr/bin/env python3
"""Isolated regular-file copier whose ONLY input is a frozen audited manifest.

No directory enumeration, old queue, broker, training guard or overwrite mode.
Immutable, disjoint node shards are balanced by bytes plus per-file overhead.
Each node holds a local flock and is bound to an exact cluster hostname. Shared
append-only receipts are the recovery authority; local files are locks only.
The existing conservative SHA256/readback/fsync/link-no-replace engine is reused.
"""
import argparse
import collections
import concurrent.futures as futures
import datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid

import migration_worker as engine
import migration_linux_open as native
from migration_parent_frontier import FrontierParentOpener

FILES = ('migration_manifest_copy.py', 'migration_worker.py',
         'migration_linux_open.py', 'migration_parent_frontier.py')
EXCLUDED = 'eai_pwm/datasets/raw'
NODES = ['master-0'] + ['worker-' + str(i) for i in range(11)]


def read_json(path):
    path = Path(path)
    for attempt in range(4):
        try:
            with open(path) as stream:
                return json.load(stream)
        except FileNotFoundError:
            if attempt == 3:
                raise
            os.listdir(path.parent)
            time.sleep(.05)


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.part')
    with open(temp, 'x') as stream:
        json.dump(value, stream, ensure_ascii=True, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def digest(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_row(row):
    rel = row['rel']
    engine._components(rel)
    if rel == EXCLUDED or rel.startswith(EXCLUDED + '/'):
        raise ValueError('excluded raw subtree in manifest')
    if row.get('kind') != 'file' or row.get('reason') != 'target_absent':
        raise ValueError('only audited absent regular files are eligible')
    source = row['source']
    for key in ('size', 'ino', 'mode', 'uid', 'gid', 'mtime_ns', 'ctime_ns'):
        if type(source[key]) is not int:
            raise ValueError('invalid source stat: ' + key)
    if source['size'] < 0 or not stat.S_ISREG(source['mode']):
        raise ValueError('not an ordinary file')
    return dict(source, rel=rel, kind='file')


def prepare(control, report, cluster, threads=64):
    """Stream once, prove ordered uniqueness, and freeze scope/code before start."""
    control, report = Path(control), Path(report)
    final = read_json(report)
    item = final['outputs']['files']
    if (final['status'] != 'complete' or final['source'] != '/share/project'
            or final['target'] != '/share-mixed/project'
            or final['exclusions'] != [EXCLUDED]):
        raise ValueError('unexpected audited scope')
    manifest = Path(item['path'])
    if digest(manifest) != item['sha256']:
        raise ValueError('audited manifest SHA256 differs')
    control.mkdir(parents=True, exist_ok=False)
    (control / 'shards').mkdir()
    (control / 'code').mkdir()
    handles = {node: gzip.open(control / 'shards' / (node + '.jsonl.gz'),
                               'wt', encoding='ascii', compresslevel=1)
               for node in NODES}
    counters = {node: collections.Counter(files=0, bytes=0, cost=0) for node in NODES}
    total, logical, previous, block, block_bytes = 0, 0, None, [], 0

    def emit():
        nonlocal block, block_bytes
        if not block:
            return
        node = min(NODES, key=lambda name: counters[name]['cost'])
        for row in block:
            handles[node].write(json.dumps(row, ensure_ascii=True,
                                           separators=(',', ':')) + '\n')
            counters[node]['files'] += 1
            counters[node]['bytes'] += row['entry']['size']
            counters[node]['cost'] += row['entry']['size'] + 262144
        block, block_bytes = [], 0

    try:
        with gzip.open(manifest, 'rt', encoding='ascii') as stream:
            for line in stream:
                entry = validate_row(json.loads(line))
                raw = os.fsencode(entry['rel'])
                if previous is not None and raw <= previous:
                    raise ValueError('manifest paths are not strictly ordered/unique')
                previous = raw
                block.append(dict(id=total, entry=entry))
                total += 1
                logical += entry['size']
                block_bytes += entry['size']
                if len(block) >= 128 or block_bytes >= 128 * 1024**2:
                    emit()
                if total % 500000 == 0:
                    print(json.dumps(dict(prepared_files=total, bytes=logical)), flush=True)
        emit()
    finally:
        for handle in handles.values():
            handle.close()
    expected = final['missing']['file']
    if (total != expected['paths'] or total != item['rows']
            or logical != expected['logical_bytes'] or digest(manifest) != item['sha256']):
        raise ValueError('audited manifest totals differ')
    shards = {}
    for node in NODES:
        path = control / 'shards' / (node + '.jsonl.gz')
        with open(path, 'rb') as stream:
            os.fsync(stream.fileno())
        shards[node] = dict(counters[node], path=str(path), sha256=digest(path))
    bundle = {}
    for name in FILES:
        source, target = Path(__file__).parent / name, control / 'code' / name
        shutil.copyfile(source, target)
        with open(target, 'rb') as stream:
            os.fsync(stream.fileno())
        bundle[name] = digest(target)
    cfg = dict(version=1, run_id=control.name, created=time.time(),
               source=final['source'], target=final['target'], excluded=[EXCLUDED],
               manifest=str(manifest), manifest_sha256=item['sha256'],
               files=total, bytes=logical, cluster=cluster, threads=threads,
               source_root_ino=os.stat(final['source']).st_ino,
               target_root_ino=os.stat(final['target']).st_ino,
               shards=shards, code=bundle,
               publication='SHA256+destination-readback+fsync+link-no-replace',
               changed_source_policy='record-and-do-not-copy',
               resume='durable shared receipts, then revalidate inode identity; file-level')
    atomic(control / 'config.json', cfg)
    print(json.dumps(dict(control=str(control), status='prepared', files=total,
                          bytes=logical, shards=shards)), flush=True)
    return cfg


def same_identity(current, snapshot):
    return all(getattr(current, 'st_' + key) == snapshot[key]
               for key in ('ino', 'size', 'mtime_ns', 'ctime_ns'))


def resume_valid(cfg, entry, receipt):
    if receipt.get('status') not in ('copied', 'skipped') or receipt.get('rel') != entry['rel']:
        return False
    try:
        for root, expected in ((cfg['source'], entry), (cfg['target'], receipt['dst_stat'])):
            parent, name = native.open_parent_readonly(root, entry['rel'])
            try:
                current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if not stat.S_ISREG(current.st_mode) or not same_identity(current, expected):
                    return False
            finally:
                os.close(parent)
        return True
    except (OSError, KeyError):
        return False


def load_receipts(folder):
    latest = {}
    for path in sorted(Path(folder).glob('receipts-*.jsonl')):
        with open(path, 'rb') as stream:
            for line in stream:
                if not line.endswith(b'\n'):
                    # A crash may tear only the last buffered line. New attempts
                    # use a new file; immutable older evidence is never edited.
                    continue
                row = json.loads(line)
                old = latest.get(row['id'])
                if old is None or row['finished'] > old['finished']:
                    latest[row['id']] = row
    return latest


def install_engine(cfg):
    engine.configure_rate_limit(0)
    engine.configure_directory_opener(native.open_parent_readonly)
    original = engine._open_parent
    engine._open_parent = FrontierParentOpener(
        cfg['target'], original, check=engine._check_cancel,
        opener=native.open_parent_readonly, anchor_opener=native.open_parent_readonly)


def copy_one(cfg, task, prior=None):
    entry = task['entry']
    if prior is not None and resume_valid(cfg, entry, prior):
        return dict(prior, resumed=True, finished=time.time())
    result = None
    for attempt in range(3):
        result = engine.copy_entry(cfg['source'], cfg['target'], entry,
                                   cfg['run_id'], verify=True)
        if result['status'] != 'error':
            break
        if attempt < 2:
            time.sleep(.2 * (attempt + 1))
    return dict(result, id=task['id'], expected_bytes=entry['size'],
                finished=time.time(), attempts=attempt + 1)


def validate_worker(control, cfg, node):
    if socket.gethostname() != 'job-' + cfg['cluster'] + '-' + node:
        raise ValueError('wrong cluster/hostname; refusing data writes')
    if cfg['source'] != '/share/project' or cfg['target'] != '/share-mixed/project' or cfg['excluded'] != [EXCLUDED]:
        raise ValueError('unexpected immutable scope')
    if (os.stat(cfg['source']).st_ino != cfg['source_root_ino']
            or os.stat(cfg['target']).st_ino != cfg['target_root_ino']
            or not os.path.ismount(cfg['source']) or not os.path.ismount(cfg['target'])):
        raise ValueError('source/target mount identity differs')
    for name, expected in cfg['code'].items():
        if name not in FILES or digest(control / 'code' / name) != expected:
            raise ValueError('frozen copy code changed: ' + name)
    for module in (engine, native, sys.modules[FrontierParentOpener.__module__]):
        if Path(module.__file__).resolve().parent != (control / 'code').resolve():
            raise ValueError('foreign engine import')
    shard = cfg['shards'][node]
    if digest(shard['path']) != shard['sha256']:
        raise ValueError('fixed node manifest changed')
    audit_name = Path(cfg['manifest']).parents[1].name
    local_audit = Path('/tmp') / ('cp-mnodes-' + audit_name.removeprefix('.hcr-'))
    previous = read_json(local_audit / 'launch.json')
    for expected in previous['old_task_still_paused']:
        try:
            fields = Path('/proc', str(expected['pid']), 'stat').read_text().rsplit(') ', 1)[1].split()
        except FileNotFoundError:
            continue
        if fields[19] == expected['start'] and fields[0] not in ('T', 't', 'Z', 'X'):
            raise ValueError('an original unrestricted copy actor is no longer paused')


def worker(control, node, limit=None, threads=None):
    control = Path(control).resolve()
    cfg = read_json(control / 'config.json')
    validate_worker(control, cfg, node)
    lockpath = Path('/tmp') / ('cp-mnodes-fixed-' + cfg['run_id'] + '-' + node + '.lock')
    lock = open(lockpath, 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    folder = control / 'nodes' / node
    folder.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, lambda *_: stop.set())
    install_engine(cfg)
    current_threads = cfg['threads'] if threads is None else threads
    if not 1 <= current_threads <= 256:
        raise ValueError('invalid threads')
    prior = load_receipts(folder)
    attempt = str(time.time_ns()) + '-' + uuid.uuid4().hex[:8]
    receipts = open(folder / ('receipts-' + attempt + '.jsonl'), 'x', buffering=262144)
    counts, samples, io_lock = collections.Counter(), collections.deque(), threading.Lock()
    written = [0]

    def progress(delta):
        with io_lock:
            written[0] += delta

    engine.configure_progress_callback(progress)
    started = time.time()
    state = dict(node=node, host=socket.gethostname(), pid=os.getpid(),
                 start=Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()[19],
                 run_id=cfg['run_id'], status='copying', started=started,
                 threads=current_threads, shard_files=cfg['shards'][node]['files'],
                 shard_bytes=cfg['shards'][node]['bytes'], limit=limit, attempt=attempt)
    atomic(folder / 'launch.json', dict(state, argv=sys.argv))
    last_publish, pending, seen, size, last_stop_check = 0, {}, 0, 0, 0

    def publish(status='copying', force=False):
        nonlocal last_publish
        now = time.time()
        if not force and now - last_publish < 3:
            return
        receipts.flush()
        os.fsync(receipts.fileno())
        with io_lock:
            value = written[0]
        samples.append((now, value, counts['completed_bytes']))
        while len(samples) > 2 and samples[1][0] < now - 60:
            samples.popleft()
        elapsed = max(.001, now - samples[0][0])
        snapshot = dict(state, status=status, updated=now, counts=dict(counts),
                        active=len(pending), submitted=seen, payload_written=value,
                        write_bytes_per_second=(value - samples[0][1]) / elapsed,
                        complete_bytes_per_second=(counts['completed_bytes'] - samples[0][2]) / elapsed,
                        sample_seconds=now - samples[0][0])
        atomic(folder / 'progress.json', snapshot)
        last_publish = now

    def record(task, result):
        if result['rel'] != task['entry']['rel'] or result['id'] != task['id']:
            raise ValueError('receipt identity differs')
        receipts.write(json.dumps(result, ensure_ascii=True, separators=(',', ':')) + '\n')
        counts['processed'] += 1
        counts[result['status']] += 1
        if result['status'] in ('copied', 'skipped'):
            counts['completed_bytes'] += task['entry']['size']
            counts[result['status'] + '_bytes'] += task['entry']['size']
        else:
            counts['attention_bytes'] += task['entry']['size']
        if result.get('resumed'):
            counts['resumed'] += 1
        if result.get('metadata_warnings'):
            counts['metadata_warnings'] += 1

    publish(force=True)
    try:
        with gzip.open(cfg['shards'][node]['path'], 'rt', encoding='ascii') as stream, \
                futures.ThreadPoolExecutor(max_workers=current_threads) as executor:
            exhausted = False
            while pending or not exhausted:
                if not stop.is_set() and time.monotonic() - last_stop_check >= 3:
                    last_stop_check = time.monotonic()
                    try:
                        with open(control / 'stop.json') as stop_file:
                            json.load(stop_file)
                    except FileNotFoundError:
                        # Control-only NFS cache refresh, never dataset checks.
                        os.listdir(control)
                    else:
                        stop.set()
                while not stop.is_set() and not exhausted and len(pending) < current_threads:
                    if limit is not None and seen >= limit:
                        exhausted = True
                        break
                    line = stream.readline()
                    if not line:
                        exhausted = True
                        break
                    task = json.loads(line)
                    entry = task['entry']
                    validate_row(dict(rel=entry['rel'], kind='file', source=entry, reason='target_absent'))
                    future = executor.submit(copy_one, cfg, task, prior.get(task['id']))
                    pending[future] = task
                    seen += 1
                    size += entry['size']
                if stop.is_set():
                    exhausted = True
                if pending:
                    done, _ = futures.wait(pending, timeout=1, return_when=futures.FIRST_COMPLETED)
                    for future in done:
                        task = pending.pop(future)
                        record(task, future.result())
                publish(status='draining' if stop.is_set() else 'copying')
        if limit is None and not stop.is_set():
            if seen != cfg['shards'][node]['files'] or size != cfg['shards'][node]['bytes']:
                raise ValueError('node manifest totals differ')
        final = ('stopped' if stop.is_set() else 'canary_complete' if limit is not None else
                 'complete_with_attention' if counts['changed'] + counts['conflict'] + counts['error'] else 'complete')
        publish(final, force=True)
    except BaseException as exc:
        state['failure'] = type(exc).__name__ + ': ' + str(exc)
        publish('failed', force=True)
        raise
    finally:
        receipts.close()
        engine.configure_progress_callback(None)
        lock.close()
    print(json.dumps(read_json(folder / 'progress.json')), flush=True)


def launch(control, node, limit=None, threads=None):
    control = Path(control).resolve()
    cfg = read_json(control / 'config.json')
    if socket.gethostname() != 'job-' + cfg['cluster'] + '-' + node:
        raise ValueError('launch host differs')
    folder = control / 'nodes' / node
    folder.mkdir(parents=True, exist_ok=True)
    command = ['/usr/bin/python3', '-B', '-u', str(control / 'code' / FILES[0]),
               'worker', '--control', str(control), '--node', node]
    if limit is not None:
        command += ['--limit', str(limit)]
    if threads is not None:
        command += ['--threads', str(threads)]
    log = open(folder / ('worker-' + str(time.time_ns()) + '.log'), 'ab')
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log,
                               stderr=subprocess.STDOUT, start_new_session=True,
                               env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
    log.close()
    result = dict(node=node, pid=process.pid, argv=command, at=time.time())
    atomic(folder / ('spawn-' + str(process.pid) + '.json'), result)
    print(json.dumps(result), flush=True)


def monitor(control):
    control = Path(control)
    cfg = read_json(control / 'config.json')
    counts, rows, speed, active = collections.Counter(), [], 0., 0
    for node in NODES:
        try:
            row = read_json(control / 'nodes' / node / 'progress.json')
        except FileNotFoundError:
            rows.append((node, None))
            continue
        counts.update(row['counts'])
        age = time.time() - row['updated']
        if age < 20 and row['status'] in ('copying', 'draining'):
            speed += row['write_bytes_per_second']
            active += row['active']
        rows.append((node, row))
    ok = counts['copied'] + counts['skipped']
    remaining = cfg['files'] - ok - counts['changed'] - counts['conflict'] - counts['error']
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
    print('\n固定清单补拷 | ' + now.strftime('%m-%d %H:%M:%S 北京时间'))
    print('范围 {:,} 文件 / {:.3f} TiB；成功 {:,} ({:.2%}) / {:.3f} TiB'.format(
        cfg['files'], cfg['bytes']/1024**4, ok, ok/cfg['files'], counts['completed_bytes']/1024**4))
    print('新复制 {:,}；原有且校验一致 {:,}；待处理 {:,}；变化/冲突/错误 {}/{}/{}'.format(
        counts['copied'], counts['skipped'], remaining, counts['changed'], counts['conflict'], counts['error']))
    print('近60秒写入 {:.3f} GiB/s；活跃线程 {}/{}；raw 已排除'.format(speed/1024**3, active, cfg['threads']*len(NODES)))
    print('\n节点       | 活跃/线程 | 成功文件     | 成功 GiB | 写入 GiB/s | 心跳 | 状态')
    for node, row in rows:
        if row is None:
            print('{:<10} | 尚未启动'.format(node))
            continue
        c = row['counts']
        print('{:<10} | {:>9} | {:>12,} | {:>8.2f} | {:>10.3f} | {:>4.0f}s | {}'.format(
            node, str(row['active'])+'/'+str(row['threads']), c.get('copied',0)+c.get('skipped',0),
            c.get('completed_bytes',0)/1024**3, row['write_bytes_per_second']/1024**3,
            time.time()-row['updated'], row['status']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'launch', 'worker', 'status'))
    parser.add_argument('--control', type=Path, required=True)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--cluster')
    parser.add_argument('--node', choices=NODES)
    parser.add_argument('--threads', type=int)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.control, args.report, args.cluster, args.threads or 64)
    elif args.command == 'worker':
        worker(args.control, args.node, args.limit, args.threads)
    elif args.command == 'launch':
        launch(args.control, args.node, args.limit, args.threads)
    else:
        while True:
            monitor(args.control)
            if not args.watch:
                break
            time.sleep(5)


if __name__ == '__main__':
    main()
