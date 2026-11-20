#!/usr/bin/env python3
"""Take and validate a local rollback snapshot while the audit is fenced."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import time

STAGE = Path(__file__).resolve().parent
WORK = STAGE.parent
RUN = WORK.parent.parent
FENCE = STAGE.name
LOCAL = Path('/var/tmp') / ('projset-audit-rollback-' + STAGE.name)
RATE = 16 * 1024 * 1024


def validate_node(topology, config):
    expected = config['nodes']['cpu-worker-0']
    if (socket.gethostname() != topology['hosts']['cpu-worker-0'] or
            os.stat('/share-mixed/projset').st_ino != expected['source_ino'] or
            os.stat('/share-mixed/project').st_ino != expected['target_ino']):
        raise RuntimeError('node or mount identity changed')


def atomic(name, value):
    path = STAGE / name
    temp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    with temp.open('x') as out:
        json.dump(value, out, sort_keys=True, separators=(',', ':'))
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)


def main():
    topology = json.loads((RUN / 'recovery/ssh.json').read_text())
    config = json.loads((RUN / 'config.json').read_text())
    validate_node(topology, config)
    if (WORK / 'STOP').read_text().strip() != FENCE:
        raise RuntimeError('upgrade fence is not present on the controller node')
    with (WORK / 'controller.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        LOCAL.mkdir(mode=0o700)
        target_path = LOCAL / 'audit.sqlite3'
        source = sqlite3.connect(str(WORK / 'audit.sqlite3'), timeout=60)
        dest = None
        start = time.monotonic()
        last_report = [0.0]
        try:
            # Opening read-write allows SQLite to recover a hot rollback
            # journal if the previous controller was interrupted mid-commit.
            source.execute('pragma cache_size=-8192')
            source_stats = source.execute('select value from meta where key="stats"').fetchone()[0]
            page_size = source.execute('pragma page_size').fetchone()[0]
            page_count = source.execute('pragma page_count').fetchone()[0]
            free = os.statvfs(LOCAL).f_bavail * os.statvfs(LOCAL).f_frsize
            if free < page_size * page_count * 2 + 1024**3:
                raise RuntimeError('insufficient local space for rollback snapshot')
            dest = sqlite3.connect(str(target_path), timeout=30)
            dest.execute('pragma cache_size=-8192')

            def progress(status, remaining, total):
                copied = max(0, total - remaining) * page_size
                delay = copied / RATE - (time.monotonic() - start)
                if delay > 0:
                    time.sleep(delay)
                if time.monotonic() - last_report[0] >= 5:
                    atomic('snapshot-state.json', {'state': 'copying', 'at': time.time(),
                           'pages_remaining': remaining, 'total_pages': total,
                           'page_size': page_size, 'local': str(target_path)})
                    last_report[0] = time.monotonic()

            atomic('snapshot-state.json', {'state': 'copying', 'at': time.time(),
                   'pages_remaining': page_count, 'total_pages': page_count,
                   'page_size': page_size, 'local': str(target_path)})
            source.backup(dest, pages=1024, progress=progress, sleep=.1)
            dest.commit()
            atomic('snapshot-state.json', {'state': 'checking', 'at': time.time(),
                   'local': str(target_path)})
            check = dest.execute('pragma quick_check(1)').fetchall()
            if check != [('ok',)]:
                raise RuntimeError('snapshot quick_check failed')
            if dest.execute('select value from meta where key="stats"').fetchone()[0] != source_stats:
                raise RuntimeError('snapshot statistics differ from the source')
            states = dict(dest.execute('select state,count(*) from dirs group by state').fetchall())
            stats = json.loads(source_stats)
            if sum(states.values()) != stats['discovered_dirs'] or states.get(2, 0) != stats['done_dirs']:
                raise RuntimeError('snapshot directory counts disagree with durable statistics')
            source_outbox = source.execute('select count(*),coalesce(max(id),0) from repair_outbox').fetchone()
            target_outbox = dest.execute('select count(*),coalesce(max(id),0) from repair_outbox').fetchone()
            if source_outbox != target_outbox:
                raise RuntimeError('snapshot repair outbox differs from source')
            repair = json.loads((WORK / 'repair/status.json').read_text())
            if repair.get('outbox_cursor', 0) > source_outbox[1]:
                raise RuntimeError('repair cursor is ahead of snapshot')
            dest.close()
            dest = None
            descriptor = os.open(target_path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            digest = hashlib.sha256()
            with target_path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b''):
                    digest.update(chunk)
            before = json.loads((STAGE / 'audit-before.json').read_text())
            before_counts = before['counts']
            for key in ('files', 'symlinks', 'file_bytes', 'entries'):
                if stats['counts'].get(key, 0) < before_counts.get(key, 0):
                    raise RuntimeError('snapshot counters regressed from preflight')
            if stats['done_dirs'] < before['done_dirs']:
                raise RuntimeError('snapshot completed directories regressed')
            result = {'state': 'ready', 'at': time.time(), 'host': socket.gethostname(),
                      'local': str(target_path), 'size': target_path.stat().st_size,
                      'sha256': digest.hexdigest(), 'quick_check': check[0][0],
                      'directory_states': states, 'outbox_count': source_outbox[0],
                      'outbox_max': source_outbox[1], 'repair_cursor': repair.get('outbox_cursor'),
                      'stats': stats, 'elapsed_seconds': time.monotonic() - start}
            atomic('snapshot-ready.json', result)
            atomic('snapshot-state.json', {'state': 'ready', 'at': time.time(), 'local': str(target_path)})
            print(json.dumps({'state': 'ready', 'local': str(target_path),
                              'size': result['size'], 'sha256': result['sha256'],
                              'elapsed_seconds': result['elapsed_seconds']}), flush=True)
        finally:
            if dest is not None:
                dest.close()
            source.close()


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        atomic('snapshot-state.json', {'state': 'failed', 'at': time.time(),
               'error_type': type(error).__name__, 'detail': str(error)[:300]})
        raise
