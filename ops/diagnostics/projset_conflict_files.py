#!/usr/bin/env python3
"""Bounded read-only content comparison of selected historical file conflicts."""
import argparse
import collections
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import stat
import time
import uuid


def identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class Limits:
    def __init__(self, bytes_per_second, pairs_per_second, max_bytes, max_file_bytes):
        if any(not math.isfinite(value) or value <= 0 for value in
               (bytes_per_second, pairs_per_second, max_bytes, max_file_bytes)):
            raise ValueError('all limits must be positive')
        self.bytes_per_second = bytes_per_second
        self.pairs_per_second = pairs_per_second
        self.max_bytes = max_bytes
        self.max_file_bytes = max_file_bytes
        self.reserved = 0
        self.next_byte = self.next_pair = time.monotonic()

    def wait_pair(self):
        now = time.monotonic()
        self.next_pair = max(now, self.next_pair)
        if self.next_pair > now:
            time.sleep(self.next_pair - now)
        self.next_pair += 1 / self.pairs_per_second

    def reserve_pair(self, size):
        if size > self.max_file_bytes or self.reserved + 4 * size > self.max_bytes:
            return False
        self.reserved += 4 * size
        return True

    def consume(self, size):
        now = time.monotonic()
        self.next_byte = max(now, self.next_byte) + size / self.bytes_per_second
        if self.next_byte > now:
            time.sleep(self.next_byte - now)


def compare_file(native, source, target, rel, limits):
    """Compare stable opened regular files; never follow links or change data."""
    descriptors = []
    try:
        parent_a, name_a = native.open_parent_readonly(source, rel)
        descriptors.append(parent_a)
        fd_a = os.open(name_a, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_a)
        descriptors.append(fd_a)
        parent_b, name_b = native.open_parent_readonly(target, rel)
        descriptors.append(parent_b)
        fd_b = os.open(name_b, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_b)
        descriptors.append(fd_b)
        a, b = os.fstat(fd_a), os.fstat(fd_b)
        if not stat.S_ISREG(a.st_mode) or not stat.S_ISREG(b.st_mode):
            return {'result': 'different_or_unsupported_type'}
        out = {'source_size': a.st_size, 'target_size': b.st_size,
               'source_mtime_ns': a.st_mtime_ns, 'target_mtime_ns': b.st_mtime_ns}
        if a.st_size != b.st_size:
            out['result'] = 'different_size'
        elif not limits.reserve_pair(a.st_size):
            out['result'] = 'not_hashed_limit'
        else:
            def digest(fd, size):
                os.lseek(fd, 0, os.SEEK_SET)
                result = hashlib.sha256()
                remaining = size
                while remaining:
                    amount = min(65536, remaining)
                    limits.consume(amount)
                    block = os.read(fd, amount)
                    if not block:
                        raise ValueError('short read')
                    result.update(block)
                    remaining -= len(block)
                return result.hexdigest()
            a_hash, b_hash = digest(fd_a, a.st_size), digest(fd_b, b.st_size)
            a_repeat, b_repeat = digest(fd_a, a.st_size), digest(fd_b, b.st_size)
            if a_hash != a_repeat or b_hash != b_repeat:
                return {'result': 'changed_during_comparison'}
            out.update(result='same_sha256' if a_hash == b_hash else 'different_sha256',
                       source_sha256=a_hash, target_sha256=b_hash, hash_passes=2)
        after_a, after_b = os.fstat(fd_a), os.fstat(fd_b)
        path_a = os.stat(name_a, dir_fd=parent_a, follow_symlinks=False)
        path_b = os.stat(name_b, dir_fd=parent_b, follow_symlinks=False)
        if identity(a) != identity(after_a) or identity(a) != identity(path_a) or \
                identity(b) != identity(after_b) or identity(b) != identity(path_b):
            return {'result': 'changed_during_comparison'}
        return out
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def atomic(path, value):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, ensure_ascii=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def candidates(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        rows = csv.DictReader(stream)
        return [row for row in rows if row.get('kind') == 'file' and
                row.get('reason') in ('destination exists with different checksum',
                                      'destination exists with different size') and
                row.get('source_current_kind') == row.get('target_current_kind') == 'file' and
                (row.get('source_current_bytes') or '').isdigit() and
                row.get('source_current_bytes') == row.get('target_current_bytes')]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--node', required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--bytes-per-second', type=int, default=1048576)
    parser.add_argument('--pairs-per-second', type=float, default=20)
    parser.add_argument('--max-bytes', type=int, default=4 * 1024 * 1024)
    parser.add_argument('--max-file-bytes', type=int, default=1024 * 1024)
    args = parser.parse_args()
    if (args.bytes_per_second > 1048576 or args.pairs_per_second > 20 or
            args.max_bytes > 4 * 1024 * 1024 or args.max_file_bytes > 1024 * 1024):
        parser.error('requested limits exceed the bounded read-only inspection budget')
    limits = Limits(args.bytes_per_second, args.pairs_per_second, args.max_bytes, args.max_file_bytes)
    os.nice(10)
    run = args.run.resolve()
    spec = importlib.util.spec_from_file_location('frozen_recovery', run / 'recovery/code/projset_recovery_copy.py')
    recovery = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(recovery)
    cfg = recovery.read(run / 'config.json')
    recovery.validate(run, cfg, args.node)
    selected = candidates(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    progress = args.output.with_name(args.output.stem + '.progress.json')
    started, last = time.time(), 0
    rows = []
    for row in selected:
        limits.wait_pair()
        try:
            result = compare_file(recovery.native, cfg['source'], cfg['target'], row['relative_path'], limits)
        except (OSError, ValueError) as error:
            result = {'result': 'unavailable', 'error': type(error).__name__,
                      'errno': getattr(error, 'errno', None)}
        result.update(rel=row['relative_path'], historical_reason=row['reason'])
        rows.append(result)
        if time.time() - last >= 30:
            atomic(progress, {'state': 'running', 'pid': os.getpid(), 'updated': time.time(),
                              'completed': len(rows), 'total': len(selected),
                              'reserved_read_bytes': limits.reserved})
            last = time.time()
    summary = {'node': args.node, 'started': started, 'updated': time.time(),
               'input': str(args.input), 'selected': len(selected),
               'results': dict(collections.Counter(row['result'] for row in rows)),
               'reserved_read_bytes': limits.reserved, 'max_read_bytes': limits.max_bytes,
               'max_file_bytes': limits.max_file_bytes, 'bytes_per_second': limits.bytes_per_second,
               'pairs_per_second': limits.pairs_per_second,
               'scope': 'read-only two-pass hashes and identity checks; live files are not an atomic snapshot'}
    atomic(args.output, {'summary': summary, 'files': rows})
    atomic(progress, dict(summary, state='finished'))
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == '__main__':
    main()
