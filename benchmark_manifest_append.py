#!/usr/bin/env python3
"""Synthetic files only: compare append/seek and buffering on this NFS mount."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

from migration_io import write_json


def run_case(directory, index, append, buffering, records=8192):
    path = directory / ('synthetic-' + str(index) + '.jsonl')
    prefix = b'{"historical_prefix":true}\n'
    with open(path, 'xb') as stream:
        stream.write(prefix)
    line = json.dumps(dict(rel='synthetic-only/' + 'x' * 180, kind='file', size=106496,
        mtime_ns=1792820000000000000, ctime_ns=1792820000000000000, mode=33188,
        ino=1234, dev=52, uid=0, gid=0, nlink=1, blocks=208)) + '\n'
    began, cpu = time.monotonic(), time.process_time()
    with open(path, 'a' if append else 'r+', buffering=buffering, encoding='utf-8') as stream:
        if not append:
            stream.seek(0, os.SEEK_END)
        flags = fcntl.fcntl(stream.fileno(), fcntl.F_GETFL)
        for number in range(records):
            stream.write(line)
            if (number + 1) % 256 == 0:
                stream.flush()
        stream.flush()
        os.fsync(stream.fileno())
    result = dict(append=append, buffering=buffering, flags=flags,
        seconds=time.monotonic() - began, cpu=time.process_time() - cpu,
        records=records, bytes=records * len(line.encode()), path=str(path))
    with open(path, 'rb') as stream:
        if stream.read(len(prefix)) != prefix:
            raise RuntimeError('synthetic historical prefix changed')
    if path.stat().st_size != len(prefix) + result['bytes']:
        raise RuntimeError('synthetic inventory length differs')
    result['records_per_second'] = records / result['seconds']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    bundle = parser.parse_args().bundle.resolve()
    if (not bundle.name.startswith('fast-p4-source-') or
            bundle.parent.parent != Path('/share-mixed/project')):
        raise ValueError('explicit P4 candidate bundle required')
    directory = bundle / ('synthetic-append-benchmark-' + uuid.uuid4().hex)
    directory.mkdir(mode=0o700)
    cases = [(True, 8192), (False, 8192), (False, 1024**2), (True, 1024**2)]
    result = dict(directory=str(directory), source_files_changed=0, runtime_changes=0, samples=[])
    for index, (append, buffering) in enumerate(cases + list(reversed(cases))):
        value = run_case(directory, index, append, buffering)
        result['samples'].append(value)
        write_json(directory / 'result.json', result)
        print(json.dumps(value), flush=True)


if __name__ == '__main__':
    main()
