#!/usr/bin/env python3
"""Readonly P4 source-prefix/FIFO-frontier cache. NEVER starts a coordinator.

All expensive parsing/pickling occurs while the original producer keeps running.
The result is prepared data only, not permission or authority for a handover.
"""
import argparse
from contextlib import closing
import gc
import hashlib
import json
import os
from pathlib import Path
import pickle
import sqlite3
import stat
import tempfile
import time
import urllib.parse

from migration_io import read_json, write_json
from migration_priority4_frontier import Priority4Prefix, FifoFrontier, observe_source_frames
from migration_scan_boundary import require_identity
from migration_warm_scan_checkpoint import read_prefix


class ObservedPrefix(Priority4Prefix):
    frontier = None

    def add(self, value):
        super().add(value)
        if self.frontier is not None:
            self.frontier.entry(value)


def digest_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory.resolve()
    expected = read_json(directory / 'intent.json')
    control, producer = Path(expected['control']), expected['producer']
    if (directory.parent != control or not directory.name.startswith('source-preparation-')
            or control.parent != Path('/share-mixed/project') or control.is_symlink()
            or producer['argv'] != ['/usr/bin/python3', '-u', str(control / 'migration_priority4_run.py'),
                                    '--control', str(control)]):
        raise ValueError('bound independent P4 original producer required')
    config = read_json(control / 'config.json')
    if ((config['source'], config['destination'], config['migration_scope'], config['exclude']) !=
            ('/share/project', '/share-mixed/project', 'project_priority4', ['eai_pwm'])):
        raise ValueError('P4 source/destination/exclusion binding differs')
    root_manifest = control / 'source-sha256.json'
    if hashlib.sha256(root_manifest.read_bytes()).hexdigest() != expected['runtime_manifest_sha256']:
        raise ValueError('original runtime manifest binding changed')
    for name, checksum in read_json(root_manifest).items():
        path = control / name
        if path.parent != control or path.is_symlink() or digest_file(path) != checksum:
            raise ValueError('original runtime source changed')
    manifest = Path(expected['manifest'])
    info = manifest.lstat()
    if (manifest.parent != control / 'manifests' or not manifest.name.startswith('p4-r1-')
            or not stat.S_ISREG(info.st_mode) or not 0 < info.st_size < 64 * 1024**3):
        raise ValueError('bound regular P4 manifest required')
    mem = {k: int(v) for k, v in (line.split() for line in Path('/sys/fs/cgroup/memory.stat').read_text().splitlines())}
    maximum = Path('/sys/fs/cgroup/memory.max').read_text().strip()
    if maximum != 'max' and int(maximum) - mem['anon'] < 2 * info.st_size + 8 * 1024**3:
        raise RuntimeError('insufficient anonymous-memory headroom for readonly preparation')
    # This bounded, primitive-only graph is acyclic. Repeated generation-2
    # traversals of a growing multi-million-path set serve no useful purpose.
    # This is a new readonly preparer, not a runtime change to the live producer.
    gc_was_enabled = gc.isenabled()
    gc.disable()
    prefix, checksum = ObservedPrefix(), hashlib.sha256()
    report_path = directory / 'progress.json'
    last_report = 0
    result = dict(status='parsing_prefix', started=time.time(), control=str(control),
                  manifest=str(manifest), producer=producer, adoption_authorized=False,
                  process_signals_sent=0, copy_threads_changed=0)

    def validate():
        require_identity(producer)
        state = read_json(control / 'state.json')
        if (state.get('status') != 'scanning' or state.get('stage') != 'p4-r1'
                or state.get('active_manifest') != str(manifest) or (control / 'STOP').exists()):
            raise RuntimeError('P4 phase or original producer changed; preparation cannot authorize adoption')
        current = manifest.stat()
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise RuntimeError('P4 manifest inode changed')
        return state

    def report(**updates):
        nonlocal last_report
        now = time.monotonic()
        if now - last_report >= 2:
            validate()
            result.update(updates, updated=time.time())
            write_json(report_path, result)
            last_report = now

    try:
        validate()
        with open(manifest, 'rb', buffering=8 * 1024**2) as stream:
            while True:
                limit = manifest.stat().st_size
                offset = read_prefix(stream, limit, prefix, checksum, report=report)
                report(offset=offset, records=prefix.records, paths=len(prefix.paths))
                if manifest.stat().st_size - offset < 1024**2:
                    break
            frames = observe_source_frames(producer, config['source'])
            broker_path = Path('/tmp/' + config['run_id'] + '-broker.sqlite')
            broker_info = broker_path.stat()
            with closing(sqlite3.connect(broker_path.as_uri() + '?mode=ro', uri=True, timeout=3)) as reader:
                completed = reader.execute("SELECT COUNT(*) FROM claims WHERE stage='p4-r1' AND status='done'").fetchone()[0]
            frontier = FifoFrontier(prefix, frames, completed_tasks=completed, capture_offset=offset)
            prefix.frontier = frontier
            write_json(directory / 'frame-observation.json', frames)
            result['status'] = 'following_fifo_boundary'
            deadline = time.monotonic() + 600
            while True:
                state = validate()
                observed_size = manifest.stat().st_size
                frontier.observe_flush(state['next_task_id'], observed_size)
                offset = read_prefix(stream, observed_size, prefix, checksum, report=report)
                proof = frontier.progress(offset, prefix.records)
                report(offset=offset, records=prefix.records, paths=len(prefix.paths), fifo=proof)
                if proof['status'] == 'prepared_fifo_frontier_only':
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError('FIFO boundary not proven within bounded preparation; original producer untouched')
                time.sleep(.5)
            validate()
            # Freeze the large stable containers before creating pickle's
            # memo, so normal cycle GC cannot repeatedly traverse the prefix.
            gc.collect()
            gc.freeze()
            private = Path(tempfile.mkdtemp(prefix=config['run_id'] + '-source-prefix-'))
            cache = private / 'prefix.pickle'
            values = prefix.containers()
            values['completed_candidates'] = frontier.candidates
            with open(cache, 'xb', buffering=8 * 1024**2) as output:
                os.chmod(cache, 0o600)
                pickle.dump(values, output, protocol=pickle.HIGHEST_PROTOCOL)
                output.flush()
                os.fsync(output.fileno())
            meta = dict(scope='priority4-threaded-fifo-source-prefix-v1', control=str(control),
                run_id=config['run_id'], job=config['job'], producer=producer,
                manifest=str(manifest), manifest_dev=info.st_dev, manifest_ino=info.st_ino,
                offset=offset, prefix_sha256=checksum.hexdigest(), records=prefix.records,
                paths=len(prefix.paths), directories=len(prefix.directories), stats=prefix.scanner.stats,
                root_identity=prefix.root_identity, cache=str(cache), cache_sha256=digest_file(cache),
                cache_bytes=cache.stat().st_size, fifo=proof, frames=str(directory / 'frame-observation.json'),
                candidate_directories=len(frontier.candidates), broker_path=str(broker_path),
                broker_dev=broker_info.st_dev, broker_ino=broker_info.st_ino,
                runtime_manifest_sha256=expected['runtime_manifest_sha256'],
                adoption_authorized=False, process_signals_sent=0,
                status='prepared_prefix_and_fifo_only', finished=time.time())
            write_json(directory / 'preparation.json', meta)
            result.update(status=meta['status'], finished=time.time(), preparation=str(directory / 'preparation.json'))
            write_json(report_path, result)
            if gc_was_enabled:
                gc.enable()
            print(json.dumps({k: v for k, v in meta.items() if k not in ('producer', 'stats')}), flush=True)
    except BaseException as exc:
        if gc_was_enabled:
            gc.enable()
        result.update(status='preparation_failed_original_untouched', error=repr(exc), finished=time.time())
        write_json(report_path, result)
        raise


if __name__ == '__main__':
    main()
