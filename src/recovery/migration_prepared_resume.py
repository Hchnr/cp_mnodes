"""Prepare a warm-recovery checkpoint while the current producer keeps running.

This helper only reads the live producer's immutable assignment prefix and a
bounded prefix of its append-only manifest. Its outputs are private staging
artifacts, not queue entries or authoritative migration state.
"""
import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import pickle
import time

from migration_io import read_json, write_json
from migration_parallel_scan import _seed
from migration_scan import _Scanner


def digest_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


class ContainerUnpickler(pickle.Unpickler):
    """Our checkpoint contains only primitive containers, never Python code."""
    def find_class(self, module, name):
        raise ValueError('checkpoint globals are forbidden')


def load_checkpoint(handover, control):
    descriptor = Path(handover['prepared_resume']).resolve()
    control = Path(control).resolve()
    if not descriptor.is_relative_to(control):
        raise ValueError('checkpoint descriptor must remain in control directory')
    meta = read_json(descriptor)
    if (meta.get('version') != 1 or meta.get('control') != str(control)
            or meta.get('source_handover') != handover.get('predecessor_handover')
            or meta.get('base_ledger') != handover.get('assigned_tasks_path')
            or meta.get('base_ledger_sha256') != handover.get('ledger_sha256')):
        raise ValueError('checkpoint does not match this handover')
    cache = Path(meta['cache'])
    if (not cache.is_absolute() or cache.is_symlink() or cache.stat().st_uid != os.getuid()
            or cache.stat().st_mode & 0o077 or digest_file(cache) != meta['cache_sha256']):
        raise ValueError('checkpoint ownership, mode or digest mismatch')
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        with open(cache, 'rb') as handle:
            values = ContainerUnpickler(handle).load()
        if (type(values) is not dict or type(values.get('assigned')) is not set
                or type(values.get('seeded_paths')) is not set
                or len(values['assigned']) != meta['assigned_paths']
                or len(values['seeded_paths']) != meta['seeded_paths']):
            raise ValueError('checkpoint container/count mismatch')
        # Permanent historical sets have no cycles to reclaim. Exclude them
        # from future generation-2 traversals while retaining normal GC for
        # the small, short-lived futures/HTTP objects allocated afterwards.
        gc.freeze()
    finally:
        if was_enabled:
            gc.enable()
    return meta, values


def prepare(control, directory):
    control, directory = Path(control).resolve(), Path(directory).resolve()
    cfg = read_json(control / 'config.json')
    state = read_json(control / 'state.json')
    if (state.get('recovery_warm') is not True or state.get('stage') != 'p3-r3'
            or state.get('recovery_replay', {}).get('phase') != 'fresh_scan'):
        raise RuntimeError('preparation requires an active warm fresh scan')
    handover = read_json(state['foreground_handover'])
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    scratch = Path('/tmp') / (cfg['run_id'] + '-' + directory.name)
    scratch.mkdir(mode=0o700)
    started = time.time()
    last = [0]
    progress = {'status': 'preparing', 'started': started, 'scratch': str(scratch),
                'control': str(control), 'handover': state['foreground_handover']}

    def report(**values):
        progress.update(values)
        if time.monotonic() - last[0] >= 5 or values.get('status') == 'ready':
            write_json(directory / 'preparation.json', dict(progress, updated=time.time()))
            print(json.dumps(progress), flush=True)
            last[0] = time.monotonic()

    # These large immutable path sets must never be walked by cyclic GC in
    # the steady-state producer. Preparation is short-lived and acyclic.
    gc.disable()
    assigned = set()
    ledger_hash = hashlib.sha256()
    count, maximum = 0, -1
    task_ids = set()
    with open(handover['assigned_tasks_path'], 'rb') as handle:
        for line in handle:
            ledger_hash.update(line)
            row = json.loads(line)
            task = row.get('task', row)
            if (not line.endswith(b'\n') or type(task['id']) is not int
                    or task['id'] < 0 or task['id'] in task_ids):
                raise RuntimeError('assignment prefix contains an invalid or repeated ID')
            task_ids.add(task['id'])
            assigned.update(e['rel'] for e in task['entries'])
            maximum = max(maximum, task['id'])
            count += 1
            if count % 4096 == 0:
                report(phase='assignments', tasks=count, assigned_paths=len(assigned))
    if (ledger_hash.hexdigest() != handover['ledger_sha256']
            or count != handover['next_task_id'] or maximum != count-1):
        raise RuntimeError('assignment prefix SHA/count changed')
    seeded = set()
    scanner = _Scanner(None, None, 1024**3, 64, {}, None, 3)
    source_dev = os.stat(cfg['source']).st_dev
    original_dev = None
    source = Path(state['active_manifest'])
    manifest = directory / 'prepared-manifest.jsonl'
    spool = scratch / 'orphans.jsonl'
    big_spool = scratch / 'large-orphans.jsonl'
    manifest_hash = hashlib.sha256()
    offset, orphan_count, big_count = 0, 0, 0
    with open(source, 'rb') as src, open(manifest, 'xb') as out, open(spool, 'xb') as small, open(big_spool, 'xb') as big:
        info = os.fstat(src.fileno())
        while src.tell() < info.st_size:
            line = src.readline()
            if not line.endswith(b'\n') or src.tell() > info.st_size:
                break
            entry = json.loads(line)
            rel = entry['rel']
            out.write(line)
            manifest_hash.update(line)
            offset += len(line)
            # The active combined manifest is already path-deduplicated.
            if rel not in seeded:
                if entry.get('kind') != 'error':
                    seeded.add(rel)
                if entry.get('kind') == 'directory' and rel == '':
                    original_dev = entry.get('dev')
                _seed(scanner, entry, source_dev, original_dev)
                if (entry.get('kind') in ('file', 'symlink')
                        and not entry.get('hardlink_to') and rel not in assigned):
                    large = entry.get('kind') == 'file' and entry.get('size', 0) >= 64 * 1024**2
                    (big if large else small).write(line)
                    orphan_count += 1
                    big_count += int(large)
            if len(seeded) % 32768 == 0:
                report(phase='manifest', manifest_bytes=offset, manifest_total=info.st_size,
                       seeded_paths=len(seeded), orphans=orphan_count, large_orphans=big_count)
        for handle in (out, small, big):
            handle.flush()
            os.fsync(handle.fileno())
    cache = scratch / 'paths.pickle'
    report(phase='saving_cache', seeded_paths=len(seeded), assigned_paths=len(assigned))
    with open(cache, 'xb') as out:
        pickle.dump({'assigned': assigned, 'seeded_paths': seeded,
                     'hardlinks': scanner.hardlinks, 'stats': scanner.stats}, out, protocol=5)
        out.flush()
        os.fsync(out.fileno())
    result = {'version': 1, 'control': str(control), 'run_id': cfg['run_id'],
              'source_handover': state['foreground_handover'],
              'source_manifest': str(source), 'source_manifest_dev': info.st_dev,
              'source_manifest_ino': info.st_ino, 'manifest_offset': offset,
              'manifest_sha256': manifest_hash.hexdigest(), 'manifest': str(manifest),
              'base_ledger': handover['assigned_tasks_path'], 'base_ledger_sha256': handover['ledger_sha256'],
              'base_next_task_id': count, 'maximum_id': maximum,
              'cache': str(cache), 'cache_sha256': digest_file(cache),
              'spools': [str(big_spool), str(spool)], 'orphans': orphan_count,
              'large_orphans': big_count, 'seeded_paths': len(seeded),
              'assigned_paths': len(assigned), 'prepared_at': time.time()}
    write_json(directory / 'prepared.json', result)
    report(status='ready', elapsed_seconds=time.time()-started)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--directory', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    prepare(args.control, args.directory)
