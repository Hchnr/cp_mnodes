#!/usr/bin/env python3
"""Prepare the next warm handover from an existing primitive checkpoint.

The live producer continues supplying tasks throughout this operation.  Old
assignment JSON and the old manifest prefix are never parsed again.  The
primitive cache is loaded once, the current manifest is copied byte-for-byte to
a private bounded snapshot, and only its uncached tail is parsed.

Candidate spools are immutable inputs and are reused in full.  A previous
``consumed`` counter is not a safe seek position: some of those candidates may
still have an in-flight probe.  The next producer's complete assignment delta
suppresses already-reserved paths before offering them again.  This may repeat
a read-only target check, but never justifies repeating a reserved copy.
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
from migration_prepared_resume import digest_file, load_checkpoint
from migration_scan import _Scanner


BLOCK = 8 * 1024 * 1024


def complete_prefix(handle, size):
    """Return a bounded complete-line prefix without scanning prior records."""
    position = size
    while position:
        start = max(0, position - BLOCK)
        handle.seek(start)
        block = handle.read(position - start)
        newline = block.rfind(b'\n')
        if newline >= 0:
            return start + newline + 1
        position = start
    return 0


def copy_prefix(source, destination, size, old_offset, old_sha, report):
    """Copy once, checking the old cache-bound prefix in the same I/O pass."""
    if size < old_offset:
        raise RuntimeError('active manifest is shorter than its cached prefix')
    whole, prefix = hashlib.sha256(), hashlib.sha256()
    source.seek(0)
    offset = 0
    with open(destination, 'xb') as out:
        while offset < size:
            chunk = source.read(min(BLOCK, size - offset))
            if not chunk:
                raise RuntimeError('active manifest was truncated while snapshotting')
            if offset < old_offset:
                prefix.update(chunk[:min(len(chunk), old_offset-offset)])
            out.write(chunk)
            whole.update(chunk)
            offset += len(chunk)
            report(phase='copy_manifest', manifest_bytes=offset, manifest_total=size)
        out.flush()
        os.fsync(out.fileno())
    if prefix.hexdigest() != old_sha:
        raise RuntimeError('active manifest no longer begins with the cached prefix')
    return whole.hexdigest()


def count_lines(path):
    with open(path, 'rb') as handle:
        return sum(block.count(b'\n') for block in iter(lambda: handle.read(BLOCK), b''))


def prepare_next(control, directory):
    control, directory = Path(control).resolve(), Path(directory).resolve()
    if not directory.is_relative_to(control) or directory == control:
        raise ValueError('private staging directory must be below the control directory')
    cfg, state = read_json(control/'config.json'), read_json(control/'state.json')
    if (state.get('recovery_warm') is not True or state.get('stage') != 'p3-r3'
            or state.get('scan_complete') or (control/'STOP').exists()):
        raise RuntimeError('preparation requires an active, incomplete warm producer')
    handover_path = state['foreground_handover']
    handover = read_json(handover_path)
    if not handover.get('prepared_resume'):
        raise RuntimeError('the running producer has no reusable primitive checkpoint')
    directory.mkdir(mode=0o700)
    scratch = Path('/tmp')/(cfg['run_id']+'-'+directory.name)
    scratch.mkdir(mode=0o700)
    started, last_report = time.time(), [0.0]
    progress = {'status':'preparing', 'started':started, 'control':str(control),
                'source_handover':handover_path, 'scratch':str(scratch)}

    def report(**values):
        progress.update(values)
        if time.monotonic()-last_report[0] >= 5 or values.get('status') == 'ready':
            write_json(directory/'preparation.json', dict(progress, updated=time.time()))
            print(json.dumps(progress), flush=True)
            last_report[0] = time.monotonic()

    report(phase='load_primitive_cache')
    previous, values = load_checkpoint(handover, control)
    source = Path(state['active_manifest']).resolve()
    if source != Path(previous['manifest']).resolve():
        raise RuntimeError('active manifest is not the previous prepared append manifest')
    if (not source.is_relative_to(control) or source.is_symlink()
            or previous['base_ledger'] != handover['assigned_tasks_path']
            or previous['base_ledger_sha256'] != handover['ledger_sha256']):
        raise RuntimeError('active manifest or immutable assignment binding changed')

    # Keep the historical assignment lower bound.  IDs can be published out of
    # order, so the eventual frozen reconciliation MUST enumerate all claims
    # and pending tasks >= this bound, rather than use a live MAX(id) watermark.
    # The old cache intentionally keeps only its existing base assignments.
    old_offset = previous['manifest_offset']
    manifest = directory/'prepared-manifest.jsonl'
    with open(source, 'rb') as handle:
        info = os.fstat(handle.fileno())
        offset = complete_prefix(handle, info.st_size)
        manifest_sha = copy_prefix(handle, manifest, offset, old_offset,
                                   previous['manifest_sha256'], report)

    scanner = _Scanner(None, None, 1024**3, 64, values['hardlinks'], None, 3)
    scanner.stats = values['stats']
    seeded, assigned = values['seeded_paths'], values['assigned']
    source_dev = os.stat(cfg['source']).st_dev
    new_spool = scratch/'prepared-tail-orphans.jsonl'
    tail_records, tail_orphans = 0, 0
    report(phase='parse_uncached_tail', cached_manifest_bytes=old_offset)
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        with open(manifest, 'rb') as src, open(new_spool, 'xb') as spool:
            src.seek(old_offset)
            for line in src:
                entry = json.loads(line)
                tail_records += 1
                rel = entry['rel']
                if rel in seeded:
                    continue
                if entry.get('kind') != 'error':
                    seeded.add(rel)
                _seed(scanner, entry, source_dev, source_dev)
                if (entry.get('kind') in ('file', 'symlink')
                        and not entry.get('hardlink_to') and rel not in assigned):
                    spool.write(line)
                    tail_orphans += 1
                if tail_records % 32768 == 0:
                    report(phase='parse_uncached_tail', tail_records=tail_records,
                           seeded_paths=len(seeded), tail_orphans=tail_orphans)
            spool.flush()
            os.fsync(spool.fileno())
        # The running producer created this immutable spool when it appended
        # its predecessor's post-checkpoint tail.  Its current in-flight offset
        # cannot be skipped safely.  New runtime tail files live in NEW scratch
        # below, avoiding collision with its existing tail-orphans.jsonl.
        spools = list(previous['spools'])
        previous_tail = Path(state.get('recovery_replay', {}).get('tail_spool')
                             or Path(previous['cache']).parent/'tail-orphans.jsonl')
        prior_tail_count = 0
        if str(previous_tail) not in spools:
            if not previous_tail.is_file():
                raise RuntimeError('running producer predecessor tail spool is missing')
            spools.append(str(previous_tail))
            prior_tail_count = count_lines(previous_tail)
        spools.append(str(new_spool))
        cache = scratch/'paths.pickle'
        report(phase='save_primitive_cache', seeded_paths=len(seeded),
               assigned_paths=len(assigned), tail_records=tail_records)
        with open(cache, 'xb') as handle:
            pickle.dump(values, handle, protocol=5)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if was_enabled:
            gc.enable()
    result = dict(previous, source_handover=handover_path,
                  source_manifest=str(source), source_manifest_dev=info.st_dev,
                  source_manifest_ino=info.st_ino, manifest_offset=offset,
                  manifest_sha256=manifest_sha, manifest=str(manifest),
                  cache=str(cache), cache_sha256=digest_file(cache), spools=spools,
                  orphans=previous['orphans']+prior_tail_count+tail_orphans,
                  seeded_paths=len(seeded), assigned_paths=len(assigned),
                  prepared_at=time.time(), preparation_kind='reuse-primitive-cache',
                  parent_prepared_resume=handover['prepared_resume'],
                  parsed_tail_records=tail_records)
    current = read_json(control/'state.json')
    if (current.get('foreground_handover') != handover_path
            or current.get('active_manifest') != str(source)):
        raise RuntimeError('producer changed during preparation; keep artifacts unbound')
    write_json(directory/'prepared.json', result)
    report(status='ready', elapsed_seconds=time.time()-started,
           parsed_tail_records=tail_records, manifest_bytes=offset)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--directory', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    prepare_next(args.control, args.directory)
