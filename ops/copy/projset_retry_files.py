#!/usr/bin/env python3
"""Retry a bounded manifest with the frozen, no-replace migration engine.

This independent process never edits the main controller's journal or counters.
Its durable receipts make successful files resumable after a process restart.
"""
import argparse
import collections
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid


def atomic(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('w') as stream:
        json.dump(value, stream, ensure_ascii=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def read_manifest(path, expected_sha256):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('retry manifest changed')
    data = json.loads(raw)
    entries = data['entries']
    names = [x['rel'] for x in entries]
    if len(names) != len(set(names)) or any(
        not name or name.startswith('/') or any(part in ('', '.', '..')
                                                for part in name.split('/'))
        for name in names
    ):
        raise ValueError('invalid or duplicate manifest path')
    if any(x['kind'] != 'file' or x['size'] < 0 for x in entries):
        raise ValueError('manifest must contain regular files only')
    return entries


def latest_receipts(path):
    """Discard only an incomplete trailing line after an interrupted write."""
    latest = {}
    if not Path(path).exists():
        return latest
    with open(path, 'r+b') as stream:
        for line in stream:
            if not line.endswith(b'\n'):
                stream.seek(-len(line), os.SEEK_CUR)
                stream.truncate()
                stream.flush()
                os.fsync(stream.fileno())
                break
            row = json.loads(line)
            latest[row['rel']] = row
    return latest


def load_recovery(run):
    path = run / 'recovery/code/projset_recovery_copy.py'
    spec = importlib.util.spec_from_file_location('frozen_projset_recovery', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_worker(run, node, manifest, sha256, output, rate, recovery=None):
    entries = read_manifest(manifest, sha256)
    recovery = recovery or load_recovery(run)
    cfg = recovery.read(run / 'config.json')
    recovery.validate(run, cfg, node)
    recovery.engine.configure_directory_opener(recovery.native.open_parent_readonly)
    recovery.engine.configure_rate_limit(rate)
    receipt_path = output / 'receipts.jsonl'
    prior = latest_receipts(receipt_path)
    counts = collections.Counter()
    counts['verified_bytes'] = 0
    state = {'state': 'running', 'node': node, 'pid': os.getpid(),
             'total_files': len(entries), 'current_rel': None, 'counts': counts,
             'rate_limit_bytes_per_second': rate}
    stop = threading.Event()

    def report():
        snapshot = dict(state, counts=dict(counts), updated=time.time())
        atomic(output / 'status.json', snapshot)

    def heartbeat():
        while not stop.wait(30):
            report()

    thread = threading.Thread(target=heartbeat, daemon=True)
    report()
    thread.start()
    try:
        with receipt_path.open('a') as receipts:
            for entry in entries:
                rel = entry['rel']
                state['current_rel'] = rel
                old = prior.get(rel)
                if old and old.get('status') in ('copied', 'skipped') and recovery.resume_valid(entry, old):
                    counts[old['status']] += 1
                    counts['verified_bytes'] += entry['size']
                    continue
                # Reuse the bounded ESTALE retry where the recovery version
                # supports it. Older frozen deployments retain their original
                # copy behavior and still record errors for later attention.
                copy_with_retry = getattr(recovery, 'copy_entry_with_estale_retry', None)
                if copy_with_retry is not None:
                    result = copy_with_retry(cfg['source'], cfg['target'], entry,
                                             cfg['run_id'] + '-retry')
                else:
                    result = recovery.engine.copy_entry(cfg['source'], cfg['target'], entry,
                                                        cfg['run_id'] + '-retry', verify=True)
                result.update(logical_bytes=entry['size'], finished=time.time(),
                              node=node, supplemental=True)
                receipts.write(json.dumps(result, ensure_ascii=True, separators=(',', ':')) + '\n')
                receipts.flush()
                os.fsync(receipts.fileno())
                counts[result['status']] += 1
                if result['status'] in ('copied', 'skipped'):
                    counts['verified_bytes'] += entry['size']
                report()
        state['state'] = 'finished' if sum(counts[k] for k in ('copied', 'skipped')) == len(entries) else 'finished_with_errors'
        state['current_rel'] = None
    except BaseException:
        state['state'] = 'crashed'
        raise
    finally:
        stop.set()
        thread.join()
        report()
    return 0


def supervise(args):
    with (args.output / 'supervisor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        command = [sys.executable, '-S', '-B', str(Path(__file__).resolve()),
                   '--run', str(args.run), '--node', args.node,
                   '--manifest', str(args.manifest), '--manifest-sha256', args.manifest_sha256,
                   '--output', str(args.output), '--rate', str(args.rate), '--mode', 'worker']
        for attempt in range(1, 11):
            with (args.output / 'worker.log').open('ab') as log:
                child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
                while child.poll() is None:
                    atomic(args.output / 'supervisor.json', dict(state='running', pid=os.getpid(),
                           child_pid=child.pid, attempt=attempt, updated=time.time()))
                    time.sleep(10)
            if child.returncode == 0:
                status = json.loads((args.output / 'status.json').read_text())
                atomic(args.output / 'supervisor.json', dict(state=status['state'], pid=os.getpid(),
                       returncode=0, attempt=attempt, updated=time.time()))
                return 0
            atomic(args.output / 'supervisor.json', dict(state='retrying', pid=os.getpid(),
                   returncode=child.returncode, attempt=attempt, updated=time.time()))
            time.sleep(30)
        atomic(args.output / 'supervisor.json', dict(state='failed', pid=os.getpid(),
               attempt=10, updated=time.time()))
        return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--node', required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rate', type=int, default=1024 * 1024)
    parser.add_argument('--mode', choices=('worker', 'supervisor'), default='supervisor')
    args = parser.parse_args()
    args.run = args.run.resolve()
    args.output = args.output.resolve()
    if not args.output.is_relative_to(args.run / 'recovery') or args.rate <= 0:
        parser.error('output must be under run/recovery and rate must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    read_manifest(args.manifest, args.manifest_sha256)
    if args.mode == 'worker':
        return run_worker(args.run, args.node, args.manifest, args.manifest_sha256,
                          args.output, args.rate)
    return supervise(args)


if __name__ == '__main__':
    raise SystemExit(main())
