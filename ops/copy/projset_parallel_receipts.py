#!/usr/bin/env python3
"""Rebuild historical receipt counters in parallel, preserving receipt order."""
import argparse
import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid


def save(path, value):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def load_copy_module():
    path = Path(__file__).with_name('projset_recovery_copy.py')
    spec = importlib.util.spec_from_file_location('projset_receipt_recovery_copy', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def shard(run, node, pending_path, output):
    """Write one complete shard; incomplete output is never accepted."""
    copy = load_copy_module()
    pending_bytes = pending_path.read_bytes()
    pending = set(json.loads(pending_bytes))
    original = json.loads((run/'recovery'/'original-counts.json').read_text())
    if node not in original:
        raise ValueError('unknown logical node')
    baseline = collections.Counter(original[node])
    folder = run/'nodes'/node
    receipt_paths = [folder/'receipts.jsonl'] + sorted(folder.glob('attempt-*/receipts.jsonl'))
    snapshots = {path: path.stat() for path in receipt_paths}
    total_bytes = sum(info.st_size for info in snapshots.values())
    digest = hashlib.sha256()
    count = prior_count = done_bytes = 0
    last_report = time.monotonic()
    tmp = output/(node+'.prior.tmp')
    final = output/(node+'.prior.jsonl')
    with tmp.open('xb') as prior:
        for path in receipt_paths:
            with path.open('rb') as source:
                for input_line in source:
                    done_bytes += len(input_line)
                    if not input_line.endswith(b'\n'):
                        break  # Match the serial replayer's torn-tail handling.
                    row = json.loads(input_line)
                    count += 1
                    if row.get('job') in pending and row.get('rel') is not None:
                        line = (json.dumps(row, ensure_ascii=True, separators=(',', ':'))+'\n').encode()
                        prior.write(line)
                        digest.update(line)
                        prior_count += 1
                    if path.parent != folder:
                        baseline[row['status']] += 1
                        baseline['processed'] += 1
                        if row['status'] in ('copied', 'skipped'):
                            baseline[row['status']+'_bytes'] += row.get('logical_bytes', 0)
                            baseline['payload_written'] += row.get('_payload_written', row.get('logical_bytes', 0) if row['status'] == 'copied' else 0)
                        if row.get('metadata_warnings'):
                            baseline['metadata_warnings'] += 1
                        if row.get('nlink', 1) > 1:
                            baseline['hardlink_paths'] += 1
                    if count % 4096 == 0 and time.monotonic()-last_report >= 25:
                        if copy.stop_requested(run):
                            raise RuntimeError('stop requested during receipt shard')
                        save(output/(node+'.progress.json'), {'node': node, 'records': count,
                            'done_bytes': done_bytes, 'total_bytes': total_bytes, 'updated': time.time()})
                        print(node, count, flush=True)
                        last_report = time.monotonic()
        if [folder/'receipts.jsonl'] + sorted(folder.glob('attempt-*/receipts.jsonl')) != receipt_paths:
            raise RuntimeError('receipt journal set changed during shard: '+node)
        for path, before in snapshots.items():
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise RuntimeError('receipt journal changed during shard: '+str(path))
        prior.flush()
        os.fsync(prior.fileno())
    os.replace(tmp, final)
    result = {'node': node, 'baseline': dict(baseline), 'records': count,
              'prior_records': prior_count, 'prior_sha256': digest.hexdigest(),
              'pending_sha256': hashlib.sha256(pending_bytes).hexdigest(),
              'done_bytes': done_bytes, 'total_bytes': total_bytes,
              'inputs': [{'path': str(path.relative_to(run)), 'ino': info.st_ino,
                          'size': info.st_size, 'mtime_ns': info.st_mtime_ns}
                         for path, info in snapshots.items()],
              'updated': time.time()}
    save(output/(node+'.result.json'), result)
    return result


def reuse_prefetch(run, db, pending, original, pointer=None):
    """Use completed read-only shards only when they cover the exact queue.

    Any failed identity, checksum, or coverage check must raise before the
    caller continues; the caller clears ``prior`` and performs a normal replay.
    """
    if pointer is None:
        pointer = json.loads((run/'recovery'/'prefetched-receipts.json').read_text())
    if pointer['state'] != 'ready' or pointer['parent_attempt'] != os.environ['PROJSET_ATTEMPT']:
        raise ValueError('prefetch is not for the active attempt')
    if pointer['generation'] != os.environ.get('PROJSET_RECOVERY_GENERATION'):
        raise ValueError('prefetch generation differs')
    prefetch_attempt = pointer['prefetch_attempt']
    if (not isinstance(prefetch_attempt, str) or not prefetch_attempt.startswith('prefetch-') or
            any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in prefetch_attempt)):
        raise ValueError('invalid prefetch attempt')
    folder = run/'recovery'/('receipt-shards-'+prefetch_attempt)
    candidate_bytes = (folder/'pending.json').read_bytes()
    if hashlib.sha256(candidate_bytes).hexdigest() != pointer['candidate_sha256']:
        raise ValueError('prefetch candidate digest differs')
    candidate = set(json.loads(candidate_bytes))
    if not pending.issubset(candidate):
        raise ValueError('prefetch candidates do not cover pending jobs')
    original_path = run/'recovery'/'original-counts.json'
    original_bytes = original_path.read_bytes()
    if (hashlib.sha256(original_bytes).hexdigest() != pointer['original_sha256'] or
            json.loads(original_bytes) != original):
        raise ValueError('prefetch original counts differ')
    snapshot = None
    if 'snapshot_sha256' in pointer:
        fence = json.loads((run/'recovery'/'legacy-cluster-fence.json').read_text())
        handover = Path(fence['handover_dir']).resolve()
        if not handover.is_relative_to((run/'recovery').resolve()):
            raise ValueError('prefetch snapshot is outside recovery')
        snapshot_bytes = (handover/'status.json').read_bytes()
        if hashlib.sha256(snapshot_bytes).hexdigest() != pointer['snapshot_sha256']:
            raise ValueError('prefetch snapshot digest differs')
        snapshot = json.loads(snapshot_bytes)
    manifest = json.loads((folder/'manifest.json').read_text())
    if manifest['attempt'] != prefetch_attempt or set(manifest['node_total_bytes']) != set(original):
        raise ValueError('prefetch manifest differs')
    baseline = {}
    for node in original:  # Match the serial node and record order.
        result = json.loads((folder/(node+'.result.json')).read_text())
        limit = manifest['node_total_bytes'][node]
        if (result['node'] != node or result['pending_sha256'] != pointer['candidate_sha256'] or
                result['done_bytes'] != limit or result['total_bytes'] != limit):
            raise ValueError('prefetch shard identity differs: '+node)
        node_folder = run/'nodes'/node
        current_paths = [node_folder/'receipts.jsonl'] + sorted(node_folder.glob('attempt-*/receipts.jsonl'))
        inputs = result['inputs']
        if [str(path.relative_to(run)) for path in current_paths] != [item['path'] for item in inputs]:
            raise ValueError('prefetch journal set changed: '+node)
        for path, expected in zip(current_paths, inputs):
            actual = path.stat()
            if (actual.st_ino, actual.st_size, actual.st_mtime_ns) != (expected['ino'], expected['size'], expected['mtime_ns']):
                raise ValueError('prefetch journal changed: '+str(path))
        digest = hashlib.sha256()
        rows = 0
        with (folder/(node+'.prior.jsonl')).open('rb') as stream:
            for line in stream:
                if not line.endswith(b'\n'):
                    raise ValueError('incomplete prefetch shard: '+node)
                digest.update(line)
                row = json.loads(line)
                if row.get('job') not in candidate or row.get('rel') is None:
                    raise ValueError('unexpected prefetch receipt: '+node)
                if row['job'] in pending:
                    db.execute('insert or replace into prior values(?,?,?)', (row['rel'], row['job'], json.dumps(row)))
                rows += 1
        if digest.hexdigest() != result['prior_sha256'] or rows != result['prior_records']:
            raise ValueError('prefetch shard checksum differs: '+node)
        if not isinstance(result['baseline'], dict) or any(not isinstance(v, int) or v < 0 for v in result['baseline'].values()):
            raise ValueError('invalid prefetch baseline: '+node)
        baseline[node] = collections.Counter(result['baseline'])
    if snapshot is not None:
        for key in ('processed', 'copied', 'skipped', 'copied_bytes', 'skipped_bytes',
                    'error', 'conflict', 'changed', 'unsupported'):
            if sum(counts.get(key, 0) for counts in baseline.values()) < snapshot['counts'].get(key, 0):
                raise ValueError('prefetch baseline below saved snapshot: '+key)
    return baseline


def rebuild_receipts(run, db, pending, original, report, ssh):
    """Return counters and insert only pending-job receipts in original order.

    This helper may modify only the empty ``prior`` table.  The caller clears
    it and performs the serial rebuild if any shard or verification fails.
    """
    attempt = os.environ['PROJSET_ATTEMPT']
    if not attempt or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in attempt):
        raise ValueError('invalid attempt')
    pointer_path = run/'recovery'/'prefetched-receipts.json'
    if os.environ.get('PROJSET_PREFETCH_MODE') != '1' and pointer_path.is_file():
        try:
            report('receipts-prefetched-validation', seen=0, force=True)
            baseline = reuse_prefetch(run, db, pending, original)
            save(run/'recovery'/'prefetch-reuse.json', {'state': 'used', 'attempt': attempt,
                                                        'updated': time.time()})
            return baseline
        except Exception as error:
            # The prior table is empty before this helper is entered; SQLite's
            # journal_mode=off does not guarantee transactional rollback.
            db.execute('delete from prior')
            save(run/'recovery'/'prefetch-reuse.json', {'state': 'rejected', 'attempt': attempt,
                 'updated': time.time(), 'error': repr(error)})
    output = run/'recovery'/('receipt-shards-'+attempt)
    output.mkdir(mode=0o700)
    pending_path = output/'pending.json'
    save(pending_path, sorted(pending))
    pending_sha = hashlib.sha256(pending_path.read_bytes()).hexdigest()
    node_totals = {}
    for node in original:
        folder = run/'nodes'/node
        paths = [folder/'receipts.jsonl'] + sorted(folder.glob('attempt-*/receipts.jsonl'))
        node_totals[node] = sum(path.stat().st_size for path in paths)
    save(output/'manifest.json', {'attempt': attempt, 'node_total_bytes': node_totals,
                                  'total_bytes': sum(node_totals.values()), 'created': time.time()})
    generation = os.environ.get('PROJSET_RECOVERY_GENERATION')
    env = ('PROJSET_RECOVERY_GENERATION='+shlex.quote(generation)+' ') if generation else ''
    priority = 'nice -n 10 ' if os.environ.get('PROJSET_PREFETCH_MODE') == '1' else ''
    processes = {}
    logs = []
    report('receipts-parallel', seen=0, force=True)
    try:
        for node in original:
            if not node or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in node):
                raise ValueError('invalid logical node')
            command = (env+priority+'python3 -S -B '+shlex.quote(str(Path(__file__).resolve()))+
                       ' --run '+shlex.quote(str(run))+' --node '+shlex.quote(node)+
                       ' --pending '+shlex.quote(str(pending_path))+' --output '+shlex.quote(str(output)))
            log = (output/(node+'.log')).open('xb')
            logs.append(log)
            processes[node] = subprocess.Popen(ssh(node, command), stdin=subprocess.DEVNULL,
                                               stdout=log, stderr=log, close_fds=True)
        last_report = time.monotonic()
        while True:
            finished = 0
            for node, process in processes.items():
                rc = process.poll()
                if rc is not None and rc != 0:
                    raise RuntimeError('receipt shard failed for '+node+'; see '+str(output/(node+'.log')))
                finished += rc is not None
            now = time.monotonic()
            if now-last_report >= 25:
                seen = 0
                for node in original:
                    for suffix in ('.result.json', '.progress.json'):
                        try:
                            seen += int(json.loads((output/(node+suffix)).read_text())['records'])
                            break
                        except (OSError, ValueError, KeyError, TypeError):
                            pass
                report('receipts-parallel', seen=seen)
                last_report = now
            if finished == len(processes):
                break
            time.sleep(1)
        baseline = {}
        for node in original:  # Same node and row order as serial replay.
            result = json.loads((output/(node+'.result.json')).read_text())
            if (result['node'] != node or result['pending_sha256'] != pending_sha or
                    result['total_bytes'] != node_totals[node] or
                    result['done_bytes'] != node_totals[node]):
                raise ValueError('receipt shard identity mismatch: '+node)
            digest = hashlib.sha256()
            rows = 0
            with (output/(node+'.prior.jsonl')).open('rb') as stream:
                for line in stream:
                    if not line.endswith(b'\n'):
                        raise ValueError('incomplete receipt shard: '+node)
                    digest.update(line)
                    row = json.loads(line)
                    if row.get('job') not in pending or row.get('rel') is None:
                        raise ValueError('unexpected pending receipt: '+node)
                    db.execute('insert or replace into prior values(?,?,?)', (row['rel'], row['job'], json.dumps(row)))
                    rows += 1
            if digest.hexdigest() != result['prior_sha256'] or rows != result['prior_records']:
                raise ValueError('receipt shard checksum mismatch: '+node)
            baseline[node] = collections.Counter(result['baseline'])
        return baseline
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
        for process in processes.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--node', required=True)
    parser.add_argument('--pending', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    shard(args.run, args.node, args.pending, args.output)


if __name__ == '__main__':
    main()
