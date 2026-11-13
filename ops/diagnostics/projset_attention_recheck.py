#!/usr/bin/env python3
"""Distributed, resumable retry of interrupted scans and no-replace conflict review.

The manifest is an immutable snapshot. Workers use the frozen migration engine,
retain every attempt, and retry transient failures without an attempt limit.
Conflicting destination content is never overwritten or selected automatically.
"""
import argparse
import collections
import contextlib
import csv
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid


RUN = Path('/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae')
OUTCOMES = Path('/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/runs/projset-copy-outcomes-20261102T144640Z-4316ae')
NODES = ['cpu-master-0'] + ['cpu-worker-%d' % i for i in range(15)]
EXCLUDED_PREFIXES = ('eai_pwm/datasets/Sekai',)


def excluded(rel):
    return any(rel == prefix or rel.startswith(prefix + '/') for prefix in EXCLUDED_PREFIXES)


def atomic(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, ensure_ascii=True, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


@contextlib.contextmanager
def swanlab_upload_slot(work, interval=120):
    """Serialize attention reporters across runs and preserve the cadence after a crash."""
    recovery = work.parent
    with (recovery / 'attention-swanlab-cadence.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = recovery / 'attention-swanlab-cadence.json'
        try:
            last = json.loads(path.read_text()).get('enqueued_at', 0)
        except FileNotFoundError:
            last = 0
        remaining = float(last) + interval - time.time()
        if remaining > 0:
            time.sleep(remaining)
        reserved = time.time()
        atomic(path, {'enqueued_at': reserved, 'interval_seconds': interval})
        try:
            yield reserved
        finally:
            atomic(path, {'enqueued_at': time.time(), 'interval_seconds': interval})


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def prepare(run, work, outcomes):
    if work.exists():
        raise ValueError('work directory already exists')
    if not work.resolve().is_relative_to((run / 'recovery').resolve()):
        raise ValueError('work directory must be under recovery')
    all_conflicts = read_csv(outcomes / 'conflict-paths.csv')
    all_errors = read_csv(outcomes / 'reconciliation/pending-errors.csv')
    conflicts = [row for row in all_conflicts if not excluded(row['relative_path'])]
    errors = [row for row in all_errors if not excluded(row['relative_path'])]
    if (not conflicts and not errors) or any(not row['relative_path'] for row in conflicts + errors):
        raise ValueError('missing manifest data')
    shards = {node: [] for node in NODES}
    for kind, rows in [('error', errors), ('conflict', conflicts)]:
        for index, row in enumerate(rows):
            key = hashlib.sha256(json.dumps([kind, row['relative_path'], row['job'],
                              row['receipt_path'], row['receipt_offset']], ensure_ascii=True).encode()).hexdigest()
            assigned = row.get('verification_node') or NODES[index % len(NODES)]
            if assigned not in NODES:
                raise ValueError('invalid verification node')
            shards[assigned].append({'key': key, 'category': kind,
                'rel': row['relative_path'], 'job': row['job'], 'reason': row['reason'],
                'receipt_path': row['receipt_path'], 'receipt_offset': row['receipt_offset'],
                'expected_kind': row.get('expected_kind', ''),
                'review_source_sha256': row.get('review_source_sha256', ''),
                'review_source_size': row.get('review_source_size', ''),
                'review_source_mtime_ns': row.get('review_source_mtime_ns', ''),
                'review_link_target': row.get('review_link_target', ''),
                'verify_sha256': row.get('verify_sha256', ''),
                'verify_link_target': row.get('verify_link_target', ''),
                'verify_size': row.get('verify_size', '')})
    all_keys = [item['key'] for items in shards.values() for item in items]
    if len(set(all_keys)) != len(all_keys):
        raise ValueError('duplicate manifest record')
    work.mkdir(mode=0o700, parents=False)
    (work / 'shards').mkdir(mode=0o700)
    (work / 'workers').mkdir(mode=0o700)
    (work / 'code').mkdir(mode=0o700)
    root = Path(__file__).resolve().parent
    for name in ('projset_attention_recheck.py', 'projset_conflict_files.py', 'projset_conflict_links.py'):
        data = (root / name).read_bytes()
        (work / 'code' / name).write_bytes(data)
    manifest = {'created': time.time(), 'nodes': NODES, 'conflicts': len(conflicts),
                'errors': len(errors), 'excluded_prefixes': list(EXCLUDED_PREFIXES),
                'excluded_conflicts': len(all_conflicts) - len(conflicts),
                'excluded_errors': len(all_errors) - len(errors), 'shards': {}, 'code': {}}
    for node, items in shards.items():
        raw = json.dumps({'node': node, 'items': items}, ensure_ascii=True,
                         separators=(',', ':')).encode()
        path = work / 'shards' / (node + '.json')
        path.write_bytes(raw)
        manifest['shards'][node] = {'sha256': hashlib.sha256(raw).hexdigest(), 'items': len(items)}
        (work / 'workers' / node).mkdir(mode=0o700)
    for path in (work / 'code').iterdir():
        manifest['code'][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    atomic(work / 'manifest.json', manifest)
    return manifest


def prepare_archive(run, work, review):
    """Select reviewed files and links for an isolated, no-replace source backup."""
    review = review.resolve()
    if not review.is_relative_to((run / 'recovery').resolve()):
        raise ValueError('review outside recovery')
    state = json.loads((review / 'status.json').read_text())
    if state['state'] != 'finished' or state['done'] != state['total']:
        raise ValueError('review is not complete')
    actionable = {'different_sha256', 'different_size', 'manual_different_link', 'manual_different_type'}
    rows, seen = [], set()
    category_counts = collections.Counter()
    for path in (review / 'workers').glob('*/receipts.jsonl'):
        with path.open() as stream:
            for line in stream:
                item = json.loads(line)
                if item.get('state') != 'done' or item.get('category') != 'conflict':
                    continue
                if item['key'] in seen:
                    raise ValueError('duplicate reviewed conflict')
                seen.add(item['key'])
                result = item['result']['result']
                category_counts[result] += 1
                if result not in actionable:
                    continue
                if result == 'manual_different_type' and item['result'].get('source_kind') not in ('file', 'symlink'):
                    continue
                details = item['result']
                expected_kind = ('symlink' if result == 'manual_different_link' else
                                 details.get('source_kind') if result == 'manual_different_type' else 'file')
                rows.append({'relative_path': item['rel'], 'job': item['job'], 'reason': item['reason'],
                             'receipt_path': item['receipt_path'], 'receipt_offset': item['receipt_offset'],
                             'expected_kind': expected_kind, 'review_source_sha256': details.get('source_sha256', ''),
                             'review_source_size': details.get('source_size', ''),
                             'review_source_mtime_ns': details.get('source_mtime_ns', ''),
                             'review_link_target': details.get('source_link', '')})
    if len(seen) != state['conflicts_total'] or not rows:
        raise ValueError('review conflict coverage incomplete')
    if len({row['relative_path'] for row in rows}) != len(rows):
        raise ValueError('duplicate source archive path')
    fields = ['relative_path', 'job', 'reason', 'receipt_path', 'receipt_offset', 'expected_kind',
              'review_source_sha256', 'review_source_size', 'review_source_mtime_ns', 'review_link_target']
    with tempfile.TemporaryDirectory(prefix='projset-archive-manifest-') as temp:
        root = Path(temp); (root / 'reconciliation').mkdir()
        for name, entries in [('conflict-paths.csv', rows), ('reconciliation/pending-errors.csv', [])]:
            with (root / name).open('w', encoding='utf-8-sig', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader(); writer.writerows(entries)
        manifest = prepare(run, work, root)
    archive = work / 'source-versions'
    archive.mkdir(mode=0o700)
    manifest.update(mode='archive', review=str(review), archive_root=str(archive),
                    review_categories=dict(category_counts))
    atomic(work / 'manifest.json', manifest)
    return manifest


def prepare_archive_verification(run, work, archive_work):
    archive_work = archive_work.resolve()
    if not archive_work.is_relative_to((run / 'recovery').resolve()):
        raise ValueError('archive outside recovery')
    status = json.loads((archive_work / 'status.json').read_text())
    source_manifest = json.loads((archive_work / 'manifest.json').read_text())
    if (source_manifest.get('mode') != 'archive' or status['state'] != 'finished' or
            status['done'] != status['total']):
        raise ValueError('source archive is not finished')
    rows, seen = [], set()
    for path in (archive_work / 'workers').glob('*/receipts.jsonl'):
        with path.open() as stream:
            for line in stream:
                item = json.loads(line)
                if item.get('state') != 'done':
                    continue
                if item['key'] in seen or item['result']['result'] != 'archived_source':
                    raise ValueError('archive has duplicate or unresolved results')
                seen.add(item['key'])
                receipt = item['result']['receipt']
                source_node = item['node']
                if source_node not in NODES or receipt['kind'] not in ('file', 'symlink'):
                    raise ValueError('unsupported archive receipt')
                if receipt['kind'] == 'file' and len(receipt.get('sha256', '')) != 64:
                    raise ValueError('archive receipt missing file digest')
                rows.append({'relative_path': item['rel'], 'job': item['job'], 'reason': item['reason'],
                             'receipt_path': item['receipt_path'], 'receipt_offset': item['receipt_offset'],
                             'expected_kind': receipt['kind'], 'verify_sha256': receipt.get('sha256', ''),
                             'verify_link_target': receipt.get('target', ''),
                             'verify_size': receipt['source_identity']['size'],
                             'verification_node': NODES[(NODES.index(source_node) + 1) % len(NODES)]})
    if len(seen) != status['total']:
        raise ValueError('archive receipt coverage incomplete')
    fields = ['relative_path', 'job', 'reason', 'receipt_path', 'receipt_offset', 'expected_kind',
              'verify_sha256', 'verify_link_target', 'verify_size', 'verification_node']
    with tempfile.TemporaryDirectory(prefix='projset-archive-verification-') as temp:
        root = Path(temp); (root / 'reconciliation').mkdir()
        for name, entries in [('conflict-paths.csv', rows), ('reconciliation/pending-errors.csv', [])]:
            with (root / name).open('w', encoding='utf-8-sig', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader(); writer.writerows(entries)
        manifest = prepare(run, work, root)
    manifest.update(mode='verify_archive', source_archive=str(archive_work),
                    archive_root=source_manifest['archive_root'])
    atomic(work / 'manifest.json', manifest)
    return manifest


def prepare_stale_directory_retry(run, work, outcomes):
    """Reclassify recorded directory ESTALE conflicts as scan retries."""
    rows = [row for row in read_csv(outcomes / 'conflict-paths.csv')
            if row.get('kind') == 'directory' and '[Errno 116]' in row.get('reason', '')]
    if not rows:
        raise ValueError('no historical directory stale-handle records')
    fields = ['relative_path', 'job', 'reason', 'receipt_path', 'receipt_offset']
    with tempfile.TemporaryDirectory(prefix='projset-stale-directory-') as temp:
        root = Path(temp); (root / 'reconciliation').mkdir()
        for name, entries in [('conflict-paths.csv', []), ('reconciliation/pending-errors.csv', rows)]:
            with (root / name).open('w', encoding='utf-8-sig', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
                writer.writeheader(); writer.writerows(entries)
        manifest = prepare(run, work, root)
    manifest.update(mode='stale_directory_retry', reclassified_from='historical_conflict')
    atomic(work / 'manifest.json', manifest)
    return manifest


def load_worker(run, work, node):
    manifest = json.loads((work / 'manifest.json').read_text())
    if node not in NODES or manifest['nodes'] != NODES:
        raise ValueError('unexpected node configuration')
    raw = (work / 'shards' / (node + '.json')).read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest['shards'][node]['sha256']:
        raise ValueError('shard changed')
    for name, digest in manifest['code'].items():
        if hashlib.sha256((work / 'code' / name).read_bytes()).hexdigest() != digest:
            raise ValueError('review code changed')
    shard = json.loads(raw)
    if shard['node'] != node or len(shard['items']) != manifest['shards'][node]['items']:
        raise ValueError('invalid shard')
    recovery = module('frozen_recovery', run / 'recovery/code/projset_recovery_copy.py')
    cfg = recovery.read(run / 'config.json')
    recovery.validate(run, cfg, node)
    recovery.engine.configure_directory_opener(recovery.native.open_parent_readonly)
    recovery.engine.configure_rate_limit(16 * 1024 * 1024)
    files = module('conflict_files', work / 'code/projset_conflict_files.py')
    links = module('conflict_links', work / 'code/projset_conflict_links.py')
    limits = files.Limits(16 * 1024 * 1024, 20, 8 * 1024**3, 1024**3)
    if manifest.get('mode') == 'archive':
        archive = Path(manifest['archive_root']).resolve()
        if archive != (work / 'source-versions').resolve() or not archive.is_relative_to((run / 'recovery').resolve()):
            raise ValueError('unsafe source archive root')
        cfg = dict(cfg, attention_archive=str(archive))
    if manifest.get('mode') == 'verify_archive':
        archive = Path(manifest['archive_root']).resolve()
        source_archive = Path(manifest['source_archive']).resolve()
        if (not source_archive.is_relative_to((run / 'recovery').resolve()) or
                archive != (source_archive / 'source-versions').resolve()):
            raise ValueError('unsafe verification archive root')
        cfg = dict(cfg, attention_verify_archive=str(archive))
    return shard['items'], recovery, cfg, files, links, limits


def kind(info):
    mode = info.st_mode
    return ('file' if stat.S_ISREG(mode) else 'symlink' if stat.S_ISLNK(mode) else
            'directory' if stat.S_ISDIR(mode) else 'special')


def lstat(native, root, rel):
    parent, name = native.open_parent_readonly(root, rel)
    try:
        info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        target = os.readlink(name, dir_fd=parent) if stat.S_ISLNK(info.st_mode) else None
        return info, target
    finally:
        os.close(parent)


class RetryWork(Exception):
    pass


def review_conflict(item, recovery, cfg, files, links, limits):
    rel = item['rel']
    source, link_source = lstat(recovery.native, cfg['source'], rel)
    try:
        target, link_target = lstat(recovery.native, cfg['target'], rel)
    except FileNotFoundError:
        if kind(source) not in ('file', 'symlink'):
            return {'result': 'manual_missing_target_type', 'source_kind': kind(source)}
        entry = dict(recovery.engine._record(source), rel=rel, kind=kind(source))
        if entry['kind'] == 'symlink':
            entry['target'] = link_source
        receipt = recovery.copy_entry_with_estale_retry(cfg['source'], cfg['target'], entry,
                                                        cfg['run_id'] + '-attention')
        if receipt['status'] in ('error', 'changed'):
            raise RetryWork(receipt.get('detail', receipt['status']))
        if receipt['status'] == 'conflict':
            return {'result': 'manual_target_changed', 'receipt': receipt}
        if receipt.get('metadata_warnings'):
            return {'result': 'manual_metadata_warning', 'receipt': receipt}
        return {'result': 'copied_missing' if receipt['status'] == 'copied' else 'same_after_race',
                'receipt': receipt}
    source_kind, target_kind = kind(source), kind(target)
    if source_kind != target_kind:
        return {'result': 'manual_different_type', 'source_kind': source_kind, 'target_kind': target_kind}
    if source_kind == 'file':
        limits.wait_pair()
        result = files.compare_file(recovery.native, cfg['source'], cfg['target'], rel, limits)
        if result['result'] == 'changed_during_comparison':
            raise RetryWork(result['result'])
        if result['result'] == 'not_hashed_limit':
            return {'result': 'manual_hash_budget', 'source_size': source.st_size,
                    'target_size': target.st_size}
        if result['result'] == 'different_or_unsupported_type':
            raise RetryWork('type changed during comparison')
        return result
    if source_kind == 'symlink':
        if link_source == link_target:
            return {'result': 'same_link_text', 'link_target': link_source}
        resolved = {}
        for name, root in [('source', cfg['source']), ('target', cfg['target'])]:
            try:
                resolved[name] = links.resolve(recovery.native, (cfg['source'], cfg['target']), root, rel)
            except (links.OutsideScope, FileNotFoundError, NotADirectoryError) as error:
                resolved[name] = {'error': type(error).__name__}
        return {'result': 'manual_different_link', 'source_link': link_source, 'target_link': link_target,
                'link_comparison': links.compare(resolved['source'], resolved['target'],
                                                  cfg['source'], cfg['target']), 'resolved': resolved}
    if source_kind == 'directory':
        return {'result': 'manual_both_directories', 'detail': 'contents not compared'}
    return {'result': 'manual_special_type'}


def archive_conflict(item, recovery, cfg):
    """Save the current source object separately; never replace either version."""
    rel = item['rel']
    if excluded(rel):
        return {'result': 'manual_excluded'}
    source, target = lstat(recovery.native, cfg['source'], rel)
    source_kind = kind(source)
    if source_kind not in ('file', 'symlink') or (item.get('expected_kind') and item['expected_kind'] != source_kind):
        return {'result': 'manual_source_type_changed', 'source_kind': source_kind}
    changed = ((item.get('review_source_size') and int(item['review_source_size']) != source.st_size) or
               (item.get('review_source_mtime_ns') and int(item['review_source_mtime_ns']) != source.st_mtime_ns) or
               (item.get('review_link_target') and item['review_link_target'] != target))
    entry = dict(recovery.engine._record(source), rel=rel, kind=source_kind)
    if source_kind == 'symlink':
        entry['target'] = target
    receipt = recovery.copy_entry_with_estale_retry(cfg['source'], cfg['attention_archive'], entry,
                                                    cfg['run_id'] + '-conflict-archive')
    if receipt.get('status') in ('error', 'changed'):
        raise RetryWork(receipt.get('detail', receipt['status']))
    if receipt.get('status') == 'conflict':
        return {'result': 'manual_archive_collision', 'receipt': receipt}
    if receipt.get('status') not in ('copied', 'skipped'):
        raise RetryWork('unexpected archive result')
    if receipt.get('metadata_warnings'):
        return {'result': 'manual_metadata_warning', 'receipt': receipt}
    if (changed or (item.get('review_source_sha256') and
                    item['review_source_sha256'] != receipt.get('sha256'))):
        return {'result': 'manual_source_changed_since_review', 'logical_bytes': source.st_size,
                'receipt': receipt}
    return {'result': 'archived_source', 'logical_bytes': source.st_size, 'receipt': receipt}


def verify_archive(item, recovery, cfg, limits):
    """Independently read back the archive without following symlinks."""
    if excluded(item['rel']):
        return {'result': 'manual_excluded'}
    parent, name = recovery.native.open_parent_readonly(cfg['attention_verify_archive'], item['rel'])
    fd = None
    try:
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if kind(before) != item['expected_kind'] or before.st_size != int(item['verify_size']):
            raise RetryWork('archive type or size differs from copy receipt')
        if item['expected_kind'] == 'symlink':
            if os.readlink(name, dir_fd=parent) != item['verify_link_target']:
                raise RetryWork('archive link target differs from copy receipt')
        elif item['expected_kind'] == 'file':
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            opened = os.fstat(fd)
            if files_identity(before) != files_identity(opened):
                raise RetryWork('archive changed while opening')
            digest = hashlib.sha256()
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                limits.consume(len(chunk)); digest.update(chunk)
            if digest.hexdigest() != item['verify_sha256']:
                raise RetryWork('archive SHA-256 differs from copy receipt')
            if files_identity(opened) != files_identity(os.fstat(fd)):
                raise RetryWork('archive changed during readback')
        else:
            raise RetryWork('unsupported archive object')
        if files_identity(before) != files_identity(os.stat(name, dir_fd=parent, follow_symlinks=False)):
            raise RetryWork('archive path changed during readback')
        return {'result': 'archive_verified', 'logical_bytes': before.st_size,
                'kind': item['expected_kind'], 'sha256': item['verify_sha256']}
    finally:
        if fd is not None:
            os.close(fd)
        os.close(parent)


def files_identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def retry_scan(item, recovery, cfg):
    """Re-run the interrupted subtree; missing files use verified no-replace copy."""
    queue = collections.deque([item['rel']])
    visited = set()
    receipts, issues = [], []
    while queue:
        rel = queue.popleft()
        if excluded(rel):
            continue
        if rel in visited:
            raise RetryWork('duplicate directory')
        visited.add(rel)
        batches, problems = [], []
        recovery.scan_directory(cfg['source'], cfg['target'], rel,
                                lambda dirs, entries: batches.append((dirs, entries)), problems.append)
        if any(row.get('status') in ('error', 'changed') for row in problems):
            raise RetryWork('directory scan error: ' + repr(problems[:2]))
        issues.extend(problems)
        for directories, entries in batches:
            queue.extend(entry['rel'] for entry in directories if not excluded(entry['rel']))
            for entry in entries:
                if excluded(entry['rel']):
                    continue
                receipt = recovery.copy_entry_with_estale_retry(cfg['source'], cfg['target'], entry,
                                                               cfg['run_id'] + '-attention')
                if receipt.get('status') in ('error', 'changed'):
                    raise RetryWork('entry retry: ' + str(receipt.get('detail', receipt.get('status'))))
                if receipt.get('status') not in ('copied', 'skipped', 'conflict'):
                    raise RetryWork('unexpected entry result')
                receipts.append(receipt)
    manual = sum(row['status'] == 'conflict' or bool(row.get('metadata_warnings')) for row in receipts)
    return {'result': 'scan_verified' if not issues and not manual else 'scan_manual_attention',
            'directories': len(visited), 'files': len(receipts),
            'copied': sum(row['status'] == 'copied' for row in receipts),
            'skipped': sum(row['status'] == 'skipped' for row in receipts),
            'manual': manual + len(issues), 'issues': issues, 'receipts': receipts}


def prior_results(path):
    rows = {}
    if not path.exists():
        return rows
    with path.open('r+b') as stream:
        while True:
            start = stream.tell()
            line = stream.readline()
            if not line:
                break
            if not line.endswith(b'\n'):
                stream.truncate(start)
                stream.flush(); os.fsync(stream.fileno())
                break
            row = json.loads(line)
            if row.get('state') == 'done':
                rows[row['key']] = row
    return rows


def worker(run, work, node):
    output = work / 'workers' / node
    with (output / 'worker.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        items, recovery, cfg, files, links, limits = load_worker(run, work, node)
        prior = prior_results(output / 'receipts.jsonl')
        pending = {row['key']: row for row in items if row['key'] not in prior}
        attempts = collections.Counter()
        next_at = {}
        stop = threading.Event()
        mutex = threading.Lock()
        state = {'node': node, 'host': socket.gethostname(), 'pid': os.getpid(),
                 'state': 'running', 'total': len(items), 'done': len(prior),
                 'pending': len(pending), 'archived_bytes': sum(x['result'].get('logical_bytes', 0) for x in prior.values()),
                 'results': dict(collections.Counter(x['result']['result'] for x in prior.values()))}

        def report():
            with mutex:
                atomic(output / 'status.json', dict(state, updated=time.time()))

        def heartbeat():
            while not stop.wait(20):
                report()

        thread = threading.Thread(target=heartbeat, daemon=True)
        report(); thread.start()
        try:
            with (output / 'receipts.jsonl').open('a') as stream:
                while pending:
                    progressed = False
                    for key, item in list(pending.items()):
                        if time.time() < next_at.get(key, 0):
                            continue
                        progressed = True
                        attempts[key] += 1
                        try:
                            result = (retry_scan(item, recovery, cfg) if item['category'] == 'error'
                                      else verify_archive(item, recovery, cfg, limits) if 'attention_verify_archive' in cfg
                                      else archive_conflict(item, recovery, cfg) if 'attention_archive' in cfg
                                      else review_conflict(item, recovery, cfg, files, links, limits))
                            row = dict(item, state='done', node=node, attempt=attempts[key],
                                       finished=time.time(), result=result)
                            del pending[key]
                            next_at.pop(key, None)
                            with mutex:
                                state['done'] += 1
                                state['pending'] = len(pending)
                                state['archived_bytes'] += result.get('logical_bytes', 0)
                                state['results'][result['result']] = state['results'].get(result['result'], 0) + 1
                        except Exception as error:
                            delay = min(300, 2 ** min(attempts[key], 8))
                            row = dict(item, state='retry', node=node, attempt=attempts[key],
                                       failed=time.time(), error_type=type(error).__name__,
                                       detail=str(error)[:500], retry_in_seconds=delay)
                            next_at[key] = time.time() + delay
                            with mutex:
                                state['retry_attempts'] = state.get('retry_attempts', 0) + 1
                        stream.write(json.dumps(row, ensure_ascii=True, separators=(',', ':')) + '\n')
                        stream.flush(); os.fsync(stream.fileno())
                        report()
                    if pending and not progressed:
                        time.sleep(min(10, max(0.1, min(next_at.values()) - time.time())))
            with mutex:
                state['state'] = 'finished'
        finally:
            stop.set(); thread.join(); report()
    return 0


def aggregate(work):
    manifest = json.loads((work / 'manifest.json').read_text())
    states = {}
    for node in NODES:
        try:
            states[node] = json.loads((work / 'workers' / node / 'status.json').read_text())
        except (OSError, ValueError):
            continue
    categories = collections.Counter()
    for state in states.values():
        categories.update(state.get('results', {}))
    return {'updated': time.time(), 'mode': manifest.get('mode', 'review'),
            'nodes_total': len(NODES), 'nodes_reporting': len(states),
            'nodes_finished': sum(s['state'] == 'finished' for s in states.values()),
            'total': sum(x['items'] for x in manifest['shards'].values()),
            'done': sum(s.get('done', 0) for s in states.values()),
            'conflicts_total': manifest['conflicts'], 'errors_total': manifest['errors'],
            'retries': sum(s.get('retry_attempts', 0) for s in states.values()),
            'archived_bytes': sum(s.get('archived_bytes', 0) for s in states.values()),
            'results': dict(categories), 'workers': states}


def supervisor(run, work):
    os.environ['PROJSET_SSH_TOPOLOGY'] = str(run / 'recovery/ssh.json')
    recovery = module('supervisor_recovery', run / 'recovery/code/projset_recovery_copy.py')
    with (work / 'supervisor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        children, next_start = {}, {}
        reporter = None
        code = work / 'code/projset_attention_recheck.py'
        while True:
            summary = aggregate(work)
            done = summary['nodes_finished'] == len(NODES) and summary['done'] == summary['total']
            summary.update(state='finished' if done else 'running', supervisor_pid=os.getpid(),
                           supervisor_host=socket.gethostname())
            atomic(work / 'status.json', summary)
            if not done:
                for node in NODES:
                    if summary['workers'].get(node, {}).get('state') == 'finished':
                        continue
                    child = children.get(node)
                    if (child is None or child.poll() is not None) and time.time() >= next_start.get(node, 0):
                        command = 'env PROJSET_SSH_TOPOLOGY=' + shlex.quote(str(run / 'recovery/ssh.json')) + ' ' + \
                            'python3 -S -B ' + shlex.quote(str(code)) + ' worker --run ' + shlex.quote(str(run)) + \
                            ' --work ' + shlex.quote(str(work)) + ' --node ' + shlex.quote(node)
                        log = (work / 'workers' / node / 'ssh.log').open('ab')
                        children[node] = subprocess.Popen(recovery.ssh(node, command), stdin=subprocess.DEVNULL,
                                                          stdout=log, stderr=log)
                        log.close()
                        next_start[node] = time.time() + 15
            try:
                repstate = json.loads((work / 'swanlab.json').read_text())
            except (OSError, ValueError):
                repstate = {}
            if repstate.get('state') != 'finished' and (reporter is None or reporter.poll() is not None):
                cfg = json.loads((run / 'recovery/ssh.json').read_text())
                log = (work / 'swanlab.log').open('ab')
                reporter = subprocess.Popen([cfg['reporter_python'], '-B', str(code), 'reporter',
                                             '--work', str(work)], stdin=subprocess.DEVNULL,
                                            stdout=log, stderr=log, start_new_session=True)
                log.close()
            if done and repstate.get('state') == 'finished':
                break
            time.sleep(10)


def reporter(work):
    with (work / 'reporter.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'all_proxy'):
            os.environ.pop(key, None)
        secrets = Path('/share-mixed/project/eai_pwm/.secrets')
        proxy = (secrets / 'swanlab_proxy').read_text().strip()
        os.environ.update({key: proxy for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY')})
        os.environ['SWANLAB_API_KEY'] = (secrets / 'swanlab_api_key').read_text().strip()
        import swanlab
        swanlab.merge_settings(swanlab.Settings(interactive=False, probe=swanlab.Settings.Probe(
            hardware=False, runtime=False, requirements=False, conda=False, git=False, swanlab=False, monitor=False)))
        path = work / 'swanlab.json'
        try:
            state = json.loads(path.read_text())
        except (OSError, ValueError):
            state = {}
        run = None
        while True:
            try:
                remaining = state.get('enqueued_at', 0) + 120 - time.time()
                if remaining > 0:
                    time.sleep(remaining)
                if run is None:
                    swanlab.login(api_key=os.environ['SWANLAB_API_KEY'], save=False, timeout=10)
                    (work / 'swanlab-sdk').mkdir(exist_ok=True)
                    manifest = json.loads((work / 'manifest.json').read_text())
                    label = ('projset-归档独立校验-' if manifest.get('mode') == 'verify_archive' else
                             'projset-冲突源版本归档-' if manifest.get('mode') == 'archive' else
                             'projset-历史错误与冲突复核-')
                    run = swanlab.init(mode='online', project='data-migration',
                                       name=label + work.name[-8:],
                                       id=state.get('run_id'), resume='allow' if state.get('run_id') else 'never',
                                       public=False, log_dir=str(work / 'swanlab-sdk'),
                                       config={'nodes': 16, 'upload_interval_seconds': 120,
                                               'conflict_policy': 'preserve_destination',
                                               'error_retry': 'unlimited_with_backoff'})
                    state.update(run_id=run.id, url=run.url, run_path=run.path)
                    atomic(path, state)
                status = json.loads((work / 'status.json').read_text())
                results = status['results']
                manual = sum(value for key, value in results.items() if key.startswith('manual_') or
                             key in ('different_size', 'different_sha256', 'scan_manual_attention'))
                identical = sum(results.get(key, 0) for key in ('same_sha256', 'same_link_text',
                                                                 'same_after_race', 'copied_missing'))
                values = {'attention/已复核项': status['done'], 'attention/总项数': status['total'],
                          'attention/待复核项': status['total'] - status['done'],
                          'attention/当前需决策项': manual, 'attention/一致或已补齐项': identical,
                          'attention/错误目录已复核': results.get('scan_verified', 0) + results.get('scan_manual_attention', 0),
                          'attention/历史错误目录总数': status['errors_total'],
                          'attention/错误重试次数': status['retries'],
                          'nodes/已报告': status['nodes_reporting'], 'nodes/已完成': status['nodes_finished']}
                if status.get('mode') == 'archive':
                    # Saving the source version does not decide which version
                    # should occupy the original destination path.
                    values['attention/当前需决策项'] = status['total']
                    values.update({'archive/源版本已保存项': results.get('archived_source', 0),
                                   'archive/源版本总项数': status['total'],
                                   'archive/待保存项': status['total'] - status['done'],
                                   'archive/人工复核项': manual,
                                   'archive/原位版本待决定项': status['total'],
                                   'archive/源版本已保存_TB': status['archived_bytes'] / 1e12})
                elif status.get('mode') == 'verify_archive':
                    values['attention/当前需决策项'] = status['total']
                    values.update({'archive/独立校验通过项': results.get('archive_verified', 0),
                                   'archive/独立校验总项数': status['total'],
                                   'archive/独立校验待处理项': status['total'] - status['done'],
                                   'archive/独立校验_TB': status['archived_bytes'] / 1e12,
                                   'archive/原位版本待决定项': status['total']})
                step = int(state.get('step', -1)) + 1
                # Reserve the cadence before enqueueing: a crash immediately
                # after run.log must not cause another upload within 120 s.
                with swanlab_upload_slot(work) as reserved:
                    state.update(state='sending', step=step, enqueued_at=reserved,
                                 interval_seconds=120, pid=os.getpid(), source_updated=status['updated'])
                    atomic(path, state)
                    run.log(values, step=step)
                state.update(state='streaming')
                state.pop('error_type', None)
                atomic(path, state)
                if status['state'] == 'finished':
                    run.finish()
                    state.update(state='finished', finished_at=time.time())
                    atomic(path, state)
                    return 0
                time.sleep(120)
            except Exception as error:
                state.update(state='retrying', error_type=type(error).__name__, updated=time.time())
                atomic(path, state)
                time.sleep(120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'prepare-archive', 'prepare-verify', 'prepare-stale-directory', 'worker', 'supervisor', 'reporter'))
    parser.add_argument('--run', type=Path, default=RUN)
    parser.add_argument('--outcomes', type=Path, default=OUTCOMES)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--node')
    parser.add_argument('--review', type=Path)
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args()
    if args.mode == 'prepare':
        print(json.dumps(prepare(args.run, args.work, args.outcomes)))
    elif args.mode == 'prepare-archive':
        if args.review is None:
            parser.error('--review is required for prepare-archive')
        print(json.dumps(prepare_archive(args.run, args.work, args.review)))
    elif args.mode == 'prepare-verify':
        if args.archive is None:
            parser.error('--archive is required for prepare-verify')
        print(json.dumps(prepare_archive_verification(args.run, args.work, args.archive)))
    elif args.mode == 'prepare-stale-directory':
        print(json.dumps(prepare_stale_directory_retry(args.run, args.work, args.outcomes)))
    elif args.mode == 'worker':
        return worker(args.run, args.work, args.node)
    elif args.mode == 'supervisor':
        return supervisor(args.run, args.work)
    else:
        return reporter(args.work)


if __name__ == '__main__':
    sys.exit(main())
