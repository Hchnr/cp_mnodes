#!/usr/bin/env python3
"""Reconcile historical source-change records with bounded retry evidence.

Only derived reports are written.  Source and destination are inspected with
anchored, no-follow stat calls; this module never starts or retries copying.
"""
import collections
import csv
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid


BATCH = 'changed-retry-20261110T1837-f59ab8'
IDENTITY = ('ino', 'size', 'mtime_ns', 'ctime_ns')
SUCCESS = 'verified_snapshot_identity_unchanged'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def unique(rows):
    mapped = {row['rel']: row for row in rows}
    if len(mapped) != len(rows):
        raise ValueError('duplicate evidence path')
    return mapped


def load_batch(run):
    """Accept the batch only when every durable proof agrees."""
    base = run / 'recovery' / BATCH
    resolution = json.loads((base / 'resolution.json').read_bytes())
    selection = json.loads((base / 'selection.json').read_bytes())
    status = json.loads((base / 'status.json').read_bytes())
    manifest_raw = (base / 'manifest.json').read_bytes()
    receipts_raw = (base / 'receipts.jsonl').read_bytes()
    verification_raw = (base / 'independent-verification.json').read_bytes()
    if not receipts_raw.endswith(b'\n'):
        raise ValueError('incomplete retry receipts')
    manifest = unique(json.loads(manifest_raw)['entries'])
    receipts = unique([json.loads(line) for line in receipts_raw.splitlines()])
    verification = json.loads(verification_raw)
    verified = unique(verification['files'])
    summary = verification['summary']
    total = resolution['files']
    if (resolution['state'] != 'verified' or status['state'] != 'finished' or
            digest(manifest_raw) != resolution['manifest_sha256'] or
            digest(manifest_raw) != selection['manifest_sha256'] or
            digest(receipts_raw) != resolution['receipt_sha256'] or
            digest(verification_raw) != resolution['independent_verification_sha256'] or
            summary['manifest_sha256'] != resolution['manifest_sha256'] or
            summary['retry_state'] != 'finished' or summary['total'] != total or
            summary['verified'] != total or summary['failures'] != 0 or
            len(manifest) != total or set(manifest) != set(receipts) or
            set(manifest) != set(verified) or
            status['counts'].get('copied', 0) + status['counts'].get('skipped', 0) != total):
        raise ValueError('incomplete or inconsistent batch evidence')
    results = {}
    for rel, entry in manifest.items():
        receipt, check = receipts[rel], verified[rel]
        source = {key: receipt['source_identity'][key] for key in IDENTITY}
        target = {key: receipt['dst_stat'][key] for key in IDENTITY}
        if (entry['kind'] != 'file' or receipt['status'] not in ('copied', 'skipped') or
                any(source[key] != entry[key] for key in IDENTITY) or
                receipt['logical_bytes'] != entry['size'] or target['size'] != entry['size'] or
                receipt.get('metadata_warnings') or
                not re.fullmatch(r'[0-9a-f]{64}', receipt['sha256']) or
                check.get('ok') is not True or check.get('result') != 'same_sha256' or
                check.get('hash_passes') != 2 or check.get('sha256') != receipt['sha256'] or
                check.get('size') != entry['size']):
            raise ValueError('inconsistent per-file evidence')
        results[rel] = dict(source=source, target=target, finished=receipt['finished'],
                            sha256=receipt['sha256'])
    return results


def current_identity(native, root, rel):
    fd, name = native.open_parent_readonly(root, rel)
    try:
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
    finally:
        os.close(fd)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('current path is not a regular file')
    return {key: getattr(info, 'st_' + key) for key in IDENTITY}


def classify(row, evidence, native, source, target):
    item = evidence.get(row['relative_path'])
    if item is None or row.get('kind') != 'file':
        return 'unconfirmed', ''
    try:
        recorded = datetime.datetime.fromisoformat(row['result_time_beijing'])
        # The exported timestamp is rounded down to seconds. A later event
        # within the same second must not be attributed to an older retry.
        if recorded.tzinfo is None or recorded.timestamp() + 1 > item['finished']:
            return 'unconfirmed', ''
        before = current_identity(native, source, row['relative_path'])
        after = current_identity(native, target, row['relative_path'])
        if before != item['source'] or after != item['target']:
            return 'identity_changed_since_verification', BATCH + '/resolution.json'
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        return 'identity_unavailable', BATCH + '/resolution.json'
    return SUCCESS, BATCH + '/resolution.json'


def replace(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def csv_bytes(fields, rows):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    return ('\ufeff' + stream.getvalue()).encode('utf-8')


def reconcile(run, outcomes, output, native, source, target):
    source_csv = outcomes / 'changed-paths.csv'
    with source_csv.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        if not fields or not {'relative_path', 'kind', 'result_time_beijing'} <= set(fields):
            raise ValueError('unexpected historical changed CSV')
        rows = list(reader)
    warnings = []
    try:
        evidence = load_batch(run)
    except (OSError, ValueError, TypeError, KeyError, OverflowError) as error:
        evidence = {}
        warnings.append({'batch': BATCH, 'error_type': type(error).__name__})
    for row in rows:
        row['reconciliation'], row['resolution_evidence'] = classify(
            row, evidence, native, source, target)
    categories = collections.Counter(row['reconciliation'] for row in rows)
    pending = [row for row in rows if row['reconciliation'] != SUCCESS]
    unique_pending = {row['relative_path'] for row in pending if row['relative_path']}
    try:
        live = json.loads((run / 'status.json').read_text())
        live_changed = live['counts'].get('changed', 0)
    except (OSError, ValueError, TypeError, KeyError):
        live_changed = None
    now = time.time()
    summary = {'updated': now, 'historical_records': len(rows),
               'categories': dict(categories), 'attention_records': len(pending),
               'attention_unique_paths': len(unique_pending), 'live_changed_counter': live_changed,
               'live_counter_minus_exported': None if live_changed is None else live_changed - len(rows),
               'source_csv_mtime': source_csv.stat().st_mtime, 'evidence_warnings': warnings,
               'scope': 'verified snapshot and current metadata observations; live files are not atomic'}
    fields = list(fields) + ['reconciliation', 'resolution_evidence']
    replace(output / 'reconciled-changed.csv', csv_bytes(fields, rows))
    replace(output / 'pending-changed.csv', csv_bytes(fields, pending))
    replace(output / 'summary.json', json.dumps(summary, ensure_ascii=True).encode())
    stamp = datetime.datetime.fromtimestamp(now, datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec='seconds')
    lines = ['# 历史源变化及处理证据', '', f'更新：{stamp}；历史记录 {len(rows)} 条。', '',
             '| 处理情况 | 记录数 |', '|---|---:|',
             f'| 已有独立校验证据且当前身份未见变化 | {categories[SUCCESS]} |',
             f'| 校验后当前身份发生变化 | {categories["identity_changed_since_verification"]} |',
             f'| 当前身份无法读取 | {categories["identity_unavailable"]} |',
             f'| 尚无匹配的完整证据 | {categories["unconfirmed"]} |', '',
             f'当前需关注 {len(pending)} 条记录，涉及 {len(unique_pending)} 个路径。', '',
             '[待关注记录](pending-changed.csv) · [全部关联记录](reconciled-changed.csv)', '',
             '分类基于限定路径的回执、独立 SHA-256 及当前 no-follow 属性检查。'
             '同秒写入和 NFS 缓存可能使身份属性不变；这些证据不是在线源或目标的原子快照。',
             '', '主看板 changed 为历史累计，不因后续处理减少；报告之间的采样并非原子。']
    if warnings:
        lines += ['', '部分证据不可用或不一致；对应记录保留为待关注，见 summary.json。']
    if live_changed is not None and live_changed != len(rows):
        lines += ['', f'主任务 changed 计数 {live_changed}，本次导出 {len(rows)} 条；两者可能存在刷新延迟。']
    replace(output / 'REPORT.md', ('\n'.join(lines) + '\n').encode())
    return summary
