#!/usr/bin/env python3
"""Join historical projset error receipts to independently recorded outcomes.

This is a read-only interpretation of the copy and retry journals. It never
starts work or changes source, destination, historical receipts, or counters.
Unmatched or ambiguous evidence is kept in the attention list.
"""
import argparse
import collections
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import time
import uuid


CURRENT_ATTEMPT = 'attempt-20261109T091641Z-ef2f7bd8'
PENDING = frozenset(('retry_pending', 'retry_attention', 'unclassified', 'ambiguous'))


def read_json(path):
    return json.loads(path.read_text())


def read_receipts(path):
    """Read a stable complete-line prefix without modifying the live journal."""
    result = {}
    with path.open('rb') as stream:
        for line in stream:
            if not line.endswith(b'\n'):
                break
            row = json.loads(line)
            result[row['rel']] = row
    return result


def error_key(row):
    """Identify one historical receipt, rather than matching a reused path."""
    return tuple(str(row.get(key, '')) for key in
                 ('node', 'job', 'relative_path', 'receipt_path', 'receipt_offset'))


def bounded_batch(run, descriptor):
    """Accept a complete, hashed, independently checked retry snapshot.

    This proves the recorded retry, not that online source data cannot change
    afterwards. No current content reads or new work are initiated here.
    """
    data = read_json(descriptor)
    if data['state'] != 'verified_snapshot':
        raise ValueError('retry snapshot is not verified')

    def artifact(name):
        ref = data[name]
        path = (run / ref['path']).resolve()
        if not path.is_relative_to((run / 'recovery').resolve()):
            raise ValueError('retry evidence outside recovery directory')
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != ref['sha256']:
            raise ValueError('retry evidence changed')
        return raw

    entries = json.loads(artifact('manifest'))['entries']
    discovery = json.loads(artifact('discovery'))
    verified = json.loads(artifact('verification'))
    errors = json.loads(artifact('error_records'))
    receipts = [json.loads(line) for line in artifact('receipts').splitlines()]

    def indexed(rows, field='rel'):
        result = {row[field]: row for row in rows}
        if len(result) != len(rows):
            raise ValueError('duplicate retry evidence')
        return result

    manifest, scanned = indexed(entries), indexed(discovery['entries'])
    files, directories = indexed(verified['files']), indexed(verified['directories'])
    wanted_dirs, receipt_map = indexed(discovery['directories']), indexed(receipts)
    roots = {row['relative_path'] for row in errors}
    if (not errors or discovery['errors'] or manifest != scanned or
            set(manifest) != set(files) or set(manifest) != set(receipt_map) or
            set(directories) != set(wanted_dirs) or
            roots != set(discovery['roots']) or
            not roots <= set(manifest) | set(directories) or
            verified['state'] != 'verified' or verified['summary']['failures'] != 0 or
            verified['finished'] < verified['started'] or
            verified['manifest_sha256'] != data['manifest']['sha256']):
        raise ValueError('incomplete retry snapshot')
    if verified['summary'] != dict(files=len(files), verified_files=len(files),
                                   directories=len(directories),
                                   verified_directories=len(directories), failures=0):
        raise ValueError('inconsistent retry verification totals')

    def same(a, b):
        return all(a[key] == b[key] for key in ('ino', 'size', 'mtime_ns', 'ctime_ns'))

    for rel, entry in manifest.items():
        proof, receipt = files[rel], receipt_map[rel]
        comparison = proof['comparison']
        if (proof['ok'] is not True or receipt['status'] not in ('copied', 'skipped') or
                verified['node'] == receipt['node'] or
                receipt.get('metadata_warnings') or
                verified['started'] < receipt['finished'] or
                proof['receipt_finished'] != receipt['finished'] or
                comparison['result'] != 'same_sha256' or comparison['hash_passes'] != 2 or
                len(receipt['sha256']) != 64 or
                comparison['source_sha256'] != comparison['target_sha256'] or
                comparison['source_sha256'] != receipt['sha256'] or
                comparison['source_size'] != entry['size'] or
                comparison['target_size'] != entry['size'] or
                receipt['logical_bytes'] != entry['size'] or
                not same(entry, receipt['source_identity']) or
                not same(entry, proof['source_identity']) or
                not same(receipt['dst_stat'], proof['target_identity'])):
            raise ValueError('inconsistent retry file proof')
    names = set(manifest) | set(directories)
    for rel, proof in directories.items():
        children = sorted(name[len(rel)+1:] for name in names
                          if name.startswith(rel + '/') and '/' not in name[len(rel)+1:])
        if (proof['ok'] is not True or proof['children'] != children or
                not same(wanted_dirs[rel], proof['source_identity'])):
            raise ValueError('inconsistent retry directory proof')
    for row in errors:
        if '[Errno 116]' not in row['reason']:
            raise ValueError('unsupported historical error')
        if row.get('result_time_beijing'):
            timestamp = datetime.datetime.fromisoformat(row['result_time_beijing']).timestamp()
            if timestamp > verified['started']:
                raise ValueError('verification predates historical error')
    keys = {error_key(row) for row in errors}
    if len(keys) != len(errors):
        raise ValueError('duplicate historical receipt')
    return keys


def verified_scan_batch(run, descriptor):
    """Match no-replace scan retry receipts to exact historical error records."""
    data = read_json(descriptor)
    if data['state'] != 'verified_scan_retry' or not data['records']:
        raise ValueError('scan retry is not complete')
    run = run.resolve()
    recovery = (run / 'recovery').resolve()
    work = (run / data['work']).resolve()
    if not work.is_relative_to(recovery):
        raise ValueError('scan retry work outside recovery')
    manifest = read_json(work / 'manifest.json')

    def record_at(path, offset):
        with path.open('rb') as stream:
            stream.seek(int(offset))
            raw = stream.readline()
        if not raw.endswith(b'\n'):
            raise ValueError('incomplete receipt')
        return raw, json.loads(raw)

    result = set()
    for item in data['records']:
        error = item['error']
        key = error_key(error)
        historical = Path(error['receipt_path']).resolve()
        nodes = (run / 'nodes').resolve()
        if (not historical.is_relative_to(nodes) or
                historical.relative_to(nodes).parts[0] != error['node']):
            raise ValueError('historical receipt outside run')
        _, original = record_at(historical, error['receipt_offset'])
        proof = item['retry']
        path = (run / proof['path']).resolve()
        if not path.is_relative_to((work / 'workers').resolve()):
            raise ValueError('retry receipt outside worker directory')
        raw, retried = record_at(path, proof['offset'])
        node = retried['node']
        key_data = ['error', error['relative_path'], error['job'],
                    error['receipt_path'], error['receipt_offset']]
        expected_key = hashlib.sha256(json.dumps(key_data, ensure_ascii=True).encode()).hexdigest()
        if (hashlib.sha256(raw).hexdigest() != proof['sha256'] or
                node not in manifest['nodes'] or path != work / 'workers' / node / 'receipts.jsonl' or
                retried.get('key') != expected_key or
                original.get('status') != 'error' or original.get('rel') != error['relative_path'] or
                str(original.get('job')) != str(error['job']) or
                original.get('detail') != retried.get('reason') or
                retried.get('category') != 'error' or retried.get('state') != 'done' or
                retried.get('rel') != error['relative_path'] or
                str(retried.get('job')) != str(error['job']) or
                retried.get('receipt_path') != error['receipt_path'] or
                str(retried.get('receipt_offset')) != str(error['receipt_offset'])):
            raise ValueError('retry receipt does not match historical error')
        result_data = retried['result']
        receipts = result_data['receipts']
        if (result_data['result'] != 'scan_verified' or result_data['directories'] < 1 or
                result_data['manual'] != 0 or result_data['issues'] or
                result_data['files'] != len(receipts) or
                result_data['copied'] != sum(r.get('status') == 'copied' for r in receipts) or
                result_data['skipped'] != sum(r.get('status') == 'skipped' for r in receipts) or
                len({r.get('rel') for r in receipts}) != len(receipts) or
                any(r.get('status') not in ('copied', 'skipped') or r.get('metadata_warnings') or
                    not r.get('rel', '').startswith(error['relative_path'] + '/') or
                    (r.get('kind') == 'file' and len(r.get('sha256', '')) != 64)
                    for r in receipts)):
            raise ValueError('scan retry has unresolved results')
        if key in result:
            raise ValueError('duplicate historical error')
        result.add(key)
    return result


def evidence_for(run):
    base = run / 'recovery/fairness-verification-20261110'
    prior = run / 'recovery/prior-error-retry-20261110'
    recent = run / 'recovery/stale-retry-20261110T1724'
    evidence = {'first_roots': set(), 'recent_roots': set(), 'rescanned': {},
                'prior_manifest': {}, 'prior_receipts': {}, 'bounded_batches': {},
                'scan_batches': {}, 'warnings': []}

    def add(name, build):
        try:
            build()
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
            evidence['warnings'].append({'evidence': name, 'error_type': type(error).__name__})

    def load_first():
        resolution = read_json(base / 'retry-resolution.json')
        independent = read_json(base / 'retry-independent-verification.json')
        cross = read_json(base / 'retry-cross-mount-visibility.json')
        roots = set(resolution['source_roots'])
        if (len(roots) != resolution['original_error_paths'] or
                not independent['all_verified'] or independent['issues'] or
                independent['files'] != resolution['copied_files'] or
                cross['issues'] or cross['visible'] != cross['total'] or
                cross['total'] != resolution['copied_files']):
            raise ValueError('incomplete first retry evidence')
        issues = run / 'nodes/cpu-worker-6' / CURRENT_ATTEMPT / 'issues.jsonl'
        root_jobs = collections.defaultdict(set)
        with issues.open('rb') as stream:
            for line in stream:
                if not line.endswith(b'\n'):
                    break
                row = json.loads(line)
                if row.get('status') == 'error' and row.get('rel') in roots:
                    root_jobs[row['rel']].add(str(row['job']))
        if set(root_jobs) != roots or any(len(jobs) != 1 for jobs in root_jobs.values()):
            raise ValueError('first retry roots do not identify unique jobs')
        evidence['first_roots'] = {(next(iter(jobs)), rel) for rel, jobs in root_jobs.items()}

    def load_recent():
        resolution = read_json(recent / 'resolution.json')
        audit = read_json(Path(resolution['source_identity_audit']))
        target = read_json(Path(resolution['root_target_check']))
        verified = read_json(Path(resolution['independent_verification']))
        manifest_raw = (recent / 'manifest.json').read_bytes()
        manifest = json.loads(manifest_raw)['entries']
        roots = {(str(row['job']), row['rel']) for row in audit['results']
                 if row.get('matches_original') is True}
        target_roots = {row['rel'] for row in target['results']
                        if row['source_kind'] == row['target_kind']}
        verified_files = {row['rel'] for row in verified['files'] if row.get('ok') is True}
        names = {row['rel'] for row in manifest}
        if (resolution['state'] != 'verified' or
                hashlib.sha256(manifest_raw).hexdigest() != resolution['manifest_sha256'] or
                len(roots) != resolution['error_roots'] or len(audit['results']) != resolution['error_roots'] or
                any(row.get('matches_original') is not True for row in audit['results']) or
                len(target['results']) != resolution['error_roots'] or
                any(row['source_kind'] != row['target_kind'] for row in target['results']) or
                {rel for _, rel in roots} != target_roots or
                len(names) != resolution['files'] or len(manifest) != resolution['files'] or
                len(verified['files']) != resolution['files'] or names != verified_files or
                any(row.get('ok') is not True for row in verified['files']) or
                verified['summary']['failures'] != 0 or
                verified['summary']['total'] != resolution['files'] or
                verified['summary']['verified'] != resolution['files']):
            raise ValueError('incomplete recent retry evidence')
        evidence['recent_roots'] = roots

    def load_scans():
        data = read_json(base / 'old-interrupted-scan-check.json')
        if (data['unconfirmed'] or data['wanted'] != data['successfully_enumerated'] or
                len(data['found']) != data['wanted']):
            raise ValueError('incomplete rescan evidence')
        evidence['rescanned'] = {str(job): row['rel'] for job, row in data['found'].items()}

    def load_prior():
        raw = (prior / 'manifest.json').read_bytes()
        deployment = read_json(prior / 'deployment.json')
        if hashlib.sha256(raw).hexdigest() != deployment['manifest_sha256']:
            raise ValueError('prior retry manifest changed')
        entries = json.loads(raw)['entries']
        manifest = {row['rel']: row for row in entries}
        if len(manifest) != len(entries):
            raise ValueError('duplicate prior retry path')
        evidence['prior_manifest'] = manifest
        evidence['prior_receipts'] = read_receipts(prior / 'receipts.jsonl')

    add('first_retry', load_first)
    add('recent_retry', load_recent)
    add('interrupted_scans', load_scans)
    add('prior_retry', load_prior)
    for descriptor in sorted((run / 'recovery/error-retry-resolutions').glob('*.json')):
        def load_batch(descriptor=descriptor):
            keys = bounded_batch(run, descriptor)
            name = str(descriptor.relative_to(run / 'recovery'))
            evidence['bounded_batches'][name] = keys
        add(str(descriptor.relative_to(run)), load_batch)
    for descriptor in sorted((run / 'recovery/scan-retry-resolutions').glob('*.json')):
        def load_scan_batch(descriptor=descriptor):
            keys = verified_scan_batch(run, descriptor)
            name = str(descriptor.relative_to(run / 'recovery'))
            evidence['scan_batches'][name] = keys
        add(str(descriptor.relative_to(run)), load_scan_batch)
    return evidence


def classify(row, evidence):
    rel, job, reason = row['relative_path'], str(row['job']), row['reason']
    matches = []
    if (reason.startswith('RuntimeError: control connection ended during scan') and
            evidence['rescanned'].get(job) == rel):
        matches.append(('rescan_confirmed', 'old-interrupted-scan-check.json'))
    if ('[Errno 116]' in reason and row['node'] == 'cpu-worker-6' and
            '/' + CURRENT_ATTEMPT + '/' in row['receipt_path']):
        if (job, rel) in evidence['first_roots']:
            matches.append(('verified_193_file_batch', 'retry-resolution.json'))
        if (job, rel) in evidence['recent_roots']:
            matches.append(('verified_481_file_batch', 'stale-retry-20261110T1724/resolution.json'))
    if '[Errno 116]' in reason and rel in evidence['prior_manifest']:
        entry = evidence['prior_manifest'][rel]
        receipt = evidence['prior_receipts'].get(rel)
        if receipt is None:
            matches.append(('retry_pending', 'prior-error-retry-20261110/manifest.json'))
        elif receipt.get('status') in ('copied', 'skipped'):
            try:
                error_time = datetime.datetime.fromisoformat(row['result_time_beijing']).timestamp()
                source = receipt['source_identity']
                target = receipt['dst_stat']
                valid = (error_time <= receipt['finished'] and len(receipt['sha256']) == 64 and
                         receipt['logical_bytes'] == entry['size'] == target['size'] and
                         all(source[key] == entry[key] for key in ('ino', 'size', 'mtime_ns', 'ctime_ns')) and
                         not receipt.get('metadata_warnings'))
            except (ValueError, TypeError, KeyError):
                valid = False
            matches.append(('verified_prior_file' if valid else 'retry_attention',
                            'prior-error-retry-20261110/receipts.jsonl'))
        else:
            matches.append(('retry_attention', 'prior-error-retry-20261110/receipts.jsonl'))
    for name, keys in evidence.get('bounded_batches', {}).items():
        if error_key(row) in keys:
            matches.append(('verified_retry_snapshot', name))
    for name, keys in evidence.get('scan_batches', {}).items():
        if error_key(row) in keys:
            matches.append(('verified_retry_scan', name))
    if len(matches) == 1:
        return matches[0]
    return ('unclassified' if not matches else 'ambiguous', '')


def replace_bytes(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def csv_bytes(fields, rows):
    import io
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    return ('\ufeff' + stream.getvalue()).encode('utf-8')


def reconcile(run, outcomes, output):
    source = outcomes / 'error-paths.csv'
    with source.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        if not fields or not {'relative_path', 'job', 'reason', 'receipt_path', 'node'} <= set(fields):
            raise ValueError('unexpected historical error CSV')
        rows = list(reader)
    evidence = evidence_for(run)
    for row in rows:
        row['reconciliation'], row['resolution_evidence'] = classify(row, evidence)
    counts = collections.Counter(row['reconciliation'] for row in rows)
    pending = [row for row in rows if row['reconciliation'] in PENDING]
    unique_pending = {row['relative_path']: row for row in pending if row['relative_path']}
    known = [int(row['recorded_source_bytes']) for row in unique_pending.values()
             if (row.get('recorded_source_bytes') or '').isdigit()]
    now = time.time()
    try:
        status = read_json(run / 'status.json')
        live_errors = status['counts'].get('error', 0)
        live_updated = status['updated']
    except (OSError, ValueError, KeyError, TypeError):
        live_errors, live_updated = None, None
    summary = {'updated': now, 'source_error_csv': str(source),
               'source_csv_mtime': source.stat().st_mtime, 'historical_records': len(rows),
               'unique_historical_paths': len({row['relative_path'] for row in rows}),
               'categories': dict(counts), 'attention_records': len(pending),
               'attention_unique_paths': len(unique_pending),
               'attention_known_TB': sum(known) / 10**12,
               'attention_unknown_size_paths': len(unique_pending) - len(known),
               'live_error_counter': live_errors, 'live_status_updated': live_updated,
               'live_counter_minus_exported': None if live_errors is None else live_errors - len(rows),
               'evidence_warnings': evidence['warnings'],
               'scope': 'historical records with time-stamped evidence; live source is not an atomic snapshot'}
    fields = list(fields) + ['reconciliation', 'resolution_evidence']
    replace_bytes(output / 'reconciled-errors.csv', csv_bytes(fields, rows))
    replace_bytes(output / 'pending-errors.csv', csv_bytes(fields, pending))
    replace_bytes(output / 'summary.json', json.dumps(summary, ensure_ascii=True).encode())
    labels = [('verified_193_file_batch', '193 文件批次校验通过'),
              ('verified_481_file_batch', '481 文件批次校验通过'),
              ('verified_retry_snapshot', '限定重试批次独立校验通过（校验时状态）'),
              ('verified_retry_scan', '中断扫描已重试且发现的子项通过校验'),
              ('verified_prior_file', '原有补拷已校验文件'),
              ('rescan_confirmed', '中断目录已重新扫描'),
              ('retry_pending', '原有补拷尚未完成'),
              ('retry_attention', '补拷结果需复核'),
              ('unclassified', '尚无足够证据'), ('ambiguous', '证据有重叠')]
    stamp = datetime.datetime.fromtimestamp(now, datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec='seconds')
    lines = ['# 历史错误与处理结果', '', f'更新：{stamp}；历史错误记录 {len(rows)} 条。', '',
             '| 处理情况 | 历史错误记录数 |', '|---|---:|']
    lines.extend(f'| {label} | {counts[key]} |' for key, label in labels)
    lines += ['', f'需关注 {len(pending)} 条，涉及 {len(unique_pending)} 个不同路径；已知记录时源数据量 '
              f'{sum(known) / 10**12:.12f} TB，未知大小 {len(unique_pending) - len(known)} 条。',
              '', '[待处理及证据不足清单](pending-errors.csv) · [全部错误与证据](reconciled-errors.csv)',
              '', '分类基于历史回执及限定范围的后续证据。重新扫描只确认目录扫描已完成，子项仍以各自回执为准。'
              '内容校验反映校验时状态；在线源可能继续变化，不能视为全局快照。',
              '', '主看板的错误计数为历史累计，不会因补拷而减少；源报告与本表分别读取，更新非原子。']
    if evidence['warnings']:
        lines += ['', '部分证据读取失败，对应记录保留为待关注；详见 summary.json。']
    if live_errors is not None and live_errors != len(rows):
        lines += ['', f'主任务当前错误计数为 {live_errors}，与本次导出的 {len(rows)} 条记录不同；'
                  '两者可能存在刷新延迟或恢复过程，待关注总数需要继续核对。']
    replace_bytes(output / 'REPORT.md', ('\n'.join(lines) + '\n').encode())
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--outcomes', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(reconcile(args.run, args.outcomes, args.output), ensure_ascii=False))


if __name__ == '__main__':
    main()
