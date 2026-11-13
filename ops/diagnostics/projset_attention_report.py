#!/usr/bin/env python3
"""Export review, retry and source-archive receipts as a human-readable report."""
import argparse
import collections
import csv
import datetime
import fcntl
import io
import json
import os
from pathlib import Path
import time
import uuid


def atomic(path, text):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x', encoding='utf-8', newline='') as stream:
        stream.write(text); stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)


def csv_text(fields, rows):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
    writer.writeheader(); writer.writerows(rows)
    return '\ufeff' + stream.getvalue()


def receipts(work):
    values = {}
    for path in (work / 'workers').glob('*/receipts.jsonl'):
        with path.open('rb') as stream:
            for line in stream:
                if not line.endswith(b'\n'):
                    break
                item = json.loads(line)
                if item['state'] == 'done':
                    values[item['key']] = item
    return values


def export(review, archive, verification=None, stale_retry=None):
    status = json.loads((review / 'status.json').read_text())
    if status['state'] != 'finished':
        raise ValueError('review not finished')
    data = receipts(review)
    if len(data) != status['total']:
        raise ValueError('review receipt coverage differs')
    try:
        archive_status = json.loads((archive / 'status.json').read_text())
    except FileNotFoundError:
        archive_status = {'state': 'starting', 'done': 0, 'total': 0}
    archive_data = {item['rel']: item for item in receipts(archive).values()}
    manifest = json.loads((archive / 'manifest.json').read_text())
    archive_root = Path(manifest['archive_root'])
    verification_status = None
    verification_data = {}
    if verification is not None:
        verification_status = json.loads((verification / 'status.json').read_text())
        verification_data = {item['rel']: item for item in receipts(verification).values()}
    stale_status = None
    stale_data = {}
    if stale_retry is not None:
        stale_status = json.loads((stale_retry / 'status.json').read_text())
        stale_data = {item['rel']: item for item in receipts(stale_retry).values()}
    conflicts, errors, children = [], [], []
    links = collections.Counter()
    for item in data.values():
        result = item['result']
        if item['category'] == 'error':
            errors.append({'historical_category': 'error', 'original_job': item['job'], 'relative_path': item['rel'],
                           'retry_node': item['node'], 'result': result['result'],
                           'directories': result.get('directories', 0), 'files': result.get('files', 0),
                           'copied': result.get('copied', 0), 'skipped': result.get('skipped', 0),
                           'manual': result.get('manual', 0), 'receipt_path': item['receipt_path'],
                           'receipt_offset': item['receipt_offset']})
            for row in result.get('receipts', []):
                children.append({'root': item['rel'], 'relative_path': row['rel'], 'status': row['status'],
                                 'sha256': row.get('sha256', ''),
                                 'logical_TB': row.get('source_identity', {}).get('size', 0) / 1e12,
                                 'node': item['node']})
            continue
        if result['result'] == 'manual_different_link':
            links[result.get('link_comparison', 'unknown')] += 1
        saved = archive_data.get(item['rel'])
        verified = verification_data.get(item['rel'])
        stale = stale_data.get(item['rel'])
        saved_result = saved['result'] if saved else {}
        saved_receipt = saved_result.get('receipt', {})
        saved_status = saved_result.get('result', 'not_selected_directory' if
                                        result['result'] == 'manual_both_directories' else 'pending')
        archived = saved_status in ('archived_source', 'manual_source_changed_since_review', 'manual_metadata_warning')
        inferred_kind = ('symlink' if result['result'] in ('manual_different_link', 'same_link_text') else
                         'directory' if result['result'] == 'manual_both_directories' else 'file')
        conflicts.append({'relative_path': item['rel'], 'source_path': '/share-mixed/projset/' + item['rel'],
                          'target_path': '/share-mixed/project/' + item['rel'],
                          'current_comparison': result['result'],
                          'source_type': result.get('source_kind', inferred_kind),
                          'target_type': result.get('target_kind', inferred_kind),
                          'source_TB': result['source_size'] / 1e12 if 'source_size' in result else '',
                          'target_TB': result['target_size'] / 1e12 if 'target_size' in result else '',
                          'source_sha256_at_review': result.get('source_sha256', ''),
                          'target_sha256_at_review': result.get('target_sha256', ''),
                          'source_link_target': result.get('source_link', ''),
                          'target_link_target': result.get('target_link', ''),
                          'link_comparison': result.get('link_comparison', ''),
                          'stale_directory_retry': stale['result']['result'] if stale else '',
                          'archive_status': saved_status,
                          'archived_source_path': str(archive_root / item['rel']) if archived else '',
                          'archived_source_sha256': saved_receipt.get('sha256', ''),
                          'independent_verify_status': verified['result']['result'] if verified else '',
                          'independent_verify_node': verified['node'] if verified else '',
                          'review_node': item['node'], 'archive_node': saved.get('node', '') if saved else '',
                          'original_reason': item['reason'], 'original_job': item['job'],
                          'receipt_path': item['receipt_path'], 'receipt_offset': item['receipt_offset']})
    for item in stale_data.values():
        result = item['result']
        errors.append({'historical_category': 'conflict_stale_directory', 'original_job': item['job'],
                       'relative_path': item['rel'], 'retry_node': item['node'], 'result': result['result'],
                       'directories': result.get('directories', 0), 'files': result.get('files', 0),
                       'copied': result.get('copied', 0), 'skipped': result.get('skipped', 0),
                       'manual': result.get('manual', 0), 'receipt_path': item['receipt_path'],
                       'receipt_offset': item['receipt_offset']})
        for row in result.get('receipts', []):
            children.append({'root': item['rel'], 'relative_path': row['rel'], 'status': row['status'],
                             'sha256': row.get('sha256', ''),
                             'logical_TB': row.get('source_identity', {}).get('size', 0) / 1e12,
                             'node': item['node']})
    conflicts.sort(key=lambda row: row['relative_path']); errors.sort(key=lambda row: row['relative_path'])
    children.sort(key=lambda row: row['relative_path'])
    if (len(conflicts) != status['conflicts_total'] or
            sum(row['historical_category'] == 'error' for row in errors) != status['errors_total']):
        raise ValueError('unexpected receipt categories')
    files = {'conflicts-current.csv': conflicts, 'retried-errors.csv': errors,
             'retried-error-children.csv': children}
    for name, rows in files.items():
        if rows:
            atomic(review / name, csv_text(list(rows[0]), rows))
    cats = collections.Counter(row['current_comparison'] for row in conflicts)
    archives = collections.Counter(row['archive_status'] for row in conflicts)
    ph2d = sum(row['relative_path'].startswith('eai_pwm/datasets/PH2D/.cache/') and
               row['relative_path'].endswith('.metadata') for row in conflicts)
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec='seconds')
    summary = {'updated': time.time(), 'updated_beijing': now, 'review': str(review),
               'archive': str(archive), 'archive_root': str(archive_root),
               'conflicts': len(conflicts), 'categories': dict(cats), 'link_categories': dict(links),
               'ph2d_metadata': ph2d, 'retry_error_roots': len(errors), 'retry_children': len(children),
               'archive_state': archive_status['state'], 'archive_done': archive_status['done'],
               'archive_total': archive_status['total'], 'archive_results': dict(archives),
               'excluded_prefixes': manifest['excluded_prefixes']}
    if stale_status is not None:
        summary.update(stale_retry=str(stale_retry), stale_retry_state=stale_status['state'],
                       stale_retry_done=stale_status['done'], stale_retry_total=stale_status['total'],
                       stale_retry_results=stale_status['results'])
    if verification_status is not None:
        summary.update(verification=str(verification), verification_state=verification_status['state'],
                       verification_done=verification_status['done'], verification_total=verification_status['total'],
                       verification_results=verification_status['results'])
    atomic(review / 'report-summary.json', json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    lines = ['# 历史冲突与错误处理', '', '更新时间：' + now, '',
             '## 历史错误', '',
             f"- 重试目录 {len(errors)} 个（含 {len(stale_data)} 条原归类为冲突的失效句柄），扫描到子项 {len(children)} 个；逐项结果见 [重试目录](retried-errors.csv) 和 [子项回执](retried-error-children.csv)。",
             '- 已验证结果反映校验时状态，在线源不是原子快照；原始历史错误计数不会被改写。',
             '', '## 冲突复核', '',
             f"- 原始冲突 {len(conflicts)} 条：[逐项比较、源/目标和归档路径](conflicts-current.csv)。",
             f"- 文件大小相同但 SHA-256 不同：{cats['different_sha256']}；大小不同：{cats['different_size']}。",
             f"- 符号链接文本不同：{cats['manual_different_link']}；其中指向源/目标对应相对路径 {links['corresponding_relative_paths_content_unverified']}，指向不同路径 {links['different_resolved_paths_content_unverified']}。两侧目录内容没有递归比较。",
             f"- 其他：源/目标类型不同 {cats['manual_different_type']}；两侧均为目录 {cats['manual_both_directories']}，其失效句柄后续重试见下文。",
             f"- 其中 PH2D 的 .cache 下 .metadata 文件 {ph2d} 条；是否可以丢弃差异应由数据负责人确认。",
             '', '## 源版本独立保存', '',
             f"- 状态：{archive_status['state']}；已处理 {archive_status['done']} / {archive_status['total']}。",
             f'- 源版本根目录：`{archive_root}`；各路径保留其原相对路径。',
             '- 现有目标版本保留原位；源文件和链接在隔离目录使用不覆盖发布。链接只保留目标文本，不跟随链接复制其指向的内容。',
             '- 归档中的相对链接可能无法在隔离目录解析；链接文本记录在清单中。',
             '- 若源版本在复核后变化、归档位置碰撞或元数据处理失败，逐项标记为需关注；不自动决定原位置应使用哪个版本。',
             '- 独立目录权限为 0700；未将用户排除的 `eai_pwm/datasets/Sekai` 纳入。',
             '']
    if verification_status is not None:
        lines += ['## 归档独立校验', '',
                  f"- 状态：{verification_status['state']}；已校验 {verification_status['done']} / {verification_status['total']} 项。",
                  '- 校验分配到与归档时不同的节点；文件回读并比对 SHA-256，链接比对文本。', '']
    if stale_status is not None:
        lines += ['## 历史冲突中的 NFS 失效句柄', '',
                  f"- 原归类为冲突的目录失效句柄已重新扫描：{stale_status['done']} / {stale_status['total']}；结果 {stale_status['results']}。",
                  '- 其子项结果包含在重试目录和子项回执 CSV 中，不据此推断在线源之后不会变化。', '']
    lines += ['以上结果不代表已完成全量增量补扫，也不替代读取环境中的链接兼容检查。', '']
    atomic(review / 'ATTENTION-REPORT.md', '\n'.join(lines))
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--review', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--verification', type=Path)
    parser.add_argument('--stale-retry', type=Path)
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--interval', type=int, default=60)
    args = parser.parse_args()
    if args.interval < 60:
        parser.error('minimum report refresh interval is 60 seconds')
    with (args.review / 'report-watcher.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        while True:
            try:
                summary = export(args.review, args.archive, args.verification, args.stale_retry)
                print(json.dumps(summary, ensure_ascii=False), flush=True)
                if not args.watch or (summary['archive_state'] == 'finished' and
                                      (args.verification is None or summary.get('verification_state') == 'finished') and
                                      (args.stale_retry is None or summary.get('stale_retry_state') == 'finished')):
                    return
            except (OSError, ValueError, KeyError) as error:
                print(json.dumps({'state': 'retrying', 'error_type': type(error).__name__,
                                  'updated': time.time()}), flush=True)
                if not args.watch:
                    raise
            time.sleep(args.interval)


if __name__ == '__main__':
    main()
