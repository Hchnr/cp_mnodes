"""Read shared scanner evidence without treating SSH failure as zero progress."""
import collections
import json
import os
from pathlib import Path
import time


def read(path):
    path = Path(path)
    for attempt in range(3):
        try:
            return json.loads(path.read_text())
        except FileNotFoundError:
            if attempt == 2:
                raise
            os.listdir(path.parent)


def returned(record, result):
    """The actual core prints this footer only after log fsync and DB close.

    This proves the scan run returned; it does not assert an observed /proc PID
    state or replace the independent final frontier/ownership audit.
    """
    if (result.get('status') != 'complete' or result.get('pending_directories') != 0
            or result.get('active_directories') != 0):
        return False
    path = Path(record['log_file'])
    try:
        with open(path, 'rb') as stream:
            stream.seek(max(0, path.stat().st_size - 512 * 1024))
            lines = stream.read().splitlines()
        footer = json.loads(lines[-1])
        return (footer['status'] == 'complete' and footer['updated'] == result['updated']
                and footer['counts'] == result['counts'] and footer['pid'] == record['identity']['pid']
                and footer['source'] == '/share/project' and footer['target'] == '/share-mixed/project'
                and footer['payload_bytes_read'] == footer['payload_bytes_written'] == 0)
    except (OSError, ValueError, IndexError, KeyError):
        return False


def snapshot(root, closed_only=False):
    root = Path(root)
    registry = read(root / 'launch-results.json')
    counts, rows = collections.Counter(), []
    for record in registry['nodes']:
        folder = Path(record['report_directory'])
        try:
            result = read(folder / 'result.json')
        except FileNotFoundError:
            if closed_only:
                rows.append(dict(node=record['node'],instance=record['instance'],error='awaiting immutable result'))
                continue
            try:
                result = read(folder / 'progress.json')
            except FileNotFoundError:
                rows.append(dict(node=record['node'], instance=record['instance'], error='no shared report'))
                continue
        if (result['source'] != '/share/project' or result['target'] != '/share-mixed/project'
                or result['excluded'] != ['eai_pwm/datasets/raw']
                or result['pid'] != record['identity']['pid']):
            raise ValueError('shared report scope/identity differs')
        counts.update(result['counts'])
        rows.append(dict(node=record['node'], instance=record['instance'], report=result,
            report_directory=str(folder), directory=record['directory'], log_file=record['log_file'],
            age=time.time()-result['updated'], run_returned=returned(record,result),
            process_observed=False))
    complete = (bool(rows) and not registry.get('transfers_pending')
                and not registry.get('repairs_pending') and all(row.get('run_returned') for row in rows))
    return dict(updated=time.time(), counts=dict(counts), nodes=rows,
        enumeration_complete=complete, evidence='shared report + actual post-close return footer',
        remote_process_states_observed=False, transfers_pending=registry.get('transfers_pending',False))


def render(root):
    import datetime
    root = Path(root)
    view = snapshot(root)
    c = view['counts']
    print('\n全范围复核 | '+datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M:%S 北京时间'))
    print('已检查 {:,} 条；缺失 {:,} 文件 / {:.3f} TiB（未复核去重的扫描观察量）'.format(
        c.get('entries',0), c.get('missing_files',0), c.get('missing_logical_bytes',0)/1024**4))
    print('缺目录 {:,}；缺链接 {:,}；大小不一致 {:,}；访问错误 {:,}；变化目录 {:,}'.format(
        c.get('missing_directories',0),c.get('missing_symlinks',0),c.get('size_mismatches',0),
        c.get('errors',0),c.get('changed_source_directories',0)))
    print('已返回扫描 {}/{}；全量枚举结束：{}；当前显示共享报告，不将 SSH 失败记成零。'.format(
        sum(row.get('run_returned',False) for row in view['nodes']),len(view['nodes']),view['enumeration_complete']))
    print('\n节点       | 扫描中/已返回 | 已检查条目   | 待扫目录 | 最近扫描心跳')
    for node in ['master-0']+['worker-'+str(i) for i in range(11)]:
        rows=[r for r in view['nodes'] if r['node']==node]
        live=[r for r in rows if r.get('report',{}).get('status')=='scanning']
        age=min((r['age'] for r in live),default=None)
        print('{:<10} | {:>13} | {:>12,} | {:>8,} | {}'.format(node,
            str(len(live))+'/'+str(sum(r.get('run_returned',False) for r in rows)),
            sum(r.get('report',{}).get('counts',{}).get('entries',0) for r in rows),
            sum(r.get('report',{}).get('pending_directories',0) for r in rows),
            '-' if age is None else '{:.0f}s'.format(age)))
    print('raw 已排除；完整内容哈希未宣称完成。控制与日志：'+str(root))
