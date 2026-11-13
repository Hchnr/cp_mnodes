#!/usr/bin/env python3
"""Incrementally export persisted non-copied outcomes; never change source/target.

Receipts are read to a bounded prefix. Each selected record is keyed by receipt path
and byte offset, making interrupted checkpoint recovery idempotent. No hashes
are recalculated and no data files are copied, updated, or removed.
"""
import argparse
import collections
import csv
import datetime
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO/'src/core'))
import migration_linux_open as native

STATUSES = ('skipped','conflict','changed','error','unsupported')
SOURCE = '/share-mixed/projset'
TARGET = '/share-mixed/project'


def stamp(ts=None):
    return datetime.datetime.fromtimestamp(time.time() if ts is None else ts,
        datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec='seconds')


def atomic(path, value):
    tmp = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with tmp.open('x') as f:
        json.dump(value,f,ensure_ascii=True);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)


def anchored_stat(root, rel):
    import stat
    try:
        fd,name=native.open_parent_readonly(root,rel)
        try:
            s=os.stat(name,dir_fd=fd,follow_symlinks=False)
            kind='file' if stat.S_ISREG(s.st_mode) else 'symlink' if stat.S_ISLNK(s.st_mode) else 'directory' if stat.S_ISDIR(s.st_mode) else 'special'
            return dict(kind=kind,bytes=s.st_size,link=os.readlink(name,dir_fd=fd) if kind=='symlink' else '',error='')
        finally:os.close(fd)
    except Exception as e:
        return dict(kind='unavailable',bytes=None,link='',error=type(e).__name__+': '+str(e))


def bounded_lines(path,start,end):
    with path.open('rb') as f:
        f.seek(start);offset=start
        while offset<end:
            line=f.readline(end-offset)
            if not line.endswith(b'\n'):break
            yield offset,line
            offset+=len(line)


def csv_write(path,fields,rows):
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with tmp.open('x',newline='',encoding='utf-8-sig') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
        f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)


def reconcile_errors(run, out):
    """Update derived resolution evidence without interrupting the main export."""
    path = Path(__file__).resolve().with_name('projset_error_reconciliation.py')
    if not path.is_file():
        return False
    try:
        spec = importlib.util.spec_from_file_location('projset_error_reconciliation', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        summary = module.reconcile(run, out, out/'reconciliation')
        atomic(out/'reconciliation-health.json', dict(state='updated', updated=time.time(),
                                                        historical_records=summary['historical_records'],
                                                        attention_records=summary['attention_records']))
        return True
    except Exception as error:
        atomic(out/'reconciliation-health.json', dict(state='error', updated=time.time(),
                                                        error_type=type(error).__name__))
        return False


def reconcile_source_changes(run, out):
    """Associate historical source changes with verified bounded retries."""
    path = Path(__file__).resolve().with_name('projset_changed_reconciliation.py')
    if not path.is_file():
        return False
    try:
        spec = importlib.util.spec_from_file_location('projset_changed_reconciliation', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        summary = module.reconcile(run, out, out/'changed-reconciliation', native, SOURCE, TARGET)
        atomic(out/'changed-reconciliation-health.json', dict(state='updated', updated=time.time(),
               historical_records=summary['historical_records'], attention_records=summary['attention_records']))
        return True
    except Exception as error:
        atomic(out/'changed-reconciliation-health.json', dict(state='error', updated=time.time(),
               error_type=type(error).__name__))
        return False


def export(run,out):
    out.mkdir(parents=True,exist_ok=True)
    statepath=out/'checkpoint.json';catalog=out/'selected-receipts.jsonl'
    state=json.loads(statepath.read_text()) if statepath.exists() else {'offsets':{}}
    selected={}
    if catalog.exists():
        valid_end=0
        with catalog.open('rb') as f:
            for line in f:
                if not line.endswith(b'\n'):break
                row=json.loads(line);selected[(row['_receipt'],row['_offset'])]=row
                valid_end=f.tell()
        # Repair only our derived catalog's torn tail; copy receipts are read-only.
        if catalog.stat().st_size>valid_end:
            with catalog.open('r+b') as f:f.truncate(valid_end)
    new=0;scanned=0
    with catalog.open('ab',buffering=1048576) as dest:
        for path in sorted((run/'nodes').glob('**/receipts.jsonl')):
            node=path.relative_to(run/'nodes').parts[0];s=path.stat();offset_key=node if path.parent.name==node else str(path)
            old=state['offsets'].get(offset_key,state['offsets'].get(node,{'offset':0,'ino':s.st_ino}) if path.parent.name==node else {'offset':0,'ino':s.st_ino})
            if old['ino']!=s.st_ino or s.st_size<old['offset']:raise RuntimeError('receipt identity/truncation differs: '+node)
            pos=old['offset'];end=s.st_size
            for offset,line in bounded_lines(path,pos,end):
                scanned+=1;pos=offset+len(line)
                if b'"status":"copied"' in line and b'"metadata_warnings"' not in line:continue
                row=json.loads(line)
                if row['status'] not in STATUSES and not row.get('metadata_warnings'):continue
                key=(str(path),offset)
                if key not in selected:
                    row.update(_node=node,_offset=offset,_receipt=str(path));selected[key]=row
                    dest.write(json.dumps(row,ensure_ascii=True,separators=(',',':')).encode()+b'\n');new+=1
            state['offsets'][offset_key]=dict(offset=pos,ino=s.st_ino,captured_size=end)
            dest.flush();os.fsync(dest.fileno());atomic(statepath,state)
        dest.flush();os.fsync(dest.fileno())
    fields=['status','kind','node','relative_path','source_path','target_path','recorded_source_bytes','recorded_source_TB',
        'source_current_kind','source_current_bytes','source_current_TB','source_link_target','source_stat_error',
        'target_current_kind','target_current_bytes','target_current_TB','target_link_target','target_stat_error',
        'reason','metadata_warnings','result_time_beijing','sha256','job','receipt_path','receipt_offset']
    rows=[];groups={};summary={};attention=[]
    for row in selected.values():
        rel=row.get('rel');status=row['status'];size=row.get('logical_bytes')
        d=dict(status=status,kind=row.get('kind','unknown'),node=row['_node'],relative_path=rel or '',
            source_path=SOURCE+'/'+rel if rel else '',target_path=TARGET+'/'+rel if rel else '',
            recorded_source_bytes=size,recorded_source_TB=f'{size/1e12:.12f}' if size is not None else '',
            reason=row.get('detail',''),metadata_warnings=json.dumps(row.get('metadata_warnings',[]),ensure_ascii=False),
            result_time_beijing=stamp(row['finished']) if row.get('finished') else '',sha256=row.get('sha256',''),
            job=row.get('job',''),receipt_path=row['_receipt'],receipt_offset=row['_offset'])
        # Only exceptions are re-statted; successful skips retain their verified receipt snapshot.
        if status!='skipped' and rel:
            for prefix,root in [('source',SOURCE),('target',TARGET)]:
                v=anchored_stat(root,rel);d.update({prefix+'_current_kind':v['kind'],prefix+'_current_bytes':v['bytes'],
                    prefix+'_current_TB':f"{v['bytes']/1e12:.12f}" if v['bytes'] is not None else '',
                    prefix+'_link_target':v['link'],prefix+'_stat_error':v['error']})
        rows.append(d)
        c=summary.setdefault(status,dict(records=0,recorded_source_bytes=0,regular_file_bytes=0,symlink_text_bytes=0,unknown_size_records=0))
        c['records']+=1;c['recorded_source_bytes']+=size or 0;c['unknown_size_records']+=size is None
        if row.get('kind')=='file':c['regular_file_bytes']+=size or 0
        elif row.get('kind')=='symlink':c['symlink_text_bytes']+=size or 0
        group='/'.join((rel or '(no path)').split('/')[:3]);g=groups.setdefault((status,group),dict(status=status,path_prefix=group,records=0,recorded_source_bytes=0))
        g['records']+=1;g['recorded_source_bytes']+=size or 0
        if status!='skipped' or row.get('metadata_warnings'):attention.append(d)
    rows.sort(key=lambda x:(x['status'],x['relative_path'],x['result_time_beijing']))
    for status in STATUSES:
        csv_write(out/(status+'-paths.csv'),fields,(x for x in rows if x['status']==status))
    csv_write(out/'attention-paths.csv',fields,attention)
    reconciliation_updated = reconcile_errors(run, out)
    changed_reconciliation_updated = reconcile_source_changes(run, out)
    grouped=sorted(groups.values(),key=lambda x:(x['status'],-x['recorded_source_bytes'],x['path_prefix']))
    for g in grouped:g['recorded_source_TB']=g['recorded_source_bytes']/1e12
    csv_write(out/'directory-summary.csv',['status','path_prefix','records','recorded_source_bytes','recorded_source_TB'],grouped)
    current=json.loads((run/'status.json').read_text());now=time.time()
    data=dict(updated=now,updated_beijing=stamp(now),run=str(run),source=SOURCE,target=TARGET,source_of_truth='durable receipt file prefixes; non-atomic live snapshot',
        statuses=summary,current_controller_counts=current['counts'],controller_updated=current['updated'],new_selected_records=new,scanned_receipt_lines=scanned,
        automatic_retry=False,overwrite_conflicts=False,refresh_seconds=120)
    atomic(out/'summary.json',data)
    lines=['# 跳过与未成功复制清单','',f'更新时间：{stamp(now)}；每120秒增量更新。',
        '',f'源：`{SOURCE}`；目标：`{TARGET}`。',
        '', '按持久化回执的固定字节前缀导出，源目录及复制仍在线；不是原子快照。数据量按记录时源逻辑大小计量，1 TB = 10^12 bytes。',
        '', '| 类别 | 记录数 | 记录时源容量（TB） | 普通文件容量（TB） | 完整路径清单 |', '|---|---:|---:|---:|---|']
    labels={'skipped':'目标已有相同：成功复用','conflict':'目标冲突：保留目标','changed':'源变化：未完成本次复制','error':'历史处理错误：查看复核','unsupported':'特殊对象：未复制'}
    for status in STATUSES:
        c=summary.get(status,dict(records=0,recorded_source_bytes=0,regular_file_bytes=0))
        lines.append(f"| {labels[status]} | {c['records']:,} | {c['recorded_source_bytes']/1e12:.12f} | {c['regular_file_bytes']/1e12:.12f} | [{status}-paths.csv]({status}-paths.csv) |")
    lines+=['','- [历史异常路径及当前两侧属性](attention-paths.csv)；[按目录汇总](directory-summary.csv)。后续处理结果以复核清单为准。',
        '- CSV 包含源/目标绝对路径、记录时源字节/TB、节点、任务号、原因、回执位置。异常条目额外查询当前两侧类型、大小和链接目标。',
        '- skipped 是已经核对内容相同的成功复用，不等于漏拷；没有重新读取这些路径的数据或重新计算哈希。',
        '- symlink 的容量只是链接文本长度，**不是链接所指向整棵数据的容量**，不能把几十个链接冲突视作只有几KB数据受影响。',
        '- 记录数不保证是唯一文件数；目录/扫描错误也可记为结果，元数据警告可能和成功结果重叠。',
        '', '## 是否自动重试或更新',
        '', '原始回执保持不变；恢复后的回执追加到 `nodes/<node>/attempt-*/receipts.jsonl` 和 `issues.jsonl`；本报告和CSV现在每120秒增量更新，检查 summary.json 的 updated_beijing 确认新鲜度。',
        '', '控制器和连接故障会由独立守护进程自动恢复未确认任务；已记账异常不会自动重新入队，也不会自动覆盖目标或自动执行第二轮更新。显式清单补拷的后续结果以对应回执和复核清单为准。',
        '', '建议人工处理顺序：先看 error 的瞬态存储错误能否重新读取，再看 changed 的在线日志/进度文件是否已稳定；conflict 需决定保留哪一侧版本。此报告只读检查，没有代替你做覆盖、删除或重试。',
        '', '## 目标已有相同内容：容量最大的目录分组','', '| 相对路径前3层 | 记录数 | 容量（TB） |','|---|---:|---:|']
    for g in [x for x in grouped if x['status']=='skipped'][:15]:lines.append(f"| `{g['path_prefix'].replace('|','&#124;')}` | {g['records']:,} | {g['recorded_source_bytes']/1e12:.9f} |")
    if reconciliation_updated:
        lines += ['', '[历史错误的后续处理与待关注清单](reconciliation/REPORT.md)；该清单不改变原始回执或累计计数。']
    if changed_reconciliation_updated:
        lines += ['', '[历史源变化的后续处理与待关注清单](changed-reconciliation/REPORT.md)；校验和当前属性的检查均不构成在线快照。']
    lines+=['','## 历史异常条目','', '下表为原始异常记录；后续可能已有单独处理，具体结果请查看复核清单。源绝对路径为 /share-mixed/projset/ + 相对路径，目标绝对路径为 /share-mixed/project/ + 相对路径。CSV提供两侧完整路径与当前属性。',
        '', '| 类别 | 相对路径 | 类型 | 记录时源容量（TB） | 原因 |','|---|---|---|---:|---|']
    for d in sorted(attention,key=lambda x:(x['status'],x['relative_path'])):
        clean=lambda x:str(x).replace('|','&#124;').replace('\n',' ')
        lines.append(f"| {d['status']} | `{clean(d['relative_path'])}` | {d['kind']} | {d['recorded_source_TB']} | {clean(d['reason'])} |")
    tmp=out/'REPORT.md.tmp';tmp.write_text('\n'.join(lines)+'\n');os.replace(tmp,out/'REPORT.md')
    print(json.dumps(dict(updated_beijing=stamp(now),new_selected=new,scanned_lines=scanned,statuses=summary)),flush=True)


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--watch',action='store_true');p.add_argument('--interval',type=int,default=120);a=p.parse_args()
    if a.interval<120:p.error('minimum interval 120 seconds')
    os.nice(10);a.output.mkdir(parents=True,exist_ok=True)
    lock=open('/tmp/'+a.run.name+'-outcomes.lock','a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    atomic(a.output/'launch.json',dict(pid=os.getpid(),host=__import__('socket').gethostname(),argv=sys.argv,started=time.time()))
    while True:
        try:export(a.run,a.output)
        except Exception as e:
            atomic(a.output/'export-error.json',dict(updated=time.time(),error_type=type(e).__name__,detail=str(e)))
            if not a.watch:raise
        if not a.watch:return
        time.sleep(a.interval)


if __name__=='__main__':main()
