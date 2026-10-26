#!/usr/bin/env python3
"""Read migration evidence without touching the copy pipeline or source data.

Produces a clearly labelled interim HTML report and portable JSON/CSV evidence.
Run again after final verification to refresh it; terminal state alone does not
constitute verification or turn this interim report into an acceptance report.
"""
import argparse
import csv
import datetime as dt
import html
import io
import json
import os
from pathlib import Path
import sys

import migration_launch as transport
from migration_io import read_json

BASE = Path(__file__).resolve().parent.parent
FIELDS = ('run_id', 'control', 'source', 'destination', 'job', 'created',
          'resume_from', 'threads', 'nodes', 'io_bytes_per_second',
          'copy_threads_per_node', 'supplement_threads', 'supplement_io_bytes_per_second',
          'broker_fs_concurrency',
          'scan_entries_per_second', 'parallel_scan_threads', 'scan_batch_files',
          'parallel_target_threads', 'distributed_target_threads', 'target_probe_slots_per_node',
          'target_filter_inflight', 'target_probe_endpoints')

REMOTE = r'''
import datetime,json,pathlib,re,sys
control=pathlib.Path(CONTROL)
sys.path.insert(0,str(control))
from migration_io import read_json
from migration_nodes import load_nodes
fields=FIELDS
warnings=[]
def load(path):
    try: return read_json(path)
    except (OSError,ValueError,TypeError) as exc:
        warnings.append(str(path)+': '+type(exc).__name__)
        return None
def optional(path):
    if path.exists() or (path.parent/'.snapshots'/path.name).exists():
        return load(path)
    return None
def clean(value):
    if isinstance(value,dict):
        return {k:clean(v) for k,v in value.items() if not any(s in k.lower() for s in
                  ('token','password','secret','authorization','credential'))}
    if isinstance(value,list): return [clean(v) for v in value]
    return value
def versions(path,history=False):
    root=path.parent/'.snapshots'/path.name
    if not root.is_dir(): return []
    found=[]
    for p in root.iterdir():
        if p.is_file() and re.fullmatch(r'[0-9]{20}-[0-9a-f]{32}\.json',p.name):
            found.append(p)
        elif p.is_dir() and re.fullmatch(r'[0-9]{10}',p.name):
            minutes=sorted(q for q in p.iterdir() if q.is_dir() and re.fullmatch(r'[0-5][0-9]',q.name))
            selected=minutes
            if history and minutes:
                # Each selected snapshot contains a 15-minute sample window.
                selected=[q for i,q in enumerate(minutes)
                          if i in (0,len(minutes)-1) or int(q.name)%10==0]
            for minute in selected:
                entries=sorted(q for q in minute.iterdir()
                               if re.fullmatch(r'[0-9]{20}-[0-9a-f]{32}\.json',q.name))
                found.extend(entries[-1:] if history else entries)
    return sorted(found,key=lambda p:p.name)
runs=[];seen=set();root=control
for _ in range(16):
    if str(root) in seen:
        warnings.append('resume chain cycle: '+str(root));break
    seen.add(str(root))
    cfg=load(root/'config.json')
    if not isinstance(cfg,dict): break
    records={}
    for name in ('state.json','progress.json','metrics.json','inventory.json',
                 'pipeline.json','training-health.json','scale-upgrade.json',
                 'pipeline-deployment.json','resume-index.json','broker-parallel-deployment.json',
                 'state-cache-deployment.json','state-cache-canary.json','state-cache-post-profile.json',
                 'state-cache-broker-probe.json','state-cache-health.json','state-cache-summary.json',
                 'recovery-deployment.json','fresh-bootstrap.json','seed-progress.json',
                 'parallel-seed-deployment.json','monitor-recovery-deployment.json',
                 'new-cluster-recovery-health.json',
                 'double-readers-deployment.json','double-concurrency-deployment.json',
                 'double-worker-launch.json','double_concurrency_deployment.json',
                 'double_worker_launch.json','extension-plan.json',
                 'performance.json','performance-watch-deployment.json','distributed-monitor-deployment.json',
                 'distributed-target-services-deployment.json','distributed-target-filter-deployment.json',
                 'distributed-target-optimization-samples.json'):
        value=optional(root/name)
        if value is not None: records[name]=value
    events=[];event_path=root/'events.jsonl'
    if event_path.exists():
        with open(event_path,encoding='utf-8') as handle:
            for index,line in enumerate(handle):
                if index>=20000:
                    warnings.append(str(event_path)+': event export capped at 20000 lines');break
                try:events.append(json.loads(line))
                except ValueError:warnings.append(str(event_path)+': incomplete/invalid line '+str(index+1))
    config_versions=[]
    for path in versions(root/'config.json'):
        value=load(path)
        if isinstance(value,dict):
            config_versions.append({'snapshot':str(path),'config':{k:value[k] for k in fields if k in value}})
    points={}
    metric_paths=versions(root/'metrics.json',history=True)
    metric_values=[load(path) for path in metric_paths]
    metric_values.append(records.get('metrics.json'))
    for metric in metric_values:
        if not isinstance(metric,dict):continue
        for point in metric.get('samples',metric.get('history',[])):
            if isinstance(point,dict) and isinstance(point.get('sample_time'),(int,float)):
                points[point['sample_time']]=point
        if isinstance(metric.get('sample_time'),(int,float)):
            point={k:metric[k] for k in ('sample_time','nodebytes','copied_bytes','streamed_bytes',
                   'verified_files','verified_bytes','completed_new_files','completed_new_bytes') if k in metric}
            points[point['sample_time']]=dict(points.get(point['sample_time'],{}),**point)
    nodes=[]
    for value in load_nodes(root,warnings=warnings):
        if isinstance(value,dict):
            node={k:value[k] for k in ('host','pid','started','run_started','updated','ended',
                  'threads','io_bytes_per_second','bytes','streamed_bytes','counts','stages',
                  'verified_files','verified_bytes','last_error','thread_error','tuning_error',
                  'inspection_warning','components','pids','latest_component_updated') if k in value}
            node['active_threads']=len(value.get('active',{}))
            nodes.append(node)
    results={}
    for path in sorted((root/'recovery-roles').glob('*.json')):
        records[str(path.relative_to(root))]=load(path)
    for path in sorted((root/'seed-handovers').glob('*.json')):
        records[str(path.relative_to(root))]=load(path)
    for path in sorted((root/'pipeline-results').glob('*.json')):
        results[str(path.relative_to(root))]=load(path)
    metadata={}
    for path in sorted((root/'metadata').glob('*')):
        if path.is_dir():
            values={name:optional(path/name) for name in ('progress.json','result.json')}
            if any(v is not None for v in values.values()):metadata[path.name]=values
    # Export issue path/size only; the full immutable issue files remain remote.
    issues=[]
    for pattern in ('*-issues.jsonl','metadata/*/*issues*.jsonl'):
        for path in sorted(root.glob(pattern)):
            issues.append({'path':str(path),'bytes':path.stat().st_size})
    runs.append({'control':str(root),'config':{k:cfg[k] for k in fields if k in cfg},
                 'config_versions':config_versions,'records':records,'events':events,
                 'nodes':nodes,'pipeline_results':results,'metadata':metadata,
                 'issues':issues,'stop_present':(root/'STOP').exists(),
                 'metric_snapshot_files_read':len(metric_paths),
                 'metric_samples':[points[t] for t in sorted(points)]})
    previous=cfg.get('resume_from')
    if not previous:break
    root=pathlib.Path(previous)
else:warnings.append('resume chain exceeded 16 controls')
print(json.dumps(clean({'collected_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                       'runs':runs,'warnings':warnings}),ensure_ascii=True))
'''


def stamp(value):
    if not value:
        return '尚无记录'
    try:
        when = dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return when.astimezone(dt.timezone(dt.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return str(value)


def size(value):
    if not isinstance(value, (int, float)):
        return '未知'
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'):
        if abs(value) < 1024 or unit == 'PiB':
            return f'{value:,.1f} {unit}'
        value /= 1024


def seconds(value):
    if not isinstance(value, (int, float)):
        return '未单独计时'
    hours, rest = divmod(int(value), 3600)
    minutes, sec = divmod(rest, 60)
    return f'{hours}h {minutes}m {sec}s'


def esc(value):
    return html.escape(str(value))


def sanitize(value):
    if isinstance(value, dict):
        return {k: sanitize(v) for k, v in value.items() if not any(
            secret in k.lower() for secret in ('token', 'password', 'secret', 'authorization', 'credential'))}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    return value


def table(headers, rows):
    return '<table><thead><tr>' + ''.join('<th>' + esc(x) + '</th>' for x in headers) + \
        '</tr></thead><tbody>' + ''.join('<tr>' + ''.join('<td>' + esc(x) + '</td>' for x in row) +
                                      '</tr>' for row in rows) + '</tbody></table>'


def thread_target(config):
    if config.get('copy_threads_per_node') is not None:
        return config['copy_threads_per_node']
    primary, extra = config.get('threads'), config.get('supplement_threads', 0)
    return primary + extra if isinstance(primary, (int, float)) and isinstance(extra, (int, float)) else '尚无记录'


def render(evidence):
    run = evidence['runs'][0]
    records = run['records']
    state = records.get('state.json', {})
    metrics = records.get('metrics.json', {})
    progress = records.get('progress.json', {})
    inventory = records.get('inventory.json', {})
    extension = records.get('extension-plan.json', {})
    extension_html = ''
    if extension.get('priority') == 4:
        extension_html = ('<h2>已追加的后续复制目标</h2>' + table(
            ('优先级', '源', '目标', '状态', '覆盖与计量范围'),
            [(4, extension.get('source'), extension.get('destination'), extension.get('status'),
              '等待前三项全部轮次、校验和元数据收尾；eai_pwm由前三项覆盖，避免重复复制。第4项尚未清点，不计入下方前三项规模；全任务总量与ETA未知。')]))
    phases = []
    names = {'1': 'home/hcr', '2': 'repos/cosmos-framework-pwm', '3': '其余 eai_pwm'}
    for phase, name in names.items():
        live = progress.get('phases', {}).get(phase, {})
        inv = inventory.get('phases', {}).get(phase, {})
        known_files = max(live.get('discovered_files', 0), inv.get('discovered_files', 0))
        known_bytes = max(live.get('discovered_bytes', 0), inv.get('discovered_bytes', 0))
        phases.append((phase + ' · ' + name, f'{known_files:,}', size(known_bytes),
                       '清单完成（在线清点）' if inv.get('inventory_complete') else '已发现下限',
                       f"{live.get('verified_files', 0):,}", size(live.get('verified_bytes', 0))))
    scans = []
    for entry in reversed(evidence['runs']):
        for event in entry['events']:
            if event.get('event') != 'scan_complete':
                continue
            s = event.get('stats', {})
            scans.append((entry['config'].get('run_id'), f"p{event.get('phase')}-r{event.get('round')}",
                          stamp(event.get('time')), f"{s.get('files', 0):,}", size(s.get('bytes')),
                          seconds(s.get('elapsed_seconds')), '完成一轮扫描'))
    timelines = []
    for entry in reversed(evidence['runs']):
        for event in entry['events']:
            timelines.append((stamp(event.get('time')), entry['config'].get('run_id'), event.get('event'),
                              event.get('stage', ''), event.get('status', '')))
    nodes = []
    components = []
    for node in run['nodes']:
        nodes.append((node.get('host'), f"{node.get('active_threads', 0)}/{node.get('threads', '?')}",
                      size(node.get('bytes')), size(node.get('streamed_bytes')),
                      node.get('io_bytes_per_second'),
                      json.dumps(node.get('counts', {}), ensure_ascii=False)))
        for component in node.get('components', []):
            errors = [str(component[k]) for k in ('last_error', 'tuning_error', 'inspection_warning') if component.get(k)]
            components.append((node.get('host'), component.get('lane', component.get('instance', 'primary')),
                               component.get('pid'),
                               f"{component.get('active_threads', 0)}/{component.get('threads', '?')}",
                               component.get('io_bytes_per_second'), stamp(component.get('updated')),
                               ('已结束：' + stamp(component['ended']) if component.get('ended') else '；'.join(errors) or
                                ('thread_error' if component.get('thread_error') else '未报告错误'))))
    config_changes = []
    for entry in reversed(evidence['runs']):
        versions = entry.get('config_versions', []) or [{'config': entry['config'], 'snapshot': ''}]
        previous = None
        for version in versions:
            config = version['config']
            values = (config.get('threads'), config.get('supplement_threads', 0), thread_target(config),
                      config.get('io_bytes_per_second'), config.get('supplement_io_bytes_per_second', 0),
                      (config['io_bytes_per_second'] + config.get('supplement_io_bytes_per_second', 0)
                       if isinstance(config.get('io_bytes_per_second'), (int, float)) else '尚无记录'),
                      config.get('parallel_scan_threads'), config.get('broker_fs_concurrency'))
            if values == previous:
                continue
            previous = values
            try:
                epoch = int(Path(version['snapshot']).name.split('-', 1)[0]) / 1e9
                recorded = dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()
            except (ValueError, KeyError, OverflowError, OSError):
                recorded = config.get('created')
            config_changes.append((entry['config'].get('run_id'), stamp(recorded), *values))
    sample_info = []
    for entry in reversed(evidence['runs']):
        points = entry['metric_samples']
        first = dt.datetime.fromtimestamp(points[0]['sample_time'], dt.timezone.utc).isoformat() if points else None
        last = dt.datetime.fromtimestamp(points[-1]['sample_time'], dt.timezone.utc).isoformat() if points else None
        gaps = sum(b['sample_time'] - a['sample_time'] > 20 for a, b in zip(points, points[1:]))
        sample_info.append((entry['config'].get('run_id'), len(points), stamp(first), stamp(last), gaps))
    issues = [(x['path'], x['bytes']) for entry in evidence['runs'] for x in entry['issues'] if x['bytes']]
    warning_text = ''.join('<li>' + esc(w) + '</li>' for w in evidence['warnings']) or '<li>本次导出未发现读取警告。</li>'
    total_copied = sum(node.get('bytes', 0) for node in run['nodes'])
    content = f'''<h1>全闪到混闪复制报告</h1>
<p class="notice">过程稿 · 任务尚未通过最终验收。不能据此删除旧路径数据。</p>
<p>统计采集：{esc(stamp(evidence['collected_at']))} 北京时间<br>
源：{esc(run['config'].get('source'))}<br>目标：{esc(run['config'].get('destination'))}<br>
当前状态：{esc(state.get('status'))}；阶段：{esc(state.get('stage'))}</p>
{extension_html}
<h2>规模与当前完成量</h2>
{table(('优先级与范围','已发现文件数','已发现逻辑容量','规模口径','当前报告核验文件数','当前报告核验容量'),phases)}
<p>本控制目录节点累计成功新复制：{esc(size(total_copied))}。这是成功载荷的逻辑字节，不能当作唯一源数据总量或物理存储吞吐。
历史恢复副本与本轮核验量存在重叠，不相加；多轮扫描量和核验量也不相加。</p>
<h2>已完成扫描记录</h2>
{table(('运行','阶段轮次','扫描结束（北京）','常规文件数','逻辑容量','扫描历时','说明'),scans)}
<p>扫描和复制并发；elapsed_seconds 是扫描器记录的历时。接管产生的分段扫描需结合交接记录计算完整跨度，不能只取末段。
复制包含源读取、写入、SHA 和目标回读校验，目前没有分别记录每一步的耗时。</p>
<h2>当前节点配置和累计计数</h2>
{table(('物理节点','主辅合计活跃/线程','成功新复制','逻辑写入流量','主辅合计I/O预算（B/s）','状态计数'),nodes)}
<p>节点按物理主机汇总主、辅进程，避免把辅进程当作新增节点。配置 threads 仅表示主进程线程数；
copy_threads_per_node 表示每物理节点的配置总目标。实际合计线程和计数以主辅节点快照为准。</p>
{table(('物理节点','进程通道','PID','活跃/线程','I/O预算（B/s）','心跳更新时间（北京）','结束或错误记录'),components)}
<h2>并发与预算配置变化</h2>
{table(('运行','配置记录（北京）','主进程线程','辅进程线程','每节点总线程目标','主预算（B/s）','辅预算（B/s）','主辅预算合计（B/s）','扫描线程','Broker文件并发'),config_changes)}
<p>主辅 I/O 预算分别列出，不能将主预算误作两进程共同预算；主节点保留的协调器预算不重复计入。
部署记录及主辅 PID、线程、预算、心跳和错误证据保存在 JSON 导出中。</p>
<h2>历史速度采样覆盖</h2>
{table(('运行','样本数','最早（北京）','最晚（北京）','超过20秒的采样缺口数'),sample_info)}
<p>完整采样导出为 CSV；按运行分别计算相邻点差分，计数下降或长采样间隔应标为缺口。
streamed_bytes 含进行中的写入、失败后重写及稀疏逻辑长度，不等于物理磁盘或网络字节。</p>
<h2>阶段事件</h2>{table(('北京时间','运行','事件','阶段','状态'),timelines)}
<h2>非空差异记录位置</h2>{table(('远端证据文件','文件字节数'),issues)}
<p>差异文件非空不等于最终仍有未解决错误；最终按每个范围最新完整轮次、后台收尾和补扫结果判定。
完整逐文件清单、SHA 回执和差异原文仍保留在远端控制目录中，本导出不扫描这些大规模文件集合。</p>
<h2>最终报告待补齐</h2>
<ul><li>三个范围最终唯一文件、目录、软链接、硬链接及容量；明确逻辑容量和实际分配空间口径。</li>
<li>总历时、暂停与恢复区间；各轮扫描、复制校验、后台收尾的跨度，说明重叠和无法独立计时部分。</li>
<li>成功新增复制、历史复用、变化后重拷；平均与分位吞吐、配置变化、采样缺口。</li>
<li>最后全范围补扫时间、源是否停止写入、最新轮真实汇总、目录收尾、队列和节点进程验收证据。</li>
<li>已解决异常与最终未解决差异逐项列明；只有核验完成后才出最终验收结论。</li></ul>
<h2>导出警告</h2><ul>{warning_text}</ul>'''
    return '''<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>全闪到混闪复制报告 · 过程稿</title>
<style>body{font:15px/1.7 system-ui,sans-serif;max-width:1440px;margin:40px auto;padding:0 24px;color:#172636;background:#f7f9fc}
h1{font-size:30px}h2{margin-top:34px;font-size:21px}table{border-collapse:collapse;width:100%;background:white;font-size:13px}
th,td{padding:9px 12px;border:1px solid #dce3eb;text-align:left;overflow-wrap:anywhere}th{background:#eaf0f6}
.notice{border-left:4px solid #c07912;padding:12px 16px;background:#fff3db}p{overflow-wrap:anywhere}
@media print{body{margin:0;background:white}table{font-size:10px}h2{break-after:avoid}tr{break-inside:avoid}}</style>''' + content + '</html>\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=BASE / 'outputs')
    parser.add_argument('--from-json', type=Path, help='render an existing evidence export without SSH')
    args = parser.parse_args()
    os.umask(0o077)
    if args.from_json:
        evidence = json.loads(args.from_json.read_text())
    else:
        cfg = read_json(BASE / 'work' / 'active_migration.json')
        transport.JOB = cfg['job']
        code = REMOTE.replace('CONTROL', repr(cfg['control']), 1).replace('FIELDS', repr(FIELDS), 1)
        _, raw = transport.ssh('master-0', code, owner=cfg.get('ssh_owner'))
        token = cfg.get('broker_token')
        if token:
            raw = raw.replace(token, '[redacted]')
        evidence = json.loads(raw)
        if evidence.get('runs'):
            current = evidence['runs'][0]
            local_records = [BASE / 'work' / name for name in
                             ('double_concurrency_deployment.json', 'double_worker_launch.json')]
            for pattern in ('nfs_control_episode_*.json', 'cache_age_episode_*.json',
                            'performance_incident_*.json'):
                local_records.extend(sorted((BASE / 'work').glob(pattern)))
            for path in local_records:
                if not path.exists():
                    continue
                try:
                    value = read_json(path)
                    if isinstance(value, dict) and value.get('control') == current['control']:
                        current['records']['local:' + path.name] = sanitize(value)
                except (OSError, ValueError, TypeError) as exc:
                    evidence.setdefault('warnings', []).append(str(path) + ': ' + type(exc).__name__)
    if not evidence.get('runs'):
        raise RuntimeError('No run evidence was available; refusing an empty report')
    evidence = sanitize(evidence)
    evidence['report_status'] = 'interim_requires_final_verification'
    evidence['measurement_notes'] = [
        'Logical byte counters are not physical network or device I/O counters.',
        'Repeated verification rounds and resume baselines must not be summed as unique source size.',
        'Scan/copy/verification overlap; no isolated copy-only versus checksum-only timers exist.',
        'Live inventory is not a consistent source snapshot; final acceptance is separate.'
    ]
    target = args.output_dir
    target.mkdir(parents=True, exist_ok=True)
    data_path = target / '混闪复制报告数据.json'
    data_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    report_path = target / '混闪复制报告.html'
    report_path.write_text(render(evidence))
    csv_path = target / '混闪复制速度记录.csv'
    columns = ('run_id', 'sample_time', 'beijing_time', 'copied_bytes', 'streamed_bytes',
               'verified_files', 'verified_bytes', 'completed_new_files', 'completed_new_bytes')
    with csv_path.open('w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for run in reversed(evidence['runs']):
            for point in run['metric_samples']:
                row = {k: point.get(k) for k in columns}
                row['run_id'] = run['config'].get('run_id')
                row['beijing_time'] = stamp(dt.datetime.fromtimestamp(point['sample_time'], dt.timezone.utc).isoformat())
                writer.writerow(row)
    print(json.dumps({'ok': True, 'report': str(report_path), 'data': str(data_path),
                      'csv': str(csv_path), 'runs': len(evidence['runs']),
                      'samples': sum(len(r['metric_samples']) for r in evidence['runs']),
                      'warnings': len(evidence['warnings'])}, ensure_ascii=False))


if __name__ == '__main__':
    main()
