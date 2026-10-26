#!/usr/bin/env python3
"""Small read-only entry for the active full-tree completeness re-audit."""
import argparse
import collections
import datetime
from pathlib import Path
import sys
import time

DEFAULT = Path('/share-mixed/project/.hcr-readonly-audit-20261026-full-0107')


def namespace_postcheck_progress(control,control_read):
    control=Path(control)
    try:launch=control_read(control/'original-postcheck-watchers-v2/launch-results.json')
    except FileNotFoundError:return
    states=collections.Counter()
    for actor in launch['watchers']:
        folder=control/'nodes'/actor['node']/'post-copy-audit-original'
        try:
            try:value=control_read(folder/'result.json')
            except FileNotFoundError:value=control_read(folder/'progress.json')
        except FileNotFoundError:
            states['no_report']+=1;continue
        status=value['status']
        key=('returned' if status.startswith('closed_namespace_') else
             'checking' if status=='checking' else
             'waiting' if status=='waiting_for_actual_original_exit' else 'attention')
        states[key]+=1
    print('目录本地收尾复核：等待退出 {}，核验中 {}，已返回 {}/{}，需检查 {}；返回不等于全部通过。'.format(
        states['waiting'],states['checking'],states['returned'],len(launch['watchers']),
        states['attention']+states['no_report']))


def copy_progress(controls,control_read):
    for kind,location in controls.items():
        folder=Path(location)
        try:cfg=control_read(folder/'config.json')
        except FileNotFoundError:
            try:
                prepared=control_read(folder/'preparation-progress.json')
                print('补拷 {}：已生成分片 {:,}/{:,} 文件。'.format(kind,prepared['files'],prepared['expected_files']))
            except FileNotFoundError:
                print('补拷 {}：准备清单。'.format(kind))
            continue
        counters=collections.Counter();active=reported=closed=0;write_rate=complete_rate=0.0
        for node in cfg['shards']:
            try:report=control_read(folder/'nodes'/node/'progress.json')
            except FileNotFoundError:continue
            reported+=1;counters.update(report['counts'])
            if report['status'].startswith('complete'):closed+=1
            elif time.time()-report['updated']<30:
                active+=report.get('active',0)
                write_rate+=report.get('write_bytes_per_second',0)
                complete_rate+=report.get('complete_bytes_per_second',0)
        title='普通文件' if kind=='files' else '目录/链接'
        print('补拷 {}：处理 {:,}/{:,}；成功 {:,}；跳过 {:,}；变化 {:,}；错误/冲突 {:,}；返回 {}/{} 路。'.format(
            title,counters['processed'],cfg['files'],counters['copied'],counters['skipped'],
            counters['changed'],counters['error']+counters['conflict'],closed,len(cfg['shards'])))
        if kind=='files':
            print('  新复制完成 {:.3f} TiB / 清单 {:.3f} TiB；在途任务 {}；近窗口写入 {:.2f} GiB/s（{:.1f} MiB/s）；确认完成 {:.2f} GiB/s（{:.1f} MiB/s）。'.format(
                counters['copied_bytes']/1024**4,cfg['bytes']/1024**4,active,write_rate/1024**3,write_rate/1024**2,
                complete_rate/1024**3,complete_rate/1024**2))


def copy_view(root,report,control_read):
    now=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%m-%d %H:%M:%S 北京时间')
    files=report['missing']['file']
    print('\n已核实清单补拷 | '+now)
    print('本批范围：{:,} 普通文件 / {:.3f} TiB；{:,} 目录；{:,} 链接。'.format(
        files['paths'],files['logical_bytes']/1024**4,report['missing']['directory']['paths'],report['missing']['symlink']['paths']))
    if report.get('whole_tree_completion_proven') is False:
        print('raw 已排除；全树活跃目录的后续变化仍待收尾，本批完成不等于全树零遗漏。')
    try:state=control_read(root/'conditional-copy-state.json')
    except FileNotFoundError:state=dict(status='等待启动',controls={})
    phases=dict(copy_preparing_files='生成文件分片',copy_preparing_namespace='文件已启动，准备目录/链接分片',
                copy_launch_in_progress='启动复制进程',copy_running='搬运中')
    print('状态：'+phases.get(state['status'],state['status']))
    controls=state.get('controls',{});copy_progress(controls,control_read)
    try:
        runtime=control_read(root/'final/incremental-namespace-closure/runtime-progress.json')
        try:proof=control_read(root/'final/incremental-namespace-closure/verified/report.json')
        except FileNotFoundError:proof={}
        detail=('覆盖证明已回放；仍待主批次结束后复核。'
                if proof.get('status')=='namespace_proof_replayed_new_findings_classified'
                else '覆盖证明及新缺失仍需合并复核。')
        print('完整性收尾：稳定核验 {:,} 目录 / {:,} 条观察；待扫 {}；{}'.format(
            runtime.get('captured_directories',0),runtime.get('observations',0),runtime.get('pending_directories','未知'),detail))
    except FileNotFoundError:
        try:
            closure=control_read(root/'final/incremental-namespace-closure/progress.json')
            print('完整性收尾准备：{:,} 个目录版本已冻结；{} 个变化目录待核验（不是已完成）。'.format(
                closure.get('baseline_directories',0),closure.get('directory_seeds','未知')))
        except FileNotFoundError:pass
    rollout=None
    try:
        pointer=control_read(root/'final/namespace-handoff-rollout.json')
        rollout=control_read(Path(pointer['folder'])/'progress.json')
        labels=dict(validating_remote='校验远端',staging_receiver='准备接收进程',
                    draining_one_source='排空当前分片',verifying_receiver='验证接续计数',
                    one_shard_handed_off='当前分片交接完成',all_handed_off='全部交接完成',
                    inspection_required='交接暂停，待检查')
        print('目录/链接远端交接：试点 1 + 后续 {}/{}；{} {}；普通文件进程不动。'.format(
            len(rollout['completed']),rollout['expected'],rollout.get('node',''),
            labels.get(rollout['status'],rollout['status'])))
    except FileNotFoundError:pass
    try:
        pilot=control_read(root/'final/namespace-handoff-pilot.json')
        folder=Path(pilot['plan']).parent
        receiver=control_read(folder/'receiver-state.json')
        labels=dict(ready_waiting_for_source_exit='远端已就绪，等旧进程排空',
                    copying_verified_suffix='远端接续未处理清单',receiver_returned='远端复制进程已返回')
        if rollout is None:
            print('目录/链接单分片对照 {}：{}；累计计数保留，其他分片不动。'.format(
                pilot['node'],labels.get(receiver['status'],receiver['status'])))
        try:
            try:checked=control_read(folder/'post-copy-audit/result.json')
            except FileNotFoundError:checked=control_read(folder/'post-copy-audit/progress.json')
            if checked['status']=='waiting_for_actual_receiver_exit':
                print('试点只读收尾复核：等待复制进程实际退出。')
            else:
                print('试点只读收尾复核：{}；已检查 {:,}/{:,} 项。'.format(
                    checked['status'],checked.get('counts',{}).get('processed',0),checked.get('expected',0)))
        except FileNotFoundError:pass
    except FileNotFoundError:pass
    try:
        checked=control_read(root/'final/namespace-pilot-attention-recheck-1/result.json')
        counts=checked['counts']
        print('试点注意项现状：{}/{} 路径现存；仅只读路径复查，不代表子树完整或元数据一致。'.format(
            counts.get('namespace_path_present',0),sum(counts.values())))
    except FileNotFoundError:pass
    try:
        extra=control_read(root/'final/late-copy-1.json')
        extra_state=control_read(Path(extra['audit_root'])/'conditional-copy-state.json')
        print('不重叠增量批次：{:,} 文件 / {:.2f} GiB；进度单列，未重复加入主队列。'.format(extra['files'],extra['bytes']/1024**3))
        copy_progress(extra_state.get('controls',{}),control_read)
        if 'files' in extra_state.get('controls',{}):
            try:
                checked=control_read(Path(extra_state['controls']['files'])/'post-copy-audit/result.json')
                if checked.get('status')=='verified_closed_batch_identities':
                    print('增量文件复核：{:,} 文件身份未变化，保留复制及目标回读 SHA256 证据；非全树完成证明。'.format(
                        checked['counts']['unchanged_verified_copy_identity']))
                else:print('增量文件复核：{}，需继续处理。'.format(checked.get('status','未知')))
            except FileNotFoundError:pass
    except FileNotFoundError:pass
    retry_folder=root/'final/main-copy-attention-1/retry-v1'
    try:
        retry=control_read(retry_folder/'result.json')
        verified=retry['counts'].get('identity_verified',0)
        print('源变化项补处理：{}/{} 已复制并复核身份；仍需收尾 {}；同一路径不重复累计文件数。'.format(
            verified,retry['original_attention'],retry['original_attention']-verified))
    except FileNotFoundError:
        try:
            retry=control_read(retry_folder/'progress.json')
            print('源变化项补处理中：已返回 {}，在途 {}；主复制不动。'.format(
                retry['counts'].get('processed',0),retry['active']))
        except FileNotFoundError:pass
    retry_folder=root/'final/main-copy-attention-2/retry-v1'
    try:
        retry=control_read(retry_folder/'result.json')
        verified=retry['counts'].get('identity_verified',0)
        print('源变化补处理第2组：{}/{} 已复制并复核；剩余仅记注意项，不覆盖现存目标。'.format(
            verified,retry['original_attention']))
    except FileNotFoundError:
        try:
            retry=control_read(retry_folder/'progress.json')
            print('源变化补处理第2组：已返回 {}，在途 {}；不重复累计主清单。'.format(
                retry['counts'].get('processed',0),retry['active']))
        except FileNotFoundError:pass
    try:
        extra=control_read(root/'final/attention-pass-1/progress.json');c=extra['counts']
        labels=dict(collecting='提取已结束任务',copying='补处理当前组',group_closed='当前组已闭合',
            bounded_pass_finished='本次有界处理已结束',no_new_completed_attention='未发现未处理的新回执',
            inspection_required='暂停待检查')
        print('源变化后续收尾：已闭合 {}/{} 组；已复核 {}/{} 项，再次变化 {}；{}。'.format(
            extra['completed_groups'],extra['max_groups'],c.get('identity_verified',0),
            c.get('original_attention',0),c.get('changed',0),labels.get(extra['status'],extra['status'])))
    except FileNotFoundError:pass
    if 'namespace' in controls:namespace_postcheck_progress(controls['namespace'],control_read)
    if 'files' not in controls:return
    folder=Path(controls['files'])
    try:cfg=control_read(folder/'config.json')
    except FileNotFoundError:return
    print('\n逻辑分片（本机执行） | 在途任务 | 新复制 GiB | 已处理/总文件 | 状态 | 心跳')
    for node,shard in cfg['shards'].items():
        try:value=control_read(folder/'nodes'/node/'progress.json')
        except FileNotFoundError:
            print('{} | - | - | 0/{:,} | 等待启动 | -'.format(node,shard['files']));continue
        c=value['counts'];age=time.time()-value['updated']
        print('{} | {} | {:.1f} | {:,}/{:,} | {} | {:.0f}s'.format(node,
            value.get('active',0) if age<30 else '-',c.get('copied_bytes',0)/1024**3,
            c.get('processed',0),shard['files'],value['status'],age))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, default=DEFAULT)
    parser.add_argument('--watch', nargs='?', type=float, const=5)
    args = parser.parse_args()
    root = args.control
    sys.path.insert(0, str(root / 'tools'))
    import migration_reaudit as audit
    from migration_reaudit_shared import render, read as control_read
    while True:
        try:frozen=control_read(root/'final/final-report.json')
        except FileNotFoundError:frozen=None
        if frozen:
            copy_view(root,frozen,control_read)
            if args.watch is None:return
            time.sleep(max(1,args.watch));continue
        render(root)
        try:
            index = control_read(root / 'final/index-state.json')
        except FileNotFoundError:
            index = None
        if index:
            print('已索引输入 {}/{}；去重候选 {:,}（{}）。'.format(
                index['indexed_inputs'],index['registered_scanners']*3,index['unique_candidates'],
                '报告已收齐' if index['status']=='indexed_all_closed_reports' else '仍在收齐报告'))
        try:
            manifest = control_read(root / 'final/revalidation/manifest.json')
        except FileNotFoundError:
            manifest = None
        if manifest:
            counters = collections.Counter()
            complete = 0
            for plan in manifest['plans']:
                if not plan['records']:
                    continue
                folder = root / 'final/revalidation/nodes' / ('dev-review-'+str(plan['slot'])) / 'report'
                result = None
                for name in ('result.json','progress.json'):
                    try:
                        result = control_read(folder / name)
                        break
                    except FileNotFoundError:
                        continue
                if result:
                    counters.update(result['counts'])
                    complete += result['status'] == 'complete'
            print('最终候选复核 {:,}/{:,}；已完成 {} 路；复核后仍缺 {:,} 文件 / {:.3f} TiB。'.format(
                counters['processed'],manifest['records'],complete,
                counters['missing_files'],counters['missing_logical_bytes']/1024**4))
        try:
            merge = control_read(root / 'final/merge-state.json')
        except FileNotFoundError:
            merge = None
        if merge:
            print('结果合并：{}；{}。'.format(merge['status'],
                '{}/24 分片'.format(merge['merged_partitions']) if 'merged_partitions' in merge else '候选清单已导出'))
        try:
            closure = Path(control_read(root / 'final/namespace-closure-active.json')['folder'])
        except FileNotFoundError:
            closure = root / 'final/namespace-closure-1'
        try:
            scopes = control_read(closure / 'manifest.json')
        except FileNotFoundError:
            scopes = None
        if scopes:
            inspected = finished = reported = failures = 0
            for plan in scopes['plans']:
                report = None
                for name in ('result.json','progress.json'):
                    try:
                        report = control_read(Path(plan.get('report_directory',str(Path(plan['path']).parent / 'report'))) / name)
                        break
                    except FileNotFoundError:
                        continue
                if report:
                    reported += 1
                    finished += report['status']=='complete'
                    failures += report['status']=='failed'
                    inspected += report['counts'].get('entries',0)
            print('变化目录补扫：{} 个独立范围；已返回 {}/{} 路；检查 {:,} 条；失败 {}。'.format(
                len(scopes['scopes']),finished,len(scopes['plans']),inspected,failures))
            try:
                scoped_review=control_read(closure/'review-manifest.json')
            except FileNotFoundError:
                scoped_review=None
            if scoped_review:
                processed=complete=0
                for plan in scoped_review['plans']:
                    for name in ('result.json','progress.json'):
                        try:
                            value=control_read(closure/'reviews'/('slot-'+str(plan['slot']))/'report'/name)
                            processed+=value['counts']['processed'];complete+=value['status']=='complete';break
                        except FileNotFoundError:continue
                print('补扫候选复核：{:,}/{:,}；返回 {}/{} 路。'.format(
                    processed,scoped_review['records'],complete,len(scoped_review['plans'])))
            try:
                closing=control_read(closure/'closure-state.json')
            except FileNotFoundError:
                closing=None
            if closing:
                detail=''
                if 'merged' in closing:detail=' {}/{}'.format(closing['merged'],closing['expected'])
                if 'scopes' in closing:detail=' {}/{} 个稳定目录范围'.format(closing['completed'],closing['scopes'])
                print('收尾阶段：'+closing['status']+detail)
        try:
            final=control_read(root/'final/final-report.json')
        except FileNotFoundError:
            final=None
        if final:
            files=final['missing']['file']
            label='当前已核实冻结清单' if final.get('whole_tree_completion_proven') is False else '最终冻结补拷基线'
            print('{}：{:,} 文件 / {:.3f} TiB；不表示已搬运完成。'.format(label,files['paths'],files['logical_bytes']/1024**4))
        try:
            acceleration = control_read(root / 'acceleration/manifest.json')
        except FileNotFoundError:
            acceleration = None
        if acceleration:
            inspected = 0
            reported = 0
            finished = 0
            failures = 0
            for plan in acceleration['plans']:
                folder = root / 'nodes' / ('local-accel-'+plan['owner']) / ('slot-'+str(plan['slot'])) / 'report'
                value = None
                for name in ('result.json','progress.json'):
                    try:
                        value = control_read(folder / name)
                        break
                    except FileNotFoundError:
                        continue
                if value:
                    reported += 1
                    inspected += value['counts'].get('entries',0)
                    finished += value['status'] == 'complete'
                    failures += value['status'] == 'failed'
            print('本机尾部并行核验：已有进度 {}/{} 路；完成 {}；失败 {}；检查观察 {:,}。'.format(
                reported,len(acceleration['plans']),finished,failures,inspected))
            print('本机观察量不与主统计直接相加；完整覆盖证明通过后才替换对应原分区。')
        try:
            policy = control_read(root / 'conditional-copy-policy.json')
            copy = control_read(root / 'conditional-copy-state.json')
        except FileNotFoundError:
            copy = None
        if copy:
            print('条件补拷：'+str(copy['status']))
            if copy.get('controls'):
                copy_progress(copy['controls'],control_read)
            if copy.get('control'):
                print('补拷控制目录：'+copy['control'])
        else:
            print('授权规则：全量去重确认遗漏 <50 TB 才自动补拷；扫描下限不作为启动依据。')
        if args.watch is None:
            return
        time.sleep(max(1, args.watch))


if __name__ == '__main__':
    main()
