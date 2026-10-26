#!/usr/bin/env python3
"""Small read-only entry for the active full-tree completeness re-audit."""
import argparse
import collections
from pathlib import Path
import sys
import time

DEFAULT = Path('/share-mixed/project/.hcr-readonly-audit-20261026-full-0107')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, default=DEFAULT)
    parser.add_argument('--watch', nargs='?', type=float, const=5)
    args = parser.parse_args()
    root = args.control
    sys.path.insert(0, str(root / 'tools'))
    import migration_reaudit as audit
    from migration_reaudit_shared import render
    while True:
        render(root)
        try:
            index = audit.read(root / 'final/index-state.json')
        except FileNotFoundError:
            index = None
        if index:
            print('已索引输入 {}/{}；已提交去重候选 {:,}（仍在收齐报告）。'.format(
                index['indexed_inputs'],index['registered_scanners']*3,index['unique_candidates']))
        try:
            manifest = audit.read(root / 'final/revalidation/manifest.json')
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
                        result = audit.read(folder / name)
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
            acceleration = audit.read(root / 'acceleration/manifest.json')
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
                        value = audit.read(folder / name)
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
            policy = audit.read(root / 'conditional-copy-policy.json')
            copy = audit.read(root / 'conditional-copy-state.json')
        except FileNotFoundError:
            copy = None
        if copy:
            print('条件补拷：'+str(copy['status']))
            if copy.get('control'):
                print('补拷控制目录：'+copy['control'])
        else:
            print('授权规则：全量去重确认遗漏 <50 TB 才自动补拷；扫描下限不作为启动依据。')
        if args.watch is None:
            return
        time.sleep(max(1, args.watch))


if __name__ == '__main__':
    main()
