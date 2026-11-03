#!/usr/bin/env python3
"""Read the independent projset copy monitor; no changes to running actors."""
import argparse
import datetime
import json
from pathlib import Path
import time

REPO = Path(__file__).resolve().parents[2]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', type=Path)
    p.add_argument('--watch', action='store_true')
    p.add_argument('--json', action='store_true')
    args = p.parse_args()
    run = args.run or Path(json.loads((REPO/'runs/projset-copy-current.json').read_text())['run'])
    while True:
        s = json.loads((run/'status.json').read_text())
        if args.json:
            print(json.dumps(s, ensure_ascii=False, indent=2))
        else:
            c=s['counts'];now=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
            lines=[now.isoformat(timespec='seconds'), f"状态：{s['state']}；节点 {s['nodes_ready']}/{s['nodes_total']}；状态更新距今 {time.time()-s['updated']:.0f}s",
                f"新复制并校验：{c.get('copied',0):,} 项 / {c.get('copied_bytes',0)/1e12:.3f} TB；已有相同：{c.get('skipped',0):,} 项 / {c.get('skipped_bytes',0)/1e12:.3f} TB",
                f"冲突 {c.get('conflict',0):,}；源变化 {c.get('changed',0):,}；错误 {c.get('error',0):,}；特殊对象 {c.get('unsupported',0):,}；元数据警告 {c.get('metadata_warnings',0):,}",
                f"任务：完成 {s['finished_jobs']:,} / 已发现 {s['discovered_jobs']:,}；在途 {s['active']}；待处理 {s['pending']}"]
            for window in ('60','300'):
                r=s['rates'][window]
                lines.append(f"近 {window}s（实际采样 {r['seconds']:.0f}s）：写入 {r['write_Bps']/1e9:.3f} GB/s；校验完成 {r['copied_Bps']/1e9:.3f} GB/s；处理 {r['files_ps']:.1f} 项/s")
            cloud=run/'swanlab/state.json'
            if cloud.exists():
                cs=json.loads(cloud.read_text());lines.append(f"SwanLab：{cs.get('url','初始化中')}；{cs.get('state')}；每 {cs.get('interval_seconds',300)}s 上报")
            lines.append('全量发现仍在进行；待处理任务数不是全部剩余量，不据此估算完成百分比。')
            lines.append(str(run))
            if args.watch:print('\033[2J\033[H',end='')
            print('\n'.join(lines),flush=True)
        if not args.watch:break
        time.sleep(5)


if __name__=='__main__':main()
