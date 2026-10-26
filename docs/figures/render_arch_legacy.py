#!/usr/bin/env python3
"""Render the architecture as portable vector SVG and 2x PNG.

Requires matplotlib and a CJK font. Example:
  python3 docs/figures/render_arch_legacy.py --font /path/to/NotoSansCJKsc-Regular.otf
SVG text is converted to vector outlines so readers need no installed CJK font.
"""
import argparse
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.path import Path as MPath


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--font', required=True, type=Path)
    args = parser.parse_args()
    font_manager.fontManager.addfont(str(args.font))
    font = font_manager.FontProperties(fname=str(args.font)).get_name()
    plt.rcParams.update({'font.family': font, 'svg.fonttype': 'path', 'axes.unicode_minus': False})
    fig, ax = plt.subplots(figsize=(18, 15.8), dpi=100)
    fig.subplots_adjust(0, 0, 1, 1)
    ax.set(xlim=(0, 1800), ylim=(1580, 0))
    ax.axis('off')
    fig.patch.set_facecolor('#f8fafc')
    ink, muted = '#15253e', '#5b6b80'
    blue, teal, amber, purple = '#3e64b0', '#137f7a', '#b36c18', '#7760a4'

    def text(x, y, s, size=12, color=ink, ha='left', va='top'):
        ax.text(x, y, s, fontsize=size, color=color, ha=ha, va=va, linespacing=1.65)

    def box(x, y, w, h, title, lines, color=blue, fill='#ffffff', title_size=14):
        ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=0,rounding_size=12',
                                   linewidth=1.25, edgecolor=color, facecolor=fill, zorder=2))
        ax.plot([x+18,x+w-18],[y+54,y+54],color=color,alpha=.2,lw=1,zorder=3)
        text(x+20,y+16,title,title_size,color)
        text(x+20,y+72,lines,11.3)

    def panel(x,y,w,h,title,subtitle='',color='#cad5e3'):
        ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=0,rounding_size=16',
                                   linewidth=1.2,edgecolor=color,facecolor='#eef3f9',zorder=0))
        text(x+22,y+18,title,15)
        if subtitle:text(x+22,y+49,subtitle,10.5,muted)

    def arrow(points,color=blue,lw=1.8,both=False,dashed=False):
        codes=[MPath.MOVETO]+[MPath.LINETO]*(len(points)-1)
        ax.add_patch(FancyArrowPatch(path=MPath(points,codes),arrowstyle='<->' if both else '-|>',
                    mutation_scale=13,linewidth=lw,color=color,linestyle=(0,(5,4)) if dashed else '-',
                    zorder=4,capstyle='round',joinstyle='round'))

    def label(x,y,s,color=muted,size=10):
        ax.text(x,y,s,fontsize=size,color=color,ha='center',va='center',
                bbox={'facecolor':'#f8fafc','edgecolor':'none','pad':3},zorder=5)

    text(40,32,'多节点共享存储复制 · 进程与并发架构',25)
    text(40,86,'配置快照  2026-10-22 18:53 CST    |    优先级 3 恢复链路    |    单一任务所有者 + 多进程只读供给',12,muted)
    for x, number, desc in [(40,'8 × 16','源元数据线程槽'),(460,'8 × 64','客户端回退线程槽'),
                            (890,'12 × 64','远端元数据操作槽'),(1320,'12 × 128','复制工作槽')]:
        text(x,133,number,23)
        text(x+155,145,desc,11,muted)

    panel(40,215,1240,730,'A  MASTER · 协调主机','下面四类工作属于不同进程/线程资源；路径预约、ID、索引与清单保持单一归属')
    box(70,315,340,230,'源目录读者：8 个子进程',
        '每进程 16 个 scan-stat 线程\n目录页面 ≤ 256 项；双向 Pipe\n父进程先选未知叶子，子进程 stat\n仅返回目录项与元数据',blue,'#f5f8ff')
    box(450,315,350,230,'唯一 producer · 主线程',
        '路径去重 / 历史候选交错供给\n硬链归属 / manifest / 索引\n预约任务 ID，组装文件批次\n收割检查和发布结果，推进阶段',blue,'#e9f0ff')
    box(840,315,400,230,'检查客户端：8 个子进程',
        '每进程一次执行 1 个批次\n每批 ≤ 64 项；正常路径 1 次 HTTP\n每进程 64 线程用于单项 / 失败回退\n总在途 ≤ 4096 项（含排队结果）',teal,'#f0faf8')
    label(430,575,'有界 IPC')
    arrow([(410,422),(450,422)])
    arrow([(800,422),(840,422)],teal,both=True)
    label(820,575,'批次 / 结果')
    arrow([(625,545),(625,650)])
    label(625,600,'主线程预约后提交')
    box(450,650,350,180,'发布池 · producer 内 8 线程',
        '64 批在途 / 待收集上限\n仅写独立 pending 文件\n批次通常 ≤ 64 项 / 约 1 GiB',blue)
    box(70,650,340,220,'独立 broker 进程',
        'HTTP 请求按线程处理\n共享 FS 操作并发门限 64\n短 SQLite 事务 + owner/task 锁\n可选 running 标记 64 分片',blue)
    box(840,650,400,180,'本机状态 · 两套独立权威',
        'producer 索引：路径 / 历史成功证明\nbroker SQLite/WAL：claims / lease\n分配不依赖 NFS 抢锁\n进程交接保留原租约与持久回执',purple,'#f7f3fc')
    text(70,895,'producer 不搬运文件内容；broker 不转发文件内容。',11,muted)

    panel(1320,215,440,420,'B  12 节点 · 只读 probe','每节点 1 个服务进程，master 也包含在内')
    box(1350,315,380,280,'认证 /probe-batch 服务',
        '每批 ≤ 64 项；保序返回逐项结果\n每服务 64 个 batch-pool 线程\n同一 FS 信号量：64 路元数据操作\nHTTP 处理槽上限 128\n另有 1 个健康上报线程\n读取共享源 / 目标元数据，不复制内容',teal,'#f0faf8')
    arrow([(1240,422),(1350,422)],teal,both=True)
    label(1295,390,'HTTP 批次',teal,9)
    box(1320,680,440,265,'旁路与阶段性进程',
        '常驻观测：metrics / performance watch\n用户 CLI：progress\n恢复：索引 helper / checkpoint 准备\n阶段收尾：回执收集 / 硬链 / 目录元数据\n它们读取持久证据，不另起任务预约者',color='#7c8795',fill='#ffffff')

    box(40,1050,330,210,'共享源目录',
        '所有执行节点可见\n扫描：目录项与 stat\n复制：文件内容直接读取\n源在线变化由身份复核 / 补扫处理',purple,'#f7f3fc')
    box(410,1050,430,290,'共享 CONTROL 目录',
        'pending / running / done（回执）\nmanifest / 配置 / 状态快照\npending、done：64 分片\nrunning：兼容旧平铺与新 64 分片\nworker 写心跳，broker 写回执\n可变 JSON 通过不可变版本读取',purple,'#f7f3fc')
    arrow([(625,830),(625,1050)])
    label(625,978,'发布任务',blue)
    arrow([(410,1160),(388,1160),(388,976),(240,976),(240,870)],blue)
    label(310,976,'领取 / 回执持久化',blue,9)
    arrow([(40,1150),(17,1150),(17,422),(70,422)],purple)
    label(108,568,'源目录 → 读者',purple,10)

    panel(900,1050,460,365,'C  每节点复制单元 × 12','master × 1 + worker × 11；全局 1536 槽',color='#dfcbb0')
    box(925,1136,410,108,'primary 进程 · 64 复制线程',
        '+ 1 心跳线程 + 1 state-cache 刷新线程',amber,'#fff8ed',13)
    box(925,1271,410,108,'supplement 进程 · 64 复制线程',
        '+ 1 心跳线程 + 1 state-cache 刷新线程',amber,'#fff8ed',13)
    arrow([(240,870),(240,1009),(1130,1009),(1130,1050)],blue,both=True)
    label(1040,1009,'HTTP claim / done',blue)
    box(1420,1050,340,365,'共享目标目录',
        '每个复制线程串行完成：\n读源 + SHA-256 → 写临时文件\n复读目标校验 → 元数据 / fsync\n检查源身份 → 发布正式名字\n一批逐文件处理，不拆分单文件\n没有独立校验线程池',purple,'#f7f3fc')
    arrow([(1360,1228),(1420,1228)],amber,lw=3)
    label(1390,1194,'写 + 校验',amber,9)
    arrow([(205,1260),(205,1450),(1130,1450),(1130,1415)],amber,lw=3)
    label(660,1450,'文件内容直达执行节点；不经 master producer / broker 中转',amber,11)
    arrow([(1540,595),(1782,595),(1782,1040),(1690,1040),(1690,1050)],teal,dashed=True)
    label(1650,994,'只读存在性 / 源身份',teal,10)
    arrow([(840,1300),(900,1300)],purple,both=True,dashed=True)
    label(870,1360,'状态 / 心跳',purple,9)

    text(40,1501,'边界：8×64 个客户端线程槽 ≠ 512 个正常批量 HTTP 并发；8 个客户端进程正常最多并行执行 8 个批次。',11,ink)
    text(40,1533,'蓝：任务与协调    绿：只读检查    橙粗线：文件内容    紫：共享/本机状态    虚线：状态或只读依赖；线程槽不是实测吞吐。',10.5,muted)
    output = Path(__file__).resolve().parent
    fig.savefig(output/'arch-legacy.svg',format='svg',facecolor=fig.get_facecolor(),metadata={'Date':None,'Creator':'cp_mnodes architecture renderer'})
    svg = output/'arch-legacy.svg'
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    fig.savefig(output/'arch-legacy.png',format='png',dpi=200,facecolor=fig.get_facecolor())
    plt.close(fig)
    print('Rendered arch.svg and arch.png')


if __name__ == '__main__':
    main()
