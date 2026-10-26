#!/usr/bin/env python3
"""Render the fixed-manifest architecture as native SVG and 2x PNG.

Requires matplotlib and a CJK font; --font /path/to/NotoSansCJKsc-Regular.otf.
The archived producer/broker figure is rendered by render_arch_legacy.py.
"""
import argparse
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.path import Path as VectorPath


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--font', required=True, type=Path)
    args = parser.parse_args()
    font_manager.fontManager.addfont(str(args.font))
    name = font_manager.FontProperties(fname=str(args.font)).get_name()
    plt.rcParams.update({'font.family': name, 'svg.fonttype': 'path'})
    fig, ax = plt.subplots(figsize=(16, 13), dpi=100)
    fig.subplots_adjust(0, 0, 1, 1)
    ax.set(xlim=(0,1600), ylim=(1300,0)); ax.axis('off')
    fig.patch.set_facecolor('#f8fafc')
    ink, muted, blue, teal, orange, purple = '#192940','#63748b','#4066af','#167d79','#b26b17','#7859a4'

    def text(x,y,s,size=12,color=ink):
        ax.text(x,y,s,fontsize=size,color=color,va='top',linespacing=1.7)

    def box(x,y,w,h,title,body,color=blue,fill='#ffffff',size=14):
        ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=0,rounding_size=13',
                                 edgecolor=color,facecolor=fill,lw=1.2,zorder=2))
        text(x+20,y+17,title,size,color)
        ax.plot([x+20,x+w-20],[y+54,y+54],lw=.8,color=color,alpha=.25,zorder=3)
        text(x+20,y+72,body,11.5)

    def arrow(points,color=blue,both=False,dashed=False,lw=1.8):
        ax.add_patch(FancyArrowPatch(path=VectorPath(points,[VectorPath.MOVETO]+[VectorPath.LINETO]*(len(points)-1)),
                     arrowstyle='<->' if both else '-|>',color=color,lw=lw,mutation_scale=13,
                     linestyle=(0,(5,4)) if dashed else '-',zorder=4))

    def label(x,y,s,color=muted,size=10.5):
        ax.text(x,y,s,ha='center',va='center',fontsize=size,color=color,zorder=5,
                bbox={'facecolor':'#f8fafc','edgecolor':'none','pad':3})

    text(40,30,'固定清单补拷 · 当前监控选择的架构',25)
    text(40,86,'2026-10-26 核对  |  12 个节点 × 1 个进程 × 64 个复制线程 = 768 个配置工作槽',13,muted)
    text(40,129,'输入边界：4,412,943 个普通文件 / 4,148,614,479,512 B；精确排除 eai_pwm/datasets/raw',12,muted)

    box(40,200,450,200,'01  只读查漏结果 · 冻结输入',
        'missing-files.jsonl.gz\n校验清单 SHA-256、路径有序且唯一\n源 inode / size / mtime / ctime 随项绑定',teal,'#effaf7')
    box(560,200,450,200,'02  离线准备 · 不接管旧队列',
        '按字节 + 每文件固定开销进行分片\n冻结 12 份互斥分片与复制源码摘要\n主机名 / 挂载身份 / 计数检查',blue,'#eef3ff')
    box(1080,200,480,200,'03  共享控制目录中的 12 份分片',
        '每节点一个不可变 .jsonl.gz\n顺序读取本节点分片；不跨节点抢占\n分片及代码 SHA 在启动时复核',purple,'#f7f2fc')
    arrow([(490,300),(560,300)],teal)
    arrow([(1010,300),(1080,300)],blue)

    ax.add_patch(FancyBboxPatch((400,490),760,370,boxstyle='round,pad=0,rounding_size=16',
                             facecolor='#fff8ed',edgecolor='#d8c2a1',lw=1.2,zorder=0))
    text(422,505,'执行单元 × 12：master × 1 + worker × 11',15)
    text(422,542,'每节点一个 OS 进程；主循环 + 最多 64 个复制线程，不另设校验线程池',11,muted)
    box(425,590,320,230,'主线程 · 顺序供给与落盘',
        '读取分片 / 检查 stop.json\n最多 64 个未收割 future\n收集结果、追加逐文件回执\n约每 3 秒 flush/fsync 和进度发布',blue)
    box(795,590,340,230,'线程池 · 最多 64 路',
        '每槽一次处理一个完整文件\n身份检查 → 临时文件复制\n源 SHA + 目标读回校验\n元数据 / fsync / 不覆盖发布',orange)
    arrow([(745,700),(795,700)],blue,both=True)
    label(770,850,'文件任务 / 完成结果',blue)

    box(40,540,300,250,'共享源目录',
        '/share/project\n只读已冻结清单内的文件\n源变化或消失进入回执\n不扩大到新文件或新版本',purple,'#f7f2fc')
    box(1220,540,340,250,'共享目标目录',
        '/share-mixed/project\n临时文件校验后 link 发布\n已有同内容副本可复用\n不同内容记冲突，不强行覆盖',purple,'#f7f2fc')
    arrow([(340,620),(370,620),(370,460),(960,460),(960,590)],orange,lw=2.7)
    label(665,460,'文件内容直读；不经中央 producer / broker',orange)
    arrow([(1135,700),(1220,700)],orange,lw=2.7)
    label(1178,665,'写入 / 读回',orange,10)
    arrow([(1320,400),(1320,435),(585,435),(585,590)],blue)
    label(1070,435,'各节点仅消费自己的固定分片',blue)

    box(400,950,760,220,'共享持久证据 · 恢复权威',
        'nodes/<node>/receipts-*.jsonl：追加、每次尝试新建，不修改旧回执\nprogress.json / launch.json / 冻结清单 / 代码 / 日志均在共享存储\n恢复时复核成功回执的当前源和目标身份；有效则跳过，不完整则按文件重试\n本机 /tmp 只放排他 flock；不存进度、不存清单',purple,'#f7f2fc')
    arrow([(585,820),(585,950)],purple)
    label(585,905,'追加回执 / 发布状态',purple)
    arrow([(1020,950),(1020,900),(770,900),(770,570),(585,570),(585,590)],purple,dashed=True)
    label(947,900,'重启复核持久回执',purple)
    box(1220,950,340,220,'只读监控 · 独立路由',
        'fixed-active.json 选择本轮\nfixed_progress → 冻结代码\n分母仅为本清单，不累加历史\n显式 --control 可看旧任务',teal,'#effaf7')
    arrow([(1160,1050),(1220,1050)],teal,dashed=True)
    box(40,950,300,220,'历史链路 · 单独保留',
        'P3 查漏与独立 P4 引擎\n各自控制目录 / broker / 租约\n旧角色不随本轮自动恢复\n历史图：arch-legacy.svg',color='#8692a2',fill='#f1f3f6',size=13)

    text(40,1220,'模式边界：无目录扫描、无中央任务发布、无 broker、无空闲 HTTP 轮询；保留逐文件复制与内容校验。',12)
    text(40,1256,'配置槽位不等于实时活跃数；清单补拷完成不等于所有历史范围及异常项均已验收。',11,muted)
    output=Path(__file__).resolve().parent
    fig.savefig(output/'arch.svg',format='svg',facecolor=fig.get_facecolor(),metadata={'Date':None})
    p=output/'arch.svg';p.write_text('\n'.join(line.rstrip() for line in p.read_text().splitlines())+'\n')
    fig.savefig(output/'arch.png',dpi=200,facecolor=fig.get_facecolor())
    plt.close(fig)
    print('Rendered current-mode arch.svg and arch.png')


if __name__=='__main__':main()
