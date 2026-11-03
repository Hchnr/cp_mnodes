"""Truthful summary of a live copy pass; never invent a total/percentage/ETA."""
import datetime
import time


def overview_metrics(status, now=None):
    now = time.time() if now is None else now
    c = status['counts']
    fresh = max(0, now - status['updated']) <= 30
    healthy = sum(bool(w['ready'] and w['alive'] and w['age'] <= 30 and fresh)
                  for w in status['workers'].values())
    attention = sum(c.get(k, 0) for k in ('conflict', 'changed', 'error', 'unsupported'))
    return {
        'overview/已确认容量_TB': (c.get('copied_bytes', 0) + c.get('skipped_bytes', 0)) / 1e12,
        'overview/本次新复制_TB': c.get('copied_bytes', 0) / 1e12,
        'overview/目标已有相同_TB': c.get('skipped_bytes', 0) / 1e12,
        'overview/近5分钟完成速度_TB每小时': status['rates']['300']['copied_Bps'] * 3600 / 1e12,
        'overview/在线节点数': healthy,
        'overview/需复核结果数': attention,
    }


def overview_rows(status, now=None):
    now = time.time() if now is None else now
    m = overview_metrics(status, now)
    c = status['counts']
    stamp = datetime.datetime.fromtimestamp(status['updated'], datetime.timezone(datetime.timedelta(hours=8)))
    return [
        ['状态', status['state'], '在线源复制中；结束后仍需核对异常'],
        ['全量总大小', '尚未确定', '旧清点未完整结束，且磁盘占用与逻辑容量口径不同'],
        ['总进度百分比 / 预计完成时间', '尚不能可靠计算', '不以已发现任务比例充当全量进度'],
        ['已确认容量', f"{m['overview/已确认容量_TB']:.3f} TB", '新复制并校验 + 已有内容相同；不含在途写入'],
        ['本次新复制', f"{m['overview/本次新复制_TB']:.3f} TB", '完成校验并发布的新副本'],
        ['目标已有相同', f"{m['overview/目标已有相同_TB']:.3f} TB", '已核对相同内容，复用目标副本'],
        ['近5分钟完成速度', f"{m['overview/近5分钟完成速度_TB每小时']:.3f} TB/h", '只统计本次新复制完成量；非物理磁盘带宽'],
        ['在线节点', f"{m['overview/在线节点数']} / {status['nodes_total']}", '连接正常且最近消息与整体状态均不超过30秒'],
        ['成功处理结果', f"{c.get('copied',0)+c.get('skipped',0):,}", '普通文件及符号链接结果；不代表全量文件数'],
        ['需复核结果', str(m['overview/需复核结果数']), f"冲突 {c.get('conflict',0)}；源变化 {c.get('changed',0)}；错误 {c.get('error',0)}；特殊对象 {c.get('unsupported',0)}"],
        ['元数据警告', str(c.get('metadata_warnings',0)), '可能与成功结果重叠，单独列示'],
        ['待扫描目录 / 待复制批次', f"{status['pending'].get('scan',0):,} / {status['pending'].get('copy',0):,}", '仅已发现队列，源目录仍在发现中'],
        ['累计运行', f"{(status['updated']-status['started'])/3600:.2f} 小时", '按任务实际开始时间计算'],
        ['数据更新时间（北京时间）', stamp.isoformat(timespec='seconds'), f"读取时数据延迟 {max(0,now-status['updated']):.0f} 秒；每2分钟上报"],
    ]
