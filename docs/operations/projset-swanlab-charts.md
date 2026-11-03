# SwanLab 迁移看板图表说明

实验：https://swanlab.cn/@hchnr/data-migration/runs/0euoq0dr

2026-11-03 增加首页“总览”分组。只替换独立上报器，复制 controller/worker 不变。每 120 秒合并上报一次；速率的 60/300 秒采样窗口与上报间隔是两回事。

容量为十进制 TB，1 TB = 10^12 bytes。数值是文件逻辑大小，不是去重后物理占用。计数包含普通文件和符号链接；processed 还包含目录/扫描异常结果，不能统一当作唯一普通文件数。横轴 Step 是上报编号，历史上改变过上报间隔，不能直接乘两分钟当作精确运行时间。

## 总览

| 指标 | 含义 |
| --- | --- |
| `overview/总进度看板` | 当前状态表：已确认容量、新复制、已有相同、完成速度、在线节点、成功结果、异常细分、待扫描/待复制队列、运行时间、数据时间及口径说明。全量总大小、完成百分比和 ETA 暂显示尚未确定。 |
| `overview/已确认容量_TB` | `copy/copied_TB + copy/skipped_TB`。本轮已确认有相同副本的逻辑容量，排除在途临时写入。 |
| `overview/本次新复制_TB` | 与 `copy/copied_TB` 相同，完成校验并发布的新副本。 |
| `overview/目标已有相同_TB` | 与 `copy/skipped_TB` 相同，目标已有内容相同的副本。 |
| `overview/近5分钟完成速度_TB每小时` | `rate_300s/copied_TB_per_second × 3600`，仅本次新复制完成速度，以 TB/h 显示。 |
| `overview/在线节点数` | ready 且 alive、最近消息不超过30秒，并且全局状态不超过30秒的节点数。正常目标16。 |
| `overview/需复核结果数` | conflict + changed + error + unsupported；是结果记录数，不保证为唯一文件数。元数据警告单独列在总览表，避免与成功结果重复相加。 |

全量百分比暂不能提供：目录发现仍在进行；旧只读清点未完整结束，且旧统计为 st_blocks 分配字节，本任务为 st_size 逻辑字节。不能将旧清点容量或已发现任务数当成完整分母。

## copy

| 指标 | 含义 |
| --- | --- |
| `copy/copied` | 新复制并校验成功的结果数，含普通文件、符号链接。 |
| `copy/copied_TB` | 上述成功结果的源逻辑大小之和。普通文件完成读回校验后才计入。 |
| `copy/skipped` | 目标已存在且内容核对相同的成功结果数；不是未经检查直接跳过。符号链接核对链接目标。 |
| `copy/skipped_TB` | 上述相同目标对应的逻辑容量。 |
| `copy/payload_written_TB` | 累计写入进度，包含在途、最终未成功发布的写入和重试开销；稀疏文件洞按逻辑长度计入，不是物理写盘字节或已完成量。 |
| `copy/processed` | 已生成处理结果的记录数，包括成功、已有相同、冲突、变化、错误和特殊对象；扫描/目录错误也可计入。 |
| `copy/conflict` | 同名目标内容或类型不同等冲突结果数。遵照授权保留原目标，不覆盖。 |
| `copy/changed` | 源文件或目录在发现、读取或发布过程中发生身份/内容变化的结果数。 |
| `copy/error` | I/O、权限、扫描等失败结果数；不是 OOM 计数。 |
| `copy/hardlink_paths` | 处理结果对应的源文件 nlink>1 的路径记录数；不是硬链接组数，也不代表目标硬链接拓扑已恢复。与 copied/skipped/error 等结果重叠。 |

若将来产生 `copy/unsupported`，表示不能按普通文件/符号链接复制的特殊对象；若产生 `copy/metadata_warnings`，表示权限、所有者、时间、xattr 等元数据警告，可能与复制成功重叠。这两个键没有产生计数前可能没有对应图表。

`copied_TB + skipped_TB` 可用作本轮已确认容量；不要再加 payload_written_TB，否则重复计算。copied + skipped 可用作成功结果数，processed 则是全部结果数；hardlink_paths 和 metadata_warnings 不能再作为互斥分类加进去。

## rate_60s / rate_300s

| 图表 | 含义 |
| --- | --- |
| `rate_60s/write_TB_per_second` | 近约1分钟的 payload_written_TB 增速，含在途写入。 |
| `rate_60s/copied_TB_per_second` | 近约1分钟的新副本校验完成容量增速。 |
| `rate_60s/files_ps` | 近约1分钟的 processed 增速，单位结果记录/秒，不是仅成功文件/秒。 |
| `rate_300s/write_TB_per_second` | 近约5分钟的累计写入增速。 |
| `rate_300s/copied_TB_per_second` | 近约5分钟的新副本校验完成容量增速，适合看稳定吞吐。 |
| `rate_300s/files_ps` | 近约5分钟的 processed 增速。 |

窗口由本地约5秒一次的状态样本计算，实际窗口通常约55秒/295秒，启动期可能更短。云端每2分钟看到一个采样点；1分钟窗口曲线不是每分钟上传。比如0.004 TB/s = 14.4 TB/h。速度是逻辑容量，不含SHA读回的全部物理I/O流量。

write 短时高于 copied，通常表示大文件仍在写入或读回校验；copied 阶跃或短时高于 write，可由此前在途大文件集中完成导致。

## nodes、queue、jobs

| 图表 | 含义 |
| --- | --- |
| `nodes/ready` | 协调器标记已通过身份检查、连接就绪的节点数；不含实时CPU/内存判断，也不自动按心跳超时扣减。总览的在线节点数额外检查消息新鲜度。 |
| `queue/active` | 已分配但尚未确认完成的任务数，含扫描任务和文件批次。每节点最多1个扫描、3个复制批次，16节点通常最多64；不等于复制线程数。 |
| `queue/pending_copy` | 已发现、尚未分配的文件批次数；每批最多128条或达到约256MiB，单个大文件可超过该容量。不是待复制文件总数。 |
| `queue/pending_scan` | 已发现、尚待扫描的目录任务数；大不等于出错。 |
| `jobs/discovered` | 累计生成的扫描任务与文件批次总数，仍可能增加。不是源文件总数。 |
| `jobs/finished` | 已处理结束并确认回执落盘的任务数；其中可以有冲突、变化或失败，不等于全部文件成功。 |

不能用 finished/discovered 得到全量迁移百分比。总范围尚未发现完整，任务工作量也不相同。

## time、health

| 图表 | 含义 |
| --- | --- |
| `time/elapsed_hours` | 协调器最近状态时间减任务开始时间，单位小时。 |
| `time/status_age_seconds` | 上报器读取时该状态快照的年龄，单位秒。持续增大提示控制器状态过期；不包含之后的云端传输延迟。 |
| `health/node_failures` | 协调器累计记录的节点连接/读取失败事件数。同一节点可产生多条事件，不等于唯一故障节点数，也不是OOM次数。 |
| `health/copying` | 状态为 copying 时为1，其余状态为0；0也可能表示停止或失败，不能仅凭0断言完成。 |

当前图表不包含CPU、内存或OOM监测。此前资源检查的证据来自逐节点读取 /proc 和 cgroup，不能从 health/copying=1 推导没有OOM。

## node：32张同类图表

以下16个节点每个有两张，共32张：`cpu-master-0`，以及 `cpu-worker-0`～`cpu-worker-14`。

| 命名模板 | 含义 |
| --- | --- |
| `node/<节点名>/copied_TB` | 该节点累计新复制并校验成功的容量。包含于全局 copy/copied_TB，不能与全局总量再相加。曲线斜率可用于对比节点速度。 |
| `node/<节点名>/heartbeat_age_seconds` | 最近收到该节点消息距状态采样时的秒数。消息包含心跳、任务完成和扫描结果，并非只计算固定心跳包。正常一般为几秒；持续上涨应检查连接或进程。 |

累计复制量少可能由领取的文件大小、共享宿主机与I/O负载不同导致，不能单凭累计量认定节点失效。
