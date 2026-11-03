# /share-mixed/projset 全局扫描（depth≤5）

2026-11-02 16:19（北京时间）启动；只读元数据，完整遍历下级，按根目录 depth=0 输出第 1～5 层目录分布。不启动复制、删除、解压或数据内容 hash。

## 实时监控

在开发环境执行（不依赖当前目录）：

```sh
python3 -S -B /share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/ops/diagnostics/projset_usage.py status --watch
```

每 5 秒更新活跃节点/扫描任务、已发现及已完成任务、累计计量条目和占用量，以及近 1/5 分钟的条目/s、容量统计速度；显示实际采样窗口长度。条目含文件和目录等路径，不等于普通文件数。

也可直接查看 [实时进度文档](../../../runs/projset-usage-20261102T081931Z-1be0a3/PROGRESS.md) 或 [status.json](../../../runs/projset-usage-20261102T081931Z-1be0a3/status.json)。运行控制与证据都在混闪的仓库 runs/，不使用 /tmp 存日志。

运行目录：

```text
/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/runs/projset-usage-20261102T081931Z-1be0a3
```

## 配置和口径

- 当前实际核验到 18 个独立 boot 节点：master-0、worker-0～worker-16，当前集群为 18c4f8f3…；旧的 worker-17～22 和 extra-* 入口不可用，没有把它们计入活跃数。
- 每节点 8 路 GNU du，共 144 路；同一有界队列按空闲槽领任务，不静态绑定顶层目录。8 个目录规划线程并行生成不重叠子树；历史大目录信息只用于细分，不复用其容量结果。
- 超大已知平铺缓存按名称分批交给原生 du 分布式计量，目录发现过程中即持续派发；不等待整个名字清单生成后才开始。
- 运行侧源挂载核验为只读；工作进程只读元数据，结果通过 SSH 返回开发环境，不在源目录落日志。
- 容量为 **GNU du 分配字节（st_blocks 口径），含目录元数据，硬链接按路径累计**，不是旧报告的纯普通文件逻辑 st_size，也不是去重后独立物理占用。
- 速度来自正在处理和已完成子树的元数据计量增量，不是读写搬运带宽。超大文件只需查询属性，因而容量统计速度可能很大。
- 尚未发现完整任务范围前不显示全量百分比或固定 ETA；不能用 df 已用容量作进度分母。
- 不跟随符号链接；启动前拒绝源下子挂载。在线更新、目录移动、权限问题会影响遍历结果，错误单独记录，不伪称原子快照。

## 结果和异常

扫描结束后生成（启动时不代表已完成）：

- `directories-depth5.csv`：第 0～5 层目录路径、占用字节、TB 和扫描状态。
- `REPORT.md`：各层容量排行。
- `errors.json`：错误或遗漏；仅 `complete` 表示本轮遍历无记录错误，`finished_with_errors` 需要结合错误解释。

持久化 `jobs.jsonl`、`scaffolds.jsonl`、`results.jsonl` 和 `samples.jsonl`，保留任务与已确认统计证据；不保存逐文件正文或 hash。运行冻结独立 scanner.py 和节点身份配置，不热改冻结文件。

控制连接失联或关闭时，工作进程只终止本次扫描自己启动的 du，不接管或停止训练、复制等其他进程。失联任务不会自动重放，以免重复计数；状态标记异常后需检查剩余任务。

## 验证

启动前 7 项私有目录测试通过：特殊文件名、隐藏项、符号链接、硬链接按路径累计、深层遍历/第 5 层筛选、完整控制器汇总及计量字节不重复。

```sh
python3 -S -B -m unittest discover -s tests/audit -p 'test_projset_usage.py'
```
