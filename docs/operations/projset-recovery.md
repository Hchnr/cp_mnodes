# Projset 复制的恢复与守护

源 `/share-mixed/projset/`，目标 `/share-mixed/project/`，保持相对路径；只使用新增的 16 个 CPU 节点。目标已有冲突仍保留，不覆盖、不删除。源仍在线，因此这是一轮复制，不是原子快照。

## 部署位置

共享运行目录：`/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae`。

- `recovery/code/`：本次恢复程序、SwanLab 上报与清单导出程序的部署副本；原 `code/` 和历史日志不修改。
- `recovery/supervisor.json`：主守护进程状态、控制器 PID 和各上报进程 PID。
- `recovery/standby.json`：备用守护的心跳及主节点可达性。
- `status.json`：复制整体状态；恢复时为 `recovering`，工作时为 `copying`。
- `recovery/controller.<attempt>.log`：每次恢复的控制器日志；`last-error.json` 为最近的守护错误，历史错误文件不代表当前仍失败。
- `nodes/<node>/attempt-<attempt>/`：每次工作进程的身份、进度、回执和异常记录；旧回执保持不变。
- `recovery/tasks.<attempt>.jsonl`、`completed.<attempt>.jsonl`：新增任务与完成确认的持久日志。

## 自动恢复流程

主守护在 `cpu-master-0` 运行，与开发容器脱离。控制器异常退出后，10 秒轮询发现，重试至少间隔 30 秒；连接/隔离失败则间隔 60 秒重试。正常终态或用户创建 `STOP` 不自动重开新一轮。

`cpu-worker-0` 每 30 秒尝试拉起主节点守护，由主节点本地排他锁保证唯一。主守护每 120 秒同样尝试拉起备用守护。它们均通过固定主节点的本地锁协调，**不会在两个主机上竞选并行控制器**。所有 SSH 使用经现有免密通道采集的固定主机公钥。

每次控制器重启前先隔离旧控制器和全部工作进程。检查主机、启动 ID、PID 启动时钟及命令参数；仅对确认属于本运行的进程发送信号。工作进程终止后才允许重放租约；不可达节点导致恢复暂停重试，不把它的未确认任务交给另一节点冒险并发写入。各节点本地工作锁再提供一层排他保护。

队列从原始任务/完成日志和所有恢复日志重建，本地 SQLite 只作缓存。完成顺序可以有空洞，不以最大完成任务号推断进度。目录和文件相对路径都有去重索引，因此扫描中途断线后重扫目录不会重复生成既有文件任务。只忽略未换行的最后一条残缺追加记录；完整行损坏或完成引用缺失任务会中止恢复。

容量计数表示历史累计成功处理量；在线源后续变化不保证仍与目标一致。写入计数恢复时以持久化回执中的写入量累计，崩溃前未形成回执的部分写入不保证可重建，早期恢复批次无写入字段时以新复制逻辑大小估计。旧工作进程退出时的最终计数保留为 `original-counts.json`，后续从恢复回执累计。未确认文件任务中，已有成功回执会核对源、目标的 inode、大小、mtime、ctime；身份一致则复用回执，不重新读取文件内容，也不重复记账。身份变化的条目重新经过原来的不覆盖复制逻辑。已经确认的冲突、源变化和文件级错误不自动覆盖或重做；本机制恢复的是进程/连接故障导致的未确认工作。

节点消息 120 秒不更新会触发隔离恢复。控制器状态 10 分钟不更新会被守护终止并恢复，日志重建阶段允许 30 分钟。工作进程失去连接会停止接新任务，最多给现有复制 120 秒的收尾时间，然后触发引擎取消。强制终止时可能遗留本任务专属临时文件；不会以临时文件存在作为复制完成的证据。

允许原先已验证为同源的 `_admin` 子挂载出现或消失，任何异源子挂载仍拒绝。节点重启后，重新校验主机、两侧挂载、根 inode 与冻结引擎代码；允许 boot ID 更新。节点分配被替换、挂载发生变化或主节点长期不可达，需要运维修复；不自动跨节点接管主控，避免网络分区导致双写。内核不可中断的 NFS I/O 也不能保证靠用户态进程重启立即解除。

SwanLab 沿用原实验 `0euoq0dr`，单位 TB，批量上传每 120 秒一次；报告导出也每 120 秒增量更新，读取原始及所有恢复批次回执。两个进程分别受守护，保留原上报实验与导出检查点。

## 检查与人工启动

```bash
RUN=/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae
cat "$RUN/recovery/supervisor.json"
cat "$RUN/recovery/standby.json"
python3 -m json.tool "$RUN/status.json"
```

如果两个守护所在容器同时退出或重启，需先启动其中一个守护；没有系统级开机服务可保证二者同时消失后的自启动。如两个守护都不在，可在对应 CPU 节点用 `nohup` 启动；脚本排他锁拒绝重复实例：

```bash
# cpu-master-0
nohup python3 -S -B "$RUN/recovery/code/projset_recovery_supervisor.py" supervise --run "$RUN" </dev/null >>"$RUN/recovery/manual-supervisor.log" 2>&1 &
# cpu-worker-0
nohup python3 -S -B "$RUN/recovery/code/projset_recovery_supervisor.py" standby --run "$RUN" </dev/null >>"$RUN/recovery/manual-standby.log" 2>&1 &
```

`STOP` 为人工停止标记；处理恢复时不要删除历史日志、回执、`original-counts.json`，不要手工重置计数。创建 `STOP` 后由控制器收尾，守护不再启动下一轮。

## 验证

`python3 -S -m unittest discover -s tests/core -p test_projset_recovery.py -v` 验证乱序完成、未确认部分回执、截断日志尾部、重复重建不重复累计、目标变化时拒绝复用，以及 PID 启动时钟不匹配时禁止发送信号。

2026-11-03 实机验证：强制终止主守护后，备用节点自动拉起主守护，旧控制器被隔离，重建队列后16个节点全部恢复。北京时间19:44:54至19:46:35的两次样本中新复制量增加0.306004 TB，后一次累计222.072510 TB，当前连接失败为0；16个节点的 `oom`、`oom_kill` 均为0。容器内存包含大量文件缓存，不用 `memory.current` 直接推断复制进程RSS。SwanLab服务端已回读确认 step 628 的复制中状态和16个在线节点；上报继续每120秒刷新。

运行证据位于共享运行目录：`recovery/fault-test-final.json`、`health-after-recovery.json`、`verification-first.json`、`verification-latest.json` 和 `swanlab-confirmation.json`。开发验证覆盖复制、总览、恢复和导出的9项测试通过；最后针对恢复及导出的5项复查亦通过。
