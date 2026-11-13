# projset 独立复制工具

范围为 `/share-mixed/projset/<相对路径>` → `/share-mixed/project/<相对路径>`，不复用历史全闪迁移队列。当前运行使用16个CPU入口，每入口32个复制线程，源文件 SHA-256、目标临时文件读回校验、fsync 后无覆盖发布；已有不同目标保留并记账。空目录和符号链接包含在路径复制范围，不跟随链接遍历。

| 入口 | 职责 |
|---|---|
| `projset_copy.py` | `probe` 检查身份；`controller` 负责本地SQLite调度和共享清单；`worker` 执行有界扫描与复制；`status` 读取已有任务。controller/worker 只能针对已准备的独立控制目录显式启动。 |
| `projset_recovery_copy.py` | 从共享日志恢复队列，复用未确认任务的有效回执，去重扫描结果；仅由守护进程启动。 |
| `projset_parallel_receipts.py` | 将历史回执按逻辑节点并行重建并校验，失败时由恢复控制器回退串行流程。 |
| `projset_recovery_supervisor.py` | 主、备用守护按锁检查并拉起对方；接管已运行进程，控制器退出后隔离旧工作进程并恢复，同时维护上报和导出。 |
| `projset_retry_files.py` | 对显式清单中的普通文件限速重试，校验清单摘要和文件身份，保留冲突并持久记录回执；支持恢复版的有限 `ESTALE` 重试。 |
| `projset_progress.py` | 只读显示进度；默认读取本机 `runs/projset-copy-current.json`，也支持 `--run`、`--watch`、`--json`。 |
| `projset_swanlab.py` | 独立上报器，每120秒一次合并上报，复用持久run ID，异常退避；SwanLab环境与复制环境分离。 |
| `projset_overview.py` | 总览纯函数：已确认容量、速度、节点消息新鲜度、异常和状态表。未掌握完整逻辑容量时不生成百分比或ETA。 |
| `../diagnostics/projset_node_health.py` | 只读采集进程身份、cgroup配额、CPU限流、内存和OOM事件，保留内核日志权限不足的边界。 |
| `../diagnostics/projset_outcomes.py` | 从固定回执前缀增量导出成功复用与异常路径、容量和原因；支持每120秒刷新报告，不自动重试、覆盖或删数据。 |
| `../diagnostics/projset_error_reconciliation.py` | 关联历史错误与后续扫描、补拷和校验回执，导出待关注清单；证据不足或冲突时保持待关注。 |
| `../diagnostics/projset_conflict_links.py` | 在限定的源、目标根内只读解析链接冲突；不同树的内容仍需另行校验。 |
| `../diagnostics/projset_conflict_files.py` | 对选定的同大小普通文件冲突限速只读两轮 SHA-256；限制读取量并检查身份变化。 |
| `../diagnostics/projset_usage.py` | 只读GNU du分布式计量与深度汇总；分配字节口径不能直接作复制逻辑容量分母。 |

开发验证只使用私有临时目录及本地模拟节点，不连接生产broker或启动实际复制：

```sh
python3 -S -B -m unittest discover -s tests/core -p 'test_projset_*.py' -v
python3 -S -B -m unittest discover -s tests/audit -p 'test_projset_*.py' -v
python3 -S -B tools/repo.py test test_projset_copy test_projset_overview test_projset_usage test_projset_outcomes test_projset_recovery test_dataset_archives
```

[运行记录](../../docs/operations/projset-copy-20261102.md)、[图表逐项说明](../../docs/operations/projset-swanlab-charts.md)、[跳过/异常清单](../../docs/operations/projset-copy-outcomes.md)。这些记录描述已有任务，代码提交不会部署或重启它。

共享任务/回执是恢复权威；本地SQLite只是调度缓存。初版入口不具备自动恢复；当前运行已部署[恢复守护机制](../../docs/operations/projset-recovery.md)，隔离旧进程后恢复未确认任务。不会自动第二轮补扫或重试已确认异常。目录元数据及硬链接拓扑未完成最终同步；在线源复制不构成时间点快照。
