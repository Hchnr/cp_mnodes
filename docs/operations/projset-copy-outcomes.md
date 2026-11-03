# projset 复制：跳过与异常路径清单

[实时更新报告](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/REPORT.md) 每120秒增量导出。报告时间和 `summary.json` 的 `updated_beijing` 是新鲜度依据；原始逐文件回执仍是事实来源。

| 清单 | 范围 |
|---|---|
| [skipped-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/skipped-paths.csv) | 目标已有、内容核对相同的成功复用路径。无需因为 skipped 状态再次搬运。 |
| [conflict-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/conflict-paths.csv) | 同名目标的内容、类型或符号链接文本不同，按授权保留目标。 |
| [changed-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/changed-paths.csv) | 源文件/目录在扫描、读取或发布前发生变化。 |
| [error-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/error-paths.csv) | 读写、权限、扫描、NFS 等错误结果。 |
| [attention-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/attention-paths.csv) | 所有需要复核的结果，含可能与成功结果重叠的元数据警告。 |
| [directory-summary.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/directory-summary.csv) | 按类别和相对路径前3层分组的容量与记录数，便于先选大目录。 |

CSV提供源和目标绝对路径、类型、记录时源容量（精确字节及TB）、节点、原因、任务号、回执文件和字节偏移。异常行还包含当前两侧的类型、容量、符号链接目标及属性读取错误。当前属性只是只读 stat/readlink，没有重新读内容或做哈希，因此不能仅凭两侧大小相等断言内容相等。

符号链接容量仅为链接文本大小；被链接的大型数据目录容量不包含在内。目录/扫描错误可能没有可用的数据量，这种情况记为未知，不假装是零字节文件。

## 是否会自动重试、覆盖或更新

- 原始 `nodes/<node>/receipts.jsonl` 和 `issues.jsonl` 持续追加。
- 本报告与CSV现已安排每120秒增量更新；扫描的是已落盘回执前缀，导出过程中任务继续运行，报告不是原子快照。
- 当前复制 worker 对每个已分配条目调用一次复制函数。冲突、源变化及错误结果记录后，不会自动重试这些结果；没有自动第二轮补扫更新，也不会覆盖不同的目标。
- 报告刷新与重新复制是两件事。新增导出器不修改源/目标，不触发复制、覆盖、删除或元数据变更。

建议先依据 error/changed 清单判断是否可以在源稳定或NFS恢复后补拷；冲突需按每条路径决定保留哪个版本。历史异常结果不自动等于该路径当前仍缺失，需结合当前目标属性及内容复核。

## 可复现证据

控制目录：`/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae`。

导出目录：`/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/runs/projset-copy-outcomes-20261102T144640Z-4316ae`。

`selected-receipts.jsonl` 保存选中的原始回执及原位置；`checkpoint.json` 保存各节点读取偏移；`deployment.json` 保存冻结代码SHA和启动身份。导出进程运行在开发/控制节点，nice=10，不占16个CPU复制容器的线程池。`summary.json` 保存分类容量和当前控制器累计，二者采样时刻可能略有差异。

源码为 `ops/diagnostics/projset_outcomes.py`。私有目录测试覆盖半行、增量去重、派生缓存恢复、容量精度、源/目标链接对照及不修改目标文件。
