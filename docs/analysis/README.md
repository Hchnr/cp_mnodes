# 分析报告索引

| 报告 | 时间与范围 | 口径和限制 |
|---|---|---|
| [空间分布归档](storage-distribution-20261027/README.md) | 2026-10-27，project/projset | 历史逻辑容量、目录和大文件分布；不是当前复制任务的全量分母。 |
| [数据集名称重叠](dataset-overlap-20261102/README.md) | 2026-11-02，候选目录 | 名称/数据集家族相似性，不证明内容相同、子集关系或可安全删除。 |
| [压缩包与解包证据](dataset-overlap-20261102/ARCHIVES.md) | 2026-11-02，压缩包目录 | 只读元数据及历史回执；不解包、不hash数据、不授权清理。 |
| [projset深度5计量](projset-depth5-20261102/README.md) | 2026-11-02启动 | 分配字节、硬链接按路径累计；运行状态以本机控制记录为准。 |

工具源码位于 [ops/analysis](../../ops/analysis)，测试位于 [tests/audit](../../tests/audit)。静态JSON/CSV为当时输出，源码重跑会产生新的在线观测，不能将旧快照当成当前完整性证明。
