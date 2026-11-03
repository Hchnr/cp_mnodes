# 中文

## 标题

新增分布式全局迁移核对、扫描均衡与 24 节点安全交接

## 变更说明

- 增加 24 节点、48 个逻辑扫描分片的全局元数据核对，控制证据和可恢复扫描前沿保存在共享存储。
- 支持排空后接续及长尾目录再均衡，验证分片所有权、前沿闭合和日志结束回执，避免重复计数及遗漏。
- 对唯一候选结果分布式复核，严格绑定 project 范围、raw 排除项及小于 20 TB 的授权门槛。
- 增加普通文件分片顺序交接与远端退出后核验，沿用清单、回执前缀和累计计数。
- 监控区分逻辑分片与实际执行节点；临时缺失或落后的子报告沿用整份上次观测，避免显示进度倒退。
- 记录 24 节点上线流程、身份验证及完整性证明边界。

## 验证

- audit 测试按模块独立运行：30 个模块、144 项测试全部通过。
- 同进程全量 discover：143 项通过、1 项失败；失败项单独运行通过，存在跨模块状态污染，尚未修复。
- 本次提交未启动、停止或重新部署运行任务，也未执行数据删除。

## 边界

- 全局核对属于在线元数据审计，不是原子快照，也不证明所有文件内容哈希相同。
- 流水线在候选复核后等待命名空间闭合；20 TB 门槛不代表已实现自动补拷或已满足最终执行条件。
- 部分运维入口绑定既有集群、挂载身份和冻结工具目录，不是通用部署入口。
- 不包含独立容量盘点报告、大型 CSV、删除路径清单及此前 PR 描述的整理改动。

# English

## Title

Add distributed migration auditing, scan rebalancing, and safe 24-node handoff

## Changes

- Add a global metadata audit with 48 logical scan shards across 24 nodes, retaining durable evidence and resumable frontiers on shared storage.
- Support drained continuation and long-tail rebalancing, validating ownership, frontier closure, and terminal log receipts to prevent omissions and double counting.
- Revalidate deduplicated findings across nodes with explicit project scope, raw exclusion, and a strictly-below-20-TB authorization gate.
- Add sequential file-shard handoff and receiver-side post-exit verification while preserving manifests, receipt prefixes, and cumulative counters.
- Distinguish logical shards from execution nodes in monitoring; retain whole previous observations when child reports are temporarily missing or older.
- Document the 24-node rollout, identity checks, and limits of completeness claims.

## Validation

- Module-isolated audit tests: all 144 tests across 30 modules passed.
- Single-process discovery: 143 passed and one failed; the failing test passes in isolation. Cross-module state contamination remains unresolved.
- This commit did not start, stop, or redeploy running jobs, or delete data.

## Limitations

- This is an online metadata audit, not an atomic snapshot or an all-file content-hash verification.
- The pipeline stops for namespace closure after candidate review; the 20 TB gate neither implements automatic copying nor proves that final execution conditions are met.
- Some operational entry points bind existing cluster identities, mount identities, and frozen tool directories; they are not generic deployment interfaces.
- Independent storage reports, large CSV exports, deletion manifests, and earlier PR-description edits are excluded.
