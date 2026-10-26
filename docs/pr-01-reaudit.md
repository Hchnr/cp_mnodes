# PR 1 — 完整复查、有限补拷与安全交接

## 中文

### 标题
完善全量复查、固定清单补拷、跨节点交接和累计进度监控

### 变更
- 增加目录捕获、增量复查、清单闭合、来源追踪及独立补拷流程。
- 为已经结束的异常回执提供有界重试，绑定原清单、文件身份和累计容量门槛。
- 增加复制后核验及等待进程实际退出的观察器；区分已搬运、身份已核验和仍需处理。
- 增加目录分片交接与顺序推广，要求旧进程退出、回执前缀吻合并继承累计计数。
- 扩展监控，显示扫描、补拷、核验和交接状态；记录复查过程及证明边界。

### 安全边界
提交代码不代表部署或启动任务；保留既有目标冲突保护及复制校验，不宣称在线源目录具有原子快照或全量内容一致性。运行控制目录、日志、令牌和数据不提交。

### 验证
复查测试发现集 87 项通过；交接、目录核验和有界重试重点集 31 项通过（两组存在重叠）；`git diff --check` 通过。

## English

### Title
Extend full re-audit, bounded manifest retries, safe shard handoff, and progress reporting

### Changes
- Add directory captures, incremental verification, manifest closure, provenance checks, and isolated late-copy workflows.
- Retry closed attention receipts within bounded batches, binding original manifests, file identities, and the cumulative size gate.
- Add post-copy verification and watchers that require actual process termination; distinguish transferred, identity-verified, and unresolved work.
- Add sequential namespace shard handoff with old-process exit fencing, exact receipt-prefix validation, and cumulative counter continuity.
- Expand monitoring for scan, retry, verification, and rollout states; document evidence and its limitations.

### Safety boundaries
This commit does not deploy or launch copying. Existing-target conflict protection and copy verification remain in place. It does not claim an atomic snapshot or whole-tree content equality for a changing source. Runtime controls, logs, credentials, and datasets are excluded.

### Validation
All 87 re-audit discovery tests and all 31 focused handoff, namespace post-check, and bounded-retry tests passed (the sets overlap). `git diff --check` passed.
