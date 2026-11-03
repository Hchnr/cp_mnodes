# PR — 完整复查、安全补拷与仓库分层 / Re-audit, safe copying, and repository organization

覆盖提交 / Included commits: `c39f4d7`, `a9bd6b6`。
比较基线 / Comparison base: `4731f52`。

## 中文

### 标题

完善全量复查、安全补拷与跨节点交接，并按功能重组仓库

### 变更

- 完善目录捕获、增量复查、清单闭合、来源追踪和独立补拷流程。
- 对已结束的异常回执执行有界重试，绑定原清单、文件身份和累计容量门槛。
- 增加复制后核验和进程退出观察器，明确区分已搬运、身份已核验与仍需处理的内容。
- 增加目录分片顺序交接，要求旧进程实际退出、回执前缀吻合，并继承累计计数。
- 扩展扫描、补拷、核验和交接监控，记录证据及完整性证明的边界。
- 将 379 个平铺文件整理至按功能分类的 `src/`、`ops/` 和 `tests/`，更新 README、源码链接及目录说明。
- 新增 `tools/repo.py`，提供入口索引、兼容运行包构建、测试及显式脚本运行；保留同级文件布局和源文件字节，记录来源及 SHA-256，拒绝文件名冲突和覆盖已有输出。
- 默认测试逐模块使用独立进程，避免历史全局引擎适配器串扰；显式识别三个独立断言夹具，避免漏测。

### 兼容性与安全边界

- 根目录旧脚本路径已迁移，应使用统一入口或生成的运行包，不直接启动分类目录中的脚本。
- 代码提交不代表部署：本次没有停止、重启或部署实际复制任务，也不修改既有冻结控制目录。
- 保留目标冲突保护、复制校验及交接身份约束；不宣称在线变化的源目录具有原子快照或全量内容一致性。
- 历史运维脚本仍可能修改状态，必须阅读约束并显式调用；控制状态、日志、回执、凭据和数据不纳入 Git。

### 验证

- 功能提交前：87 项复查测试和 31 项重点测试通过，两组存在重叠。
- 重组后：379 个迁移文件与功能提交的 Git blob 摘要一致；381 个 Python 文件语法检查通过。
- 在同字节本地临时运行包中，逐模块隔离的 1,370 项 unittest、另外 3 个独立断言夹具及 9 项仓库工具测试通过。
- 首次同进程全量 discover 有 25 项状态串扰失败，相关 46 项独立复跑通过；默认入口改用模块级进程隔离。原有资源未关闭警告仍存在。
- 共享盘全量测试在失败且夹具清理缓慢后中止，不计为通过；完整回归日志保存在混闪。`git diff --check` 通过。

## English

### Title

Extend re-audit, safe manifest copying, and shard handoff; organize the repository by responsibility

### Changes

- Extend directory capture, incremental verification, manifest closure, provenance checks, and isolated late-copy workflows.
- Retry closed attention receipts in bounded batches, binding original manifests, file identities, and cumulative size limits.
- Add post-copy verification and process-exit watchers, distinguishing transferred, identity-verified, and unresolved work.
- Add sequential namespace shard handoff with actual old-process exit fencing, exact receipt-prefix validation, and cumulative counter continuity.
- Expand scan, retry, verification, and handoff monitoring, documenting evidence and completeness limitations.
- Organize 379 flat files into categorized `src/`, `ops/`, and `tests/` trees; update navigation, source links, and layout documentation.
- Add `tools/repo.py` for entry discovery, compatibility bundle builds, tests, and explicit execution. Preserve source bytes and sibling-file layout, record source paths and SHA-256 hashes, and reject duplicate filenames or existing output directories.
- Isolate default test modules in separate processes to prevent global engine-adapter leakage; explicitly run three standalone assertion fixtures rather than silently omitting them.

### Compatibility and safety boundaries

- Old repository-root script paths have moved. Use the runner or a generated flat bundle rather than executing categorized sources directly.
- Committing code is not deployment: no live copy tasks were stopped, restarted, or deployed, and existing frozen controls were not modified.
- Existing-target conflict protection, copy verification, and handoff identity checks remain. No atomic snapshot or whole-tree content equality is claimed for a changing source.
- Historical operations scripts may mutate state and require explicit review and execution. Runtime state, logs, receipts, credentials, and datasets are excluded from Git.

### Validation

- Before the functional commit, all 87 re-audit tests and 31 focused tests passed; these sets overlap.
- After reorganization, all 379 moved files retained their functional-commit Git blob hashes; syntax checks passed for 381 Python files.
- Module-isolated validation using byte-identical local sources passed all 1,370 unittest cases, three additional standalone assertion fixtures, and nine repository-tool tests.
- Initial single-process discovery had 25 cross-module state failures; the affected 46-case subset passed independently. Default testing now isolates modules. Existing unclosed-resource warnings remain.
- The shared-filesystem full run was interrupted after a failure and slow fixture cleanup and is not counted as passing. Complete regression logs remain on mixed storage. `git diff --check` passed.
