# PR 2 — 仓库分层与兼容入口 / Repository organization and compatibility tooling

比较基线 / Comparison base: `c39f4d7`（第一份功能提交 / the first functional commit）。如果拆成两个 PR，请以第一个 PR 分支为本 PR 的基线，避免重复展示功能改动。

## 中文

### 标题
按功能重组源码、运维脚本和测试，增加兼容运行包入口

### 变更
- 将根目录平铺的 Python 文件整理至 `src/`、`ops/` 和 `tests/`，按功能继续分层。
- 增加 `tools/repo.py`，统一提供文件索引、独立运行包构建、测试及显式脚本运行。
- 运行包保持原有文件字节和同级文件布局，记录来源路径和 SHA-256；拒绝文件名冲突和覆盖已有输出。
- 更新 README、目录设计及调用方式；修正把历史固定批次误称为当前全范围的导航描述。
- 默认测试逐模块隔离进程，避免历史测试安装的全局引擎适配器相互污染；显式 unittest 参数仍原样传递。

### 兼容性与风险
仓库根目录的旧脚本路径已迁移，应使用统一入口或生成的运行包；不直接运行分类目录中的脚本。此改动不是生产部署，不修改冻结控制目录、复制进程、任务分配或校验逻辑。历史运维脚本仍有副作用，必须阅读约束并显式调用。

### 验证
- 379 个迁移文件与第一份提交的 Git blob 摘要逐一一致；381 个 Python 文件语法检查通过。
- 本地临时运行包逐模块隔离验证：1,370 项 unittest 通过；另行执行的 3 个历史 main 断言夹具均通过；新仓库工具测试 9 项通过。
- 首次同进程全量 discover 出现 25 项状态串扰失败，相关 46 项测试独立复跑通过；默认入口因此采用模块级进程隔离。原有资源未关闭警告仍存在，未借目录整理修改引擎代码。
- 共享盘全量测试在出现失败且夹具清理缓慢后中止，不计为通过；完整回归使用同字节本地临时包，正式测试日志保存在混闪。`git diff --check` 通过。

## English

### Title
Organize sources, operations, and tests by responsibility with a compatibility bundle runner

### Changes
- Move flat root-level Python files into categorized `src/`, `ops/`, and `tests/` trees.
- Add `tools/repo.py` for entry discovery, isolated bundle builds, testing, and explicit script execution.
- Preserve source bytes and the original sibling-file runtime layout; record source paths and SHA-256 hashes, reject duplicate basenames and existing output directories.
- Update navigation and invocation guidance, and clarify that the historical fixed batch is not the current whole-task scope.
- Isolate default test modules in separate processes to prevent historical process-global engine adapters from leaking between modules; explicit unittest arguments are still forwarded.

### Compatibility and risk
Old repository-root script paths have moved. Use the runner or a generated flat bundle instead of executing categorized sources directly. This is not a production deployment: frozen controls, copy actors, allocation, and verification logic are unchanged. Historical operations scripts still require explicit review and execution.

### Validation
- All 379 moved files retain their original Git blob hashes; syntax checks passed for 381 Python files.
- Module-isolated validation in a disposable local bundle passed all 1,370 unittest cases. The three standalone assertion fixtures and nine new repository-tool tests also passed.
- An initial single-process discovery run had 25 cross-module state failures; the affected 46-case subset passed independently. Default testing now isolates modules. Existing unclosed-resource warnings remain; no engine changes were folded into this reorganization.
- The shared-filesystem full run was interrupted after a failure and slow fixture cleanup and is not counted as passing. Complete regression used byte-identical local sources, with authoritative test logs retained on mixed storage. `git diff --check` passed.
