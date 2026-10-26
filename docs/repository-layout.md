# 仓库结构与兼容运行包

## 目录职责

```text
cp_mnodes/
├── src/
│   ├── core/          # 数据复制、broker、任务协调及基础设施
│   ├── discovery/     # 源扫描、预检、任务生产流水线
│   ├── recovery/      # 恢复索引、检查点、回执与历史接续
│   ├── audit/         # 固定清单复查、补拷核验与目录交接
│   └── monitoring/    # 状态、吞吐、报告与观测
├── ops/
│   ├── deployment/   # 历史部署、准备、滚动交接与收尾脚本
│   ├── diagnostics/  # 检查、观察与诊断脚本
│   └── benchmarks/   # 性能实验和 Linux 集成夹具
├── tests/            # core/discovery/recovery/audit/monitoring
├── tools/            # repo.py 与构建工具测试
└── docs/             # 架构、设计、历史记录和双语 PR 说明
```

此分层用于代码导航，不是 Python 包重构：旧模块之间仍使用原有顶层导入。跨功能模块按主要职责归类，不应从分类目录直接启动脚本或仅复制该分类部署。

## 为什么保留平铺运行包

原有部署器会通过 `__file__` 找到同级源码，冻结文件摘要并传到远端；测试也依赖同级入口及子进程的工作目录。改写所有导入、文件定位和冻结规则会扩大运行风险。因此本次只移动源文件，不改其字节，通过 `tools/repo.py` 构建兼容包：

1. 收集 `src/`、`ops/`、`tests/` 下的 Python 文件，遇到重复文件名立即拒绝。
2. 在 `.build/runtime-*` 下创建独立目录，保持每个源文件名称、内容和权限；不覆盖已有目录。
3. 写入 `SOURCE_MAP.json`，记录来源相对路径及 SHA-256。
4. `build` 只构建；`test` 执行 unittest；仅 `run` 显式执行指定入口。

不使用符号链接，避免 `Path(__file__).resolve()` 回到分类源码目录破坏同级关系；不复用旧构建，避免残留模块。运行包是兼容开发/打包视图，**不是已获准的生产部署**。生产端仍须使用原有授权、代码摘要、配置和进程身份校验。

## 常用命令

```bash
python3 tools/repo.py list
python3 tools/repo.py build --output /absolute/path/to/new-bundle
python3 tools/repo.py test
python3 tools/repo.py test test_migration_reaudit_copy test_migration_namespace_rollout
python3 -m unittest discover -s tools -p 'test_*.py'
```

`--output` 必须是不存在的新目录。相对路径参数以运行包为基准；外部配置、控制目录、清单和输出位置应传绝对路径。测试产物可能保留在构建目录，失败时可直接检查；`.build/` 不纳入 Git。不要在入口仍运行时移除其运行包。`run` 返回子进程退出码，未增加后台启动、自动重试或自动部署行为。

默认 `test` 将每个测试模块放入独立 Python 进程，所有模块执行完后汇总失败模块并返回非零退出码。这是因为历史测试会安装进程级复制引擎适配器，同一解释器的全量 discover 存在模块间状态串扰；显式传入 unittest 参数时则原样执行，便于复现。仅隔离测试进程，不改变任何生产代码。

另外三个历史测试是 `main()` 断言夹具而不是 unittest 用例：`test_migration_fresh_resume`、`test_migration_seed_handover`、`test_migration_seed_handover_guards`。默认入口通过显式白名单以脚本执行它们，不将“发现零用例”误报为通过，也不会对其他失败模块自动改换执行方式。

历史文档保留当时入口名称和命令作为记录，不代表根目录仍有同名文件，也不代表可对当前任务直接执行。可用 `list` 找到源文件，用 `run` 调用兼容入口。现有共享监控命令不变。

## 维护原则

- 新模块放到对应功能目录，文件名在整个运行包内保持唯一。
- 新测试放到对应的 `tests/` 子目录，通过统一入口运行。
- 运维脚本有实际状态变更风险，目录名称不构成安全承诺；必须检查脚本和参数。
- 不在源码目录保存控制状态、数据、凭据、运行日志或回执。
- 这次重组不修改冻结的运行侧代码，不重新派发任务，不重启复制。
