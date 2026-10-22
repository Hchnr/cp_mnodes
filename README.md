# cp_mnodes：多节点共享存储复制工具

这是本次全闪到混闪迁移使用的 Python 代码归档，包含复制引擎、恢复机制、监控、测试及历次运维脚本。本文于 2026-10-22 按本地代码更新；归档内容、已部署版本和任务实际运行状态需要分别核对。上传代码本身不会启动、暂停或修改正在运行的复制任务。

## 仓库导航

- [设计文档](docs/design.md)：架构、断点续传、性能瓶颈与对应优化，以及仍需验证的边界。
- `migration_*.py`：复制、扫描、协调、状态与观测模块；`test_*.py`：对应的本地测试。
- 历史 `deploy_*`、`start_*`、`rollout_*` 等脚本是运维记录，不是安全的通用启动入口；请先读下方“历史运维脚本”。

本仓库只管理源码和文档。任务控制目录、回执、令牌、数据库、运行日志和测试临时目录均不纳入 Git。针对运行中的任务，先使用下方的只读监控命令；不要把 `git clone` 或 `git pull` 当作部署/重启操作。

## 环境与数据边界

- 运行节点使用 Linux，已验证环境为 Python 3.12。核心引擎只依赖 Python 标准库；需要系统提供 SSH、`nice`、`ionice`，以及可用的共享存储挂载。
- 所有执行节点必须看到相同的源目录、目标目录和共享控制目录。主节点还使用本机 SQLite/WAL 和 `flock`；这些本机文件不能当作跨节点共享锁。
- 任务只复制数据，不移动或删除源文件。隐藏文件、目录结构和符号链接在扫描范围内；目录元数据和硬链接另有收尾步骤。特殊对象及无法保留的元数据会记录为问题，必须检查最终结果。
- 已有目标内容需要核对。不同内容且不属于本任务的目标记录为冲突；只有符合本任务所有权与当前文件身份检查的副本才允许更新。
- 源目录在线变化时，复制不构成某一时刻的一致快照，需要后续轮次补扫并说明仍在变化的文件。

## 架构

```text
并行目录扫描 / 目标存在性检查
              │ 任务批次
              ▼
主节点 HTTP broker + 本机 SQLite 唯一分配
              │ 租约
              ▼
各节点 primary / supplement 复制进程
              │ 逐文件回执
              ▼
回执收集 / 历史索引合并 / 硬链接与目录元数据收尾

metrics / inventory / performance watch / progress：独立观测
```

NFS 上的 `rename`、`O_EXCL` 不用于跨节点任务竞争。broker 的 SQLite 记录是分配权威，重试必须保留任务 ID、owner 和租约语义。

数据复制由 `migration_worker.py` 的文件读写实现，不是调用 `cp` 或 `rsync`。新复制文件在读取源内容时计算 SHA-256，再读取目标临时文件计算 SHA-256，对比成功、检查源身份未改变并 `fsync` 后发布。已有副本可能经过内容比较，或在源和目标身份均符合历史成功回执时复用验证记录；“确认副本”不表示每一轮都重新完整读取所有文件。

断点恢复以文件为单位：持久成功回执可复用；没有完整成功证明的文件可能需要整文件重拷。恢复时沿 `resume_from` 祖先链重建索引，不能仅依赖已回收节点的 `/tmp`。

## 模块与入口

| 文件 | 用途 |
| --- | --- |
| `migration_worker.py`、`migration_run.py` | 文件复制、内容校验、限速、工作线程、回执和基础协调逻辑；基础 Coordinator 不是现有任务的通用重启入口 |
| `migration_cached_worker.py`、`migration_state_cache.py` | primary / supplement 的缓存包装入口；减少重复状态读取，过期时停止领取新任务，STOP 仍直接检查 |
| `migration_worker128_run.py` | supplement 进程及独立 owner 区间；与 primary 合并为物理节点的总并行度 |
| `migration_broker.py`、`migration_broker_parallel.py` | 主节点任务分配、幂等回执和并行控制文件处理；后者是并行 broker 入口 |
| `migration_scan.py`、`migration_parallel_scan.py` | 按优先级扫描并生产批次，支持并行目录扫描与有界队列 |
| `migration_pipeline.py`、`migration_pipeline_v2.py` | 多轮扫描复制、回执收集、硬链接和目录元数据流水线 |
| `migration_parallel_pipeline.py`、`migration_parallel_pipeline64.py` | 并行扫描协调器及保留既有任务的交接机制 |
| `migration_fresh_resume.py` | 新控制目录的恢复入口；要求前任任务隔离证明，拒绝重复初始化已有索引 |
| `migration_overlap_resume.py` | 缺失目标优先续传与历史索引恢复并行进行 |
| `migration_overlap_parallel_filter.py` | 并行检查目标是否存在的 producer；接管原 seed helper、队列、租约和本机索引 |
| `migration_distributed_target_filter.py`、`migration_target_probe.py` | 把只读目标存在性和源元数据检查分散到多个节点；服务有认证和并发边界，网络或协议失败回退本地检查，不把失败当作存在或缺失。当前 probe 的范围限定于原 eai_pwm 的优先级 3 |
| `migration_resume_index.py`、`migration_resume_index_parallel.py`、`migration_resume_index_parallel_memory.py` | 从祖先回执恢复验证索引；分别保留基准、并行读取及内存 TEMP 优化实现 |
| `migration_seed_handover.py` | 在有完整交接证据时接管已有恢复工作，不是任意状态下可重跑的启动器 |
| `migration_receipt_prefetch.py` | 有界只读回执预取辅助程序；不替代验证或索引构建 |
| `migration_io.py` | 状态 JSON 的不可变快照读写 |
| `migration_nodes.py` | 合并主、辅助进程为物理节点，保留各组件心跳与身份 |
| `migration_inventory.py`、`migration_metrics.py` | 独立清点和速率采样 |
| `migration_performance.py`、`migration_performance_watch.py` | 按工作负载和连续有效采样判断性能；观测程序不自动增加并发或重启复制 |
| `migration_progress.py` | 文本 / JSON 监控，含恢复索引、后台任务、四项范围和物理节点表格 |
| `migration_watchdog.py` | 只读观察训练日志与进程，必要时写入复制 STOP；不修改或重启训练 |
| `migration_metadata.py`、`migration_report.py` | 元数据辅助处理与过程 / 最终报告归档 |
| `migration_control_observation.py` | 只读统计控制目录中的队列、问题文件名称和大小，不打开 JSON 或数据内容；读取失败明确报告，结果不是原子快照 |
| `profile_copy_proc_only.py` | 通过真实进程的 PID、启动时间和 argv 绑定，只读采集 /proc 证据 |
| `migration_priority4_scan.py`、`migration_priority4_worker.py`、`migration_priority4_broker.py` | 独立 P4 范围的扫描、工作进程边界与分配组件，精确排除顶层 eai_pwm，并保护控制目录；不是完整启动器 |
| `migration_priority4_resume.py`、`migration_priority4_index_merge.py` | P4 同范围祖先回执恢复，以及带范围绑定的分批索引合并；保留已有索引行，不以原 eai_pwm 相对路径直接续接 P4 |
| `migration_priority4_metadata.py`、`migration_priority4_file_metadata.py`、`migration_priority4_hardlink_proof.py` | P4 目录收尾、基于 FD 的普通文件元数据观测和当前硬链接内容/所有权证明；只读证明不等于发布或元数据验收完成 |
| `migration_priority4_dependency_records.py`、`migration_priority4_process_observation.py` | P4 前置依赖的保存记录预检及只读进程证据；任何单项通过都不能授权启动 P4 |
| `migration_launch.py`、`migration_status.py`、`read_progress_once.py`、`profile_copy_workers.py` | SSH 传输与只读检查工具；使用调用方提供的 active 指针和 SSH 身份 |
| `test_*.py` | 并发、恢复、交接、监控与性能判断的本地验证用例 |

入口会随安全交接演进。确认一个正在运行的任务，应读取其最新 `pipeline-deployment.json`、相应部署记录，以及真实进程的 PID、启动时间和完整 argv；不能只看旧 active 字段、脚本名称或 PID 数字。

## 本次归档新增的工作

- 跨节点只读 target probe、producer 客户端及相关部署/观测脚本，缓解主节点集中检查目标元数据的压力。它们不替代复制后校验，也不扩大既有 probe 的路径范围。
- cache age 的单节点、单 lane canary、空闲窗口检查、身份绑定和进程观测工具，例如 `rollout_cache_age.py`、`observe_cache_canary.py`、`inspect_cache_lane.py`。缓存刷新失败或超过配置的新鲜度仍停止领新任务；不能因代码目录出现这些脚本就推断所有节点已更换参数。
- P4 独立组件、前置依赖记录/进程预检、硬链接当前内容与元数据联合证明、索引合并，以及相应本地与 Linux 集成 fixture。模块标注 `UNDEPLOYED` 的组件仍按候选代码理解；fixture 通过不等于完成生产交接或全量验收。
- 控制队列只读目录观测与精确进程观测。错误必须保留，不能把无法读取的状态显示为零或“已排空”。

这些新增内容描述的是归档中的能力与研究/验证过程。实际部署参数、当前入口和某项优化是否启用，仍以对应运行控制目录中的最新记录与实际进程为准。

## 配置、状态与监控

代码目录和运行控制目录是两个概念。控制目录包含任务专属 `config.json`、manifest、回执、状态快照和交接证据。代码归档不提供运行令牌，也不能替代控制目录或其祖先回执。

使用现有控制目录时，所有可变 JSON 必须通过下列接口读取最新版本：

```python
from pathlib import Path
from migration_io import read_json

state = read_json(Path("<控制目录>") / "state.json")
```

`migration_io.write_json` 以不可变快照发布更新，canonical 文件可能只是首次创建的旧版本。不要直接 `json.load` canonical 状态文件，不要以 `os.replace` 改写可变状态路径，也不要清理 `.snapshots` 历史。

只读监控示例，在代码目录执行并提供已存在的控制目录：

```bash
python3 migration_progress.py --control <控制目录> --watch
python3 migration_progress.py --control <控制目录> --json
```

已经安装稳定监控入口的执行节点，也可使用：

```bash
python3 /share-mixed/project/.hcr-copy-monitor/progress.py --watch
```

监控的历史恢复量与本轮确认量可能重叠，不能相加。未完成全范围清点时，总规模只能是下限，不能给出完整百分比或准确 ETA。逻辑传输字节包含稀疏文件长度，不等于存储物理带宽。

## 已登记的迁移范围

1. `/share/project/eai_pwm/home/hcr` → 对应的 `/share-mixed/project/eai_pwm/home/hcr`。
2. `/share/project/eai_pwm/repos/cosmos-framework-pwm` → 对应混闪路径。
3. `/share/project/eai_pwm` 的其余内容，排除前两棵已覆盖子树。
4. `/share/project` → `/share-mixed/project`，登记为后继范围；精确排除顶层 `eai_pwm`，复用前三项最终证据并避免重复计算。

第四项的持久计划是 `extension-plan.json`。P4 的扫描、worker、broker、恢复、元数据及验证组件已经在库中，但它们仍不构成一个已经完成生产交接的独立协调器。是否已启动，应读取计划、最新实际状态及进程证据；不能由归档文件的存在推断。原 1–3 阶段引擎不能仅改源路径就直接执行第四项，P4 必须使用独立控制目录、阶段与索引，完成前置验收和幂等交接。没有 `/home` → `/home-mixed` 迁移任务。前三项完成不等于整个任务完成。

## 历史运维脚本

`deploy_*.py`、`start_*.py`、`prepare_*.py`、`recover_*.py`、`restart_*.py`、`rollout_*.py`、`repair_*.py`、`migration_*_launch.py`、`benchmark_*.py` 等保留了本次操作过程，可能带有固定控制目录、节点身份、旧入口或一次性交接假设；其中一些在导入时就执行操作。

不要批量导入这些文件，也不要在新机器上直接重跑。上传本归档不代表授权重新部署、恢复旧 benchmark、改变 active 指针、启动 broker 或复制进程。确需恢复时，应先确认旧写入进程隔离、核对持久回执与交接记录，再使用独立控制目录或经过验证的显式交接；不得直接重跑旧 Coordinator 构造器删除现有索引。

最终验收必须检查最新各轮的真实 summary / finalize、目录元数据、未解决 issues、全部队列和后台进程。`complete_with_changes` 需要逐项审阅差异，不能当作所有文件无条件一致。

本目录可能保留 `UPLOAD_INFO.json`、`SYNC_INFO_*.json` 和 `SHA256SUMS`，它们是上传时的历史快照记录，修改源码或文档后不再代表当前工作树，故不纳入 Git。运行控制目录中的回执、状态与访问令牌单独保留，不属于本代码仓库。
