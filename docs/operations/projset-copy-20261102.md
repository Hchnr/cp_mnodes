# projset → project 复制（2026-11-02）

北京时间 2026-11-02 22:47 启动。全量任务正在运行，不能将启动、已发现任务数或清点容量视为完成证明。

源 `/share-mixed/projset`，目标 `/share-mixed/project`。保持相对路径，例如
`/share-mixed/projset/eai_pwm/abc` → `/share-mixed/project/eai_pwm/abc`。
包括隐藏文件、空目录及符号链接；不跟随符号链接遍历。特殊对象记录为 unsupported。

用户明确选择：只使用新增 `cpu-master-0`、`cpu-worker-0`～`cpu-worker-14`；已有不同内容的目标保留并记录冲突。原有 20 个节点及其清点任务不变。16 个入口具有独立容器 hostname，11 个不同的宿主机 boot ID；不能宣称 16 台独占物理宿主机。

## 进度

SwanLab 私有实验：<https://swanlab.cn/@hchnr/data-migration/runs/0euoq0dr>

本地只读监控：

```sh
python3 -S -B /share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/ops/copy/projset_progress.py --watch
```

机器可读输出用 `--json`。当前任务指针为 `runs/projset-copy-current.json`，不复用任何旧迁移 active 指针。

控制目录：

```text
/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae
```

`status.json` 每约 5 秒更新。分别显示新复制并校验完成、目标已有相同内容、冲突、源变化、错误、特殊对象和元数据警告。`payload_written` 包含尚未发布的在途副本，不能当作已完成量。速率按逻辑字节计量，不是物理磁盘带宽。

目录发现与复制交错；尚未完成全量发现时没有准确分母或 ETA。原只读清点的分配字节口径与复制逻辑字节口径不同，不能直接相除作百分比。

## 执行与校验

- 每入口 32 个复制线程，共最多 512；每入口最多 1 个目录扫描任务及 3 个文件批次。
- 每批最多 128 个条目，或累计到 256 MiB 后派发；单个超大文件仍是一个任务条目。
- 扫描和复制不跟随祖先目录符号链接；发现与实际打开时校验源身份。共享根 inode、导出地址、容器 hostname 和 boot ID 在每节点启动时绑定。
- 个别节点的目标 `_admin` 子挂载只有在 NFS 导出与父导出及相对路径严格一致时允许；其他子挂载拒绝。
- 复用冻结的保守复制核心：读取源计算 SHA-256，临时副本读回校验，fsync 后 link-no-replace 发布。已有普通文件读取两侧内容确认，内容不同则不替换。
- 单文件冲突、变化、权限或 I/O 问题写回执并继续；不移动、删除源文件，不删除目标多余文件。
- 普通文件、符号链接沿用引擎元数据处理。目录保证存在；目录原元数据和硬链接拓扑未做最终同步，`hardlink_paths` 单独计数。
- 这是在线源的一轮路径复制，不是时间点快照；有变化或冲突需依据结果单独复核。任务终态不会自动宣称全量验收通过。

## 持久证据与恢复边界

- `config.json`：冻结代码 SHA、范围、节点身份和授权选择。
- `code/`：冻结运行代码，不随仓库编辑热改。
- `tasks.jsonl`：先持久化再派发的任务清单，包括源文件属性。目录任务按相对路径去重。
- `completed.jsonl`：worker 将回执 fsync 后确认的完成任务。
- `directories.jsonl`：已遍历目录及源属性。
- `nodes/<node>/receipts.jsonl`：逐条结果、源与目标身份、SHA 和时间。
- `nodes/<node>/issues.jsonl`：冲突、源变化、错误、特殊对象和元数据警告。
- `nodes/<node>/launch.json`：PID、启动时间、hostname、boot ID 和 argv。
- `nodes/<node>/progress.json`：独立节点进度；日志在 `logs/`。

本地 SQLite 仅作调度缓存，位置记录在 `controller.json`；不在 NFS 上运行 SQLite。恢复权威是共享任务清单、完成记录和回执。不能以本地数据库丢失为由删除共享证据或从零重跑。当前入口拒绝第二次启动同一任务/节点；不自动跨节点重派状态不明的在途文件。恢复应先核实旧 PID/启动时间/argv 已退出，再重建未完成任务并校验已有成功副本；单文件只有完整发布才可复用，不承诺字节级续传。

## SwanLab

参考 Cosmos 的 `docs/swanlab_sidecar.md` 和 `tools/swanlab_wandb_bridge.py`，使用同一个独立 Python 环境和私有凭据/代理文件；不在日志、argv、配置或聊天中复制凭据值。

独立单实例上报器：`ops/copy/projset_swanlab.py`；冻结副本位于当前控制目录 `code/`。

- 项目 `data-migration`，独立私有 run，不写入已有训练实验。
- 每 **300 秒**仅一次批量标量 log；禁止配置更短间隔。
- 每 **900 秒**最多一次服务端 summary 回读；本地入队和服务端确认分别记录。
- 关闭硬件、环境、git、依赖等探测，不上传媒体或训练日志。
- 上传失败以 10 分钟起的指数退避重试，上限 1 小时；复制进程不依赖上报器。
- `swanlab/state.json` 保存固定 run ID、链接、上报时间和服务端确认 step。重启上报器复用 run；不产生密集历史补传。
- `swanlab/reporter.private.log` 是私有 SDK 日志；不要直接将内容发到公共聊天。

## 启动验证

3 项测试通过，包含真实双节点协议和 560 个文件的控制器/worker 集成，覆盖目录发现、分批无重复、完整任务/完成日志、隐藏项、空目录、符号链接、特殊对象、源变化、冲突保护和祖先符号链接拒绝。

```sh
python3 -S -B -m unittest discover -s tests/core -p test_projset_copy.py -v
```

启动前 16 个入口均完成双侧挂载及源码身份检查。线上 `testfile` 已经独立试拷并读回验证，证据为 `canary.json`。全量启动后确认 16/16 worker ready，无启动错误。

## 2026-11-02 23:26：SwanLab 单位更新

用户要求所有字节指标显示为 TB。容量使用十进制 TB（1 TB = 10^12 bytes），速率使用 TB/s，文件数保持原值。新指标使用 `copy/copied_TB`、`copy/skipped_TB`、`copy/payload_written_TB` 和 `rate_60s/write_TB_per_second` 等明确单位名称；旧 bytes/Bps 曲线只保留历史，不混写换算后的值。

只替换独立上报器，复用原 run ID `0euoq0dr`，复制 controller/worker 均未停止或重启。更新后的上报器位于控制目录 `swanlab/reporter-tb-20261102.py`，摘要及进程身份记录在 `swanlab/tb-update.json`；原冻结 `code/` 与配置摘要保持原样。上报器在重启后仍检查距上次入队的时间，保证间隔至少 300 秒。服务端 summary 查询异常单独记录，不改变正常数据上报频率。

## 2026-11-02 23:55：上报间隔改为两分钟

按用户最新要求，上报间隔由 300 秒改为 120 秒，保留 TB 和 TB/s 单位及原 run ID。只替换上报进程，复制进程未停止或重启；服务端 summary 回读仍每 900 秒，网络异常继续退避。当前上报入口为控制目录 `swanlab/reporter-tb-120s-20261102.py --interval 120`，部署证据为 `swanlab/interval-120s-update.json`。前文的五分钟间隔为历史配置。

## 删除旧字节图表

按用户明确要求，通过网页前端所用图表管理接口从 Default 视图删除 7 张旧图表：`copy/skipped_bytes`、`copy/payload_written`、`copy/copied_bytes`，以及 `rate_60s`/`rate_300s` 下的 `write_Bps` 和 `copied_Bps`。保留 TB、TB/s 图表及文件数、节点数等计数。执行计划、删除回执及删除前后图表清单位于控制目录 `swanlab/delete-byte-charts-*.json` 和 `swanlab/charts-*.json`。此操作删除看板图表，不删除逐文件复制回执或旧指标原始数据；复制与两分钟上报均继续。

## 2026-11-03：总进度总览

按用户要求新增并置顶“总览”分组，内含一张状态表和六条汇总曲线：已确认容量、新复制、已有相同、5分钟完成速度（TB/h）、在线节点、需复核结果。全量逻辑容量尚未确定；旧清点为 failed 且口径为分配字节，因此总量/百分比/ETA 明确显示尚未确定。

新上报代码冻结在控制目录 `swanlab/overview-v1-20261103/`，复制进程保持运行，每120秒上报到原实验。云端数据和布局均已回读核验；证据为该目录下 `server-verification.json` 和 `layout-verification.json`。完整逐图说明见仓库 `docs/operations/projset-swanlab-charts.md`。

## 2026-11-09：复制任务公平调度

运行采样显示，队列较小时按配置顺序分配会让前几个节点先占满复制槽。控制器现按每节点在途复制批次数从少到多分配，并轮转打破平局；目录扫描配额和每节点并发上限不变。部署使用现有 supervisor 的节点 fencing 与日志重建恢复，任务清单和逐文件回执仍为恢复依据。部署后应核对 16 个节点的复制计数增量是否更均匀、待复制批次是否有进展，以及确认进度是否连续。


## 2026-11-03：迁移至 CPU 节点并增加自动恢复

开发容器退出后原控制器丢失，CPU工作节点正常收尾，未见OOM kill。现从原共享日志恢复，由 CPU 主节点的独立守护管理控制器，CPU worker-0 的备用守护自动拉起主守护；SwanLab及清单导出同时纳入守护。原冻结代码和回执不变，新运行文件位于 `recovery/` 及各节点 `attempt-*`。架构、边界、路径和检查方式见[恢复说明](projset-recovery.md)。


## 2026-11-10：公平调度上线后验证

修复版控制器 PID 464751 于北京时间 11/09 17:16:41 开始恢复，19:28:03 重建结束，历时约 2 小时 11 分钟；11/10 11:10 检查时仍为同一进程。16/16 工作进程身份正确，全部 cgroup 的 oom/oom_kill 均为 0，内核检查未发现 OOM，工作进程 RSS 约 139–177 MiB。

110 秒线上采样中，16 个节点均完成 4131–4830 项，最忙四个节点占总处理量 26.3%；修复前的一次 28.8 分钟采样中，前四个节点占 98.6%。调度分布已明显改善，但两次采样的任务内容不同，不能将吞吐差异全部归因于修复。当前采样约 237.7 万项/小时、0.0361 TB/小时，以小文件为主。累计进度保持连续，采样期间错误计数未增加；历史错误仍有 208 条，保留在既有清单中供后续复核。

线上原始状态与节点健康证据：控制目录 `recovery/fairness-verification-20261110/`；部署记录 `recovery/scheduler-fairness-deployment.json` 已标记为 deployed_and_verified。此次验证没有重启控制器或工作进程。


## 2026-11-10：定向补拷本次运行的 Stale file handle 遗漏

复查本次运行的 80 条历史错误路径：52 个目录和 28 个普通文件当前均可读取。完整清点这些根路径，去重后共 80 个目录、193 个普通文件，文件合计 7,141,934 bytes；193 个目标文件均缺失。使用冻结复制引擎在 cpu-worker-14 串行定向补拷，193 项均 copied，执行后再次逐文件读取源/目标并比对 SHA-256，全部一致；观察端挂载的 193 个目标路径也已全部可见。主任务持续运行，16/16 节点在线，无重启。

补拷证据位于控制目录 `recovery/fairness-verification-20261110/`，包括 `retry-inventory.json`、`retry-receipts.jsonl`、`retry-summary.json`、`retry-independent-verification.json`、`retry-cross-mount-visibility.json`、`retry-resolution.json`。历史错误清单保留，不回写主控制器计数；看板的 208 条累计错误不会因这次补拷自动减少。其余历史错误与冲突仍需另外复核。

## 2026-11-10：低速补拷更早的错误文件

重新检查其余历史错误，80 个普通文件的源路径可访问、目标路径缺失，合计 26,808,132,624 bytes。独立补拷运行在 `cpu-worker-14`，使用冻结的复制与验证引擎，聚合读取、写入和校验限速为 1 MiB/s；已经存在且内容不同的目标仍记录为冲突，不覆盖。补拷按文件记录持久回执，完成的文件在进程重启后会重新核对身份并跳过；独立 supervisor 在异常退出时最多重启 10 次。主控制器、工作进程及主看板计数均不重启或回写。

补拷 manifest、独立 `status.json`、`supervisor.json`、逐文件 `receipts.jsonl` 与日志位于控制目录 `recovery/prior-error-retry-20261110/`。部署前使用其中一个文件做线上试拷及校验；主 manifest 仍包含该文件，后续由引擎校验已有内容后记为 skipped。原始 208 条历史错误继续保留，不能据此推断补拷已完成；查看独立状态和回执确认结果。另有 48 条中断扫描回执涉及 40 个任务：后续运行的目录日志中均找到重新扫描完成记录，且它们已不在当前待处理队列；子项仍以各自回执为准。检查证据位于 `recovery/fairness-verification-20261110/old-interrupted-scan-check.json`。

SwanLab 复用原实验及 120 秒批量上报，在 `retry/` 下单独显示已校验文件数、总文件数、已校验 TB、总 TB、需关注文件数和运行状态；不改写主复制曲线。上报器由 supervisor 单独重启，主迁移未重启；服务端已确认 step 5421 的补拷指标。部署和服务端回读证据位于 `recovery/prior-error-retry-20261110/swanlab-deployment.json`。

## 2026-11-10：准备失效文件句柄的有限重试

当前工作进程遇到 `ESTALE` 后会记为错误并继续，历史错误不会自动重试。恢复程序增加有限重试：只针对 `ESTALE` 重新打开路径，核对目录身份，并继续使用原有不覆盖和源文件身份校验；详细边界见[恢复说明](projset-recovery.md)。隔离测试覆盖重试成功、重试上限、其他错误、目标冲突、源变化、目录替换和不重复分发；并在 `cpu-worker-6` 的隔离目录使用本次运行冻结的引擎进行验证。

恢复启动代码已更新，更新前的代码和验证记录保留在控制目录 `recovery/estale-inline-canary-c0ab01ae81/`。现有控制器和工作进程没有重启，也没有热加载修复；只有下一次自然恢复启动新进程后，新重试逻辑才会生效。历史错误及当前进程后续出现的错误仍需单独复核，不应据此宣称已修复或完成迁移。

截至北京时间 17:11，对此前两批新增的 254 个错误根路径做只读清点和去重，涉及 459 个普通文件，共约 0.00000737 TB；再次检查时源路径均可访问、文件大小与清点时一致，目标路径均缺失。这是上述两批的已知结果，不代表全部历史异常或后续新增异常的总量。清点及复查明细分别为 `recovery/fairness-verification-20261110/new-stale-inventory-20261110.json`、`new-stale-inventory-20261110-1709.json`、`new-stale-unresolved-recheck-20261110-1710.json`；此次复查没有启动新的补拷。

17:20 进一步清点最新 12 个错误根路径，其中含 22 个目标缺失文件。合计 266 个根路径已对照共享任务日志，源路径身份均与原任务一致；其 137 个任务 ID 在当时的调度缓存中均不再待处理或租出，因此不能依靠当前主队列自动重试。三批清单去重后共 481 个文件、约 0.00000764 TB，已逐个重新核对源文件身份及目标缺失情况，生成 `recovery/fairness-verification-20261110/stale-pending-review-20261110-1720/manifest.json`；对应 `summary.json` 记录校验范围、摘要及当前处理状态。此清单与先前 80 文件补拷清单无重叠；它也不包括清点之后可能新增的错误。

17:26 开始在 `cpu-worker-7` 使用清单执行定向补拷，聚合读写与校验限速 1 MiB/s，源身份变化或目标冲突均不覆盖。481 个文件全部复制成功；随后独立读取源和已发布目标进行 SHA-256 校验，481 / 481 与逐文件回执一致。另在 `cpu-master-0` 的独立挂载抽样校验 10 / 10，266 个原异常根路径在目标端的类型均与源端一致。本次清单、进程日志、逐项回执、独立校验和关联记录位于 `recovery/stale-retry-20261110T1724/`。SwanLab 服务端 step 5600 已确认 `retry/verified_files=508`、`retry/total_files=561`；这是本次 481 个与先前 80 文件任务的合计，不代表所有历史错误均已处理。原主任务的错误计数保留历史，不会因补拷而减少。

17:44 在独立导出器接入历史错误和后续证据的关联报告：[处理汇总](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/reconciliation/REPORT.md)、[待关注清单](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/reconciliation/pending-errors.csv)。关联按任务号、路径、回执及验证记录分类；已重新扫描的目录与内容校验成功的文件分开，证据不足或冲突保留为待关注。导出器每轮处理结束后等待120秒继续更新；两次线上刷新已观察到待关注记录从 52 条变为 51 条。仅导出器经 supervisor 重启，主控制器和 SwanLab 上报器没有重启；部署证据位于 `recovery/outcomes-reconciliation-canary-e8b66eb9/verification.json`。

18:09 修正恢复启动代码中目标目录创建遇到 `ESTALE` 时的分类：有限重试耗尽后记为存储错误而非目标冲突，源目录仍继续枚举。隔离测试覆盖重试上限、真实类型冲突保留和后续源扫描；部署及节点验证记录为 `recovery/estale-inline-canary-c0ab01ae81/deployment-v3.json`、`results-v3.json`。当前运行中的工作进程尚未加载该修正，不重启主任务；历史冲突记录也不会被回写。
