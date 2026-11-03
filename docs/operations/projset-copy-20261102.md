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
