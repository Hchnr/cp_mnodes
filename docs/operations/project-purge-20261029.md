# `/share/project` 清理记录（2026-10-29）

以下运行证据更新至北京时间 **17:19:40**，不是实时监控。时间来自控制文件和回执，不以准备目录名称推断启动时间；部署成功不代表全盘删除完成。

## 范围与执行方式

本次是管理员授权的 **purge（删除）**，不是之前的 copy（迁移）：清空 `/share/project` 内容，包含隐藏项、其他用户目录及 `raw`，保留挂载根；不跟随符号链接、不跨文件系统，拒绝嵌套挂载。混闪 `/share-mixed/project` 及其中控制记录不在删除范围。

助手准备代码、只读预检和私有测试；此前由管理员手动执行启动、交接命令，随后按明确要求“部署吧”“做优化吧”执行专用入口。实际删除使用 GNU `rm -rf --one-file-system`；每组由一个生产端细分目录，节点从该 controller 内容量 **4096** 的共享队列取活。后续协作组仅细分现有大树的限定范围，不做第二次全盘枚举；与原大树的遍历仍有明确重叠，已消失目录及普通错误会记录、重试。

## 时间线

- **09:53:08**：原 `prepared-v3` 启动，后续保留其仍在推进的大树任务。
- **10:42:01**：首个固定补充组启动；**12:46:48** 完成 25/25 批，零失败。
- **11:32:00**：流式 tail 组启动，12 节点×2 路；**12:47:46** 交接至每节点 4 路，补充上限 24→48。
- **14:15:26**：runtime 版本启动；**14:15:33**，worker-3 完成握手热加入，48→52 路。
- **14:20:48–14:20:53**：另外 5 节点从同队列热加入，52→72 路，无第二个生产端。
- **15:17:24**：管理员执行 40-node registry 交接；**15:17:39** 连续两次确认旧补充组已退出，原任务仍有 4 路。
- **15:17:45**：新 run `tail-boost-20261029T071739Z-569aae` 启动；**15:17:58** 状态就绪，随后核验 36 个 worker 的实际进程身份。配置为 **36×4=144 路补充＋原4路**，满槽共148路，并非40节点各4路。
- **16:15:44.689**：执行 parent-policy-v1 专用交接，仅停止上述补充组；**16:15:51.443** 连续两次确认其 controller、自有 helper/rm 全部退出。原 worker-21 此时继续运行。
- **16:15:51**：新 run `tail-boost-20261029T081551Z-204554` 准备并启动，39 个执行节点各 4 路、上限 156；**16:16:00.356** 首个成功回执，距交接 claim 15.67 秒；**16:16:29.735** 全部 39 worker 实际身份/boot 与调度状态核验通过并切换指针。
- **16:17:06.720**：同值 `limit=2` 热调的 generation/operation 回执确认，无需重启、未改变实际额度。
- **16:18:16**：原 worker-21 的最后长任务自然完成，旧原程序进入失败任务重试；**16:18:18** 因已消失父目录触发旧版 `rc=126`，原控制器自行停止。下文记录该未解决的全盘收尾问题，不能继续声称原组仍运行。

新增16个 SSH 别名仅4个具有此前未出现的 boot 身份，其余12个与旧节点共享内核；不能称为16台新增物理机器。新组 `accepted=[]` 正常：36个成员均为初始配置，accepted只列后续热加入者。

## 40 节点升级后的实测

- **15:20:06 负载快照**：36 个补充 worker 身份全部匹配，3 秒两点活跃 **142→143/144 路**。原任务由 4 路降为 2 路，剩 worker-10、21；worker-11、22 已退出并空闲，此时 38 个别名有任务，40 是已登记节点数，不是始终同时活跃数。
- **15:19:27.868–15:21:33.010 独立窗口**：125.142 秒净释放 **3.271 TB，26.14 GB/s**；五段 25 秒约 27.07、24.11、26.33、25.85、27.33 GB/s。全部采样晚于新组 readiness；窗口内待重试批次保持 1、无致命错误，队列始终积压 4,096 批。源盘当时占用约 262.76 TB，不等于精确待删清单。
- 相比此前 18 节点组 **14:22:39–14:24:44** 的 21.85 GB/s 短窗，观测值高约 19.6%；两次并非受控 A/B，文件组成、原任务并行及空间回收都会影响结果，不能将增幅全部归因扩容。
- **热点证据**：143 个在途补充 `rm` 中，138 个处理同一父目录 `eai_pwm/datasets/raw/web_video_train/Cambrian-10M/websight/images`，每批 128 个 PNG；其中 76 个处于 `rwsem_down_write_slowpath`、44 个处于 `walk_component`、18 个等待 Ustor 请求。40 个别名共 17 个 boot；同内核、同父目录争用是下一步候选优化点，不能仅据此判定后端带宽满载。

该证据提出“每 boot、每热点父目录限 1–2 路＋跨目录调度”的候选方案，而非继续直接翻倍线程。它是当时的负载快照，不代表此后一直存在同一热点。

## 本次优化：平铺文件批的父目录配额

**负载变化先于上线**：15:38:29 的只读抽样观察到 72 个补充 `rm`，其中 71 个处理 `eai_pwm/pipeline_store/materialized/koala-36m_v2` 下的不同视频子树，共 71 个不同视频 ID，均为单个名称参数；未见之前的 128 名称平铺批。部分打开的目录 FD 也确认了不同子树。单名称不等于单文件，不能据此把独立目录也当作平铺文件限流。原任务此时只剩 worker-21。

**15:54:14 再核验**：36 个补充 worker 身份均匹配；100 个在途 `rm` 全是单名称子树任务，其中 99 个 Koala、1 个 yuanhai，仍未见平铺批。队列积压 4,096，累计成功 341,076 批、待重试 3 批；原组成功 265 批、普通错误 11 次，两组均运行、无致命错误。相隔 98.18 秒两次空间采样净释放 2.577 TB，约 **26.25 GB/s**；源盘占用约 202.05 TB。它不是限流对照结果，也不是精确文件进度。

因此策略限定为：只有生产端明确标记 `flat_batch=True` 的非目录叶子批，才占用 **(boot、父目录相对路径、父目录 inode)** 配额；独立目录子树、收尾任务不占此配额，仍保留原每 worker 4 路上限。两个共享 boot 的 SSH 别名共用配额，不能各自再拿两路。生产队列仍只有一个，待派发总上限仍为 4,096，不重复枚举或另外制作文件清单。

实现位于 [内存调度器](../../project_cleanup_20261029/parent_scheduler.py)、[热调绑定](../../project_cleanup_20261029/parent_tuning.py) 和 [调度入口](../../project_cleanup_20261029/rm24.py)：

- 按父目录轮转，阻塞的平铺批不会挡住队列内其他父目录或同父目录的独立子树；若整个有界前沿都是同一热点，不能凭空发现队列之后的冷目录，也不承诺提速。
- 精确记录任务与节点租约，匹配回执（含普通 `rc=1` 错误）才释放配额；重复、未知回执停止本组，不自动重放身份未确认的在途任务。停止会唤醒被配额或队列容量阻塞的线程。
- 可选新运行需显式启用 `parent_policy`，冻结完整 40 别名的 boot 映射及两个 helper 的 SHA。初始和热加入 worker 均须在领取任务前返回匹配的 node/boot；未启用的新旧流程保持兼容。
- 新版本上线后，支持 `off/1/2` 热调；调低只约束后续领取，不取消在途 `rm`。关掉配额仍跟踪租约，再启用时不会丢失既有活跃数。
- 热调请求绑定 run、controller 的 PID/start/boot、冻结配置 SHA、generation 和 operation。每 5 秒最多读一次，领取任务不读共享控制文件；无效、缺失或过期策略保留上次有效值并告警，不停止生产。`status.json` 增加纯内存的 `parent_scheduler`、`parent_tuning` 快照。

**开发阶段决定（15:54）**：当时只完成源码与私有测试，没有改变运行状态；因热点已变为独立子树，没有自行交接。随后管理员明确要求部署，才执行下面的专用流程。不能把内存调度测试当成真实存储 A/B 收益。

## 新调度部署与上线验证

部署入口为 `upgrade40_tailboost.py prepare/apply --parent-limit 2` 的显式 40→40 模式，使用独立 `parent-deploy-current.json` 指针，不放宽旧 24→40 模式。计划冻结全部 40 节点 boot、引擎与 helper SHA、两组 controller 身份；停止前再次核验，原 run 的冻结文件、配置和 STOP 均未被交接入口修改。旧组全退出后才启动新组，启动前后再次核验源/冻结副本、boot 和实际 worker，失败不自动重跑。

部署前全量 **185 项私有测试通过**：此前 169 项，加 16 项新部署边界测试。40/40 SSH、hostname、源根 inode/挂载边界通过；旧补充组 36 个动态/初始成员均被覆盖，未知目标 `rm` 为零。原任务只占 worker-21，因此新组实际为 **39 个 worker、每个 4 路**，不是 40 台独立物理机；boot 仍为 17 组。

上线时主队列持续派发，39 个实际 worker 全部验证通过，调度器 `limit=2`、租约 `claims-releases=active_leases` 守恒。实际提交一次同值 `2` 请求，generation `1793261821789239845`、operation `ed8104130cac45349aa26f82a49ba7f7` 已由控制器确认，`error=null`；只验证热调链路，没有抬高或降低生产额度。

**测速不能据此声称提速**：上线前 16:06–16:07 净释放约 **33.21 GB/s**；上线后 16:17:37–16:18:36 的 59.26 秒净释放 **1.247 TB，21.04 GB/s**。后一个窗口内主前沿耗尽、活跃数降为 1，而且原组进入重试后自行停止，工作组成完全不同，不是受控 A/B。7 次新组采样均正常且租约守恒，`active_flat_groups=0`、`quota_waits=0`，该窗口并未触发平铺配额，不能把速度变化归因于限流。

## 当前异常与收尾边界

原 `prepared-v3` 的最后长任务成功后进入 11 批重试。worker-2 重试 `linzhe/roboclaw-vlabench/runs` 时，父路径已不存在；旧冻结版未兼容该 `FileNotFoundError`，worker 将其转为 `rc=126`，控制器在 16:18:18 自行写 STOP，随后取消另一个重试（`rc=-15`）。原组最终 268 批成功、9 批尚未完成确认；这不表示还有 9 个真实残留目录。交接没有向原组发布 STOP，但不能因此断言协作删除与这次旧版竞态的时序完全无关。

新源码及本次冻结运行已兼容父目录/执行 cwd 已消失的情况，不需要修改旧冻结代码。16:20 只读浅层检查根目录仍见 `eai_pwm`、`linzhe_backup`，不能宣告全盘为空。新补充组当前待派发为 0、仍有 1 个长任务在途，另有 1 批普通 `fts_read: No such file or directory` 错误待本组重试；它不做根目录最终收尾，`supplement_complete` 也不能等同全盘完成。

后续按 `retry_of` 合并原回执，再以父目录 inode 锚定和 `lstat` 分类：9 项中 **7 项实际已不存在**，仅 `eai_pwm/home/yuanhai`、`eai_pwm/datasets/download_jt` 两个目录条目仍存在。这里不递归、不读取内容，不代表这两个目录都有数据，也不是全盘残留清单；[分类证据](../../project_cleanup_20261029/runs/upgrade40-20261029T081456Z-1d9590/post-deploy-residual-review.json) 保留逐项结果。

本轮未删除旧 STOP、未重启旧程序，未再启动另一套根级删除。最终收尾须待现组真正结束、全 40 节点确认相关 helper/rm/已知孤儿全部退出，再核对实际残留并以新版单队列处理、最终验证挂载根为空；不能仅凭 `pending=0` 叠加第二队列。

## 大树长尾协作加速（17:06–17:16）

后续管理员明确要求优化。只读证据确认：主组整棵 `yuanhai/ego_r2_runs/20261024-ego-r1-1024-pipeline-v3` 被默认深度 2 的规划器作为一个任务领取，队列已空，CPU/内存未形成当前资源瓶颈，平铺额度等待为零。源码没有在途任务热拆接口；因此保留原 `rm`，另起**仅限该树子目录**的协作生产端，不改原租约、不发旧 STOP、不修改主指针。协作避让仅是 FD 快照，原进程未来仍可能走到相同目录，不能保证零重复遍历或原进程生命周期不受竞态影响。

新增 [受控协作入口](../../project_cleanup_20261029/cooperative_tail.py) 和私有测试，最终全量 **213 项测试通过**。准备/启动前验证全部 40 别名、精确 helper/rm PID/start/boot/argv、真实 cwd inode、至少一个未删除的后代 FD 经 `Root.parent` 贯通当前树，并前后核对 pipeline inode；同名重建、未知进程、源码/冻结 helper 变动均拒绝。可选范围只允许现存的 `tasks`、`label_work`、`mcap_cache` 直接目录；实际启动时 `mcap_cache` 已消失，仅取剩余两个 scope，以默认两层展开派到 batch 下的 job 等细粒度任务。

- **17:06:42**：`cooperative-run-20261029T090642Z-0ac071` 启动，extra-worker-2、12、13、14 共 4 个独立 boot、每 worker 4 路。原主 `rm` 继续运行；共享队列很快积压 4,096，16 个租约持续活跃。
- **17:13:03–17:13:08**：经试点观察后，从同一队列在线接入 master-0、worker-0、2、6、8、10、12、14、16、18、20、22，12 个请求全部接受，无生产端/worker 重启。共 **16 个独立 boot、64 路协作**；主组另有 1 个在途大树任务。16/16 worker 的实际 `/proc` 身份、命令行和 boot 再次独立验证通过。
- 当前使用全部 17 个 boot 域（协作 16＋主组 1），不是把 40 个别名当作 40 台独立机器；暂不继续叠加同内核别名。主 tail 指针与旧原程序的 STOP 均保持原样。

实测窗口均为全源**净空间释放**，不是实际物理 I/O 带宽，也不是严格 A/B：

- 紧邻试点的 **17:01:34–17:02:04**：4.37 MB/s；此前 14.12 GB/s 属于不同阶段，不能拿来判断此轮优化退步与否。
- 4 节点 **17:10:02–17:11:02**：8.37 MB/s，106 批/分钟，零新增普通失败。
- 16 节点 **17:14:59–17:15:59**：15.17 MB/s，491 批/分钟（8.18 批/s）；比试点完成批次速率约 4.63 倍。64 租约持续满载、待派 4,095–4,096，租约守恒、无致命错误、无平铺配额等待。净释放分别约为试点的 1.81 倍、紧邻单任务基线的 3.47 倍，不能将增幅全部归因并发，也不能据此保证吞吐已最大化。

出现 1 次非致命 `rc=1`：extra-worker-14 删除 `label_work/egolive_watch_b000485.published/episodes` 时返回 `Directory not empty`，已保留回执等待有界重试；未证明具体是并发遍历、缓存还是新写入造成。不能写成“完全无错误”。

**17:19:40 最新核验**：协作组已成功 6,330 批、累计 7 批普通失败待重试，64 个租约活跃、待派 4,096；主组仍有 1 个长租约。两组均为新鲜 `rm_running`、无 STOP、无致命错误、租约守恒。上述“1 次失败”是测速窗口当时的快照，不是之后始终只有一次。

17:06 的有界浅层核对发现源根只剩 `eai_pwm`，其下 `.partial`、`.secrets` 为空，另外仅 `home/yuanhai/ego_r2_runs`；`linzhe_backup`、`datasets`、`prepared_datasets` 均已不存在。这不构成剩余树的大小清单，尤其不能把整卷约 133.9 TB 占用当作待删分母。最终根级收尾仍未执行，不能将协作组完成当成全盘完成。

本轮 [协作计划指针](../../project_cleanup_20261029/runs/cooperative-tail-current.json)、[计划/启动/扩容证据](../../project_cleanup_20261029/runs/cooperative-tail-20261029T090603Z-ee157d/)、[实际运行目录](../../project_cleanup_20261029/runs/cooperative-run-20261029T090642Z-0ac071/)、[16 worker 实证](../../project_cleanup_20261029/runs/cooperative-tail-20261029T090603Z-ee157d/expansion-worker-readiness.json)、[试点测速](../../project_cleanup_20261029/runs/cooperative-tail-20261029T090603Z-ee157d/post-pilot-throughput.json)、[扩容后测速](../../project_cleanup_20261029/runs/cooperative-tail-20261029T090603Z-ee157d/post-expansion-throughput.json) 均在混闪保存。协作指针独立，默认主 tail 的 status/stop 不代表这组。

查看本组实时状态（从仓库根目录）：

```sh
python3 -S -B project_cleanup_20261029/rm24.py status --run project_cleanup_20261029/runs/cooperative-run-20261029T090642Z-0ac071
```

`execution_nodes` 冻结初始 4 个节点，后加 12 个节点记录于 `status.json/runtime_join.accepted`；不要只看初始名单以为仅 4 节点。不要重复 prepare/apply 或删除 `cooperative-claim.json`；后续加入必须显式向此协作 run 提交，默认 `join_tailboost.py` 指向主组。

## 修复与待验证项

升级前 **126 项私有测试通过**。已修复旧24节点/旧集群身份与线程池限制；交接枚举完整旧 registry，覆盖动态成员；按 PID/start/boot 正向追踪自有进程与孤儿，保护原任务；旧 run 独占 claim 防重复交接；不再把 controller 启动等同全部 worker 可用。旧组无排空接口，交接会取消其在途命令，再枚举剩余子树，并非零空窗或零重扫。

新调度新增纯内存配额、公平性、容量背压、停止唤醒、降低/关闭额度及重新启用、回执守恒测试；私有控制器/真实 fixture worker 验证目录不误限、`rc=1` 重试、错误 boot、未知/重复回执、冻结 SHA 和热调跨阶段继承。热调另覆盖共享盘错误与坏请求的恢复；不操作真实目标、不启动远端任务。最终全量回归 **169 项全部通过**（原 126＋调度 16＋热调 17＋整合 10）。

当前父目录调度**已部署**且热调回执已验证，但存储端收益尚未得到受控验证；不保证限制并发或继续增加并发必然提速。当前全盘收尾仍受上述旧原程序异常影响。

## 查看状态与证据

从仓库根目录只读查看：

```bash
python3 -S -B project_cleanup_20261029/tailboost.py status
df -h /share/project
```

批次数不等于文件数；空间净释放不是物理搬运带宽，也不等于精确待删清单。短窗口会受空间回收延迟影响，不能据此保证完成时间。

入口为 [当前 run 指针](../../project_cleanup_20261029/runs/tail-boost-current.json) 和首次 [升级计划指针](../../project_cleanup_20261029/runs/upgrade40-current.json)。首次 [交接证据](../../project_cleanup_20261029/runs/upgrade40-20261029T071100Z-81dd16/) 含计划、连续退出观察、`worker-readiness.json` 与 `ready.json`；[历史运行目录](../../project_cleanup_20261029/runs/tail-boost-20261029T071739Z-569aae/) 含 `status.json`、`jobs.jsonl`、失败回执及 `logs/`；[新增节点预检](../../project_cleanup_20261029/runs/extra16-readonly-preflight-20261029T064657Z.json) 保存 hostname、挂载与 boot 证据。

上段是首次 registry 升级的历史目录。本轮 [parent 部署计划指针](../../project_cleanup_20261029/runs/parent-deploy-current.json)、[完整部署证据](../../project_cleanup_20261029/runs/upgrade40-20261029T081456Z-1d9590/)、[现运行目录](../../project_cleanup_20261029/runs/tail-boost-20261029T081551Z-204554/)、[热调确认](../../project_cleanup_20261029/runs/upgrade40-20261029T081456Z-1d9590/hot-policy-confirmation.json) 及 [上线后空间采样](../../project_cleanup_20261029/runs/upgrade40-20261029T081456Z-1d9590/post-deploy-throughput.json) 保存实际上线信息。旧原程序 [首错](../../project_cleanup_20261029/runs/prepared-v3/first-error.json) 和 [最终状态](../../project_cleanup_20261029/runs/prepared-v3/status.json) 是收尾异常的依据。

本次 [40 节点负载快照](../../project_cleanup_20261029/runs/upgrade40-readonly-review-20261029T072006Z.json) 与 [升级后独立测速](../../project_cleanup_20261029/runs/post-upgrade40-throughput-20261029T071927Z.json) 保留原始样本；[旧 18 节点组测速](../../project_cleanup_20261029/runs/post-bulk-throughput-20261029T062239Z.json) 是比较基线，不是受控试验。

[共享内核/源设备映射](../../project_cleanup_20261029/runs/parent-lockdomain-review-20261029T073543Z.json) 与 [Koala 子树形态抽样](../../project_cleanup_20261029/runs/koala-task-shape-20261029T073829Z.json) 记录了为何不能直接沿用早先平铺热点结论。

[15:54 的任务形态与空间样本](../../project_cleanup_20261029/runs/flat-shape-current-20261029T075414Z.json) 是本次决定不打断现有运行的最新依据。

`runs/` 被 gitignore 排除；链接指向混闪上的持久本地证据，GitHub 不一定可访问。不要重复执行 apply/start，不要删除 `start-claim.json`、`apply-claim.json`、`upgrade40-claim.json` 或 STOP 来绕过保护；未知进程或 SSH 验证失败时，应查记录，不盲目重启。
