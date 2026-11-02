# GNU rm 集群删除：24 → 40 节点

[执行记录与简要说明](../docs/operations/project-purge-20261029.md) 保存操作时间线、已部署版本、测速证据及注意事项；它是历史快照，不替代实时状态。共享盘 `runs/` 保留原始证据，但不纳入 Git。

本目录绑定历史路径、根 inode、集群与部分进程身份，不是通用即用配置。执行真实删除入口前必须核对当前环境，不要盲目重跑历史启动或交接命令。

私有回归测试（从仓库根目录运行，不连接生产节点）：

```sh
python3 -S -B -m unittest discover -s project_cleanup_20261029 -p 'test_*.py'
```

只使用 GNU `rm -rf` 执行实际删除，取消逐文件计数、容量盘点、文件身份清单、HTTP broker 和 SQLite。准备过程不删除 `/share/project` 中的数据。

在本仓库根目录执行这一条命令，启动真实删除：

```sh
python3 project_cleanup_20261029/rm24.py start
```

目标是清空 `/share/project` 的全部内容，含隐藏项和 raw，保留挂载根目录。初始修复版的状态和少量任务日志保存在本目录 `runs/prepared-v3/`，位于混闪，不受清理影响；旧 `runs/prepared/`、`runs/prepared-v2/` 保留原样作为故障证据。初始冻结组为 24 节点、每节点 8 个 rm，最多 192 个同时运行；每节点仅一条长期 SSH 连接。启动入口返回后，调度进程独立运行。后续源码已支持两个集群共 40 个别名，各在运行组的实际节点与并发以自己的冻结配置为准；不要重复启动初始组。

启动前重新核对全部节点的集群、GNU rm、源挂载 inode、只读状态及已有 rm。实际执行的形式为 `rm -rf --one-file-system -- <子项名称...>`；不带 verbose，不统计每个文件的大小，也不读取文件内容。子项使用 argv 传递，不经过 shell 拼接，挂载根目录本身不会成为 rm 参数。

调度器边枚举浅层目录边派发，通常拆到第 3 层，最多展开 1,024 个目录；同一目录直接包含的普通文件/链接按最多 128 个参数一批。队列最多保留 4,096 批，哪个节点完成就继续领下一批，不固定分配顶层子树。已交给 rm 的整棵子树不会中途拆开，因此仍可能有少数巨大子树拖尾；此方案不承诺严格负载均匀或 24 倍加速。

GNU rm 的普通删除失败（退出码 1，包含非空目录、权限等错误）记录到 `failed-jobs.jsonl`，其他节点和后续任务继续运行，不触发全局停止。正常任务完成后，每个失败批次最多额外重试两次。SSH/控制协议中断、执行异常、根目录或父目录身份不符等问题仍会停止本次运行。

分片结束后按深度清理分区父目录，再检查根目录为空，才标记 `complete_root_retained`。有残留则标记 `finished_with_remaining_entries`，保留 `remaining-jobs.json`，不会伪报完成，也不会无限重试。使用前应停止源端写入、移动和挂载变更。预检发现任何子挂载会拒绝启动；不跟随符号链接。

查看简要状态和调度日志：

```sh
python3 project_cleanup_20261029/rm24.py status
tail -f project_cleanup_20261029/runs/prepared-v3/logs/controller.log
```

停止：

```sh
python3 project_cleanup_20261029/rm24.py stop
```

保活通过现有 SSH 输入流每 5 秒传递，工作进程按本机单调时钟计时；不再读取 NFS 上反复替换的心跳文件。显式取消、SSH 输入意外关闭或超过 90 秒收不到控制消息时，节点终止自己启动的 rm。已阻塞在存储系统调用中的进程不能保证立即退出。SSH 中断会使本次运行报错，不会自动重新派发可能仍在执行的子树。

`status.json` 和 `logs/controller.log` 每 5 秒更新简要阶段及已完成 rm 批次数；批次不是文件数，也不等于容量进度。控制器保留 `first-error.json`，节点保留 `workers/<node>-first-stop.json`，避免其他节点随后的 SIGTERM 错误掩盖最初原因。

`start` 每个运行目录只能执行一次，重复启动会拒绝。需要重试时先确认旧进程退出，再准备独立的新运行目录；不用恢复任何文件级计数。不要通过删除 `start-claim.json` 强行重跑。

```sh
python3 project_cleanup_20261029/rm24.py prepare --run project_cleanup_20261029/runs/retry-01
python3 project_cleanup_20261029/rm24.py start --run project_cleanup_20261029/runs/retry-01
```

准备命令仅做只读 SSH/挂载检查并生成配置；真正删除只发生在手动 `start` 之后。初始并发可以通过 prepare 的 `--per-node` 调整，范围 1–32；增大并发不保证更快。

测试只操作私有临时目录：

```sh
python3 -S -B -m unittest discover -s project_cleanup_20261029 -p 'test_*.py' -v
```

## 原任务运行时补充空闲节点

`boost.py prepare` 只读观察全部节点，选最多 8 个当前没有 rm 的节点，有限枚举在途大子树中的子目录，跳过已观察到打开的分支，生成独立运行目录。它不会修改原运行的冻结代码、配置或进程。原 rm 仍会遍历父树，所以属于协作删除，存在范围重叠；不能保证共享存储竞争下原进程速度绝对不变。

已准备好后，由管理员手动启动补充任务：

```sh
python3 project_cleanup_20261029/boost.py start
```

查看主任务及补充任务、或只停止补充任务：

```sh
python3 project_cleanup_20261029/boost.py status
python3 project_cleanup_20261029/boost.py stop
```

补充任务只处理准备时选出的子目录，不执行全局枚举、分区父目录清理或全盘完成判定；已经被原 rm 删除的父目录视为已不存在。完成只标记 `supplement_complete`，不能当作整个任务完成。

另行准备时可指定每个空闲节点的并发，例如 `boost.py prepare --per-node 2`。已开始的补充任务不会被自动替换；需先检查并停止，避免叠加重复进程。

## 大目录长尾：跨全部在途目录持续补充分派

`tailboost.py` 解决第一版补充器的两个覆盖限制：全局 64 个目录的扫描预算容易被前几个范围耗尽；固定种子列表耗尽后不会再取新任务。新补充器从当前原任务仍在删除的全部目录轮询生成细分任务，平铺文件每批最多 128 个参数，子目录通常拆两层；遇到正在打开的分支继续往下寻找其他分支，最多拆 8 层。任务按需生成、共用有界队列，完成得快的节点继续领取，不制作逐文件清单、不统计大小。

准备只进行只读观察、最多 96 个任务的旁路预览，以及空闲节点的 SSH/挂载检查；不会启动实际删除。默认最多选择 12 个**当前无 rm** 的节点，每节点 2 个 rm：

```sh
python3 project_cleanup_20261029/tailboost.py prepare --per-node 2 --max-nodes 12
```

管理员确认后手动启动真实删除：

```sh
python3 project_cleanup_20261029/tailboost.py start
```

查看状态、或只停止这组新的补充任务：

```sh
python3 project_cleanup_20261029/tailboost.py status
python3 project_cleanup_20261029/tailboost.py stop
```

控制目录为独立的 `runs/tail-boost-*`，不改动两个现有运行的冻结代码、配置、指针或进程；第一组补充器声明过的子树也排除。它仍与原 rm 的父树遍历存在协作重叠，打开目录快照不是独占锁，无法保证原任务吞吐不受共享存储竞争影响。启动后必须用整体空间释放速度判断收益，不能将批次数当作容量百分比。新补充任务不清理全局分区父目录，也不声称全盘删除完成；最终收尾仍由原任务负责。

## 新补充组并发翻倍：2 → 4 路/节点

现有运行不支持热改并发，也没有真正的排空接口。扩容需要只停止这一组、确认进程退出，再重新扫描尚存目录接续；原任务和第一组补充任务不停止。`rm` 的停止不是复制文件的断点，已经 unlink 的条目保持删除，尚存条目重新派发。交接存在供给空窗，不保证零吞吐影响。

只读验证和准备计划（不会停止/启动删除）：

```sh
python3 project_cleanup_20261029/scale_tailboost.py prepare --per-node 4
```

管理员执行一条交接命令：

```sh
python3 project_cleanup_20261029/scale_tailboost.py apply
```

交接严格绑定旧 tail 的目录、配置/脚本哈希、controller 身份及选中节点，只给这一组写 STOP；不使用 `pkill` 或广泛信号。必须确认旧 controller、全部旧 helper 和目标内 rm 消失，SSH 不可用或未知存活进程均阻止启动；最多等待 150 秒。随后重新执行严格节点预检并启动独立的 4 路/节点新组，后台 controller 达到新鲜的运行状态后才报告上线成功。原来的范围和仍存在的失败目录都会保留，其他组正在打开的目录快照会刷新；指针在新启动后切换，诊断记录保存在独立 `runs/tail-scale-*`。

翻倍的是新补充组（12 × 2 → 12 × 4，即 24 → 48 路），不是整个集群的实际 rm 总数。是否提速必须比较扩容前后数分钟净释放速度，不能仅根据进程数或 CPU 空闲判断。

## 同队列在线加节点：保持 4 路/节点

当前冻结运行没有在线加入接口。下面的只读准备生成升级计划，保持现有 12 节点各 4 路，并先验证空闲的 worker-3：

```sh
python3 project_cleanup_20261029/scale_tailboost.py prepare --runtime-join --per-node 4 --trial-node worker-3
```

管理员执行一次交接及单节点试点：

```sh
python3 project_cleanup_20261029/scale_tailboost.py apply
```

第一次仍需停止并替换当前补充组、重新枚举其剩余范围，存在供给空窗；不停止原任务仍在运行的大目录 rm。新组确认运行后才请求 worker-3 加入，将补充组上限从 48 提至 52 路。应观察至少 5–10 分钟的整体净释放速度和错误率，再决定是否加入其他空闲节点；容量释放有波动，不能把批次数、进程数或短窗口变化当作确定收益。

升级后，管理员可逐个加入其他空闲节点，例如：

```sh
python3 project_cleanup_20261029/join_tailboost.py worker-6
```

后续加入共用同一生产端和有界任务队列，不重新扫描、不改冻结配置、不停止现有在途任务。新节点先核对集群、挂载、GNU rm、已有删除进程及工作进程首个心跳，验证成功才领任务。加入前失败只拒绝这个请求；领到真实任务后发生控制连接错误，仍沿用本运行原有的安全停止策略。

在线加入启用后，不再支持旧的 `scale_tailboost.py` 停组翻倍流程；冻结配置不包含后续加入的成员，不能用静态成员表判断全部进程已退出。该入口会直接拒绝，不会写 STOP。每节点并发保持原值，需要更多执行端时使用 `join_tailboost.py`。

`status.json` 的 `runtime_join` 显示已接受节点、正在验证的节点、待领任务数及拒绝数；请求和结果分别保存在 `runtime-requests/`、`runtime-acks/`。仅主分派阶段接受加入，STOP、主队列耗尽或进入重试后拒绝。加入请求超时不等于失败，先检查原请求的 ACK，勿反复提交。若交接已成功但试点加入未确认，不能重跑 `apply`；先检查新运行状态和原请求的 ACK。单纯的加入前验证失败不会停止基础组，其他运行故障仍以 `status.json` 为准。

### 批量热扩容

2026-10-29 14:16（北京时间）已确认升级成功、worker-3 已接入、公共队列积压 4,096 批；worker-6、13、14、15、16 无删除进程且资源压力低。无需再写扩容框架或交接控制器，管理员可并行调用已有加入入口：

```sh
printf '%s\n' worker-6 worker-13 worker-14 worker-15 worker-16 |
  xargs -P 5 -n 1 python3 -S -B project_cleanup_20261029/join_tailboost.py
```

这是针对上述一次只读核验的节点集合；以后使用仍会由加入服务重新严格预检。五个节点各保持 4 路，全部接受后补充组上限为 18×4＝72 路，加上当时原任务 6 路，合计上限 78 路、24 个节点参与。不会重新扫描、重启已有进程或改变其线程数，但仍会增加共享存储竞争，不能保证吞吐按并发线性增长。不要重复执行此前的 `scale_tailboost.py apply`。

每个请求各有唯一 nonce、预检和 ACK；拒绝或超时逐节点显示，`xargs` 在子进程失败时返回非零，不会回滚已接受的节点。仅 `accepted`/`already_active` 表示加入确认，超时先查原 ACK，勿盲目重投。扩容后比较至少 5–10 分钟的整体净空间释放速度和错误率；批次数不是容量吞吐。

## 新集群接入：升级到 40 节点名单

源码明确绑定两个集群的真实 hostname：旧 `2dadf47a…` 的 `master-0`、`worker-0` 至 `worker-22`，以及新 `1f50be08…` 的 `extra-master-0`、`extra-worker-0` 至 `extra-worker-14`。不修改系统 hostname、不伪装旧节点身份。新配置冻结完整 `node_hosts`，预检和派发池支持 40 个节点。

旧运行的节点白名单、单集群身份校验和线程池已经冻结，不能直接热加入第二个集群。先只读准备专用升级计划：

```sh
python3 -S -B project_cleanup_20261029/upgrade40_tailboost.py prepare
```

管理员执行一条升级命令：

```sh
python3 -S -B project_cleanup_20261029/upgrade40_tailboost.py apply
```

这次**仅取消并替换旧补充组**，原任务及其在途 `rm` 不停止。交接检查整个旧 24 节点名单，覆盖初始成员、动态加入和正在接入的节点；依据 helper/子 `rm` 的 PID、启动时间、boot 身份识别归属并跟踪已知孤儿。旧 controller、全部旧组 helper 和自有 `rm` 连续两次确认退出后，才允许新组启动。无法归属的删除进程、SSH 故障或超时均阻止继续，不做广泛 kill。

旧组没有导出内存队列或自然排空接口，因此不能承诺原样接续 4,096 条待派发任务，也不是零中断热更新。旧组退出后，仅从继承且 inode 一致的尚存范围重建前沿，不做全盘扫描、不同时运行两个补充分派器；已删除的条目保持删除。首次跨集群升级存在补充组供给空窗。

新组使用同一个生产端、同一个有界队列，所有新 16 节点与当前不被原任务占用的旧节点均参与，每节点保持 4 路。例如原任务仍占 5 个节点时，新组为 35×4＝140 路；原 5 路另计，原节点结束后还可以在线加入。新组不仅须达到新鲜 controller 状态，还须逐节点确认 worker 的真实 hostname、PID/启动时间/boot 和准确命令行，并再次检查运行状态，才切换当前指针；不能只因 `rm_running` 出现就宣称全员上线。完整计划、进程检查、worker 验证、替代 run 和失败证据保存在独立 `runs/upgrade40-*`。

执行前在被替换的旧 run 中独占创建持久 `upgrade40-claim.json`，而不只锁定某份计划；两份计划不能同时替换同一个运行。执行失败后先查这些记录，不删除 run claim 或 plan claim 强行重跑，也不要再使用旧的 `scale_tailboost.py apply`。检测未知 GNU rm 时还检查源外 cwd 下的绝对/相对源操作数和包含源的父路径，不只查看进程 cwd；该检查为词法匹配，不沿任意外部符号链接探测。

只读预检发现新增 16 个别名中有 12 个与旧节点共享 boot 内核，仅 4 个内核此前未出现；别名数量不等于独立物理机数量。新容器配额为 20 核、约 161 GB 内存。部署后按净空间释放速度验证实际收益，不假定吞吐随 40 个别名线性增长。

## 可选的平铺父目录调度（已部署，支持热调）

`parent_scheduler.py` 按父目录公平分派，只有明确标记的平铺文件/链接批占用每 boot、每父目录配额；独立目录子树不受此配额限制。共享内核的多个别名共用额度。调低配额不会取消在途任务，任务领取不增加共享盘控制文件读取。

该功能须由准备器显式向 `rm24.prepare(..., runtime_join=True, parent_policy=...)` 传入完整 40 别名的实测 boot 映射及初始 `limit`（`None`、`1` 或 `2`）；准备器冻结两个 helper 的 SHA。默认 prepare 不启用，不会因更新仓库源码自动生效；不要手改正在运行的配置。

已有 40 节点补充组可通过专用模式交接（下面是执行流程说明，不是要求重复执行已经完成的部署）：

```sh
python3 -S -B project_cleanup_20261029/upgrade40_tailboost.py prepare --parent-limit 2
python3 -S -B project_cleanup_20261029/upgrade40_tailboost.py apply --parent-limit 2
```

该模式使用独立的 `runs/parent-deploy-current.json` 计划指针。准备阶段只读核对全部 40 别名及 boot；apply 再验证配置/代码 SHA 和实际进程归属，独占旧 run 的交接锁，仅停止旧补充组。全部旧 worker、自有 rm 和 controller 连续两次确认退出后，才从剩余范围启动新组；原任务不停止。不存在零空窗保证。新组须通过完整 worker 身份/boot、调度状态和配额核验才切换当前 run 指针；失败保留记录，不自动重跑或删除 claim。不要省略 apply 的 `--parent-limit`，无该参数仍是旧 24→40 registry 升级入口。

只有完成验证并部署这种新运行后，才可提交下面的非破坏性热调请求（`RUN` 必须换成那个已启用新运行的绝对路径）：

```sh
python3 -S -B project_cleanup_20261029/rm24.py parent-limit --run RUN --limit 2
# --limit 1 降为一路；--limit off 关闭配额，不停止在途 rm
```

返回 `submitted: true, applied: false` 仅表示请求已写入；须在 `RUN/status.json` 的 `parent_tuning` 中确认相同 `generation`、`operation` 和 `limit`，且 `error` 为空，才算已应用。控制器最多约 5 秒读取一次；无效/过期请求保留上次有效配置。未启用、配置/helper 哈希变化、controller 身份不符、STOP 或状态过期时拒绝提交。

最初 15:38 的实际负载已从同父目录 128 文件批转为不同 Koala 视频子树，因此开发阶段没有交接或限流现有删除任务；后续是否实际部署及其验证记录，以上方执行记录为准。配额可能降低有效并行，必须基于适用负载和整体净空间释放速度验证，不以活跃进程数量判断成功。

2026-10-29 16:16（北京时间）已按明确授权部署上述模式，39 个实际 worker 验证通过，`limit=2` 同值热调已确认。随后旧原程序在失败任务重试中因父目录已消失而自行停止；当前新组只剩长任务及自身重试，**不等于全盘清理完成**。原 STOP 和冻结文件保留，未启动额外根级队列；详情与当前证据见执行记录，不要重复 apply 或重启旧原程序。

## 单棵大树的受控协作加速

2026-10-29 17:06 后，按授权保留主组长任务，同时由 `cooperative_tail.py` 将该 pipeline 的现存 `tasks`、`label_work` 子树细分至 job 等粒度。不是在途租约热拆，也不是排他交接；原进程未来可能遇到已删路径。仅使用支持 `already_gone`、普通 `rc=1` 有界重试的新版引擎。

该入口限定固定的 pipeline、真实进程及目录 inode，prepare 只读，apply 才启动；核对 40 别名及实际 cwd/后代 FD，使用独立计划指针和每 anchor 独占 claim，不修改主 tail 指针或旧 STOP。已执行的试点和扩容不要重复运行。当前协作组初始 4 节点＋在线加入 12 节点，16 个独立 boot、每节点 4 路，共 64 路；主组另有 1 个任务。40 个别名不能当作独立硬件数量。

查看协作组（默认主 tail 的 status 不包含它）：

```sh
python3 -S -B project_cleanup_20261029/rm24.py status --run project_cleanup_20261029/runs/cooperative-run-20261029T090642Z-0ac071
```

冻结配置只列初始 4 节点，另外 12 个见 `status.json/runtime_join.accepted`；该组仅辅助限定子树，完成不等于全盘清理完成。[执行记录](../docs/operations/project-purge-20261029.md) 包含范围、时间线、实测速率、错误及持久证据链接。全部控制记录继续存放混闪的 `runs/`，不使用 `/tmp`。
