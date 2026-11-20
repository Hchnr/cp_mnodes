# projset 迁移后可优先删除的目录

> 历史快照：此文记录截至 2026-11-16 的复查结果，部分路径之后已删除。请以[后续候选清单](projset-verified-deletion-candidates-20261117.md)和执行时的源、目标状态为准，勿将本文直接作为当前删除清单。

更新时间：2026-11-16 15:23（北京时间）。15:19 从管理环境重新检查：文中八个已复查目录的源、目标根目录均存在，根目录身份与 10:39 的记录一致；四个历史目录的源路径仍不存在。[最新检查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/candidate-rootcheck-20261116T1519BJT.json)仅核查根目录，不是重新递归复查，也不能证明目录内部未变化。10:39 的[此前检查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/candidate-rootcheck-20261116T1040BJT.json)来自 16 节点；EgoLive 整目录另于 10:34 由 16 节点检查根目录。

[大目录复查自动进展](/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/docs/operations/projset-large-candidates-live.md)每五分钟更新；尚未完成的候选不会自动计入可删除容量。

[Egocentric 已复核子目录自动清单](/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/docs/operations/projset-egocentric-verified-branches.md)在独立对账及 16 节点根目录检查完成后更新；等待期间不计入可清理容量。

## 当前可优先清理

### OmniVitac 提取目录：99.537043 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021`
- 共 151,516 个文件、20,156 个目录。全目录复核和 16 节点源、目标逐项复查完成；目录成员、文件身份和访问属性未发现差异，历史冲突和未解决扫描错误为零。
- 首轮复制成功回执覆盖全部文件，记录源文件 SHA-256 并读取目标副本校验；本次将源、目标文件身份与回执逐项比对，未重新读取全部文件内容计算哈希。
- 证据：[OmniVitac 提取目录复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/screen-more-20261115T1410Z/OMNIVITAC-VERIFIED.md)。此父目录包含此前列出的 `Adjustment` 和 `Grasping`，不应重复计算。

### Nymeria：54.099820 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/Nymeria`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/Nymeria`
- 共 58,586 个文件、25,056 个目录。已完成一轮全目录复核及源、目标逐项复查，目录成员和文件身份与成功回执一致；未发现差异、历史冲突、缺失补传项或访问属性差异。
- 首轮复制或恢复阶段已记录每个文件的源端 SHA-256 并读取目标副本校验；本次没有重新读取全部文件内容计算哈希。为缩短慢节点的尾部等待，已保留完整目录的复查结果，剩余目录交给其他节点并行核对，汇总时核对目录数、文件数和字节数。
- 证据：[Nymeria 逐项复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/merged/tail/NYMERIA.md)。

### EgoPro：12.291040 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/EgoPro`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/EgoPro`
- 共 31,470 个文件、2,235 个目录。16 个节点完成源端清点及源、目标逐项复查，目录成员和文件身份与成功回执一致；未发现差异、历史冲突、缺失补传项或访问属性差异。
- 复制或恢复阶段已记录每个文件的源端 SHA-256 并读取目标副本校验；本次没有重新读取全部文件内容计算哈希。
- 证据：[本次逐项复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/CANDIDATES.md)。

### Paxini-Omnisharing：9.708534 TB

- 源路径：`/share-mixed/projset/eai_pwm/paxini_cleaning/source/Paxini-Omnisharing`
- 目标路径：`/share-mixed/project/eai_pwm/paxini_cleaning/source/Paxini-Omnisharing`
- 共 45,119 个文件、33 个目录。16 个节点完成源、目标逐项复查，目录成员和文件身份与成功回执一致；未发现差异、历史冲突、缺失补传项或访问属性差异。
- 复制或恢复阶段已记录每个文件的源端 SHA-256 并读取目标副本校验；本次没有重新读取全部文件内容计算哈希。
- 证据：[本次逐项复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/CANDIDATES.md)。

当前合计 **175.636437 TB、286,691 个文件**，按 1 TB = 10¹² 字节计算。这是文件逻辑容量，实际释放的存储空间以存储系统统计为准。

**执行删除前**：确认相关业务读取已切换到对应的 `/share-mixed/project` 路径，且源目录已停止写入，并在执行时再次确认源、目标未发生变化。源文件系统在线，已有核对不是原子快照。

**检查旧路径链接和其他依赖**：复核中发现部分符号链接在源、目标两端均保留 `/share/mixed/projset/...` 绝对路径；该路径在当前开发容器及抽查节点不存在，其他计算环境的挂载可能不同。[本次三个链接的只读抽查](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/symlink-path-audit-20261112T061905Z-6cb1daa9/SAMPLE-20261116T0831BJT.md)不表明本文每个候选目录都被这些链接引用。清理前需确认相关业务挂载、其他目录中指向候选源路径的链接及其他读路径依赖均已处理。历史一个恢复代次曾记录 [835,224 个保留旧前缀的不同链接路径](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/symlink-path-audit-20261112T061905Z-6cb1daa9/current-attempt-snapshot-20261112T062437Z-48a10ed2/README.md)，并非全迁移总数。

该历史快照已针对本文八个完整候选目录（包括 EgoLive 整目录）筛查：**未发现链接直接指向这些候选或其子路径，也未发现链接指向其祖先路径**。[筛查方法和结果](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/symlink-path-audit-20261112T061905Z-6cb1daa9/candidate-references-20261116T0234Z/RESULT.md)。快照不包括其他代次、后续新增链接、其他挂载别名或应用配置，因此不能据此排除全部读路径依赖。

## 已完成源文件复查，目标端有额外临时文件

### OpenLoong-Community_visuo-tactile：17.249460 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/tactile/OpenLoong-Community_visuo-tactile`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/tactile/OpenLoong-Community_visuo-tactile`
- 共 **9,250 个文件、118 个目录**。11 月 16 日 00:24 完成 16 节点当前逐项复查：源文件和目录均有对应目标，源、目标文件身份与成功回执一致，未发现非预期差异或访问属性差异。此前全目录复核中的扫描错误已结清。
- 目标端另保留 **46 个 `.migration-*.tmp` 文件，合计约 0.066855 TB**；它们是目标端额外路径，不作为上述源文件的对应副本，本次没有删除。逐项见 [目标端临时文件清单](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/openloong-fresh-20261115T1618Z/TARGET-TEMPORARIES.csv)。
- 首轮或恢复阶段的 9,250 个文件均有源 SHA-256 和目标读取校验记录；本次未重新读取全部内容计算哈希。从源文件覆盖角度，业务确认目标端额外文件可接受、读取已切换、源端停写并在执行前再次确认源和目标未变化后，可考虑清理源路径。[本次复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/openloong-fresh-20261115T1618Z/RECHECK.md)。

若上述目标端额外临时文件可接受，将此目录与前面四个目录合计为 **192.885897 TB、295,941 个文件**；该数为逻辑容量，实际释放量以存储系统统计为准。

## 已完成文件复查，需确认访问权限

### pipeline_store/sources/egostandard：22.719924 TB

- 源路径：`/share-mixed/projset/eai_pwm/pipeline_store/sources/egostandard`
- 目标路径：`/share-mixed/project/eai_pwm/pipeline_store/sources/egostandard`
- 共 327,494 个文件、6,090 个目录。已完成全目录复核和当前逐项复查，文件及目录成员与成功回执一致，未发现文件差异、历史冲突或缺失补传项。
- 其中 24 个目录源端权限为 `755`、目标端为 `700`；其余目录访问属性一致。目标端相应目录的访问权限需由业务确认后再处理源目录。本次没有重新读取全部文件内容计算哈希。
- 证据：[逐项复查记录及权限差异明细](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/pipeline-tail/PIPELINE.md)。该目录未计入上面的无异常清理容量。

### RoboMIND2.0-Ark：10.446248 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/RoboMIND2.0-Ark`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/RoboMIND2.0-Ark`
- 共 **14,913 个文件、29,825 个目录**。07:27 完成 16 节点当前逐项复查及最终汇总：目录成员、文件身份与已校验回执逐路径一致；文件及路径差异、历史问题和未解决扫描错误均为零。
- 全部 **29,825 个目录**的源端权限为 `555`、目标端为 `755`，所有权均为 `root:root`；需确认目标端权限可接受，详见[权限差异清单](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/ark-full-verify-20261115T2017Z/ACCESS-DIFFERENCES.csv)。
- 其中 14,857 个文件有历史源 SHA-256 和目标读回校验回执，另 56 个小文件经过源、目标独立 SHA-256 比对；本次核对当前文件身份，未重新读取全部内容计算哈希。[完整复查结果](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/ark-full-verify-20261115T2017Z/VERIFIED.md)。

如果业务确认 pipeline 24 个目录和 Ark 的权限差异均可接受，且确认 OpenLoong 目标端额外临时文件可接受，上述七个目录合计 **226.052069 TB、638,348 个文件**。执行前仍须确认对应源目录停写、业务读取迁移并复核源和目标。

### EgoLive 整目录：35.492503 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/EgoLive`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/EgoLive`
- 共 **199,655 个文件、2,831 个目录**。11 月 16 日 10:32，16 个节点完成整目录成员、文件身份及访问属性检查，并独立将全部任务清单和历史成功回执逐路径、SHA-256 及源、目标身份对账；文件和路径差异为零。[完整复查结果及检查点快照](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egolive-receipt-verify-20261115T1732Z/full-proof-20261116T1034BJT/VERIFIED.md)。
- 历史复制回执包含每个源文件的 SHA-256 和目标读回校验；本次核对当前 inode、大小和时间戳，未重新读取全部文件内容计算哈希。10:34 的[16 节点检查](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egolive-receipt-verify-20261115T1732Z/full-rootcheck-20261116T1034BJT.json)确认源、目标根目录均存在，仅检查根目录而非再次递归扫描。
- 全部 **2,831 个目录**源端权限为 `555`、目标端为 `755`，所有权均为 `root:root`，文件权限差异为零；[权限差异逐项清单](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egolive-receipt-verify-20261115T1732Z/full-proof-20261116T1034BJT/ACCESS-DIFFERENCES.csv)。业务需确认目标端权限可接受，且在清理前确认源端停写、读取已迁移并再次核对源和目标。源文件系统在线，本次复核不是原子快照。
- 若同时接受前述三个目录的权限或额外文件条件，以上七个目录与 EgoLive 整目录合计 **261.544572 TB、838,003 个文件**。EgoLive 整目录包含此前列出的 321 个子目录，不能重复计算。

### Egocentric：27 个已完成子目录，4.501533 TB

- 2026-11-16 15:22，已对 Egocentric 的 **27 个互不包含子目录**完成独立对账，涉及 **96,158 个文件、5,567 个目录、4.501533 TB**；16 个节点检查源、目标根目录均存在。完整逐路径名单见文末汇总表，也可查阅[自动清单](/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/docs/operations/projset-egocentric-verified-branches.md)和[独立对账证据](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/BRANCH-VERIFIED.json)。
- 这 5,567 个目录的权限均由源端 `555` 变为目标端 `755`，文件访问属性差异为零；需业务确认权限变化可接受。此次复查依据历史 SHA-256 和目标读回校验回执核对当前目录成员和文件身份，没有重新读取全部内容计算哈希。
- **只能考虑表中明确列出的 27 个子目录**；Egocentric 父目录的复查尚未完成，不能据此清理父目录。清理前仍须确认业务读取迁移、源端停写、旧路径依赖已处理，并重新核对。

## 历史记录：源路径已不存在

截至 2026-11-15 17:43（北京时间），下列四个源路径均已不存在，目标路径仍存在；原 **117.174560 TB** 仅作为历史记录保留，不再计入当前可清理容量。这四个源目录曾完成一轮全目录复核及所需的缺失项处理，并经过逐项复查。

## 1. EgoStandard：49.261095 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/EgoStandard`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/EgoStandard`
- 共 333,706 个文件、6,186 个目录。已完成全目录复核和当前逐项复查；目录成员及源、目标文件身份与首轮成功回执一致，差异、历史冲突、缺失补传项及访问属性差异均为 0。
- 首轮所有文件均完成复制，记录源文件 SHA-256，并对目标副本进行了读取校验。本次没有重新读取全部文件内容计算哈希。
- 为缩短慢节点的尾部等待，已完成部分沿用原节点的持久记录，剩余目录由其他节点并行复查；汇总时核对目录数、文件数和字节数，避免重复计算。
- 证据：[EgoStandard 复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egostandard-tail-20261115T0820Z/EGOSTANDARD-VERIFIED.md)

## 2. OmniVitac：38.616023 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/tactile/OmniVitac_platform_208546131716604003`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/tactile/OmniVitac_platform_208546131716604003`
- 共 738 个文件、8 个目录。16 个节点的当前逐项复查全部结束，目录成员和文件身份均与首轮成功回执一致；差异、历史冲突、缺失补传项及访问属性差异均为 0。
- 首轮 738 个文件均完成复制，并记录源文件 SHA-256 和目标副本读取校验结果。本次没有重新读取全部文件内容计算哈希。
- 源端 `st_blocks` 报告的已分配容量约 38.616023 TB，未发现硬链接；后端实际回收量可能不同。
- 证据：[逐项复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/tactile-screening-20261115T0758Z/CANDIDATES.md)

## 3. Ego-1K：17.459345 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/Ego-1K`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/Ego-1K`
- 共 11,176 个文件、962 个目录。当前逐项复查 100% 完成，目录成员和文件身份均与首轮成功回执一致；差异、历史冲突、缺失补传项及访问属性差异均为 0。
- 首轮所有文件均完成复制，记录源文件 SHA-256，并对目标副本进行了读取校验。本次没有重新读取全部文件内容计算哈希。
- 证据：[逐项复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/candidate-screening-20261115T0610Z/CANDIDATES.md)

## 4. RoboMIND：11.838097 TB

- 源路径：`/share-mixed/projset/eai_pwm/datasets/RoboMIND`
- 目标路径：`/share-mixed/project/eai_pwm/datasets/RoboMIND`
- 共 1,403 个文件、30 个目录。此前全目录复核没有差异、历史冲突或缺失补传项；2026-11-15 16:10（北京时间）16 节点再次逐项复查时，源、目标文件身份与首轮回执一致。
- 首轮目标已存在这些文件；逐文件完整读取源、目标并比对 SHA-256 后，记录为一致跳过。本次复查没有重新计算全部内容哈希。
- 源端 30 个目录权限为 `755`，目标端为 `700`；此差异已由用户确认可以接受。
- 证据：[RoboMIND 最新复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/robomind-fresh-20261115T0809Z/FRESH-RECHECK.md)

## 尚未纳入删除清单

- 2026-11-16 11:02 补充筛选：`eai_pwm/datasets/EgoBody3M`、`eai_pwm/datasets/OpenVidHD`、`eai_pwm/datasets/AgiBotWorld2026` 和 `eai_pwm/datasets/Galaxea-Open-World-Dataset_20260826_hf` 的历史盘点分别为 **6.208 TB、5.201 TB、4.996 TB、2.868 TB**，合计约 **19.273 TB**；相应历史文件数分别为 **4,724、333、175、701**。四处源、目标根目录当前存在，主复核目录队列初筛均已完成；历史容量及队列状态不证明当前完整覆盖。正在使用 16 节点筛查复制回执，待源、目标逐项对账及权限确认完成后才可决定是否纳入清理清单。[筛查任务及限速保护](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/additional-large-initial-20261116T0254Z/README.md)。
- `eai_pwm/datasets/molmobot-data_20260914`：历史全目录 **10.320 TB、14,399 个文件**，其中数据目录的历史 ≥1 GiB 文件约 **10.307 TB、4,691 个**。11:02 的主复核队列初筛显示实际数据子目录已完成，但 `.cache/huggingface/download` 下仍有未完成目录，因此不能将整目录列入清理清单。对五个数据子目录共 15 个源、目标文件的抽样中，大小和修改时间一致，目标 ctime 集中在 11 月 4 日 01:03–01:12 UTC；这些样本只用于定位历史回执，不能证明文件内容或整个目录一致。已将有界日志窗口加入上述 16 节点回执筛查，当前容量、回执覆盖、逐项复查及历史问题记录仍待确认。
- `eai_pwm/datasets/Lightwheel-Purchased-0901_1`：历史成功回执覆盖约 30.281014 TB、1,126,246 个文件，整体目录复核已完成；2026-10-27 的历史盘点为 52.560 TB、1,351,495 个文件，与本次回执数不同，当前源目录完整规模及回执覆盖仍待核实。当前逐项复查正在 16 个节点限速运行，并支持断点续跑；主任务速度持续偏低时自动暂停，恢复后继续。[保护切换及观察记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/lightwheel/GUARDED-ROLLOUT-20261116.md)。已发现目录访问属性差异，抽查的 80 个目录均为源端 `555`、目标端 `755`，完整范围和业务影响仍待确认。现有工作进程仅统计权限差异数量，未逐项保存路径和权限值，需由业务确认或另行补查。复查结束后将自动核对全树清单、回执和检查点；结果出来前不能按此清单删除。汇总已支持在节点状态暂不可读时使用持久检查点，避免遗漏该节点整份进度；若状态和检查点均不可读，汇总会明确标注。进度见 [Lightwheel 复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/lightwheel/CANDIDATES.md)。
  - 05:05 的[检查点子目录初筛](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/lightwheel/subtree-screen-result.json)发现 1,097 个一级子目录的任务已越过检查点，合计 371,150 个文件、约 10.023 TB，最大单个子目录约 0.095 TB。这是初筛，最终清单与回执的逐路径核对及权限差异确认尚未完成，不构成删除依据；这些数据已包含在上述 Lightwheel 候选容量中。
- `eai_pwm/datasets/Egocentric-10K-unpacked`：2026-10-27 历史盘点为 **17.963 TB、403,924 个文件**；06:19 确认源、目标根目录存在，此前全目录复核的 21,733 个目录均完成，未记录冲突或未解决扫描错误。当前逐文件回执覆盖和访问属性尚未完成复查；根目录源端权限 `555`、目标端 `755`。历史数量不代表当前规模，暂不能删除。已完成 16 节点低速回执时间采样，结果仅供定位历史日志，见 [采样记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/ctime-sampling-20261115T1800Z/SAMPLE.md)；首版规划只保留日志边界片段，无法证明覆盖目标时间窗口。16 节点已完成回执窗口提取和摘要核验，得到 **403,924 个有效唯一回执、17.962617 TB**，与历史盘点相同，未发现多身份记录。[回执汇总](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-receipt-windows-20261115T2222Z/COVERAGE.md)。这不是当前全树覆盖证明，历史问题与未解决扫描错误查询已完成，均为零；[16 节点逐项复查](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/CANDIDATES.md)已启动，并在主任务明显降速时自动暂停。仍需完成当前源、目标逐项身份核对及访问权限确认。[问题记录查询](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-receipt-windows-20261115T2222Z/audit-issues-current.json)。
- `eai_pwm/datasets/Ego-Exo4D`：2026-10-27 历史盘点为 **10.715 TB、205,787 个文件**；源、目标根目录当前存在，此前全目录复核的 40,602 个目录均完成，未记录冲突或未解决扫描错误。其中 `takes` 子目录历史盘点为 **10.523 TB、87,801 个文件**，其 20,141 个目录已完成主复核。复核既有 16 节点提取结果后，已找到该子目录 **55,978 个唯一成功回执、10.214652 TB**；历史盘点与当前范围可能不同，回执覆盖仍未证明完整。根目录源端权限 `555`、目标端 `755`，暂不能删除。[回执初筛记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egoexo-takes-receipt-reconcile-20261115T1900Z/RESULT.md)。
- `eai_pwm/datasets/action_processed_r1/RoboMIND2.0-Franka-EEF`：2026-10-27 历史盘点为 **15.338 TB、847,359 个文件**。最新队列清单中 1,157 个目录均已完成复核，其对应问题记录和未解决扫描错误为零，源、目标根目录当前存在且根目录权限均为 `755`。历史盘点为 1,151 个目录，与本次队列数量不同；完整当前文件数、回执覆盖及 16 节点逐项复查仍待确认，暂不能删除。16 节点对 128 个目录重复采样，108 个文件的路径、目标 ctime 和大小与此前记录完全一致；其中 96 个集中在 11 月 5 日 15:00–16:00 UTC，已据此完成[回执窗口规划](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/franka-eef-receipt-windows-20261115T2236Z/README.md)，16 节点已核验 104 个日志分段，找到 **622,495 个有效唯一回执、15.326974 TB**，无多身份记录；与历史盘点的文件数仍有差距，不能把差值直接视为缺失文件。[回执汇总](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/franka-eef-receipt-windows-20261115T2236Z/COVERAGE.md)。 回执全部位于 `real` 子目录，其中 `real/cameras-c064307290__fps15` 有 **11.342702 TB、468,576 条回执**；父子路径不能重复相加，[分组线索](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/franka-eef-receipt-windows-20261115T2236Z/RECEIPT-GROUPS.md)仍不是当前覆盖或删除依据。采样不构成复查或删除依据。[采样记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/ctime-franka-eef-20261115T2235Z/SAMPLE.md)。[队列问题查询记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/franka-eef-issues.json)。
- `eai_pwm/datasets/RoboMIND2.0-Agilex-mobile`：2026-10-27 历史盘点为 **10.234 TB、13,952 个文件**，不代表当前规模。主复核队列中 27,903 个目录已完成，且清单中父目录关系完整；2026-11-16 05:10 源、目标根目录仍存在，根目录权限分别为 `555`、`755`。16 节点低频采样了 128 个目录、64 个目标文件；目标 ctime 集中在 11 月 5 日 22:00–24:00 UTC，仅作为定位历史回执的线索，见 [采样记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/ctime-agilex-mobile-20261115T2110Z/SAMPLE.md)。[回执窗口提取](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/agilex-mobile-receipt-windows-20261115T2114Z/README.md)已在 16 节点完成，读取约 0.000319 TB，并受主任务健康保护。当前全树回执覆盖和逐项复查尚未完成，不能删除。
  - 此路径及其子路径未出现在[历史冲突清单](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/attention-20261112T084400Z-ba8209ff/conflicts-current.csv)中。
  - 所选回执窗口已由 16 节点读取并核验全部 45 段，得到 **12,523 个唯一成功回执、约 9.193 TB**，未发现多身份记录；见[窗口汇总](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/agilex-mobile-receipt-windows-20261115T2114Z/COVERAGE.md)。与历史盘点仍有差距，可能存在窗口未覆盖的回执或当前范围变化，不能将差值视为已确认缺失文件。
  - 现有 64 个目标文件样本中，5 个不在本次回执索引内，目标 ctime 仍落在所选时间范围；[抽样路径及说明](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/agilex-mobile-receipt-windows-20261115T2114Z/SAMPLE-RECEIPT-GAPS.md)。这 5 个样本的源和目标均存在，大小及修改时间一致，详见[元数据核对](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/agilex-mobile-receipt-windows-20261115T2114Z/SAMPLE-GAP-METADATA.md)；本次未重算内容哈希，不能据此推断整个目录的覆盖情况。
  - 06:13 已低频读取主复核问题和失败记录至当时表尾，覆盖 27,903 个目录的清单，未发现该范围的问题项或未解决失败；101 次数据库锁重试已结束。[扫描结果](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/agilex-mobile-receipt-windows-20261115T2114Z/audit-issues-current.json)不是在线源的原子快照，不能单独作为删除依据。
- `eai_pwm/datasets/RoboMIND2.0-UR5`、`eai_pwm/datasets/RoboMIND2.0-Franka-Part-3`、`eai_pwm/datasets/RoboMIND2.0-Agilex`：历史盘点分别为 **15.584 TB、13.900 TB、13.280 TB**，合计约 **42.764 TB**；2026-11-16 03:40 源、目标根目录均存在。此前每个目录仅筛查了前 30,001 条主复核目录记录，尚不能认定全目录完成；正在按检查点继续筛查，并在主复核较慢时暂停。这些是历史容量，当前范围、冲突、回执覆盖和 16 节点逐项复查均待确认，暂不能删除。[深度筛查状态](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/deep-roots-20261115T1940Z/README.md)。
- `eai_pwm/datasets/action_processed_r1/_processing`：历史记录为 **39.065 TB、约 1,741 万个文件**；06:33 的只读结构初筛中，源和目标当前各仅见一个 `RoboCasa365/status.json`，不能将历史容量计入当前可清理量。[初筛记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/processing-current-screen-20261116.md)。
- `eai_pwm/datasets/IndEgo`、`eai_pwm/datasets/action_processed_r1/RoboMIND2.0-Joint` 和 `eai_pwm/datasets/action_processed_r3`：2026-10-27 历史盘点分别为 **19.061 TB、22,889 个文件**，**27.856 TB、1,704,217 个文件**和 **20.157 TB、318,173 个文件**；11 月 16 日 08:00 初筛时三处的源、目标根目录均存在，根目录权限均为 `755`。历史容量不代表当前规模；全目录复核、回执覆盖和 16 节点逐项复查均待确认，暂不能删除。[低频索引初筛](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/large-extra-20261116T0002Z/README.md)已启动，并在主任务健康或速度保护未通过时自动暂停。
- `eai_pwm/datasets/tactile/zhen`、`eai_pwm/datasets/tactile/OpenNeoData_20260915`：2026-10-27 历史盘点分别约 **11.532 TB、100.69 万个文件**和 **9.070 TB、7.83 万个文件**；07:11 初筛源、目标根目录均存在且根目录权限均为 `755`，旧冲突清单未匹配到这些路径。历史容量不代表当前规模；全目录复核、回执覆盖和 16 节点当前逐项复查均未确认，暂不能删除。[受保护的索引初筛](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-roots-screen-20261115T1754Z/tactile-extra-20261115T2325Z/README.md)已启动，两处均发现主复核尚未完成的子目录，程序会等待并重试。
- `tactile` 整体有历史冲突，不能按本清单删除整个父目录；本清单仅涵盖上面列出的具体子目录。

## 已复查目录汇总表

截至 **2026-11-16 15:23（北京时间）**，以下 8 个完整目录及 27 个 Egocentric 子目录均已完成各自范围的复核和逐项对账。前 8 个目录的源、目标根目录于 15:19 检查存在，27 个 Egocentric 子目录的源、目标根目录于 15:22 由 16 个节点检查存在。表中容量和文件数来自各自复查时的记录；最近一次仅重新检查了根目录，未重新遍历全树。目标路径由表中源路径的 `/share-mixed/projset/` 前缀替换为 `/share-mixed/project/` 得到。

| 源目录 | 文件逻辑容量（TB） | 文件数 | 目录数 | 清理前的额外确认 | 复查证据 |
|---|---:|---:|---:|---|---|
| `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021` | 99.537043 | 151,516 | 20,156 | 无已发现的额外差异 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/screen-more-20261115T1410Z/OMNIVITAC-VERIFIED.md) |
| `/share-mixed/projset/eai_pwm/datasets/Nymeria` | 54.099820 | 58,586 | 25,056 | 无已发现的额外差异 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/merged/tail/NYMERIA.md) |
| `/share-mixed/projset/eai_pwm/datasets/EgoPro` | 12.291040 | 31,470 | 2,235 | 无已发现的额外差异 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/CANDIDATES.md) |
| `/share-mixed/projset/eai_pwm/paxini_cleaning/source/Paxini-Omnisharing` | 9.708534 | 45,119 | 33 | 无已发现的额外差异 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/CANDIDATES.md) |
| `/share-mixed/projset/eai_pwm/datasets/EgoLive` | 35.492503 | 199,655 | 2,831 | 2,831 个目录权限从源端 `555` 变为目标端 `755`，需业务确认 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egolive-receipt-verify-20261115T1732Z/full-proof-20261116T1034BJT/VERIFIED.md) |
| `/share-mixed/projset/eai_pwm/pipeline_store/sources/egostandard` | 22.719924 | 327,494 | 6,090 | 24 个目录权限从源端 `755` 变为目标端 `700`，需业务确认 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/pipeline-tail/PIPELINE.md) |
| `/share-mixed/projset/eai_pwm/datasets/tactile/OpenLoong-Community_visuo-tactile` | 17.249460 | 9,250 | 118 | 目标端额外保留 46 个临时文件，需业务确认可接受 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/openloong-fresh-20261115T1618Z/RECHECK.md) |
| `/share-mixed/projset/eai_pwm/datasets/RoboMIND2.0-Ark` | 10.446248 | 14,913 | 29,825 | 29,825 个目录权限从源端 `555` 变为目标端 `755`，需业务确认 | [复查记录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/ark-full-verify-20261115T2017Z/VERIFIED.md) |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_014` | 0.882187 | 20,527 | 995 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | [子目录对账](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/BRANCH-VERIFIED.json) |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_013` | 0.561495 | 13,058 | 619 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_026` | 0.427532 | 9,957 | 500 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_025` | 0.396913 | 9,245 | 470 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_012` | 0.390332 | 9,079 | 435 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_011` | 0.385418 | 8,975 | 444 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_018` | 0.275625 | 6,416 | 324 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_021` | 0.167155 | 3,898 | 189 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_007` | 0.158276 | 792 | 299 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_022` | 0.130381 | 3,039 | 147 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_015` | 0.105922 | 2,466 | 126 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_020` | 0.092372 | 2,151 | 104 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_019` | 0.075933 | 1,772 | 104 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_010` | 0.066248 | 1,544 | 83 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_001` | 0.057796 | 290 | 109 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_008` | 0.048583 | 243 | 94 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_009` | 0.046956 | 236 | 91 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_023` | 0.041972 | 977 | 52 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_003` | 0.041967 | 212 | 81 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_002` | 0.032387 | 162 | 66 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_004` | 0.030185 | 152 | 62 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_016` | 0.029840 | 153 | 58 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_006` | 0.015434 | 77 | 33 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_024` | 0.013239 | 311 | 25 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_005` | 0.011797 | 63 | 32 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_028` | 0.007931 | 185 | 13 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_017` | 0.007656 | 178 | 12 | 目录权限从源端 `555` 变为目标端 `755`，需业务确认 | 同上一项 |
| **无已发现额外差异的小计** | **175.636437** | **286,691** | **47,480** | 前四行；仍须满足下列共同条件 | — |
| **需确认额外条件的小计** | **90.409668** | **647,470** | **44,431** | 四个完整目录及 27 个 Egocentric 子目录；额外条件均需确认 | — |
| **35 个路径合计** | **266.046105** | **934,161** | **91,911** | 不包含源路径已不存在或尚未完成复查的目录 | — |

**所有目录共同条件**：业务确认读取已迁往目标、源端已停写，并检查旧路径符号链接和其他读取依赖；执行清理时重新核对源、目标状态。源文件系统在线，历史复核不是原子快照。本次逐项复查主要依据历史 SHA-256 及目标读回校验回执、当前文件身份和目录成员，未重新读取所有文件内容计算哈希。

容量按 1 TB = 10¹² 字节统计，是文件逻辑容量；实际释放空间以存储系统统计为准。EgoLive 整目录已包含其子目录，不得重复计算；Egocentric 仅纳入表中 27 个子目录，其父目录尚未完成复查。单行容量保留 6 位小数，合计按精确字节汇总后取整，因此单行数值相加可能有尾数差异。上文 4 个源路径已不存在的历史目录未计入本表。
