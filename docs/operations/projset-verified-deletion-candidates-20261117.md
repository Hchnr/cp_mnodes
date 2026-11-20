# projset 已复查且尚未删除的目录

更新时间：2026-11-20 14:52（北京时间）。已排除 2026-11-17 用户指定并已删除的 27 个路径，以及 11 月 20 日用户告知已删除的 Lightwheel；本清单目前有 **8 个独立子目录**，其根目录状态沿用 11:11 的检查。

以下路径已完成各自范围的复核和历史回执逐项对账，未发现文件或路径差异。11 月 20 日 11:11 从管理节点检查，源、目标根目录均存在，inode、权限、修改时间和变更时间与 11 月 16 日记录一致；**这只是非递归根目录检查，不能证明内部没有发生变化**。源目录在线，复查不是原子快照。

| 源目录 | 逻辑容量（TB） | 文件数 | 目录数 |
|---|---:|---:|---:|
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_002` | 0.032387 | 162 | 66 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_004` | 0.030185 | 152 | 62 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_016` | 0.029840 | 153 | 58 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_006` | 0.015434 | 77 | 33 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_024` | 0.013239 | 311 | 25 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_005` | 0.011797 | 63 | 32 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_028` | 0.007931 | 185 | 13 |
| `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked/factory_017` | 0.007656 | 178 | 12 |
| **合计** | **0.148469** | **1,281** | **301** |

目标路径将前缀 `/share-mixed/projset/` 替换为 `/share-mixed/project/`。容量是文件逻辑大小（1 TB = 10¹² 字节），实际回收量以存储系统统计为准。

这 8 个子目录下的目录权限从源端 `555` 变为目标端 `755`，文件访问属性未发现差异；执行删除前仍须确认业务接受该权限、读取已切换、源端已停写，并核对旧路径依赖及执行时源目标状态。本次复查依据历史 SHA-256 和目标读回校验回执及当时的文件身份，未重新读取全部文件内容计算哈希。

[逐项复查及回执对账](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/BRANCH-VERIFIED.json) · [权限差异明细](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/BRANCH-ACCESS-DIFFERENCES.csv) · [11 月 16 日 16 节点根目录检查](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/branch-rootcheck.json) · [原汇总](projset-verified-deletion-candidates-20261115.md)

## Lightwheel：历史复查记录，源目录已删除

| 源目录 | 逻辑容量（TB） | 文件数 | 目录数 | 状态 |
|---|---:|---:|---:|---|
| `/share-mixed/projset/eai_pwm/datasets/Lightwheel-Purchased-0901_1` | 30.281014 | 1,126,246 | 678,835 | 用户告知源目录已删除；11 月 20 日 14:52 源根目录不存在，不计入候选 |

16 节点全树回执身份复查于 11 月 18 日 06:26 完成，回执索引与清单逐路径、SHA-256 和源目标身份对账一致，文件或路径差异为 0，文件访问属性差异为 0。**678,835 项目录存在访问属性差异**，复查只记录数量，未保存逐项权限值：不能据此推断所有目录的具体差异。11 月 20 日 11:11 抽查时源、目标根目录均存在，权限分别为 `555`、`755`，双方 uid/gid 均为 `0/0`；这不能代表所有子目录。用户随后告知已删除源目录；14:52 非递归检查源根路径不存在，目标根目录存在，未重新扫描目标内部。本项不计入上方可清理合计。[Lightwheel 复查证据](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/lightwheel/VERIFIED.md)。

复查比对历史回执及当时文件身份和元数据，没有重读全部文件计算内容哈希，在线源不是原子快照。删除前须确认停写、业务读路径和指向源路径的链接已迁移，以及执行时源目标状态。

Egocentric 父目录整体复查尚未完成，不能据此清理整个父目录；进度见[大目录复查自动进展](projset-large-candidates-live.md)。


## 11 月 19 日复查更新

截至 11 月 19 日 11:49，现有完成证明中未增加可直接清理的独立目录。上表 8 个 Egocentric 子目录仍合计 0.148469 TB、1,281 个文件，源、目标根目录的 inode、修改时间及变更时间与 11 月 16 日记录一致。该检查只覆盖根目录。Lightwheel 30.281014 TB、1,126,246 个文件的完成证明继续有效于其原复查范围，目录权限明细仍待补查或业务确认。

Egocentric 整体最新记录为 278,151 / 403,924 个文件、14,861 / 21,733 个目录，尚未完成，节点状态需要复核。大容量少文件候选的 16 节点历史回执分段提取已完成，覆盖对账尚未发布完成证明，暂不能计入本清单。


## 11 月 20 日复查更新

截至 11 月 20 日 11:11，未发现新增完整复查证明。上表 8 个 Egocentric 子目录的源、目标根目录均存在，inode、权限、修改时间和变更时间与 11 月 16 日记录一致；本次为非递归根目录核查。Lightwheel 的源、目标根目录均存在，根目录权限分别为 555 和 755，uid/gid 均为 0/0。其 678,835 项目录访问属性差异仍无逐项权限记录，需进一步确认。

Egocentric 整体复查记录为 360,429 / 403,924 个文件、19,376 / 21,733 个目录，尚未完成，后续须核查历史问题。大容量少文件候选尚无覆盖对账完成证明，不计入可清理合计。
