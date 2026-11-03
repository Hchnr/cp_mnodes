# 混闪空间分布与清理准备：全量扫描报告（第 5 层补充统计另行进行）

报告更新：2026-10-27 14:14:27（北京时间）。统计快照：2026-10-27 14:13:46 北京时间。

> 本轮完成核验：全部 123,682,056 个目录任务完成；两侧共 661,056,361 个普通文件路径、2,036.466 TB 逻辑大小，已入库读取错误为 0，completion-audit.json 全项通过。第 5 层补充扫描属于新一轮在线统计，结果不会混入本轮总量。

## 1. 结论与总体进度

扫描范围为 `/share-mixed/project` 与 `/share-mixed/projset`，包含隐藏目录。project 已完成本轮遍历；projset 的巨型缓存批次及其新发现子目录均已完成。本轮全范围扫描已完成，完整性核验通过；第 5 层补充扫描另行运行。本次没有删除数据。

| 范围 | 已统计逻辑 TB | 数据量完成率 | 普通文件路径数 | 文件数完成率 | 已完成目录数 |
|---|---:|---|---:|---|---:|
| /share-mixed/project | 1,028.126 | 100%（本轮遍历） | 433,469,269 | 100%（本轮遍历） | 42,989,431 |
| /share-mixed/projset | 1,008.340 | 100%（本轮遍历） | 227,587,092 | 100%（本轮遍历） | 80,692,625 |
| 合计 | 2,036.466 | 100%（本轮遍历） | 661,056,361 | 100%（本轮遍历） | 123,682,056 |

本轮恢复扫描最终状态：
- 节点状态：`{'complete': 24}`；恢复扫描累计完成 **5,282,858 个目录任务**。
- 实时队列：待扫描 **0 个目录**，已领取未完成 **0 个目录**。本轮队列已闭合，全部节点退出完成；容量与文件数完成率均为 100%。
- 失效连接遗留的 8,768 个未完成任务已重新入队，已入库结果保留；历史失败记录已归档。
- 已入库读取错误：project 0，projset 0。无待返回任务，完整性核验全部通过。

这是在线盘点，不是同一时刻的原子快照；本轮完成不代表之后没有新增。容量及文件数均按路径累计，硬链接未全量去重。

## 2. 巨型 frame_cache 的拖尾说明

```text
/share-mixed/projset/eai_pwm/datasets/action_processed_r2/_work/molmobot_stage1_multiview_20261009_v1/shared_visual_cache/frame_cache
```

- **当前状态：所在的 64 目录批次已于今天约 13:52 完成回收，不再等待该批次。** 从约 05:52 开始观察到读取，拖尾约 **8 小时**；起始时间来自观察记录，并非精确启动日志。
- 独立名称清点完成：**29,693,722 个直接子项（不含 `.`、`..`，包含隐藏项）**，读取至目录结束并检查名称唯一性，耗时约 46.13 分钟。目录项数量不能直接当作普通文件数。
- 完成回执统计：整批 **34,820,834 个普通文件、2,856,148,842,206 字节（2.856 TB）**，并发布 **703,570 个子目录**继续递归扫描。这些是 64 目录整批统计，不能全部归属于 frame_cache。
- 随后已恢复 24 节点扫描新发现的下级目录，抽查任务位于同一工作区域的 `tasks/molmobot_joint_native_…` 子树。父目录批次回收不等于整棵子树完成。
- 原扫描每进程 8 个目录线程、共享 64 个属性查询线程，每次取 256 个目录项；巨型单目录没有跨节点分片，整批结束前缺少内部完成计数。因此此前只能观察进程活动，无法计算文件级进度或 ETA；现在的批次完成以持久化回执为证据。
- 曾尝试额外并行优化，短时收益未在持续运行中保持，已停止候选扫描；候选部分结果未加入正式统计。单目录集中大量条目与扫描器缺少目录内分片共同造成拖尾。
- 后续应评估缓存生命周期、分层目录和适合访问方式的数据打包，并为扫描器增加增量回执与可恢复进度。尚不能可靠承诺全范围完成时刻。

## 3. 容量口径与 df 对照

TB=10^12 字节；TiB=2^40 字节。扫描逻辑容量累计普通文件 st_size，文件数也是普通文件路径数；硬链接未全量 inode 去重。st_blocks×512 同样按路径累计，不是精确可释放物理容量。符号链接单独统计且不跟随，跨 st_dev 的条目不继续扫描。

此前“约 82%”来自逻辑大小除以 df 已用空间，分子分母口径不同，**撤回其作为扫描完成率的解释**。

```text
Filesystem                                                                                                                    1B-blocks             Used       Available Use% Mounted on
www.nfs.com:/mnt/zhiyuan/airs/sharefs/a5833582-1c4a-49af-8d0b-be91b4088ca3/a5198d7a-cb99-4eec-bbd9-2c9b9a9b0926/PUBLIC 1125281431552000 1050045640081408  75235791470592  94% /share-mixed/project
www.nfs.com:/mnt/zhiyuan/airs/sharefs/a5833582-1c4a-49af-8d0b-be91b4088ca3/PUBLIC                                      1425899906924544 1227065306120192 198834600804352  87% /share-mixed/projset
```

两个路径是不同 NFS 导出，返回不同容量上限，不能都套用 project 的约 1 PB 上限；是否共享底层存储池尚未确认，不能将两行简单相加为独立物理容量。df 是采集时文件系统口径，扫描则跨多个时刻。

## 4. 一级目录容量明细

以下路径以 `/share-mixed/` 为前缀。同级排行不是完整总量分解，上层直接存放的文件及未列出的目录不包含在表内；父子目录不能重复相加。

### project

| 路径 | 状态 | 逻辑 TB | 普通文件路径数 | 目录数 |
|---|---|---:|---:|---:|
| `project/eai_pwm` | 完成 | 803.840 | 332,538,124 | 24,270,761 |
| `project/yhzhang` | 完成 | 149.707 | 7,851,876 | 3,328,309 |
| `project/gz` | 完成 | 40.481 | 22,580,610 | 2,368,588 |
| `project/liyue` | 完成 | 16.160 | 19,942,733 | 4,926,961 |
| `project/linzhe` | 完成 | 10.040 | 31,038,961 | 7,178,141 |
| `project/zhiwei` | 完成 | 6.153 | 738,372 | 20,613 |
| `project/zy` | 完成 | 0.788 | 6,580,980 | 676,126 |
| `project/.hcr-copy-20261021T141055Z-overlap-2db42a` | 完成 | 0.360 | 5,715,561 | 103,548 |
| `project/.hcr-copy-20261023T141342Z-2dad-d7de938a` | 完成 | 0.174 | 2,152,096 | 56,915 |
| `project/lizhiyu` | 完成 | 0.133 | 91,936 | 536 |
| `project/.hcr-copy-p4-20261024T015159Z-2dad-26b053` | 完成 | 0.123 | 1,401,426 | 16,152 |
| `project/.hcr-copy-20261023T083418Z-c3559-4e81a5a7` | 完成 | 0.074 | 2,417,733 | 11,849 |
| `project/.hcr-readonly-audit-20261026-full-0107` | 完成 | 0.037 | 1,514 | 918 |
| `project/.hcr-copy-20261021T064717Z-resume-e25450` | 完成 | 0.014 | 347,775 | 14,507 |
| `project/.hcr-copy-fixed-reaudit-20261026T042442Z-a22826ff-files` | 完成 | 0.013 | 377 | 81 |

### projset

| 路径 | 状态 | 逻辑 TB | 普通文件路径数 | 目录数 |
|---|---|---:|---:|---:|
| `projset/eai_pwm` | 完成 | 992.721 | 226,414,014 | 80,588,848 |
| `projset/baiyu` | 完成 | 12.109 | 996,176 | 102,627 |
| `projset/liyue` | 完成 | 2.729 | 9,026 | 292 |
| `projset/materialized` | 完成 | 0.781 | 167,822 | 846 |
| `projset/ks3_cleanup_20261009` | 完成 | 0.000 | 26 | 3 |
| `projset/trex_ks3_transfer_20261009` | 完成 | 0.000 | 22 | 3 |
| `projset/RoboMIND2.0-Franka-Part-2` | 完成 | 0.000 | 1 | 2 |
| `projset/trex_audit_20261009` | 完成 | 0.000 | 3 | 2 |
| `projset/hcr` | 完成 | 0.000 | 1 | 1 |

## 5. 三级目录容量明细

以下路径以 `/share-mixed/` 为前缀。同级排行不是完整总量分解，上层直接存放的文件及未列出的目录不包含在表内；父子目录不能重复相加。

### project

| 路径 | 状态 | 逻辑 TB | 普通文件路径数 | 目录数 |
|---|---|---:|---:|---:|
| `project/eai_pwm/pipeline_store/materialized` | 完成 | 273.660 | 42,229,573 | 3,315,472 |
| `project/eai_pwm/datasets/AgiBotWorld-Beta` | 完成 | 48.104 | 6,527 | 311 |
| `project/eai_pwm/datasets/download_jt` | 完成 | 46.052 | 14,124,056 | 628,628 |
| `project/eai_pwm/action_line/runs` | 完成 | 43.921 | 46,108 | 4,446 |
| `project/eai_pwm/pipeline_store/latents` | 完成 | 42.715 | 71,116 | 566 |
| `project/eai_pwm/pipeline_store/sources` | 完成 | 33.119 | 1,817,011 | 373,544 |
| `project/yhzhang/hand_pose_1023/egostandard` | 完成 | 28.820 | 2,340,846 | 1,039,053 |
| `project/yhzhang/data/ego_exo4d` | 完成 | 25.880 | 202,230 | 43,556 |
| `project/eai_pwm/datasets/action_processed_r1` | 完成 | 23.907 | 2,212,336 | 1,216,405 |
| `project/eai_pwm/datasets/train` | 完成 | 23.711 | 256,707 | 56,867 |
| `project/gz/DexTrain/results` | 完成 | 19.913 | 144,443 | 19,369 |
| `project/yhzhang/data/xperience` | 完成 | 18.958 | 57,316 | 29,513 |
| `project/yhzhang/hand_pose_1023/lightwheel` | 完成 | 17.639 | 1,817,658 | 805,984 |
| `project/yhzhang/data/Eidon-Tracker-POV` | 完成 | 13.797 | 110,846 | 27,428 |
| `project/eai_pwm/datasets/stanford-vision-lab` | 完成 | 12.952 | 8,170 | 7 |
| `project/yhzhang/hand_pose_1023/egolive` | 完成 | 12.805 | 582,163 | 259,097 |
| `project/eai_pwm/prepared_datasets/gpic_train_v1_94_incremental` | 完成 | 12.784 | 100,055,905 | 111,534 |
| `project/eai_pwm/home/hcr` | 完成 | 12.237 | 2,111,707 | 314,196 |
| `project/eai_pwm/datasets/RoboMIND` | 完成 | 11.838 | 1,403 | 30 |
| `project/yhzhang/data/PsiBotAI_SynData` | 完成 | 11.405 | 1,576 | 275 |

### projset

| 路径 | 状态 | 逻辑 TB | 普通文件路径数 | 目录数 |
|---|---|---:|---:|---:|
| `projset/eai_pwm/datasets/action_processed_r1` | 完成 | 198.642 | 49,297,657 | 4,502,057 |
| `projset/eai_pwm/datasets/tactile` | 完成 | 179.479 | 6,706,897 | 2,885,705 |
| `projset/eai_pwm/datasets/Nymeria` | 完成 | 54.100 | 58,586 | 25,056 |
| `projset/eai_pwm/datasets/Lightwheel-Purchased-0901_1` | 完成 | 52.560 | 1,351,495 | 678,835 |
| `projset/eai_pwm/datasets/EgoStandard` | 完成 | 49.261 | 333,706 | 6,186 |
| `projset/eai_pwm/datasets/EgoLive` | 完成 | 35.493 | 199,655 | 2,831 |
| `projset/eai_pwm/pipeline_store/sources` | 完成 | 22.720 | 327,494 | 6,092 |
| `projset/eai_pwm/datasets/Sekai` | 完成 | 20.509 | 24,936 | 6,547 |
| `projset/eai_pwm/datasets/action_processed_r3` | 完成 | 20.157 | 318,173 | 11,398 |
| `projset/eai_pwm/datasets/IndEgo` | 完成 | 19.061 | 22,889 | 1,592 |
| `projset/eai_pwm/datasets/Egocentric-10K-unpacked` | 完成 | 17.963 | 403,924 | 21,733 |
| `projset/eai_pwm/datasets/Ego-1K` | 完成 | 17.459 | 11,176 | 962 |
| `projset/eai_pwm/datasets/RoboMIND2.0-UR5` | 完成 | 15.584 | 54,592 | 109,185 |
| `projset/eai_pwm/unpacked/robomind1` | 完成 | 15.034 | 105,260 | 212,479 |
| `projset/eai_pwm/datasets/RoboMIND2.0-Franka-Part-3` | 完成 | 13.900 | 38,792 | 77,579 |
| `projset/eai_pwm/datasets/RoboMIND2.0-Agilex` | 完成 | 13.280 | 22,274 | 44,547 |
| `projset/eai_pwm/datasets/EgoPro` | 完成 | 12.291 | 31,470 | 2,235 |
| `projset/eai_pwm/datasets/OpenAoE-2000h` | 完成 | 11.842 | 212,759 | 168,104 |
| `projset/eai_pwm/datasets/RoboMIND` | 完成 | 11.838 | 1,403 | 30 |
| `projset/baiyu/project/cosmos-framework-private` | 完成 | 11.423 | 39,013 | 1,769 |

## 6. 文件与目录密度

以下为两个范围合并的三级目录排行，使用报告标注的汇总快照。

### 文件数量前 12 名

| 路径 | 状态 | 逻辑 TB | 普通文件路径数 | 目录数 |
|---|---|---:|---:|---:|
| `projset/eai_pwm/datasets/action_processed_r2` | 完成 | 11.406 | 164,391,941 | 71,323,110 |
| `project/eai_pwm/prepared_datasets/gpic_train_v1_94_incremental` | 完成 | 12.784 | 100,055,905 | 111,534 |
| `projset/eai_pwm/datasets/action_processed_r1` | 完成 | 198.642 | 49,297,657 | 4,502,057 |
| `project/eai_pwm/pipeline_store/materialized` | 完成 | 273.660 | 42,229,573 | 3,315,472 |
| `project/eai_pwm/prepared_datasets/pwm_reference_media_v1_20260808` | 完成 | 1.034 | 23,605,873 | 3,379 |
| `project/liyue/cosmos-framework/outputs` | 完成 | 9.007 | 18,528,455 | 4,828,730 |
| `project/linzhe/xiaomi0-libero/runs` | 完成 | 1.975 | 18,143,509 | 4,715,659 |
| `project/eai_pwm/prepared_datasets/pwm_reference_media` | 完成 | 2.187 | 15,845,748 | 128,031 |
| `project/eai_pwm/datasets/download_jt` | 完成 | 46.052 | 14,124,056 | 628,628 |
| `project/gz/data/tac_data` | 完成 | 9.357 | 12,250,097 | 729,067 |
| `project/eai_pwm/action_line/abs16` | 完成 | 1.904 | 9,115,454 | 227,332 |
| `project/eai_pwm/datasets/SPAR` | 完成 | 2.261 | 8,438,123 | 134,552 |

### 目录数量前 12 名

| 路径 | 状态 | 逻辑 TB | 普通文件路径数 | 目录数 |
|---|---|---:|---:|---:|
| `projset/eai_pwm/datasets/action_processed_r2` | 完成 | 11.406 | 164,391,941 | 71,323,110 |
| `project/eai_pwm/home/yuanhai` | 完成 | 6.790 | 8,160,463 | 8,491,564 |
| `project/liyue/cosmos-framework/outputs` | 完成 | 9.007 | 18,528,455 | 4,828,730 |
| `project/linzhe/xiaomi0-libero/runs` | 完成 | 1.975 | 18,143,509 | 4,715,659 |
| `projset/eai_pwm/datasets/action_processed_r1` | 完成 | 198.642 | 49,297,657 | 4,502,057 |
| `project/eai_pwm/pipeline_store/materialized` | 完成 | 273.660 | 42,229,573 | 3,315,472 |
| `projset/eai_pwm/datasets/tactile` | 完成 | 179.479 | 6,706,897 | 2,885,705 |
| `project/eai_pwm/datasets/action_processed_r2` | 完成 | 0.540 | 4,378,018 | 2,286,556 |
| `project/linzhe/roboclaw-vlabench/runs` | 完成 | 4.262 | 7,665,735 | 1,607,216 |
| `project/eai_pwm/datasets/_gl` | 完成 | 0.001 | 185 | 1,356,852 |
| `project/eai_pwm/datasets/action_processed_r1` | 完成 | 23.907 | 2,212,336 | 1,216,405 |
| `project/eai_pwm/pipeline_store/vimix-14m` | 完成 | 0.156 | 1,209,871 | 1,195,093 |

### 文件大小分布（已入库部分）

| 范围 | 文件大小 | 文件数 | 逻辑 TB |
|---|---|---:|---:|
| project | <4 KiB | 65,836,897 | 0.067 |
| project | 4 KiB 至 <1 MiB | 313,773,433 | 41.033 |
| project | 1 MiB 至 <1 GiB | 53,771,634 | 586.381 |
| project | ≥1 GiB | 87,305 | 400.645 |
| projset | <4 KiB | 60,148,954 | 0.142 |
| projset | 4 KiB 至 <1 MiB | 150,205,513 | 15.172 |
| projset | 1 MiB 至 <1 GiB | 17,098,273 | 639.641 |
| projset | ≥1 GiB | 134,352 | 353.385 |

## 7. 重复数据与清理核查对象

| 对象及位置 | 数据规模 | 证据与边界 |
|---|---|---|
| 两侧 `eai_pwm/datasets/RoboMIND` | 每侧 1,403 文件、11.838 TB | 相对路径及大小完全一致；15 个小文件完整 SHA-256 一致，1,388 个大文件头中尾抽样一致。尚非全量内容校验，不能直接认定可释放 11.838 TB。 |
| `project/eai_pwm/datasets/train/IKEA-ASM/data` | 两组单侧合计 304.192 GB | 两个迁移临时文件分别与 all_depth_frames.tar.gz、ikea_asm_dataset_RGB_multiview.zip 完整 SHA-256 相同；不同 inode、nlink=1。需确认任务及引用后讨论清理。 |
| `project/eai_pwm/home/hcr/cosmos3_demo` 的三份 `__0_0.distcp` | 每份 20.169 GB | 三份完整 SHA-256 相同，不同 inode、nlink=1；仅证明分片相同，不代表整个 checkpoint 或实验可替代。 |
| Qwen3-VL-2B 权重：`project/eai_pwm/models/Qwen3-VL-2B-Instruct/model.safetensors` 与 `project/gz/DexTrain/checkpoints/qwen/model.safetensors` | 每份 4.255 GB | 完整 SHA-256 一致、inode 不同；用途待确认。 |
| `projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021` | 151,516 文件、99.537 TB | 已完成盘点的大容量隐藏目录；没有据此认定重复或可删除。 |

阶段大文件分析索引了 221,657 个 ≥1 GiB 文件，在有明确选择上限的候选中发现 138 组内容抽样匹配，其中 94 组跨两个范围；抽样读取错误及属性变化为 0。这不是全量重复组数，不能与上述专项候选容量累加。

### 硬链接影响

- project：39,999 条普通文件路径的 nlink>1，按路径累计逻辑大小 0.487 TB。
- projset：39,091,919 条普通文件路径的 nlink>1，按路径累计逻辑大小 165.658 TB。

这些不是独立副本数或可释放容量。当前没有保存所有小文件的 inode 明细，不能从现有结果精确恢复全范围去重物理占用。

## 8. 后续工作与证据位置

1. 第 5 层补充扫描单独运行，完成后新增第 5 层明细与 CSV；本轮第 1—4 层完整结果和最终重复候选已生成。
2. 对大容量对象确认使用方、保留副本、硬链接和任务引用；本报告不构成删除清单。
3. 对巨型缓存目录评估分层目录、缓存生命周期，以及批量访问场景中的文件打包；扫描器需支持目录内分片、增量回执和可恢复进度。

原始结果目录：`/tmp/hcr-space-inventory/distributed-20261026T170411Z`。主要证据：

- [directory-status.json](/tmp/hcr-space-inventory/distributed-20261026T170411Z/directory-status.json)
- [distribution.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/distribution.md)
- [directories-level1-3.csv](/tmp/hcr-space-inventory/distributed-20261026T170411Z/directories-level1-3.csv)
- [tail-names-summary.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/tail-names-summary.md)
- [tail-names-complete.json](/tmp/hcr-space-inventory/distributed-20261026T170411Z/tail-names-complete.json)
- [worker-8-observations.jsonl](/tmp/hcr-space-inventory/distributed-20261026T170411Z/worker-8-observations.jsonl)
- [robomind-comparison.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/robomind-comparison.md)
- [large-groups-full-hash.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/large-groups-full-hash.md)
- [checkpoint-full-hash.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/checkpoint-full-hash.md)
- [duplicates-near-complete.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/duplicates-near-complete.md)
- [hardlink-notes.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/hardlink-notes.md)
- [filesystem-capacity-notes.md](/tmp/hcr-space-inventory/distributed-20261026T170411Z/filesystem-capacity-notes.md)

- [恢复扫描实时状态](/tmp/hcr-space-inventory/distributed-20261026T170411Z/recovery-progress.json)
- [缓存批次完成回执](/tmp/hcr-space-inventory/distributed-20261026T170411Z/worker-8-replacement.json)

- [恢复扫描实时状态](/tmp/hcr-space-inventory/distributed-20261026T170411Z/recovery-progress.json)
- [缓存批次完成回执](/tmp/hcr-space-inventory/distributed-20261026T170411Z/worker-8-replacement.json)

**可直接使用的完整明细：**

- [完整第 1—4 层 CSV](/tmp/hcr-space-inventory/distributed-20261026T170411Z/directories-level1-4.csv)：1,739,909 行，已完成本轮全量扫描。
- [≥1 GiB 大文件深层分布](large-files.md)：报告展示至第 5 层，完整 CSV 最深 17 层；只统计大文件。
- [大文件全部深度 CSV](/tmp/hcr-space-inventory/distributed-20261026T170411Z/large-file-directories-all-depths.csv)：129,238 行目录汇总，路径累计，未全量去重。
