# 混闪空间分布与清理准备：合并表版

更新时间：2026-10-27 14:31:16（北京时间）；数据快照：2026-10-27 14:28:13 北京时间。

> 本轮完成核验：全部 123,682,056 个目录任务完成；两侧共 661,056,361 个普通文件路径、2,036.466 TB 逻辑大小，已入库读取错误为 0，completion-audit.json 全项通过。第 5 层补充扫描属于新一轮在线统计，结果不会混入本轮总量。

本版把原报告的总体进度、两个范围不同目录深度的容量与密度排行、文件大小分布及 df 对照合并为下方一张表。目录在多个排行中出现时只保留一行；目录明细覆盖原报告列出的路径及本次快照重新计算的同范围排行，并非所有目录的全量清单。

目录深度相对于 `/share-mixed/project` 或 `/share-mixed/projset`：根目录为 0，直接子目录为 1。project 与 projset 均列入同一表。按范围及路径排序，便于查看层级。

**统计口径：** 逻辑 TB=普通文件 st_size 按路径累计÷10^12；路径分配 TiB=st_blocks×512 按路径累计÷2^40。硬链接未全量去重，文件数指普通文件路径数。父子目录、根目录合计、大小分组存在重叠，不能再次相加；df 行为文件系统口径，也不能与目录行相加。

| 记录类型 | 范围 | 深度 | 路径／分组 | 状态／完成率 | 逻辑 TB | 路径分配 TiB | 普通文件路径数 | 已统计目录数 | 说明 |
|---|---|---:|---|---|---:|---:|---:|---:|---|
| 总计 | 合计 | — | `两个范围已入库合计` | 100%（本轮遍历） | 2,036.466 | 1,851.828 | 661,056,361 | 123,682,056 | 本轮完整结果；非独立物理占用 |
| 范围汇总 | project | 0 | `/share-mixed/project` | 100%（本轮遍历） | 1,028.126 | 934.216 | 433,469,269 | 42,989,431 | 已入库读取错误 0 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-copy-20261021T064717Z-resume-e25450` | 完成 | 0.014 | 0.013 | 347,775 | 14,507 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-copy-20261021T141055Z-overlap-2db42a` | 完成 | 0.360 | 0.329 | 5,715,561 | 103,548 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-copy-20261023T083418Z-c3559-4e81a5a7` | 完成 | 0.074 | 0.068 | 2,417,733 | 11,849 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a` | 完成 | 0.174 | 0.159 | 2,152,096 | 56,915 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-copy-fixed-reaudit-20261026T042442Z-a22826ff-files` | 完成 | 0.013 | 0.012 | 377 | 81 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053` | 完成 | 0.123 | 0.112 | 1,401,426 | 16,152 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/.hcr-readonly-audit-20261026-full-0107` | 完成 | 0.037 | 0.034 | 1,514 | 918 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/eai_pwm` | 完成 | 803.840 | 731.060 | 332,538,124 | 24,270,761 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/action_line/abs16` | 完成 | 1.904 | 1.735 | 9,115,454 | 227,332 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/action_line/runs` | 完成 | 43.921 | 39.946 | 46,108 | 4,446 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/AgiBotWorld-Beta` | 完成 | 48.104 | 43.750 | 6,527 | 311 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/RoboMIND` | 完成 | 11.838 | 10.767 | 1,403 | 30 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/SPAR` | 完成 | 2.261 | 2.059 | 8,438,123 | 134,552 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/_gl` | 完成 | 0.001 | 0.006 | 185 | 1,356,852 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/action_processed_r1` | 完成 | 23.907 | 21.751 | 2,212,336 | 1,216,405 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/action_processed_r2` | 完成 | 0.540 | 0.506 | 4,378,018 | 2,286,556 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/download_jt` | 完成 | 46.052 | 41.889 | 14,124,056 | 628,628 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/stanford-vision-lab` | 完成 | 12.952 | 11.780 | 8,170 | 7 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/datasets/train` | 完成 | 23.711 | 21.460 | 256,707 | 56,867 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/home/hcr` | 完成 | 12.237 | 11.120 | 2,111,707 | 314,196 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/home/yuanhai` | 完成 | 6.790 | 6.230 | 8,160,463 | 8,491,564 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/pipeline_store/latents` | 完成 | 42.715 | 38.849 | 71,116 | 566 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/pipeline_store/materialized` | 完成 | 273.660 | 248.926 | 42,229,573 | 3,315,472 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/pipeline_store/sources` | 完成 | 33.119 | 30.124 | 1,817,011 | 373,544 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/pipeline_store/vimix-14m` | 完成 | 0.156 | 0.149 | 1,209,871 | 1,195,093 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/prepared_datasets/gpic_train_v1_94_incremental` | 完成 | 12.784 | 11.651 | 100,055,905 | 111,534 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/prepared_datasets/pwm_reference_media` | 完成 | 2.187 | 1.993 | 15,845,748 | 128,031 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/eai_pwm/prepared_datasets/pwm_reference_media_v1_20260808` | 完成 | 1.034 | 0.946 | 23,605,873 | 3,379 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/gz` | 完成 | 40.481 | 35.864 | 22,580,610 | 2,368,588 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/gz/DexTrain/results` | 完成 | 19.913 | 17.221 | 144,443 | 19,369 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/gz/data/tac_data` | 完成 | 9.357 | 8.518 | 12,250,097 | 729,067 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/linzhe` | 完成 | 10.040 | 9.186 | 31,038,961 | 7,178,141 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/linzhe/roboclaw-vlabench/runs` | 完成 | 4.262 | 3.890 | 7,665,735 | 1,607,216 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/linzhe/xiaomi0-libero/runs` | 完成 | 1.975 | 1.835 | 18,143,509 | 4,715,659 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/liyue` | 完成 | 16.160 | 14.734 | 19,942,733 | 4,926,961 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/liyue/cosmos-framework/outputs` | 完成 | 9.007 | 8.227 | 18,528,455 | 4,828,730 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/lizhiyu` | 完成 | 0.133 | 0.121 | 91,936 | 536 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/yhzhang` | 完成 | 149.707 | 136.179 | 7,851,876 | 3,328,309 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/data/Eidon-Tracker-POV` | 完成 | 13.797 | 12.549 | 110,846 | 27,428 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/data/PsiBotAI_SynData` | 完成 | 11.405 | 10.373 | 1,576 | 275 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/data/ego_exo4d` | 完成 | 25.880 | 23.538 | 202,230 | 43,556 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/data/xperience` | 完成 | 18.958 | 17.243 | 57,316 | 29,513 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/hand_pose_1023/egolive` | 完成 | 12.805 | 11.647 | 582,163 | 259,097 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/hand_pose_1023/egostandard` | 完成 | 28.820 | 26.218 | 2,340,846 | 1,039,053 | 父子行存在包含关系 |
| 目录明细 | project | 3 | `/share-mixed/project/yhzhang/hand_pose_1023/lightwheel` | 完成 | 17.639 | 16.048 | 1,817,658 | 805,984 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/zhiwei` | 完成 | 6.153 | 5.597 | 738,372 | 20,613 | 父子行存在包含关系 |
| 目录明细 | project | 1 | `/share-mixed/project/zy` | 完成 | 0.788 | 0.723 | 6,580,980 | 676,126 | 父子行存在包含关系 |
| 文件大小分组 | project | — | <4 KiB | 已入库部分 | 0.067 | — | 65,836,897 | — | 根目录汇总的分类明细，不能重复相加 |
| 文件大小分组 | project | — | 4 KiB 至 <1 MiB | 已入库部分 | 41.033 | — | 313,773,433 | — | 根目录汇总的分类明细，不能重复相加 |
| 文件大小分组 | project | — | 1 MiB 至 <1 GiB | 已入库部分 | 586.381 | — | 53,771,634 | — | 根目录汇总的分类明细，不能重复相加 |
| 文件大小分组 | project | — | ≥1 GiB | 已入库部分 | 400.645 | — | 87,305 | — | 根目录汇总的分类明细，不能重复相加 |
| 范围汇总 | projset | 0 | `/share-mixed/projset` | 100%（本轮遍历） | 1,008.340 | 917.612 | 227,587,092 | 80,692,625 | 已入库读取错误 0 |
| 目录明细 | projset | 1 | `/share-mixed/projset/RoboMIND2.0-Franka-Part-2` | 完成 | 0.000 | 0.000 | 1 | 2 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/baiyu` | 完成 | 12.109 | 11.014 | 996,176 | 102,627 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/baiyu/project/cosmos-framework-private` | 完成 | 11.423 | 10.389 | 39,013 | 1,769 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/eai_pwm` | 完成 | 992.721 | 903.405 | 226,414,014 | 80,588,848 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Ego-1K` | 完成 | 17.459 | 15.879 | 11,176 | 962 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/EgoLive` | 完成 | 35.493 | 32.280 | 199,655 | 2,831 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/EgoPro` | 完成 | 12.291 | 11.179 | 31,470 | 2,235 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/EgoStandard` | 完成 | 49.261 | 44.803 | 333,706 | 6,186 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked` | 完成 | 17.963 | 16.337 | 403,924 | 21,733 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/IndEgo` | 完成 | 19.061 | 17.336 | 22,889 | 1,592 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Lightwheel-Purchased-0901_1` | 完成 | 52.560 | 47.808 | 1,351,495 | 678,835 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Nymeria` | 完成 | 54.100 | 49.204 | 58,586 | 25,056 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/OpenAoE-2000h` | 完成 | 11.842 | 10.771 | 212,759 | 168,104 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/RoboMIND` | 完成 | 11.838 | 10.767 | 1,403 | 30 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/RoboMIND2.0-Agilex` | 完成 | 13.280 | 12.079 | 22,274 | 44,547 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/RoboMIND2.0-Franka-Part-3` | 完成 | 13.900 | 12.642 | 38,792 | 77,579 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/RoboMIND2.0-UR5` | 完成 | 15.584 | 14.174 | 54,592 | 109,185 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Sekai` | 完成 | 20.509 | 18.653 | 24,936 | 6,547 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/action_processed_r1` | 完成 | 198.642 | 180.701 | 49,297,657 | 4,502,057 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/action_processed_r2` | 完成 | 11.406 | 10.846 | 164,391,941 | 71,323,110 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/action_processed_r3` | 完成 | 20.157 | 18.333 | 318,173 | 11,398 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/datasets/tactile` | 完成 | 179.479 | 163.254 | 6,706,897 | 2,885,705 | 父子行存在包含关系 |
| 目录明细 | projset | 4 | `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021` | 完成 | 99.537 | 90.529 | 151,516 | 20,156 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/pipeline_store/sources` | 完成 | 22.720 | 20.664 | 327,494 | 6,092 | 父子行存在包含关系 |
| 目录明细 | projset | 3 | `/share-mixed/projset/eai_pwm/unpacked/robomind1` | 完成 | 15.034 | 13.674 | 105,260 | 212,479 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/hcr` | 完成 | 0.000 | 0.000 | 1 | 1 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/ks3_cleanup_20261009` | 完成 | 0.000 | 0.000 | 26 | 3 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/liyue` | 完成 | 2.729 | 2.482 | 9,026 | 292 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/materialized` | 完成 | 0.781 | 0.710 | 167,822 | 846 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/trex_audit_20261009` | 完成 | 0.000 | 0.000 | 3 | 2 | 父子行存在包含关系 |
| 目录明细 | projset | 1 | `/share-mixed/projset/trex_ks3_transfer_20261009` | 完成 | 0.000 | 0.000 | 22 | 3 | 父子行存在包含关系 |
| 文件大小分组 | projset | — | <4 KiB | 已入库部分 | 0.142 | — | 60,148,954 | — | 根目录汇总的分类明细，不能重复相加 |
| 文件大小分组 | projset | — | 4 KiB 至 <1 MiB | 已入库部分 | 15.172 | — | 150,205,513 | — | 根目录汇总的分类明细，不能重复相加 |
| 文件大小分组 | projset | — | 1 MiB 至 <1 GiB | 已入库部分 | 639.641 | — | 17,098,273 | — | 根目录汇总的分类明细，不能重复相加 |
| 文件大小分组 | projset | — | ≥1 GiB | 已入库部分 | 353.385 | — | 134,352 | — | 根目录汇总的分类明细，不能重复相加 |
| df 容量对照 | project | — | `/share-mixed/project` | 文件系统采集值 | — | — | — | — | 总容量 1,125.281 TB；已用 1,050.063 TB；可用 75.219 TB。不能用于计算扫描完成率 |
| df 容量对照 | projset | — | `/share-mixed/projset` | 文件系统采集值 | — | — | — | — | 总容量 1,425.900 TB；已用 1,227.065 TB；可用 198.835 TB。不能用于计算扫描完成率 |

**巨型 frame_cache：**

```text
/share-mixed/projset/eai_pwm/datasets/action_processed_r2/_work/molmobot_stage1_multiview_20261009_v1/shared_visual_cache/frame_cache
```

独立名称清点得到 **29,693,722 个直接子项（不含 `.`、`..`，包含隐藏项）**；它们可以是文件、子目录或符号链接，不是已确认的普通文件总数。所在 64 目录批次约于今天 13:52 完成回收，较约 05:52 的首次读取观察耗时约 **8 小时**。整批新增 **34,820,834 个普通文件、2.856 TB**，并发布 **703,570 个子目录**继续扫描；这些批次数字不能全部归到 frame_cache。

当前状态：本轮全范围队列已闭合，完整性核验通过；第 5 层补充扫描单独记账。原扫描的单目录没有跨节点分片，整批回传前缺少内部计数，导致此前无法给出目录内完成率。

**重复数据和清理核查：** RoboMIND 两侧各 11.838 TB、1,403 个文件，路径与大小完全一致，15 个小文件完整 SHA-256 及 1,388 个大文件内容抽样一致；大文件尚未全量内容校验。IKEA-ASM 两对文件完整 SHA-256 一致，单侧合计 304.192 GB。Cosmos3 三份分片各 20.169 GB，完整 SHA-256 一致；只证明所列分片一致。上述容量不相加作为可释放量，使用方、引用关系和保留副本仍需确认。

更完整的专项证据与限制见 [原报告](overview.md)。机器可读当前分布源：`/tmp/hcr-space-inventory/distributed-20261026T170411Z/directory-status.json`。本次没有删除数据。

**可直接使用的完整明细：**

- [完整第 1—4 层 CSV](/tmp/hcr-space-inventory/distributed-20261026T170411Z/directories-level1-4.csv)：1,739,909 行，已完成本轮全量扫描。
- [≥1 GiB 大文件深层分布](large-files.md)：报告展示至第 5 层，完整 CSV 最深 17 层；只统计大文件。
- [大文件全部深度 CSV](/tmp/hcr-space-inventory/distributed-20261026T170411Z/large-file-directories-all-depths.csv)：129,238 行目录汇总，路径累计，未全量去重。
