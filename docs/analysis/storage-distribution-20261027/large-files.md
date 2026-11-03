# 混闪大文件目录分布（≥1 GiB）

基于已完成扫描：2026-10-27 14:28:13 北京时间。本报告仅重新汇总已保存清单，不访问源文件。

共 **221,657 条大文件路径、754.030 TB**；大文件所在目录最深为根目录以下 **17 层**。已核验路径唯一性，以及两侧文件数、字节数与全量扫描大文件分组完全一致。

逻辑大小和分配块均按路径累计，未按 inode 全局去重；不能当作独立副本容量或可释放空间。父子目录相互包含，不能再次相加。此表不包含 <1 GiB 文件，不能表示目录全部数据量。

project、projset 根目录均为第 0 层。下表每个范围、每个深度列出逻辑容量最大的 10 个目录（第 0 层全部列出），合并为一张表；完整 CSV 包含所有实际深度。

| 范围 | 深度 | 目录 | ≥1 GiB 文件路径数 | 大文件逻辑 TB | 路径分配 TiB |
|---|---:|---|---:|---:|---:|
| project | 0 | `/share-mixed/project` | 87,305 | 400.645 | 363.376 |
| project | 1 | `/share-mixed/project/eai_pwm` | 63,684 | 306.971 | 279.006 |
| project | 1 | `/share-mixed/project/yhzhang` | 16,670 | 51.726 | 47.045 |
| project | 1 | `/share-mixed/project/gz` | 4,647 | 24.231 | 21.219 |
| project | 1 | `/share-mixed/project/liyue` | 1,533 | 11.341 | 10.314 |
| project | 1 | `/share-mixed/project/zhiwei` | 259 | 3.352 | 3.048 |
| project | 1 | `/share-mixed/project/linzhe` | 441 | 2.335 | 2.118 |
| project | 1 | `/share-mixed/project/zy` | 24 | 0.189 | 0.171 |
| project | 1 | `/share-mixed/project/.hcr-copy-20261021T141055Z-overlap-2db42a` | 9 | 0.168 | 0.152 |
| project | 1 | `/share-mixed/project/lizhiyu` | 18 | 0.122 | 0.111 |
| project | 1 | `/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a` | 2 | 0.115 | 0.105 |
| project | 2 | `/share-mixed/project/eai_pwm/datasets` | 27,519 | 146.987 | 133.565 |
| project | 2 | `/share-mixed/project/eai_pwm/pipeline_store` | 19,181 | 59.087 | 53.740 |
| project | 2 | `/share-mixed/project/eai_pwm/action_line` | 6,497 | 52.484 | 47.734 |
| project | 2 | `/share-mixed/project/yhzhang/data` | 16,662 | 51.699 | 47.020 |
| project | 2 | `/share-mixed/project/eai_pwm/home` | 3,872 | 19.492 | 17.666 |
| project | 2 | `/share-mixed/project/gz/DexTrain` | 3,299 | 18.441 | 15.954 |
| project | 2 | `/share-mixed/project/eai_pwm/robotwin_fd_runs` | 599 | 6.590 | 5.993 |
| project | 2 | `/share-mixed/project/eai_pwm/models` | 1,595 | 6.517 | 5.927 |
| project | 2 | `/share-mixed/project/eai_pwm/repos` | 1,672 | 5.892 | 5.358 |
| project | 2 | `/share-mixed/project/liyue/training_log` | 672 | 4.920 | 4.475 |
| project | 3 | `/share-mixed/project/eai_pwm/datasets/AgiBotWorld-Beta` | 1,153 | 48.036 | 43.688 |
| project | 3 | `/share-mixed/project/eai_pwm/action_line/runs` | 4,738 | 43.913 | 39.938 |
| project | 3 | `/share-mixed/project/eai_pwm/pipeline_store/latents` | 16,097 | 42.668 | 38.807 |
| project | 3 | `/share-mixed/project/gz/DexTrain/results` | 3,203 | 18.243 | 15.827 |
| project | 3 | `/share-mixed/project/eai_pwm/datasets/download_jt` | 2,703 | 17.706 | 16.104 |
| project | 3 | `/share-mixed/project/yhzhang/data/ego_exo4d` | 4,495 | 17.634 | 16.038 |
| project | 3 | `/share-mixed/project/yhzhang/data/xperience` | 7,387 | 13.959 | 12.695 |
| project | 3 | `/share-mixed/project/eai_pwm/datasets/train` | 5,189 | 13.269 | 11.963 |
| project | 3 | `/share-mixed/project/eai_pwm/datasets/stanford-vision-lab` | 8,000 | 12.796 | 11.638 |
| project | 3 | `/share-mixed/project/eai_pwm/datasets/RoboMIND` | 1,315 | 11.798 | 10.731 |
| project | 4 | `/share-mixed/project/eai_pwm/datasets/AgiBotWorld-Beta/AgiBotWorld-Beta-hf-original` | 1,153 | 48.036 | 43.688 |
| project | 4 | `/share-mixed/project/eai_pwm/action_line/runs/cosmos3_action_fd` | 4,514 | 41.807 | 38.023 |
| project | 4 | `/share-mixed/project/eai_pwm/datasets/download_jt/HiFi-UMI-2K` | 2,388 | 16.009 | 14.560 |
| project | 4 | `/share-mixed/project/yhzhang/data/xperience/ropedia` | 7,387 | 13.959 | 12.695 |
| project | 4 | `/share-mixed/project/yhzhang/data/ego_exo4d/takes` | 3,731 | 13.127 | 11.939 |
| project | 4 | `/share-mixed/project/eai_pwm/datasets/stanford-vision-lab/gpic` | 8,000 | 12.796 | 11.638 |
| project | 4 | `/share-mixed/project/eai_pwm/datasets/train/ego4d_v1` | 5,176 | 12.486 | 11.356 |
| project | 4 | `/share-mixed/project/eai_pwm/pipeline_store/latents/lightwheel_wan2pt2_tokenizer-t480-20eb789667fa_v1` | 3,861 | 11.409 | 10.376 |
| project | 4 | `/share-mixed/project/eai_pwm/home/hcr/repos` | 1,470 | 10.191 | 9.265 |
| project | 4 | `/share-mixed/project/eai_pwm/pipeline_store/supplement_2to20/latents` | 1,430 | 9.039 | 8.221 |
| project | 5 | `/share-mixed/project/eai_pwm/datasets/AgiBotWorld-Beta/AgiBotWorld-Beta-hf-original/observations` | 1,112 | 46.121 | 41.947 |
| project | 5 | `/share-mixed/project/eai_pwm/action_line/runs/cosmos3_action_fd/action_sft` | 4,514 | 41.807 | 38.023 |
| project | 5 | `/share-mixed/project/eai_pwm/datasets/stanford-vision-lab/gpic/train` | 8,000 | 12.796 | 11.638 |
| project | 5 | `/share-mixed/project/eai_pwm/datasets/train/ego4d_v1/components` | 5,176 | 12.486 | 11.356 |
| project | 5 | `/share-mixed/project/eai_pwm/pipeline_store/latents/lightwheel_wan2pt2_tokenizer-t480-20eb789667fa_v1/out` | 3,860 | 11.407 | 10.375 |
| project | 5 | `/share-mixed/project/eai_pwm/home/hcr/repos/cosmos-framework-pwm` | 1,125 | 8.403 | 7.643 |
| project | 5 | `/share-mixed/project/yhzhang/data/xperience/ropedia/part2` | 3,675 | 6.953 | 6.324 |
| project | 5 | `/share-mixed/project/eai_pwm/robotwin_fd_runs/outputs/cosmos3_action_fd/action_sft` | 592 | 6.559 | 5.966 |
| project | 5 | `/share-mixed/project/eai_pwm/pipeline_store/latents/egostandard_wan2pt2_tokenizer-t480-20eb789667fa_v1/out` | 2,084 | 6.043 | 5.496 |
| project | 5 | `/share-mixed/project/yhzhang/data/PsiBotAI_SynData/glove-origin/tasks` | 680 | 5.662 | 5.149 |
| projset | 0 | `/share-mixed/projset` | 134,352 | 353.385 | 321.402 |
| projset | 1 | `/share-mixed/projset/eai_pwm` | 130,066 | 345.682 | 314.396 |
| projset | 1 | `/share-mixed/projset/baiyu` | 3,926 | 4.975 | 4.525 |
| projset | 1 | `/share-mixed/projset/liyue` | 360 | 2.728 | 2.481 |
| projset | 2 | `/share-mixed/projset/eai_pwm/datasets` | 117,018 | 326.130 | 296.613 |
| projset | 2 | `/share-mixed/projset/eai_pwm/ace_ego_hand_preprocess` | 10,871 | 11.742 | 10.680 |
| projset | 2 | `/share-mixed/projset/baiyu/project` | 3,926 | 4.975 | 4.525 |
| projset | 2 | `/share-mixed/projset/eai_pwm/experiments` | 524 | 3.502 | 3.185 |
| projset | 2 | `/share-mixed/projset/liyue/model_ckpts` | 360 | 2.728 | 2.481 |
| projset | 2 | `/share-mixed/projset/eai_pwm/home` | 234 | 1.832 | 1.666 |
| projset | 2 | `/share-mixed/projset/eai_pwm/unpacked` | 829 | 1.129 | 1.027 |
| projset | 2 | `/share-mixed/projset/eai_pwm/paxini_cleaning` | 570 | 1.075 | 0.978 |
| projset | 2 | `/share-mixed/projset/eai_pwm/_ops` | 20 | 0.273 | 0.249 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/tactile` | 47,895 | 118.844 | 108.088 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Nymeria` | 7,944 | 47.299 | 43.018 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Sekai` | 6,457 | 20.029 | 18.217 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/IndEgo` | 3,227 | 17.814 | 16.201 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/Ego-1K` | 9,779 | 17.204 | 15.647 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/RoboMIND` | 1,315 | 11.798 | 10.731 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/molmobot-data_20260914` | 4,691 | 10.307 | 9.374 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/action_processed_r3` | 4,995 | 7.805 | 7.099 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/action_processed_r1` | 4,160 | 7.195 | 6.544 |
| projset | 3 | `/share-mixed/projset/eai_pwm/datasets/EgoBody3M` | 2,231 | 6.067 | 5.518 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021` | 38,791 | 57.045 | 51.882 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/tactile/OmniVitac_platform_208546131716604003` | 736 | 38.616 | 35.121 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/Ego-1K/shards` | 9,779 | 17.204 | 15.647 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/tactile/OpenLoong-Community_visuo-tactile` | 7,727 | 16.047 | 14.595 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/Sekai/20260814` | 4,048 | 12.208 | 11.103 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/molmobot-data_20260914/data` | 4,691 | 10.307 | 9.374 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/IndEgo/5_Miscellaneous` | 987 | 8.651 | 7.868 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/action_processed_r3/_runs` | 4,995 | 7.805 | 7.099 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/tactile/zhen` | 640 | 7.135 | 6.489 |
| projset | 4 | `/share-mixed/projset/eai_pwm/datasets/RoboMIND/benchmark1_0_compressed` | 762 | 6.871 | 6.250 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/tactile/OmniVitac_platform_208546131716604003/OmniVitac_20260417` | 736 | 38.616 | 35.121 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021/Grasping` | 18,805 | 26.805 | 24.379 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/Ego-1K/shards/train` | 8,802 | 15.483 | 14.081 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021/Adjustment` | 8,739 | 13.788 | 12.540 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/molmobot-data_20260914/data/molmobot-data_20260914` | 4,691 | 10.307 | 9.374 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021/Assembly` | 6,147 | 8.756 | 7.964 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/IndEgo/5_Miscellaneous/vrs` | 987 | 8.651 | 7.868 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/tactile/zhen/cosmos-tactile` | 640 | 7.135 | 6.489 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021/Cutting` | 3,788 | 6.020 | 5.476 |
| projset | 5 | `/share-mixed/projset/eai_pwm/datasets/AgiBotWorld2026/data/AgiBotWorld2026_20260412` | 152 | 4.995 | 4.543 |

[完整目录 CSV（所有深度，共 129,238 行）](/tmp/hcr-space-inventory/distributed-20261026T170411Z/large-file-directories-all-depths.csv)

同一 inode 的判断应结合文件系统身份；现有清单不含 st_dev，不能仅凭跨挂载 inode 数字相等就判定硬链接。内容相同但 inode 不同的文件仍分别累计；完整校验及抽样重复证据见 overview.md。
