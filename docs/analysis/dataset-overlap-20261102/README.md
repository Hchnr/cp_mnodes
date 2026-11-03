# project / projset：按数据集名称归并的清理候选

本轮按你的口径分析：**名字相同或能归到同一个数据集，就列为清理候选，不要求 hash 一致。没有执行任何删除。**

路径状态核对时间：2026-11-02 11:19（北京时间）。下文容量统一为 **2026-10-27 历史逻辑 TB（10^12 字节）**，不是今天重新盘点的容量，更不是保证可释放的物理空间。

追加分类：[已解压数据保留、源压缩包清理候选](ARCHIVES.md)。该独立分析的压缩包容量来自 **11 月 2 日当前文件 stat**，不要套用本页历史容量时间，也不要与本页同名副本体量直接相加。

## 结论与优先顺序

发现 133 个同名候选组，其中 39 个跨 project/projset，其他为单侧内部多份；另外归纳 40 个近似命名的数据集家族。两种分类有重叠，不能相加当作独立重复组。27 个空目录和 8 个已消失路径已从有内容的候选中剔除。

建议先看下面这批。这里的“候选多余一份”是**假设按数据集名字只保留一份，把另一份列为取舍对象**，不是已证明较小目录包含在较大目录中；保留方向暂按历史容量和目录用途建议，并未实施。

`P` = `/share-mixed/project`；`S` = `/share-mixed/projset`。

| 数据集 | 候选多余目录（历史 TB） | 候选保留目录（历史 TB） | 名称/目录层面的判断 |
|---|---|---|---|
| RoboMIND | `P/eai_pwm/datasets/RoboMIND`，11.838 | `S/eai_pwm/datasets/RoboMIND`，11.838 | 同名；优先归并 |
| OpenNeoData_20260915 | `P/eai_pwm/datasets/OpenNeoData_20260915`，9.065 | `S/eai_pwm/datasets/tactile/OpenNeoData_20260915`，9.070 | 同名，一级目录也同为 umi、aloha、arx5、flexiv、ur 等 |
| RoboMIND2.0-Franka-Part-1～5 | `P/eai_pwm/datasets/RoboMIND2.0-Franka-Part-{1..5}`，合计 3.692 | `S/eai_pwm/datasets/` 下对应五个分片，合计约 50.871 | 逐个同名分片配对；不能把五个 Part 当成彼此重复 |
| Ego-Exo4D / ego_exo4d | `S/eai_pwm/datasets/Ego-Exo4D`，10.715 | `P/yhzhang/data/ego_exo4d`，25.880 | 同数据集命名；S 侧见 annotations、takes，P 侧还含 captures 和元数据，疑似覆盖范围不同 |
| Paxini-Omnisharing | `P/eai_pwm/datasets/Paxini-Omnisharing`，8.656 | `S/eai_pwm/paxini_cleaning/source/Paxini-Omnisharing`，9.709 | 同名；两边都有大量 zhiyuan_DF2_*、RQ*、清单和同名 HDF5 条目；需选定源版本 |
| OpenAoE-2000h | `P/yhzhang/data/OpenAoE-2000h-raw`，7.754 | `S/eai_pwm/datasets/OpenAoE-2000h`，11.842 | 数据集名吻合，带 raw 后缀；按你的一数据集留一份策略列候选，格式/覆盖范围未确认 |

前四行共 8 个目录配对，“较小一份”历史逻辑大小合计 **35.311 TB**；加入 Paxini 和 OpenAoE 两行是 **51.721 TB**。它们是候选目录体量，不是已确认重复字节数、当前大小或物理释放保证。

五个 Franka 分片的 project 侧明细：Part-1 1.067 TB、Part-2 0.733 TB、Part-3 0.688 TB、Part-4 0.639 TB、Part-5 0.565 TB。

完整绝对路径见 [优先候选 CSV](priority-candidates.csv)。它是审阅清单，不是可直接执行的删除授权。

## 更宽松的“同数据集只留所需版本”候选

这些目录按数据集名字看确实是一家，值得进一步选择保留版本；但有原始数据、姿态、动作加工、转码视频、latent 的区别，不能把所有体量直接叫作相同副本。这里不要求再算 hash，也不因此排除它们，重点是你是否还需要这些派生版本。

- **OmniVitac，按新增需求优先审阅源压缩包**：`S/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021` 历史 99.537 TB；同级 `OmniVitac_platform_208546131716604003` 历史 38.616 TB、`OmniVitac` 历史 1.752 TB。若后续使用解压数据，应保留解压目录，而不是因隐藏目录名将其当缓存删掉。新增核对发现五个历史解压交付 PASS 分类的源分卷目前约 **35.840 TB**；Peeling 未完整解压，不计入，详见 [压缩包专项](ARCHIVES.md)。
- **EgoStandard**：`S/eai_pwm/datasets/EgoStandard` 49.261 TB；`S/eai_pwm/pipeline_store/sources/egostandard` 22.720 TB；`P/yhzhang/hand_pose_1023/egostandard` 28.820 TB；`P/eai_pwm/pipeline_store/materialized/egostandard_v2` 9.116 TB、`egostandard_lw2030_v2` 2.788 TB。按数据集归并看有多版，但姿态和转码不是原始数据的等价副本。
- **EgoLive / EgoPro**：对应原始目录在 `S/eai_pwm/datasets/`，分别 35.493 / 12.291 TB；`P/yhzhang/hand_pose_1023/` 下的同名姿态目录为 12.805 / 4.128 TB，materialized 下也有同名加工版。
- **Lightwheel**：`S/eai_pwm/datasets/Lightwheel-Purchased-0901_1` 52.560 TB；`P/yhzhang/hand_pose_1023/lightwheel` 17.639 TB；`P/eai_pwm/pipeline_store/materialized/lightwheel_v2` 23.350 TB、`lightwheel_lw2030_v2` 2.789 TB，另有 latent。
- **Egocentric-10K**：`S/eai_pwm/datasets/Egocentric-10K-unpacked` 17.963 TB；`P/eai_pwm/pipeline_store/materialized/egocentric-10k_v2` 20.602 TB、`egocentric-10k_lw2030_v2` 1.985 TB，另有 latent。
- **AgiBotWorld**：`P/eai_pwm/datasets/AgiBotWorld-Beta` 48.104 TB；`S/eai_pwm/datasets/action_processed_r1/AgiBotWorld-Beta` 11.001 TB；还有 2026 版、r2/r2_new 和 materialized。Beta 与 2026 不能因为同一品牌就直接合并成一份。
- **GPIC**：`P/eai_pwm/datasets/stanford-vision-lab/gpic` 12.952 TB，与 `P/eai_pwm/prepared_datasets/gpic_train_v1_94_incremental` 12.784 TB 是同数据集的不同组织形式，可按是否还需要训练打包版来取舍。
- **OakInk 2（project 内部）**：`P/eai_pwm/datasets/OakInk-v2` 2.132 TB，对比 `P/gz/data/tac_data/oakink2` 4.273 TB；属于拼写不同的同数据集候选。
- **TACO / DexYCB / HO3D（project 内部）**：`P/eai_pwm/datasets/` 和 `P/gz/data/tac_data/` 下有同名或近似名的多份，完整路径和历史容量见 CSV。

## 不应误计的同名目录

目前 project 侧的 Nymeria、EgoLive、Ego-Exo4D、Egocentric-10K-unpacked、OpenAoE-2000h，以及多种 RoboMIND2.0 机器人型号目录是空占位，不能按 projset 对应目录的大小推算重复量。Franka Part-1～5 则不是空目录，已单独列出。

`action_processed_r1`、`action_processed_r2`、`tactile` 是集合/处理阶段，不是一个具体数据集。两侧父目录名字一样，不代表可以删除整个父目录。通用子目录名如 data、raw、takes、annotations、hand_pose、consistent，以及日期/序列编号也不会独立算成重复数据集。

## 方法、覆盖范围与文件

只读取历史 CSV、当前路径属性和少量目录项；不读数据文件正文、不算 hash、不删除、不启动或重启任何搬运/清理任务。

- 复用全盘盘点的 1,739,909 行一级至四级目录汇总。
- 辅助使用历史 ≥1 GiB 大文件目录索引中深度不超过 7 的已知数据集名称；这部分大小只覆盖大文件，明确标记为下限，不能当作全目录容量。
- 补充查看数据集、下载、动作加工、pipeline sources/materialized/latents 等容器的当前直接子目录；每个容器最多读取 4,096 项，本次没有触及上限或出现读取错误。
- 最终核对 620 个候选路径，585 个仍是非空目录、27 个空、8 个消失；仅验证非空，不重新盘点子树当前容量。
- 同名匹配忽略大小写和连接符，但保留 Part、版本数字、mobile 等标识；宽松匹配另按显式数据集名称规则归家族。未做全树任意深度命名穷举，不能据此宣称找到了所有冗余。
- 硬链接、存储压缩、共享块和在线更新都会影响实际可释放空间。父子路径和不同匹配分类存在包含/重复关系，所有候选容量不能简单求和。

文件：

- [优先候选 CSV](priority-candidates.csv)：先讨论的 10 个配对，含候选保留位置。
- [完整候选 CSV](candidates.csv)：同名和同家族两种分类；`same_name` / `same_family` 有重叠。
- [结构化结果及空目录记录](analysis.json)：包含所有候选、路径状态、历史大小来源及扫描覆盖。
- [可复现的只读分析脚本](../../../ops/analysis/dataset_overlap.py)：在仓库根目录运行 `python3 -S -B ops/analysis/dataset_overlap.py`，只更新本目录报告数据，不删除源目录。
- [压缩包与解压数据专项](ARCHIVES.md)、[当前压缩包文件清单](archive-files.csv)、[专项结构化证据](archive-analysis.json)：追加的分析分类，不是删除执行清单。

`size_basis=full_20261027` 表示历史全目录汇总；`large_files_only_20261027` 表示只含 ≥1 GiB 文件的历史下限；`unknown` 表示没有可用的历史容量，**不是 0 字节**。
