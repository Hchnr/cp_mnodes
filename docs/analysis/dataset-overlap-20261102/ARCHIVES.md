# 追加分类：保留已解压数据，清理源压缩包

结论：**OmniVitac 五个分类的源压缩包，是这一类最明确的审阅候选，现存 683 个分卷、逻辑大小合计 35.840 TB。** 已找到历史解压交付 PASS 记录，对应解压目录目前仍非空。但本轮尚未确认哪些训练/处理任务仍会读原压缩包，因此不写成“已确认正在使用解压版、可以直接删除”。没有执行删除、解压或新 hash。

本专项压缩包大小来自 **2026-11-02 当前 stat**，不同于主报告沿用的 10 月 27 日目录容量。TB=10^12 字节；逻辑大小不是物理释放保证。

## 1. 新增判断口径

这是与“两个位置的同名副本”并列的一类：同一数据集的分发压缩包与解压后数据并存，在后续只保留解压后数据的策略下，压缩包可以作为冗余交付物候选。

候选分开记录三个条件：

1. **源包确实存在**：识别 zip、tar.gz 和分卷后缀，读取名称、文件大小、硬链接数；不能仅看目录里有 compressed/zip 字样。
2. **对应解压输出存在**：按数据集、分类或任务名称对应；有现成完成记录则复用，不再重新做全量内容核验。本轮只确认当前目录非空，不把它当作现时全量完整性证明。
3. **原包不再是读取入口**：训练配置、数据读取方式或使用方应明确指向解压数据。本轮没有完成全节点使用引用审计，所有候选的当前使用状态仍为待确认。

名字和轻量元数据足以筛选，不要求重新算 hash。正式决定保留哪些版本时，不得把条件 2 自动当作条件 3。

## 2. OmniVitac：五类压缩包约 35.84 TB

压缩包父目录：

```text
/share-mixed/projset/eai_pwm/datasets/tactile/OmniVitac_platform_208546131716604003/OmniVitac_20260417
```

应保留的对应解压父目录：

```text
/share-mixed/projset/eai_pwm/datasets/tactile/.OmniVitac_extract_20261021
```

| 分类 | 压缩包子目录 | 当前分卷数 | 当前源包逻辑 TB | 历史解压/交付核验 | 当前对应解压目录 |
|---|---|---:|---:|---|---|
| Grasping | `Grasping_zip_new` | 300 | 15.750867 | PASS / PASS | 非空 |
| Assembly | `Assembly_zip_new` | 134 | 7.014920 | PASS / PASS | 非空 |
| Adjustment | `Adjustment_zip_new` | 130 | 6.827514 | PASS / PASS | 非空 |
| Cutting | `Cutting_zip_new` | 67 | 3.523991 | PASS / PASS | 非空 |
| Wiping | `Wiping_zip_new` | 52 | 2.722955 | PASS / PASS | 非空 |
| 合计候选 | 仅以上五类 | **683** | **35.840247** | 历史五类交付完成 | 五类目录均存在且非空 |
| Peeling：排除 | `Peeling_zip_new` | 53 | 2.775776 | pending / pending_extraction | stage 中对应分类不存在 |

五类源包当前大小逐类与历史解压记录中的 `compressed_total` 一致；这是容量核对，不是本轮重新做内容 hash。压缩包硬链接数 >1 的文件为 0，但仍不能据此承诺文件系统立即净释放相同字节。

**Peeling 不能跟着整棵压缩包父目录一起算可清理。** 历史记录缺少 `Peeling.tar.gz.03`，完整分类未完成解压。另一个 `.../tactile/OmniVitac/Peeling` 虽有部分目录，也不能当作完整解压库。

现成证据在：

```text
/share-mixed/projset/eai_pwm/operations_logs/2026-10-21/omnivitac-unpack/status.json
/share-mixed/projset/eai_pwm/operations_logs/2026-10-21/omnivitac-unpack/REQUIRED_CATEGORIES_VERIFIED_COMPLETE.json
/share-mixed/projset/eai_pwm/operations_logs/2026-10-21/omnivitac-unpack/output_verification_summary.json
/share-mixed/projset/eai_pwm/operations_logs/2026-10-21/omnivitac-unpack/source_gate.json
```

这些是 10 月 22–24 日的历史解压/输出核验记录，不是当前进程运行状态；本轮没有执行其中的命令，也没有重新计算已保存的摘要。

历史记录同时保留 `cleanup_allowed_only_after_all_six_verified=true`，核验摘要的 `all_six_output_closures_pass=false`、`canonical_promoted=false`。因此本次只是把五类分别列为源包清理候选，**没有改变旧作业的保留约束，也不代表整套 archive root 已获准清理**。未来若选择五类源包，需单独确认这一保留范围及使用入口。

## 3. 其他发现：不能一律按压缩包删除

- **RoboMIND：两侧观察到 2,748 个分卷，合计约 23.538 TB。** 位于 project/projset 各自的 `eai_pwm/datasets/RoboMIND/benchmark*_compressed/h5_*`，文件确实是 `*.tar.gz.part-aa` 等分卷。project 下存在 `pipeline_store/sources/robomind1` 和对应任务名，但这可能是加工视频/抽取结果，不能当作完整原始 HDF5 解压库；旧盘点中的 `/share-mixed/projset/eai_pwm/unpacked/robomind1` 目前不存在。因此暂不加入“已完整解压、源包可取舍”的较强候选集合。此处容量还与主报告 RoboMIND 同名副本候选重叠，不可再次累加。
- **Assembly101：2 个 zip，约 71.97 GB。** 主要是 `AssemblyPoses.zip`，并列存在 `poses@60fps` 目录；可作为小一档的对应候选，但尚未确认解压覆盖和读取入口。
- **EPIC-KITCHENS-100：1 个 zip，约 10.37 GB。** 目录中另有 `data/`，对应关系、完整程度和使用入口待确认。
- **IKEA-ASM：6 个归档文件，约 425.58 GB。** 包括 RGB/depth zip 与 `all_depth_frames.tar.gz`。当前 `data/` 内未看到对应的图像/深度解压子目录；annotations、trained_models 不能替代这些原始数据，不按这一类直接建议清理。
- **GPIC：tar 不等于废弃下载包。** `.../datasets/stanford-vision-lab/gpic/train/gpic_train_*.tar` 可能就是直接读取的训练分片；本轮只抽看 32 个文件，不统计其全量压缩包空间。已有 prepared_datasets/GPIC 打包或转换结果，也不证明这组 tar 已经完整解压、不再需要。必须与真正的交付压缩包分开。

## 4. 覆盖与产物

本轮只读查看 59 个限定目录，观察到 3,525 个归档文件，逻辑大小约 62.713 TB。**62.713 TB 是观察到的归档体量，不是可删空间。** GPIC 限 32 项，两个 OpenAoE 目录各限 4,096 项，均明确标记扫描未完整；不是两棵共享目录的全量压缩包盘点。其他已列目录的枚举没有读取错误。

没有递归检查解压后数亿文件，也没有读归档正文、启动解压、算新 hash、删除文件或调整运行任务。

- [archive-analysis.json](archive-analysis.json)：逐分类状态、目录覆盖范围、当前归档容量、历史保留限制及对应路径状态。
- [archive-files.csv](archive-files.csv)：观察到的归档绝对路径、当前大小、分卷信息和硬链接数；不是删除执行清单。
- [dataset_archives.py](../../../ops/analysis/dataset_archives.py)：只读分析脚本；仅将结果写入本报告目录。
- [test_dataset_archives.py](../../../tests/audit/test_dataset_archives.py)：6 项私有临时目录测试通过，覆盖分卷识别、临时文件排除、符号链接拒绝和候选判断。

在仓库根目录运行：

```sh
python3 -S -B ops/analysis/dataset_archives.py
python3 -S -B -m unittest discover -s tests/audit -p 'test_dataset_archives.py'
```

后续确认只使用解压数据后，建议优先审阅 OmniVitac 已完成的五类源包，保留解压目录和作业凭据；Peeling 及未证明存在完整解压对应物的归档另列，不做整棵父目录级清理。
