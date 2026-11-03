# 2026-10-27 空间分布静态归档

- [总体报告](overview.md)：全量历史盘点、容量口径及完整性边界。
- [目录汇总表](directory-summary.md)：空间分布的紧凑目录视图。
- [≥1 GiB大文件目录分布](large-files.md)：按目录深度汇总，完整导出最深17层。
- [归档摘要清单](MANIFEST.json)：原文件名、字节数及SHA-256；gzip无时间戳、无原文件名字段，内容已读回校验。

| 文件 | 内容 |
|---|---|
| [directories-depth4.csv.gz](exports/directories-depth4.csv.gz) | 历史全量目录汇总，原 `docs/sheets/file_all_depth4.csv`。 |
| [large-file-directories-depth17.csv.gz](exports/large-file-directories-depth17.csv.gz) | ≥1 GiB文件的目录聚合，原 `docs/sheets/file_1gb_depth17.csv`。 |
| [directories-depth4-over-1tb.csv](exports/directories-depth4-over-1tb.csv) | 大容量目录筛选，原 `file_all_depth4_filter_1tb.csv`。 |
| [directories-depth4-over-1tb-readable.csv](exports/directories-depth4-over-1tb-readable.csv) | 可读筛选表，供只读计量器准备细分候选。 |
| [lightwheel-right-eye-paths-20261027.md.gz](exports/lightwheel-right-eye-paths-20261027.md.gz) | 历史225,249条右目视频路径清单。只归档原文本，**不代表当前删除授权，不能直接执行其中的清理指令**。 |

三个大型原文件合计约287MB，以gzip归档后约29MB；解压内容的SHA-256与原始文件完全一致。可用 `gzip -dc <文件>` 读取；分析脚本流式读取CSV，无需生成解压副本。

本目录为历史快照。父子目录行重叠、硬链接按路径累计、逻辑容量与物理占用不同，均不能直接换算成可释放空间或当前复制百分比。
