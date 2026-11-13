# projset 复制：跳过与异常路径清单

## 2026-11-12：历史错误与冲突复核

本轮结束后，使用新集群的 16 个 `extra-*` 节点处理历史异常。历史错误计数保留为 996 条；原待关注的 12 条中断扫描重新执行并验证了发现的 46 个子项，当前 [错误复核报告](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/reconciliation/REPORT.md) 的待关注数为 0。其余历史记录依据各自的重试、校验或重扫证据分类，不能把重扫目录本身视为对全部后代的独立内容校验。重试结果按原节点、任务号、相对路径、回执文件及偏移关联，原始历史回执和累计计数不变。

对 2012 条历史冲突重新检查后，有 1189 条普通文件 SHA-256 不同、734 条文件大小不同、87 条链接文本不同、1 条源/目标类型不同。另 1 条实际为目录 `Stale file handle`，不是据此确认的内容冲突；后续重试扫描 7 个目录和 14 个子项，14 项均核对相同。1851 条位于 `eai_pwm/datasets/PH2D/.cache/` 且以 `.metadata` 结尾；是否可丢弃差异需要数据负责人确认。87 条链接中，35 条指向两侧对应的相对路径，52 条指向不同相对路径；这不证明目录内容一致。

按照保留目标版本的策略，1924 个源文件和 87 个源链接已以原相对路径另存到独立目录，文件校验、链接文本复核由与归档时不同的 16 个节点完成，2011 / 2011 通过。两侧均为目录的 1 条没有递归归档。源版本保存量为 0.000531478356 TB，其中链接只计链接文本长度，不包含指向的数据。保存源版本并不决定原目标路径应选择哪一份；隔离目录中的相对链接可能无法解析。

- [处理报告和逐项 CSV](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/attention-20261112T084400Z-ba8209ff/ATTENTION-REPORT.md)
- [冲突源版本隔离目录](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/attention-archive-20261112T085207Z-5ddee33a/source-versions)
- [错误与冲突复核看板](https://swanlab.cn/@hchnr/data-migration/runs/oxqmje1k) · [源版本归档看板](https://swanlab.cn/@hchnr/data-migration/runs/ic8ks30g) · [独立校验看板](https://swanlab.cn/@hchnr/data-migration/runs/jr64ni66) · [NFS 目录重试看板](https://swanlab.cn/@hchnr/data-migration/runs/simdxvwd)

用户排除的 `eai_pwm/datasets/Sekai` 没有纳入本轮。开始时源路径已不存在，目标侧同名目录仍存在；本轮没有对目标侧执行删除。所有节点使用各自的持久回执、重试退避和续跑；上报间隔不短于 120 秒，错误和源变化不会被当作成功放行。

[实时更新报告](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/REPORT.md) 按固定间隔增量导出。报告时间和 `summary.json` 的 `updated_beijing` 是新鲜度依据；原始逐文件回执仍是事实来源。

[历史错误的处理进度](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/reconciliation/REPORT.md) 将原始错误与后续扫描、补拷及校验回执关联；[待关注路径](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/reconciliation/pending-errors.csv) 保留尚未完成、缺少证据或证据有冲突的记录。[全部关联结果](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/reconciliation/reconciled-errors.csv) 提供逐条证据来源。

[历史源变化的后续处理进度](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/changed-reconciliation/REPORT.md) 关联限定路径的重试、独立校验与当前属性；[待关注记录](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/changed-reconciliation/pending-changed.csv) 保留证据缺失、时间顺序不明或当前身份变化的结果。2026-11-10 的 10 条定向重试范围和回执另见控制目录 `recovery/changed-retry-20261110T1837-f59ab8/REPORT.md`。`changed-paths.csv` 和看板上的 changed 数字保留历史记录，不会因后续重试自动减少。

| 清单 | 范围 |
|---|---|
| [skipped-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/skipped-paths.csv) | 目标已有、内容核对相同的成功复用路径。无需因为 skipped 状态再次搬运。 |
| [conflict-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/conflict-paths.csv) | 同名目标的内容、类型或符号链接文本不同，按授权保留目标。 |
| [changed-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/changed-paths.csv) | 源文件/目录在扫描、读取或发布前发生变化。 |
| [error-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/error-paths.csv) | 读写、权限、扫描、NFS 等错误结果。 |
| [attention-paths.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/attention-paths.csv) | 所有需要复核的结果，含可能与成功结果重叠的元数据警告。 |
| [directory-summary.csv](../../runs/projset-copy-outcomes-20261102T144640Z-4316ae/directory-summary.csv) | 按类别和相对路径前3层分组的容量与记录数，便于先选大目录。 |

CSV提供源和目标绝对路径、类型、记录时源容量（精确字节及TB）、节点、原因、任务号、回执文件和字节偏移。异常行还包含当前两侧的类型、容量、符号链接目标及属性读取错误。当前属性只是只读 stat/readlink，没有重新读内容或做哈希，因此不能仅凭两侧大小相等断言内容相等。

符号链接容量仅为链接文本大小；被链接的大型数据目录容量不包含在内。目录/扫描错误可能没有可用的数据量，这种情况记为未知，不假装是零字节文件。

2026-11-10 18:07 的冲突只读复核及快照位于控制目录 `recovery/conflict-analysis-20261110T1803/REPORT.md`，包含按源大小排序的文件冲突和链接解析结果。2012 条历史记录中，1188 条当时校验和不同，735 条当时大小不同，87 条链接文本不同，1 条文件类型不同；另 1 条是 NFS 失效句柄被记为目录 conflict，不能据此认定内容冲突。历史原因不能自动决定哪一侧是正确版本。

87 条链接检查时均能在限定的源和目标根内解析：35 条最终落在两侧对应的相对路径，52 条最终相对路径不同。18 对普通文件端点当前 SHA-256 相同；69 对目录端点没有递归比较。检查未主动写入、覆盖或删除目标内容，也未主动修改链接；只读访问可能由文件系统更新访问时间。源码 `ops/diagnostics/projset_conflict_links.py` 限制只读解析范围，遇到范围外链接、变化或无法读取会保留为未确认。

18:16–18:17 对冲突快照中当前大小相同的 1189 对普通文件，限速只读两轮 SHA-256，检查时均不同；独立节点抽样复核 10 / 10 一致。逐项源/目标哈希和修改时间见控制目录 `recovery/conflict-analysis-20261110T1803/file-hash-current.csv`。不能以文件新旧或哈希差异直接判断正确版本。目标 NFS 隔离测试中同秒同长度改写未改变可见的 inode、大小及时间戳，因此两轮哈希和身份检查也不构成在线源或目标的原子快照。

## 是否会自动重试、覆盖或更新

- 原始 `nodes/<node>/receipts.jsonl` 和 `issues.jsonl` 持续追加。
- 每轮导出结束后等待120秒再开始下一轮，因此实际完成间隔包含导出耗时。扫描的是已落盘回执前缀，导出过程中任务继续运行，报告不是原子快照。
- 当前运行中的复制 worker 不自动重试失效句柄；恢复启动代码已准备有限重试，将在下次自然恢复后生效。已记账的冲突、源变化和错误不会自动重新入队，也不会自动覆盖不同目标。
- 报告刷新与重新复制是两件事。新增导出器不修改源/目标，不触发复制、覆盖、删除或元数据变更。

复核结果中的“已校验”指后续回执和校验时的状态；“已重新扫描”只确认该目录扫描完成，子项仍以各自回执为准。没有足够证据的历史错误保留在待关注清单，源目录持续变化时不能将历史校验视为全局快照。`reconciliation-health.json` 显示最近一次复核刷新是否成功。

2026-11-11 对剩余 344 条 ESTALE 记录先在 CPU worker 6、7 重新打开路径，均恢复可读，目标仍缺失。限定这 251 个文件和 93 个目录清点得到 596 个普通文件、0.000015991719 TB；通知后在 CPU worker 7 限速补拷，596 个全部成功。此次范围、回执与 CPU worker 8 的独立双轮 SHA-256 及目录清单复核位于控制目录 `recovery/stale-retry-20261111T0310-ed9045/`。独立复核以 `independent-verification.json` 的最终状态为准，不以补拷进程退出代替验证。

复核器支持 `recovery/error-retry-resolutions/*.json` 中登记的后续批次，只接受哈希一致、文件/目录覆盖完整且时间顺序正确的独立校验证据。按节点、任务号、相对路径、原回执文件及偏移匹配历史记录，避免同一路径的旧结果掩盖新错误。缺证据或证据矛盾时保留待关注；`verified_retry_snapshot` 只证明补拷与独立校验时的状态，不持续重读在线源内容，也不减少看板中的历史错误累计。导出器每轮重新载入复核模块，部署此扩展无需重启复制、导出或看板进程。

建议先依据 error/changed 清单判断是否可以在源稳定或NFS恢复后补拷；冲突需按每条路径决定保留哪个版本。历史异常结果不自动等于该路径当前仍缺失，需结合当前目标属性及内容复核。

## 可复现证据

控制目录：`/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae`。

导出目录：`/share-mixed/project/eai_pwm/home/hcr/repos/cp_mnodes/runs/projset-copy-outcomes-20261102T144640Z-4316ae`。

`selected-receipts.jsonl` 保存选中的原始回执及原位置；`checkpoint.json` 保存各节点读取偏移；部署记录保存代码SHA和启动身份。导出进程运行在 CPU 主节点，nice=10，不占复制工作线程池。`summary.json` 保存分类容量和当前控制器累计，二者采样时刻可能略有差异。

源码为 `ops/diagnostics/projset_outcomes.py`、`ops/diagnostics/projset_error_reconciliation.py` 和 `ops/diagnostics/projset_changed_reconciliation.py`。私有目录测试覆盖半行、增量去重、派生缓存恢复、容量精度、证据缺失或源/目标身份变化时保留待关注，以及不修改目标文件。
