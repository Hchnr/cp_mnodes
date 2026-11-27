# 大目录复查自动进展

更新时间：2026-11-27 15:45:10（北京时间）。本文件由程序生成；未完成项不计入可删除容量。

## EgoLive

- 源：`/share-mixed/projset/eai_pwm/datasets/EgoLive`
- 目标：`/share-mixed/project/eai_pwm/datasets/EgoLive`
- 状态：**根目录不存在或暂不可确认**。
- 复查完成时间：2026-11-16 10:32:06（北京时间）；发布时只重新确认根目录存在。
- 复查范围：199,655 个文件、35.492503 TB。
- 权限差异：目录 2,831 项、文件 0 项。
- [最终复查证据](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egolive-receipt-verify-20261115T1732Z/VERIFIED.md)。

## Lightwheel

- 源：`/share-mixed/projset/eai_pwm/datasets/Lightwheel-Purchased-0901_1`
- 目标：`/share-mixed/project/eai_pwm/datasets/Lightwheel-Purchased-0901_1`
- 状态：**根目录不存在或暂不可确认**。
- 复查完成时间：2026-11-18 06:26:27（北京时间）；发布时只重新确认根目录存在。
- 复查范围：1,126,246 个文件、30.281014 TB。
- 权限差异：目录 678,835 项、文件 0 项。
- 此轮仅保存权限差异数量，未逐项保存路径和权限值；需业务确认或补查。
- [最终复查证据](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/large-candidates-20261115T0945Z/more/lightwheel/VERIFIED.md)。

## RoboMIND2.0-Ark

- 源：`/share-mixed/projset/eai_pwm/datasets/RoboMIND2.0-Ark`
- 目标：`/share-mixed/project/eai_pwm/datasets/RoboMIND2.0-Ark`
- 状态：**根目录不存在或暂不可确认**。
- 复查完成时间：2026-11-16 07:27:45（北京时间）；发布时只重新确认根目录存在。
- 复查范围：14,913 个文件、10.446248 TB。
- 权限差异：目录 29,825 项、文件 0 项。
- [最终复查证据](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/ark-full-verify-20261115T2017Z/VERIFIED.md)。

## Egocentric-10K-unpacked

- 源：`/share-mixed/projset/eai_pwm/datasets/Egocentric-10K-unpacked`
- 目标：`/share-mixed/project/eai_pwm/datasets/Egocentric-10K-unpacked`
- 状态：**根目录不存在或暂不可确认**。
- 复查完成时间：2026-11-21 14:59:44（北京时间）；发布时只重新确认根目录存在。
- 复查范围：403,924 个文件、17.962617 TB。
- 权限差异：目录 21,733 项、文件 0 项。
- [最终复查证据](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/full-audit-20261112T1150Z-6c0af294/egocentric-full-verify-20261115T2251Z/VERIFIED.md)。

执行任何清理前，须确认业务读取已切换、源目录停写，确认权限差异可接受，并再次核对源和目标。源文件系统在线，既有核查不是原子快照；逻辑容量不保证等于后端回收量。

部分符号链接仍保留 `/share/mixed/projset/...` 旧绝对路径，链接文本一致不代表业务读取已切换。清理前须核查业务环境挂载以及其他目录中指向候选源路径的链接和读路径依赖；[只读抽查](/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae/recovery/symlink-path-audit-20261112T061905Z-6cb1daa9/SAMPLE-20261116T0831BJT.md)不代表每个候选目录都被引用。
