# 中文

## 标题

新增集群清理工具、动态扩容与受控长尾调度

## 变更说明

- 新增独立的 `project_cleanup_20261029/` 运维工具集，不修改历史复制引擎。
- 使用 GNU rm、有界共享队列及按需领取实现目录清理；支持失败批次有界重试、保留挂载根和完成状态核验。
- 支持补充任务、长尾细分、运行时节点加入及跨集群 24→40 别名扩容；交接前核验原进程实际退出。
- 增加按 boot 身份和父目录 inode 约束的平铺批配额、公平调度与绑定运行身份的额度热调。
- 增加实际 worker 身份核验，以及限定子树的协作长尾入口；检查 PID、启动时间、boot、argv、cwd/目录 FD 和冻结文件摘要。
- 记录历史部署、吞吐采样、故障原因和收尾边界，更新仓库导航。

## 验证

- 2026-11-02：`python3 -S -B -m unittest discover -s project_cleanup_20261029 -p 'test_*.py'`，213 项测试通过。
- 仓库工具的 9 项回归测试通过；本目录 22 个 Python 文件语法检查通过。
- 测试使用私有临时目录和模拟连接，不连接或操作生产节点。
- `git diff --cached --check` 通过。

## 风险与范围

- 工具含真实删除入口，仅在管理员确认目标和当前环境后显式执行；本次提交不启动、停止或部署任务。
- 路径、集群 hostname、根 inode 及部分进程身份绑定历史环境，不是通用即用配置；不要盲目重跑文档中的历史命令。
- 40 个 SSH 别名不等于 40 台独立机器。协作删除与原父树遍历可能重叠，不保证线性提速或零重复遍历。
- 批次数不是文件数或容量百分比，整卷占用不是待删容量；历史测速不是受控 A/B。
- 补充组完成不等于全盘完成，仍需全局收尾及根目录为空的核验；运维记录是历史快照，不代表当前状态。
- 不提交运行目录、日志、凭据、大体积盘点 CSV、删除清单或编辑器配置。

# English

## Title

Add distributed cleanup tooling, runtime expansion, and guarded tail scheduling

## Summary

- Add the standalone `project_cleanup_20261029/` operations toolkit without changing the historical copy engine.
- Use GNU rm with a bounded shared queue and demand-driven dispatch; support bounded retries, mount-root retention, and completion verification.
- Add supplemental work, streaming tail subdivision, runtime worker joins, and cross-cluster expansion from 24 to 40 aliases, with actual old-process exit checks before handover.
- Add boot-aware, parent-inode-bound quotas for flat batches, fair scheduling, and run-identity-bound live quota tuning.
- Verify actual worker identities and add a subtree-restricted cooperative tail launcher with PID/start/boot, argv, cwd/directory-FD, and frozen-source checks.
- Document historical deployments, throughput samples, failures, and finalization limits; update repository navigation.

## Validation

- 2026-11-02: `python3 -S -B -m unittest discover -s project_cleanup_20261029 -p 'test_*.py'` passed all 213 tests.
- All nine repository-tool regression tests passed; syntax checks passed for all 22 Python files in the cleanup directory.
- Tests use private temporary trees and simulated connections; they do not connect to or operate production nodes.
- `git diff --cached --check` passed.

## Risks and scope

- These tools contain real deletion entry points and require explicit administrator review of the target and current environment. This commit does not start, stop, or deploy jobs.
- Paths, cluster hostnames, root inode, and some process identities bind historical environments; this is not a generic ready-to-run configuration. Do not blindly replay historical commands.
- Forty SSH aliases do not mean forty independent machines. Cooperative deletion may overlap the enclosing traversal; linear speedups and zero duplicate traversal are not guaranteed.
- Batch counts are neither file counts nor byte percentages; volume usage is not a remaining-deletion denominator. Historical measurements are not controlled A/B results.
- Supplemental completion is not whole-root completion: final cleanup and an empty-root check are still required. Operations records are historical snapshots, not current status.
- Runtime state, logs, credentials, large inventory CSVs, deletion manifests, and editor settings are excluded from this commit.
