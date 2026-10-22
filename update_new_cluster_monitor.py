import json,os,pathlib,sys,tomllib
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_io import read_json
r=read_json(pathlib.Path(__file__).parent/'new_cluster_recovery.json')
role=lambda n:r['roles'].get('master-0:'+n,{}).get('pid')
prompt=f'''继续跟进本聊天已授权的全闪→混闪复制，直至四个范围全部核验、交付最终报告并通知用户。正常前进无实质变化时保持安静；只在阶段最终完成、全量完成、异常/停滞或需用户操作时中文通知。不要归档聊天。

【当前新集群与恢复状态，覆盖全部旧节点说明】
用户已明确确认旧job-{r['old_job']}已停止或删除。新集群job-{r['job']}，SSH owner=ybai，master-0、worker-0..10。SSH用户job-{r['job']}-{{node}}.ybai.baai-eai_pwm.ch1-calc1.job@ssh.platform-multi.baai.ac.cn，端口2222。
当前控制目录{r['control']}；本地work/active_migration.json和work/new_cluster_recovery.json必须migration_io.read_json读取最新版本。稳定入口 python3 /share-mixed/project/.hcr-copy-monitor/progress.py --watch，--json单次纯JSON；已有watch需Ctrl+C重开加载更新。当前正在preparing_resume恢复历史索引，seed-progress.json显示实际枚举/读取/索引计数，不能当成本轮重新核验完成量。已确认12物理节点24复制进程1536线程就绪、队列无重复、无STOP；恢复索引结束后同一个producer自动运行8轮，不需要另一次启动。
master producer PID{role('producer')}，入口当前C/migration_fresh_resume.py run --control C；broker PID{role('broker')}，migration_broker_parallel.py --control C --port37651；metrics PID{role('metrics')}，watchdog PID{role('watchdog')}。所有身份以最新recovery-roles/host-role.json的PID/startticks/exact argv和/proc核实，不能只信这些PID数字。恢复记录roles含12个inventory后台及inventory-prepare。当前NOFILE软限额65536，nice15、ionice空闲级。新节点预检没发现训练进程，watchdog只读旧action128日志且从当时EOF开始；不允许改或重启训练。
旧C={r['old_control']}，已加STOP，replacement-job.json记录用户停止确认，全部证据保留。其祖先 .hcr-copy-20261021T054248Z-resume-0c6401 和 .hcr-copy-20261021T033600Z-e1113e 同样STOP且保留。新索引从全部祖先持久receipts重建，不依赖旧/tmp。旧worker2大zip未找到完成回执，按未完成文件整文件重拷，旧临时碎片保留，不盲目采用或清理。
恢复新run已有fresh-bootstrap.json；严禁重复run、旧Coordinator构造器、prepare_new_cluster.py、任何旧scale/handover/rollout脚本；已有本地索引/WAL/SHM不删不重建。已部署role launcher为C/migration_recovery_role.py的launch接口；本地同名新版只是未部署候选，接口/证据格式不同，绝不能覆盖运行bundle或直接重放prepare。work/recover_new_cluster.py也是未使用候选，不运行。若恢复异常，先检查真实进程、state/error、seed进展、日志和新临时SQLite；安全排障不能绕过STOP。
当前同一heartbeat暂为5分钟观察恢复；确认index发布、真实扫描/校验前进且12节点健康后，将本heartbeat恢复60分钟并更新状态，不能创建重复automation。

【范围、优先级与资源】
源/share/project/eai_pwm→目标/share-mixed/project/eai_pwm。首轮严格1 home/hcr →2 repos/cosmos-framework-pwm →3其余(排除前两棵)。保留结构、隐藏内容、链接、元数据，只复制，绝不移动或删除源。用户自行适配重启实验后自行删除旧数据。已完成文件通过持久SHA和inode/大小/时间身份记录复用，改变文件重新校验，未完成按整文件重拷。
每物理节点128复制线程=primary64+supplement64，12节点1536路；扫描64目录线程、无条目速率上限、每批64文件/1GiB。config.threads必须64；copy_threads_per_node128/supplement_threads64/parallel_scan_threads64。主辅各10,000,000,000B/s；master主进程额外扣32MiB/s供串行后台硬链接SHA；每节点读取+写入+校验合计上限20GB/s，12节点240GB/s。保留nice15+ionice3。不能回退旧低并发或低限速，不再做用户取消的cp/rsync benchmark。监控必须migration_nodes.load_nodes合并nodes和nodes-extra到12物理节点，组件心跳取最老，保留每组件PID/错误，不能新组件掩盖旧组件死亡。
独立清点每节点1线程100条目/s，metrics每5秒采样、30秒独立汇总。完整规模尚未确定时只能下限，不能准确全量百分比/ETA；历史基线与本轮确认重叠不相加，大文件逻辑streamed写入不等于物理带宽，不能拿大文件峰值推小文件。

【调度、持久化与恢复保护】
所有可变JSON用migration_io不可变快照按小时分钟读写；canonical第一次创建后会陈旧，禁止直接json.load/read_text canonical状态、禁止os.replace可变JSON、不清理snapshots。broker为主节点HTTP+主节点本地SQLite唯一分配；NFS rename/O_EXCL跨节点竞争已实测不可靠，不能回退共享文件锁分配。主索引/tmp/{r['run_id']}-index.sqlite，broker/tmp/{r['run_id']}-broker.sqlite；不可删除租约或已确认索引。collect所有权继承必须inode与sha同时匹配，不能OR prior.owned。
检查state/progress/seed-progress/pipeline/metrics.sample_time/inventory/两路nodes/training-health/broker/events/issues、队列和真实进程。work/heartbeat_inspect.py已支持recovery-role producer证据，work/profile_copy_workers.py可只读采样。不要因活跃少或心跳抖动就重启；先核对IO、NFS锁、RPC、producer供给和当前文件分布，保护活跃超大文件不重拷。StateCache每进程0.5秒刷新state、最大3秒新鲜度，失效不领新任务，STOP直接检查。任何STOP先读原因并说明，不自动绕过。
新run是全新任务命名空间，8轮p1r1,p2r1,p3r1,p1r2,p2r2,p3r2,p1r3,p2r3；先前已复制文件由seed索引快速确认。pipeline后台collect/hardlinks串行、目录收尾并行，_background_collect_pending不是最终结果；必须等待真实summary/finalize、directory_jobs/shared_ancestors。各最后轮目录元数据32线程，先mkdir屏障再深到浅，根/home/repos最后处理。继承或恢复helper必须PID/startticks/exact argv及结果证据，先确认前helper结束才开下一个32MiB/s哈希helper，禁止双finalizer；无结果退出不能误报成功。producer故障恢复须新明确交接或新control祖先链，不能盲目重跑一次性bootstrap。

【已授权优先级4，不做/home迁移】
P4 /share/project → /share-mixed/project，排在1–3全部最终验收后；只排除顶层eai_pwm，由1–3覆盖，不能父路径规模重复相加，也不能误排其他同名home/hcr或repos目录。当前C/extension-plan.json最新queued，depends_on.control已改当前C，covered_by含原三祖先与当前C；原旧C计划保留证据。固定successor run_id hcr-copy-p4-20261021100403-b763f9，control /share-mixed/project/.hcr-copy-p4-20261021100403-b763f9，master本地/tmp/runid-dispatch.lock防重。只有全部4范围验收后才整体完成或删除heartbeat。
依赖释放前确认1–3最后p1r3,p2r3,p3r2真实summary/finalize、目录及shared_ancestors、无pending占位/fatal，审阅issues并安全补拷稳定文件；所有pending/running/broker租约排空，24worker和helper确实结束。complete_with_changes不是无条件成功触发；failed/stopped/STOP不得触发P4。
P4需要独立不可变bundle/DB/receipts，不能改原source/destination或拿eai_pwm相对路径直接resume_from。当前引擎只支持1–3，需适配真实p4r1..3及broker正则/scanner/metrics/inventory/monitor/report，精确排除顶层eai_pwm，复用原copy_entry/process_entry和SQLite唯一分配。只同步复制子目录元数据，保留/share-mixed/project挂载根mode/owner/ACL，不触碰eai_pwm或控制目录。跨eai_pwm硬链接须核验当前源/目标一致后还原，无法保真明确记录。P4启动前后预算不叠加，指针/计划/heartbeat同步更新，保留前三项验收covered_by证据。

【监控与最终验收】
1–4统一去重统计，各优先级同列展示，不再另设后续目标块；P4未知时全量百分比/ETA未知。每轮文本96个=、复制进度开始|北京时间及横线，表格对齐，--json纯JSON。恢复seed处理计数不计入本轮确认副本。
完成前核实各phase最新轮真实summary/finalize、目录metadata、issues、全部队列/租约、后台结果、实际进程；首轮或文件数据排空都不能称全完。源在线不保证一致快照，complete_with_changes读取具体差异、安全补拷稳定文件，持续写入/冲突目标/不支持特殊对象明确交代；目标已有额外内容保留，不覆盖未经本任务拥有的冲突数据。
保留全过程祖先control、manifest、receipts、events、issues、metadata、配置及metrics历史，不清理。只读work/migration_report.py可生成outputs/混闪复制报告.html（过程稿）、混闪复制报告数据.json、混闪复制速度记录.csv，不遍历源内容或改进程，归档失败不打断复制。最终报告覆盖四范围按唯一路径去重文件/目录/链接、逻辑容量及分配空间、总历时及暂停恢复、各轮扫描复制核验metadata跨度重叠、历史复用与新复制/重拷、吞吐曲线及线程/I/O变更、异常最后差异、全范围补扫和在线写入边界、队列/后台/进程验收证据。copy/checksum时间不能精确拆分要明说，稀疏逻辑字节非物理IO，基线多轮不重复相加，采样缺口保留。不能仅凭state终态称全量成功。全部验收、通知并交付可点击最终报告后删除本heartbeat，不归档聊天。
'''
p=os.path.join(os.environ.get('CODEX_HOME',os.path.expanduser('~/.codex')),'automations/automation/automation.toml');a=tomllib.load(open(p,'rb'))
args={'id':'automation','mode':'update','kind':'heartbeat','name':a['name'],'status':'ACTIVE','rrule':'RRULE:FREQ=MINUTELY;INTERVAL=5','targetThreadId':a['target_thread_id'],'prompt':prompt}
out=pathlib.Path(__file__).parent/'new_cluster_automation_update.json';out.write_text(json.dumps(args,ensure_ascii=False));print(len(prompt))
