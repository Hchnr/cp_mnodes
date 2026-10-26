#!/usr/bin/env python3
"""Fresh full-tree read-only completeness audit; no payload/copy APIs."""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import time
import uuid

ORIGINAL = Path('/share-mixed/project/.hcr-readonly-audit-20261024-raw-excluded-72d05c')
CLUSTER = '2dadf47a-b549-47bb-a267-d56dc877aec9'
NODES = ['master-0'] + ['worker-' + str(i) for i in range(11)]
HELPERS = ('poll_cp_mnodes_readonly_audit_20261024.py',
           'rebalance_cp_mnodes_readonly_audit_20261024.py',
           'auto_balance_cp_mnodes_readonly_audit_20261024.py')


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.part')
    with open(temp, 'x') as stream:
        json.dump(value, stream, ensure_ascii=True, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)


def read(path):
    path = Path(path)
    for attempt in range(4):
        try:
            return json.loads(path.read_text())
        except FileNotFoundError:
            if attempt == 3:
                raise
            os.listdir(path.parent)
            time.sleep(.1)


def sha(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_partition(units):
    roots = {row['rel']:row['leaves_only'] for row in units}
    if len(roots) != len(units) or '' not in roots:
        raise ValueError('duplicate or missing namespace root')
    for rel in roots:
        if not rel:
            continue
        parent = rel.rpartition('/')[0]
        if parent not in roots or not roots[parent]:
            raise ValueError('uncovered parent or overlapping recursive units: ' + rel)
        if rel == 'eai_pwm/datasets/raw' or rel.startswith('eai_pwm/datasets/raw/'):
            raise ValueError('excluded raw subtree')
    return True


def remote(node, code, timeout=60):
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', node, 'python3 -B -u -'],
        input=code, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(node + ': ' + result.stderr[-3000:] + result.stdout[-1000:])
    return json.loads(result.stdout)


PREFLIGHT = r'''
import json,os,pathlib,socket
assert socket.gethostname()=='job-'+CLUSTER+'-'+NODE
old=json.loads(pathlib.Path('/tmp/cp-mnodes-readonly-audit-20261024-raw-excluded-72d05c/launch.json').read_text())
for e in old['old_task_still_paused']:
 try:f=pathlib.Path('/proc',str(e['pid']),'stat').read_text().rsplit(') ',1)[1].split()
 except FileNotFoundError:continue
 assert f[19]!=e['start'] or f[0] in ('T','t','Z','X'),'old copier is active'
c=pathlib.Path('/share-mixed/project/.hcr-copy-fixed-20261025T032630Z-2dad-72d05c/nodes')/NODE
copy=json.loads((c/'launch.json').read_text())
try:f=pathlib.Path('/proc',str(copy['pid']),'stat').read_text().rsplit(') ',1)[1].split()
except FileNotFoundError:f=None
assert f is None or f[19]!=copy['start'] or f[0] in ('Z','X'),'fixed copier still active'
assert os.path.ismount('/share/project') and os.path.ismount('/share-mixed/project')
print(json.dumps(dict(node=NODE,host=socket.gethostname(),old_task_still_paused=old['old_task_still_paused'],
 source_ino=os.stat('/share/project').st_ino,target_ino=os.stat('/share-mixed/project').st_ino)))
'''


START = r'''
import hashlib,json,os,pathlib,socket,subprocess,sys,time
p=PAYLOAD;assert socket.gethostname()==p['host']
assert os.stat('/share/project').st_ino==p['source_ino'] and os.stat('/share-mixed/project').st_ino==p['target_ino']
for e in p['old_task_still_paused']:
 try:f=pathlib.Path('/proc',str(e['pid']),'stat').read_text().rsplit(') ',1)[1].split()
 except FileNotFoundError:continue
 assert f[19]!=e['start'] or f[0] in ('T','t','Z','X')
control=pathlib.Path(p['control']);local=pathlib.Path(p['directory']);report=control/'nodes'/p['node']/p['instance']/'report'
assert not local.exists(), 'do not retry uncertain process launches'
assert os.stat('/tmp').st_dev not in (os.stat('/share/project').st_dev,os.stat('/share-mixed/project').st_dev)
for name,digest in p['hashes'].items():
 with open(control/'tools'/name,'rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==digest
local.mkdir(parents=True,mode=0o700)
folder=report.parent;folder.mkdir(parents=True,exist_ok=True)
plan=folder/'plan.json'
with open(plan,'x') as stream:
 json.dump(p['plan'],stream,ensure_ascii=True);stream.flush();os.fsync(stream.fileno())
command=[sys.executable,'-B','-u',str(control/'tools/cp_mnodes_readonly_scan_20261024.py'),
 '--plan',str(plan),'--output',str(report),'--frontier',str(local/'report/frontier.sqlite'),
 '--threads','32','--stat-threads','64']
with open(folder/'scan.log','ab',buffering=0) as log:
 child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
  start_new_session=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
f=pathlib.Path('/proc',str(child.pid),'stat').read_text().rsplit(') ',1)[1].split()
record=dict(node=p['node'],instance=p['instance'],host=p['host'],directory=str(local),
 identity=dict(pid=child.pid,start=f[19],state=f[0],argv=command),units=len(p['plan']['units']),at=time.time(),
 mode='read_only_metadata',old_task_still_paused=p['old_task_still_paused'],copy_workers_started=0,
 report_directory=str(report),log_file=str(folder/'scan.log'),persistent_control=str(folder),
 directory_threads=32,stat_threads=64,exclusion=['eai_pwm/datasets/raw'])
for path in (local/'launch.json',folder/'launch.json'):
 with open(path,'x') as stream:json.dump(record,stream);stream.flush();os.fsync(stream.fileno())
print(json.dumps(record),flush=True)
'''


def prepare(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    tools = root / 'tools'
    tools.mkdir()
    for name in HELPERS:
        shutil.copyfile(ORIGINAL / 'tools' / name, tools / name)
    shutil.copyfile(ORIGINAL / 'tools/cp_mnodes_readonly_scan_20261024.py', tools / 'readonly_scan_core.py')
    if sha(tools / 'readonly_scan_core.py') != 'd389bf994429c9ca65e9f0933afab70f8d286fdb5b34d9746021a840cb0466f7':
        raise ValueError('historical scanner core changed')
    shutil.copyfile(Path(__file__).with_name('migration_reaudit_scan.py'), tools / 'cp_mnodes_readonly_scan_20261024.py')
    shutil.copyfile(__file__, tools / 'migration_reaudit.py')
    sys.path.insert(0, str(tools))
    import readonly_scan_core as core
    units, issues = core.prepare_units('/share/project', depth=4, cap=256)
    validate_partition(units)
    # Reproducible spreading avoids neighboring large trees sharing one lane.
    random.Random(20261026).shuffle(units)
    plan = dict(source='/share/project', target='/share-mixed/project',
                exclusions=list(core.EXCLUDED), units=units, coverage_roots=[u['rel'] for u in units],
                planning_errors=issues, created=time.time(), cluster=CLUSTER)
    atomic(root / 'plan.json', plan)
    atomic(root / 'source-sha256.json', {p.name:sha(p) for p in tools.glob('*.py')})
    atomic(root / 'exclusion-policy.json', dict(data_copy_authorized=False,
        exclude=list(core.EXCLUDED), source=plan['source'], target=plan['target']))
    print(json.dumps(dict(status='prepared', root=str(root), units=len(units), planning_errors=issues)), flush=True)


def launch(root):
    root = Path(root)
    lock = open(root / 'rebalance.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (root / 'launch-results.json').exists():
        raise ValueError('existing launch intent: inspect before retry')
    plan, hashes = read(root / 'plan.json'), read(root / 'source-sha256.json')
    with ThreadPoolExecutor(max_workers=3) as pool:
        verified = list(pool.map(lambda node:remote(node, 'CLUSTER='+repr(CLUSTER)+'\nNODE='+repr(node)+'\n'+PREFLIGHT), NODES))
    atomic(root / 'preflight.json', dict(at=time.time(), nodes=verified))
    registry = dict(status='launching', mode='read_only_metadata', exclusions=plan['exclusions'],
                    nodes=[], transfers_pending=False, coverage_roots=plan['coverage_roots'])
    atomic(root / 'launch-results.json', registry)
    def start_node(v):
        records = []
        node = v['node']
        base = Path('/tmp') / ('cp-mnodes-' + root.name.removeprefix('.hcr-'))
        index = NODES.index(node)
        for lane in range(3):
            instance = node if lane == 0 else node + '-seed-' + str(lane)
            directory = base if lane == 0 else base / instance
            own = dict(plan, host=v['host'], units=plan['units'][index+lane*12::36])
            payload = dict(v, control=str(root), instance=instance, directory=str(directory),
                           hashes=hashes, plan=own)
            records.append(remote(node, 'PAYLOAD='+repr(payload)+'\n'+START))
            print(json.dumps(dict(node=node, instance=instance, pid=records[-1]['identity']['pid'],
                                  units=records[-1]['units'])), flush=True)
        return records
    with ThreadPoolExecutor(max_workers=3) as pool:
        for records in pool.map(start_node, verified):
            registry['nodes'].extend(records)
            atomic(root / 'launch-results.json', registry)
    if sum(record['units'] for record in registry['nodes']) != len(plan['units']):
        raise ValueError('partition ownership mismatch')
    registry.update(status='launched', at=time.time())
    atomic(root / 'launch-results.json', registry)


def supervise(root):
    root = Path(root)
    sys.path.insert(0, str(root / 'tools'))
    import poll_cp_mnodes_readonly_audit_20261024 as poll
    import auto_balance_cp_mnodes_readonly_audit_20261024 as balance
    lock = open(root / 'supervisor.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    state = dict(pid=os.getpid(), started=time.time(), status='monitoring', waves=0)
    atomic(root / 'supervisor-state.json', state)
    while True:
        result = poll.snapshot(root)
        state.update(updated=time.time(), counts=result['counts'])
        if result['enumeration_complete']:
            state['status'] = 'enumeration_complete_needs_final_validation'
            atomic(root / 'supervisor-state.json', state)
            return
        registry = read(root / 'launch-results.json')
        plan, reason = balance.choose(result, registry, time.time(), max_scans_per_node=3, tail_mode=True)
        state['reason'] = reason
        atomic(root / 'supervisor-state.json', state)
        if plan:
            wave = state['waves'] + 1
            token = 'full-r' + str(wave).zfill(4)
            command = [sys.executable, '-B', '-u', str(root / 'tools' / HELPERS[1]),
                '--directory', str(root), '--donor', plan['donor'], '--token', token,
                '--maximum', str(plan['maximum']), '--retained', str(plan['retained']),
                '--destinations', ','.join(plan['destinations']),
                '--directory-threads', '32', '--stat-threads', '64']
            completed = subprocess.run(command, timeout=600)
            if completed.returncode not in (0, 75):
                state.update(status='transfer_needs_inspection', failed_command=command)
                atomic(root / 'supervisor-state.json', state)
                raise RuntimeError('work transfer incomplete; do not retry blindly')
            if completed.returncode == 0:
                state['waves'] = wave
                atomic(root / 'supervisor-state.json', state)
        time.sleep(20)


def status(root):
    root = Path(root)
    view = read(root / 'summary.json')
    c = view['counts']
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
    print('\n全范围只读复核 | '+now.strftime('%m-%d %H:%M:%S 北京时间'))
    print('枚举结束：{}；已检查 {:,} 条；缺失 {:,} 文件 / {:.3f} GiB'.format(
        view['enumeration_complete'], c.get('entries',0), c.get('missing_files',0), c.get('missing_logical_bytes',0)/1024**3))
    print('缺目录 {:,}；缺链接 {:,}；大小不一致 {:,}；错误 {:,}；变化目录 {:,}'.format(
        c.get('missing_directories',0),c.get('missing_symlinks',0),c.get('size_mismatches',0),
        c.get('errors',0),c.get('changed_source_directories',0)))
    print('节点       | 活跃扫描进程 | 已检查条目 | 待扫目录')
    for node in NODES:
        rows=[r for r in view['nodes'] if r['node']==node]
        print('{:<10} | {:>12} | {:>10,} | {:>8,}'.format(node,
            sum(r.get('report',{}).get('status')=='scanning' for r in rows),
            sum(r.get('report',{}).get('counts',{}).get('entries',0) for r in rows),
            sum(r.get('report',{}).get('pending_directories',0) for r in rows)))
    print('仅核验元数据，不写数据、不启动复制；全量内容哈希未宣称完成。')
    print('控制与日志：'+str(root))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare','launch','supervise','status'))
    parser.add_argument('--control',type=Path,required=True)
    parser.add_argument('--watch',action='store_true')
    args=parser.parse_args()
    if args.command=='prepare': prepare(args.control)
    elif args.command=='launch': launch(args.control)
    elif args.command=='supervise': supervise(args.control)
    else:
        while True:
            status(args.control)
            if not args.watch:break
            time.sleep(5)


if __name__=='__main__':main()
