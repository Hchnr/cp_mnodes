#!/usr/bin/env python3
"""Fresh global metadata audit. No data copy, repair, or automatic relaunch."""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import time
from migration_reaudit_shared import read as read_pinned, returned

CLUSTER = '2dadf47a-b549-47bb-a267-d56dc877aec9'
NODES = ['master-0'] + ['worker-' + str(i) for i in range(23)]
BASE = Path('/share-mixed/project/.hcr-readonly-audit-20261026-full-0107/tools')
CORE_SHA = 'd389bf994429c9ca65e9f0933afab70f8d286fdb5b34d9746021a840cb0466f7'
EXCLUDED = ['eai_pwm/datasets/raw']


def atomic(path, value):
    tmp = path.with_name(path.name + '.part')
    with tmp.open('w') as f:
        json.dump(value, f); f.flush(); os.fsync(f.fileno())
    tmp.replace(path)


def read(path):
    return read_pinned(path)


def check_root(root):
    if root.parent != Path('/share-mixed/project') or not root.name.startswith('.hcr-global-check-') or root.is_symlink():
        raise ValueError('dedicated mixed-storage audit control required')


def validate_partition(units):
    roots = {r['rel']: r['leaves_only'] for r in units}
    if len(roots) != len(units) or '' not in roots:
        raise ValueError('duplicate or missing root')
    for rel in roots:
        if rel.startswith('/') or any(p in ('.', '..') for p in rel.split('/')):
            raise ValueError('unsafe relative path')
        if rel == EXCLUDED[0] or rel.startswith(EXCLUDED[0] + '/'):
            raise ValueError('raw exclusion violated')
        if rel and roots.get(rel.rpartition('/')[0]) is not True:
            raise ValueError('partition overlap or gap')
    return list(roots)


def remote(node, code):
    p = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
        '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', node,
        'python3 -S -B -u -'], input=code, capture_output=True, text=True, timeout=55)
    if p.returncode:
        raise RuntimeError(node + ': ' + p.stderr[-1500:])
    return json.loads(p.stdout)


PREFLIGHT = '''
import os,json,socket
assert socket.gethostname()=='job-'+CLUSTER+'-'+NODE
assert os.path.ismount('/share/project') and os.path.ismount('/share-mixed/project')
assert os.stat('/share/project').st_ino==69783039777
assert os.stat('/share-mixed/project').st_ino==3706574426
print(json.dumps(dict(node=NODE,host=socket.gethostname())))
'''

START = '''
import os,json,socket,pathlib,subprocess,sys,time,hashlib
root=pathlib.Path(ROOT)
assert socket.gethostname()=='job-'+CLUSTER+'-'+NODE
assert os.stat('/share/project').st_ino==69783039777
assert os.stat('/share-mixed/project').st_ino==3706574426
for name,digest in json.loads((root/'tools-sha256.json').read_text()).items():
 assert hashlib.sha256((root/'tools'/name).read_bytes()).hexdigest()==digest
plan=json.loads((root/'plan.json').read_text())
records=[]
for lane in range(2):
 p=root/'nodes'/NODE/str(lane)
 p.mkdir(parents=True,exist_ok=False)
 own=dict(plan,host=socket.gethostname(),units=plan['units'][INDEX+lane*24::48])
 with (p/'plan.json').open('x') as f:
  json.dump(own,f);f.flush();os.fsync(f.fileno())
 argv=[sys.executable,'-S','-B','-u',str(root/'tools'/'migration_global_scan.py'),
       '--plan',str(p/'plan.json'),'--output',str(p/'report'),'--threads','32','--stat-threads','64']
 with (p/'scan.log').open('xb',buffering=0) as log:
  proc=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,cwd=str(root/'tools'))
 rec=dict(node=NODE,lane=lane,pid=proc.pid,report=str(p/'report'),launched=time.time(),argv=argv)
 with (p/'launch.json').open('x') as f:
  json.dump(rec,f);f.flush();os.fsync(f.fileno())
 records.append(rec)
print(json.dumps(records))
'''


def launch(root):
    check_root(root)
    with (root/'launch-claim.json').open('x') as f:
        json.dump(dict(time=time.time(),pid=os.getpid()),f); f.flush(); os.fsync(f.fileno())
    tools = root/'tools'
    tools.mkdir()
    core = BASE/'readonly_scan_core.py'
    if hashlib.sha256(core.read_bytes()).hexdigest() != CORE_SHA:
        raise ValueError('unrecognized frozen scanner')
    shutil.copyfile(core, tools/core.name)
    shutil.copyfile(BASE/'cp_mnodes_readonly_scan_20261024.py', tools/'migration_reaudit_scan.py')
    shutil.copyfile(Path(__file__).with_name('migration_global_scan.py'),tools/'migration_global_scan.py')
    atomic(root/'tools-sha256.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in tools.iterdir()})
    with ThreadPoolExecutor(max_workers=6) as pool:
        tasks=[pool.submit(remote,n,'CLUSTER='+repr(CLUSTER)+'\nNODE='+repr(n)+'\n'+PREFLIGHT) for n in NODES]
        preflight=[t.result() for t in tasks]
    atomic(root/'preflight.json',preflight)
    sys.path.insert(0,str(tools))
    import readonly_scan_core as core
    units,errors=core.prepare_units('/share/project',depth=4,cap=256)
    roots=validate_partition(units)
    random.Random(20261027).shuffle(units)
    atomic(root/'plan.json',dict(source='/share/project',target='/share-mixed/project',exclusions=EXCLUDED,
        units=units,coverage_roots=roots,planning_errors=errors,created=time.time(),
        data_copy_authorized=False,content_hashes_verified=False,frontier_storage='shared DELETE/FULL single owner'))
    state=dict(status='launching',records=[],errors=[])
    atomic(root/'launch-results.json',state)
    with ThreadPoolExecutor(max_workers=6) as pool:
        tasks={pool.submit(remote,n,'ROOT='+repr(str(root))+'\nCLUSTER='+repr(CLUSTER)+'\nNODE='+repr(n)+'\nINDEX='+str(i)+'\n'+START):n for i,n in enumerate(NODES)}
        for t in as_completed(tasks):
            try:state['records'].extend(t.result())
            except Exception as e:state['errors'].append(dict(node=tasks[t],error=str(e),uncertain=True))
            state['updated']=time.time();atomic(root/'launch-results.json',state)
            print(json.dumps(dict(launched=len(state['records']),errors=state['errors'])),flush=True)
    state['status']='launched' if len(state['records'])==48 and not state['errors'] else 'launch_incomplete'
    atomic(root/'launch-results.json',state)


def snapshot(root):
    state=read(root/'launch-results.json');counts=collections.Counter();statuses=collections.Counter();rows=[]
    for rec in state['records']:
        report=Path(rec['report'])
        try:
            p=report/'result.json'
            row=read(p if p.exists() else report/'progress.json')
            assert row['pid']==rec['pid'] and row['source']=='/share/project' and row['target']=='/share-mixed/project'
            assert row['excluded']==EXCLUDED and row['payload_bytes_read']==0 and row['payload_bytes_written']==0 and row['copy_tasks_published']==0
            closed = returned(dict(log_file=str(report.parent/'scan.log'),identity=dict(pid=rec['pid'])),row)
            counts.update(row['counts']);statuses[row['status']]+=1
            rows.append(dict(node=rec['node'],lane=rec['lane'],status=row['status'],counts=row['counts'],updated=row['updated'],run_returned=closed,
                physical_actors=row.get('physical_actors',[])))
        except (OSError,ValueError,AssertionError) as e:
            statuses['unavailable']+=1;rows.append(dict(node=rec['node'],lane=rec['lane'],error=str(e)))
    return dict(time=time.time(),counts=dict(counts),statuses=dict(statuses),nodes=rows,
        launched=len(state['records']),launch_errors=state['errors'],content_hashes_verified=False,
        enumeration_complete=len(state['records'])==48 and all(r.get('run_returned',False) for r in rows) and not state['errors'])


def reference_progress(row, total):
    """Historical observations are a reference, never a completion gate."""
    if type(total) is not int or total <= 0:
        raise ValueError('reference total must be a positive integer')
    entries=row['counts'].get('entries',0)
    if row['enumeration_complete']:
        return '本轮枚举已结束：{:,} 项；候选复核及完整性收尾另计。'.format(entries)
    return ('历史规模参考：{:,} / 约 {:,} 项（{:.1f}%）；参考差额 {:,} 项；'
            '本轮总量未定，非精确完成率。').format(
                entries,total,entries/total*100,max(0,total-entries))


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('action',choices=['launch','status']);ap.add_argument('--root',type=Path,required=True)
    ap.add_argument('--watch',action='store_true');ap.add_argument('--json',action='store_true')
    ap.add_argument('--reference-total',type=int,default=445_000_000,
        help='historical approximate entry count, not a current-scope denominator (default: 445000000)')
    a=ap.parse_args();check_root(a.root)
    if a.reference_total<=0:ap.error('--reference-total must be positive')
    if a.action=='launch':return launch(a.root)
    previous=None
    while True:
        row=snapshot(a.root);c=row['counts']
        speed='采样中' if previous is None else '{:,.0f} 项/s'.format((c.get('entries',0)-previous['counts'].get('entries',0))/(row['time']-previous['time']))
        if a.json:print(json.dumps(row),flush=True)
        else:
            print(reference_progress(row,a.reference_total),flush=True)
            print('扫描 {:,} 项 | 缺失文件 {:,} / {:.3f} TiB | 缺失目录 {:,} | 缺失链接 {:,} | 完成 {}/48 | {}'.format(
                c.get('entries',0),c.get('missing_files',0),c.get('missing_logical_bytes',0)/2**40,
                c.get('missing_directories',0),c.get('missing_symlinks',0),row['statuses'].get('complete',0),speed),flush=True)
            print('状态 {}；大小不同 {}，链接不同 {}，元数据差异 {:,}，错误 {}；仅元数据核对。'.format(row['statuses'],
                c.get('size_mismatches',0),c.get('symlink_mismatches',0),c.get('metadata_differences',0),c.get('errors',0)),flush=True)
            for owner in row['nodes']:
                if owner.get('physical_actors'):
                    print('均衡分片 {}/{}：{}'.format(owner['node'],owner['lane'],', '.join(
                        '{} {} {:,}项{}'.format(a['node'],a['status'],a.get('entries',0),
                            '（沿用上次观测）' if a.get('observation_reused') else '')
                        for a in owner['physical_actors'])),flush=True)
        if not a.watch:break
        previous=row;time.sleep(15)


if __name__=='__main__':main()
