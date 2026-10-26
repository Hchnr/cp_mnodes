"""Observe one explicit lane, then dispatch the validated idle gate at most once.

The observation loop is read-only. It does not drain to create an idle lane.
The original protocol-2 controller retains its five-second/two-heartbeat gate,
flocks, exact-identity checks, ownership, FD, STOP and counter guards.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

from migration_io import read_json, write_json
import migration_launch as transport

BASE = Path(__file__).resolve().parent
SHA = '24fcf5410606d65b228cab6471bd7216b0a7a1f313d7e712741bb230a596b248'
BUNDLE = 'cache-age-visibility-20261021-24fcf541/rollout_cache_age.py'


def stamp(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def eligible(master, worker, expected, budget, now):
    """Fail closed on every missing, stale, changed, occupied or unsafe field."""
    try:
        return bool(
            master['stop'] is False and worker['stop'] is False
            and master['status'] in ('scanning', 'copying')
            and master['training_state'] == 'watching'
            and master['helper_sha256'] == SHA
            and master['owner_count'] == 64 and master['leased_by_stage'] == {}
            and master['host_draining'] is False
            and master['helpers'] == [] and worker['helpers'] == []
            and all(worker[k] == expected[k] for k in ('pid', 'start_ticks', 'argv'))
            and worker['process_state'] in ('R', 'S', 'D', 'I')
            and worker['active'] == 0 and worker['payload_fds'] == 0
            and worker['threads'] == 64 and worker['budget'] == budget
            and worker['ended'] is None and not worker['thread_error']
            and 0 <= now - worker['at'] <= 5
            and 0 <= now - master['at'] <= 5
            and 0 <= now - stamp(worker['updated']) <= 3
            # The watchdog stamps the start of its NFS state/log check and
            # then sleeps 15s. These observation limits do not replace the
            # direct STOP checks or the original post-drain fresh-HB gate.
            and 0 <= now - stamp(master['state_updated']) <= 60
            and 0 <= now - stamp(master['training_updated']) <= 90)
    except (KeyError, ValueError, TypeError, AttributeError):
        return False


def observe_once(probe, dispatch, expected, budget, *, wait_seconds, clock=time.time,
                 monotonic=time.monotonic, sleep=time.sleep, before_dispatch=lambda _: None,
                 allow_dispatch=True):
    deadline = monotonic() + wait_seconds
    samples = []
    while True:
        master, worker = probe()
        samples.append({'master': master, 'worker': worker})
        if master.get('stop') is not False or worker.get('stop') is not False:
            return {'status': 'stopped', 'samples': samples, 'controller_invoked': False}
        if master.get('status') not in ('scanning', 'copying') or master.get('training_state') != 'watching':
            return {'status': 'unsafe_control', 'samples': samples, 'controller_invoked': False}
        if any(worker.get(k) != expected[k] for k in ('pid', 'start_ticks', 'argv')):
            return {'status': 'identity_changed', 'samples': samples, 'controller_invoked': False}
        if eligible(master, worker, expected, budget, clock()):
            if not allow_dispatch:
                return {'status': 'eligible_read_only', 'samples': samples, 'controller_invoked': False}
            result = {'status': 'dispatching_original_controller', 'samples': samples,
                      'controller_invoked': True}
            before_dispatch(result)
            # No retry, including a failed gate, transport error or ambiguous result.
            result['controller_result'] = dispatch()
            result['status'] = 'controller_returned'
            return result
        if monotonic() >= deadline:
            return {'status': 'waiting_payload', 'samples': samples, 'controller_invoked': False}
        sleep(.5)


COMMON = r'''
import sys,os,pathlib,time,json,datetime,hashlib,sqlite3,urllib.request
sys.dont_write_bytecode=True
c=pathlib.Path(CONTROL);sys.pycache_prefix='/tmp/'+c.name.removeprefix('.')+'-inspection-pycache';sys.path.insert(0,str(c))
from migration_io import read_json
def require(condition,message):
 if not condition:raise ValueError(message)
def helpers():
 out=[]
 for p in pathlib.Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   args=(p/'cmdline').read_bytes().decode().strip('\0').split('\0')
   if any(pathlib.Path(x).name=='rollout_cache_age.py' for x in args):out.append(int(p.name))
  except (OSError,UnicodeError):pass
 return out
'''
MASTER = r'''
cfg=read_json(c/'config.json')
require((cfg['source'],cfg['destination'])==('/share/project/eai_pwm','/share-mixed/project/eai_pwm'),'wrong roots')
require(cfg['run_id']==c.name.removeprefix('.'),'wrong run')
db=sqlite3.connect('file:/tmp/'+cfg['run_id']+'-broker.sqlite?mode=ro',uri=True,timeout=3)
db.execute('PRAGMA query_only=ON')
owners=[HOST+'-'+str(i) for i in range(START,START+64)]
rows=db.execute("SELECT stage,COUNT(*) FROM claims WHERE status='leased' AND owner IN ("+','.join('?' for _ in owners)+') GROUP BY stage',owners).fetchall();db.close()
state=read_json(c/'state.json');health=read_json(c/'training-health.json')
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
req=urllib.request.Request(cfg['broker_url']+'/control',headers={'Authorization':'Bearer '+cfg['broker_token']})
with op.open(req,timeout=8) as response:ctl=json.load(response)
print(json.dumps({'at':time.time(),'stop':(c/'STOP').exists(),'status':state.get('status'),
 'state_updated':state.get('updated'),'training_state':health.get('state'),'training_updated':health.get('updated'),
 'owner_count':len(owners),'leased_by_stage':dict(rows),'host_draining':HOST in ctl['draining_hosts'],
 'helper_sha256':hashlib.sha256(pathlib.Path(HELPER).read_bytes()).hexdigest(),'helpers':helpers()}))
'''
WORKER = r'''
require(os.uname().nodename==HOST,'wrong host')
n=read_json(c/FOLDER/(HOST+'.json'));p=pathlib.Path('/proc')/str(EXPECTED['pid'])
fields=(p/'stat').read_text().rsplit(') ',1)[1].split()
argv=(p/'cmdline').read_bytes().decode().strip('\0').split('\0')
fds=0
for fd in (p/'fd').iterdir():
 try:
  x=os.readlink(fd)
  if any(x==root or x.startswith(root+'/') for root in ('/share/project/eai_pwm','/share-mixed/project/eai_pwm')):fds+=1
 except OSError:pass
print(json.dumps({'at':time.time(),'stop':(c/'STOP').exists(),'pid':n['pid'],
 'start_ticks':fields[19],'argv':argv,'process_state':fields[0],'active':len(n.get('active',{})),
 'payload_fds':fds,'threads':n.get('threads'),'budget':n.get('io_bytes_per_second'),
 'updated':n.get('updated'),'ended':n.get('ended'),'thread_error':n.get('thread_error'),'helpers':helpers()}))
'''


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--node', required=True, choices=['master-0'] + ['worker-' + str(i) for i in range(11)])
    ap.add_argument('--lane', required=True, choices=('primary', 'supplement'))
    ap.add_argument('--inspection', required=True, type=Path)
    ap.add_argument('--proc-profile', required=True, type=Path)
    ap.add_argument('--wait', type=float, default=0)
    ap.add_argument('--observe-only', action='store_true', help='never dispatch even if all preflight checks pass')
    ap.add_argument('--output', required=True, type=Path)
    a = ap.parse_args()
    if not 0 <= a.wait <= 45:
        ap.error('read-only wait must be 0..45 seconds')
    active = read_json(BASE / 'active_migration.json')
    inspection = read_json(a.inspection)
    profile = read_json(a.proc_profile)
    require(profile['control'] == active['control'] and profile['job'] == active['job'], 'foreign profile')
    require(inspection['state']['run_id'] == Path(active['control']).name.removeprefix('.'), 'foreign inspection')
    require(profile['rate_complete'] and not profile['errors'] and profile['valid_processes'] == 24, 'incomplete process proof')
    require(inspection['stop'] is False and inspection['training']['state'] == 'watching', 'unsafe training or STOP')
    require(not inspection['broker_control']['draining_hosts'] and not inspection['duplicate_leased_owners'], 'drain or duplicate lease')
    require(0 <= time.time() - inspection['checked_at'] <= 120, 'stale inspection')
    require(0 <= time.time() - profile['finished_epoch'] <= 120, 'stale process proof')
    lane_key = 'primary' if a.lane == 'primary' else 'extra64'
    expected = next(n for n in profile['nodes'] if n['node'] == a.node)['second']['lanes'][lane_key]
    host = 'job-' + active['job'] + '-' + a.node
    component = next(n for n in inspection['node_components'] if n['host'] == host and n['lane'] == lane_key)
    require(component['pid'] == expected['pid'] and component['threads'] == 64, 'component identity or thread mismatch')
    budget = component['io_bytes_per_second']
    require(budget == (9966445568 if a.node == 'master-0' and a.lane == 'primary' else 10000000000), 'budget changed')
    require(hashlib.sha256((BASE / 'rollout_cache_age.py').read_bytes()).hexdigest() == SHA, 'controller differs from validated protocol 2')
    helper = active['control'] + '/' + BUNDLE
    transport.JOB = active['job']
    common = COMMON.replace('CONTROL', repr(active['control']), 1)
    master_code = common + MASTER.replace('HOST', repr(host)).replace('START', str(0 if a.lane == 'primary' else 64)).replace('HELPER', repr(helper))
    worker_code = common + WORKER.replace('HOST', repr(host)).replace('FOLDER', repr('nodes' if a.lane == 'primary' else 'nodes-extra')).replace('EXPECTED', repr(expected))
    def probe():
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(transport.ssh, 'master-0', master_code, owner=active['ssh_owner'])
            second = pool.submit(transport.ssh, a.node, worker_code, owner=active['ssh_owner'])
            return json.loads(first.result()[1]), json.loads(second.result()[1])
    base = {'control': active['control'], 'job': active['job'], 'node': a.node, 'lane': a.lane,
            'helper': helper, 'helper_sha256': SHA, 'started': time.time()}
    def save(value):
        write_json(a.output, {**base, **value})
    def dispatch():
        command = ['python3', str(BASE / 'rollout_cache_age.py'), '--node', a.node, '--lane', a.lane,
                   '--max-age', '30', '--gate-timeout', '5', '--remote-script', helper]
        with open(str(a.output) + '.log', 'w') as log:
            rc = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=240).returncode
        # The original controller owns outcome ambiguity and exact operation recovery.
        result = read_json(BASE / 'rollout_cache_age.json')
        return {'returncode': rc, 'result': result}
    result = observe_once(probe, dispatch, expected, budget, wait_seconds=a.wait, before_dispatch=save,
                          allow_dispatch=not a.observe_only)
    save(result)
    print(json.dumps({'status': result['status'], 'controller_invoked': result['controller_invoked'],
                      'samples': len(result['samples']), 'controller_result': result.get('controller_result')}))


if __name__ == '__main__':
    main()
