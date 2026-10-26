"""Read exact saved worker identities through /proc without remote NFS JSON.

This fallback never opens payloads or changes a remote process. A stale saved
identity produces an error, never automatic adoption, restart, or a zero rate.
"""
import argparse
import collections
import concurrent.futures
import json
from pathlib import Path
import time

from migration_io import read_json, write_json
import migration_launch as transport


ARGV_BASELINE = Path(__file__).parent / 'cache_age_canary_proc_20261021T2003_after.json'
EXPECTED_NODES = {'master-0'} | {'worker-' + str(i) for i in range(11)}


def normalized_argv(value):
    if not isinstance(value, list) or not value or any(not isinstance(arg, str) or '\0' in arg for arg in value):
        raise ValueError('a complete saved argv list is required')
    result = list(value)
    while result and result[-1] == '':
        result.pop()
    if not result or not result[0]:
        raise ValueError('an executable is required in saved argv')
    return result


def bind_worker_argv(baseline, argv_profile, control, job):
    """Bind immutable authorized argv evidence to the explicit PID/start baseline.

    The rich profile supplies the authorized identities, but its identities
    objects do not contain argv. The separately verified post-canary /proc
    profile supplies full argv; both its samples must agree with each other
    and with every rich-profile identity. No current PID can be adopted here.
    """
    for profile in (baseline, argv_profile):
        if (profile.get('control'), profile.get('job')) != (control, job):
            raise ValueError('baseline belongs to another job/control')
    if argv_profile.get('errors') != [] or argv_profile.get('valid_processes') != 24 or argv_profile.get('rate_complete') is not True:
        raise ValueError('argv baseline was not a complete successful process profile')
    prefix = 'job-' + job + '-'
    nodes = {}
    for node in baseline['nodes']:
        host = node['host']
        if not isinstance(host, str) or not host.startswith(prefix) or host[len(prefix):] not in EXPECTED_NODES:
            raise ValueError('unexpected baseline host')
        name = host[len(prefix):]
        if name in nodes or set(node['identities']) != {'primary', 'extra64'}:
            raise ValueError('duplicate host or incomplete lane identities')
        nodes[name] = {}
        for lane, identity in node['identities'].items():
            if (identity.get('identity_matches') is not True or type(identity.get('pid')) is not int
                    or identity['pid'] <= 0 or not str(identity.get('start_ticks', '')).isdigit()
                    or int(identity['start_ticks']) <= 0):
                raise ValueError('baseline identity was not validated')
            nodes[name][lane] = dict(identity)
    if set(nodes) != EXPECTED_NODES:
        raise ValueError('baseline must cover all twelve physical nodes')
    seen = set()
    for node in argv_profile['nodes']:
        name = node.get('node')
        if name not in nodes or name in seen:
            raise ValueError('unexpected or duplicate argv-baseline node')
        seen.add(name)
        first, second = node['first']['lanes'], node['second']['lanes']
        if set(first) != {'primary', 'extra64'} or set(second) != set(first):
            raise ValueError('incomplete argv-baseline lanes')
        for lane, expected in nodes[name].items():
            saved = []
            for sample in (first[lane], second[lane]):
                if (sample.get('identity_valid') is not True or sample.get('pid') != expected['pid']
                        or str(sample.get('start_ticks')) != str(expected['start_ticks'])
                        or sample.get('state') == 'Z' or 'error' in sample):
                    raise ValueError('argv evidence does not match authorized PID/start')
                saved.append(normalized_argv(sample.get('argv')))
            if saved[0] != saved[1]:
                raise ValueError('argv changed within authorized baseline')
            argv = saved[0]
            kind = 'primary' if lane == 'primary' else 'supplement'
            for left, right in ((control + '/migration_cached_worker.py', kind), ('--control', control), ('--threads', '64')):
                if not any(argv[i:i + 2] == [left, right] for i in range(len(argv) - 1)):
                    raise ValueError('argv evidence has the wrong worker scope/lane/threads')
            expected['argv'] = argv
    if seen != EXPECTED_NODES:
        raise ValueError('argv baseline must cover all twelve physical nodes')
    return nodes


REMOTE = r'''import collections,json,pathlib,time
identities=IDENTITIES
roles=ROLES
control=CONTROL
def process(expected,lane=None):
 p=pathlib.Path('/proc')/str(expected['pid'])
 try:
  fields=(p/'stat').read_text().rsplit(') ',1)[1].split()
  argv=(p/'cmdline').read_bytes().decode().rstrip('\0').split('\0')
  if fields[19]!=str(expected['start_ticks']) or fields[0]=='Z':
   raise ValueError('saved PID/startticks no longer identifies a live process')
  original=list(expected['argv'])
  while original and original[-1]=='':original.pop()
  if argv!=original:
   raise ValueError('full argv differs from the authorized baseline')
  if lane:
   kind='primary' if lane=='primary' else 'supplement'
   assert any(argv[i]==control+'/migration_cached_worker.py' and argv[i+1]==kind for i in range(len(argv)-1))
   assert any(argv[i]=='--control' and argv[i+1]==control for i in range(len(argv)-1))
   assert any(argv[i]=='--threads' and argv[i+1]=='64' for i in range(len(argv)-1))
  counters={k:int(v) for k,v in (line.split(':',1) for line in (p/'io').read_text().splitlines())}
  waits=collections.Counter();wait_errors=0
  for thread in (p/'task').iterdir():
   try:waits[(thread/'wchan').read_text().strip()]+=1
   except OSError:wait_errors+=1
  after=(p/'stat').read_text().rsplit(') ',1)[1].split()
  argv_after=(p/'cmdline').read_bytes().decode().rstrip('\0').split('\0')
  if after[19]!=fields[19] or after[0]=='Z' or argv_after!=argv:
   raise ValueError('process identity/argv changed while sampling /proc')
  return {'pid':expected['pid'],'start_ticks':fields[19],'argv':argv,'identity_valid':True,
          'state':fields[0],'threads':int(fields[17]),'io':counters,
          'wchan':dict(waits),'wchan_read_errors':wait_errors}
 except Exception as exc:
  return {'pid':expected['pid'],'error':type(exc).__name__+': '+str(exc)}
def snapshot():
 return {'epoch':time.time(),'monotonic':time.monotonic(),
         'lanes':{lane:process(value,lane) for lane,value in identities.items()},
         'roles':{name:process(value) for name,value in roles.items()}}
first=snapshot();time.sleep(DURATION);second=snapshot()
print(json.dumps({'first':first,'second':second}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True, help='saved full worker profile with exact identities')
    parser.add_argument('--argv-baseline', default=str(ARGV_BASELINE), help='previously verified full argv profile, bound to --baseline PID/start')
    parser.add_argument('--roles', help='optional saved heartbeat with master role identities/argv')
    parser.add_argument('--duration', type=float, default=5)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if not 0 < args.duration <= 30:
        parser.error('duration must be greater than zero and at most30 seconds')
    active = read_json(Path(__file__).parent / 'active_migration.json')
    baseline = read_json(args.baseline)
    nodes = bind_worker_argv(baseline, read_json(args.argv_baseline), active['control'], active['job'])
    roles = {}
    if args.roles:
        heartbeat = read_json(args.roles)
        if heartbeat.get('state', {}).get('control', active['control']) != active['control']:
            raise ValueError('master role evidence belongs to another control')
        roles = {name: {key: value[key] for key in ('pid', 'start_ticks', 'argv')}
                 for name, value in heartbeat['processes'].items()}
        if any(active['control'] not in value['argv'] for value in roles.values()):
            raise ValueError('master role argv lacks this control')
    transport.JOB = active['job']

    def inspect(node, identities):
        code = REMOTE.replace('IDENTITIES', repr(identities), 1).replace('ROLES', repr(roles if node == 'master-0' else {}), 1)
        code = code.replace('CONTROL', repr(active['control']), 1).replace('DURATION', repr(args.duration), 1)
        _, raw = transport.ssh(node, code, owner=active.get('ssh_owner'))
        return dict(json.loads(raw), node=node)

    started = time.time()
    results, errors = [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(inspect, name, identities): name for name, identities in nodes.items()}
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append({'node': futures[future], 'error': type(exc).__name__ + ': ' + str(exc)})
    rates, valid = collections.Counter(), 0
    for node in results:
        first, second = node['first'], node['second']
        interval = second['monotonic'] - first['monotonic']
        for lane, old in first['lanes'].items():
            new = second['lanes'][lane]
            if (not old.get('identity_valid') or not new.get('identity_valid') or
                    old.get('argv') != new.get('argv')):
                errors.append({'node': node['node'], 'lane': lane, 'first': old, 'second': new})
                continue
            delta = {key: new['io'][key] - old['io'][key] for key in ('rchar', 'wchar')}
            if interval <= 0 or min(delta.values()) < 0:
                errors.append({'node': node['node'], 'lane': lane, 'error': 'invalid interval/counter delta'})
                continue
            valid += 1
            rates.update({key: value / interval for key, value in delta.items()})
        for name, old in first['roles'].items():
            new = second['roles'][name]
            if not old.get('identity_valid') or not new.get('identity_valid'):
                errors.append({'node': node['node'], 'role': name, 'first': old, 'second': new})
    result = {'control': active['control'], 'job': active['job'], 'baseline': args.baseline,
              'argv_baseline': args.argv_baseline, 'full_argv_bound_to_baseline': True,
              'role_baseline': args.roles, 'started_epoch': started, 'finished_epoch': time.time(),
              'duration_requested_seconds': args.duration, 'nodes': results, 'errors': errors,
              'valid_processes': valid, 'rate_complete': valid == 24 and not errors,
              'logical_io_bytes_per_second': dict(rates), 'no_nfs_json_or_payload_reads': True,
              'mutation_performed': False,
              'notes': ['Rates are logical process I/O, not physical storage bandwidth.',
                        'STOP, queues, budgets and training still require the separate control inspection.']}
    write_json(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key != 'nodes'}))
    if not result['rate_complete']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
