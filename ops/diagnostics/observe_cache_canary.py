"""Read-only canary counters using previously verified, exact lane identities.

This never invokes a controller or adopts another PID. Global recovery, STOP,
queues and training still require the separate current control inspection.
"""
import argparse
import json
from pathlib import Path

from migration_io import read_json, write_json
import migration_launch as transport


REMOTE = r'''import datetime,json,os,pathlib,sys,time
c=pathlib.Path(CONTROL);sys.path.insert(0,str(c))
from migration_io import read_json
assert os.uname().nodename==HOST, 'wrong canary node'
out={'control':str(c),'node':NODE,'started_at':time.time(),
     'stop':(c/'STOP').exists(),'lanes':{}}
for lane,exp in EXPECTED.items():
 n=read_json(c/('nodes' if lane=='primary' else 'nodes-extra')/(HOST+'.json'))
 assert n['pid']==exp['pid'], 'unexpected heartbeat PID'
 p=pathlib.Path('/proc')/str(exp['pid'])
 f=(p/'stat').read_text().rsplit(') ',1)[1].split()
 argv=(p/'cmdline').read_bytes().decode().strip('\0').split('\0')
 assert f[19]==exp['start_ticks'] and argv==exp['argv'], 'identity changed'
 assert f[0] not in ('Z','X'), 'canary lane is no longer executing'
 assert n['threads']==64 and n['io_bytes_per_second']==10000000000, 'budget changed'
 assert not n.get('ended') and not n.get('thread_error'), 'unhealthy lane'
 now=time.time();updated=datetime.datetime.fromisoformat(n['updated']).timestamp()
 sizes=[x.get('size',0) for x in n.get('active',{}).values()]
 out['lanes'][lane]={'node':n,'identity':exp,'observed_at':now,
  'heartbeat_age_seconds':now-updated,'process_state':f[0],
  'active_size_bins':{'small_le_1MiB':sum(x<=1048576 for x in sizes),
    'middle':sum(1048576<x<67108864 for x in sizes),
    'large_ge_64MiB':sum(x>=67108864 for x in sizes)}}
out['at']=time.time();out['stop']=out['stop'] or (c/'STOP').exists()
print(json.dumps(out))
'''


def observe(node, previous):
    active = read_json(Path(__file__).parent / 'active_migration.json')
    old = read_json(previous)
    if old['control'] != active['control']:
        raise ValueError('previous observation belongs to another control')
    host = 'job-' + active['job'] + '-' + node
    if set(old['lanes']) != {'primary', 'supplement'}:
        raise ValueError('both previously verified lanes are required')
    expected = {}
    launcher = str(Path(active['control']) / 'migration_cached_worker.py')
    for lane, value in old['lanes'].items():
        identity, snapshot = value['identity'], value['node']
        argv = identity['argv']
        if (snapshot['host'] != host or snapshot['pid'] != identity['pid'] or
                type(identity['pid']) is not int or
                not isinstance(identity['start_ticks'], str) or
                not identity['start_ticks'].isdigit() or
                argv[:4] != ['/usr/bin/python3', '-u', launcher, lane] or
                argv[4:8] != ['--control', active['control'], '--threads', '64']):
            raise ValueError('previous exact identity does not describe this node/lane')
        expected[lane] = identity
    # Pass validated values as Python literals over stdin, without a remote
    # shell argument or any caller-provided Python source.
    values = {'CONTROL': active['control'], 'HOST': host,
              'NODE': node, 'EXPECTED': expected}
    script = '\n'.join(key + '=' + repr(value) for key, value in values.items()) + '\n' + REMOTE
    transport.JOB = active['job']
    current = json.loads(transport.ssh(node, script, owner=active['ssh_owner'])[1])
    comparisons = {}
    for lane, value in current['lanes'].items():
        snapshot, before = value['node'], old['lanes'][lane]['node']
        if snapshot['run_started'] != before['run_started']:
            raise ValueError('canary original run_started changed')
        keys = ('bytes', 'streamed_bytes', 'verified_bytes', 'verified_files',
                'completed_batches', 'broker_retries')
        delta = {key: snapshot.get(key, 0) - before.get(key, 0) for key in keys}
        counts = {key: snapshot.get('counts', {}).get(key, 0) -
                  before.get('counts', {}).get(key, 0)
                  for key in set(snapshot.get('counts', {})) | set(before.get('counts', {}))}
        previous_at = old['lanes'][lane].get('observed_at', old['at'])
        elapsed = value['observed_at'] - previous_at
        if elapsed <= 0 or min([*delta.values(), *counts.values()]) < 0:
            raise ValueError('invalid elapsed time or counter rollback')
        comparisons[lane] = {'elapsed_seconds': elapsed, 'deltas': delta,
                             'counts_delta': counts, 'active': len(snapshot.get('active', {})),
                             'file_mix': value['active_size_bins'],
                             'heartbeat_age_seconds': value['heartbeat_age_seconds'],
                             'broker_error': snapshot.get('broker_error')}
    current.update(job=active['job'], previous_observation=str(previous),
                   comparison_to_previous=comparisons, mutation_performed=False,
                   no_payload_reads=True,
                   notes=['Different file mixtures are not a controlled performance comparison.',
                          'This observation alone does not resolve the global performance incident.'])
    return current


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', required=True, choices=['worker-' + str(i) for i in range(11)])
    parser.add_argument('--previous', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    value = observe(args.node, args.previous)
    write_json(args.output, value)
    print(json.dumps({'saved': str(args.output), 'stop': value['stop'],
                      'comparison': value['comparison_to_previous']}))


if __name__ == '__main__':
    main()
