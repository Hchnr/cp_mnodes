#!/usr/bin/env python3
"""Read-only native PID/start/argv audit of the existing 48 copy processes."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
from pathlib import Path
import subprocess
import time

from migration_io import read_json, write_json


CONTROL3 = '/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a'
BASELINE = CONTROL3 + '/fast-p3-scan-466d08d7ea874d91a73534fb5caabac5'
NODES = ['master-0'] + ['worker-' + str(i) for i in range(11)]


def validate_expected_audit(value):
    nodes = value.get('nodes', [])
    if len(nodes) != 12 or {row['node'] for row in nodes} != set(NODES):
        raise ValueError('complete twelve-node expected audit required')
    for row in nodes:
        identities = row.get('identities', [])
        if len(identities) != 4 or len({x['pid'] for x in identities}) != 4:
            raise ValueError('four unique expected copy identities per node required')
        for item in identities:
            if (type(item.get('pid')) is not int or item['pid'] <= 0
                    or not str(item.get('start_ticks', item.get('starttime_ticks'))).isdigit()
                    or not isinstance(item.get('argv'), list) or not item['argv']
                    or any(not isinstance(x, str) for x in item['argv'])):
                raise ValueError('exact PID/start/argv expected identity required')
    return nodes


def inspect_against(row):
    expected = [dict(pid=item['pid'], start_ticks=str(item.get('start_ticks',item.get('starttime_ticks'))),
                     argv=item['argv']) for item in row['identities']]
    code = '''import sys,json,socket,time
sys.path.insert(0,BASELINE)
from migration_scan_boundary import require_identity
host=socket.gethostname()
assert host=='job-2dadf47a-b549-47bb-a267-d56dc877aec9-'+NODE
values=[require_identity(item) for item in EXPECTED]
assert len(values)==4
print(json.dumps(dict(node=NODE,host=host,identities=values,verified_at=time.time(),copy_identities_unchanged=True)))
'''
    code = 'BASELINE='+repr(BASELINE)+'\nNODE='+repr(row['node'])+'\nEXPECTED='+repr(expected)+'\n'+code
    result = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',
        '-o','ForwardAgent=no','-o','ForwardX11=no','-o','ServerAliveInterval=5',
        '-o','ServerAliveCountMax=2',row['node'],'python3 -B -u -'], input=code,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40, check=True)
    value = json.loads(result.stdout)
    print(row['node'] + ': verified 4 unchanged post-canary copy identities', flush=True)
    return value


def inspect(node):
    code = '''import sys,json,socket,time
from pathlib import Path
sys.path.insert(0, BASELINE)
from migration_io import read_json
from migration_scan_boundary import require_identity
base=read_json(Path(BASELINE)/('copy-verification-'+NODE+'.json'))
assert base['host']==socket.gethostname()=='job-2dadf47a-b549-47bb-a267-d56dc877aec9-'+NODE
assert len(base['identities'])==4
values=[]
for expected in base['identities']:
 actual=require_identity(expected)
 values.append(dict(actual,scope=expected['scope'],role=expected['role']))
print(json.dumps(dict(node=NODE,host=socket.gethostname(),identities=values,
 verified_at=time.time(),copy_identities_unchanged=True)))
'''
    code = 'BASELINE=' + repr(BASELINE) + '\nNODE=' + repr(node) + '\n' + code
    completed = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        '-o', 'ForwardAgent=no', '-o', 'ForwardX11=no', '-o', 'ServerAliveInterval=5',
        '-o', 'ServerAliveCountMax=2', node, 'python3 -u -'], input=code,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40, check=True)
    value = json.loads(completed.stdout)
    print(node + ': verified 4 unchanged copy identities', flush=True)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--expected-audit', type=Path,
                        help='explicit post-canary baseline; default retains the original D3 identities')
    args = parser.parse_args()
    nodes = validate_expected_audit(read_json(args.expected_audit)) if args.expected_audit else NODES
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(inspect_against if args.expected_audit else inspect, nodes))
    record = dict(verified_at=time.time(), nodes=results, copy_process_count=48,
                  all_unchanged=True, signals_sent=0,
                  expected_baseline=str(args.expected_audit) if args.expected_audit else BASELINE)
    write_json(args.output, record)
    print(json.dumps({key: value for key, value in record.items() if key != 'nodes'}), flush=True)


if __name__ == '__main__':
    main()
