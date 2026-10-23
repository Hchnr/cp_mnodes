#!/usr/bin/env python3
"""Stage an identity-bound probe-only update; never restart copying services.

Run prepare on the actual master, then node sequentially on each physical node.
The existing one-shot probe helper owns the scoped replacement transaction.
Copy worker, producer and broker identities are checked before and after it.
"""

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import shutil
import socket
import urllib.request
import uuid

import deploy_fast_open_target_probes as deploy
from migration_io import read_json, write_json
from migration_small_probe_delegate import validate_policy


NODES = ('master-0', *(f'worker-{i}' for i in range(11)))
SOURCES = ('deploy_probe_policy.py', 'deploy_fast_open_target_probes.py',
           'migration_probe_fast_open.py', 'migration_target_probe.py',
           'migration_linux_open.py', 'migration_small_probe_delegate.py',
           'migration_io.py', 'migration_scan.py', 'migration_worker.py')


def unchanged_process(expected):
    if not deploy.same_process(deploy.identity(expected['pid']), expected):
        raise RuntimeError('protected process identity changed: ' + str(expected['pid']))


def guard_node(control, bundle, node):
    protected = read_json(bundle / 'protected-services.json')
    if protected['control'] != str(control) or protected['run_id'] != read_json(control / 'config.json')['run_id']:
        raise ValueError('protected services belong to another control/run')
    host = socket.gethostname()
    if host != protected['hosts'][node]:
        raise ValueError('wrong physical node for protected identities')
    for expected in protected['workers'][node].values():
        unchanged_process(expected)
    if node == 'master-0':
        for expected in protected['master'].values():
            unchanged_process(expected)
    return protected


def prepare(control, predecessor, producer_pid, max_file_bytes):
    control, predecessor = Path(control).resolve(), Path(predecessor).resolve()
    cfg = read_json(control / 'config.json')
    if (cfg.get('control') != str(control) or predecessor.parent != control or
            (cfg.get('source'), cfg.get('destination')) !=
            ('/share/project/eai_pwm', '/share-mixed/project/eai_pwm') or
            (control / 'STOP').exists()):
        raise ValueError('wrong copy scope, predecessor, or STOP present')
    policy = validate_policy(dict(read_json(predecessor / 'probe-policy.json'),
                                  max_file_bytes=max_file_bytes))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def health(endpoint):
        request = urllib.request.Request(endpoint + '/health',
                    headers={'Authorization': 'Bearer ' + cfg['broker_token']})
        with opener.open(request, timeout=10) as response:
            result = json.load(response)
        node = result['host'].removeprefix('job-' + cfg['job'] + '-')
        expected_argv = deploy.probe_argv(predecessor / 'migration_probe_fast_open.py',
                            control, result['port'], endpoint.split('://')[1].rsplit(':', 1)[0],
                            predecessor / 'roles' / node / 'probe.json')
        if (node not in NODES or result.get('ok') is not True or
                result.get('control') != str(control) or result.get('run_id') != cfg['run_id'] or
                result.get('endpoint') != endpoint or result.get('argv') != expected_argv or
                result.get('slots') != 64 or result.get('batch_limit') != 64 or
                result.get('small_probe_delegate', {}).get('policy') !=
                read_json(predecessor / 'probe-policy.json')):
            raise ValueError('probe health does not match frozen predecessor/node')
        return node, result

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        rows = list(pool.map(health, cfg['target_probe_endpoints']))
    roles = dict(rows)
    if len(rows) != 12 or set(roles) != set(NODES):
        raise ValueError('expected exactly twelve distinct healthy probe nodes')
    identities = read_json(control / 'native-worker-final-identities.json')
    if identities.get('all_24_identities_verified') is not True:
        raise ValueError('complete protected worker identity baseline required')
    workers, hosts = {}, {}
    for row in identities['nodes']:
        node = row['node']
        if node in workers or node not in NODES or row['host'] != roles[node]['host']:
            raise ValueError('duplicate or mismatched worker identity baseline')
        lanes = {lane['lane']: lane['identity'] for lane in row['lanes']}
        if set(lanes) != {'primary', 'supplement'}:
            raise ValueError('both protected copy lanes required')
        for lane, identity in lanes.items():
            folder = 'nodes' if lane == 'primary' else 'nodes-extra'
            heartbeat = read_json(control / folder / (row['host'] + '.json'))
            if (heartbeat['pid'] != identity['pid'] or heartbeat.get('ended') or
                    heartbeat.get('thread_error') or heartbeat.get('tuning_error')):
                raise RuntimeError('worker heartbeat differs from protected baseline')
        workers[node], hosts[node] = lanes, row['host']
    if set(workers) != set(NODES):
        raise ValueError('all twelve protected worker nodes required')
    producer = deploy.identity(producer_pid)
    broker = deploy.identity(read_json(control / 'broker.json')['pid'])
    state = read_json(control / 'state.json')
    entry = str(Path(state['foreground_handover']).parent / 'migration_interleaved_target_filter.py')
    if (not deploy.same_process(producer, producer or {}) or entry not in producer['argv'] or
            str(control) not in producer['argv'] or
            not deploy.same_process(broker, broker or {}) or str(control) not in broker['argv']):
        raise RuntimeError('protected producer/broker identity does not match this run')
    protected = {'control': str(control), 'run_id': cfg['run_id'], 'hosts': hosts,
                 'workers': workers, 'master': {'producer': producer, 'broker': broker}}
    if socket.gethostname() != hosts['master-0']:
        raise ValueError('prepare must run on the actual master')
    bundle = control / ('target-probe-policy-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    for name in SOURCES:
        shutil.copyfile(Path(__file__).resolve().parent / name, bundle / name)
    write_json(bundle / 'expected-roles.json', roles)
    write_json(bundle / 'protected-services.json', protected)
    write_json(bundle / 'probe-policy.json', policy)
    manifest = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in bundle.iterdir() if p.is_file()}
    write_json(bundle / 'source-sha256.json', manifest)
    digest = deploy.validate_sources(bundle)
    guard_node(control, bundle, 'master-0')
    return {'bundle': str(bundle), 'prepared_only': True, 'source_manifest_sha256': digest,
            'policy': policy, 'protected_workers': 24}


def run_node(control, bundle, node):
    control, bundle = Path(control).resolve(), Path(bundle).resolve()
    if bundle.parent != control or node not in NODES:
        raise ValueError('wrong bundle or physical node')
    deploy.validate_sources(bundle)
    guard_node(control, bundle, node)
    result = deploy.run_node(control, bundle, node)
    guard_node(control, bundle, node)
    result['protected_processes_unchanged'] = True
    return result


def tune(bundle, max_file_bytes):
    """Future updates use the already running background reloader, no restart."""
    bundle = Path(bundle).resolve()
    deploy.validate_sources(bundle)
    protected = read_json(bundle / 'protected-services.json')
    control = Path(protected['control'])
    cfg = read_json(control / 'config.json')
    if (bundle.parent != control or cfg.get('control') != str(control) or
            cfg.get('run_id') != protected['run_id'] or (control / 'STOP').exists()):
        raise ValueError('runtime policy has wrong control/run or STOP present')
    policy = validate_policy(dict(read_json(bundle / 'probe-policy.json'),
                                  max_file_bytes=max_file_bytes))
    write_json(bundle / 'roles' / 'probe-policy-runtime.json', policy)
    return {'policy': policy, 'published_only': True,
            'note': 'Verify live health policy/revision; publication alone is not acknowledgement.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--control', required=True)
    p.add_argument('--predecessor', required=True)
    p.add_argument('--producer-pid', type=int, required=True)
    p.add_argument('--max-file-bytes', type=int, default=1024 * 1024)
    n = sub.add_parser('node')
    n.add_argument('--control', required=True)
    n.add_argument('--bundle', required=True)
    n.add_argument('--node', choices=NODES, required=True)
    t = sub.add_parser('tune')
    t.add_argument('--bundle', required=True)
    t.add_argument('--max-file-bytes', type=int, required=True)
    args = parser.parse_args()
    if args.mode == 'prepare':
        value = prepare(args.control, args.predecessor, args.producer_pid, args.max_file_bytes)
    elif args.mode == 'node':
        value = run_node(args.control, args.bundle, args.node)
    else:
        value = tune(args.bundle, args.max_file_bytes)
    print(json.dumps(value), flush=True)


if __name__ == '__main__':
    main()
