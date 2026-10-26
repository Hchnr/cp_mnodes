"""Read-only12-node process evidence for P4's existing priorities1-3 run.

This binds42 registered identities from their saved deployment/authorization
proofs, then sends the observer as stdin memory to each existing SSH node.
No migration path is opened remotely, no PID is adopted/signalled and no role
is installed or launched. Even a quiescent result cannot authorize P4 dispatch.
"""
import argparse
import collections
import concurrent.futures
import hashlib
import json
import math
from pathlib import Path
import time

from migration_io import read_json, write_json
from profile_copy_proc_only import bind_worker_argv, normalized_argv, EXPECTED_NODES
import migration_launch as transport


BASE = Path(__file__).resolve().parent
FILES = {'rich': 'cache_age_canary_profile_20261021T2003_after.json',
         'argv': 'cache_age_canary_proc_20261021T2003_after.json',
         'authorization': 'cache_age_canary_identity_validation_20261021T2003.json',
         'recovery': 'overlap_resume_recovery.json', 'seed': 'overlap_seed_helper_identity.json',
         'services': 'distributed_target_services_deployment.json'}
MASTER = {'producer': (1824893, '500686447'), 'broker': (166974, '500376938'),
          'metrics': (166975, '500376939'), 'performance': (982825, '500522783')}


def build_inventory(active, rich, argv, authorization, recovery, seed, services):
    control, job = active['control'], active['job']
    workers = bind_worker_argv(rich, argv, control, job)
    if (authorization.get('control'), authorization.get('job')) != (control, job):
        raise ValueError('canary authorization belongs to another run')
    if (authorization.get('operation') != '7e9c628399b4439cb7af7e40045e9e23'
            or authorization.get('changed') != [['worker-5', 'primary', 1881537, '4105099']]
            or authorization.get('new_verified_baseline') != 'work/' + FILES['rich']
            or authorization.get('full_argv_verification', {}).get('path') != 'work/' + FILES['argv']
            or authorization.get('full_argv_verification', {}).get('errors') != []):
        raise ValueError('unexpected canary authorization/identity proof')
    checks = authorization['full_argv_verification'].get('checks')
    if not isinstance(checks, list) or len(checks) != 24:
        raise ValueError('authorization must cover exactly24 worker identities')
    covered = set()
    for check in checks:
        key = check.get('node'), check.get('lane')
        if key in covered or key[0] not in workers or key[1] not in workers[key[0]]:
            raise ValueError('duplicate or unknown authorized lane')
        want = workers[key[0]][key[1]]
        if (check.get('pid'), str(check.get('start_ticks'))) != (want['pid'], str(want['start_ticks'])):
            raise ValueError('authorization does not bind this worker PID/start')
        covered.add(key)
    result = {node: [] for node in EXPECTED_NODES}

    def add(node, role, record, ticks='start_ticks'):
        command = normalized_argv(record.get('argv'))
        pid, start = record.get('pid'), str(record.get(ticks, ''))
        if (type(pid) is not int or pid <= 0 or not start.isdigit() or int(start) <= 0
                or not any(arg == control or arg.startswith(control + '/') for arg in command)):
            raise ValueError('incomplete or out-of-scope deployment identity')
        result[node].append({'role': role, 'pid': pid, 'start_ticks': str(int(start)), 'argv': command})

    for node, lanes in workers.items():
        for lane, record in lanes.items():
            add(node, lane, record)
    masters = [node for node in argv['nodes'] if node['node'] == 'master-0']
    if len(masters) != 1:
        raise ValueError('exactly one master argv proof required')
    master = masters[0]
    for role, (pid, ticks) in MASTER.items():
        first, second = master['first']['roles'][role], master['second']['roles'][role]
        if any(record.get('identity_valid') is not True or record.get('pid') != pid
               or str(record.get('start_ticks')) != ticks for record in (first, second)):
            raise ValueError('master role identity differs from the registered proof')
        if normalized_argv(first['argv']) != normalized_argv(second['argv']):
            raise ValueError('master role argv changed within its saved proof')
        add('master-0', role, first)
    if (recovery.get('control'), recovery.get('job')) != (control, job):
        raise ValueError('watchdog deployment belongs to another run')
    watchdog = recovery['roles']['master-0:watchdog']
    if (watchdog.get('pid'), str(watchdog.get('startticks'))) != (167021, '500376945'):
        raise ValueError('unregistered watchdog identity')
    add('master-0', 'watchdog', watchdog, 'startticks')
    if (seed.get('pid'), str(seed.get('starttime_ticks'))) != (179557, '500401283'):
        raise ValueError('unregistered seed identity')
    add('master-0', 'seed', seed, 'starttime_ticks')
    if services.get('control') != control or set(services.get('nodes', {})) != EXPECTED_NODES:
        raise ValueError('metadata-probe deployment must cover this exact control and12 nodes')
    for node, deployment in services['nodes'].items():
        role = deployment['role']
        if role.get('control') != control or role.get('host') != 'job-' + job + '-' + node:
            raise ValueError('metadata probe belongs to another node/control')
        add(node, 'metadata_probe', role, 'starttime_ticks')
    if (result['worker-10'][-1]['pid'], result['worker-10'][-1]['start_ticks']) != (288589, '738952359'):
        raise ValueError('worker10 adopted daemon identity differs from the saved proof')
    for node, roles in result.items():
        if len(roles) != (9 if node == 'master-0' else 3):
            raise ValueError('incomplete registered role inventory')
        if len({value['pid'] for value in roles}) != len(roles) or len({value['role'] for value in roles}) != len(roles):
            raise ValueError('duplicate registered PID/role within a node')
    return result


def summarize_observations(inventory, observations, errors, control):
    complete = not errors and set(observations) == set(inventory)
    all_exited = True
    statuses = collections.Counter()
    for node, value in observations.items():
        if node not in inventory:
            complete = False
            continue
        times = [value.get(point, {}).get(key) for point in ('first', 'second')
                 for key in ('started_at', 'finished_at')]
        if (any(type(item) not in (int, float) or not math.isfinite(item) for item in times)
                or not times[0] <= times[1] < times[2] <= times[3]):
            complete = False
        for point in ('first', 'second'):
            observation = value.get(point, {})
            records = observation.get('registered', [])
            expected = {item['role']: item for item in inventory[node]}
            seen = set()
            if (observation.get('control') != control or observation.get('complete') is not True
                    or observation.get('errors') != [] or not isinstance(observation.get('live_associated'), list)):
                complete = False
            if observation.get('live_associated') != []:
                all_exited = False
            for record in records:
                role = record.get('role')
                if role not in expected or role in seen or any(record.get(key) != expected[role][key] for key in ('pid', 'start_ticks', 'argv')):
                    complete = False
                    all_exited = False
                    continue
                seen.add(role)
                current, status = record.get('observation'), record.get('status')
                exited = False
                if status == 'pid_absent':
                    valid, exited = current is None, True
                elif isinstance(current, dict) and current.get('pid') == record['pid']:
                    if status == 'pid_reused':
                        valid = (isinstance(current.get('start_ticks'), str)
                                 and current['start_ticks'].isdigit() and int(current['start_ticks']) > 0
                                 and current['start_ticks'] != record['start_ticks'])
                        exited = True
                    elif status == 'zombie_no_execution_or_fds':
                        valid = (current.get('start_ticks') == record['start_ticks']
                                 and current.get('state') == 'Z' and current.get('argv') == []
                                 and type(current.get('open_fd_count')) is int and current['open_fd_count'] == 0)
                        exited = True
                    elif status in ('running_exact', 'same_pid_start_argv_changed'):
                        valid = (current.get('start_ticks') == record['start_ticks']
                                 and current.get('state') in ('R', 'S', 'D', 'T', 't', 'I', 'W', 'P')
                                 and isinstance(current.get('argv'), list)
                                 and (current['argv'] == record['argv']) == (status == 'running_exact'))
                    else:
                        valid = False
                else:
                    valid = False
                if not valid or record.get('old_identity_exited') is not exited:
                    complete = False
                if not valid or not exited:
                    all_exited = False
            if seen != set(expected):
                complete = False
            if point == 'second':
                statuses.update(record.get('status', 'unknown') for record in records)
    quiescent = complete and all_exited and all(value[point].get('quiescent_snapshot') is True
                                 for value in observations.values() for point in ('first', 'second'))
    return {'full_cluster_process_observation_complete': complete,
            'registered_identities': sum(len(value) for value in inventory.values()),
            'latest_registered_status_counts': dict(statuses),
            'two_point_process_quiescence_observed': quiescent, 'launch_ready': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--interval', type=float, default=2)
    args = parser.parse_args()
    if not 0.1 <= args.interval <= 5:
        parser.error('interval must be0.1..5 seconds')
    active = read_json(BASE / 'active_migration.json')
    saved = {name: read_json(BASE / filename) for name, filename in FILES.items()}
    inventory = build_inventory(active, **saved)
    source = (BASE / 'migration_priority4_process_observation.py').read_text()
    transport.JOB = active['job']
    started, observations, errors = time.time(), {}, []

    def inspect(node):
        code = "import sys,json,time\nsys.dont_write_bytecode=True\nns={}\n"
        code += 'exec(compile(%r,"<readonly-P4-process-observer>","exec"),ns)\n' % source
        code += 'control=%r\nidentities=%r\n' % (active['control'], inventory[node])
        code += "first=ns['observe_node'](control,identities)\ntime.sleep(%r)\n" % args.interval
        code += "second=ns['observe_node'](control,identities)\nprint(json.dumps({'first':first,'second':second}))\n"
        return json.loads(transport.ssh(node, code, owner=active['ssh_owner'])[1])

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(inspect, node): node for node in inventory}
        for future in concurrent.futures.as_completed(futures):
            node = futures[future]
            try:
                observations[node] = future.result()
            except Exception as exc:
                errors.append({'node': node, 'error': type(exc).__name__ + ': ' + str(exc)})
    summary = summarize_observations(inventory, observations, errors, active['control'])
    result = {**summary, 'control': active['control'], 'job': active['job'],
              'started_at': started, 'finished_at': time.time(), 'interval_requested_seconds': args.interval,
              'sources': FILES, 'observer_source_sha256': hashlib.sha256(source.encode()).hexdigest(),
              'inventory': inventory, 'inventory_sha256': hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest(),
              'observations': observations, 'errors': errors,
              'migration_storage_accessed_remotely': False, 'production_code_deployed': False,
              'runtime_mutation_performed': False, 'new_pid_adoption_performed': False,
              'limitations': 'Two non-atomic process snapshots only. New same-control live roles block quiescence. Terminal/scope/STOP, queues/leases, issues/manifests/receipts/metadata, fresh training protection and actual local dispatch/coordinator locks still require independent acceptance. No launch, signals or cleanup are implemented.'}
    write_json(args.output, result)
    print(json.dumps({'saved': str(args.output), **summary, 'errors': errors,
                      'runtime_mutation_performed': False}))
    if not summary['full_cluster_process_observation_complete']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
