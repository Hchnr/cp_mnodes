#!/usr/bin/env python3
"""Bounded, explicit multi-node native-worker rollout using existing guards.

One real master dispatch hold covers the entire pass. Each physical node still
runs the existing sequential two-lane controller. Only the complete drain and
release RPCs are serialized; no FD, lease, identity, counter or replacement
guard is weakened. An ambiguous RPC stops new nodes/lanes, never retries replace,
and lets already-active transactions perform their existing cleanup.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import threading
import time
import uuid

import deploy_directory_fastpath as deploy
from migration_io import read_json, write_json


_RUN_LOCK = threading.Lock()


class StoppedBeforeLane(RuntimeError):
    """No remote action occurred for this lane after another node failed."""


def run(control, bundle, nodes, concurrency=3, lanes=None):
    nodes = list(nodes)
    lanes = list(deploy.LANES if lanes is None else lanes)
    if (not nodes or len(set(nodes)) != len(nodes) or any(n not in deploy.NODES for n in nodes)
            or not lanes or len(set(lanes)) != len(lanes)
            or any(lane not in deploy.LANES for lane in lanes)
            or type(concurrency) is not int or not 1 <= concurrency <= 3):
        raise ValueError('require distinct known nodes/lanes and concurrency within 1..3')
    if not _RUN_LOCK.acquire(blocking=False):
        raise RuntimeError('another runner is already using the temporary controller binding')
    original_rpc = deploy.DirectoryRPC
    try:
        control, bundle = Path(control).resolve(), Path(bundle).resolve()
        cfg, manifest = deploy.verify_bundle(control, bundle)
        if 'worker-directory-policy.json' not in manifest:
            raise ValueError('parallel runner requires an explicitly verified native bundle')
        state = read_json(control / 'state.json')
        if (control / 'STOP').exists() or state.get('status') in deploy.protocol.TERMINAL:
            raise RuntimeError('STOP or terminal state')
        operation = uuid.uuid4().hex
        directory = control / ('native-parallel-rollout-' + operation)
        directory.mkdir(mode=0o700)
        output = directory / 'group.json'
        record = {'control': str(control), 'bundle': str(bundle), 'operation': operation,
                  'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  'source_sha256': manifest, 'concurrency': concurrency, 'nodes': nodes,
                  'lanes': lanes, 'started': time.time(), 'status': 'starting',
                  'shared_master_hold': True, 'entries': [], 'ambiguities': []}
        state_lock = threading.Lock()
        control_lock = threading.Lock()
        stop_new = threading.Event()
        next_index = 0

        def save():
            record['updated'] = time.time()
            write_json(output, record)

        def ambiguous(node, where, exc):
            with state_lock:
                stop_new.set()
                record['ambiguities'].append({'node': node, 'where': where,
                    'error': type(exc).__name__ + ': ' + str(exc), 'at': time.time()})
                save()

        class SharedHoldRPC(original_rpc):
            def __init__(self, **kwargs):
                if stop_new.is_set():
                    raise StoppedBeforeLane('another node needs inspection; no new lane started')
                super().__init__(**kwargs)
                self.shared_master_hold = operation

            @contextmanager
            def hold(self):
                # The outer real hold remains owned and live until all threads
                # finish. Do not acquire/release a per-lane master dispatch lock.
                yield {'locked': True, 'shared_hold': True, 'operation': operation}

            def __call__(self, node, action, *, lane=None, identity=None):
                try:
                    if action in ('drain', 'release'):
                        # Serialize the entire remote read-modify-write exchange,
                        # not just one local request or one half of the exchange.
                        with control_lock:
                            return super().__call__(node, action, lane=lane, identity=identity)
                    return super().__call__(node, action, lane=lane, identity=identity)
                except BaseException as exc:
                    ambiguous(self.node, action, exc)
                    raise

        def worker():
            nonlocal next_index
            while True:
                with state_lock:
                    if stop_new.is_set() or next_index == len(nodes):
                        return
                    node = nodes[next_index]
                    next_index += 1
                    entry = {'node': node, 'started': time.time(), 'status': 'running',
                             'shared_master_hold': operation}
                    record['entries'].append(entry)
                    save()
                try:
                    result = deploy.rollout(control, bundle, [node], lanes)
                    if result.get('status') not in ('complete', 'partial_busy'):
                        raise RuntimeError('unexpected node rollout result; inspect before retry')
                    with state_lock:
                        entry.update(status=result['status'], outcome=result, finished=time.time())
                        save()
                except StoppedBeforeLane as exc:
                    with state_lock:
                        entry.update(status='stopped_before_next_lane', error=str(exc),
                                     finished=time.time())
                        save()
                    return
                except BaseException as exc:
                    ambiguous(node, 'node_controller', exc)
                    with state_lock:
                        entry.update(status='inspect_before_retry',
                                     error=type(exc).__name__ + ': ' + str(exc),
                                     finished=time.time())
                        save()
                    return

        outer = original_rpc(control=str(control), job=cfg['job'], node=nodes[0], lane=lanes[0],
                             remote_script=str(bundle / 'rollout_cache_age.py'), ssh_prefix='copy-',
                             output=str(output), operation=operation, gate_timeout=30,
                             max_age=30, control_timeout=30)
        save()
        try:
            with outer.hold():
                record.update(status='running', hold_acquired=time.time())
                save()
                deploy.DirectoryRPC = SharedHoldRPC
                with ThreadPoolExecutor(max_workers=min(concurrency, len(nodes)),
                                        thread_name_prefix='native-node-rollout') as pool:
                    futures = [pool.submit(worker) for _ in range(min(concurrency, len(nodes)))]
                    for future in futures:
                        future.result()
            record['hold_released'] = time.time()
        except BaseException as exc:
            ambiguous('master-0', 'global_hold_or_runner', exc)
            raise
        finally:
            deploy.DirectoryRPC = original_rpc
            with state_lock:
                record['not_started'] = nodes[next_index:]
                record['status'] = ('inspect_before_retry' if stop_new.is_set() else
                                    'partial_busy' if any(e['status'] == 'partial_busy'
                                                          for e in record['entries']) else 'complete')
                record['finished'] = time.time()
                save()
        if stop_new.is_set():
            raise RuntimeError('parallel rollout stopped; inspect ' + str(output))
        return {'status': record['status'], 'output': str(output),
                'nodes_complete': sum(e['status'] == 'complete' for e in record['entries']),
                'not_started': record['not_started']}
    finally:
        deploy.DirectoryRPC = original_rpc
        _RUN_LOCK.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--bundle', required=True)
    parser.add_argument('--nodes', required=True, nargs='+', choices=deploy.NODES)
    parser.add_argument('--concurrency', type=int, choices=(1, 2, 3), default=3)
    args = parser.parse_args()
    print(json.dumps(run(args.control, args.bundle, args.nodes, args.concurrency)), flush=True)


if __name__ == '__main__':
    main()
