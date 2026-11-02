#!/usr/bin/env python3
"""Operator-triggered join: only the active runtime-enabled shared queue is used."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import uuid

import rm24
import tailboost


def request_join(run, node, timeout=90):
    run = Path(run).resolve()
    cfg = rm24.load(run/'config.json')
    if (cfg.get('target') != rm24.TARGET or cfg.get('root_inode') != rm24.ROOT_INODE
            or cfg.get('supplement_of') != str(rm24.RUN.resolve())
            or cfg.get('runtime_join') is not True or node not in rm24.NODES
            or type(cfg.get('per_node')) is not int or not 1 <= cfg['per_node'] <= 8):
        raise ValueError('join requires the bound runtime-enabled supplement and an allowed node')
    if hashlib.sha256((run/'rm24.py').read_bytes()).hexdigest() != cfg['sha256']:
        raise ValueError('frozen worker code changed')
    status = rm24.load(run/'status.json')
    if status['state'] != 'rm_running' or time.time()-status['updated'] > 15 or (run/'STOP').exists():
        raise ValueError('supplement is not freshly accepting work')
    actor = rm24.load(run/'controller.json')
    try:
        matches = rm24.identity(actor['pid']) == actor
    except FileNotFoundError:
        matches = False
    if not matches:
        raise ValueError('controller identity changed or exited')
    nonce = uuid.uuid4().hex
    requests = run/'runtime-requests'
    acknowledgements = run/'runtime-acks'
    requests.mkdir(exist_ok=True)
    acknowledgements.mkdir(exist_ok=True)
    request = dict(version=1, nonce=nonce, run=str(run), controller=actor,
                   config_sha256=hashlib.sha256((run/'config.json').read_bytes()).hexdigest(),
                   node=node, per_node=cfg['per_node'])
    rm24.atomic(requests/(nonce+'.json'), request)
    print(json.dumps(dict(requested=node, nonce=nonce, run=str(run))), flush=True)
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        try:
            result = rm24.load(acknowledgements/(nonce+'.json'))
        except (FileNotFoundError, json.JSONDecodeError):
            result = None
        if result is not None:
            if (not isinstance(result, dict) or type(result.get('version', 1)) is not int
                    or result.get('version', 1) != 1
                    or result.get('node') != node or result.get('nonce') != nonce):
                raise ValueError('join acknowledgement identity differs')
            for key in ('run', 'controller', 'config_sha256', 'per_node'):
                if key in result and result[key] != request[key]:
                    raise ValueError('join acknowledgement binding differs: '+key)
            if result.get('state') not in ('accepted', 'already_active', 'rejected'):
                raise ValueError('invalid join acknowledgement state')
            if result['state'] not in ('accepted', 'already_active'):
                raise RuntimeError('join rejected; existing members remain unchanged: '+json.dumps(result))
            try:
                matches = rm24.identity(actor['pid']) == actor
            except FileNotFoundError:
                matches = False
            if not matches or (run/'STOP').exists():
                raise RuntimeError('controller changed/stopped after acknowledgement; inspect '+str(run))
            print(json.dumps(result), flush=True)
            return result
        try:
            matches = rm24.identity(actor['pid']) == actor
        except FileNotFoundError:
            matches = False
        if not matches or (run/'STOP').exists():
            raise RuntimeError('controller changed/stopped while awaiting join; inspect '+str(run))
        time.sleep(1)
    raise RuntimeError('join acknowledgement timed out; do not assume rejected or submit again; inspect '+str(run))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('node', choices=rm24.NODES)
    args = parser.parse_args()
    current = rm24.load(tailboost.POINTER)
    request_join(Path(current['run']), args.node)


if __name__ == '__main__':
    main()
