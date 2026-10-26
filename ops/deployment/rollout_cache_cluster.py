#!/usr/bin/env python3
"""Sequential protocol-2 cache-age rollout over existing SSH aliases.

Only an idle lane can be replaced, using rollout_cache_age.roll_one unchanged.
Each lane owns one master dispatch hold. An ambiguous operation stops the pass;
there is no mutation retry and no forced termination of a remote helper.
"""
import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import shlex
import subprocess
import time
import uuid

from migration_io import read_json, write_json
import rollout_cache_age as protocol


DEFAULT_POINTER = Path('/share-mixed/project/.hcr-copy-monitor/active.json')
NODES = ['master-0'] + ['worker-' + str(i) for i in range(11)]
LANES = ['primary', 'supplement']


def complete_identity(identity):
    return (isinstance(identity, dict) and type(identity.get('pid')) is int
            and identity['pid'] > 0
            and str(identity.get('starttime_ticks', '')).isdigit()
            and isinstance(identity.get('argv'), list) and bool(identity['argv'])
            and all(isinstance(value, str) for value in identity['argv']))


def target_age(identity):
    """Inspect actual process arguments, never a prior rollout checkpoint."""
    if not complete_identity(identity):
        return False
    argv = identity['argv']
    values = []
    for index, value in enumerate(argv):
        if value == '--cache-max-age':
            if index + 1 >= len(argv):
                return False
            values.append(argv[index + 1])
        elif value.startswith('--cache-max-age='):
            values.append(value.partition('=')[2])
    try:
        return len(values) == 1 and float(values[0]) == 30.0
    except (TypeError, ValueError):
        return False


def source_versions():
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (Path(__file__), Path(protocol.__file__))}


HEALTH_SCRIPT = r'''
import datetime,hashlib,json,pathlib,socket,sys,time
helper=pathlib.Path(HELPER);sys.dont_write_bytecode=True;sys.path.insert(0,str(helper.parent))
from migration_io import read_json
c=pathlib.Path(CONTROL)
n=read_json(c/('nodes' if LANE=='primary' else 'nodes-extra')/(HOST+'.json'))
p=pathlib.Path('/proc')/str(EXPECTED['pid'])
fields=(p/'stat').read_text().rsplit(') ',1)[1].split()
identity={'pid':EXPECTED['pid'],'starttime_ticks':fields[19],
 'argv':(p/'cmdline').read_bytes().decode(errors='surrogateescape').strip('\0').split('\0')}
age=time.time()-datetime.datetime.fromisoformat(n['updated'].replace('Z','+00:00')).timestamp()
healthy=(socket.gethostname()==HOST
 and all(identity[key]==EXPECTED[key] for key in ('pid','starttime_ticks','argv'))
 and n.get('pid')==EXPECTED['pid']
 and fields[0] not in ('Z','X') and not n.get('ended') and not n.get('thread_error')
 and n.get('threads')==64 and 0<=age<=10 and not (c/'STOP').exists())
print('RESULT '+json.dumps({'healthy':healthy,'identity':identity,'heartbeat_age_seconds':age,
 'helper_sha256':hashlib.sha256(helper.read_bytes()).hexdigest()}))
'''


class BridgeRPC:
    def __init__(self, *, control, job, node, lane, remote_script, ssh_prefix,
                 output, operation, gate_timeout=5, max_age=30, control_timeout=10):
        self.control, self.job, self.node, self.lane = control, job, node, lane
        self.host = 'job-' + job + '-' + node
        self.remote_script, self.ssh_prefix = remote_script, ssh_prefix
        self.operation, self.gate_timeout, self.max_age = operation, gate_timeout, max_age
        self.control_timeout = control_timeout
        self.runtime = Path(output).with_name(Path(output).name + '.runtime')

    def ssh_command(self, node, remote_command):
        if node not in NODES:
            raise ValueError('unknown physical node')
        return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
                '-o', 'ServerAliveInterval=15', self.ssh_prefix + node, remote_command]

    def command(self, node, action, identity=None):
        args = ['python3', '-u', self.remote_script, '--action', action,
                '--control', self.control, '--max-age', str(float(self.max_age)),
                '--gate-timeout', str(float(self.gate_timeout)),
                '--control-timeout', str(float(self.control_timeout)), '--operation', self.operation]
        if action != 'hold':
            args.extend(['--host', self.host, '--lane', self.lane])
        if identity is not None:
            args.extend(['--identity-json', json.dumps(identity)])
        return self.ssh_command(node, shlex.join(args))

    def diagnostics(self, node, action, stdout='', stderr='', error=None):
        self.runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
        def text(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else (value or '')
        path = self.runtime / (self.operation + '-' + node + '-' + action + '-' + uuid.uuid4().hex + '.json')
        actual = write_json(path, {'operation': self.operation, 'node': node,
                                  'host': self.host, 'lane': self.lane, 'action': action,
                                  'stdout': text(stdout), 'stderr': text(stderr), 'error': error,
                                  'at': time.time()})
        actual.chmod(0o600)
        return path

    def execute(self, node, action, command, timeout):
        try:
            result = subprocess.run(command, stdin=subprocess.DEVNULL, text=True,
                                    capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            path = self.diagnostics(node, action, exc.stdout, exc.stderr, 'timeout')
            raise RuntimeError(action + ' RPC timed out; inspect ' + str(path)) from exc
        try:
            values = [json.loads(line[7:]) for line in result.stdout.splitlines()
                      if line.startswith('RESULT ')]
            if result.returncode or not values or not isinstance(values[-1], dict):
                raise ValueError('nonzero exit or missing RESULT object')
        except (ValueError, TypeError) as exc:
            path = self.diagnostics(node, action, result.stdout, result.stderr, str(exc))
            raise RuntimeError(action + ' RPC failed; inspect ' + str(path)) from exc
        return values[-1]

    def __call__(self, node, action, *, lane=None, identity=None):
        if lane is not None and lane != self.lane:
            raise ValueError('lane changed within one operation')
        timeout = (self.control_timeout + 90 if action == 'replace' else
                   self.gate_timeout + self.control_timeout + 20 if action == 'gate' else
                   self.control_timeout + 20)
        return self.execute(node, action, self.command(node, action, identity), timeout)

    def health(self, identity):
        values = {'HELPER': self.remote_script, 'CONTROL': self.control,
                  'HOST': self.host, 'LANE': self.lane, 'EXPECTED': identity}
        code = '\n'.join(key + '=' + repr(value) for key, value in values.items()) + '\n' + HEALTH_SCRIPT
        command = self.ssh_command(self.node, shlex.join(['python3', '-c', code]))
        return self.execute(self.node, 'health', command, self.control_timeout + 20)

    @contextmanager
    def hold(self):
        process = subprocess.Popen(self.command('master-0', 'hold'), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line, failure = '', None
        try:
            ready, _, _ = select.select([process.stdout], [], [], 30)
            if not ready:
                raise RuntimeError('master dispatch hold observation timed out')
            line = process.stdout.readline()
            try:
                value = json.loads(line[7:]) if line.startswith('RESULT ') else {}
            except ValueError:
                value = {}
            if value.get('locked') is not True:
                raise RuntimeError('master dispatch hold was not acquired')
            yield value
        except BaseException as exc:
            failure = exc
            raise
        finally:
            if process.stdin:
                process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired as exc:
                path = self.diagnostics('master-0', 'hold', error='hold exit not confirmed; no signal sent')
                raise RuntimeError('master dispatch hold exit not confirmed; inspect ' + str(path)) from exc
            stdout, stderr = line + process.stdout.read(), process.stderr.read()
            if process.returncode or failure is not None:
                path = self.diagnostics('master-0', 'hold', stdout, stderr,
                                        str(failure) if failure is not None else 'hold exited nonzero')
            if process.returncode and failure is None:
                raise RuntimeError('master dispatch hold exited nonzero; inspect ' + str(path))
            process.stdout.close()
            process.stderr.close()


def run_cluster(*, control, job, remote_script, output, nodes=None, lanes=None,
                ssh_prefix='copy-', rounds=1, gate_timeout=5, max_age=30, visibility_timeout=0,
                control_timeout=10,
                rpc_factory=BridgeRPC):
    nodes, lanes = list(NODES if nodes is None else nodes), list(LANES if lanes is None else lanes)
    if (not nodes or len(set(nodes)) != len(nodes) or any(node not in NODES for node in nodes)
            or not lanes or len(set(lanes)) != len(lanes) or any(lane not in LANES for lane in lanes)):
        raise ValueError('select distinct known nodes and lanes')
    if type(rounds) is not int or rounds < 1 or max_age != 30 or not 0 < gate_timeout <= 30:
        raise ValueError('require positive rounds, max-age=30 and 0<gate-timeout<=30')
    if not math.isfinite(visibility_timeout) or visibility_timeout < 0:
        raise ValueError('visibility timeout must be finite and nonnegative')
    if not math.isfinite(control_timeout) or not 0 < control_timeout <= 60:
        raise ValueError('control timeout must be finite and 0<control-timeout<=60')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', ssh_prefix):
        raise ValueError('SSH prefix must form an alias, not SSH options')
    if not isinstance(job, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', job):
        raise ValueError('missing or invalid job identity')
    if not Path(control).is_absolute() or not Path(remote_script).is_absolute():
        raise ValueError('control and remote helper must be absolute paths')
    record = {'schema_version': 1, 'control': str(control), 'job': job,
              'remote_script': str(remote_script), 'ssh_prefix': ssh_prefix,
              'max_age': max_age, 'gate_timeout': gate_timeout,
              'visibility_timeout': visibility_timeout, 'control_timeout': control_timeout, 'rounds': rounds,
              'versions': source_versions(), 'started': time.time(), 'status': 'running',
              'entries': [{'node': node, 'lane': lane, 'status': 'pending', 'attempts': []}
                          for node in nodes for lane in lanes]}
    def save():
        record['updated'] = time.time()
        record['summary'] = dict(Counter(entry['status'] for entry in record['entries']))
        write_json(output, record)
    save()
    try:
        for current_round in range(1, rounds + 1):
            record['round'] = current_round
            for entry in record['entries']:
                if entry['status'] not in ('pending', 'waiting_payload'):
                    continue
                operation = uuid.uuid4().hex
                attempt = {'operation': operation, 'round': current_round,
                           'started': time.time(), 'status': 'acquiring_dispatch'}
                entry['attempts'].append(attempt)
                entry['status'] = 'running'
                save()
                rpc = rpc_factory(control=str(control), job=job, node=entry['node'],
                                  lane=entry['lane'], remote_script=str(remote_script),
                                  ssh_prefix=ssh_prefix, output=output, operation=operation,
                                  gate_timeout=gate_timeout, max_age=max_age, control_timeout=control_timeout)
                with rpc.hold() as held:
                    attempt['dispatch_hold'] = held
                    attempt['status'] = 'inspecting'
                    save()
                    before = rpc(entry['node'], 'inspect', lane=entry['lane'])
                    attempt['before'] = before
                    if not complete_identity(before.get('identity')):
                        raise RuntimeError('worker identity is incomplete')
                    health = rpc.health(before['identity'])
                    attempt['worker_health'] = health
                    if health.get('helper_sha256') != record['versions'][Path(protocol.__file__).name]:
                        raise RuntimeError('deployed helper differs from the local protocol-2 controller')
                    if (health.get('healthy') is not True or
                            not protocol.same_identity(health.get('identity', {}), before['identity'])):
                        raise RuntimeError('worker health or identity was not confirmed')
                    if before.get('checkpoint_protocol') == 2 and target_age(before['identity']):
                        result = {'status': 'at_target', 'identity': before['identity'], 'health': health}
                    else:
                        attempt['status'] = 'rolling'
                        save()
                        result = protocol.roll_one(rpc, entry['node'], entry['lane'],
                                                   gate_timeout=gate_timeout, max_age=max_age,
                                                   visibility_timeout=visibility_timeout)
                    attempt.update(status=result['status'], result=result, finished=time.time())
                    entry['status'] = result['status']
                    save()
                attempt['dispatch_hold_released'] = time.time()
                save()
            if not any(entry['status'] == 'waiting_payload' for entry in record['entries']):
                break
        record.update(status='pass_finished', finished=time.time())
        save()
        return record
    except BaseException as exc:
        if 'attempt' in locals():
            attempt.update(status='attention', error=type(exc).__name__ + ': ' + str(exc),
                           finished=time.time())
            entry['status'] = 'attention'
        record.update(status='attention', error=type(exc).__name__ + ': ' + str(exc), finished=time.time())
        save()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', help='defaults to the stable active.json pointer')
    parser.add_argument('--remote-script', required=True)
    parser.add_argument('--nodes', nargs='+', choices=NODES, default=NODES)
    parser.add_argument('--lanes', nargs='+', choices=LANES, default=LANES)
    parser.add_argument('--ssh-prefix', default='copy-')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--rounds', type=int, default=1)
    parser.add_argument('--gate-timeout', type=float, default=5)
    parser.add_argument('--visibility-timeout', type=float, default=0)
    parser.add_argument('--control-timeout', type=float, default=10)
    parser.add_argument('--max-age', type=float, default=30)
    args = parser.parse_args(argv)
    control = args.control or read_json(DEFAULT_POINTER)['control']
    cfg = read_json(Path(control) / 'config.json')
    previous_umask = os.umask(0o077)
    try:
        record = run_cluster(control=control, job=cfg['job'], remote_script=args.remote_script,
                             nodes=args.nodes, lanes=args.lanes, ssh_prefix=args.ssh_prefix,
                             output=args.output, rounds=args.rounds, gate_timeout=args.gate_timeout,
                             max_age=args.max_age, visibility_timeout=args.visibility_timeout,
                             control_timeout=args.control_timeout)
    finally:
        os.umask(previous_umask)
    print(json.dumps({'output': str(args.output), 'status': record['status'],
                      'summary': record['summary']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
