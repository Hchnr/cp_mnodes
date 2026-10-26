#!/usr/bin/env python3
"""Explicit one-lane P4 canary, reusing the tested protocol-2 transaction.

Only worker-10 supplement may be replaced. No copy engine, thread count, rate,
producer or broker changes. Each remote action runs in an immutable controller
bundle. An ambiguous replacement is inspected, never automatically repeated.
"""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib
import json
import os
from pathlib import Path
import resource
import shlex
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import time
import urllib.request
import uuid

from migration_io import read_json, write_json
import rollout_cache_age as protocol
from rollout_cache_cluster import BridgeRPC


CONTROL = Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053')
BUNDLE = CONTROL / 'p4-copy-path-0f78c471bb9e4a99afdd2260b4ac4fae'
FAST = CONTROL / 'fast-p4-source-b3163c438f2248a4a95372fb74e47841'
AUTHORIZATION = '允许，先做一个 P4 复制进程的滚动对照'
SCOPE = 'priority4-native-parent-open-checkpoint-v1'
JOB = '2dadf47a-b549-47bb-a267-d56dc877aec9'
HOST = 'job-' + JOB + '-worker-10'
FILES = ('deploy_priority4_path_canary.py', 'rollout_cache_age.py',
         'rollout_cache_cluster.py', 'migration_io.py')
CHECKPOINT = BUNDLE / 'cache-age-rollout' / (HOST + '-supplement.json')
CONTROLLER_ENTRY = 'deploy_priority4_path_canary.py'
CANDIDATE_ENTRY = 'migration_priority4_path_worker.py'
CANDIDATE_MODULE = 'migration_priority4_path_worker'
DIRECTORY_STRATEGY = 'native_readonly'
PREDECESSOR_LOG = CONTROL / 'logs' / (HOST + '-supplement.log')


def native_identity(value):
    return dict(pid=value['pid'], starttime_ticks=str(value.get('starttime_ticks', value.get('start_ticks'))),
                argv=value['argv'])


def require_actor(expected):
    current = protocol.proc_identity(expected['pid'])
    if current['state'] in ('Z', 'X') or not protocol.same_identity(current, native_identity(expected)):
        raise RuntimeError('protected native identity changed: ' + str(expected['pid']))
    return current


def freeze_verify(directory):
    manifest = json.loads((directory / 'source-sha256.json').read_bytes())
    for name, digest in manifest.items():
        if (Path(name).name != name or (directory / name).is_symlink()
                or hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest):
            raise RuntimeError('frozen controller/candidate source differs')
    return manifest


def validate_audit(value, now=None):
    now = time.time() if now is None else now
    nodes = value.get('nodes', [])
    if (value.get('all_unchanged') is not True or value.get('signals_sent') != 0
            or value.get('copy_process_count') != 48 or len(nodes) != 12
            or {x['node'] for x in nodes} != {'master-0'} | {'worker-' + str(i) for i in range(11)}
            or not 0 <= now - value['verified_at'] <= 90
            or any(not 0 <= now - x['verified_at'] <= 90 for x in nodes)):
        raise RuntimeError('fresh complete 48-copy native audit required')


def predecessor_done(expected):
    try:
        current = protocol.proc_identity(expected['pid'])
    except (FileNotFoundError, ProcessLookupError):
        return True
    if str(current['starttime_ticks']) != str(expected['starttime_ticks']):
        raise RuntimeError('predecessor PID was reused; inspect, never signal it')
    return current['state'] in ('Z', 'X')


def child_command(checkpoint, operation, python):
    return [python, '-u', str(BUNDLE / CANDIDATE_ENTRY), 'supplement',
            '--control', str(CONTROL), '--threads', '64', '--nice', '0', '--cache-max-age', '30.0',
            '--progress-checkpoint', str(checkpoint), '--progress-operation', operation]


def verify_rollout_policy(cfg, target):
    """The second entry binds configuration/identity before any host drain."""
    if CANDIDATE_MODULE == 'migration_priority4_path_worker':
        return
    if CANDIDATE_MODULE != 'migration_priority4_nearest_worker':
        raise RuntimeError('unknown canary entry; no rollout')
    path = BUNDLE / 'nearest-parent-policy.json'
    if path.is_symlink() or (path.parent / '.snapshots' / path.name).exists():
        raise RuntimeError('immutable second-canary policy required')
    policy = json.loads(path.read_bytes())
    digest = hashlib.sha256(json.dumps(cfg, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if (policy.get('scope') != SCOPE or policy.get('directory_strategy') != DIRECTORY_STRATEGY
            or policy.get('control') != str(CONTROL) or policy.get('node') != 'worker-10'
            or policy.get('lane') != 'supplement' or policy.get('predecessor') != target
            or policy.get('config_sha256') != digest
            or policy.get('predecessor_stdout_log') != str(PREDECESSOR_LOG)):
        raise RuntimeError('second-canary identity/configuration/log policy differs; no rollout')


def validate_candidate_checkpoint(checkpoint, operation):
    if CANDIDATE_MODULE not in ('migration_priority4_path_worker', 'migration_priority4_nearest_worker'):
        raise RuntimeError('unknown canary entry; no replacement')
    candidate = importlib.import_module(CANDIDATE_MODULE)
    _, policy = candidate.validate_bundle(CONTROL, BUNDLE, 'supplement')
    args = (CONTROL, BUNDLE, checkpoint, operation, 'supplement', read_json)
    if CANDIDATE_MODULE == 'migration_priority4_nearest_worker':
        args += (policy,)
    return candidate.checkpoint_identity(*args)


def previous_aborted_before_signal(record, target):
    return (record.get('status') == 'waiting_payload' and record.get('signals_sent') == 0
            and not record.get('replacement_pid') and not record.get('helper')
            and protocol.same_identity(record.get('expected', {}), target))


def live_data_fds(pid, roots, log):
    """Exclude only the two exact append-only, inode-matching stdout logs.

    Logs live below the destination mount but are reserved control artifacts,
    not copy payload. Unknown FDs (including other control FDs) remain blockers.
    """
    result = []
    for item in protocol.payload_fds(pid, roots):
        excluded = False
        if item['fd'] in ('1', '2') and item['path'] == str(log):
            try:
                info = Path('/proc') / str(pid) / 'fdinfo' / item['fd']
                flags = int(next(line.split()[1] for line in info.read_text().splitlines()
                                 if line.startswith('flags:')), 8)
                actual = os.stat(Path('/proc') / str(pid) / 'fd' / item['fd'])
                named = Path(log).lstat()
                excluded = (stat.S_ISREG(named.st_mode) and (actual.st_dev, actual.st_ino) ==
                    (named.st_dev, named.st_ino) and flags & os.O_ACCMODE == os.O_WRONLY
                    and bool(flags & os.O_APPEND))
            except (OSError, StopIteration, ValueError):
                pass
        if not excluded:
            result.append(item)
    return result


class Remote:
    def __init__(self, directory, operation):
        self.directory, self.operation = directory, operation
        freeze_verify(directory)
        freeze_verify(BUNDLE)
        self.cfg = read_json(CONTROL / 'config.json')
        if (self.cfg['job'] != JOB or self.cfg['migration_scope'] != 'project_priority4'
                or (self.cfg['source'], self.cfg['destination'], self.cfg['exclude']) !=
                   ('/share/project', '/share-mixed/project', ['eai_pwm'])):
            raise RuntimeError('exact P4 control/scope required')
        auth = read_json(directory / 'authorization.json')
        if auth != dict(operation=operation, user_authorization=AUTHORIZATION, control=str(CONTROL),
                        bundle=str(BUNDLE), node='worker-10', lane='supplement', signal_limit=1):
            raise RuntimeError('explicit scope-bound canary authorization required')
        host = socket.gethostname()
        if host not in ('job-' + JOB + '-master-0', HOST):
            raise RuntimeError('actual bound cluster master or canary worker required')
        self.host = host
        self.baseline = read_json(BUNDLE / 'preparation-after.json')
        self.target = native_identity(self.baseline['target'])
        verify_rollout_policy(self.cfg, self.target)
        self.node_path = CONTROL / 'nodes-extra' / (HOST + '.json')
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def active(self):
        state = read_json(CONTROL / 'state.json')
        if (CONTROL / 'STOP').exists() or (state.get('status'), state.get('stage')) != ('scanning', 'p4-r1'):
            raise RuntimeError('STOP or P4 stage changed; no replacement')

    def protected(self):
        if self.host == HOST:
            for actor in self.baseline['actors']:
                if actor['pid'] != self.target['pid']:
                    require_actor(actor)
        else:
            intent = read_json(FAST / 'intent.json')
            for actor in intent['preserved_actors']:
                require_actor(actor)
            require_actor(read_json(FAST / 'producer-role.json'))

    def master(self):
        if self.host != 'job-' + JOB + '-master-0':
            raise RuntimeError('broker authority is master-only')
        self.protected()

    def worker(self):
        if self.host != HOST:
            raise RuntimeError('replacement is worker-10-only')
        self.protected()

    def broker(self, payload=None):
        req = urllib.request.Request(self.cfg['broker_url'] + '/control',
            data=None if payload is None else json.dumps(payload).encode(),
            headers={'Authorization': 'Bearer ' + self.cfg['broker_token'], 'Content-Type': 'application/json'},
            method='GET' if payload is None else 'POST')
        with self.opener.open(req, timeout=15) as response:
            value = json.load(response)
        if value.get('ok') is not True:
            raise RuntimeError('broker control was not acknowledged')
        return value

    def record(self):
        value = read_json(CHECKPOINT)
        if value['operation'] != self.operation:
            raise RuntimeError('another operation owns the canary checkpoint')
        return value

    def save(self, value):
        value['updated'] = time.time()
        write_json(CHECKPOINT, value)

    def inspect(self):
        self.worker()
        n = read_json(self.node_path)
        try:
            record = read_json(CHECKPOINT)
        except FileNotFoundError:
            record = {}
        if record and record.get('operation') != self.operation:
            if not previous_aborted_before_signal(record, self.target):
                raise RuntimeError('another operation has an unproven replacement outcome')
            record = {}
        expected = record.get('replacement') if n['pid'] != self.target['pid'] else self.target
        if expected is None:
            raise RuntimeError('unrecorded worker PID; inspect before any action')
        actual = require_actor(expected)
        ready = bool(record.get('status') in ('workers_ready', 'complete')
                     and record.get('replacement') and n['pid'] == record['replacement']['pid'])
        if ready:
            protocol.counter_check(record['idle'], n)
        log = (PREDECESSOR_LOG if n['pid'] == self.target['pid']
               else self.directory / 'worker.log')
        return dict(identity=actual, active_count=len(n.get('active', {})),
            payload_fd_count=len(live_data_fds(n['pid'], (self.cfg['source'], self.cfg['destination']), log)),
            record_status=record.get('status'), record_expected=record.get('expected'),
            record_operation=record.get('operation'), checkpoint_protocol=2,
            replacement_ready=ready)

    def drain(self, audit):
        self.master(); self.active()
        validate_audit(read_json(audit))
        if CHECKPOINT.exists() or (CHECKPOINT.parent / '.snapshots' / CHECKPOINT.name).exists():
            previous = read_json(CHECKPOINT)
            # Re-gating an aborted pre-signal observation is not reissuing a
            # replacement: native inspection must still prove the ORIGINAL
            # PID/start/argv alive, and no launch/signal may have occurred.
            if not previous_aborted_before_signal(previous, self.target):
                raise RuntimeError('prior checkpoint may have attempted replacement; inspect, never repeat')
            write_json(self.directory / 'previous-aborted-checkpoint.json', previous)
        ctl = self.broker(); hosts = set(ctl['draining_hosts'])
        if HOST in hosts:
            return dict(skipped=True, reason='preexisting host drain')
        n = read_json(self.node_path)
        if n['pid'] != self.target['pid'] or n.get('ended') or n.get('thread_error'):
            raise RuntimeError('original P4 heartbeat changed or unhealthy')
        record = dict(control=str(CONTROL), migration_scope=SCOPE, directory_strategy=DIRECTORY_STRATEGY,
            user_authorization=AUTHORIZATION, host=HOST, lane='supplement', operation=self.operation,
            expected=self.target, before=n, max_age=30, status='drain_requested',
            added_by_this_rollout=True, prior_drains=sorted(hosts), started=time.time(), signals_sent=0)
        self.save(record)
        hosts.add(HOST); self.broker({'draining_hosts': sorted(hosts)})
        record.update(status='draining', barrier=time.time()); self.save(record)
        return dict(draining=True)

    def observe(self):
        self.master()
        try:
            record = self.record()
        except FileNotFoundError:
            record = {}
        return dict(status=record.get('status'),
                    drain_owned_current=record.get('added_by_this_rollout') is True,
                    draining=HOST in self.broker()['draining_hosts'])

    def release(self, replacement_pid=None):
        self.master()
        deadline = time.monotonic() + 30
        while True:
            record = self.record()
            if replacement_pid is not None:
                ready = (record['status'] == 'workers_ready'
                         and record.get('replacement', {}).get('pid') == replacement_pid)
            else:
                ready = record['status'] in ('drain_requested', 'draining', 'idle_verified')
            if ready:
                if replacement_pid is None:
                    break
                n = read_json(self.node_path)
                if n['pid'] == replacement_pid:
                    if n.get('active') or n.get('ended') or n.get('thread_error'):
                        raise RuntimeError('candidate heartbeat violates the held idle barrier')
                    protocol.counter_check(record['idle'], n)
                    break
                if n['pid'] != self.target['pid']:
                    raise RuntimeError('unexpected target identity while awaiting heartbeat visibility')
            if time.monotonic() >= deadline:
                raise RuntimeError('replacement outcome not proven; retain this one host drain')
            time.sleep(.25)
        hosts = set(self.broker()['draining_hosts'])
        if record.get('added_by_this_rollout'):
            hosts.discard(HOST); self.broker({'draining_hosts': sorted(hosts)})
        record['drain_released'] = time.time()
        record['status'] = 'complete' if record['status'] == 'workers_ready' else 'waiting_payload'
        self.save(record)
        return dict(released=True, status=record['status'], draining_hosts=sorted(hosts))

    def gate(self):
        self.master()
        record = self.record(); end = time.monotonic() + 30
        meta = read_json(Path(read_json(FAST / 'intent.json')['assignment_preparation']))
        path = Path('/tmp/' + self.cfg['run_id'] + '-broker.sqlite')
        info = path.lstat()
        if path.is_symlink() or (info.st_dev, info.st_ino) != (meta['broker_dev'], meta['broker_ino']):
            raise RuntimeError('original P4 broker database inode changed')
        good, previous = 0, None
        with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2) as db:
            while time.monotonic() < end:
                self.active(); self.protected()
                if HOST not in self.broker()['draining_hosts']:
                    raise RuntimeError('host drain barrier disappeared')
                leases = protocol.lane_leases(db, HOST, 'supplement')
                n = read_json(self.node_path)
                if n['pid'] != self.target['pid']:
                    raise RuntimeError('target PID changed while draining')
                if not leases and protocol.idle_sample(n, self.target, record['barrier']):
                    if n['updated'] != previous:
                        good += 1; previous = n['updated']
                    if good >= 2:
                        record.update(status='idle_verified', idle=n, lane_leases={},
                                      samples=good, idle_verified=time.time())
                        self.save(record)
                        return dict(ready=True, operation=self.operation, checkpoint_protocol=2)
                else:
                    good, previous = 0, None
                time.sleep(.25)
        return dict(ready=False, reason='lane not idle within gate; payload retained')

    def replace(self):
        self.worker(); self.active()
        record = self.record()
        if record['status'] != 'idle_verified' or record['lane_leases'] != {} or record['samples'] < 2:
            raise RuntimeError('two fresh idle samples and lease-zero proof required')
        lock = open('/tmp/' + self.cfg['run_id'] + '-supplement-cache-age-replace.lock', 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = require_actor(self.target)
        n = read_json(self.node_path)
        if (HOST not in self.broker()['draining_hosts']
                or not protocol.idle_sample(n, self.target, record['barrier'])
                or live_data_fds(n['pid'], (self.cfg['source'], self.cfg['destination']),
                                 PREDECESSOR_LOG)):
            raise RuntimeError('lane is not natively idle with zero payload FDs')
        sys.path.insert(0, str(BUNDLE))
        validate_candidate_checkpoint(CHECKPOINT, self.operation)
        nice = os.getpriority(os.PRIO_PROCESS, current['pid'])
        if nice != 0 or os.getpriority(os.PRIO_PROCESS, 0) != nice:
            raise RuntimeError('preserve original worker CPU scheduling priority')
        ioprio = None
        if shutil.which('ionice'):
            ioprio = subprocess.check_output(['ionice', '-p', str(current['pid'])], text=True).strip()
            if subprocess.check_output(['ionice', '-p', str(os.getpid())], text=True).strip() != ioprio:
                raise RuntimeError('controller and original I/O priorities differ')
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard != resource.RLIM_INFINITY and hard < 65536:
            raise RuntimeError('worker NOFILE cannot reach 65536')
        resource.setrlimit(resource.RLIMIT_NOFILE, (max(65536, soft), hard))
        role_path = CONTROL / 'roles' / (HOST + '-supplement.json')
        old_role = read_json(role_path)
        if not protocol.same_identity(native_identity(old_role), self.target):
            raise RuntimeError('old P4 role changed')
        self.protected(); require_actor(self.target); freeze_verify(BUNDLE)
        record.update(status='replacing', helper=protocol.proc_identity(os.getpid()), nice=nice, ioprio=ioprio)
        self.save(record)
        # Exactly one signal to exactly the proved-idle target. No SIGKILL.
        os.kill(current['pid'], signal.SIGTERM)
        record.update(signals_sent=1, stop_sent_at=time.time()); self.save(record)
        deadline = time.monotonic() + 30
        while not predecessor_done(self.target):
            self.protected()
            if time.monotonic() >= deadline:
                raise RuntimeError('idle predecessor exit unproven; no escalation or duplicate launch')
            time.sleep(.1)
        with open('/tmp/' + self.cfg['run_id'] + '-extra64-worker.lock', 'a') as old_lock:
            fcntl.flock(old_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.active(); self.protected()
        if HOST not in self.broker()['draining_hosts']:
            raise RuntimeError('lost host barrier before new launch')
        command = child_command(CHECKPOINT, self.operation, self.target['argv'][0])
        log_fd = os.open(self.directory / 'worker.log', os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_APPEND, 0o600)
        with os.fdopen(log_fd, 'ab', buffering=0) as log:
            child = subprocess.Popen(command, cwd=BUNDLE, stdin=subprocess.DEVNULL,
                stdout=log, stderr=subprocess.STDOUT, close_fds=True, start_new_session=True,
                env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=str(BUNDLE)))
        record.update(status='launched', replacement_pid=child.pid, launched=time.time()); self.save(record)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            self.protected()
            if child.poll() is not None:
                raise RuntimeError('candidate exited; inspect, never launch twice')
            n = read_json(self.node_path)
            if n['pid'] == child.pid and not n.get('ended') and not n.get('thread_error'):
                if n['threads'] != 64 or n.get('active') or n['io_bytes_per_second'] != record['idle']['io_bytes_per_second']:
                    raise RuntimeError('candidate heartbeat resource/barrier invariant differs')
                protocol.counter_check(record['idle'], n)
                replacement = protocol.proc_identity(child.pid)
                if replacement['state'] in ('Z','X') or os.getpriority(os.PRIO_PROCESS, child.pid) != nice:
                    raise RuntimeError('candidate is not live at original CPU priority')
                if ioprio is not None and subprocess.check_output(['ionice','-p',str(child.pid)],text=True).strip()!=ioprio:
                    raise RuntimeError('candidate I/O priority differs')
                write_json(self.directory / 'predecessor-role.json', old_role)
                write_json(role_path, dict(node='worker-10', role='supplement', started=time.time(),
                    pid=child.pid, start_ticks=replacement['starttime_ticks'], argv=replacement['argv'],
                    predecessor=old_role, source_bundle=str(BUNDLE), checkpoint=str(CHECKPOINT)))
                record.update(status='workers_ready', replacement=replacement, ready=time.time())
                self.save(record)
                return dict(ready=True, replacement=replacement, threads=64, counters_preserved=True)
            time.sleep(.25)
        raise RuntimeError('candidate heartbeat unproven; retain drain and inspect')


class RPC(BridgeRPC):
    def ssh_command(self, node, remote_command):
        if node not in ('master-0', 'worker-10'):
            raise ValueError('one exact canary node only')
        return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','ForwardAgent=no',
                '-o','ForwardX11=no','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2',node,remote_command]

    def command(self, node, action, identity=None):
        args = ['/usr/bin/python3','-u',self.remote_script,'remote',
            '--directory',str(Path(self.remote_script).parent),'--operation',self.operation,
            '--action',action,'--audit',str(Path(self.remote_script).parent/'before-copy-audit.json')]
        if action == 'release' and getattr(self, 'replacement_pid', None) is not None:
            args.extend(['--expected-replacement-pid', str(self.replacement_pid)])
        return self.ssh_command(node, shlex.join(args))

    def __call__(self, node, action, *, lane=None, identity=None):
        for _ in range(6 if action == 'gate' else 1):
            result = super().__call__(node, action, lane=lane, identity=identity)
            if action == 'replace' and result.get('ready'):
                self.replacement_pid = result['replacement']['pid']
            elif action == 'inspect' and result.get('replacement_ready'):
                self.replacement_pid = result['identity']['pid']
            if action != 'gate' or result.get('ready'):
                return result
            print(json.dumps(dict(status='waiting_original_inflight_batches', gate=result)), flush=True)
        return result


def stage():
    if read_json(BUNDLE / 'preparation.json')['status'] != 'prepared_native_tests_passed':
        raise RuntimeError('validated native P4 candidate required')
    freeze_verify(BUNDLE)
    directory = BUNDLE / ('controller-' + uuid.uuid4().hex)
    directory.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parent
    hashes = {}
    for name in FILES:
        source = BUNDLE / name if name == 'migration_io.py' else repository / name
        shutil.copyfile(source, directory / name)
        (directory / name).chmod(0o444)
        hashes[name] = hashlib.sha256((directory / name).read_bytes()).hexdigest()
    write_json(directory / 'source-sha256.json', hashes)
    operation = uuid.uuid4().hex
    write_json(directory / 'authorization.json', dict(operation=operation,user_authorization=AUTHORIZATION,
        control=str(CONTROL),bundle=str(BUNDLE),node='worker-10',lane='supplement',signal_limit=1))
    print(json.dumps(dict(directory=str(directory),operation=operation,status='controller_staged_not_applied')),flush=True)


def run(directory):
    if directory != Path(__file__).resolve().parent or directory.parent != BUNDLE:
        raise RuntimeError('run only the frozen controller inside its exact candidate')
    freeze_verify(directory)
    auth = read_json(directory/'authorization.json'); operation=auth['operation']
    validate_audit(read_json(directory/'before-copy-audit.json'))
    result_path=directory/'result.json'
    if result_path.exists():
        raise RuntimeError('operation already started; inspect, never repeat blindly')
    write_json(result_path,dict(status='running',operation=operation,started=time.time()))
    rpc=RPC(control=str(CONTROL),job=JOB,node='worker-10',lane='supplement',
        remote_script=str(directory/CONTROLLER_ENTRY),ssh_prefix='copy-',
        output=result_path,operation=operation,gate_timeout=30,max_age=30,control_timeout=30)
    try:
        with rpc.hold():
            result=protocol.roll_one(rpc,'worker-10','supplement',gate_timeout=30,max_age=30,visibility_timeout=30)
        write_json(result_path,dict(result,operation=operation,finished=time.time()))
        print(json.dumps(result),flush=True)
    except BaseException as exc:
        write_json(result_path,dict(status='inspect_before_retry',error=repr(exc),operation=operation,finished=time.time()))
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=('stage','run','remote'))
    p.add_argument('--directory',type=Path)
    p.add_argument('--operation');p.add_argument('--action');p.add_argument('--audit',type=Path)
    p.add_argument('--expected-replacement-pid', type=int)
    args=p.parse_args()
    if args.mode=='stage':return stage()
    if args.mode=='run':return run(args.directory)
    if args.directory!=Path(__file__).resolve().parent or args.directory.parent!=BUNDLE:
        raise RuntimeError('remote controller must remain frozen inside its exact candidate')
    remote=Remote(args.directory,args.operation)
    if args.action=='hold':
        remote.master()
        with open('/tmp/'+remote.cfg['run_id']+'-cache-age-dispatch.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            print('RESULT '+json.dumps(dict(locked=True)),flush=True);sys.stdin.read()
        return
    if args.action=='drain':value=remote.drain(args.audit)
    elif args.action=='release':value=remote.release(args.expected_replacement_pid)
    elif args.action in ('inspect','observe','gate','replace'):value=getattr(remote,args.action)()
    else:raise ValueError('unknown explicit remote action')
    print('RESULT '+json.dumps(value),flush=True)


if __name__=='__main__':main()
