#!/usr/bin/env python3
"""Manually started, dynamically dispatched GNU rm jobs. No per-file statistics."""
import argparse
import collections
import concurrent.futures as futures
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import select
import shlex
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid

TARGET = '/share/project'
ROOT_INODE = 69783039777
CLUSTER = '2dadf47a-b549-47bb-a267-d56dc877aec9'
EXTRA_CLUSTER = '1f50be08-3b34-4d39-b932-7f539bef2d27'
NODE_HOSTS = {node: 'job-'+CLUSTER+'-'+node
              for node in ['master-0'] + ['worker-'+str(i) for i in range(23)]}
NODE_HOSTS.update({'extra-master-0': 'job-'+EXTRA_CLUSTER+'-master-0',
                   **{'extra-worker-'+str(i): 'job-'+EXTRA_CLUSTER+'-worker-'+str(i)
                      for i in range(15)}})
NODES = list(NODE_HOSTS)
HERE = Path(__file__).resolve().parent
RUN = HERE / 'runs/prepared-v3'
FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
CONTROL_INTERVAL = 5
CONTROL_TIMEOUT = 90
JOIN_READY_TIMEOUT = 20


def atomic(path, data):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.part')
    with tmp.open('x') as stream:
        json.dump(data, stream, ensure_ascii=True)
        stream.flush()
        os.fsync(stream.fileno())
    tmp.replace(path)


def load(path):
    with open(path) as stream:
        return json.load(stream)


def identity(pid):
    fields = Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, start=fields[19], boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


class ControlLease:
    """Receipt times are local/monotonic; no NFS or cross-host wall clocks."""
    def __init__(self, timeout=CONTROL_TIMEOUT, clock=time.monotonic):
        self.clock, self.timeout = clock, timeout
        self.last = clock()

    def touch(self):
        self.last = self.clock()

    def age(self):
        return self.clock() - self.last

    def expired(self):
        return self.age() > self.timeout


def component(value):
    if not isinstance(value, str) or value in ('', '.', '..') or '/' in value or '\0' in value:
        raise ValueError('unsafe entry component')
    return value


def mount_id(fd):
    return int(next(line.split()[1] for line in Path('/proc/self/fdinfo/'+str(fd)).read_text().splitlines()
                    if line.startswith('mnt_id:')))


class Root:
    def __init__(self, path, inode):
        fd = os.open('/', FLAGS)
        try:
            for part in path.split('/'):
                if part:
                    child = os.open(component(part), FLAGS, dir_fd=fd)
                    os.close(fd)
                    fd = child
            if os.fstat(fd).st_ino != inode:
                raise ValueError('root inode differs')
        except BaseException:
            os.close(fd)
            raise
        self.fd, self.inode, self.path = fd, inode, path
        self.mount = mount_id(fd)

    def close(self):
        os.close(self.fd)

    @contextlib.contextmanager
    def parent(self, rel, inode):
        parts = rel.split('/') if rel else []
        for part in parts:
            component(part)
        if os.stat(self.path, follow_symlinks=False).st_ino != self.inode:
            raise ValueError('root changed')
        fd = os.dup(self.fd)
        try:
            for part in parts:
                child = os.open(part, FLAGS, dir_fd=fd)
                os.close(fd)
                fd = child
                if mount_id(fd) != self.mount:
                    raise ValueError('nested mount excluded')
            if os.fstat(fd).st_ino != inode:
                raise ValueError('parent directory inode changed')
            yield fd
        finally:
            os.close(fd)


def ssh(node, script, action, run=None):
    args = ['python3', '-S', '-B', '-u', str(script), action, '--node', node]
    if run:
        args += ['--run', str(run)]
    return ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
            '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', node, shlex.join(args)]


def rm_touches_target(args, cwd):
    """Conservatively classify rm's lexical scope without resolving operands.

    /proc arguments may be bytes or strings. In addition to a cwd inside the
    source, an operand can name the source, a descendant, or an ancestor such
    as /share. Options are not operands unless they follow the -- delimiter.
    This only compares strings; it never follows links or executes a command.
    """
    if not args or os.path.basename(os.fsdecode(args[0])) != 'rm':
        return False
    target = os.path.normpath(TARGET)
    cwd = os.path.normpath(os.fsdecode(cwd))
    if cwd == target or cwd.startswith(target.rstrip('/')+'/'):
        return True
    operands = False
    for raw in args[1:]:
        value = os.fsdecode(raw)
        if not value:
            continue
        if not operands and value == '--':
            operands = True
            continue
        if not operands and value.startswith('-') and value != '-':
            continue
        path = os.path.normpath(value if os.path.isabs(value) else os.path.join(cwd, value))
        if (path == target or path.startswith(target.rstrip('/')+'/')
                or target.startswith(path.rstrip('/')+'/')):
            return True
    return False


def node_check(node, reject_running_rm=False):
    if not isinstance(node, str) or node not in NODE_HOSTS:
        raise ValueError('unknown node alias')
    if socket.gethostname() != NODE_HOSTS[node]:
        raise ValueError('cluster/node differs')
    if not os.path.ismount(TARGET) or os.stat(TARGET, follow_symlinks=False).st_ino != ROOT_INODE:
        raise ValueError('source mount differs')
    if os.statvfs(TARGET).f_flag & os.ST_RDONLY:
        raise ValueError('source mount is read-only')
    # GNU --one-file-system uses devices, so reject nested same-device bind
    # mounts too. Mount changes after preflight must be prevented administratively.
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        field = line.split()[4]
        for before, after in (('\\040', ' '), ('\\011', '\t'), ('\\012', '\n'), ('\\134', '\\')):
            field = field.replace(before, after)
        if field.startswith(TARGET+'/'):
            raise ValueError('nested mount exists: '+field)
    if reject_running_rm:
        for path in Path('/proc').iterdir():
            if not path.name.isdigit():
                continue
            try:
                args = path.joinpath('cmdline').read_bytes().split(b'\0')
                cwd = os.readlink(path/'cwd')
                if rm_touches_target(args, cwd):
                    raise ValueError('existing rm under target: '+path.name)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
    version = subprocess.check_output(['/usr/bin/rm', '--version'], text=True).splitlines()[0]
    if 'GNU coreutils' not in version:
        raise ValueError('GNU rm required')
    return dict(node=node, host=socket.gethostname(), rm=version, checked=time.time())


def plan(root, depth=3, expansion_limit=1024):
    """Enumerate shallow partition only; never stat sizes or read data files."""
    waiting = collections.deque([dict(rel='', ino=root.inode, depth=0)])
    expanded = 0
    while waiting:
        current = waiting.popleft()
        rel, ino = current['rel'], current['ino']
        if rel and (current['depth'] >= depth or expanded >= expansion_limit):
            yield dict(kind='task', parent=current['parent'], ino=current['parent_ino'], names=[rel.rsplit('/', 1)[-1]])
            continue
        expanded += 1
        with root.parent(rel, ino) as fd, os.scandir(fd) as entries:
            leaves = []
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    waiting.append(dict(rel='/'.join(filter(None, (rel, entry.name))),
                        ino=entry.inode(), depth=current['depth']+1, parent=rel, parent_ino=ino))
                else:
                    leaves.append(entry.name)
                    if len(leaves) == 128:
                        yield dict(kind='task', parent=rel, ino=ino, names=leaves, flat_batch=True)
                        leaves = []
            if leaves:
                yield dict(kind='task', parent=rel, ino=ino, names=leaves, flat_batch=True)
        if rel:
            yield dict(kind='scaffold', parent=current['parent'], ino=current['parent_ino'],
                       names=[rel.rsplit('/', 1)[-1]], depth=current['depth'])
    yield dict(kind='end')


def cooperative_plan(root, scopes, blocked=(), busy=(), split_depth=2, max_depth=8):
    """Stream a fair, bounded-memory frontier beneath already-running rm jobs.

    This is cooperative deletion, not an exclusive transfer: original rm
    processes still own the enclosing trees. Expanded parents are retained for
    the original process. No sizes, data files, or per-file manifests are read.
    scan_tick records allow cancellation/preview limits even in empty trees.
    """
    if not 1 <= split_depth <= max_depth <= 12:
        raise ValueError('invalid cooperative split depth')
    blocked, busy = tuple(blocked), tuple(busy)
    for path in blocked + busy:
        if not path.startswith(root.path+'/'):
            raise ValueError('protection outside source')
        for part in path[len(root.path)+1:].split('/'):
            component(part)
    rels = []
    for scope in scopes:
        rel = scope['rel']
        for part in rel.split('/'):
            component(part)
        if any(rel == p or rel.startswith(p+'/') or p.startswith(rel+'/') for p in rels):
            raise ValueError('cooperative source scopes overlap')
        rels.append(rel)

    def covered(path):
        return any(path == p or path.startswith(p+'/') for p in blocked)

    def ancestor(path, paths):
        return any(path == p or p.startswith(path+'/') for p in paths)

    def task(rel, ino, names):
        return dict(kind='task', parent=rel, ino=ino, names=names)

    def walk(rel, ino, depth):
        if covered(root.path+'/'+rel):
            return
        try:
            with root.parent(rel, ino) as fd, os.scandir(fd) as entries:
                yield dict(kind='scan_tick')
                leaves = []
                for entry in entries:
                    child = rel+'/'+entry.name
                    absolute = root.path+'/'+child
                    if covered(absolute):
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        descend = (depth+1 < split_depth or ancestor(absolute, blocked)
                                   or ancestor(absolute, busy))
                        if descend:
                            if depth+1 < max_depth:
                                yield from walk(child, entry.inode(), depth+1)
                        else:
                            yield task(rel, ino, [entry.name])
                    elif absolute not in busy:
                        leaves.append(entry.name)
                        if len(leaves) == 128:
                            yield dict(task(rel, ino, leaves), flat_batch=True)
                            leaves = []
                if leaves:
                    yield dict(task(rel, ino, leaves), flat_batch=True)
        except FileNotFoundError:
            # A concurrent original rm may already have removed this parent.
            return

    waiting = collections.deque(walk(s['rel'], s['ino'], 0) for s in scopes)
    try:
        while waiting:
            current = waiting.popleft()
            try:
                item = next(current)
            except StopIteration:
                continue
            except BaseException:
                current.close()
                raise
            waiting.append(current)
            yield item
    finally:
        for current in waiting:
            current.close()


def interrupt_child(process):
    try:
        fd = os.pidfd_open(process.pid)
        try:
            if process.poll() is None:
                signal.pidfd_send_signal(fd, signal.SIGTERM)
        finally:
            os.close(fd)
    except ProcessLookupError:
        pass


def native_rm(root, task, stop, children, children_lock, error_log):
    if stop.is_set():
        return dict(rc=125, error='stopped')
    for value in task['names']:
        component(value)
    if not task['names'] or len(task['names']) > 128:
        raise ValueError('invalid task size')
    # A cooperative supplementary job can find that the original rm already
    # removed its parent. Missing work is success; changed roots still fail.
    if os.stat(root.path, follow_symlinks=False).st_ino != root.inode:
        raise ValueError('root changed')
    try:
        parent = root.parent(task['parent'], task['ino'])
        fd = parent.__enter__()
    except FileNotFoundError:
        return dict(rc=0, already_gone=True)
    try:
        # Only single-component names are arguments; the mount root is never an
        # rm argument. cwd is an anchored parent FD, not an unchecked pathname.
        process = subprocess.Popen(['/usr/bin/rm', '-rf', '--one-file-system', '--']+task['names'],
            cwd='/proc/self/fd/'+str(fd), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=error_log, start_new_session=True)
        with children_lock:
            children.add(process)
            if stop.is_set():
                interrupt_child(process)
        try:
            return dict(rc=process.wait())
        finally:
            with children_lock:
                children.discard(process)
    except FileNotFoundError:
        # /proc/self/fd cwd may disappear between the parent open and exec
        # when a cooperative rm has already finished this directory.
        if not os.path.exists('/usr/bin/rm'):
            raise
        return dict(rc=0, already_gone=True)
    finally:
        parent.__exit__(None, None, None)


def validate_parent_policy(value, runtime_join=True):
    if (runtime_join is not True or len(NODES) != 40 or not isinstance(value, dict)
            or set(value) != {'limit', 'node_boots'}):
        raise ValueError('parent policy requires the complete runtime-enabled forty-node registry')
    limit, boots = value['limit'], value['node_boots']
    if limit is not None and (type(limit) is not int or limit not in (1, 2)):
        raise ValueError('parent policy limit must be None, 1 or 2')
    if (not isinstance(boots, dict) or set(boots) != set(NODES)
            or any(not isinstance(boot, str) or not boot for boot in boots.values())):
        raise ValueError('parent policy requires a complete immutable node-to-boot map')
    return dict(limit=limit, node_boots=dict(boots))


def parent_helpers(run, cfg, execute=True):
    """Load only verified bytes from this exact frozen run, never import cache."""
    policy = validate_parent_policy(cfg.get('parent_policy'), cfg.get('runtime_join'))
    if (cfg.get('target') != TARGET or cfg.get('root_inode') != ROOT_INODE
            or cfg.get('nodes') != NODES or cfg.get('node_hosts') != NODE_HOSTS):
        raise ValueError('parent policy node registry differs')
    expected = cfg.get('parent_helpers')
    if not isinstance(expected, dict) or set(expected) != {'parent_scheduler.py', 'parent_tuning.py'}:
        raise ValueError('parent helper manifest differs')
    modules = {}
    for name, digest in expected.items():
        path = Path(run)/name
        source = path.read_bytes()
        if hashlib.sha256(source).hexdigest() != digest:
            raise ValueError('frozen parent helper changed: '+name)
        if execute:
            module_name = '_frozen_'+path.stem+'_'+uuid.uuid4().hex
            spec = importlib.util.spec_from_file_location(module_name, path)
            module = importlib.util.module_from_spec(spec)
            # Dataclass-like stdlib decorators may inspect their own module;
            # install only this fresh unique name, then discard it immediately.
            sys.modules[module_name] = module
            try:
                exec(compile(source, str(path), 'exec'), module.__dict__)
            finally:
                sys.modules.pop(module_name, None)
            modules[name] = module
    if execute and modules['parent_tuning.py'].validate_policy(policy) != policy:
        raise ValueError('parent helper policy validation differs')
    return modules


def worker(run, node):
    cfg = load(run/'config.json')
    if cfg['target'] != TARGET or cfg['root_inode'] != ROOT_INODE or cfg['nodes'] != NODES:
        raise ValueError('scope differs')
    if 'node_hosts' in cfg and cfg['node_hosts'] != NODE_HOSTS:
        raise ValueError('node hostname registry differs')
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != cfg['sha256']:
        raise ValueError('frozen script changed')
    node_check(node)
    policy = cfg.get('parent_policy')
    actual_boot = None
    if policy is not None:
        parent_helpers(run, cfg, execute=False)
        actual_boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if node not in policy['node_boots'] or actual_boot != policy['node_boots'][node]:
            raise ValueError('worker boot differs from parent policy')
    root = Root(TARGET, ROOT_INODE)
    stop = threading.Event()
    children, lock, output_lock = set(), threading.Lock(), threading.Lock()
    lease = ControlLease()
    reason_lock = threading.Lock()
    first_reason = []

    def request_stop(reason):
        stop.set()
        with reason_lock:
            if first_reason:
                return
            first_reason.append(reason)
        evidence = dict(reason=reason, pid=os.getpid(), node=node, at=time.time(),
                        seconds_since_control=lease.age())
        try:
            atomic(run/'workers'/(node+'-first-stop.json'), evidence)
        except OSError:
            pass
        print(json.dumps(evidence), file=sys.stderr, flush=True)

    signal.signal(signal.SIGTERM, lambda *_: request_stop('worker_received_SIGTERM'))

    def watchdog():
        while not stop.wait(1):
            if lease.expired():
                request_stop('ssh_control_timeout')
        with lock:
            for process in children:
                interrupt_child(process)
    thread = threading.Thread(target=watchdog, daemon=True)
    thread.start()
    actor = dict(identity(os.getpid()), node=node, host=socket.gethostname(), script=str(__file__))
    atomic(run/'workers'/(node+'.json'), actor)
    with (run/'logs'/(node+'-rm-errors.log')).open('ab', buffering=0) as errors:
        def execute(task):
            try:
                result = native_rm(root, task, stop, children, lock, errors)
            except Exception as exc:
                result = dict(rc=126, error=repr(exc))
            result['id'] = task['id']
            try:
                with output_lock:
                    print(json.dumps(result), flush=True)
            except (BrokenPipeError, OSError):
                request_stop('result_pipe_closed')
        try:
            with futures.ThreadPoolExecutor(max_workers=cfg['per_node']) as pool:
                graceful = False
                try:
                    for line in sys.stdin:
                        message = json.loads(line)
                        lease.touch()
                        if stop.is_set():
                            break
                        kind = message.get('kind')
                        if kind == 'ping':
                            with output_lock:
                                response = dict(kind='pong')
                                if policy is not None:
                                    response.update(node=node, boot=actual_boot)
                                print(json.dumps(response), flush=True)
                        elif kind == 'cancel':
                            request_stop('controller_cancelled')
                            break
                        elif kind == 'finish':
                            graceful = True
                            break
                        else:
                            pool.submit(execute, message)
                finally:
                    if graceful:
                        stop.set()
                    else:
                        request_stop('control_stream_closed_before_finish')
        finally:
            stop.set()
            thread.join(timeout=3)
            root.close()


def preflight(script, nodes=None):
    def check(node):
        result = subprocess.run(ssh(node, script, '_check'), capture_output=True, text=True, timeout=25)
        if result.returncode:
            raise RuntimeError(node+': '+result.stderr[-1000:])
        return json.loads(result.stdout)
    with futures.ThreadPoolExecutor(max_workers=len(NODES)) as pool:
        return list(pool.map(check, NODES if nodes is None else nodes))


def prepare(run, per_node, execution_nodes=None, seed_tasks=None, supplement_of=None,
            tail_scopes=None, blocked_paths=(), busy_paths=(), runtime_join=False,
            parent_policy=None):
    execution_nodes = NODES if execution_nodes is None else execution_nodes
    if not execution_nodes or any(n not in NODES for n in execution_nodes):
        raise ValueError('invalid execution nodes')
    if (seed_tasks is not None or tail_scopes is not None) and not supplement_of:
        raise ValueError('supplement requires original run binding')
    if seed_tasks is not None and tail_scopes is not None:
        raise ValueError('choose one supplementary planner')
    if type(runtime_join) is not bool:
        raise ValueError('runtime_join must be boolean')
    if parent_policy is not None:
        parent_policy = validate_parent_policy(parent_policy, runtime_join)
        if per_node != 4:
            raise ValueError('parent scheduling keeps the existing four worker threads')
    checks = preflight(Path(__file__).resolve(), execution_nodes)
    run.mkdir(parents=True, exist_ok=False, mode=0o700)
    (run/'logs').mkdir()
    (run/'workers').mkdir()
    import shutil
    shutil.copyfile(__file__, run/'rm24.py')
    cfg = dict(target=TARGET, root_inode=ROOT_INODE, nodes=NODES, per_node=per_node,
               node_hosts=dict(NODE_HOSTS),
               sha256=hashlib.sha256((run/'rm24.py').read_bytes()).hexdigest(), preflight=checks)
    if seed_tasks is not None:
        cfg.update(seed_tasks=seed_tasks, execution_nodes=execution_nodes,
                   supplement_of=str(supplement_of))
    if tail_scopes is not None:
        cfg.update(tail_scopes=tail_scopes, blocked_paths=list(blocked_paths),
                   busy_paths=list(busy_paths), execution_nodes=execution_nodes,
                   supplement_of=str(supplement_of))
    if runtime_join:
        cfg.update(runtime_join=True, execution_nodes=list(execution_nodes))
        (run/'runtime-requests').mkdir()
        (run/'runtime-acks').mkdir()
    if parent_policy is not None:
        manifest = {}
        for name in ('parent_scheduler.py', 'parent_tuning.py'):
            shutil.copyfile(Path(__file__).resolve().parent/name, run/name)
            manifest[name] = hashlib.sha256((run/name).read_bytes()).hexdigest()
        cfg.update(parent_policy=parent_policy, parent_helpers=manifest)
        parent_helpers(run, cfg)
    atomic(run/'config.json', cfg)
    atomic(run/'status.json', dict(state='prepared_not_started', updated=time.time()))
    print(json.dumps(dict(prepared=str(run), verified_nodes=len(checks), rm_per_node=per_node)))


class RuntimeJoins:
    """Admission only: all workers consume the existing controller queue.

    Requests never alter the frozen config, start a second planner, or steal an
    in-flight task. The admission lock also fences phase closure and queue
    claims, so an unready or late joining worker cannot receive work.
    """
    def __init__(self, run, cfg, source, finished, stop, nodes, actor=None):
        self.run, self.cfg = Path(run).resolve(), cfg
        self.source, self.finished, self.stop = source, finished, stop
        self.actor = identity(os.getpid()) if actor is None else actor
        self.config_sha256 = hashlib.sha256((self.run/'config.json').read_bytes()).hexdigest()
        self.lock, self.poll_lock = threading.RLock(), threading.Lock()
        self.nodes, self.starting = list(nodes), {}
        self.accepted, self.seen, self.acks = [], set(), {}
        self.accepted_nonce = {}
        self.rejected, self.closed_reason = 0, None
        for name in ('runtime-requests', 'runtime-acks'):
            (self.run/name).mkdir(exist_ok=True)

    def _closed(self, check_stop=True):
        if self.closed_reason is None:
            published_stop = False
            if check_stop:
                try:
                    published_stop = (self.run/'STOP').exists()
                except OSError:
                    pass
            if self.stop.is_set() or published_stop:
                self.closed_reason = 'controller stopping'
            elif self.finished.is_set() and self.source.empty():
                self.closed_reason = 'main queue exhausted'
        return self.closed_reason

    def close(self, reason='main phase closed'):
        with self.lock:
            self.closed_reason = self.closed_reason or reason

    def _ack(self, request, state, reason):
        ack = dict(version=1, nonce=request['nonce'], node=request.get('node'),
                   state=state, reason=reason, at=time.time())
        self.acks[request['nonce']] = ack
        self._flush_acks()

    def _flush_acks(self):
        for nonce, ack in list(self.acks.items()):
            try:
                atomic(self.run/'runtime-acks'/(nonce+'.json'), ack)
            except OSError:
                continue  # transient shared-storage errors cannot stop rm
            self.acks.pop(nonce, None)

    def reject(self, request, reason):
        with self.lock:
            node = request.get('node')
            if self.starting.get(node, {}).get('nonce') == request['nonce']:
                self.starting.pop(node, None)
            # A ready connection that fails before its first task is removable
            # without changing any original worker's failure policy.
            if self.accepted_nonce.get(node) == request['nonce']:
                self.accepted_nonce.pop(node)
                self.accepted.remove(node)
                self.nodes.remove(node)
            self.rejected += 1
            self._ack(request, 'rejected', reason)

    def ready(self, request):
        with self.lock:
            reason = self._closed()
            try:
                if hashlib.sha256((self.run/'config.json').read_bytes()).hexdigest() != self.config_sha256:
                    reason = 'frozen config changed during worker readiness'
            except OSError as exc:
                reason = 'cannot confirm frozen config: '+repr(exc)
            if reason:
                self.reject(request, reason)
                return False
            node = request['node']
            if self.starting.get(node) != request:
                self.reject(request, 'admission reservation lost')
                return False
            self.starting.pop(node)
            self.nodes.append(node)
            self.accepted.append(node)
            self.accepted_nonce[node] = request['nonce']
            self._ack(request, 'accepted', 'preflight and worker pong verified')
            return True

    def claim(self, node=None):
        with self.lock:
            # Admission/poll and the one-second controller heartbeat check the
            # shared STOP file. Task dispatch itself only reads cached state.
            if self._closed(check_stop=False):
                raise queue.Empty
            if hasattr(self.source, 'claim'):
                return self.source.claim(node)
            return self.source.get_nowait()

    def snapshot(self):
        with self.lock:
            return dict(enabled=True, accepted=list(self.accepted),
                        starting=list(self.starting), rejected=self.rejected,
                        pending=self.source.qsize(),
                        closed_reason=self.closed_reason)

    def poll(self, submit=None):
        """submit(request) returns a dispatch future; None only rejects late requests."""
        result = []
        with self.poll_lock, self.lock:
            self._flush_acks()
            closed = self._closed()
            if submit is None and not closed:
                return result
            try:
                paths = sorted((self.run/'runtime-requests').glob('*.json'))
            except OSError:
                return result
            for path in paths:
                nonce = path.stem
                if (nonce in self.seen or not nonce or len(nonce) > 128
                        or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in nonce)):
                    continue
                try:
                    request = load(path)
                except OSError:
                    continue
                except (ValueError, TypeError) as exc:
                    request = dict(nonce=nonce)
                    self.seen.add(nonce)
                    self.reject(request, 'invalid request JSON: '+str(exc))
                    continue
                self.seen.add(nonce)
                node = request.get('node') if isinstance(request, dict) else None
                fallback = dict(nonce=nonce, node=node if isinstance(node, str) else None)
                try:
                    if not isinstance(request, dict):
                        raise ValueError('request must be an object')
                    if type(request.get('version')) is not int or request['version'] != 1 or request.get('nonce') != nonce:
                        raise ValueError('version/nonce differs')
                    if request.get('run') != str(self.run):
                        raise ValueError('run differs')
                    if request.get('controller') != self.actor:
                        raise ValueError('controller identity differs')
                    if request.get('config_sha256') != self.config_sha256:
                        raise ValueError('config SHA differs')
                    if hashlib.sha256((self.run/'config.json').read_bytes()).hexdigest() != self.config_sha256:
                        raise ValueError('frozen config changed')
                    if request.get('node') not in NODES:
                        raise ValueError('unknown node')
                    if type(request.get('per_node')) is not int or request['per_node'] != self.cfg['per_node']:
                        raise ValueError('per-node concurrency differs')
                    reason = self._closed()
                    if reason:
                        raise ValueError(reason)
                    node = request['node']
                    if node in self.nodes:
                        self._ack(request, 'already_active', 'node already active')
                        continue
                    if node in self.starting:
                        raise ValueError('join still starting; await its original acknowledgement')
                    self.starting[node] = request
                    result.append(submit(request))
                except Exception as exc:
                    self.reject(fallback, str(exc))
        return result


def ready_pong(proc, stop, timeout=JOIN_READY_TIMEOUT):
    """Bounded first response, without TextIO read-ahead or a reader thread."""
    deadline, data = time.monotonic()+timeout, bytearray()
    while not stop.is_set():
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError('worker ready pong timed out')
        if not select.select([proc.stdout.fileno()], [], [], min(.1, remaining))[0]:
            continue
        byte = os.read(proc.stdout.fileno(), 1)
        if not byte:
            raise RuntimeError('worker exited before ready pong')
        data.extend(byte)
        if len(data) > 65536:
            raise ValueError('oversized worker ready response')
        if byte == b'\n':
            response = json.loads(data)
            if response.get('kind') != 'pong':
                raise ValueError('worker did not return ready pong')
            return response
    raise RuntimeError('controller stopping before worker ready')


def controller(run):
    cfg = load(run/'config.json')
    script = run/'rm24.py'
    policy = cfg.get('parent_policy')
    helpers = parent_helpers(run, cfg) if policy is not None else {}
    planned = threading.Event()
    stop = threading.Event()
    parent_sources, parent_sources_lock = [], threading.Lock()
    parent_tuner = [None]
    parent_limit = [policy['limit'] if policy is not None else None]
    parent_view = [None]

    def new_source():
        if policy is None:
            return queue.Queue(maxsize=4096)
        with parent_sources_lock:
            source = helpers['parent_scheduler.py'].ParentQueue(
                policy['node_boots'], nodes=NODES, maxsize=4096, limit=parent_limit[0])
            parent_sources.append(source)
            parent_view[0] = source
            return source

    def apply_parent_limit(limit):
        with parent_sources_lock:
            parent_limit[0] = limit
            for source in parent_sources:
                source.set_limit(limit)

    def stop_sources():
        with parent_sources_lock:
            for source in parent_sources:
                source.stop()

    pending = new_source()
    if policy is not None:
        parent_tuner[0] = helpers['parent_tuning.py'].ParentPolicyTuner(
            run, identity(os.getpid()), hashlib.sha256((run/'config.json').read_bytes()).hexdigest(),
            policy['limit'], apply_parent_limit)
    lock = threading.Lock()
    completed = 0
    failed_events = 0
    failed_jobs = {}
    scaffolds = []
    errors = []
    monitor_thread = None
    producer = None
    state_lock = threading.Lock()
    current_state = ['checking_nodes', {}]
    execution_nodes = list(cfg.get('execution_nodes', NODES))
    runtime_manager = [None]

    def abort(reason):
        stop.set()
        stop_sources()
        with lock:
            errors.append(reason)
            # Preserve the first observed cause instead of overwriting it with
            # the ensuing SIGTERM errors from the other nodes.
            if not (run/'first-error.json').exists():
                atomic(run/'first-error.json', dict(reason=reason, at=time.time()))
        atomic(run/'STOP', dict(reason=reason, at=time.time()))

    def state(value=None, **extra):
        with state_lock:
            if value is not None:
                current_state[:] = [value, extra]
            with lock:
                view = dict(state=current_state[0], updated=time.time(), completed_rm_jobs=completed,
                            failed_rm_events=failed_events, unresolved_rm_jobs=len(failed_jobs),
                            errors=errors[:20], **current_state[1])
            if runtime_manager[0] is not None:
                view['runtime_join'] = runtime_manager[0].snapshot()
            if parent_view[0] is not None:
                view['parent_scheduler'] = parent_view[0].snapshot()
                view['parent_tuning'] = parent_tuner[0].snapshot()
            atomic(run/'status.json', view)
            print(json.dumps(view), flush=True)

    def heartbeat():
        next_report = 0
        while not stop.is_set():
            if (run/'STOP').exists():
                stop.set()
                stop_sources()
                break
            if time.monotonic() >= next_report:
                if parent_tuner[0] is not None:
                    parent_tuner[0].poll()
                state()
                next_report = time.monotonic() + 5
            if runtime_manager[0] is not None:
                runtime_manager[0].poll()  # only acknowledges closed-phase requests
            stop.wait(1)

    def produce():
        proc = None
        try:
            if 'tail_scopes' in cfg:
                root = Root(TARGET, ROOT_INODE)
                stream = cooperative_plan(root, cfg['tail_scopes'],
                    cfg.get('blocked_paths', ()), cfg.get('busy_paths', ()))
                try:
                    for item in stream:
                        if stop.is_set():
                            break
                        if item['kind'] != 'task':
                            continue
                        item['id'] = uuid.uuid4().hex
                        while not stop.is_set():
                            try:
                                pending.put(item, timeout=1)
                                break
                            except queue.Full:
                                pass
                finally:
                    stream.close()
                    root.close()
                return
            if 'seed_tasks' in cfg:
                for seed in cfg['seed_tasks']:
                    item = dict(seed, id=uuid.uuid4().hex)
                    while not stop.is_set():
                        try:
                            pending.put(item, timeout=1)
                            break
                        except queue.Full:
                            pass
                return
            with (run/'logs/planner.log').open('ab', buffering=0) as log:
                proc = subprocess.Popen(ssh('master-0', script, '_plan', run), stdout=subprocess.PIPE,
                                        stderr=log, text=True)
                ended = False
                for line in proc.stdout:
                    item = json.loads(line)
                    if item['kind'] == 'scaffold':
                        scaffolds.append(item)
                    elif item['kind'] == 'end':
                        ended = True
                    else:
                        item['id'] = uuid.uuid4().hex
                        while not stop.is_set():
                            try:
                                pending.put(item, timeout=1)
                                break
                            except queue.Full:
                                pass
                    if stop.is_set():
                        break
                if stop.is_set():
                    proc.terminate()
                if proc.wait() or not ended:
                    raise RuntimeError('planner did not return its end marker')
        except Exception as exc:
            abort(repr(exc))
        finally:
            if proc is not None:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pass
                proc.stdout.close()
            planned.set()

    def dispatch(node, source, finished, joins=None, request=None):
        nonlocal completed, failed_events
        claimed = threading.Event()
        if request is not None:
            try:
                preflight(script, [node])
            except Exception as exc:
                joins.reject(request, 'join preflight failed: '+repr(exc))
                return
        try:
            log = (run/'logs'/(node+'-ssh.log')).open('ab', buffering=0)
        except Exception as exc:
            if request is not None:
                joins.reject(request, 'join log open failed: '+repr(exc))
                return
            raise
        with log:
            try:
                proc = subprocess.Popen(ssh(node, script, '_worker', run), stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=log, text=True, bufsize=1)
            except Exception as exc:
                if request is not None:
                    joins.reject(request, 'join connection failed: '+repr(exc))
                    return
                raise
            active = {}
            write_lock = threading.Lock()
            connection_done = threading.Event()

            def send(message):
                with write_lock:
                    proc.stdin.write(json.dumps(message, ensure_ascii=True)+'\n')
                    proc.stdin.flush()

            def controls():
                while not connection_done.is_set():
                    try:
                        send(dict(kind='cancel' if stop.is_set() else 'ping'))
                        if stop.is_set():
                            return
                    except (OSError, ValueError) as exc:
                        if not connection_done.is_set() and (request is None or claimed.is_set()):
                            abort(node+': control write failed: '+repr(exc))
                        return
                    connection_done.wait(CONTROL_INTERVAL)

            control_thread = threading.Thread(target=controls, daemon=True)
            control_thread.start()
            try:
                if request is not None or policy is not None:
                    pong = ready_pong(proc, stop, timeout=JOIN_READY_TIMEOUT)
                    if policy is not None:
                        if pong.get('node') != node or pong.get('boot') != policy['node_boots'].get(node):
                            raise ValueError(node+': ready pong node/boot differs from frozen parent policy')
                        source.register_node(node, pong['boot'])
                if request is not None:
                    if not joins.ready(request):
                        return
                while not stop.is_set():
                    while len(active) < cfg['per_node'] and not stop.is_set():
                        try:
                            task = (joins.claim(node) if joins is not None else
                                    source.claim(node) if policy is not None else source.get(timeout=.1))
                        except queue.Empty:
                            break
                        active[task['id']] = task
                        claimed.set()
                        send(task)
                    if not active:
                        if finished.is_set() and source.empty():
                            break
                        time.sleep(.1)
                        continue
                    line = proc.stdout.readline()
                    if not line:
                        raise RuntimeError(node+': SSH worker exited with tasks in flight')
                    result = json.loads(line)
                    if result.get('kind') == 'pong':
                        if policy is not None and (result.get('node') != node or
                                result.get('boot') != policy['node_boots'].get(node)):
                            raise ValueError(node+': periodic pong node/boot changed')
                        continue
                    if result.get('id') not in active or type(result.get('rc')) is not int:
                        raise ValueError(node+': unknown, duplicate, or malformed worker receipt')
                    task = active.pop(result['id'])
                    if policy is not None:
                        source.release(task['id'], node)
                    with lock:
                        with (run/'jobs.jsonl').open('a') as history:
                            history.write(json.dumps(dict(node=node, task=task, result=result, at=time.time()), ensure_ascii=True)+'\n')
                        key = task.get('retry_of', task['id'])
                        if result['rc'] == 1:
                            # GNU rm rc=1 is a partial/ordinary removal error.
                            # Preserve this job and keep the healthy fleet busy.
                            failed_events += 1
                            failed_jobs[key] = task
                            with (run/'failed-jobs.jsonl').open('a') as failed:
                                failed.write(json.dumps(dict(node=node, task=task, result=result,
                                    at=time.time()), ensure_ascii=True)+'\n')
                            continue
                        if result['rc']:
                            raise RuntimeError(node+': rm failed: '+json.dumps(result))
                        failed_jobs.pop(key, None)
                        completed += 1
                connection_done.set()
                control_thread.join(timeout=20)
                send(dict(kind='cancel' if stop.is_set() else 'finish'))
                with write_lock:
                    proc.stdin.close()
                if proc.wait(timeout=20) and not stop.is_set():
                    raise RuntimeError(node+': worker failed')
            except Exception as exc:
                if request is not None and not claimed.is_set():
                    joins.reject(request, 'join failed before its first task: '+repr(exc))
                else:
                    abort(repr(exc))
            finally:
                connection_done.set()
                cleanup_errors = []
                try:
                    control_thread.join(timeout=20)
                except Exception as exc:
                    cleanup_errors.append('control join: '+repr(exc))
                try:
                    with write_lock:
                        if not proc.stdin.closed:
                            proc.stdin.close()
                except Exception as exc:
                    cleanup_errors.append('stdin close: '+repr(exc))
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        proc.terminate()
                        proc.wait(timeout=10)
                    except Exception as exc:
                        cleanup_errors.append('worker termination: '+repr(exc))
                except Exception as exc:
                    cleanup_errors.append('worker wait: '+repr(exc))
                try:
                    proc.stdout.close()
                except Exception as exc:
                    cleanup_errors.append('stdout close: '+repr(exc))
                if cleanup_errors:
                    reason = node+': dispatch cleanup failed: '+'; '.join(cleanup_errors)
                    if request is not None and not claimed.is_set():
                        joins.reject(request, reason)
                    else:
                        stop.set()
                        abort(reason)
                # Never release/requeue an unconfirmed in-flight task. A
                # global abort prevents new claims; the replacement workflow
                # must prove old processes exited before rebuilding work.

    def fleet(source, finished, allow_runtime=False):
        with futures.ThreadPoolExecutor(max_workers=len(NODES)) as pool:
            if not allow_runtime:
                jobs = [pool.submit(dispatch, node, source, finished) for node in execution_nodes]
                for job in futures.as_completed(jobs):
                    job.result()
                return
            joins = RuntimeJoins(run, cfg, source, finished, stop, execution_nodes)
            runtime_manager[0] = joins
            jobs = {pool.submit(dispatch, node, source, finished, joins) for node in execution_nodes}
            try:
                while jobs:
                    jobs.update(joins.poll(lambda request: pool.submit(
                        dispatch, request['node'], source, finished, joins, request)))
                    done, jobs = futures.wait(jobs, timeout=.2, return_when=futures.FIRST_COMPLETED)
                    for job in done:
                        job.result()
            except BaseException as exc:
                # Wake dispatchers and the bounded producer before any shared
                # error-file write, even if that write itself fails. Otherwise
                # closing admission with pending work can deadlock pool exit.
                stop.set()
                abort('runtime fleet failed: '+repr(exc))
                raise
            finally:
                joins.close()
                joins.poll()
                with joins.lock:
                    execution_nodes[:] = joins.nodes

    def retry_failed():
        # At most two additional attempts per failed job. Retries run after
        # normal work, never while the previous owner might still be active.
        for _ in range(2):
            with lock:
                todo = [(key, task) for key, task in failed_jobs.items()
                        if task.get('retry_attempt', 0) < 2]
            if not todo or stop.is_set():
                return
            for offset in range(0, len(todo), 4096):
                source = new_source()
                for key, task in todo[offset:offset+4096]:
                    source.put(dict(task, id=uuid.uuid4().hex, retry_of=key,
                                    retry_attempt=task.get('retry_attempt', 0)+1))
                finished = threading.Event()
                finished.set()
                state('retrying_failed_jobs', retry_jobs=len(todo))
                fleet(source, finished)
                if stop.is_set():
                    return

    try:
        state('checking_nodes')
        atomic(run/'launch-preflight.json', preflight(script, cfg.get('execution_nodes', NODES)))
        monitor_thread = threading.Thread(target=heartbeat, daemon=True)
        monitor_thread.start()
        state('rm_running')
        producer = threading.Thread(target=produce, daemon=True)
        producer.start()
        fleet(pending, planned, allow_runtime=cfg.get('runtime_join', False))
        producer.join(timeout=15)
        retry_failed()
        if not stop.is_set():
            for depth in sorted({x['depth'] for x in scaffolds}, reverse=True):
                source = new_source()
                for task in scaffolds:
                    if task['depth'] == depth:
                        task['id'] = uuid.uuid4().hex
                        source.put(task)
                finished = threading.Event()
                finished.set()
                state('removing_partition_parents', depth=depth)
                fleet(source, finished)
                retry_failed()
                if stop.is_set():
                    break
        if stop.is_set():
            atomic(run/'STOP', dict(reason='stopped_or_error', at=time.time()))
            state('stopped_or_error')
        elif 'supplement_of' in cfg:
            with lock:
                unresolved = list(failed_jobs.values())
            if unresolved:
                atomic(run/'remaining-jobs.json', unresolved)
            state('supplement_finished_with_errors' if unresolved else 'supplement_complete')
        else:
            result = subprocess.run(ssh('master-0', script, '_verify', run), capture_output=True, text=True, timeout=25)
            if result.returncode:
                raise RuntimeError('root verification failed: '+result.stderr[-1000:])
            if json.loads(result.stdout)['empty']:
                with lock:
                    failed_jobs.clear()
                state('complete_root_retained')
            else:
                with lock:
                    atomic(run/'remaining-jobs.json', list(failed_jobs.values()))
                state('finished_with_remaining_entries')
    except BaseException as exc:
        abort(repr(exc))
        state('stopped_or_error')
        raise
    finally:
        stop.set()
        stop_sources()
        if producer is not None:
            producer.join(timeout=15)
        if monitor_thread is not None:
            monitor_thread.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'start', 'status', 'stop', 'parent-limit', '_check', '_plan', '_worker', '_run', '_verify'))
    parser.add_argument('--run', type=Path, default=RUN)
    parser.add_argument('--node', choices=NODES)
    parser.add_argument('--per-node', type=int, default=8)
    parser.add_argument('--limit', choices=('off', '1', '2'))
    args = parser.parse_args()
    run = args.run.resolve()
    if run == Path(TARGET) or Path(TARGET) in run.parents:
        raise ValueError('control directory must be outside the deletion target')
    if args.action == 'prepare':
        if not 1 <= args.per_node <= 32:
            raise ValueError('--per-node must be 1..32')
        prepare(run, args.per_node)
    elif args.action == 'start':
        with (run/'start-claim.json').open('x') as stream:
            json.dump(dict(at=time.time()), stream)
            stream.flush()
            os.fsync(stream.fileno())
        with (run/'logs/controller.log').open('ab', buffering=0) as log:
            proc = subprocess.Popen([sys.executable, '-S', '-B', '-u', str(run/'rm24.py'), '_run', '--run', str(run)],
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        atomic(run/'controller.json', identity(proc.pid))
        print(json.dumps(dict(controller_pid=proc.pid, run=str(run))))
    elif args.action == 'stop':
        atomic(run/'STOP', dict(at=time.time()))
        print('STOP published; workers interrupt only their own rm children.')
    elif args.action == 'status':
        print(json.dumps(load(run/'status.json'), ensure_ascii=False, indent=2))
    elif args.action == 'parent-limit':
        if args.limit is None:
            parser.error('parent-limit requires --limit off|1|2')
        cfg = load(run/'config.json')
        modules = parent_helpers(run, cfg)
        result = modules['parent_tuning.py'].publish_limit(
            run, None if args.limit == 'off' else int(args.limit),
            expected_target=TARGET, expected_root_inode=ROOT_INODE)
        print(json.dumps(result), flush=True)
    elif args.action == '_check':
        print(json.dumps(node_check(args.node, reject_running_rm=True)))
    elif args.action == '_run':
        controller(run)
    elif args.action == '_worker':
        worker(run, args.node)
    else:
        node_check(args.node)
        root = Root(TARGET, ROOT_INODE)
        try:
            if args.action == '_verify':
                with root.parent('', ROOT_INODE) as fd, os.scandir(fd) as entries:
                    print(json.dumps(dict(empty=next(entries, None) is None)))
            else:
                for item in plan(root):
                    print(json.dumps(item, ensure_ascii=True), flush=True)
        finally:
            root.close()


if __name__ == '__main__':
    main()
