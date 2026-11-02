#!/usr/bin/env python3
"""Read-only, bounded readiness verification; never start, stop, or signal a task."""
import concurrent.futures as futures
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time

import rm24


TERMINAL_COMPLETE = {'supplement_complete', 'complete_root_retained'}
FATAL = {'stopped_or_error', 'supplement_finished_with_errors', 'finished_with_remaining_entries'}
MAX_PROBES = 8
SETTLE_SECONDS = .5


def _controller_alive(actor):
    try:
        fields = Path('/proc/'+str(actor['pid'])+'/stat').read_text().rsplit(')', 1)[1].split()
        return fields[0] != 'Z' and rm24.identity(actor['pid']) == actor
    except (FileNotFoundError, ProcessLookupError):
        return False


def _identity(actor):
    if (not isinstance(actor, dict) or type(actor.get('pid')) is not int or actor['pid'] <= 0
            or not isinstance(actor.get('start'), str) or not actor['start']
            or not isinstance(actor.get('boot'), str) or not actor['boot']):
        raise ValueError('invalid process identity')
    return {key: actor[key] for key in ('pid', 'start', 'boot')}


def _control(run, nodes, expected=None):
    cfg_bytes = (run/'config.json').read_bytes()
    cfg = json.loads(cfg_bytes)
    if (cfg.get('target') != rm24.TARGET or cfg.get('root_inode') != rm24.ROOT_INODE
            or cfg.get('supplement_of') != str(rm24.RUN.resolve())
            or cfg.get('nodes') != rm24.NODES or cfg.get('node_hosts') != rm24.NODE_HOSTS
            or cfg.get('runtime_join') is not True or type(cfg.get('per_node')) is not int
            or cfg['per_node'] != 4):
        raise ValueError('readiness configuration scope/registry differs')
    if set(cfg.get('execution_nodes', ())) != set(nodes):
        raise ValueError('readiness must verify the complete initial execution-node set')
    frozen = run/'rm24.py'
    frozen_sha = hashlib.sha256(frozen.read_bytes()).hexdigest()
    if cfg.get('sha256') != frozen_sha:
        raise ValueError('frozen worker code changed')
    actor = rm24.load(run/'controller.json')
    if _identity(actor) != actor:
        raise ValueError('controller identity contains unexpected fields')
    binding = dict(run=str(run), controller=actor,
                   config_sha256=hashlib.sha256(cfg_bytes).hexdigest(), frozen_sha256=frozen_sha)
    if expected is not None and binding != expected:
        raise ValueError('run/controller/config binding changed during readiness')
    status = rm24.load(run/'status.json')
    if ((run/'STOP').exists() or (run/'first-error.json').exists()
            or status.get('state') in FATAL or status.get('errors')):
        raise RuntimeError('replacement stopped or failed; no tasks were stopped by verifier')
    if status.get('state') in TERMINAL_COMPLETE and time.time()-status['updated'] <= 15:
        return binding, status, True
    if not _controller_alive(actor):
        raise RuntimeError('replacement controller exited or identity changed')
    return binding, status, False


def _expected_cmdline(run, node):
    return ['-S', '-B', '-u', str(run/'rm24.py'), '_worker', '--node', node, '--run', str(run)]


def _claim_identity(run, node, actor):
    process = _identity(actor)
    if (actor.get('node') != node or actor.get('host') != rm24.NODE_HOSTS[node]
            or actor.get('script') != str(run/'rm24.py')):
        raise ValueError(node+': shared worker claim binding differs')
    return process


def _probe_worker(run, node, actor, deadline):
    remaining = deadline-time.monotonic()
    if remaining <= 0:
        raise TimeoutError('readiness deadline reached before worker probe')
    # Only /proc and hostname are read on the remote node; no project files or
    # worker code are executed by this probe.
    code = ('import json,os,pathlib,socket\n'
            'p=pathlib.Path("/proc")/'+repr(str(actor['pid']))+'\n'
            'fields=(p/"stat").read_text().rsplit(")",1)[1].split()\n'
            'print(json.dumps(dict(node='+repr(node)+',host=socket.gethostname(),'
            'pid='+str(actor['pid'])+',start=fields[19],state=fields[0],'
            'boot=pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip(),'
            'cmdline=[os.fsdecode(x) for x in (p/"cmdline").read_bytes().split(b"\\0") if x])))\n')
    result = subprocess.run(['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
                             '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=1',
                             node, shlex.join(['python3', '-S', '-B', '-'])],
                            input=code, capture_output=True, text=True, timeout=min(12, remaining))
    if result.returncode:
        raise OSError(node+': read-only SSH probe failed: '+result.stderr[-500:])
    proof = json.loads(result.stdout)
    if (not isinstance(proof, dict) or proof.get('node') != node
            or proof.get('host') != rm24.NODE_HOSTS[node]
            or any(proof.get(key) != actor[key] for key in ('pid', 'start', 'boot'))
            or proof.get('state') not in ('R', 'S', 'D', 'I')
            or not isinstance(proof.get('cmdline'), list) or not proof['cmdline']
            or Path(proof['cmdline'][0]).name != 'python3'
            or proof['cmdline'][1:] != _expected_cmdline(run, node)):
        raise ValueError(node+': live worker identity/hostname/cmdline differs')
    return proof


def wait_workers_ready(run, nodes, timeout=90):
    """Confirm every initial worker or report natural completion separately.

    SSH failures can retry within the shared deadline. A refusal/timeout never
    publishes STOP, rolls back, or repeats an operator's launch.
    """
    if type(timeout) not in (int, float) or not 0 < timeout <= 600:
        raise ValueError('timeout must be in (0, 600] seconds')
    run = Path(run).resolve()
    if run == Path(rm24.TARGET) or Path(rm24.TARGET) in run.parents:
        raise ValueError('control directory must be outside target')
    nodes = list(nodes)
    if not nodes or len(nodes) != len(set(nodes)) or any(node not in rm24.NODES for node in nodes):
        raise ValueError('invalid readiness node set')
    deadline, binding, last = time.monotonic()+timeout, None, {}

    def terminal(status):
        return dict(state='terminal_complete', terminal_complete=True, verified_nodes=0,
                    nodes=[], proofs=[], status=status, controller=binding['controller'])

    while time.monotonic() < deadline:
        try:
            current, status, complete = _control(run, nodes, binding)
            binding = current
            if complete:
                return terminal(status)
            fresh = time.time()-status['updated'] <= 15
            if not fresh or status.get('state') != 'rm_running':
                last = dict(status='not freshly rm_running', state=status.get('state'))
            else:
                claims, missing = {}, []
                for node in nodes:
                    try:
                        actor = rm24.load(run/'workers'/(node+'.json'))
                    except (OSError, json.JSONDecodeError):
                        missing.append(node)
                        continue
                    claims[node] = _claim_identity(run, node, actor)
                if missing:
                    last = dict(missing_claims=missing)
                else:
                    proofs, failed = [], {}
                    with futures.ThreadPoolExecutor(max_workers=min(MAX_PROBES, len(nodes))) as pool:
                        jobs = {pool.submit(_probe_worker, run, node, claims[node], deadline): node for node in nodes}
                        for job in futures.as_completed(jobs):
                            node = jobs[job]
                            try:
                                proofs.append(job.result())
                            except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
                                failed[node] = str(exc)
                    if failed:
                        last = dict(unconfirmed_probes=failed)
                    else:
                        remaining = deadline-time.monotonic()
                        if remaining < SETTLE_SECONDS:
                            break
                        time.sleep(SETTLE_SECONDS)
                        _, final, complete = _control(run, nodes, binding)
                        if complete:
                            return terminal(final)
                        if final.get('state') == 'rm_running' and time.time()-final['updated'] <= 15:
                            unchanged = all(_claim_identity(run, node, rm24.load(run/'workers'/(node+'.json'))) == claims[node]
                                            for node in nodes)
                            if unchanged:
                                return dict(state='workers_ready', terminal_complete=False,
                                            verified_nodes=len(nodes), nodes=nodes,
                                            proofs=sorted(proofs, key=lambda row: nodes.index(row['node'])),
                                            status=final, controller=binding['controller'])
                        last = dict(status='worker claims or main phase changed during stability check')
        except (OSError, json.JSONDecodeError) as exc:
            last = dict(shared_metadata=str(exc))
        remaining = deadline-time.monotonic()
        if remaining > 0:
            time.sleep(min(1, remaining))
    raise RuntimeError('worker readiness unconfirmed at timeout; no tasks stopped or relaunched: '+json.dumps(last))
