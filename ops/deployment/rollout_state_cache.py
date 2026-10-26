"""Roll an immutable cached-state launcher onto drained, idle copy processes."""
import argparse
import base64
import datetime
import fcntl
import json
import pathlib
import select
import shlex
import subprocess
import sys
import time
import uuid

from migration_io import read_json, write_json
import migration_launch as transport

BASE = pathlib.Path(__file__).absolute().parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--nodes', nargs='+', default=['worker-4'])
    ap.add_argument('--prepare-only', action='store_true')
    ap.add_argument('--new-unused-bundle', action='store_true')
    ap.add_argument('--gate-timeout', type=int, default=600)
    a = ap.parse_args()
    active = read_json(BASE / 'active_migration.json')
    transport.JOB = active['job']
    control = active['control']
    deployment_path = BASE / 'state_cache_rollout.json'
    lock = open(BASE / 'state_cache_rollout.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def show(kind, value):
        print(kind + ' ' + json.dumps(value, ensure_ascii=False), flush=True)

    try:
        deployment = read_json(deployment_path)
        assert deployment['control'] == control and deployment['job'] == active['job']
        if a.new_unused_bundle:
            assert deployment['status'] == 'prepared' and not deployment['nodes']
            raise FileNotFoundError('replace unused preparation with new immutable bundle')
    except FileNotFoundError:
        tag = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:6]
        bundle = control + '/state-cache-workers-' + tag
        new_files = {n: base64.b64encode((BASE / n).read_bytes()).decode() for n in (
            'migration_cached_worker.py', 'migration_state_cache.py', 'state_cache_rollout_helper.py')}
        code = r'''import base64,hashlib,json,pathlib,py_compile,sys
c=pathlib.Path(CONTROL);b=pathlib.Path(BUNDLE);b.mkdir(mode=0o700)
sys.path.insert(0,str(c))
from migration_io import read_json,write_json
assert not (c/'STOP').exists()
deployment=read_json(c/'pipeline-deployment.json')
old_bundle=pathlib.Path(deployment['bundle'])
for name in ('migration_run.py','migration_worker.py','migration_scan.py','migration_io.py','migration_nodes.py'):
 (b/name).write_bytes((c/name).read_bytes())
(b/'migration_worker128_run.py').write_bytes((old_bundle/'migration_worker128_run.py').read_bytes())
for name,value in FILES.items():(b/name).write_bytes(base64.b64decode(value))
hashes={}
for p in b.glob('*.py'):
 p.chmod(0o700);py_compile.compile(str(p),doraise=True);hashes[p.name]=hashlib.sha256(p.read_bytes()).hexdigest()
proof={'control':str(c),'bundle':str(b),'hashes':hashes,'original_supplement_bundle':str(old_bundle)}
write_json(b/'bundle-proof.json',proof)
print(json.dumps(proof))
'''.replace('CONTROL', repr(control)).replace('BUNDLE', repr(bundle)).replace('FILES', repr(new_files))
        _, out = transport.ssh('master-0', code)
        proof = json.loads(out)
        deployment = dict(proof, job=active['job'], status='prepared', nodes={}, created=time.time())
        write_json(deployment_path, deployment)
        show('PREPARED', {'bundle': bundle, 'files': len(proof['hashes'])})
    bundle = deployment['bundle']
    if a.prepare_only:
        return

    def command(node, action, host=None):
        destination = transport.ssh_destination(node, job=active['job'], owner=active.get('ssh_owner'))
        args = ['python3', '-u', bundle + '/state_cache_rollout_helper.py', '--control', control,
                '--action', action]
        if host is not None: args += ['--host', host, '--timeout', str(a.gate_timeout)]
        return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20', '-o', 'ServerAliveInterval=20',
                '-p', '2222', destination, shlex.join(args)]

    def call(node, action, host, timeout=180):
        r = subprocess.run(command(node, action, host), stdin=subprocess.DEVNULL,
                           text=True, capture_output=True, timeout=timeout)
        lines = [x[7:] for x in r.stdout.splitlines() if x.startswith('RESULT ')]
        if r.returncode or not lines:
            # Helper never logs credentials; preserve exact diagnostics locally.
            (BASE / 'state_cache_rollout_error.txt').write_text(r.stdout + r.stderr)
            raise RuntimeError('remote ' + action + ' failed on ' + node + ': ' + r.stderr[-2000:])
        return json.loads(lines[-1])

    hold = subprocess.Popen(command('master-0', 'hold'), stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        readable, _, _ = select.select([hold.stdout], [], [], 50)
        if not readable: raise RuntimeError('master orchestration lock timed out')
        line = hold.stdout.readline()
        if not line.startswith('RESULT ') or not json.loads(line[7:]).get('locked'):
            raise RuntimeError('master orchestration lock not acquired')
        deployment.update(status='rolling', updated=time.time())
        write_json(deployment_path, deployment)
        for node in a.nodes:
            assert node in transport.HOSTS
            host = 'job-' + active['job'] + '-' + node
            prior = deployment['nodes'].get(node, {})
            if prior.get('status') == 'complete':
                show('ALREADY_DONE', {'node': node}); continue
            if prior.get('status') not in (None, 'waiting_payload', 'skipped_preexisting_drain'):
                raise RuntimeError('unfinished checkpoint requires inspection before retry: ' + node)
            entry = {'host': host, 'status': 'drain_requested', 'started': time.time()}
            deployment['nodes'][node] = entry
            write_json(deployment_path, deployment)
            show('DRAIN', {'node': node})
            drain = call('master-0', 'drain', host)
            if drain.get('skipped'):
                entry.update(status='skipped_preexisting_drain', detail=drain, updated=time.time())
                write_json(deployment_path, deployment); show('SKIP', entry); continue
            entry['status'] = 'draining'; write_json(deployment_path, deployment)
            ready = call('master-0', 'gate', host, timeout=a.gate_timeout + 90)
            entry['gate'] = ready
            if not ready.get('ready'):
                release = call('master-0', 'release', host)
                entry.update(status='waiting_payload', release=release, updated=time.time())
                write_json(deployment_path, deployment); show('WAITING', {'node': node, **ready})
                if ready.get('terminal_or_stop'): break
                continue
            entry['status'] = 'idle_verified'; write_json(deployment_path, deployment)
            show('REPLACE_IDLE', {'node': node, 'wait_seconds': ready.get('wait_seconds')})
            # On a replacement failure retain the drain until a human/root has
            # inspected actual processes and checkpoint; never release blindly.
            replaced = call(node, 'replace', host, timeout=180)
            entry.update(status='workers_ready', replacement=replaced, updated=time.time())
            write_json(deployment_path, deployment)
            release = call('master-0', 'release', host)
            entry.update(status='complete', release=release, updated=time.time())
            write_json(deployment_path, deployment)
            show('COMPLETE', {'node': node, **replaced})
        deployment.update(status='pass_finished', updated=time.time())
        write_json(deployment_path, deployment)
    except BaseException as exc:
        deployment.update(status='attention', last_error=type(exc).__name__ + ': ' + str(exc), updated=time.time())
        write_json(deployment_path, deployment)
        raise
    finally:
        if hold.stdin: hold.stdin.close()
        try: hold.wait(timeout=15)
        except subprocess.TimeoutExpired: hold.terminate()


if __name__ == '__main__':
    main()
