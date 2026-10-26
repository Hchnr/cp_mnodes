#!/usr/bin/env python3
"""One-shot, identity-bound replacement of a single read-only target probe.

Stage a new, immutable flat source bundle directly in the existing control
directory. Its source-sha256.json must cover every staged file, including this
helper and expected-roles.json (a node -> complete predecessor role mapping).
Only the frozen predecessor probe is signalled. An ambiguous attempt is never
retried automatically: inspect its durable roles/NODE/operation.json first.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from migration_io import read_json, write_json


REQUIRED_SOURCES = frozenset((
    'deploy_fast_open_target_probes.py', 'migration_probe_fast_open.py',
    'migration_target_probe.py', 'migration_linux_open.py', 'expected-roles.json'))


def identity(pid):
    try:
        proc = Path('/proc') / str(pid)
        fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        argv = (proc / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
        return {'pid': pid, 'starttime_ticks': fields[19], 'state': fields[0], 'argv': argv}
    except (OSError, ValueError, IndexError):
        return None


def same_process(actual, expected):
    return (isinstance(actual, dict) and actual.get('state') in ('R', 'S', 'D', 'I', 'T', 't', 'W', 'P')
            and type(actual.get('pid')) is int and actual['pid'] > 1
            and isinstance(actual.get('argv'), list)
            and actual.get('pid') == expected.get('pid')
            and str(actual.get('starttime_ticks')) == str(expected.get('starttime_ticks'))
            and actual.get('argv') == expected.get('argv'))


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def validate_sources(bundle):
    """Bind all flat staged files; runtime roles and bytecode are not sources."""
    bundle = Path(bundle)
    manifest_path = bundle / 'source-sha256.json'
    if manifest_path.is_symlink():
        raise ValueError('source manifest must not be a symlink')
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or not REQUIRED_SOURCES <= manifest.keys():
        raise ValueError('source manifest does not cover required staged files')
    present = set()
    for path in bundle.iterdir():
        if path.is_symlink():
            raise ValueError('staged paths must not be symlinks')
        if path.is_dir():
            if path.name not in ('roles', '__pycache__'):
                raise ValueError('unexpected staged source directory')
        elif path.name != 'source-sha256.json':
            present.add(path.name)
    if present != set(manifest):
        raise ValueError('source manifest must cover every staged flat file')
    for name, digest in manifest.items():
        if (not isinstance(name, str) or Path(name).name != name
                or name in ('.', '..') or not isinstance(digest, str)
                or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest)):
            raise ValueError('invalid source manifest entry')
        path = bundle / name
        if (not path.is_file() or path.is_symlink()
                or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
            raise RuntimeError('staged source digest mismatch: ' + name)
    return hashlib.sha256(raw).hexdigest()


def validate_role(control, bundle, node, cfg):
    """Validate a frozen full role, not a guessed PID or mutable role lookup."""
    if (not isinstance(node, str) or not node or Path(node).name != node
            or node in ('.', '..')):
        raise ValueError('invalid node name')
    roles = json.loads((bundle / 'expected-roles.json').read_bytes())
    if not isinstance(roles, dict) or not isinstance(roles.get(node), dict):
        raise ValueError('expected-roles.json must contain a full node role')
    old = roles[node]
    if (old.get('control') != str(control) or old.get('run_id') != cfg.get('run_id')
            or old.get('host') != socket.gethostname()
            or not old['host'].endswith('-' + node)
            or old.get('slots') != 64 or old.get('stop_cache_seconds') != .1
            or old.get('batch_limit') != 64
            or (old.get('source'), old.get('destination')) !=
               (cfg.get('source'), cfg.get('destination'))
            or type(old.get('pid')) is not int or old['pid'] <= 1
            or not str(old.get('starttime_ticks', '')).isdigit()
            or type(old.get('port')) is not int or not 1 <= old['port'] <= 65535):
        raise ValueError('frozen role does not match this node/run/limits')
    endpoint = urlsplit(old.get('endpoint', ''))
    if (endpoint.scheme != 'http' or not endpoint.hostname or endpoint.port != old['port']
            or endpoint.username or endpoint.password or endpoint.path or endpoint.query
            or endpoint.fragment or ':' in endpoint.hostname):
        raise ValueError('frozen probe endpoint is not an exact IPv4 endpoint')
    # Numeric addresses prevent a DNS change from changing the advertised node.
    try:
        socket.inet_pton(socket.AF_INET, endpoint.hostname)
    except OSError as exc:
        raise ValueError('probe advertise address must be numeric IPv4') from exc
    argv = old.get('argv')
    if not isinstance(argv, list) or len(argv) < 3 or not isinstance(argv[2], str):
        raise ValueError('frozen role must include full argv')
    old_entry = Path(argv[2])
    predecessor = old_entry.parent
    root_cold_probe = (predecessor == control and cfg.get('cold_frontier_v1') is True
                      and old['host'] == 'job-' + str(cfg.get('job')) + '-' + node
                      and old_entry.name == 'migration_probe_fast_open.py')
    if (not old_entry.is_absolute() or (predecessor.parent != control and not root_cold_probe) or predecessor == bundle
            or old_entry.name not in ('migration_target_probe.py', 'migration_probe_fast_open.py',
                                      'migration_probe_path_open.py', 'migration_probe_batch_parent.py')):
        raise ValueError('predecessor entry is not a probe directly within this control')
    if root_cold_probe:
        old_record = control / 'probe-roles' / (old['host'] + '.json')
        expected_argv = ['/usr/bin/python3', '-u', str(old_entry), '--control', str(control),
                         '--port', str(old['port']), '--slots', '64', '--stop-cache-seconds', '0.1',
                         '--advertise-address', endpoint.hostname, '--record', str(old_record)]
    else:
        old_record = predecessor / 'roles' / node / 'probe.json'
        expected_argv = probe_argv(old_entry, control, old['port'], endpoint.hostname, old_record)
    if argv != expected_argv:
        raise ValueError('frozen role full argv does not match its run/node/limits')
    return old, endpoint.hostname


def probe_argv(entry, control, port, address, record):
    return ['/usr/bin/python3', '-u', str(entry), '--control', str(control),
            '--port', str(port), '--advertise-address', address, '--slots', '64',
            '--record', str(record), '--stop-cache-seconds', '0.1']


def new_entrypoint(control, bundle, cfg):
    policy_path = bundle / 'parent-open.json'
    batch_path = bundle / 'batch-parent.json'
    if batch_path.exists():
        if policy_path.exists():
            raise ValueError('only one parent strategy may be selected')
        policy = json.loads(batch_path.read_bytes())
        if policy != {'version': 1, 'control': str(control), 'run_id': cfg['run_id'],
                      'parent_reuse': 'request-scoped'}:
            raise ValueError('batch parent policy is not bound to this run')
        entry = bundle / 'migration_probe_batch_parent.py'
        manifest = json.loads((bundle / 'source-sha256.json').read_bytes())
        if entry.name not in manifest or batch_path.name not in manifest:
            raise ValueError('batch parent policy and entry must be source-manifest bound')
        return entry
    if not policy_path.exists():
        return bundle / 'migration_probe_fast_open.py'
    policy = json.loads(policy_path.read_bytes())
    if policy != {'version': 1, 'control': str(control), 'run_id': cfg['run_id'],
                  'parent_open': 'O_PATH'}:
        raise ValueError('O_PATH probe policy is not bound to this run')
    entry = bundle / 'migration_probe_path_open.py'
    manifest = json.loads((bundle / 'source-sha256.json').read_bytes())
    if entry.name not in manifest:
        raise ValueError('O_PATH entry is not source-manifest bound')
    return entry


def valid_health(health, child_pid, argv, old, control, cfg):
    return (isinstance(health, dict) and health.get('ok') is True
            and health.get('pid') == child_pid and health.get('argv') == argv
            and health.get('slots') == 64 and health.get('batch_limit') == 64
            and health.get('stop_cache_seconds') == .1
            and health.get('control') == str(control) and health.get('run_id') == cfg['run_id']
            and health.get('host') == old['host'] and health.get('port') == old['port']
            and health.get('endpoint') == old['endpoint']
            and (Path(argv[2]).name != 'migration_probe_path_open.py'
                 or health.get('parent_open_mode') == 'O_PATH')
            and (Path(argv[2]).name != 'migration_probe_batch_parent.py'
                 or health.get('batch_parent_reuse') == 'request-scoped')
            and same_process(identity(child_pid), health))


def protocol_checks(request, marker):
    """Only read target metadata; reject unsafe paths before filesystem work."""
    status, unauthorized = request('/health', authenticated=False)
    if status != 401:
        raise RuntimeError('probe authentication validation failed')

    def metadata_request(route, body):
        # Production batches can occupy all metadata slots during validation.
        # Retry only these read-only checks, never a replacement transaction.
        deadline = time.monotonic() + 10
        while True:
            status, result = request(route, body)
            busy = status == 503
            if route == '/probe-batch' and status == 200 and isinstance(result, dict):
                rows = result.get('results')
                # The batch envelope can succeed while individual items are
                # temporarily slot-limited. Retry that read-only observation,
                # not the replacement. Never retry away malformed/safety errors.
                busy |= (result.get('ok') is True and isinstance(rows, list)
                         and len(rows) == len(body['entries'])
                         and all(isinstance(row, dict) and row.get('status') in (200, 503)
                                 for row in rows)
                         and any(row['status'] == 503 for row in rows))
            if not busy or time.monotonic() >= deadline:
                return status, result
            time.sleep(.1)

    entry = {'rel': 'datasets/__fast_open_validation_' + marker, 'kind': 'file', 'size': 0}
    status, single = metadata_request('/probe', {'entry': entry})
    status2, batch = metadata_request('/probe-batch', {'entries': [entry, entry]})
    if (status != 200 or single.get('ok') is not True or single.get('missing') is not True
            or status2 != 200 or batch.get('ok') is not True
            or len(batch.get('results', [])) != 2
            or any(row.get('status') != 200 or row.get('result') != single
                   for row in batch['results'])):
        raise RuntimeError('single/batch compatibility check failed')
    bad = {'rel': '../__unsafe_probe_validation', 'kind': 'file', 'size': 0}
    if (request('/probe', {'entry': bad})[0] != 400
            or request('/probe-batch', {'entries': [entry, bad]})[0] != 400):
        raise RuntimeError('unsafe path rejection validation failed')
    return {'auth_401': True, 'single_probe_200': True, 'batch_probe_200': True,
            'ordered_compatible_results': True, 'unsafe_path_400': True}


def run_node(control, bundle, node):
    control, bundle = Path(control).resolve(), Path(bundle).resolve()
    if bundle.parent != control:
        raise ValueError('probe rollout bundle must be directly within this control')
    cfg = read_json(control / 'config.json')
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present')
    manifest_digest = validate_sources(bundle)
    old, address = validate_role(control, bundle, node, cfg)
    entry = new_entrypoint(control, bundle, cfg)
    if not same_process(identity(old['pid']), old):
        raise RuntimeError('live predecessor PID/startticks/full argv do not match')
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('probe NOFILE hard limit is too low')
    if soft < 65536:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, hard))
    role_dir = bundle / 'roles' / node
    if (bundle / 'roles').is_symlink() or role_dir.is_symlink():
        raise ValueError('runtime role directory must not be a symlink')
    role_dir.mkdir(parents=True, exist_ok=True)
    operation_path = role_dir / 'operation.json'
    new_record = role_dir / 'probe.json'
    if new_record.exists() or (role_dir / 'probe.log').exists():
        raise RuntimeError('new role/log exists; inspect this bundle before any retry')
    new_argv = probe_argv(entry, control, old['port'], address,
                          new_record)
    operation = {'node': node, 'control': str(control), 'bundle': str(bundle), 'started': now(),
                 'state': 'validated', 'old_identity': old, 'new_argv': new_argv,
                 'source_manifest_sha256': manifest_digest}
    # Exclusive durable operation marker is the no-retry boundary before signal.
    with open(operation_path, 'x') as handle:
        json.dump(operation, handle, ensure_ascii=True)
        handle.flush()
        os.fsync(handle.fileno())
    directory_fd = os.open(role_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)

    def save(**values):
        operation.update(values, updated=now())
        write_json(operation_path, operation)

    try:
        # Revalidate the complete immutable binding immediately before signal.
        if (validate_sources(bundle) != manifest_digest or (control / 'STOP').exists()
                or not same_process(identity(old['pid']), old)):
            raise RuntimeError('predecessor/source/STOP changed before scoped signal')
        until = time.monotonic() + 20
        os.kill(old['pid'], signal.SIGTERM)
        save(state='old_probe_signalled')
        while True:
            exiting = identity(old['pid'])
            if (exiting is None or exiting.get('state') == 'Z'
                    or str(exiting.get('starttime_ticks')) != str(old['starttime_ticks'])):
                break
            if time.monotonic() >= until:
                raise TimeoutError('predecessor probe did not exit after SIGTERM')
            time.sleep(.1)
        # Exit and lock teardown share one deadline. Never kill an unknown holder.
        with open('/tmp/' + cfg['run_id'] + '-targetprobe.lock', 'r+') as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= until:
                        raise TimeoutError('probe lock remains held after predecessor exit')
                    time.sleep(.1)
        if (control / 'STOP').exists() or validate_sources(bundle) != manifest_digest:
            raise RuntimeError('STOP or staged sources changed before probe launch')
        with open(role_dir / 'probe.log', 'xb') as log:
            child = subprocess.Popen(['nice', '-n', '15', 'ionice', '-c', '3', *new_argv],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True, close_fds=True)
        save(state='new_probe_launched', new_pid=child.pid)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        url = 'http://127.0.0.1:' + str(old['port'])

        def request(route, body=None, authenticated=True):
            headers = {'Authorization': 'Bearer ' + cfg['broker_token']} if authenticated else {}
            if body is not None:
                headers['Content-Type'] = 'application/json'
            data = None if body is None else json.dumps(body).encode()
            query = urllib.request.Request(url + route, data=data, headers=headers)
            try:
                with opener.open(query, timeout=10) as response:
                    return response.status, json.load(response)
            except urllib.error.HTTPError as exc:
                return exc.code, json.load(exc)

        until = time.monotonic() + 45
        while True:
            if child.poll() is not None:
                raise RuntimeError('new probe exited; inspect its one-shot log')
            try:
                status, health = request('/health')
                if status == 200 and valid_health(health, child.pid, new_argv, old, control, cfg):
                    break
            except (OSError, ValueError):
                pass
            if time.monotonic() >= until:
                raise TimeoutError('new probe did not validate ready within 45 seconds')
            time.sleep(.2)
        checks = protocol_checks(request, bundle.name)
        save(state='ready', health=health, protocol_checks=checks)
        return {'node': node, 'state': 'ready', 'pid': child.pid,
                'starttime_ticks': health['starttime_ticks'], 'endpoint': old['endpoint'],
                'slots': 64, 'batch_limit': 64, 'operation': str(operation_path),
                'record': str(new_record)}
    except BaseException as exc:
        save(state='failed_inspect_before_retry', error=type(exc).__name__ + ': ' + str(exc))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--bundle', required=True)
    parser.add_argument('--node', required=True)
    args = parser.parse_args()
    print(json.dumps(run_node(args.control, args.bundle, args.node)), flush=True)
