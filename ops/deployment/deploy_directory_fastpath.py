#!/usr/bin/env python3
"""Explicit, lease-free rolling deployment of the directory open-first fix.

Prepare only stages immutable sources. Rollout uses the existing protocol-2
drain/idle/FD/identity/counter guards, with a digest-bound single-file engine
transition. No producer, probe or broker is restarted. An ambiguous replacement
stops the pass; it is never automatically repeated.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import time
import uuid

from migration_io import read_json, write_json
import rollout_cache_age as protocol
from rollout_cache_cluster import BridgeRPC, NODES, LANES


RUNTIME_FILES = (
    'migration_cached_worker.py', 'migration_state_cache.py',
    'migration_run.py', 'migration_worker.py', 'migration_scan.py',
    'migration_worker128_run.py', 'migration_io.py', 'migration_nodes.py',
)
HELPER_FILES = ('rollout_cache_age.py', 'rollout_cache_cluster.py',
                'deploy_directory_fastpath.py', 'migration_launch.py')
NATIVE_FILES = ('migration_linux_open.py', 'worker-directory-policy.json')
NATIVE_POLICY = {'scope': 'native-safe-parent-open-v1'}
PATH_POLICY = {'scope': 'destination-opath-checkpoint-v1'}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_native_root(control):
    """Bind a cold-recovery root already running the native read-only opener.

    Unlike an arbitrary control root, a cold deployment has a complete static
    source manifest. Only its exact, no-symlink runtime may be the predecessor
    of an engine-identical O_PATH canary. Return the subset that the successor
    must bind, so changing both an old source and its root manifest cannot
    silently change the successor's declared base.
    """
    control = Path(control).resolve()
    required = set(RUNTIME_FILES + NATIVE_FILES)
    try:
        manifest = read_json(control / 'source-sha256.json')
    except (OSError, ValueError) as exc:
        raise ValueError('root previous bundle requires a static source manifest') from exc
    if not isinstance(manifest, dict) or not required.issubset(manifest):
        raise ValueError('root previous bundle source manifest is incomplete')
    for name in required:
        source = control / name
        if (source.is_symlink() or source.resolve().parent != control or
                manifest[name] != digest(source)):
            raise ValueError('root previous bundle source digest differs: ' + name)
    if read_json(control / 'worker-directory-policy.json') != NATIVE_POLICY:
        raise ValueError('root previous bundle must use the native read-only policy')
    return {name: manifest[name] for name in sorted(required)}


def verify_bundle(control, bundle, _seen=None):
    control, bundle = Path(control).resolve(), Path(bundle).resolve()
    if bundle.parent != control or not bundle.name.startswith('directory-open-first-'):
        raise ValueError('require a direct immutable directory-open-first bundle')
    seen = set() if _seen is None else set(_seen)
    if bundle in seen:
        raise ValueError('cyclic previous bundle binding')
    seen.add(bundle)
    manifest = read_json(bundle / 'source-sha256.json')
    legacy_names = set(RUNTIME_FILES + HELPER_FILES + ('engine-transition.json',))
    native = set(manifest) == legacy_names | set(NATIVE_FILES)
    if set(manifest) != legacy_names and not native:
        raise ValueError('source manifest does not cover exactly the staged files')
    for name, expected in manifest.items():
        if (Path(name).name != name or (bundle / name).is_symlink()
                or digest(bundle / name) != expected):
            raise ValueError('staged digest mismatch: ' + name)
    cfg = read_json(control / 'config.json')
    if (cfg.get('control'), cfg.get('source'), cfg.get('destination')) != (
            str(control), '/share/project/eai_pwm', '/share-mixed/project/eai_pwm'):
        raise ValueError('wrong control or copy scope')
    transition = read_json(bundle / 'engine-transition.json')
    previous = control
    if native:
        policy = read_json(bundle / 'worker-directory-policy.json')
        if policy not in (NATIVE_POLICY, PATH_POLICY):
            raise ValueError('native worker directory policy differs')
        if (policy == PATH_POLICY) != (transition.get('scope') == PATH_POLICY['scope']):
            raise ValueError('directory policy and transition scope differ')
        previous_value = transition.get('previous_bundle')
        if not isinstance(previous_value, str) or not Path(previous_value).is_absolute():
            raise ValueError('native transition requires an explicit previous bundle')
        previous = Path(previous_value).resolve()
        if (previous_value != str(previous) or previous == bundle or
                (previous != control and previous.parent != control)):
            raise ValueError('native transition previous bundle must be a distinct direct control bundle')
        if previous == control:
            if (policy != PATH_POLICY or
                    transition.get('previous_root_sha256') != verify_native_root(control)):
                raise ValueError('root previous bundle binding differs or is not an O_PATH transition')
        else:
            if 'previous_root_sha256' in transition:
                raise ValueError('nested previous bundle must not claim a root source binding')
            verify_bundle(control, previous, _seen=seen)
    elif 'previous_bundle' in transition:
        raise ValueError('legacy transition must not silently select a different base')
    protocol.validate_engine_transition(previous, bundle,
                                        bundle / 'engine-transition.json', control)
    return cfg, manifest


def prepare(control, previous_bundle=None, destination_opath=False):
    control = Path(control).resolve()
    cfg = read_json(control / 'config.json')
    if cfg.get('control') != str(control) or (control / 'STOP').exists():
        raise ValueError('wrong control or STOP present')
    if destination_opath and previous_bundle is None:
        raise ValueError('destination O_PATH requires an exact previous native bundle')
    previous = control
    previous_root_sha256 = None
    if previous_bundle is not None:
        previous = Path(previous_bundle).resolve()
        if previous == control and destination_opath:
            previous_root_sha256 = verify_native_root(control)
        else:
            verify_bundle(control, previous)
    bundle = control / ('directory-open-first-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    source = Path(__file__).resolve().parent
    for name in RUNTIME_FILES:
        changed = (name == 'migration_worker.py' and not destination_opath) or (previous_bundle is not None and
                                                   name == 'migration_cached_worker.py')
        shutil.copyfile((source if changed else previous) / name,
                        bundle / name)
    for name in HELPER_FILES:
        shutil.copyfile(source / name, bundle / name)
    transition = {
        'scope': PATH_POLICY['scope'] if destination_opath else 'directory-open-first-v1', 'control': str(control),
        'bundle': str(bundle), 'old_sha256': digest(previous / 'migration_worker.py'),
        'new_sha256': digest(bundle / 'migration_worker.py'),
    }
    if previous_bundle is not None:
        transition['previous_bundle'] = str(previous)
        if previous_root_sha256 is not None:
            transition['previous_root_sha256'] = previous_root_sha256
        shutil.copyfile(source / 'migration_linux_open.py', bundle / 'migration_linux_open.py')
        write_json(bundle / 'worker-directory-policy.json', PATH_POLICY if destination_opath else NATIVE_POLICY)
        if destination_opath:
            transition['launcher_sha256'] = {name: digest(bundle / name) for name in (
                'migration_cached_worker.py', 'migration_linux_open.py', 'worker-directory-policy.json')}
    write_json(bundle / 'engine-transition.json', transition)
    names = RUNTIME_FILES + HELPER_FILES + ('engine-transition.json',)
    if previous_bundle is not None:
        names += NATIVE_FILES
    write_json(bundle / 'source-sha256.json', {name: digest(bundle / name) for name in names})
    verify_bundle(control, bundle)
    return {'bundle': str(bundle), 'control': str(control), 'prepared_only': True,
            'worker_changed': transition['old_sha256'] != transition['new_sha256'],
            **({'previous_bundle': str(previous)} if previous_bundle is not None else {})}


class DirectoryRPC(BridgeRPC):
    def __init__(self, *, ssh_aliases=False, **kwargs):
        if type(ssh_aliases) is not bool:
            raise ValueError('ssh_aliases must be an explicit boolean')
        self.ssh_aliases = ssh_aliases
        super().__init__(**kwargs)

    def __call__(self, node, action, *, lane=None, identity=None):
        # A 64-file task can have a >30s tail. Continue only the read-only
        # lease/idle observation while retaining the same drain and operation;
        # never repeat replace or control writes after an uncertain response.
        for attempt in range(3 if action == 'gate' else 1):
            result = super().__call__(node, action, lane=lane, identity=identity)
            if (action != 'gate' or result.get('ready') is not False or
                    result.get('reason') != 'lane not idle within gate; payload retained'):
                return result
            print(json.dumps({'node': self.node, 'lane': self.lane,
                              'status': 'waiting_existing_tasks',
                              'observation_window': attempt + 1}), flush=True)
        return result

    def ssh_command(self, node, remote_command):
        if node not in NODES:
            raise ValueError('unknown physical node')
        if self.ssh_aliases:
            # Current user-managed bridges expose master-0 / worker-N directly.
            # Retain their configured agent, identity and port; the remote
            # protocol still checks the exact job hostname and PID/start ticks.
            return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                    '-o', 'ServerAliveInterval=15', node, remote_command]
        command = ['ssh', '-o', 'IdentityAgent=none', '-o', 'BatchMode=yes',
                   '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15']
        if node == 'master-0':
            command += ['copy-cluster-entry-1e56']
        else:
            jump = shlex.join(['ssh', '-o', 'IdentityAgent=none', '-o', 'BatchMode=yes',
                              '-o', 'ConnectTimeout=10', '-W', '%h:%p',
                              'copy-cluster-entry-1e56'])
            command += ['-o', 'ProxyCommand=' + jump, 'copy-' + node]
        return command + [remote_command]

    def command(self, node, action, identity=None):
        result = super().command(node, action, identity)
        if action == 'replace':
            bundle = Path(self.remote_script).parent
            # The remote command is always the final argv item, quoted once.
            result[-1] += ' ' + shlex.join([
                '--wrapper', str(bundle / 'migration_cached_worker.py'),
                '--engine-manifest', str(bundle / 'engine-transition.json')])
        return result


def rollout(control, bundle, nodes, lanes, *, ssh_aliases=False):
    if type(ssh_aliases) is not bool:
        raise ValueError('ssh_aliases must be an explicit boolean')
    control, bundle = Path(control).resolve(), Path(bundle).resolve()
    cfg, manifest = verify_bundle(control, bundle)
    state = read_json(control / 'state.json')
    if (control / 'STOP').exists() or state.get('status') in protocol.TERMINAL:
        raise RuntimeError('STOP or terminal state')
    operation = uuid.uuid4().hex
    output = bundle / ('rollout-' + operation + '.json')
    record = {'control': str(control), 'bundle': str(bundle), 'operation': operation,
              'transport': 'ssh-aliases' if ssh_aliases else 'legacy-bridge',
              'started': time.time(), 'status': 'running', 'entries': []}

    def save():
        record['updated'] = time.time()
        write_json(output, record)

    save()
    try:
        for node in nodes:
            for lane in lanes:
                verify_bundle(control, bundle)
                rpc = DirectoryRPC(control=str(control), job=cfg['job'], node=node, lane=lane,
                                   remote_script=str(bundle / 'rollout_cache_age.py'),
                                   ssh_prefix='copy-', output=str(output),
                                   operation=uuid.uuid4().hex, gate_timeout=30,
                                   max_age=30, control_timeout=30,
                                   ssh_aliases=ssh_aliases)
                entry = {'node': node, 'lane': lane, 'operation': rpc.operation,
                         'started': time.time(), 'status': 'inspecting'}
                record['entries'].append(entry)
                save()
                print(json.dumps(entry), flush=True)
                with rpc.hold():
                    observed = rpc(node, 'inspect')
                    wrapper = str(bundle / 'migration_cached_worker.py')
                    if wrapper in observed['identity']['argv']:
                        health = rpc.health(observed['identity'])
                        if not health.get('healthy'):
                            raise RuntimeError('already upgraded lane is unhealthy')
                        entry.update(status='already_upgraded', identity=observed['identity'])
                    else:
                        if 'worker-directory-policy.json' in manifest:
                            transition = read_json(bundle / 'engine-transition.json')
                            previous_wrapper = str(Path(transition['previous_bundle']) /
                                                   'migration_cached_worker.py')
                            observed_wrappers = [value for value in observed['identity']['argv']
                                                 if Path(value).name == 'migration_cached_worker.py']
                            if observed_wrappers != [previous_wrapper]:
                                raise RuntimeError('native rollout live wrapper does not match previous bundle')
                        outcome = protocol.roll_one(
                            rpc, node, lane, gate_timeout=30, max_age=30,
                            visibility_timeout=30)
                        entry.update(outcome)
                        if outcome.get('status') == 'complete':
                            expected = outcome['replacement'].get('replacement')
                            if expected is None:
                                expected = rpc(node, 'inspect')['identity']
                            if wrapper not in expected['argv'] or not rpc.health(expected).get('healthy'):
                                raise RuntimeError('upgraded lane failed wrapper/health postflight')
                entry['finished'] = time.time()
                save()
                print(json.dumps(entry), flush=True)
                if entry['status'] not in ('complete', 'already_upgraded', 'waiting_payload'):
                    raise RuntimeError('unexpected rollout outcome; inspect before continuing')
        record['status'] = ('complete' if all(e['status'] in ('complete', 'already_upgraded')
                                             for e in record['entries']) else 'partial_busy')
        save()
    except BaseException as exc:
        record.update(status='inspect_before_retry', error=type(exc).__name__ + ': ' + str(exc))
        save()
        raise
    return {'output': str(output), 'status': record['status'],
            'lanes_complete': sum(e['status'] in ('complete', 'already_upgraded')
                                  for e in record['entries'])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'rollout'))
    parser.add_argument('--control', required=True)
    parser.add_argument('--bundle')
    parser.add_argument('--previous-bundle',
                        help='prepare a native-safe worker successor to this exact immutable bundle')
    parser.add_argument('--destination-opath', action='store_true',
                        help='prepare only a guarded destination O_PATH launcher; retain all six engine files')
    parser.add_argument('--ssh-aliases', action='store_true',
                        help='roll out through existing master-0 / worker-N SSH aliases, retaining agent configuration')
    parser.add_argument('--nodes', nargs='+', choices=NODES, default=NODES)
    parser.add_argument('--lanes', nargs='+', choices=LANES, default=LANES)
    args = parser.parse_args()
    if args.action == 'prepare':
        if args.ssh_aliases:
            parser.error('--ssh-aliases is only valid for rollout')
        result = prepare(args.control, previous_bundle=args.previous_bundle,
                         destination_opath=args.destination_opath)
    else:
        if args.previous_bundle or args.destination_opath:
            parser.error('--previous-bundle is only valid for prepare')
        if not args.bundle:
            parser.error('rollout requires --bundle')
        if len(set(args.nodes)) != len(args.nodes) or len(set(args.lanes)) != len(args.lanes):
            parser.error('nodes and lanes must be distinct')
        result = rollout(args.control, args.bundle, args.nodes, args.lanes,
                         ssh_aliases=args.ssh_aliases)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
