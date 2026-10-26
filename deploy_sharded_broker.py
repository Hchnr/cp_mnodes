#!/usr/bin/env python3
"""Auditable, one-shot handover of the existing durable local broker.

Preflight is read-only. Handover signals only the bound old broker, waits for
its local flock, and starts exactly one staged broker on the existing SQLite
database and port. Copy workers and producer are never signalled.
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
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request

from migration_io import read_json, write_json

OLD_PID = 166974
OLD_START = '500376938'
PORT = 37651


def identity(pid):
    proc = Path('/proc') / str(pid)
    try:
        fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        argv = (proc / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
        return {'pid': pid, 'starttime_ticks': fields[19], 'state': fields[0], 'argv': argv}
    except OSError:
        return None


def same_process(actual, expected):
    return (actual is not None and actual['state'] != 'Z'
            and actual['pid'] == expected['pid']
            and actual['starttime_ticks'] == expected['starttime_ticks']
            and actual['argv'] == expected['argv'])


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def workers(control):
    result = {}
    for lane in ('nodes', 'nodes-extra'):
        records = list((control / lane).glob('*.json'))
        if len(records) != 12:
            raise RuntimeError('expected twelve worker records in each lane')
        for path in records:
            node = read_json(path)
            if node.get('ended') or node.get('threads') != 64:
                raise RuntimeError('copy worker is ended or has an unexpected thread count')
            result[lane + '/' + node['host']] = {
                'pid': node['pid'], 'started': node['started'],
                'run_started': node.get('run_started'), 'threads': node['threads']}
    return result


def database_snapshot(path):
    info = os.stat(path)
    uri = 'file:' + urllib.parse.quote(str(path), safe='/') + '?mode=ro'
    reader = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        reader.execute('BEGIN')
        counts = dict(reader.execute('SELECT status,COUNT(*) FROM claims GROUP BY status'))
        leases = list(reader.execute(
            "SELECT stage,id,owner,lease,running FROM claims WHERE status='leased' ORDER BY stage,id"))
        payload_digests = {stage + '/' + str(task_id): hashlib.sha256(payload.encode()).hexdigest()
                           for stage, task_id, payload in reader.execute(
                               "SELECT stage,id,payload FROM claims WHERE status='leased'")}
        schema = list(reader.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE tbl_name='claims' ORDER BY type,name"))
        max_id = reader.execute('SELECT MAX(id) FROM claims').fetchone()[0]
        journal = reader.execute('PRAGMA journal_mode').fetchone()[0]
    finally:
        reader.close()
    if journal != 'wal':
        raise RuntimeError('existing broker database is not in WAL mode')
    return {'path': str(path), 'dev': info.st_dev, 'inode': info.st_ino,
            'size': info.st_size, 'counts': counts, 'max_id': max_id,
            'leased': leases, 'leased_sha256': hashlib.sha256(json.dumps(leases).encode()).hexdigest(),
            'lease_payload_sha256': payload_digests,
            'schema': schema, 'journal_mode': journal}


def expected_broker(control, expected_identity=None, *, priority4=False):
    """Bind a complete broker identity from a frozen file in a run bundle."""
    control = Path(control).resolve()
    if expected_identity is None:
        if priority4:
            raise ValueError('P4 broker handover requires a frozen explicit identity')
        return {'control': str(control), 'pid': OLD_PID, 'starttime_ticks': OLD_START,
                'argv': ['/usr/bin/python3', '-u', str(control / 'migration_broker_parallel.py'),
                         '--control', str(control), '--port', str(PORT)]}
    path = Path(expected_identity).resolve()
    if (path.parent.parent != control or not path.is_file()
            or (path.parent / '.snapshots' / path.name).exists()):
        raise ValueError('expected identity must be a frozen JSON file inside a run bundle')
    expected = read_json(path)
    if (expected.get('control') != str(control) or type(expected.get('pid')) is not int
            or expected['pid'] <= 1 or not isinstance(expected.get('starttime_ticks'), str)
            or not expected['starttime_ticks'].isdigit()
            or not isinstance(expected.get('argv'), list) or len(expected['argv']) != 7
            or any(not isinstance(part, str) for part in expected['argv'])):
        raise ValueError('expected broker requires control, PID, starttime and full argv')
    argv = expected['argv']
    entry = Path(argv[2]).resolve()
    port = 37654 if priority4 else PORT
    entries = (('migration_priority4_broker.py', 'migration_priority4_broker_sharded.py')
               if priority4 else ('migration_broker_parallel.py', 'migration_broker_sharded.py'))
    if (not entry.is_relative_to(control) or not entry.is_file()
            or entry.name not in entries
            or argv != ['/usr/bin/python3', '-u', str(entry), '--control', str(control),
                        '--port', str(port)]):
        raise ValueError('expected argv must name this run broker entry, control and port')
    return {**expected, 'identity_path': str(path),
            'identity_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def protected_identities(expected):
    values = expected.get('protected_processes')
    if (not isinstance(values, list) or len(values) < 6
            or len({v.get('pid') for v in values if isinstance(v, dict)}) != len(values)):
        raise ValueError('P4 handover requires distinct pinned producer/copy/helper identities')
    for value in values:
        if (not isinstance(value, dict) or type(value.get('pid')) is not int
                or value['pid'] <= 1 or value['pid'] == expected['pid']
                or not isinstance(value.get('starttime_ticks'), str)
                or not value['starttime_ticks'].isdigit()
                or not isinstance(value.get('argv'), list) or not value['argv']
                or not all(isinstance(part, str) for part in value['argv'])):
            raise ValueError('invalid protected process identity')
        if not same_process(identity(value['pid']), value):
            raise RuntimeError('a protected producer/copy/helper identity changed')
    return values


def protected_workers(control, cfg, priority4):
    result = {'current/' + key: value for key, value in workers(control).items()}
    if priority4:
        predecessor = Path(cfg['parallel_with_control'])
        if (predecessor.parent != control.parent or predecessor == control
                or predecessor.is_symlink() or (predecessor / 'STOP').exists()):
            raise ValueError('P4 predecessor identity or STOP is not safe')
        result.update({'predecessor/' + key: value
                       for key, value in workers(predecessor).items()})
    return result


def preflight(control, expected_identity=None, *, priority4=False):
    control = Path(control).resolve()
    cfg = read_json(control / 'config.json')
    expected = expected_broker(control, expected_identity, priority4=priority4)
    port = 37654 if priority4 else PORT
    if priority4:
        from migration_priority4_worker import ScopeGuard
        ScopeGuard(str(control), cfg)
        if urllib.parse.urlparse(cfg['broker_url']).port != port:
            raise ValueError('P4 configured broker port differs')
        protected_identities(expected)
    old_pid = expected['pid']
    actual = identity(old_pid)
    if not same_process(actual, expected):
        raise RuntimeError('old broker PID/starttime/full argv do not match')
    role = read_json(control / 'broker.json')
    if role.get('pid') != old_pid or role.get('host') != socket.gethostname() or role.get('port') != port:
        raise RuntimeError('broker role does not match this host and port')
    if (control / 'STOP').exists():
        raise RuntimeError('STOP is present')
    dbpath = Path('/tmp/' + cfg['run_id'] + '-broker.sqlite')
    lockpath = Path('/tmp/' + cfg['run_id'] + '-broker.lock')
    lock = os.stat(lockpath)
    held = []
    for line in Path('/proc/locks').read_text().splitlines():
        fields = line.split()
        if (len(fields) >= 6 and fields[1] == 'FLOCK'
                and fields[4] == str(old_pid) and fields[5].endswith(':' + str(lock.st_ino))):
            held.append(line)
    if not held:
        raise RuntimeError('bound old broker does not hold its expected flock')
    return cfg, {'control': str(control), 'host': socket.gethostname(), 'captured': now(),
                 'old_identity': actual, 'expected_identity': expected,
                 'old_nice': os.getpriority(os.PRIO_PROCESS, old_pid),
                 'workers': protected_workers(control, cfg, priority4),
                 'protected_processes': expected.get('protected_processes', []),
                 'database': database_snapshot(dbpath),
                 'lock_path': str(lockpath), 'lock_inode': lock.st_ino,
                 'drain': (read_json(control / 'drain-hosts.json')
                           if (control / 'drain-hosts.json').exists() else {'draining_hosts': []})}


def staged_source(bundle, expected_manifest, *, priority4=False):
    manifest_path = bundle / 'source-sha256.json'
    if hashlib.sha256(manifest_path.read_bytes()).hexdigest() != expected_manifest:
        raise RuntimeError('staged source manifest binding changed')
    manifest = read_json(manifest_path)
    for name, digest in manifest.items():
        path = (bundle / name).resolve()
        if path.parent != bundle or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise RuntimeError('staged source file binding changed')
    entry = bundle / ('migration_priority4_broker_sharded.py' if priority4 else 'migration_broker_sharded.py')
    required = ((entry.name, 'migration_priority4_broker.py', 'migration_broker.py') if priority4 else
                (entry.name, 'migration_broker_parallel.py', 'migration_broker.py'))
    if any(name not in manifest for name in required):
        raise RuntimeError('staged bundle lacks the sharded broker entry and inherited modules')
    return entry


def validate_preserved_database(before, after):
    if ((before['dev'], before['inode']) != (after['dev'], after['inode'])
            or [tuple(row) for row in before['schema']] != [tuple(row) for row in after['schema']]
            or sum(after['counts'].values()) < sum(before['counts'].values())
            or after['counts'].get('done', 0) < before['counts'].get('done', 0)):
        raise RuntimeError('broker database inode/schema/row-count was not preserved')
    reader = sqlite3.connect('file:' + urllib.parse.quote(before['path'], safe='/') + '?mode=ro',
                             uri=True, timeout=5)
    try:
        for stage, task_id, owner, lease, running in before['leased']:
            row = reader.execute(
                'SELECT owner,lease,running,status,payload FROM claims WHERE stage=? AND id=?',
                (stage, task_id)).fetchone()
            if row is None or row[:3] != (owner, lease, running) or row[3] not in ('leased', 'done'):
                raise RuntimeError('an inherited lease/owner/running path was lost or changed')
            payload_digest = before.get('lease_payload_sha256', {}).get(stage + '/' + str(task_id))
            if payload_digest is not None and hashlib.sha256(row[4].encode()).hexdigest() != payload_digest:
                raise RuntimeError('an inherited lease payload was changed')
    finally:
        reader.close()


def handover(control, bundle, manifest_sha, exit_timeout=60, expected_identity=None, *, priority4=False):
    control, bundle = Path(control).resolve(), Path(bundle).resolve()
    if bundle.parent != control:
        raise ValueError('new broker bundle must be directly within the control directory')
    entry = staged_source(bundle, manifest_sha, priority4=priority4)
    port = 37654 if priority4 else PORT
    if expected_identity is not None and Path(expected_identity).resolve().parent != bundle:
        raise ValueError('expected identity must be frozen inside the new broker bundle')
    cfg, before = preflight(control, expected_identity, priority4=priority4)
    old_pid = before['old_identity']['pid']
    old_start = before['old_identity']['starttime_ticks']
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if hard != resource.RLIM_INFINITY and hard < 65536:
        raise RuntimeError('broker NOFILE hard limit is too low')
    if soft < 65536:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, hard))
    path = bundle / 'broker-handover-operation.json'
    operation = {'scope': ('priority4-sharded-broker-handover-v1' if priority4 else
                           'sharded-broker-handover-v1'), 'state': 'preflight_validated',
                 'bundle_manifest_sha256': manifest_sha, 'before': before}
    with open(path, 'x') as handle:
        json.dump(operation, handle)
        handle.flush()
        os.fsync(handle.fileno())

    def save(**updates):
        operation.update(updates, updated=now())
        write_json(path, operation)

    try:
        staged_source(bundle, manifest_sha, priority4=priority4)
        if expected_broker(control, expected_identity, priority4=priority4) != before['expected_identity']:
            raise RuntimeError('frozen expected broker identity changed before scoped signal')
        if priority4:
            protected_identities(before['expected_identity'])
        if (control / 'STOP').exists() or not same_process(identity(old_pid), before['old_identity']):
            raise RuntimeError('old broker identity or STOP changed before scoped signal')
        os.kill(old_pid, signal.SIGTERM)
        save(state='old_broker_signalled')
        deadline = time.monotonic() + exit_timeout
        with open(before['lock_path'], 'a') as lock:
            while True:
                actual = identity(old_pid)
                exited = (actual is None or actual['state'] == 'Z'
                          or actual['starttime_ticks'] != old_start)
                if exited:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        pass
                if time.monotonic() > deadline:
                    raise TimeoutError('old broker exit/flock release timed out; inspect before any retry')
                time.sleep(.1)
            frozen = database_snapshot(before['database']['path'])
            validate_preserved_database(before['database'], frozen)
            save(state='old_broker_exited_and_locked', frozen_database=frozen)
        if (control / 'STOP').exists():
            raise RuntimeError('STOP changed before new broker launch')
        staged_source(bundle, manifest_sha, priority4=priority4)
        argv = ['/usr/bin/python3', '-u', str(entry), '--control', str(control), '--port', str(port)]
        with open(bundle / 'broker.log', 'xb') as log:
            child = subprocess.Popen(['nice', '-n', str(before['old_nice']), 'ionice', '-c', '3', *argv],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True, close_fds=True)
        save(state='new_broker_launched', new_pid=child.pid, new_argv=argv)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def get(route):
            query = urllib.request.Request('http://127.0.0.1:' + str(port) + route,
                         headers={'Authorization': 'Bearer ' + cfg['broker_token']})
            with opener.open(query, timeout=10) as response:
                return json.load(response)

        deadline = time.monotonic() + 45
        while True:
            if child.poll() is not None:
                raise RuntimeError('new broker exited; inspect its one-shot log')
            try:
                health = get('/health')
                actual = identity(child.pid)
                if (health.get('ok') and health.get('pid') == child.pid and actual
                        and actual['argv'] == argv and actual['state'] != 'Z'):
                    break
            except (OSError, ValueError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError('new broker health did not validate within45 seconds')
            time.sleep(.2)
        observed = get('/control')
        if observed.get('draining_hosts') != sorted(before['drain']['draining_hosts']):
            raise RuntimeError('broker drain state was not preserved')
        after = database_snapshot(before['database']['path'])
        validate_preserved_database(frozen, after)
        after_workers = protected_workers(control, cfg, priority4)
        if after_workers != before['workers']:
            raise RuntimeError('copy worker identities changed during broker handover')
        role = read_json(control / 'broker.json')
        if priority4:
            protected_identities(before['expected_identity'])
        if role.get('pid') != child.pid or role.get('port') != port:
            raise RuntimeError('new broker role publication does not match its health')
        save(state='ready', new_identity=actual, after_database=after,
             worker_identities_unchanged=True, control_reply=observed)
        return {'state': 'ready', 'pid': child.pid, 'starttime_ticks': actual['starttime_ticks'],
                'inherited_leases': len(frozen['leased']), 'worker_identities_unchanged': True,
                'operation': str(path)}
    except BaseException as exc:
        save(state='failed_inspect_before_retry', error=type(exc).__name__ + ': ' + str(exc))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('preflight', 'handover'))
    parser.add_argument('--control', required=True)
    parser.add_argument('--bundle')
    parser.add_argument('--manifest-sha256')
    parser.add_argument('--expected-identity', help='frozen JSON with control, pid, starttime_ticks and full argv')
    parser.add_argument('--priority4', action='store_true', help='explicit P4 scope, port and protected roles')
    args = parser.parse_args()
    if args.mode == 'preflight':
        _, value = preflight(args.control, args.expected_identity, priority4=args.priority4)
        # Lease credentials are preserved in private handover evidence but
        # never printed by read-only operator preflight.
        value['database'].pop('leased')
        print(json.dumps(value), flush=True)
    else:
        if not args.bundle or not args.manifest_sha256:
            parser.error('handover requires --bundle and --manifest-sha256')
        print(json.dumps(handover(args.control, args.bundle, args.manifest_sha256,
                                 expected_identity=args.expected_identity, priority4=args.priority4)), flush=True)
