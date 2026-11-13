#!/usr/bin/env python3
"""Two-node, isolated NFS exercise of a resumable synthetic snapshot.

The paths, source dataset and node are fixed. No production queue is opened.
The separate parent stops test processes if the live task becomes unhealthy.
"""
import importlib.util
import json
import os
from pathlib import Path
import shlex
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import projset_paced_nfs_canary as safety
import projset_paced_read as pacing
import projset_resumable_snapshot_canary as snapshot
import projset_snapshot_capture_canary as capture


AUDIT = safety.AUDIT
PROTO = safety.PROTO
WORK = PROTO / 'resume-nfs-canary-20261113T0626Z'
SOURCE = WORK / capture.LEGACY
CODE = Path(__file__).resolve()
ROWS = 512

SCHEMA = '''
create table dirs(id integer primary key, rel text not null unique, state integer not null default 0,
                  node text, attempt integer not null default 0, next real not null default 0,
                  updated real not null default 0);
create index dir_queue on dirs(state,next,id);
create table issues(parent integer not null, rel text not null, issue text not null,
                    attempt integer not null, data text not null, primary key(parent,rel,issue));
create table failures(dir integer primary key, attempts integer not null, detail text not null,
                      last_time real not null, resolved integer not null default 0);
create table meta(key text primary key, value text not null);
create table repair_outbox(id integer primary key autoincrement, rel text not null, data text not null);
create trigger repair_missing_outbox after insert on issues when new.issue='missing_target'
begin insert into repair_outbox(rel,data) values(new.rel,new.data); end;
'''


def atomic(path, data):
    if path.parent != WORK or path.is_symlink():
        raise ValueError('not a synthetic canary output')
    tmp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    with tmp.open('x') as stream:
        json.dump(data, stream, ensure_ascii=True, separators=(',', ':'))
        stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)


def validate_work():
    if (WORK.name != 'resume-nfs-canary-20261113T0626Z' or WORK.is_symlink() or
            WORK.resolve().parent != PROTO.resolve() or not WORK.is_dir()):
        raise ValueError('not the fixed synthetic work directory')
    if SOURCE.is_symlink():
        raise ValueError('synthetic source is a symlink')


def prepare():
    validate_work()
    safety.live_states()
    if SOURCE.exists() or (WORK / 'prepared.json').exists():
        raise ValueError('synthetic source already prepared')
    with SOURCE.open('xb'):
        pass
    db = sqlite3.connect(str(SOURCE), timeout=3)
    try:
        db.execute('pragma journal_mode=delete')
        db.execute('pragma synchronous=full')
        db.executescript(SCHEMA)
        with db:
            db.execute('insert into meta values(?,?)', ('stats', '{"synthetic":true}'))
            db.executemany('insert into dirs(rel) values(?)',
                           [(json.dumps('synthetic/%06d/' % i + 'x' * 64),) for i in range(ROWS)])
    finally:
        db.close()
    identity = capture.install(SOURCE)
    result = {'capture_id': identity, 'initial_rows': ROWS, 'source_bytes': SOURCE.stat().st_size, 'at': time.time()}
    atomic(WORK / 'prepared.json', result)
    return result


def writer():
    validate_work()
    ready = json.loads((WORK / 'prepared.json').read_text())
    if (WORK / 'WRITER_STOP').exists():
        raise ValueError('synthetic writer was stopped before start')
    db = sqlite3.connect(str(SOURCE), timeout=3)
    count = 0
    started = time.monotonic()
    try:
        if capture.verify(db) != ready['capture_id']:
            raise ValueError('synthetic writer capture identity changed')
        while not (WORK / 'WRITER_STOP').exists() and time.monotonic() - started < 120:
            count += 1
            rel = 'during-snapshot-%05d' % count
            with db:
                db.execute('update dirs set updated=?,state=? where id=?',
                           (1794550000.125 + count, count % 3, 1 + count % ROWS))
                db.execute('insert into dirs(rel) values(?)', (json.dumps(rel),))
                db.execute('insert into issues values(?,?,?,?,?)',
                           (1, json.dumps(rel + '/missing'), 'missing_target', 1,
                            json.dumps({'rel': rel + '/missing', 'issue': 'missing_target'})))
                db.execute('update meta set value=? where key=?', (json.dumps({'transactions': count}), 'stats'))
            if count == 1:
                atomic(WORK / 'writer-ready.json', {'pid': os.getpid(), 'at': time.time()})
            time.sleep(.5)
        last = db.execute('select coalesce(max(seq),0) from handoff_changes').fetchone()[0]
    finally:
        db.close()
    result = {'transactions': count, 'last_seq': last, 'at': time.time()}
    atomic(WORK / 'writer-done.json', result)
    return result


def compare(source_path, target_path, pacer):
    left = sqlite3.connect('file:' + str(source_path) + '?mode=ro', uri=True, timeout=3)
    right = sqlite3.connect('file:' + str(target_path) + '?mode=ro', uri=True, timeout=3)
    try:
        pacing.disable_sqlite_readahead(source_path)
        left.execute('pragma mmap_size=0')
        left.execute('pragma cache_size=-256')
        order = {'dirs': 'id', 'issues': 'parent,rel,issue', 'failures': 'dir',
                 'meta': 'key', 'repair_outbox': 'id', 'handoff_changes': 'seq',
                 'handoff_meta': 'id', 'sqlite_sequence': 'name'}
        counts = {}
        for table, key in order.items():
            left_cursor = left.execute('select * from ' + table + ' order by ' + key)
            right_cursor = right.execute('select * from ' + table + ' order by ' + key)
            count = 0
            while True:
                a, b = left_cursor.fetchmany(32), right_cursor.fetchmany(32)
                pacer.throttle()
                if a != b:
                    raise ValueError('synthetic source and snapshot differ in ' + table)
                count += len(a)
                if not a:
                    break
            counts[table] = count
        return counts
    finally:
        right.close(); left.close()


class InjectedStop(Exception):
    pass


def scan():
    validate_work()
    prepared = json.loads((WORK / 'prepared.json').read_text())
    baseline = safety.live_states()[1]
    start = time.monotonic()
    def health():
        if time.monotonic() - start > 130:
            raise TimeoutError('synthetic snapshot deadline reached')
        safety.live_states(baseline)
    pacer = pacing.ReadPacer(512 * 1024, health=health, max_batch=512 * 1024)
    with tempfile.TemporaryDirectory(prefix=capture.MARKER + '-nfs-resume-', dir='/var/tmp') as directory:
        target = Path(directory) / capture.SNAPSHOT
        pages = [0]
        def interrupt(stage, detail):
            if stage == 'copy_committed':
                pages[0] += 1
                if pages[0] == 4:
                    raise InjectedStop('synthetic interruption')
        try:
            snapshot.snapshot(SOURCE, target, page_size=32, hook=interrupt, pacer=pacer,
                              advise=pacing.disable_sqlite_readahead)
        except InjectedStop:
            pass
        else:
            raise RuntimeError('synthetic interruption was not observed')
        observed = {'resumed': False, 'pages_while_writer_running': 0}
        def continue_hook(stage, detail):
            if stage == 'resumed':
                observed['resumed'] = True
            elif stage == 'copy_committed':
                if not (WORK / 'writer-done.json').exists():
                    observed['pages_while_writer_running'] += 1
                time.sleep(.04)
            elif stage == 'before_boundary':
                with (WORK / 'WRITER_STOP').open('x') as stream:
                    stream.write('stop isolated writer\n'); stream.flush(); os.fsync(stream.fileno())
                deadline = time.monotonic() + 25
                while time.monotonic() < deadline and not (WORK / 'writer-done.json').exists():
                    health(); time.sleep(.5)
                if not (WORK / 'writer-done.json').exists():
                    raise TimeoutError('synthetic writer did not stop')
        result = snapshot.snapshot(SOURCE, target, page_size=32, hook=continue_hook, pacer=pacer,
                                   advise=pacing.disable_sqlite_readahead)
        writer_result = json.loads((WORK / 'writer-done.json').read_text())
        if (not observed['resumed'] or not observed['pages_while_writer_running'] or
                writer_result['transactions'] < 2 or result['capture_id'] != prepared['capture_id'] or
                result['seq'] != writer_result['last_seq'] or result['seq'] <= result['base_seq']):
            raise ValueError('synthetic concurrent resume was not proven')
        counts = compare(SOURCE, target, pacer)
        return {'state': 'passed', 'snapshot': result, 'writer': writer_result,
                'overlap': observed, 'table_rows': counts,
                'elapsed_seconds': time.monotonic()-start,
                'note': 'isolated NFS source and local destination; no production queue changes'}


def remote_writer_processes(recovery):
    """Check for the exact synthetic writer after its stop request."""
    program = '''import json, os, sys
matches = []
for name in os.listdir('/proc'):
    if not name.isdigit():
        continue
    try:
        root = '/proc/' + name
        args = [os.fsdecode(x) for x in open(root + '/cmdline', 'rb').read().split(b'\\0') if x]
        if (args and os.path.basename(args[0]).startswith('python') and
                sys.argv[1] in args and '--writer' in args and '-c' not in args):
            fields = open(root + '/stat').read().rsplit(') ', 1)[1].split()
            matches.append({'pid': int(name), 'state': fields[0]})
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        continue
print(json.dumps(matches))
'''
    remote = 'timeout -s TERM -k 2s 8s python3 -S -B -c ' + shlex.quote(program) + ' ' + shlex.quote(str(CODE))
    result = subprocess.run(recovery.ssh('cpu-worker-13', remote), capture_output=True,
                            text=True, timeout=12, check=True)
    return json.loads(result.stdout.strip().splitlines()[-1])


def parent():
    states, identities = safety.live_states()
    monitor = json.loads((AUDIT / 'monitor20/status.json').read_text())
    if (monitor.get('state') != 'running' or monitor.get('audit_state') != 'running' or
            time.time() - monitor.get('updated', 0) > 1800):
        raise RuntimeError('fresh live throughput baseline unavailable')
    guard = safety.health_guard.Guard(monitor['rates'], window_seconds=60, ratio=.7)
    guard.observe(states[0])
    initial_memory = safety.memory_events()
    if WORK.exists() or WORK.is_symlink() or WORK.parent.resolve() != PROTO.resolve():
        raise ValueError('synthetic work directory already exists or escaped')
    WORK.mkdir(mode=0o700)
    start = time.monotonic()
    reason = None
    children = []
    writer_proc = None
    recovery = None

    def check():
        if time.monotonic() - start > 170:
            raise TimeoutError('synthetic integration deadline')
        current, _ = safety.live_states(identities)
        latest = guard.observe(current[0])
        if latest and latest['all_below_threshold']:
            raise RuntimeError('live throughput guard')
        if safety.memory_events() != initial_memory:
            raise RuntimeError('test node OOM counter changed')

    def launch(role):
        proc = subprocess.Popen([sys.executable, '-S', '-B', str(CODE), '--' + role],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)
        children.append(proc)
        return proc

    def wait(proc, timeout):
        deadline = time.monotonic() + timeout
        while proc.poll() is None and time.monotonic() < deadline:
            check(); time.sleep(.5)
        if proc.poll() is None:
            raise TimeoutError('synthetic participant timed out')
        out, err = proc.communicate(timeout=2)
        if proc.returncode:
            raise RuntimeError('synthetic participant failed: ' + (out + err)[-500:])
        return json.loads(out.strip().splitlines()[-1])

    report = {'at': time.time(), 'production_controllers': identities}
    try:
        report['prepare'] = wait(launch('prepare'), 35)
        recovery_path = AUDIT.parent.parent / 'recovery/code/projset_recovery_copy.py'
        spec = importlib.util.spec_from_file_location('resume_canary_recovery', recovery_path)
        recovery = importlib.util.module_from_spec(spec); spec.loader.exec_module(recovery)
        os.environ['PROJSET_SSH_TOPOLOGY'] = str(AUDIT.parent.parent / 'recovery/ssh.json')
        remote = ('nice -n 19 ionice -c 3 timeout -s TERM -k 3s 150s python3 -S -B ' +
                  shlex.quote(str(CODE)) + ' --writer')
        writer_proc = subprocess.Popen(recovery.ssh('cpu-worker-13', remote), stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        children.append(writer_proc)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not (WORK / 'writer-ready.json').exists():
            check()
            if writer_proc.poll() is not None:
                raise RuntimeError('synthetic writer stopped before becoming ready')
            time.sleep(.5)
        if not (WORK / 'writer-ready.json').exists():
            raise TimeoutError('synthetic writer was not ready')
        report['scan'] = wait(launch('scan'), 135)
        report['writer'] = wait(writer_proc, 20)
        report['passed'] = (report['scan'].get('state') == 'passed' and
                            report['writer']['transactions'] == report['scan']['writer']['transactions'])
    except Exception as error:
        reason = type(error).__name__ + ': ' + str(error)
        report.update(passed=False, error=reason)
    finally:
        if writer_proc is not None and writer_proc.poll() is None:
            try:
                (WORK / 'WRITER_STOP').write_text('stop isolated writer\n')
            except OSError:
                pass
        for proc in children:
            if proc is not writer_proc and proc.poll() is None:
                proc.terminate()
        if writer_proc is not None and writer_proc.poll() is None:
            try:
                writer_proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                writer_proc.terminate()
        for proc in children:
            try: proc.wait(timeout=3)
            except subprocess.TimeoutExpired: pass
        report['elapsed_seconds'] = time.monotonic()-start
        report['throughput_guard'] = {'baseline': guard.baseline, 'latest': guard.latest}
        report['remaining_children'] = [proc.pid for proc in children if proc.poll() is None]
        if report['remaining_children']:
            report['passed'] = False
        if writer_proc is not None:
            try:
                report['remote_writer_remaining'] = remote_writer_processes(recovery)
                if report['remote_writer_remaining']:
                    report['passed'] = False
            except (OSError, subprocess.SubprocessError, ValueError, IndexError) as error:
                report['passed'] = False
                report['remote_probe_error'] = type(error).__name__ + ': ' + str(error)
        atomic(WORK / 'result.json', report)
        print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--prepare']:
            print(json.dumps(prepare()), flush=True)
        elif sys.argv[1:] == ['--writer']:
            print(json.dumps(writer()), flush=True)
        elif sys.argv[1:] == ['--scan']:
            print(json.dumps(scan()), flush=True)
        elif not sys.argv[1:]:
            raise SystemExit(parent())
        else:
            raise ValueError('unexpected canary argument')
    except Exception as error:
        print(json.dumps({'state': 'stopped', 'type': type(error).__name__, 'detail': str(error)}), flush=True)
        raise SystemExit(1)
