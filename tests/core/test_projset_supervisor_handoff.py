import concurrent.futures
import ctypes
import errno
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'ops/copy/projset_recovery_supervisor.py'
spec = importlib.util.spec_from_file_location('projset_supervisor_handoff', SCRIPT)
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        time.sleep(.05)
    raise AssertionError('condition not reached before timeout')


def set_subreaper(enabled):
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, int(enabled), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'PR_SET_CHILD_SUBREAPER failed')


def reap_adopted(pid):
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            got, _ = os.waitpid(pid, os.WNOHANG)
            if got:
                return
        except ChildProcessError:
            return
        time.sleep(.05)
    try:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
    except (ProcessLookupError, ChildProcessError):
        pass


def test_detach_reuses_lock_and_starts_only_one_role(tmp_path):
    run = tmp_path / ('role-' + str(os.getpid()))
    run.mkdir()
    marker, stop = run/'started', run/'stop'
    role_script = run/'role.py'
    role_script.write_text('''import argparse, fcntl, os, time\np=argparse.ArgumentParser();p.add_argument("mode");p.add_argument("--run");p.add_argument("--lock-fd",type=int,required=True);a=p.parse_args()\nfcntl.flock(a.lock_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)\nwith open(os.path.join(a.run,"started"),"a") as f:f.write(str(os.getpid())+"\\n");f.flush()\nwhile not os.path.exists(os.path.join(a.run,"stop")):time.sleep(.05)\n''')
    lock_path = supervisor.lock_path(run, 'standby')
    launched = []
    set_subreaper(True)
    try:
        command = [sys.executable, '-S', '-c', supervisor.detach(role_script, run, 'standby')]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(lambda _: subprocess.run(command, capture_output=True, timeout=5), range(8)))
        assert all(result.returncode == 0 for result in results)
        wait_for(marker.exists)
        time.sleep(.25)
        launched = [int(x) for x in marker.read_text().splitlines()]
        assert len(launched) == 1
        # A later health check while the role lives must not start a child.
        assert subprocess.run(command, timeout=5).returncode == 0
        assert marker.read_text().splitlines() == [str(launched[0])]
        stop.touch()
        wait_for(lambda: os.waitpid(launched[0], os.WNOHANG)[0] == launched[0])
        launched.clear()
    finally:
        for pid in launched:
            reap_adopted(pid)
        set_subreaper(False)
        lock_path.unlink(missing_ok=True)


def test_inherited_lock_must_match_role_lock(tmp_path):
    run = tmp_path/('invalid-inherited-lock-' + str(os.getpid()))
    other = tmp_path/'other-lock'
    supervisor.lock_path(run, 'standby').touch()
    fd = os.open(other, os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        with pytest.raises(RuntimeError, match='does not match'):
            supervisor.acquire_lock(run, 'standby', fd)
    finally:
        supervisor.lock_path(run, 'standby').unlink(missing_ok=True)


def test_real_standby_starts_with_inherited_lock_without_duplicate(tmp_path):
    run = tmp_path/('standby-' + str(os.getpid()))
    code = run/'recovery/code'
    code.mkdir(parents=True)
    script = code/'projset_recovery_supervisor.py'
    script.write_text(SCRIPT.read_text())
    (run/'status.json').write_text(json.dumps({'state': 'copying', 'updated': time.time()}))
    cfg = {'hosts': {'cpu-master-0': 'fake-master'},
           'known_hosts': str(tmp_path/'known_hosts'), 'key': str(tmp_path/'key')}
    (run/'recovery/ssh.json').write_text(json.dumps(cfg))
    bindir = tmp_path/'bin'
    bindir.mkdir()
    (bindir/'ssh').write_text('#!/bin/sh\nexit 0\n')
    (bindir/'ssh').chmod(0o755)
    env = dict(os.environ, PATH=str(bindir)+os.pathsep+os.environ.get('PATH', ''))
    command = [sys.executable, '-S', '-c', supervisor.detach(script, run, 'standby')]
    pid = None
    set_subreaper(True)
    try:
        assert subprocess.run(command, env=env, timeout=5).returncode == 0
        state = wait_for(lambda: json.loads((run/'recovery/standby.json').read_text()))
        pid = state['pid']
        assert Path('/proc/%d' % pid).exists()
        for _ in range(3):
            assert subprocess.run(command, env=env, timeout=5).returncode == 0
        with pytest.raises(BlockingIOError):
            supervisor.acquire_lock(run, 'standby')
        assert json.loads((run/'recovery/standby.json').read_text())['pid'] == pid
    finally:
        if pid is not None:
            reap_adopted(pid)
        set_subreaper(False)
        supervisor.lock_path(run, 'standby').unlink(missing_ok=True)


def test_unknown_controller_lock_is_detected(tmp_path):
    run = tmp_path/('controller-lock-' + str(os.getpid()))
    run.mkdir()
    (run/'config.json').write_text(json.dumps({'run_id': run.name}))
    lock_path = Path('/tmp/'+run.name+'-controller.lock')
    try:
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert supervisor.controller_lock_held(run)
        assert not supervisor.controller_lock_held(run)
    finally:
        lock_path.unlink(missing_ok=True)


def test_unknown_process_identity_never_triggers_recovery(monkeypatch, tmp_path):
    previous = {'pid': os.getpid()}
    monkeypatch.setattr(supervisor, 'identity_matches', lambda *args: None)
    child = supervisor.adopt(previous, tmp_path/'controller.py', 'controller', tmp_path/'status.json')
    assert child is not None
    assert child.poll() is None
    monkeypatch.setattr(supervisor.os, 'kill', lambda *args: pytest.fail('unknown PID must not be signalled'))
    child.terminate()


def test_terminal_state_is_only_accepted_for_matching_controller(monkeypatch, tmp_path):
    status = tmp_path/'status.json'
    status.write_text(json.dumps({'controller_pid': 15, 'state': 'copy_pass_finished', 'updated': time.time()}))
    monkeypatch.setattr(supervisor, 'identity_matches', lambda *args: False)
    dead = supervisor.AdoptedProcess({'pid': 14}, tmp_path/'controller.py', 'controller', status)
    assert dead.poll() == 1
    terminal = supervisor.AdoptedProcess({'pid': 15}, tmp_path/'controller.py', 'controller', status)
    assert terminal.poll() == 0


def test_unavailable_or_invalid_status_does_not_stop_controller(tmp_path):
    status = tmp_path/'status.json'

    class Controller:
        terminated = 0

        def terminate(self):
            self.terminated += 1

    controller = Controller()
    now = 10000
    assert supervisor.check_controller_status(controller, status, now) == 'FileNotFoundError'
    status.write_text('{')
    assert supervisor.check_controller_status(controller, status, now) == 'JSONDecodeError'
    status.write_text(json.dumps({'state': 'copying', 'updated': float('nan')}))
    assert supervisor.check_controller_status(controller, status, now) == 'ValueError'
    assert controller.terminated == 0
    status.write_text(json.dumps({'state': 'copying', 'updated': now - 601}))
    assert supervisor.check_controller_status(controller, status, now) is None
    assert controller.terminated == 1
    status.write_text(json.dumps({'state': 'recovering', 'updated': now - 601}))
    assert supervisor.check_controller_status(controller, status, now) is None
    assert controller.terminated == 1
    status.write_text(json.dumps({'state': 'recovering', 'updated': now - 1801}))
    assert supervisor.check_controller_status(controller, status, now) == 'RecoveringStatusStale'
    assert controller.terminated == 1


def test_stale_nfs_status_is_treated_as_unknown(monkeypatch, tmp_path):
    status = tmp_path/'status.json'

    def stale(_path):
        raise OSError(errno.ESTALE, 'Stale file handle')

    monkeypatch.setattr(Path, 'read_text', stale)
    assert supervisor.read_status(status) == (None, 'OSError')


def test_dead_adopted_controller_waits_for_readable_status(monkeypatch, tmp_path):
    status = tmp_path/'status.json'
    monkeypatch.setattr(supervisor, 'identity_matches', lambda *args: False)
    previous = {'pid': 15}
    child = supervisor.adopt(previous, tmp_path/'controller.py', 'controller', status)
    assert child is not None
    assert child.poll() is None
    assert child.returncode is None
    status.write_text('{')
    assert child.poll() is None
    status.write_text(json.dumps({'controller_pid': 15, 'state': 'copy_pass_finished', 'updated': time.time()}))
    assert child.poll() == 0
    status.write_text(json.dumps({'controller_pid': 15, 'state': 'copying', 'updated': time.time()}))
    failed = supervisor.adopt(previous, tmp_path/'controller.py', 'controller', status)
    assert failed.poll() == 1


def test_standby_checks_anchor_while_status_is_temporarily_missing(monkeypatch, tmp_path):
    run = tmp_path/('unavailable-status-' + str(os.getpid()))
    root = run/'recovery'
    root.mkdir(parents=True)
    cfg = {'hosts': {'cpu-master-0': 'fake-master'},
           'known_hosts': str(tmp_path/'known_hosts'), 'key': str(tmp_path/'key')}
    (root/'ssh.json').write_text(json.dumps(cfg))
    calls = []

    def fake_ssh(*args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0)

    def complete_on_next_iteration(_seconds):
        (run/'status.json').write_text(json.dumps({'state': 'copy_pass_finished', 'updated': time.time()}))

    monkeypatch.setattr(supervisor.subprocess, 'run', fake_ssh)
    monkeypatch.setattr(supervisor.time, 'sleep', complete_on_next_iteration)
    try:
        supervisor.standby(run)
        heartbeat = json.loads((root/'standby.json').read_text())
        assert heartbeat['status_error'] == 'FileNotFoundError'
        assert heartbeat['anchor_reachable'] is True
        assert len(calls) == 1
    finally:
        supervisor.lock_path(run, 'standby').unlink(missing_ok=True)


def test_supervisor_keeps_children_running_while_status_is_missing(monkeypatch, tmp_path):
    run = tmp_path/('supervisor-status-' + str(os.getpid()))
    root = run/'recovery'
    (root/'code').mkdir(parents=True)
    cfg = {'hosts': {'cpu-master-0': socket.gethostname(), 'cpu-worker-0': 'fake-worker'},
           'known_hosts': str(tmp_path/'known_hosts'), 'key': str(tmp_path/'key'),
           'reporter_python': sys.executable, 'outcomes': str(tmp_path/'outcomes')}
    (root/'ssh.json').write_text(json.dumps(cfg))
    for name in ('controller', 'swanlab', 'outcomes'):
        (root/(name+'-child.json')).write_text(json.dumps({'pid': 123}))

    class Child:
        def __init__(self, pid):
            self.pid, self.returncode, self.terminated = pid, None, False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

    children = {name: Child(pid) for name, pid in [('controller', 123), ('swanlab', 124), ('outcomes', 125)]}

    def fake_adopt(previous, script, role=None, status_path=None):
        name = 'controller' if role == 'controller' else script.stem.removeprefix('projset_')
        return children[name]

    class EndIteration(Exception):
        pass

    monkeypatch.setattr(supervisor, 'adopt', fake_adopt)
    monkeypatch.setattr(supervisor.subprocess, 'run', lambda *a, **kw: subprocess.CompletedProcess(a, 0))
    monkeypatch.setattr(supervisor.time, 'sleep', lambda seconds: (_ for _ in ()).throw(EndIteration()))
    try:
        with pytest.raises(EndIteration):
            supervisor.supervise(run)
        heartbeat = json.loads((root/'supervisor.json').read_text())
        assert heartbeat['state'] == 'running'
        assert heartbeat['status_error'] == 'FileNotFoundError'
        assert heartbeat['controller_pid'] == 123
        assert heartbeat['sidecars'] == {'swanlab': 124, 'outcomes': 125}
        assert not any(child.terminated for child in children.values())
    finally:
        supervisor.lock_path(run, 'supervise').unlink(missing_ok=True)


CONTROLLER = '''import fcntl,json,os,sys,time\nfrom pathlib import Path\nrun=Path(sys.argv[sys.argv.index("--run")+1]); lock=open("/tmp/"+run.name+"-controller.lock","a");fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)\nwhile True:\n state="stopped" if (run/"STOP").exists() else "copying"\n data={"state":state,"updated":time.time(),"controller_pid":os.getpid()}\n tmp=run/("status-"+str(os.getpid())+".tmp");tmp.write_text(json.dumps(data));os.replace(tmp,run/"status.json")\n if state=="stopped":break\n time.sleep(.1)\n'''
SIDECAR = '''import time\nwhile True:time.sleep(.1)\n'''
OLD_SUPERVISOR = '''import fcntl,json,os,socket,subprocess,sys,time\nfrom pathlib import Path\nrun=Path(sys.argv[1]);root=run/"recovery";lock=open("/var/tmp/"+run.name+"-supervisor.lock","a");fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)\ndef identity(pid):\n return {"pid":pid,"start":Path("/proc/%d/stat"%pid).read_text().rsplit(") ",1)[1].split()[19],"host":socket.gethostname(),"boot":Path("/proc/sys/kernel/random/boot_id").read_text().strip()}\ncmds={"controller":[sys.executable,str(root/"code/projset_recovery_copy.py"),"controller","--run",str(run)],"swanlab":[sys.executable,str(root/"code/projset_swanlab.py"),"--run",str(run)],"outcomes":[sys.executable,str(root/"code/projset_outcomes.py"),"--run",str(run)]}\nfor name,cmd in cmds.items():\n p=subprocess.Popen(cmd,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)\n data=identity(p.pid)\n if name=="controller":data["attempt"]="fixture-attempt"\n (root/(name+"-child.json")).write_text(json.dumps(data))\n(root/"old-ready").touch()\nwhile True:time.sleep(.1)\n'''


def test_supervisor_handoff_preserves_children_and_recovers_failed_controller(tmp_path):
    run = tmp_path/('handoff-' + str(os.getpid()))
    code = run/'recovery/code'
    code.mkdir(parents=True)
    (code/'projset_recovery_supervisor.py').write_text(SCRIPT.read_text())
    (code/'projset_recovery_copy.py').write_text(CONTROLLER)
    (code/'projset_swanlab.py').write_text(SIDECAR)
    (code/'projset_outcomes.py').write_text(SIDECAR)
    old_script = tmp_path/'old_supervisor.py'
    old_script.write_text(OLD_SUPERVISOR)
    ssh_dir = tmp_path/'bin'
    ssh_dir.mkdir()
    fake_ssh = ssh_dir/'ssh'
    fake_ssh.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$FAKE_SSH_LOG"\nexit 0\n')
    fake_ssh.chmod(0o755)
    cfg = {'hosts': {'cpu-master-0': socket.gethostname(), 'cpu-worker-0': 'fake-worker'},
           'known_hosts': str(tmp_path/'known_hosts'), 'key': str(tmp_path/'key'),
           'reporter_python': sys.executable, 'outcomes': str(tmp_path/'outcomes')}
    (run/'recovery/ssh.json').write_text(json.dumps(cfg))
    (run/'config.json').write_text(json.dumps({'run_id': run.name}))
    env = dict(os.environ, PATH=str(ssh_dir)+os.pathsep+os.environ.get('PATH', ''),
               FAKE_SSH_LOG=str(tmp_path/'ssh.log'))
    old = first = second = rejected = None
    child_pids = []
    set_subreaper(True)
    try:
        old = subprocess.Popen([sys.executable, str(old_script), str(run)], env=env)
        wait_for((run/'recovery/old-ready').exists)
        wait_for((run/'status.json').exists)
        identities = {name: json.loads((run/'recovery'/(name+'-child.json')).read_text())
                      for name in ('controller','swanlab','outcomes')}
        child_pids = [entry['pid'] for entry in identities.values()]
        command = [sys.executable, '-S', '-B', str(code/'projset_recovery_supervisor.py'),
                   'supervise', '--run', str(run)]
        # The exclusive role lock prevents an extra supervisor from touching children.
        rejected = subprocess.Popen(command, env=env)
        assert rejected.wait(timeout=5) == 0
        assert all(Path('/proc/%d' % pid).exists() for pid in child_pids)

        old.terminate()
        old.wait(timeout=5)
        first = subprocess.Popen(command, env=env)
        def matching(proc):
            p = run/'recovery/supervisor.json'
            data = json.loads(p.read_text())
            return data if data.get('pid') == proc.pid and data.get('state') == 'running' else None
        first_state = wait_for(lambda: matching(first))
        assert first_state['controller_pid'] == identities['controller']['pid']
        assert first_state['sidecars'] == {n: identities[n]['pid'] for n in ('swanlab','outcomes')}
        first.terminate()
        first.wait(timeout=5)
        assert all(Path('/proc/%d' % pid).exists() for pid in child_pids)

        second = subprocess.Popen(command, env=env)
        second_state = wait_for(lambda: matching(second))
        assert second_state['controller_pid'] == identities['controller']['pid']
        assert second_state['sidecars'] == {n: identities[n]['pid'] for n in ('swanlab','outcomes')}
        # A genuinely failed adopted controller still goes through fenced recovery.
        dead_pid = identities['controller']['pid']
        os.kill(dead_pid, signal.SIGTERM)
        os.waitpid(dead_pid, 0)
        child_pids.remove(dead_pid)
        def new_controller():
            data = json.loads((run/'recovery/supervisor.json').read_text())
            pid = data.get('controller_pid')
            return pid if data.get('state') == 'running' and pid and pid != dead_pid else None
        replacement = wait_for(new_controller, timeout=30)
        assert Path('/proc/%d' % replacement).exists()
        assert 'fence' in (tmp_path/'ssh.log').read_text()
        (run/'STOP').touch()
        assert second.wait(timeout=25) == 0
    finally:
        (run/'STOP').touch()
        for process in (old, first, second, rejected):
            if process is not None and process.poll() is None:
                process.terminate()
                try:process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait(timeout=3)
        # All deliberately orphaned fixture children belong to this test's
        # subreaper, so the test itself does not leave zombies behind.
        for pid in child_pids:
            reap_adopted(pid)
        set_subreaper(False)
        supervisor.lock_path(run, 'supervise').unlink(missing_ok=True)
        Path('/tmp/'+run.name+'-controller.lock').unlink(missing_ok=True)
