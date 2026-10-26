"""UNDEPLOYED read-only process evidence for a future P4 dependency gate.

All reads are under a directory-FD-anchored Linux /proc. This never signals a
process, opens migration storage, adopts a new PID, or authorizes dispatch.
Two observations and a fresh terminal/scope/STOP/queue/lease/receipt check,
plus the actual local dispatch/coordinator locks, remain the caller's job.
"""
import errno
import os
from pathlib import PurePosixPath
import re
import tempfile
import time


DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _argv(value):
    if not isinstance(value, list) or not value or any(not isinstance(arg, str) or '\0' in arg for arg in value):
        raise ValueError('complete argv evidence is required')
    result = list(value)
    while result and result[-1] == '':
        result.pop()
    if not result or not result[0]:
        raise ValueError('saved executable is missing')
    return result


def _read(directory, name, limit):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    try:
        parts, size = [], 0
        while True:
            chunk = os.read(fd, min(65536, limit + 1 - size))
            if not chunk:
                return b''.join(parts)
            size += len(chunk)
            if size > limit:
                raise ValueError('proc record exceeds the complete-read limit')
            parts.append(chunk)
    finally:
        os.close(fd)


def _stat(directory, pid):
    value = _read(directory, 'stat', 65536).decode()
    prefix, fields = value.rsplit(') ', 1)
    fields = fields.split()
    if int(prefix.split(' ', 1)[0]) != pid or len(fields) < 20 or int(fields[19]) <= 0:
        raise ValueError('malformed process identity')
    return {'pid': pid, 'start_ticks': str(int(fields[19])), 'state': fields[0]}


def _process(root, pid, count_fds=False):
    directory = os.open(str(pid), DIRECTORY, dir_fd=root)
    try:
        before = _stat(directory, pid)
        command = _read(directory, 'cmdline', 2097152)
        argv = command.decode().rstrip('\0').split('\0') if command else []
        result = {**before, 'argv': argv}
        if count_fds:
            fds = os.open('fd', DIRECTORY, dir_fd=directory)
            try:
                result['open_fd_count'] = len(os.listdir(fds))
            finally:
                os.close(fds)
        after = _stat(directory, pid)
        command_after = _read(directory, 'cmdline', 2097152)
        if (before['start_ticks'] != after['start_ticks']
                or (before['state'] == 'Z') != (after['state'] == 'Z') or command_after != command):
            raise ValueError('PID/start/argv or exit state changed during observation')
        return result
    finally:
        os.close(directory)


def observe_node(control, identities, proc_root='/proc'):
    """Observe exact registered identities and all visible same-control processes.

    Missing, denied, malformed or changing census entries make completeness
    false. An absent registered PID is distinct from a reused PID. A same-start
    zombie is accepted as exited only with an empty cmdline and zero real FDs.
    Unknown live processes referring to the exact control still block the
    snapshot; a matching script under a sibling control does not.
    """
    if (not isinstance(control, str) or not control.startswith('/') or control.endswith('/')
            or '\0' in control or '..' in control.split('/') or str(PurePosixPath(control)) != control):
        raise ValueError('an exact absolute control is required')
    if not isinstance(identities, list) or not identities:
        raise ValueError('explicit registered identities are required')
    associated = re.compile(r'(^|[\s=;\'\"])' + re.escape(control) + r'($|[/\s;\'\"])')
    expected, pids, roles = [], set(), set()
    for value in identities:
        if not isinstance(value, dict):
            raise ValueError('registered identities must be explicit objects')
        pid, role = value.get('pid'), value.get('role')
        ticks, argv = str(value.get('start_ticks', '')), _argv(value.get('argv'))
        if (type(pid) is not int or pid <= 0 or not ticks.isdigit() or int(ticks) <= 0
                or not isinstance(role, str) or not role or pid in pids or role in roles
                or not any(associated.search(arg) for arg in argv)):
            raise ValueError('duplicate, incomplete or out-of-control registered identity')
        pids.add(pid); roles.add(role)
        expected.append({'pid': pid, 'role': role, 'start_ticks': str(int(ticks)), 'argv': argv})
    result = {'control': control, 'started_at': time.time(), 'registered': [],
              'live_associated': [], 'errors': [], 'census_processes': 0,
              'complete': False, 'quiescent_snapshot': False, 'launch_ready': False,
              'atomic_snapshot': False, 'migration_storage_accessed': False,
              'runtime_mutation_performed': False}

    def error(where, exc):
        result['errors'].append({'where': where, 'error': type(exc).__name__ + ': ' + str(exc)})

    root = None
    try:
        root_path = os.fspath(proc_root)
        if not isinstance(root_path, str):
            raise ValueError('proc root must be a text path')
        fixture_root = root_path != '/proc'
        if fixture_root:
            candidate = PurePosixPath(root_path)
            if (str(candidate) != root_path or candidate.parent != PurePosixPath(tempfile.gettempdir())
                    or not candidate.name.startswith('hcr-p4-process-fixture-')):
                raise ValueError('alternate proc root must be an owned, dedicated temporary fixture tree')
        root = os.open(proc_root, DIRECTORY)
        anchor = os.fstat(root)
        if fixture_root and anchor.st_uid != os.geteuid():
            raise ValueError('alternate proc fixture root is not owned by this user')
        for identity in expected:
            record = {**identity, 'observation': None, 'old_identity_exited': False}
            try:
                current = _process(root, identity['pid'], count_fds=True)
                record['observation'] = current
                if current['start_ticks'] != identity['start_ticks']:
                    record.update(status='pid_reused', old_identity_exited=True)
                elif current['state'] == 'Z':
                    if current['argv'] or current['open_fd_count'] != 0:
                        raise ValueError('zombie has unexpected argv or open file descriptors')
                    record.update(status='zombie_no_execution_or_fds', old_identity_exited=True)
                elif current['argv'] != identity['argv']:
                    record['status'] = 'same_pid_start_argv_changed'
                else:
                    record['status'] = 'running_exact'
            except OSError as exc:
                # Only absence of the PID directory proves absence. A missing
                # child file, or a process that disappeared mid-read, is unknown.
                if exc.errno == errno.ENOENT:
                    try:
                        os.stat(str(identity['pid']), dir_fd=root, follow_symlinks=False)
                    except FileNotFoundError:
                        record.update(status='pid_absent', old_identity_exited=True)
                    else:
                        record['status'] = 'unknown'
                        error(identity['role'], exc)
                else:
                    record['status'] = 'unknown'
                    error(identity['role'], exc)
            except (ValueError, UnicodeError) as exc:
                record['status'] = 'unknown'
                error(identity['role'], exc)
            result['registered'].append(record)
        for name in os.listdir(root):
            if not name.isdigit():
                continue
            pid = int(name)
            try:
                current = _process(root, pid)
                result['census_processes'] += 1
                if current['state'] != 'Z' and any(associated.search(arg) for arg in current['argv']):
                    result['live_associated'].append(current)
            except (OSError, ValueError, UnicodeError) as exc:
                error('census/' + name, exc)
        final = os.stat(proc_root, follow_symlinks=False)
        if (anchor.st_dev, anchor.st_ino) != (final.st_dev, final.st_ino):
            raise ValueError('proc root binding changed')
    except (OSError, ValueError, UnicodeError) as exc:
        error('proc_root', exc)
    finally:
        if root is not None:
            os.close(root)
    result['finished_at'] = time.time()
    result['complete'] = not result['errors'] and len(result['registered']) == len(expected)
    result['quiescent_snapshot'] = (result['complete'] and not result['live_associated']
                                    and all(item['old_identity_exited'] for item in result['registered']))
    return result
