"""Read-only observations for a controller that already stopped source readers.

This is NOT a signal/rollout tool and does not authorize pruning.  The caller
must also bind the exact cold-scanner implementation and phase, check accepted
target/publication work has drained, and subsequently verify a graceful exit
flushed the manifest. Queue metadata alone cannot prove a Python message loop
has finished processing a message already removed from the kernel queue.
"""
import os
from pathlib import Path
import socket
import stat

from migration_unix_queues import unix_queues


class NotQuiescent(RuntimeError):
    pass


def identity(pid):
    p = Path('/proc') / str(pid)
    fields = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
    return {'pid': pid, 'start_ticks': fields[19], 'state': fields[0],
            'ppid': int(fields[1]),
            'argv': (p / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')}


def require_identity(expected):
    value = identity(expected['pid'])
    if (value['state'] in ('Z', 'X') or any(value[k] != expected[k]
            for k in ('pid', 'start_ticks', 'argv'))):
        raise NotQuiescent('process identity changed')
    return value


def sockets(pid):
    result = {}
    for p in (Path('/proc') / str(pid) / 'fd').iterdir():
        try:
            link = os.readlink(p)
        except FileNotFoundError:
            continue
        if link.startswith('socket:[') and link.endswith(']'):
            inode = int(link[8:-1])
            if inode in result:
                raise NotQuiescent('socket FD alias requires explicit inspection')
            result[inode] = int(p.name)
    return result


def stopped_threads(pid):
    states = [(p / 'stat').read_text().rsplit(') ', 1)[1].split()[0]
              for p in (Path('/proc') / str(pid) / 'task').iterdir()]
    if not states or any(s not in ('T', 't') for s in states):
        raise NotQuiescent('every source-reader thread must already be stopped')
    return len(states)


def _polling_leader(pid):
    p = Path('/proc') / str(pid) / 'task' / str(pid)
    fields = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
    wchan = (p / 'wchan').read_text().strip()
    if fields[0] != 'S' or not wchan.startswith('do_poll'):
        raise NotQuiescent('source event-loop wait has not been observed')
    return {'state': fields[0], 'wchan': wchan}


def observe_boundary(parent, readers, source_root):
    """Observe frozen reader FDs + empty output queues + an idle main poll.

    Only valid for a separately SHA/phase-bound scanner whose main-thread poll
    is migration_process_scan.walk's source connection wait. No socket data is
    read and no process is signalled. Pending parent->child commands are allowed:
    their directories are unentered work, not completed inventory.
    """
    if not readers or len(readers) > 64 or len({r['pid'] for r in readers}) != len(readers):
        raise ValueError('require a bounded, unique set of explicit source-reader identities')
    require_identity(parent)
    source_root = os.path.abspath(os.fspath(source_root))
    root_stat = os.stat(source_root, follow_symlinks=False)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise NotQuiescent('source root is not a real directory')
    root_identity = root_stat.st_dev, root_stat.st_ino
    parent_sockets, child_sockets, frames = sockets(parent['pid']), {}, []
    for reader in readers:
        current = require_identity(reader)
        if current['ppid'] != parent['pid']:
            raise NotQuiescent('reader no longer belongs to the original producer')
        threads = stopped_threads(reader['pid'])
        descriptors = Path('/proc') / str(reader['pid']) / 'fd'
        found_anchor = False
        for p in descriptors.iterdir():
            link = os.readlink(p)
            if link != source_root and not link.startswith(source_root + '/'):
                continue
            if link.endswith(' (deleted)'):
                raise NotQuiescent('reader directory was deleted or renamed')
            value = p.stat()
            if not stat.S_ISDIR(value.st_mode):
                raise NotQuiescent('source reader holds an unexpected payload descriptor')
            rel = os.path.relpath(link, source_root)
            rel = '' if rel == '.' else rel
            if not rel and (value.st_dev, value.st_ino) == root_identity:
                found_anchor = True
            frames.append({'pid': reader['pid'], 'fd': int(p.name), 'rel': rel,
                           'dev': value.st_dev, 'ino': value.st_ino})
        if not found_anchor:
            raise NotQuiescent('stopped reader lacks its expected source anchor')
        child_sockets[reader['pid']] = sockets(reader['pid'])
    before = _polling_leader(parent['pid'])
    queues = unix_queues(set(parent_sockets).union(*(set(v) for v in child_sockets.values())))
    pairs = []
    for reader in readers:
        matches = []
        for inode, fd in child_sockets[reader['pid']].items():
            child = queues.get(inode)
            if not child or child.get('peer') not in parent_sockets:
                continue
            peer = queues.get(child['peer'])
            if (peer is None or peer.get('peer') != inode
                    or child['type'] != socket.SOCK_STREAM or peer['type'] != socket.SOCK_STREAM
                    or child['state'] != 1 or peer['state'] != 1):
                raise NotQuiescent('source socket pair is not an observed connected stream')
            if child['send_bytes'] or peer['receive_bytes']:
                raise NotQuiescent('source output IPC bytes have not drained')
            matches.append({'pid': reader['pid'], 'child_fd': fd, 'child_inode': inode,
                            'parent_fd': parent_sockets[child['peer']], 'parent_inode': child['peer'],
                            'child_send_bytes': 0, 'parent_receive_bytes': 0})
        if len(matches) != 1:
            raise NotQuiescent('expected exactly one source-parent IPC pair per reader')
        pairs.extend(matches)
    after = _polling_leader(parent['pid'])
    require_identity(parent)
    for reader in readers:
        require_identity(reader)
        stopped_threads(reader['pid'])
    return {'scope': 'stopped-source-readers-idle-poll-observation-v1',
            'parent': parent, 'readers': readers, 'root_identity': root_identity,
            'before': before, 'after': after, 'ipc': pairs, 'directory_fds': frames,
            'safe_to_prune': False, 'manifest_flush_still_required': True,
            'data_processes_signalled_by_observer': 0}
