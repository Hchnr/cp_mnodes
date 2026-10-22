"""Spawned, bounded source readers for ``migration_parallel_scan``.

Only the parent owns the resume path set and inventory/task side effects.  A
reader sends a page of names, waits for the parent's selection, and stats only
selected leaves.  This preserves the cheap readdir-only path for known files
without copying a multi-million-path set into each process.  Directory handles
remain in the reader until its selection has been consumed; symlinks are never
used for traversal.
"""
from concurrent.futures import ThreadPoolExecutor
import errno
import multiprocessing
from multiprocessing.connection import wait
import os
import stat
import time

from migration_scan import PRIORITY_SUBTREES, _open_directory, _stat_fields
from migration_worker import _open_root


def _error(rel, operation, exc):
    return {'rel': rel, 'kind': 'error', 'operation': operation,
            'error': str(exc), 'errno': getattr(exc, 'errno', None)}


def _open_task(anchor, rel, expected):
    fd = os.dup(anchor)
    try:
        for part in rel.split('/') if rel else ():
            child = _open_directory(part, dir_fd=fd)
            os.close(fd)
            fd = child
        current = os.fstat(fd)
        if (current.st_dev, current.st_ino) != expected:
            raise OSError(errno.ESTALE, 'directory changed before parallel enumeration')
        return fd
    except BaseException:
        os.close(fd)
        raise


def _leaf_record(parent_fd, name, rel):
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        return _error(rel, 'stat', exc)
    fields = _stat_fields(current)
    if stat.S_ISDIR(current.st_mode):
        # The name changed from a leaf after readdir.  Do not silently omit its
        # subtree; use the same identity-checked directory path as other dirs.
        return {'rel': rel, 'kind': '_late_directory', **fields}
    if stat.S_ISREG(current.st_mode):
        return {'rel': rel, 'kind': 'file', **fields}
    if stat.S_ISLNK(current.st_mode):
        try:
            target = os.readlink(name, dir_fd=parent_fd)
            after = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if (after.st_dev, after.st_ino, after.st_ctime_ns) != (
                    current.st_dev, current.st_ino, current.st_ctime_ns):
                raise OSError(errno.ESTALE, 'symlink changed during inventory')
        except OSError as exc:
            return _error(rel, 'readlink', exc)
        return {'rel': rel, 'kind': 'symlink', 'target': target,
                'link_target': target, **fields}
    kind = ('fifo' if stat.S_ISFIFO(current.st_mode) else
            'socket' if stat.S_ISSOCK(current.st_mode) else
            'char' if stat.S_ISCHR(current.st_mode) else
            'block' if stat.S_ISBLK(current.st_mode) else 'unknown')
    return {'rel': rel, 'kind': kind, 'unsupported': True, **fields}


def _reader(connection, source_root, source_identity, phase, batch_size, threads):
    """One spawned process; a page is its only in-flight IPC payload."""
    anchor = None
    frames = []
    try:
        anchor = _open_root(source_root)
        root_stat = os.fstat(anchor)
        if (root_stat.st_dev, root_stat.st_ino) != source_identity:
            raise OSError(errno.ESTALE, 'source root changed before reader startup')
        connection.send(('ready', os.getpid()))
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix='scan-stat') as pool:
            while True:
                command = connection.recv()
                if command[0] == 'stop':
                    return
                if command[0] != 'directory':
                    raise RuntimeError('unexpected source reader command')
                rel, expected = command[1:]
                started_cpu = time.process_time()

                def push(path, identity):
                    fd = None
                    try:
                        fd = _open_task(anchor, path, identity)
                        iterator = os.scandir(fd)
                    except OSError as exc:
                        if fd is not None:
                            os.close(fd)
                        return _error(path, 'open_directory', exc)
                    frames.append((fd, iterator, path))

                initial_error = push(rel, expected)
                if initial_error:
                    connection.send(('records', [initial_error]))
                while frames:
                    fd, iterator, parent = frames[-1]
                    if fd is None:
                        # Deferred local work stores only a relative path and
                        # identity. Opening it when reached keeps FDs bounded
                        # by traversal depth, even for very wide directories.
                        frames.pop()
                        error = push(parent, iterator)
                        if error:
                            connection.send(('records', [error]))
                        continue
                    leaves, directories, errors = [], [], []
                    exhausted = False
                    for _ in range(batch_size):
                        try:
                            child = next(iterator)
                        except StopIteration:
                            exhausted = True
                            break
                        except OSError as exc:
                            errors.append(_error(parent, 'scandir', exc))
                            exhausted = True
                            break
                        path = parent + '/' + child.name if parent else child.name
                        if phase == 3 and path in PRIORITY_SUBTREES:
                            continue
                        try:
                            is_directory = child.is_dir(follow_symlinks=False)
                        except OSError as exc:
                            errors.append(_error(path, 'stat', exc))
                            continue
                        if not is_directory:
                            leaves.append((child.name, path))
                            continue
                        child_fd = None
                        try:
                            current = child.stat(follow_symlinks=False)
                            child_fd = _open_directory(child.name, dir_fd=fd)
                            actual = os.fstat(child_fd)
                            if (actual.st_dev, actual.st_ino) != (current.st_dev, current.st_ino):
                                raise OSError(errno.ESTALE, 'directory changed during inventory')
                            directories.append({'rel': path, 'kind': 'directory', **_stat_fields(actual)})
                        except OSError as exc:
                            errors.append(_error(path, 'open_directory', exc))
                        finally:
                            if child_fd is not None:
                                os.close(child_fd)
                    local_dirs, late_dirs = [], []
                    if leaves or directories or errors:
                        connection.send(('page', [path for _, path in leaves], directories, errors))
                        command = connection.recv()
                        if command[0] != 'select':
                            raise RuntimeError('unexpected page selection')
                        selected, local_dirs = command[1:]
                        futures = [pool.submit(_leaf_record, fd, *leaves[index]) for index in selected]
                        entries = [future.result() for future in futures]
                        # A leaf that became a directory must be checked before
                        # it is emitted or scheduled, just like the normal path.
                        for index, entry in enumerate(entries):
                            if entry.get('kind') != '_late_directory':
                                continue
                            child_fd = None
                            try:
                                name = entry['rel'].rsplit('/', 1)[-1]
                                child_fd = _open_directory(name, dir_fd=fd)
                                actual = os.fstat(child_fd)
                                if (actual.st_dev, actual.st_ino) != (entry['dev'], entry['ino']):
                                    raise OSError(errno.ESTALE, 'directory changed during inventory')
                                entries[index] = {'rel': entry['rel'], 'kind': 'directory', **_stat_fields(actual)}
                                late_dirs.append((entry['rel'], (actual.st_dev, actual.st_ino)))
                            except OSError as exc:
                                entries[index] = _error(entry['rel'], 'open_directory', exc)
                            finally:
                                if child_fd is not None:
                                    os.close(child_fd)
                        connection.send(('records', entries))
                    if exhausted:
                        iterator.close()
                        os.close(fd)
                        frames.pop()
                    for index in reversed(local_dirs):
                        entry = directories[index]
                        frames.append((None, (entry['dev'], entry['ino']), entry['rel']))
                    for path, identity in reversed(late_dirs):
                        frames.append((None, identity, path))
                connection.send(('done', time.process_time() - started_cpu))
    except (EOFError, BrokenPipeError):
        pass
    except BaseException as exc:
        try:
            connection.send(('fatal', type(exc).__name__, str(exc)))
        except (EOFError, BrokenPipeError, OSError):
            pass
    finally:
        for fd, iterator, _ in reversed(frames):
            if fd is not None:
                iterator.close()
                os.close(fd)
        if anchor is not None:
            os.close(anchor)
        connection.close()


def walk(source_root, source_identity, root_rel, root_identity, phase, consume, *,
         seeded_paths, skip_seeded_nondirectories, processes, threads_per_process,
         batch_size, queue_size, check_cancel, on_tick=None):
    """Walk with a bounded spawn/Pipe protocol; all callbacks run on the caller."""
    context = multiprocessing.get_context('spawn')
    children, connections = [], []
    pending = [(root_rel, root_identity)]
    busy = set()
    ready = set()
    metrics = {'processes': processes, 'threads_per_process': threads_per_process,
               'start_method': 'spawn', 'pids': [], 'directory_tasks_by_pid': {},
               'pages': 0, 'leaf_names': 0, 'selected_leaves': 0,
               'skipped_seeded_leaves': 0, 'reader_cpu_seconds': 0.0,
               'peak_pending_directories': 1, 'local_directory_overflows': 0}
    last_tick = 0
    completed = False
    try:
        for _ in range(processes):
            parent, child = context.Pipe(duplex=True)
            process = context.Process(target=_reader,
                                      args=(child, source_root, source_identity, phase,
                                            batch_size, threads_per_process))
            process.start()
            child.close()
            children.append(process)
            connections.append(parent)
        while pending or busy or len(ready) != len(children):
            now = time.monotonic()
            if now - last_tick >= .1:
                check_cancel()
                if on_tick is not None:
                    on_tick()
                last_tick = now
            # A fast first spawn must not finish a small/warm tree while its
            # peers are still importing. Start work after every reader is ready.
            if len(ready) == len(children):
                for index in ready - busy:
                    if not pending:
                        break
                    rel, expected = pending.pop()
                    connections[index].send(('directory', rel, expected))
                    busy.add(index)
            readable = wait(connections, timeout=.1)
            for connection in readable:
                index = connections.index(connection)
                try:
                    message = connection.recv()
                except EOFError as exc:
                    raise RuntimeError('source reader exited without completing its task') from exc
                kind = message[0]
                if kind == 'ready':
                    ready.add(index)
                    metrics['pids'].append(message[1])
                    metrics['directory_tasks_by_pid'][message[1]] = 0
                elif kind == 'page':
                    paths, directories, errors = message[1:]
                    metrics['pages'] += 1
                    metrics['leaf_names'] += len(paths)
                    selected = [i for i, path in enumerate(paths)
                                if not skip_seeded_nondirectories or path not in seeded_paths]
                    metrics['selected_leaves'] += len(selected)
                    metrics['skipped_seeded_leaves'] += len(paths) - len(selected)
                    local_dirs = []
                    for directory_index, entry in enumerate(directories):
                        consume(entry)
                        if len(pending) < queue_size:
                            pending.append((entry['rel'], (entry['dev'], entry['ino'])))
                        else:
                            local_dirs.append(directory_index)
                            metrics['local_directory_overflows'] += 1
                    metrics['peak_pending_directories'] = max(metrics['peak_pending_directories'], len(pending))
                    for entry in errors:
                        consume(entry)
                    connection.send(('select', selected, local_dirs))
                elif kind == 'records':
                    for entry in message[1]:
                        consume(entry)
                        # A late leaf->directory change is walked locally by
                        # that reader, so even this race cannot grow pending
                        # beyond its configured bound.
                elif kind == 'done':
                    busy.remove(index)
                    metrics['directory_tasks_by_pid'][children[index].pid] += 1
                    metrics['reader_cpu_seconds'] += message[1]
                elif kind == 'fatal':
                    raise RuntimeError('source reader ' + message[1] + ': ' + message[2])
                else:
                    raise RuntimeError('unexpected source reader response')
            for process in children:
                if process.exitcode is not None:
                    raise RuntimeError('source reader exited unexpectedly: ' + str(process.exitcode))
        check_cancel()
        completed = True
        return metrics
    finally:
        if completed:
            for connection in connections:
                try:
                    connection.send(('stop',))
                except (EOFError, BrokenPipeError, OSError):
                    pass
        for process in children:
            if not completed and process.is_alive():
                process.terminate()
        for process in children:
            process.join(timeout=2)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
        for connection in connections:
            connection.close()
