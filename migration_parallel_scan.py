"""Bounded parallel source inventory with one manifest/task producer.

Directory readers never write manifests or call emit_batch. The calling thread
serializes inventory records, hardlink canonical selection and batch emission.
resume_records are copied unchanged into a NEW combined manifest, counted once,
and excluded from new emissions; their directories are still traversed.
"""
import errno
import json
import os
import queue
import stat
import threading
import time

from migration_scan import PHASE_PATHS, PRIORITY_SUBTREES, _Scanner, _stat_fields, _open_directory
from migration_worker import _open_root


class ScanCancelled(RuntimeError):
    pass


def _seeded_nondirectory(child, path, seeded_paths):
    """Avoid re-statting replayed leaves, but never prune directory walks.

    DirEntry can answer from readdir type information, falling back to stat
    when needed. A former file that is now a directory must still be walked.
    Observation failures take the ordinary checked inventory path instead.
    """
    if path not in seeded_paths:
        return False
    try:
        return not child.is_dir(follow_symlinks=False)
    except OSError:
        return False


def _seed(scanner, entry, source_dev=None, original_root_dev=None):
    kind = entry.get('kind')
    stats = scanner.stats
    if kind == 'directory':
        stats['dirs'] += 1
    elif kind == 'file':
        stats['files'] += 1
        stats['bytes'] += entry.get('size', 0)
        if entry.get('nlink', 1) > 1:
            canonical = entry.get('hardlink_to', entry['rel'])
            scanner.hardlinks.setdefault((entry['dev'], entry['ino']), canonical)
            if source_dev is not None and entry.get('dev') == original_root_dev:
                scanner.hardlinks.setdefault((source_dev, entry['ino']), canonical)
        if entry.get('hardlink_to'):
            stats['hardlinks'] += 1
        else:
            stats['copy_bytes'] += entry.get('size', 0)
            stats['allocated_bytes'] += entry.get('blocks', 0) * 512
            stats['copy_entries'] += 1
    elif kind == 'symlink':
        stats['symlinks'] += 1
        stats['copy_entries'] += 1
    elif kind == 'error':
        stats['errors'] += 1
    elif entry.get('unsupported'):
        stats['unsupported'] += 1


def _consume(scanner, entry):
    kind = entry['kind']
    stats = scanner.stats
    if kind == 'directory':
        stats['dirs'] += 1
    elif kind == 'file':
        stats['files'] += 1
        stats['bytes'] += entry['size']
        if entry.get('nlink', 1) > 1:
            canonical = scanner.hardlinks.setdefault((entry['dev'], entry['ino']), entry['rel'])
            if canonical != entry['rel']:
                entry['hardlink_to'] = canonical
                stats['hardlinks'] += 1
                scanner.write(entry)
                return
        stats['copy_bytes'] += entry['size']
        stats['allocated_bytes'] += entry.get('blocks', 0) * 512
    elif kind == 'symlink':
        stats['symlinks'] += 1
    elif kind == 'error':
        stats['errors'] += 1
    else:
        stats['unsupported'] += 1
    scanner.write(entry)
    if kind in ('file', 'symlink'):
        scanner.enqueue(entry)
    elif kind == 'error' or entry.get('unsupported'):
        if scanner.on_error is not None:
            scanner.on_error(entry)


def scan_phase(source_root, phase, emit_batch, manifest_path, *,
               batch_bytes=1024**3, batch_files=32, hardlink_index=None,
               on_error=None, overwrite_manifest=False, entries_per_second=0,
               workers=32, queue_size=8192, control=None, cancel=None,
               resume_records=None, skip_rel=None, seed_stats=None,
               skip_seeded_nondirectories=False, initial_seeded_paths=None,
               append_manifest=False, on_tick=None, processes=0,
               threads_per_process=16, process_batch_size=256):
    """Compatible scan_phase plus parallelism and explicit combined-manifest resume.

    resume_records must be complete parsed records from an older immutable
    manifest, not from manifest_path being written. Old copyable records are NOT
    emitted: caller must recover their already-published/orphan tasks separately.
    Optional skip_rel suppresses whole relative paths (but never directory walk).
    seed_stats is an alternative to counting resume_records; supply totals only
    for old records represented by skip_rel, never unscanned current records.
    Cancellation raises ScanCancelled and leaves a readable partial manifest.
    skip_seeded_nondirectories avoids repeated leaf metadata/event work only
    for paths already represented by resume_records; directories still walk.
    processes enables independent spawn-based readers, each using a bounded
    name/selection/metadata exchange. The seeded set stays in the caller and
    all manifest, hardlink and task callbacks retain their single owner.
    """
    if phase not in PHASE_PATHS:
        raise ValueError('phase must be 1, 2, or 3')
    if min(batch_bytes, batch_files, workers, queue_size) < 1:
        raise ValueError('batch, worker and queue limits must be positive')
    if (not isinstance(processes, int) or not 0 <= processes <= 64
            or not isinstance(threads_per_process, int) or not 1 <= threads_per_process <= 128
            or not isinstance(process_batch_size, int) or not 1 <= process_batch_size <= 4096):
        raise ValueError('invalid process reader limits')
    source_root = os.path.abspath(os.fspath(source_root))
    manifest_path = os.path.abspath(os.fspath(manifest_path))
    if os.path.commonpath((os.path.realpath(source_root), os.path.realpath(manifest_path))) == os.path.realpath(source_root):
        raise ValueError('manifest must be outside source_root')
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    started = time.time()
    stop = threading.Event()
    events = queue.Queue(maxsize=queue_size)
    directories = queue.Queue(maxsize=queue_size)
    pending_lock = threading.Lock()
    outstanding = [0]
    skipped = set() if skip_rel is None else set(skip_rel)
    seeded_keys = set()
    if initial_seeded_paths is not None and resume_records is not None:
        raise ValueError('prepared paths and manifest replay are mutually exclusive')
    if append_manifest and initial_seeded_paths is None:
        raise ValueError('append requires an explicitly prepared inventory')
    seeded_paths = set() if initial_seeded_paths is None else initial_seeded_paths
    seen_paths = set()
    threads = []
    anchor = None
    process_metrics = None

    def external_cancelled():
        if cancel is not None:
            if bool(cancel() if callable(cancel) else cancel.is_set()):
                return True
        return control is not None and os.path.exists(os.path.join(os.fspath(control), 'STOP'))

    def emit(event):
        while not stop.is_set():
            try:
                events.put(event, timeout=0.1)
                return True
            except queue.Full:
                pass
        return False

    def error(rel, operation, exc):
        emit({'rel': rel, 'kind': 'error', 'operation': operation,
              'error': str(exc), 'errno': getattr(exc, 'errno', None)})

    def open_task(rel, expected):
        fd = os.dup(anchor)
        try:
            for part in rel.split('/') if rel else ():
                child = _open_directory(part, dir_fd=fd)
                os.close(fd)
                fd = child
            actual = os.fstat(fd)
            if (actual.st_dev, actual.st_ino) != expected:
                raise OSError(errno.ESTALE, 'directory changed before parallel enumeration')
            return fd
        except BaseException:
            os.close(fd)
            raise

    def schedule(rel, current):
        with pending_lock:
            outstanding[0] += 1
        try:
            directories.put_nowait((rel, (current.st_dev, current.st_ino)))
            return True
        except queue.Full:
            with pending_lock:
                outstanding[0] -= 1
            return False

    def enumerate_task(task):
        rel, expected = task
        try:
            fd = open_task(rel, expected)
        except OSError as exc:
            error(rel, 'open_directory', exc)
            return
        frames = []
        def push(directory_fd, path):
            try:
                iterator = os.scandir(directory_fd)
            except OSError as exc:
                os.close(directory_fd)
                error(path, 'scandir', exc)
                return
            frames.append((directory_fd, iterator, path))
        push(fd, rel)
        try:
            while frames and not stop.is_set():
                fd, iterator, parent = frames[-1]
                try:
                    child = next(iterator)
                except StopIteration:
                    iterator.close(); os.close(fd); frames.pop()
                    continue
                except OSError as exc:
                    error(parent, 'scandir', exc)
                    iterator.close(); os.close(fd); frames.pop()
                    continue
                path = parent + '/' + child.name if parent else child.name
                if phase == 3 and path in PRIORITY_SUBTREES:
                    continue
                if (skip_seeded_nondirectories
                        and _seeded_nondirectory(child, path, seeded_paths)):
                    continue
                try:
                    current = child.stat(follow_symlinks=False)
                except OSError as exc:
                    error(path, 'stat', exc)
                    continue
                fields = _stat_fields(current)
                if stat.S_ISDIR(current.st_mode):
                    try:
                        child_fd = _open_directory(child.name, dir_fd=fd)
                        actual = os.fstat(child_fd)
                        if (actual.st_dev, actual.st_ino) != (current.st_dev, current.st_ino):
                            os.close(child_fd)
                            raise OSError(errno.ESTALE, 'directory changed during inventory')
                    except OSError as exc:
                        error(path, 'open_directory', exc)
                        continue
                    if not emit({'rel': path, 'kind': 'directory', **_stat_fields(actual)}):
                        os.close(child_fd)
                        break
                    if schedule(path, actual):
                        os.close(child_fd)
                    else:
                        # A full directory queue must never deadlock all readers.
                        # Consume overflow as local DFS, holding only its depth.
                        push(child_fd, path)
                    continue
                if stat.S_ISREG(current.st_mode):
                    entry = {'rel': path, 'kind': 'file', **fields}
                elif stat.S_ISLNK(current.st_mode):
                    try:
                        target = os.readlink(child.name, dir_fd=fd)
                        after = os.stat(child.name, dir_fd=fd, follow_symlinks=False)
                        if (after.st_dev, after.st_ino, after.st_ctime_ns) != (current.st_dev, current.st_ino, current.st_ctime_ns):
                            raise OSError(errno.ESTALE, 'symlink changed during inventory')
                    except OSError as exc:
                        error(path, 'readlink', exc)
                        continue
                    entry = {'rel': path, 'kind': 'symlink', 'target': target, 'link_target': target, **fields}
                else:
                    kind = ('fifo' if stat.S_ISFIFO(current.st_mode) else
                            'socket' if stat.S_ISSOCK(current.st_mode) else
                            'char' if stat.S_ISCHR(current.st_mode) else
                            'block' if stat.S_ISBLK(current.st_mode) else 'unknown')
                    entry = {'rel': path, 'kind': kind, 'unsupported': True, **fields}
                emit(entry)
        finally:
            for fd, iterator, _ in reversed(frames):
                iterator.close(); os.close(fd)

    def reader():
        while not stop.is_set():
            try:
                task = directories.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                enumerate_task(task)
            except BaseException as exc:
                emit({'_fatal': exc})
            finally:
                with pending_lock:
                    outstanding[0] -= 1
                directories.task_done()

    if append_manifest and not os.path.isfile(manifest_path):
        raise FileNotFoundError(manifest_path)
    with open(manifest_path, 'a' if append_manifest else 'w' if overwrite_manifest else 'x', encoding='utf-8') as manifest:
        scanner = _Scanner(manifest, emit_batch, batch_bytes, batch_files,
                           {} if hardlink_index is None else hardlink_index,
                           on_error, phase, entries_per_second)
        try:
            if external_cancelled():
                raise ScanCancelled('STOP requested before scan')
            try:
                anchor = _open_root(source_root)
            except OSError as exc:
                scanner.error('', 'open_source_root', exc)
            source_dev = os.fstat(anchor).st_dev if anchor is not None else None
            original_root_dev = None
            if resume_records is not None:
                last_seed_cancel_check = 0
                for entry in resume_records:
                    if time.monotonic() - last_seed_cancel_check >= 0.1:
                        if external_cancelled():
                            raise ScanCancelled('STOP requested during manifest seed')
                        last_seed_cancel_check = time.monotonic()
                    if entry.get('kind') == 'directory' and entry.get('rel') == '':
                        original_root_dev = entry.get('dev')
                    scanner.write(entry)
                    seeded_keys.add((entry.get('rel'), entry.get('kind')))
                    if entry.get('kind') != 'error':
                        seeded_paths.add(entry.get('rel'))
                    _seed(scanner, entry, source_dev, original_root_dev)
            if seed_stats is not None:
                for key in scanner.stats:
                    if key != 'phase' and key in seed_stats:
                        scanner.stats[key] = seed_stats[key]
            def consume(entry):
                if (entry.get('rel') not in skipped and entry.get('rel') not in seeded_paths
                        and entry.get('rel') not in seen_paths
                        and (entry.get('rel'), entry.get('kind')) not in seeded_keys):
                    _consume(scanner, entry)
                    if entry.get('kind') != 'error':
                        seen_paths.add(entry.get('rel'))
            if anchor is not None:
                fd = os.dup(anchor)
                phase_rel = ''
                try:
                    consume({'rel': '', 'kind': 'directory', **_stat_fields(os.fstat(fd))})
                    for part in PHASE_PATHS[phase].split('/') if PHASE_PATHS[phase] else ():
                        phase_rel = phase_rel + '/' + part if phase_rel else part
                        try:
                            child_fd = _open_directory(part, dir_fd=fd)
                        except OSError as exc:
                            scanner.error(phase_rel, 'open_phase_root', exc)
                            break
                        os.close(fd); fd = child_fd
                        consume({'rel': phase_rel, 'kind': 'directory', **_stat_fields(os.fstat(fd))})
                    else:
                        if processes:
                            from migration_process_scan import walk

                            def check_process_cancel():
                                if external_cancelled():
                                    raise ScanCancelled('STOP requested during process scan')

                            root_stat, phase_stat = os.fstat(anchor), os.fstat(fd)
                            process_metrics = walk(
                                source_root, (root_stat.st_dev, root_stat.st_ino),
                                phase_rel, (phase_stat.st_dev, phase_stat.st_ino), phase, consume,
                                seeded_paths=seeded_paths,
                                skip_seeded_nondirectories=skip_seeded_nondirectories,
                                processes=processes, threads_per_process=threads_per_process,
                                batch_size=process_batch_size, queue_size=queue_size,
                                check_cancel=check_process_cancel, on_tick=on_tick)
                        else:
                            schedule(phase_rel, os.fstat(fd))
                            for index in range(min(256, int(workers))):
                                thread = threading.Thread(target=reader, name='scan-dir-' + str(index), daemon=True)
                                threads.append(thread); thread.start()
                            last_cancel_check = 0
                            while True:
                                if time.monotonic() - last_cancel_check >= 0.1:
                                    if external_cancelled():
                                        raise ScanCancelled('STOP requested during parallel scan')
                                    last_cancel_check = time.monotonic()
                                    if on_tick is not None:
                                        on_tick()
                                try:
                                    entry = events.get(timeout=0.1)
                                except queue.Empty:
                                    with pending_lock:
                                        if outstanding[0] == 0:
                                            break
                                    continue
                                if '_fatal' in entry:
                                    raise entry['_fatal']
                                consume(entry)
                finally:
                    os.close(fd)
            if external_cancelled():
                raise ScanCancelled('STOP requested before final batch')
            scanner.flush_batch()
            if external_cancelled():
                raise ScanCancelled('STOP requested during final batch')
            manifest.flush(); os.fsync(manifest.fileno())
            result = {**scanner.stats, 'source_root': source_root, 'manifest_path': manifest_path,
                    'elapsed_seconds': round(time.time() - started, 3),
                    'scan_complete': scanner.stats['errors'] == 0,
                    'fully_copyable': scanner.stats['unsupported'] == 0,
                    'workers': min(256, int(workers)), 'resumed_records': len(seeded_keys)}
            if process_metrics is not None:
                result['process_readers'] = process_metrics
            return result
        finally:
            stop.set()
            for thread in threads:
                thread.join()
            if anchor is not None:
                os.close(anchor)
