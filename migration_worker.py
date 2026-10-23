"""Conservative, resumable copy primitives. Never remove or write source data.

copy_entry accepts scanner records with rel, kind and flat stat fields. A copied
file is published by link(2), which cannot replace an existing destination.
"""
import errno
import hashlib
import math
import os
import re
import stat
import threading
import time
import uuid

BUFFER_SIZE = 8 * 1024 * 1024
IDENTITY_FIELDS = ('dev', 'ino', 'size', 'mtime_ns', 'ctime_ns')
_cancel_event = None
_progress_callback = None
_directory_opener = None


class CopyCancelled(Exception):
    pass


def _check_cancel():
    event = _cancel_event
    if event is not None and event.is_set():
        raise CopyCancelled('copy paused')


class _RateLimiter:
    """One aggregate token bucket for payload I/O across this process."""

    def __init__(self):
        self.condition = threading.Condition()
        self.rate = 0.0
        self.capacity = 0.0
        self.tokens = 0.0
        self.updated = time.monotonic()

    def configure(self, bytes_per_second):
        rate = 0.0 if bytes_per_second is None else float(bytes_per_second)
        if not math.isfinite(rate) or rate < 0:
            raise ValueError('I/O rate must be a finite nonnegative number')
        with self.condition:
            self.rate = rate
            # At most one buffer of burst, and at most 100 ms at lower rates.
            self.capacity = min(BUFFER_SIZE, max(1.0, rate * 0.1)) if rate else 0.0
            self.tokens = 0.0
            self.updated = time.monotonic()
            self.condition.notify_all()

    def consume(self, byte_count):
        _check_cancel()
        remaining = float(byte_count)
        while remaining > 0:
            _check_cancel()
            with self.condition:
                if not self.rate:
                    return
                current = time.monotonic()
                self.tokens = min(self.capacity, self.tokens +
                                  (current - self.updated) * self.rate)
                self.updated = current
                portion = min(remaining, self.capacity)
                if self.tokens >= portion:
                    self.tokens -= portion
                    remaining -= portion
                else:
                    # Keep long reservations interruptible by reconfiguration
                    # and avoid a single blocking sleep longer than a second.
                    self.condition.wait(min(0.25, (portion - self.tokens) / self.rate))


_io_limiter = _RateLimiter()


def configure_rate_limit(bytes_per_second):
    """Set process-wide source-read + destination-write/readback bytes/sec.

    All worker threads share this budget. A verified normal copy charges its
    payload three times; identical-existing-file checks charge both reads.
    None or zero disables the cap. Call once per node worker process.
    """
    _io_limiter.configure(bytes_per_second)


def configure_cancel_event(event_or_none):
    """Use a shared threading.Event to pause all copies cooperatively."""
    if event_or_none is not None and not callable(getattr(event_or_none, 'is_set', None)):
        raise TypeError('cancel event must expose is_set() or be None')
    global _cancel_event
    _cancel_event = event_or_none
    with _io_limiter.condition:
        _io_limiter.condition.notify_all()


def configure_progress_callback(fn):
    """Report successfully written logical payload bytes across all threads.

    fn(delta_bytes) must be fast and thread safe. Sparse-hole seeks count their
    logical length. Partial copies/retries are included, checksums are excluded.
    None disables reporting without adding a callback invocation to I/O loops.
    """
    if fn is not None and not callable(fn):
        raise TypeError('progress callback must be callable or None')
    global _progress_callback
    _progress_callback = fn


def configure_directory_opener(fn_or_none):
    """Opt in to an equally strict read-only whole-parent opener.

    The callable accepts (root, rel) and returns an owned (parent_fd, basename).
    It must reject symlinks in every ancestor. Missing destination directories
    still use the conservative creation walk; no payload/verification behavior
    changes. Configure once before starting worker threads. None is the default.
    """
    if fn_or_none is not None and not callable(fn_or_none):
        raise TypeError('directory opener must be callable or None')
    global _directory_opener
    _directory_opener = fn_or_none


class SourceChanged(Exception):
    pass


class DestinationConflict(Exception):
    pass


def _record(st):
    return {key: getattr(st, 'st_' + key) for key in
            ('dev', 'ino', 'size', 'mode', 'uid', 'gid', 'nlink',
             'mtime_ns', 'ctime_ns', 'atime_ns')}


def _identity(st):
    return tuple(getattr(st, 'st_' + key) for key in IDENTITY_FIELDS)


def _check_scanned(st, entry):
    # st_dev identifies this node's mount, so a scan from another node may
    # report a different device number for the same shared inode.
    for key in IDENTITY_FIELDS:
        if key == 'dev':
            continue
        if key in entry and getattr(st, 'st_' + key) != entry[key]:
            raise SourceChanged('source changed since scan: ' + key)


def _components(rel, allow_empty=False):
    if not isinstance(rel, str) or '\x00' in rel or os.path.isabs(rel):
        raise ValueError('invalid relative path')
    if rel in ('', '.') and allow_empty:
        return []
    parts = rel.split('/')
    if not parts or any(part in ('', '.', '..') for part in parts):
        raise ValueError('unsafe relative path: ' + repr(rel))
    return parts


def _directory_flags():
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, 'O_CLOEXEC', 0)


def _open_directory_component(parent_fd, name, create=False):
    """Open an anchored real directory, creating only when it is missing.

    Existing ancestors are by far the common case. Avoid mkdir(EEXIST), which
    still takes a directory write lock on shared filesystems. Both opens reject
    symlinks; a concurrent creator may win mkdir without weakening that check.
    """
    try:
        return os.open(name, _directory_flags(), dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
    try:
        os.mkdir(name, mode=0o755, dir_fd=parent_fd)
    except FileExistsError:
        pass
    return os.open(name, _directory_flags(), dir_fd=parent_fd)


def _open_root(root, create=False):
    """Walk every component with O_NOFOLLOW, including the root's ancestors."""
    opener = _directory_opener
    if opener is not None:
        try:
            return opener(root, '__directory_open_root__')[0]
        except FileNotFoundError:
            if not create:
                raise
    root = os.path.abspath(root)
    fd = os.open('/', _directory_flags())
    try:
        for part in root.split('/'):
            if not part:
                continue
            newfd = _open_directory_component(fd, part, create=create)
            os.close(fd)
            fd = newfd
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_parent(root, rel, create=False):
    parts = _components(rel)
    opener = _directory_opener
    if opener is not None:
        try:
            return opener(root, rel)
        except FileNotFoundError:
            if not create:
                raise
    fd = _open_root(root, create=create)
    try:
        for part in parts[:-1]:
            newfd = _open_directory_component(fd, part, create=create)
            os.close(fd)
            fd = newfd
        return fd, parts[-1]
    except BaseException:
        os.close(fd)
        raise


def ensure_destination_directory(dst_root, rel=''):
    """Create a real directory tree, rejecting any symlink component."""
    parts = _components(rel, allow_empty=True)
    fd = _open_root(dst_root, create=True)
    try:
        for part in parts:
            newfd = _open_directory_component(fd, part, create=True)
            os.close(fd)
            fd = newfd
        return _record(os.fstat(fd))
    finally:
        os.close(fd)


def _hash_fd(fd):
    _check_cancel()
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    remaining = os.fstat(fd).st_size
    while remaining:
        count = min(BUFFER_SIZE, remaining)
        _io_limiter.consume(count)
        block = os.read(fd, count)
        if not block:
            break
        digest.update(block)
        remaining -= len(block)
    return digest.hexdigest()


def _copy_xattrs(src, dst, warnings, follow_symlinks=True):
    if not hasattr(os, 'listxattr'):
        return
    kwargs = {} if isinstance(src, int) else {'follow_symlinks': follow_symlinks}
    try:
        names = os.listxattr(src, **kwargs)
    except OSError as exc:
        if exc.errno not in (errno.ENOTSUP, errno.EOPNOTSUPP):
            warnings.append('listxattr: ' + str(exc))
        return
    for name in names:
        try:
            value = os.getxattr(src, name, **kwargs)
            dst_kwargs = {} if isinstance(dst, int) else {'follow_symlinks': follow_symlinks}
            os.setxattr(dst, name, value, **dst_kwargs)
        except OSError as exc:
            warnings.append('xattr ' + name + ': ' + str(exc))


def _metadata_fd(src_fd, dst_fd, source_stat, warnings):
    dst_stat = os.fstat(dst_fd)
    if (dst_stat.st_uid, dst_stat.st_gid) != (source_stat.st_uid, source_stat.st_gid):
        try:
            os.fchown(dst_fd, source_stat.st_uid, source_stat.st_gid)
        except OSError as exc:
            warnings.append('ownership: ' + str(exc))
    _copy_xattrs(src_fd, dst_fd, warnings)
    os.fchmod(dst_fd, stat.S_IMODE(source_stat.st_mode))
    os.utime(dst_fd, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))


def _existing_file(src_fd, dst_parent, dst_name, source_stat, warnings):
    """Hash both stable inodes before restoring metadata on identical data."""
    try:
        dst_lstat = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(dst_lstat.st_mode):
        raise DestinationConflict('destination exists with a different type')
    if dst_lstat.st_size != source_stat.st_size:
        raise DestinationConflict('destination exists with different size')
    dst_fd = os.open(dst_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dst_parent)
    try:
        dst_before = os.fstat(dst_fd)
        if _identity(dst_before) != _identity(dst_lstat):
            raise DestinationConflict('destination changed during inspection')
        source_hash = _hash_fd(src_fd)
        if _identity(os.fstat(src_fd)) != _identity(source_stat):
            raise SourceChanged('source changed during checksum')
        destination_hash = _hash_fd(dst_fd)
        if _identity(os.fstat(dst_fd)) != _identity(dst_before):
            raise DestinationConflict('destination changed during checksum')
        if source_hash != destination_hash:
            raise DestinationConflict('destination exists with different checksum')
        # The open descriptor still refers to the inode verified above. Ensure
        # the name has not been exchanged before touching its metadata.
        if _identity(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)) != _identity(dst_before):
            raise DestinationConflict('destination path changed during checksum')
        _check_cancel()
        _metadata_fd(src_fd, dst_fd, source_stat, warnings)
        return source_hash
    finally:
        os.close(dst_fd)


def _anchored_path(parent_fd, name, fallback):
    proc_path = '/proc/self/fd/' + str(parent_fd)
    return os.path.join(proc_path, name) if os.path.isdir(proc_path) else fallback


def _metadata_symlink(src_parent, src_name, dst_parent, dst_name, src_root, dst_root, rel, source_stat, warnings):
    try:
        os.chown(dst_name, source_stat.st_uid, source_stat.st_gid,
                 dir_fd=dst_parent, follow_symlinks=False)
    except (OSError, NotImplementedError) as exc:
        current = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
        if (current.st_uid, current.st_gid) != (source_stat.st_uid, source_stat.st_gid):
            warnings.append('symlink ownership: ' + str(exc))
    _copy_xattrs(_anchored_path(src_parent, src_name, os.path.join(src_root, rel)),
                 _anchored_path(dst_parent, dst_name, os.path.join(dst_root, os.path.dirname(rel), dst_name)),
                 warnings, follow_symlinks=False)
    try:
        os.utime(dst_name, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
                 dir_fd=dst_parent, follow_symlinks=False)
    except (OSError, NotImplementedError) as exc:
        warnings.append('symlink timestamps: ' + str(exc))


def _copy_symlink(src_root, dst_root, entry, src_parent, src_name, dst_parent, dst_name, temp_name, warnings, created):
    src_before = os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)
    _check_scanned(src_before, entry)
    if not stat.S_ISLNK(src_before.st_mode):
        raise SourceChanged('source is no longer a symlink')
    target = os.readlink(src_name, dir_fd=src_parent)
    expected_target = entry.get('target', entry.get('link_target', target))
    if target != expected_target or _identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != _identity(src_before):
        raise SourceChanged('source symlink changed')
    try:
        existing = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if not stat.S_ISLNK(existing.st_mode) or os.readlink(dst_name, dir_fd=dst_parent) != target:
            raise DestinationConflict('destination exists with different symlink target or type')
        if _identity(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)) != _identity(existing):
            raise DestinationConflict('destination symlink changed during inspection')
        _check_cancel()
        _metadata_symlink(src_parent, src_name, dst_parent, dst_name, src_root, dst_root, entry['rel'], src_before, warnings)
        return {'status': 'skipped', 'bytes': 0, 'target': target, 'source_identity': _record(src_before)}
    _check_cancel()
    os.symlink(target, temp_name, dir_fd=dst_parent)
    created.append(temp_name)
    _metadata_symlink(src_parent, src_name, dst_parent, temp_name, src_root, dst_root, entry['rel'], src_before, warnings)
    if _identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != _identity(src_before):
        raise SourceChanged('source symlink changed before publication')
    _check_cancel()
    try:
        os.link(temp_name, dst_name, src_dir_fd=dst_parent, dst_dir_fd=dst_parent, follow_symlinks=False)
    except FileExistsError:
        raise DestinationConflict('destination appeared during symlink copy')
    return {'status': 'copied', 'bytes': 0, 'target': target, 'source_identity': _record(src_before)}


def copy_entry(src_root, dst_root, entry, run_id, verify=True, allow_replace_snapshot=None):
    """Copy one file/symlink; return a JSON-compatible receipt, never raise.

    status is copied/skipped/changed/conflict/error. bytes counts newly copied
    payload only. Metadata failures that do not prevent copying are warnings.
    Existing different files are conflicts unless caller supplies the exact
    identity snapshot of its own earlier copy as allow_replace_snapshot.
    """
    result = {'rel': entry.get('rel'), 'kind': entry.get('kind'), 'bytes': 0}
    warnings = []
    src_parent = dst_parent = src_fd = temp_fd = None
    temp_created = False
    temp_name = '.migration-' + re.sub(r'[^a-zA-Z0-9_-]', '_', str(run_id))[:24] + '-' + uuid.uuid4().hex + '.tmp'
    try:
        _check_cancel()
        rel = entry['rel']
        src_parent, src_name = _open_parent(src_root, rel)
        dst_parent, dst_name = _open_parent(dst_root, rel, create=True)
        if entry['kind'] == 'symlink':
            # Cleanup is constrained to this unique, exclusively created name.
            created = []
            try:
                result.update(_copy_symlink(src_root, dst_root, entry, src_parent, src_name, dst_parent, dst_name, temp_name, warnings, created))
            finally:
                for created_name in created:
                    try:
                        os.unlink(created_name, dir_fd=dst_parent)
                    except FileNotFoundError:
                        pass
            result['dst_stat'] = _record(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False))
            return result
        if entry['kind'] != 'file':
            raise ValueError('unsupported entry kind: ' + str(entry['kind']))
        src_fd = os.open(src_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=src_parent)
        src_before = os.fstat(src_fd)
        _check_scanned(src_before, entry)
        if not stat.S_ISREG(src_before.st_mode):
            raise SourceChanged('source is no longer a regular file')
        replace_owned = False
        if allow_replace_snapshot is not None:
            if any(key not in allow_replace_snapshot for key in IDENTITY_FIELDS):
                raise ValueError('replacement snapshot requires full inode identity')
            try:
                present = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
            except FileNotFoundError:
                present = None
            if present is not None:
                if not stat.S_ISREG(present.st_mode) or any(getattr(present, 'st_' + key) != allow_replace_snapshot[key] for key in IDENTITY_FIELDS if key != 'dev'):
                    raise DestinationConflict('owned destination no longer matches replacement snapshot')
                replace_owned = True
        existing_hash = None if replace_owned else _existing_file(src_fd, dst_parent, dst_name, src_before, warnings)
        if existing_hash is not None:
            if _identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != _identity(src_before):
                raise SourceChanged('source path changed during checksum')
            result.update(status='skipped', sha256=existing_hash, source_identity=_record(src_before), dst_stat=_record(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)))
            return result
        temp_fd = os.open(temp_name, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=dst_parent)
        temp_created = True
        os.lseek(src_fd, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        remaining = src_before.st_size
        while remaining:
            count = min(BUFFER_SIZE, remaining)
            _io_limiter.consume(count)
            block = os.read(src_fd, count)
            if not block:
                raise SourceChanged('source became shorter during copy')
            digest.update(block)
            remaining -= len(block)
            # Preserve large zero extents instead of allocating them on the
            # destination. ftruncate below also materializes trailing holes.
            if block.count(0) == len(block):
                os.lseek(temp_fd, len(block), os.SEEK_CUR)
                callback = _progress_callback
                if callback is not None:
                    callback(len(block))
            else:
                view = memoryview(block)
                while view:
                    _io_limiter.consume(len(view))
                    written = os.write(temp_fd, view)
                    if not written:
                        raise OSError('zero-byte write')
                    callback = _progress_callback
                    if callback is not None:
                        callback(written)
                    view = view[written:]
        os.ftruncate(temp_fd, src_before.st_size)
        if _identity(os.fstat(src_fd)) != _identity(src_before) or _identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != _identity(src_before):
            raise SourceChanged('source changed during copy')
        source_hash = digest.hexdigest()
        if verify and _hash_fd(temp_fd) != source_hash:
            raise OSError('destination checksum mismatch')
        _check_cancel()
        _metadata_fd(src_fd, temp_fd, src_before, warnings)
        if _identity(os.fstat(src_fd)) != _identity(src_before) or _identity(os.stat(src_name, dir_fd=src_parent, follow_symlinks=False)) != _identity(src_before):
            raise SourceChanged('source metadata changed before publication')
        _check_cancel()
        os.fsync(temp_fd)
        _check_cancel()
        if replace_owned:
            current = os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)
            if any(getattr(current, 'st_' + key) != allow_replace_snapshot[key] for key in IDENTITY_FIELDS if key != 'dev'):
                raise DestinationConflict('owned destination changed before replacement')
            os.replace(temp_name, dst_name, src_dir_fd=dst_parent, dst_dir_fd=dst_parent)
            result['replaced_owned_copy'] = True
        else:
            try:
                os.link(temp_name, dst_name, src_dir_fd=dst_parent, dst_dir_fd=dst_parent, follow_symlinks=False)
            except FileExistsError:
                existing_hash = _existing_file(src_fd, dst_parent, dst_name, src_before, warnings)
                if existing_hash is None:
                    raise DestinationConflict('destination appeared and disappeared during publication')
                result.update(status='skipped', sha256=existing_hash, source_identity=_record(src_before), dst_stat=_record(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)))
                return result
        result.update(status='copied', bytes=src_before.st_size, sha256=source_hash,
                      source_identity=_record(src_before),
                      dst_stat=_record(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False)))
    except CopyCancelled as exc:
        result.update(status='error', cancelled=True,
                      detail='CopyCancelled: ' + str(exc))
    except SourceChanged as exc:
        result.update(status='changed', detail=str(exc))
    except DestinationConflict as exc:
        result.update(status='conflict', detail=str(exc))
    except Exception as exc:
        result.update(status='error', detail=type(exc).__name__ + ': ' + str(exc))
    finally:
        if temp_fd is not None:
            os.close(temp_fd)
        if temp_created and dst_parent is not None:
            try:
                os.unlink(temp_name, dir_fd=dst_parent)
            except FileNotFoundError:
                pass
            except OSError as exc:
                warnings.append('temporary cleanup: ' + str(exc))
        if result.get('status') in ('copied', 'skipped') and dst_parent is not None:
            try:
                # unlinking the temporary hard link changes inode ctime. Take
                # the final snapshot only after that cleanup has completed.
                result['dst_stat'] = _record(os.stat(dst_name, dir_fd=dst_parent, follow_symlinks=False))
            except OSError as exc:
                result.update(status='conflict', detail='destination unavailable after publication: ' + str(exc))
        if warnings:
            result['metadata_warnings'] = warnings
        for fd in (src_fd, src_parent, dst_parent):
            if fd is not None:
                os.close(fd)
    return result
