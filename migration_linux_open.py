"""Read-only, no-symlink directory opens with an optional Linux fast path.

``open_parent_readonly`` has the same anchoring rule as the conservative copy
worker: every directory in the absolute root and relative parent must be a real
directory, not a symlink. Linux openat2 can enforce that for the entire pathname
in one syscall. Unsupported systems retain the component-by-component walk.

This module does not create directories, open a payload file, or cache directory
FDs. The returned FD is owned by the caller and must be closed by the caller.
"""

import ctypes
import errno
import os
import platform
import sys


_AT_FDCWD = -100
_RESOLVE_NO_MAGICLINKS = 0x02
_RESOLVE_NO_SYMLINKS = 0x04
_RESOLVE_BENEATH = 0x08
# Do not guess syscall numbers on untested ABIs. Both supported 64-bit Linux
# architectures use 437; every other platform safely uses the original walk.
_SYSCALL_NUMBERS = {'x86_64': 437, 'aarch64': 437}
_UNSUPPORTED_ERRNOS = frozenset((errno.ENOSYS, errno.EINVAL, errno.E2BIG,
                               errno.EPERM))
_openat2_unavailable = False
_openat2_succeeded = False


class _OpenHow(ctypes.Structure):
    _fields_ = [('flags', ctypes.c_uint64), ('mode', ctypes.c_uint64),
                ('resolve', ctypes.c_uint64)]


def _load_syscall():
    if sys.platform != 'linux':
        return None, None
    number = _SYSCALL_NUMBERS.get(platform.machine().lower())
    if number is None:
        return None, None
    try:
        library = ctypes.CDLL(None, use_errno=True)
        syscall = library.syscall
        syscall.restype = ctypes.c_long
        return syscall, number
    except (OSError, AttributeError):
        return None, None


_syscall, _syscall_number = _load_syscall()


def _directory_flags():
    return (os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW |
            getattr(os, 'O_CLOEXEC', 0))


def _components(rel):
    if not isinstance(rel, str) or '\x00' in rel or os.path.isabs(rel):
        raise ValueError('invalid relative path')
    parts = rel.split('/')
    if any(part in ('', '.', '..') for part in parts):
        raise ValueError('unsafe relative path: ' + repr(rel))
    return parts


def _walk_absolute_directory(path, flags=None):
    """Fallback matching an O_NOFOLLOW walk from /, without mkdir or stat."""
    flags = _directory_flags() if flags is None else flags
    fd = os.open('/', flags)
    try:
        for part in path.split('/'):
            if not part:
                continue
            newfd = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = newfd
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_absolute_directory(path, flags=None):
    global _openat2_unavailable, _openat2_succeeded
    flags = _directory_flags() if flags is None else flags
    if _syscall is not None and not _openat2_unavailable:
        how = _OpenHow(flags, 0,
                       _RESOLVE_NO_SYMLINKS | _RESOLVE_NO_MAGICLINKS)
        while True:
            ctypes.set_errno(0)
            fd = _syscall(ctypes.c_long(_syscall_number), ctypes.c_int(_AT_FDCWD),
                          ctypes.c_char_p(os.fsencode(path)), ctypes.byref(how),
                          ctypes.c_size_t(ctypes.sizeof(how)))
            if fd >= 0 or ctypes.get_errno() != errno.EINTR:
                break
        if fd >= 0:
            _openat2_succeeded = True
            return int(fd)
        code = ctypes.get_errno()
        if code not in _UNSUPPORTED_ERRNOS:
            # In particular, never retry ELOOP, ENOTDIR, ENOENT or EACCES with
            # a less restricted open. Ordinary path errors remain visible.
            raise OSError(code, os.strerror(code), path)
        # Old kernels, disabled syscalls/seccomp, and unsupported open_how
        # versions use the equally restrictive walk. Once disabled, avoid
        # adding one guaranteed-failing syscall to every file's hot path.
        _openat2_unavailable = True
    return _walk_absolute_directory(path, flags)


def open_directory_beneath(anchor, rel):
    """Open a source directory from an existing anchor without following links.

    The full relative walk is one openat2 syscall when supported. Fallback
    retains the original component walk and O_NOATIME behavior. No payload
    file or destination is opened; callers still verify the directory inode.
    """
    global _openat2_unavailable, _openat2_succeeded
    if not isinstance(rel,str):
        raise ValueError('relative source directory must be a string')
    parts = _components(rel) if rel else []
    if not parts:
        return os.dup(anchor)
    flags = _directory_flags() | getattr(os, 'O_NOATIME', 0)
    if _syscall is not None and not _openat2_unavailable:
        how = _OpenHow(flags, 0, _RESOLVE_BENEATH | _RESOLVE_NO_SYMLINKS | _RESOLVE_NO_MAGICLINKS)
        while True:
            ctypes.set_errno(0)
            fd = _syscall(ctypes.c_long(_syscall_number), ctypes.c_int(anchor),
                          ctypes.c_char_p(os.fsencode(rel)), ctypes.byref(how),
                          ctypes.c_size_t(ctypes.sizeof(how)))
            if fd >= 0 or ctypes.get_errno() != errno.EINTR:
                break
        if fd >= 0:
            _openat2_succeeded = True
            return int(fd)
        code = ctypes.get_errno()
        if code not in _UNSUPPORTED_ERRNOS:
            raise OSError(code, os.strerror(code), rel)
        _openat2_unavailable = True
    from migration_scan import _open_directory
    fd = os.dup(anchor)
    try:
        for part in parts:
            child = _open_directory(part, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def safe_open_backend():
    """Report actual syscall support, not merely a recognized architecture.

    A first health query opens and immediately closes / with the same read-only
    flags. Later queries are in-memory unless support has become unavailable.
    This never reports openat2 before a real successful openat2 call.
    """
    if _syscall is None or _openat2_unavailable:
        return 'fallback'
    if not _openat2_succeeded:
        fd = _open_absolute_directory('/')
        os.close(fd)
    return 'openat2' if _openat2_succeeded and not _openat2_unavailable else 'fallback'


def _parent_path(root, rel):
    parts = _components(rel)
    root = os.fsdecode(os.fspath(root))
    if '\x00' in root:
        raise ValueError('embedded null byte in root')
    return os.path.join(os.path.abspath(root), *parts[:-1]), parts[-1]


def open_parent_readonly(root, rel):
    """Return ``(directory_fd, basename)``; reject symlinks in all ancestors.

    ``rel`` must be a nonempty strict relative path without '.', '..', empty
    components or NULs. The root follows the existing worker's ``abspath``
    normalization, including support for a path-like or bytes root.
    Missing directories are errors; callers needing mkdir must use their
    conservative creation path instead of this read-only helper.
    """
    parent, name = _parent_path(root, rel)
    return _open_absolute_directory(parent), name


def open_parent_path(root, rel):
    """Same strict ancestor resolution, returning a directory-only O_PATH FD.

    Used only as a dir_fd for *at operations and fstat, never for directory
    reads, fchmod or fsync. Unsupported platforms keep the read-only behavior.
    """
    parent, name = _parent_path(root, rel)
    flags = (getattr(os, 'O_PATH', os.O_RDONLY) | os.O_DIRECTORY |
             os.O_NOFOLLOW | getattr(os, 'O_CLOEXEC', 0))
    return _open_absolute_directory(parent, flags), name
