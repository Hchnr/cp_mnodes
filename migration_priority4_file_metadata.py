"""UNDEPLOYED, FD-only regular-file metadata observations for P4 hardlinks.

No payload, links, directories, file metadata or indexes are written. Callers
must supply scope-checked, no-follow FDs and perform the separate current SHA,
ownership, dependency and publication checks. A result never permits linking.
The native ACL backend covers Linux POSIX access ACLs, not other ACL models.
"""
import ctypes
import ctypes.util
import functools
import hashlib
import os
import stat
import sys


class MetadataUnavailable(RuntimeError):
    pass


class MetadataChanged(RuntimeError):
    pass


class MetadataCancelled(RuntimeError):
    pass


def _check_stop(stop):
    if stop is not None and stop.is_set():
        raise MetadataCancelled('STOP requested during file metadata observation')


class LinuxPosixACL:
    """Read numeric access ACLs through libacl without a getfacl subprocess."""
    def __init__(self):
        if not sys.platform.startswith('linux'):
            raise MetadataUnavailable('Linux POSIX ACL backend is required')
        name = ctypes.util.find_library('acl')
        if not name:
            raise MetadataUnavailable('libacl is unavailable; ACLs cannot be assumed empty')
        self.library = ctypes.CDLL(name, use_errno=True)
        self.library.acl_get_fd.argtypes = [ctypes.c_int]
        self.library.acl_get_fd.restype = ctypes.c_void_p
        self.library.acl_to_any_text.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                               ctypes.c_char, ctypes.c_int]
        self.library.acl_to_any_text.restype = ctypes.c_void_p
        self.library.acl_free.argtypes = [ctypes.c_void_p]
        self.library.acl_free.restype = ctypes.c_int

    def __call__(self, fd):
        acl = self.library.acl_get_fd(fd)
        if not acl:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        text = None
        try:
            # TEXT_NUMERIC_IDS from Linux acl/include/libacl.h. Numeric IDs
            # avoid name-service aliases or NSS changes concealing differences.
            text = self.library.acl_to_any_text(acl, None, b'\n', 0x08)
            if not text:
                error = ctypes.get_errno()
                raise OSError(error, os.strerror(error))
            return ctypes.string_at(text)
        finally:
            if text is not None:
                self.library.acl_free(text)
            self.library.acl_free(acl)


@functools.lru_cache(maxsize=1)
def _native_acl():
    return LinuxPosixACL()


def _signature(value):
    return tuple(getattr(value, 'st_' + key) for key in
                 ('dev', 'ino', 'size', 'mode', 'uid', 'gid', 'nlink',
                  'mtime_ns', 'ctime_ns'))


def _digest(value):
    if not isinstance(value, bytes):
        raise TypeError('metadata readers must return bytes')
    return {'bytes': len(value), 'sha256': hashlib.sha256(value).hexdigest()}


def _xattrs(fd, stop):
    _check_stop(stop)
    names = os.listxattr(fd)
    if any(not isinstance(name, str) for name in names) or len(set(names)) != len(names):
        raise ValueError('invalid extended attribute names')
    result = {}
    for name in sorted(names):
        _check_stop(stop)
        result[name] = _digest(os.getxattr(fd, name))
    return result


def snapshot(fd, *, acl_reader=None, stop=None):
    """Observe stable metadata; unsupported or unreadable ACLs/xattrs fail."""
    _check_stop(stop)
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError('regular-file metadata requires a regular file FD')
    if not callable(getattr(os, 'listxattr', None)) or not callable(getattr(os, 'getxattr', None)):
        raise MetadataUnavailable('extended attribute APIs are unavailable')
    reader = _native_acl() if acl_reader is None else acl_reader
    attributes = _xattrs(fd, stop)
    _check_stop(stop)
    access_acl = _digest(reader(fd))
    _check_stop(stop)
    attributes_after = _xattrs(fd, stop)
    access_acl_after = _digest(reader(fd))
    after = os.fstat(fd)
    _check_stop(stop)
    if (_signature(before) != _signature(after) or attributes != attributes_after or
            access_acl != access_acl_after):
        raise MetadataChanged('regular-file metadata changed during observation')
    return {'identity': dict(zip(('dev', 'ino', 'size', 'mode', 'uid', 'gid', 'nlink',
                                 'mtime_ns', 'ctime_ns'), _signature(before))),
            'mode': stat.S_IMODE(before.st_mode), 'uid': before.st_uid, 'gid': before.st_gid,
            'mtime_ns': before.st_mtime_ns, 'atime_ns_observed': before.st_atime_ns,
            'visible_xattrs': attributes, 'posix_access_acl': access_acl}


def _stable_observation(value):
    return {key: item for key, item in value.items() if key != 'atime_ns_observed'}


def compare(source_fd, target_fd, *, acl_reader=None, stop=None):
    """Compare bounded current observations, without authorizing publication."""
    source = snapshot(source_fd, acl_reader=acl_reader, stop=stop)
    target = snapshot(target_fd, acl_reader=acl_reader, stop=stop)
    source_after = snapshot(source_fd, acl_reader=acl_reader, stop=stop)
    target_after = snapshot(target_fd, acl_reader=acl_reader, stop=stop)
    if (_stable_observation(source) != _stable_observation(source_after) or
            _stable_observation(target) != _stable_observation(target_after)):
        raise MetadataChanged('source or target changed while observing the other file')
    fields = ('mode', 'uid', 'gid', 'mtime_ns', 'visible_xattrs', 'posix_access_acl')
    differences = [key for key in fields if source[key] != target[key]]
    _check_stop(stop)
    return {'source': source, 'destination': target,
            'compared_fields': list(fields), 'differences': differences,
            'equivalent_observed_metadata': not differences,
            'acl_scope': 'Linux POSIX regular-file access ACL',
            'atime_policy': 'observed only; future owned publication must restore and verify it after payload reads',
            'verification_only': True, 'publication_ready': False,
            'publication_performed': False, 'metadata_written': False,
            'payload_read': False,
            'remaining': ['scope/dependency and current content/ownership proof',
                          'other ACL models if the filesystem exposes them',
                          'immediate pre-publication rechecks and final alias inode/metadata snapshots']}
