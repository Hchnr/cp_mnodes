"""Unactivated, anchored missing-parent optimization for private experiments.

Existing parents use the same strict whole-path O_RDONLY opener. If the leaf
parent is missing, find its nearest existing ancestor rather than reopening
every ancestor from the mount root. All new components are opened relative to
that descriptor with O_DIRECTORY|O_NOFOLLOW. A fixed striped lock table avoids
duplicate mkdir races within this one process; no directory FD is cached.

This module never installs itself in a live worker or launches any process.
"""
import os
import threading

from migration_linux_open import open_parent_readonly


def parts_of(rel):
    if not isinstance(rel, str) or '\x00' in rel or rel.startswith('/'):
        raise ValueError('strict relative file path required')
    parts = rel.split('/')
    if any(part in ('', '.', '..') for part in parts):
        raise ValueError('unsafe relative path')
    return parts


class FrontierParentOpener:
    def __init__(self, destination, original, *, check=lambda: None,
                 opener=open_parent_readonly, anchor_opener=None, stripes=1024):
        if type(stripes) is not int or not 1 <= stripes <= 4096:
            raise ValueError('bounded fixed lock table required')
        if not callable(original) or not callable(check) or not callable(opener):
            raise TypeError('callable original, cancellation check and strict opener required')
        self.destination = os.path.abspath(os.fsdecode(destination))
        self.original, self.check, self.opener = original, check, opener
        self.anchor_opener = opener if anchor_opener is None else anchor_opener
        if not callable(self.anchor_opener):
            raise TypeError('strict ancestor opener required')
        self.locks = tuple(threading.Lock() for _ in range(stripes))
        self.os = os

    def _component(self, parent, name):
        flags = (self.os.O_RDONLY | self.os.O_DIRECTORY | self.os.O_NOFOLLOW
                 | getattr(self.os, 'O_CLOEXEC', 0))
        try:
            return self.os.open(name, flags, dir_fd=parent)
        except FileNotFoundError:
            pass
        info = self.os.fstat(parent)
        lock = self.locks[hash((info.st_dev, info.st_ino, name)) % len(self.locks)]
        self.check()
        while not lock.acquire(timeout=.25):
            self.check()
        try:
            self.check()
            try:
                return self.os.open(name, flags, dir_fd=parent)
            except FileNotFoundError:
                pass
            try:
                self.os.mkdir(name, mode=0o755, dir_fd=parent)
            except FileExistsError:
                # A creator outside this process may win. Reopen with the
                # same no-symlink rules; EEXIST never proves a directory.
                pass
            return self.os.open(name, flags, dir_fd=parent)
        finally:
            lock.release()

    def __call__(self, root, rel, create=False):
        # Preserve source and all non-creating call semantics. This is not a
        # two-argument read-only directory_opener; it replaces _open_parent
        # only in a separately authorized private/new worker entry.
        if os.path.abspath(os.fsdecode(root)) != self.destination or not create:
            return self.original(root, rel, create=create)
        parts = parts_of(rel)
        self.check()
        try:
            return self.opener(root, rel)
        except FileNotFoundError:
            pass
        fd = None
        try:
            # The full parent has already failed. Usually only its final
            # directory is missing, so the very next probe is enough.
            for depth in range(len(parts) - 2, -1, -1):
                self.check()
                try:
                    fd, _ = self.anchor_opener(root, '/'.join(parts[:depth] + [parts[-1]]))
                except FileNotFoundError:
                    continue
                break
            if fd is None:
                # Never create a missing production mount/root. In a real
                # P4 worker this is independently required by scope/mount gates.
                raise FileNotFoundError('destination root or all ancestors unavailable')
            for component in parts[depth:-1]:
                self.check()
                child = self._component(fd, component)
                self.os.close(fd)
                fd = child
            result, fd = (fd, parts[-1]), None
            return result
        finally:
            if fd is not None:
                self.os.close(fd)
