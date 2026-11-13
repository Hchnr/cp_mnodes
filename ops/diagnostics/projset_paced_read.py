#!/usr/bin/env python3
"""Best-effort safeguards for small, paced SQLite reads on shared storage.

These helpers do not bound kernel or server I/O: readahead and remote storage
may ignore advisory hints and a single filesystem call may block indefinitely.
Callers must use bounded indexed queries and independent health supervision.
"""
import fcntl
import math
import os
from pathlib import Path
import stat
import time


def disable_sqlite_readahead(source):
    """Advise RANDOM on the read-only SQLite descriptors for exactly source.

    The advice is made on the descriptors SQLite actually uses, rather than a
    separately opened file description. Fail closed if no matching descriptor
    exists or a matching descriptor is writable.
    """
    source = Path(source)
    info = source.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('SQLite source must be a regular, non-symlink file')
    identity = (info.st_dev, info.st_ino)
    matches = []
    for name in os.listdir('/proc/self/fd'):
        if not name.isdecimal():
            continue
        fd = int(name)
        try:
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino) != identity:
                continue
            flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        except OSError:
            continue  # descriptor closed after listing /proc
        if flags & os.O_ACCMODE != os.O_RDONLY:
            raise ValueError('matching SQLite source descriptor is writable')
        matches.append(fd)
    if not matches:
        raise ValueError('SQLite source descriptor was not found')
    for fd in matches:
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_RANDOM)
    after = source.lstat()
    if not stat.S_ISREG(after.st_mode) or (after.st_dev, after.st_ino) != identity:
        raise ValueError('SQLite source identity changed during setup')
    return len(matches)


def read_process_io():
    """Read process-attributed logical and physical read counters in bytes."""
    with open('/proc/self/io', encoding='ascii') as stream:
        values = dict(line.split(':', 1) for line in stream)
    result = tuple(int(values[key].strip()) for key in ('rchar', 'read_bytes'))
    if any(value < 0 for value in result):
        raise ValueError('negative process I/O counter')
    return result


class ReadPacer:
    """Charge each batch by its larger logical or process-attributed read.

    No idle-time credit accumulates. A query can overshoot max_batch before
    control returns; exceeding it stops the caller instead of continuing.
    """
    def __init__(self, rate, counter=read_process_io, health=lambda: None,
                 clock=time.monotonic, sleep=time.sleep, max_sleep=1.0,
                 max_batch=1024 * 1024):
        if (not isinstance(rate, int) or isinstance(rate, bool) or rate <= 0 or
                not callable(counter) or not callable(health) or not callable(clock) or not callable(sleep) or
                not isinstance(max_sleep, (int, float)) or isinstance(max_sleep, bool) or
                not math.isfinite(max_sleep) or max_sleep <= 0 or
                not isinstance(max_batch, int) or isinstance(max_batch, bool) or max_batch <= 0):
            raise ValueError('invalid read pacing configuration')
        self.rate, self.counter, self.health = rate, counter, health
        self.clock, self.sleep, self.max_sleep = clock, sleep, max_sleep
        self.max_batch = max_batch
        self.last = self._counter()

    def _counter(self):
        value = self.counter()
        if (not isinstance(value, (tuple, list)) or len(value) != 2 or
                any(not isinstance(x, int) or isinstance(x, bool) or x < 0 for x in value)):
            raise ValueError('invalid process read counters')
        return tuple(value)

    def throttle(self):
        current = self._counter()
        delta = tuple(a - b for a, b in zip(current, self.last))
        if any(x < 0 for x in delta):
            raise ValueError('process read counter regressed')
        charge = max(delta)
        if charge > self.max_batch:
            raise ValueError('single read batch exceeded its budget '
                             '(logical=%d, process_read=%d, limit=%d)' %
                             (delta[0], delta[1], self.max_batch))
        remaining = charge / self.rate
        while remaining > 0:
            self.health()
            started = self.clock()
            self.sleep(min(remaining, self.max_sleep))
            elapsed = self.clock() - started
            if not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed) or elapsed <= 0:
                raise ValueError('clock failed while pacing')
            remaining -= elapsed
        self.last = current
        return {'logical_bytes': delta[0], 'process_read_bytes': delta[1], 'charged_bytes': charge}
