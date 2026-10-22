"""Process-local, fail-closed cache for one migration control state path.

Only the refresh daemon calls the supplied reader for ``state_path``. The
returned state is copied for each caller, so worker code cannot mutate the
cache. Other JSON paths are always passed directly to the original reader.
No path is resolved and no filesystem operation is used to match paths.
"""

import copy
import errno
import math
import os
import threading
import time


_UNSET = object()


def lexical_absolute(path):
    """Normalize a path lexically, without following links or touching NFS."""
    return os.path.abspath(os.fsdecode(os.fspath(path)))


class StateCache:
    """Refresh one state JSON in a daemon; never fall back to stale data.

    ``reader`` must be the saved, original ``migration_io.read_json`` function,
    not the patched dispatcher. A failed latest refresh invalidates reads
    immediately. A blocked refresher also fails closed after ``max_age``.
    """

    def __init__(self, state_path, reader, refresh_interval=0.5, max_age=3.0,
                 clock=time.monotonic):
        if (not math.isfinite(refresh_interval) or refresh_interval <= 0 or
                not math.isfinite(max_age) or max_age <= refresh_interval):
            raise ValueError('require 0 < refresh_interval < max_age, both finite')
        self.state_path = lexical_absolute(state_path)
        self.reader = reader
        self.refresh_interval = refresh_interval
        self.max_age = max_age
        self.clock = clock
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._value = _UNSET
        self._read_started = None
        self._error = None

    def refresh_once(self):
        """Perform one refresh outside the snapshot lock; return success.

        This synchronous entry point is intended for small local fixtures.
        Production callers should only use ``start`` and ``read_json``.
        """
        with self._refresh_lock:
            started = self.clock()
            try:
                value = copy.deepcopy(self.reader(self.state_path))
            except Exception as exc:
                # Store a plain message rather than an exception traceback
                # retaining potentially large JSON objects indefinitely.
                error = type(exc).__name__ + ': ' + str(exc)
                with self._lock:
                    self._error = error
                return False
            with self._lock:
                self._value = value
                # Measure freshness from the start of the underlying read.
                # A read stuck on NFS must not become "fresh" on completion.
                self._read_started = started
                self._error = None
            return True

    def _run(self):
        while not self._stop.is_set():
            self.refresh_once()
            self._stop.wait(self.refresh_interval)

    def start(self):
        """Start the sole daemon immediately; initialization stays fail closed."""
        with self._lock:
            if self._thread is not None:
                raise RuntimeError('state cache has already been started')
            if self._stop.is_set():
                raise RuntimeError('state cache is closed')
            self._thread = threading.Thread(
                target=self._run, name='migration-state-cache', daemon=True)
            thread = self._thread
        thread.start()
        return self

    def close(self, timeout=1.0):
        """Stop refreshes, without waiting indefinitely for blocked NFS I/O."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)

    def read(self):
        """Return an independent state snapshot, or raise ``OSError``."""
        with self._lock:
            value = self._value
            started = self._read_started
            error = self._error
            closed = self._stop.is_set()
        if closed:
            reason = 'state cache is closed'
        elif error is not None:
            reason = 'latest state refresh failed: ' + error
        elif value is _UNSET:
            reason = 'state cache has not initialized'
        elif self.clock() - started > self.max_age:
            reason = 'state cache is stale'
        else:
            # Published values are never mutated. Copy outside the lock so
            # readers and the refresh daemon only contend for a brief moment.
            return copy.deepcopy(value)
        raise OSError(errno.EIO, reason, self.state_path)

    def read_json(self, path):
        """Intercept only this exact lexical absolute state path."""
        try:
            matches = lexical_absolute(path) == self.state_path
        except (TypeError, ValueError):
            matches = False
        return self.read() if matches else self.reader(path)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.close()
