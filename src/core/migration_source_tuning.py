"""Background-only, identity-bound source dispatch tuning; no payload changes."""
from pathlib import Path

from migration_io import read_json
from migration_state_cache import StateCache


class SourceLimit:
    def __init__(self, path, control, run_id, maximum, initial=None, *, scope='warm-source-reader-limit-v1'):
        initial=maximum if initial is None else initial
        if (type(maximum) is not int or not 1<=maximum<=64
                or type(initial) is not int or not 1<=initial<=maximum):
            raise ValueError('invalid prepared source budget')
        if scope not in ('warm-source-reader-limit-v1', 'priority4-source-reader-limit-v1'):
            raise ValueError('invalid source reader tuning scope')
        self.scope = scope
        self.control, self.run_id, self.maximum = str(control), run_id, maximum
        self.value, self.error = initial, None
        self.cache = StateCache(Path(path), read_json, refresh_interval=5, max_age=60)

    def start(self):
        self.cache.start()
        return self

    def __call__(self):
        try:
            value = self.cache.read()  # RAM only, including during blocked NFS.
            if (value.get('scope') != self.scope
                    or value.get('control') != self.control or value.get('run_id') != self.run_id
                    or type(value.get('processes')) is not int
                    or not 1 <= value['processes'] <= self.maximum):
                raise ValueError('invalid source reader limit control/run/scope/budget')
            self.value, self.error = value['processes'], None
        except (OSError, ValueError, AttributeError) as exc:
            # A stale/missing/invalid tuning request never stops healthy scans
            # or silently raises concurrency above the last accepted budget.
            self.error = type(exc).__name__ + ': ' + str(exc)
        return self.value

    def close(self):
        self.cache.close(timeout=.1)
