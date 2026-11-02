"""Bound, last-good hot tuning for optional flat-batch parent scheduling.

This module never starts workers, signals processes, or deletes target data.
Publishing a request is not an acknowledgement that a controller applied it.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
import uuid


SCOPE = 'flat-parent-per-boot-v1'
HELPERS = ('parent_scheduler.py', 'parent_tuning.py')
INTERVAL = 5


def _limit(value):
    if value is not None and (type(value) is not int or value not in (1, 2)):
        raise ValueError('limit must be None (off), 1 or 2')
    return value


def validate_policy(value):
    if not isinstance(value, dict) or set(value) != {'limit', 'node_boots'}:
        raise ValueError('parent_policy requires exactly limit and node_boots')
    boots = value['node_boots']
    if (not isinstance(boots, dict) or not boots or
            any(not isinstance(n, str) or not n or not isinstance(b, str) or not b
                for n, b in boots.items())):
        raise ValueError('node_boots must be a nonempty node-to-boot mapping')
    return dict(limit=_limit(value['limit']), node_boots=dict(boots))


def _actor(value):
    if (not isinstance(value, dict) or set(value) != {'pid', 'start', 'boot'} or
            type(value['pid']) is not int or value['pid'] <= 0 or
            not isinstance(value['start'], str) or not value['start'].isdigit() or
            not isinstance(value['boot'], str) or not value['boot']):
        raise ValueError('invalid controller identity')
    return dict(value)


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _load(path):
    with Path(path).open() as stream:
        return json.load(stream)


def _identity(pid):
    fields = Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')', 1)[1].split()
    if fields[0] in ('Z', 'X', 'x'):
        raise ValueError('controller has exited')
    return dict(pid=pid, start=fields[19],
                boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def _request(value, run, controller, config_sha256):
    fields = {'version', 'scope', 'run', 'controller', 'config_sha256',
              'generation', 'operation', 'limit'}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('invalid policy request fields')
    if type(value['version']) is not int or value['version'] != 1 or value['scope'] != SCOPE:
        raise ValueError('policy version or scope differs')
    if value['run'] != str(run) or _actor(value['controller']) != controller:
        raise ValueError('policy run or controller differs')
    if value['config_sha256'] != config_sha256:
        raise ValueError('policy frozen config differs')
    if type(value['generation']) is not int or value['generation'] < 1:
        raise ValueError('generation must be a positive integer')
    if not isinstance(value['operation'], str) or not re.fullmatch('[0-9a-f]{32}', value['operation']):
        raise ValueError('operation must be a UUID hex string')
    _limit(value['limit'])
    return value


class ParentPolicyTuner:
    """Poll at most once per five seconds, never on the task-claim path.

    Lowering a quota only affects future claims. Existing leases stay intact.
    Bad/missing/stale requests retain the last accepted limit and expose a
    warning; they do not abort production or replay any in-flight work.
    """
    def __init__(self, run, controller, config_sha256, initial_limit, apply_limit_callback):
        self.run = Path(run).resolve()
        self.controller = _actor(controller)
        if not isinstance(config_sha256, str) or not re.fullmatch('[0-9a-f]{64}', config_sha256):
            raise ValueError('invalid config SHA256')
        self.config_sha256 = config_sha256
        self._limit = _limit(initial_limit)
        self._callback = apply_limit_callback
        self._generation, self._operation, self._error = 0, None, None
        self._next_poll, self._applied_at = 0, None
        self._lock = threading.RLock()

    @property
    def limit(self):
        with self._lock:
            return self._limit

    def snapshot(self):
        with self._lock:
            return dict(scope=SCOPE, limit=self._limit, generation=self._generation,
                        operation=self._operation, applied_at=self._applied_at,
                        error=self._error)

    def poll(self, force=False):
        with self._lock:
            now = time.monotonic()
            if not force and now < self._next_poll:
                return False
            self._next_poll = now + INTERVAL
            request_loaded = False
            try:
                value = _load(self.run/'parent-policy.json')
                request_loaded = True
                value = _request(value, self.run, self.controller, self.config_sha256)
                if _sha(self.run/'config.json') != self.config_sha256:
                    raise ValueError('frozen config changed')
                if value['generation'] == self._generation:
                    if (value['operation'], value['limit']) != (self._operation, self._limit):
                        raise ValueError('generation reused with different operation or limit')
                    self._error = None
                    return False
                if value['generation'] < self._generation:
                    raise ValueError('stale policy generation')
                self._callback(value['limit'])
                self._limit = value['limit']
                self._generation, self._operation = value['generation'], value['operation']
                self._applied_at, self._error = time.time(), None
                return True
            except FileNotFoundError as exc:
                self._error = None if self._generation == 0 and not request_loaded else str(exc)
            except (ValueError, TypeError, KeyError, OSError) as exc:
                self._error = str(exc)
            return False


def _atomic(path, value):
    part = path.with_name(path.name+'.'+uuid.uuid4().hex+'.part')
    replaced = False
    try:
        with part.open('x') as stream:
            json.dump(value, stream, ensure_ascii=True)
            stream.flush()
            os.fsync(stream.fileno())
        part.replace(path)
        replaced = True
    finally:
        if not replaced:
            try:
                part.unlink()
            except OSError:
                pass  # Do not hide the original write/replace failure.


def publish_limit(run, limit, *, expected_target='/share/project',
                  expected_root_inode=69783039777):
    """Submit only to a live, already-enabled and SHA-frozen controller.

    Operator must confirm matching generation/operation in status.json before
    claiming success. Older running engines are intentionally not retrofitted.
    """
    limit = _limit(limit)
    run, target = Path(run).resolve(), Path(expected_target).resolve()
    if run == target or target in run.parents:
        raise ValueError('control directory must be outside deletion target')
    config_sha256 = _sha(run/'config.json')
    cfg = _load(run/'config.json')
    if cfg.get('target') != expected_target or cfg.get('root_inode') != expected_root_inode:
        raise ValueError('target or root inode differs')
    if cfg.get('runtime_join') is not True:
        raise ValueError('runtime-enabled engine required')
    validate_policy(cfg.get('parent_policy'))
    helpers = cfg.get('parent_helpers')
    if not isinstance(helpers, dict) or set(helpers) != set(HELPERS):
        raise ValueError('frozen parent helpers missing')
    for name in HELPERS:
        if _sha(run/name) != helpers[name]:
            raise ValueError('frozen helper changed: '+name)
    if _sha(run/'rm24.py') != cfg.get('sha256'):
        raise ValueError('frozen engine changed')
    actor = _actor(_load(run/'controller.json'))

    def live_status():
        status = _load(run/'status.json')
        updated = status.get('updated')
        if (status.get('state') != 'rm_running' or status.get('errors') or
                type(updated) not in (int, float) or not -5 <= time.time()-updated <= 15 or
                (run/'STOP').exists()):
            raise ValueError('controller is not fresh, healthy and rm_running')
        if _identity(actor['pid']) != actor:
            raise ValueError('controller identity changed')
        return status

    status = live_status()
    generation = time.time_ns()
    previous = run/'parent-policy.json'
    previous_error = None
    if previous.exists():
        try:
            old = _request(_load(previous), run, actor, config_sha256)
            generation = max(generation, old['generation']+1)
        except (ValueError, TypeError, KeyError, FileNotFoundError) as exc:
            # The controller explicitly tolerates malformed/obsolete requests.
            # A verified live operator must still be able to hot-disable the
            # quota without manually removing a corrupt request first.
            previous_error = str(exc)
    acknowledged = status.get('parent_tuning', {}).get('generation', 0)
    if type(acknowledged) is int:
        generation = max(generation, acknowledged+1)
    value = dict(version=1, scope=SCOPE, run=str(run), controller=actor,
                 config_sha256=config_sha256, generation=generation,
                 operation=uuid.uuid4().hex, limit=limit)
    live_status()
    if _sha(run/'config.json') != config_sha256:
        raise ValueError('frozen config changed during submission')
    _atomic(previous, value)
    return dict(submitted=True, applied=False, run=str(run),
                generation=generation, operation=value['operation'], limit=limit,
                replaced_invalid_request=previous_error)
