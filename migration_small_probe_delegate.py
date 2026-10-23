"""Delegate redundant small-file prechecks to the copy worker, never to proof.

The target probe is an optional work-queue filter, not a copy receipt. After
real missing observations warm a directory, this policy may return a candidate
without a target lookup. The unchanged copy worker still checks the destination
and hashes existing data before skipping, and never overwrites a conflict.

Delegated responses retain ``missing=True`` only for wire compatibility: that
means "send to the worker for copy/verification", NOT confirmed target absence.
They are counted separately and never added to the probe's real missing count.
"""

import collections
import os
import stat
import threading
import time
import weakref

import migration_target_probe as probe_module


SCOPE = 'fresh-small-copy-worker-precheck-v1'
_INTEGER_LIMITS = {'max_file_bytes': (0, 1024 * 1024), 'min_missing': (16, 65536),
                   'recheck_every': (1, 64), 'max_directories': (1, 4096)}
_STAT_INTEGERS = ('size', 'mode', 'ino', 'dev', 'mtime_ns', 'ctime_ns', 'uid', 'gid')


def validate_policy(policy):
    if (not isinstance(policy, dict) or policy.get('scope') != SCOPE or
            set(policy) != {'scope', *_INTEGER_LIMITS}):
        raise ValueError('unsupported small-file probe delegation policy')
    result = {'scope': SCOPE}
    for name, (minimum, maximum) in _INTEGER_LIMITS.items():
        value = policy[name]
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError('invalid small-file probe delegation policy: ' + name)
        result[name] = value
    return result


def _eligible(entry, policy):
    return (entry.get('kind') == 'file' and
            all(type(entry.get(key)) is int for key in _STAT_INTEGERS) and
            stat.S_ISREG(entry['mode']) and
            0 <= entry['size'] <= policy['max_file_bytes'] and
            not entry.get('_refresh_source') and
            (entry.get('known') is None or entry.get('known') == {}))


class _ServiceState:
    def __init__(self, policy):
        self.policy = policy
        self.mutex = threading.RLock()
        self.directories = collections.OrderedDict()
        self.evictable = collections.OrderedDict()
        self.disabled_directories = 0
        self.saturated_disabled = False
        self.actual_missing_samples = 0
        self.actual_existing_samples = 0
        self.failed_samples = 0

    def directory(self, parent, create=False):
        record = self.directories.get(parent)
        if record is not None:
            self.directories.move_to_end(parent)
            if not record['disabled']:
                self.evictable.move_to_end(parent)
            return record
        if not create or self.saturated_disabled:
            return None
        if len(self.directories) >= self.policy['max_directories']:
            if not self.evictable:
                # Remembering every existing observation forever and bounded
                # memory are compatible only by disabling on saturation.
                self.saturated_disabled = True
                return None
            victim, _ = self.evictable.popitem(last=False)
            del self.directories[victim]
        record = {'missing': 0, 'disabled': False, 'cursor': 0}
        self.directories[parent] = record
        self.evictable[parent] = None
        return record

    def observed(self, parent, eligible, result):
        # A malformed/error response is never evidence of absence.
        if not isinstance(result, dict) or result.get('ok') is not True:
            self.failed_samples += 1
            return
        missing = result.get('missing')
        if type(missing) is not bool or result.get('precheck_delegated'):
            self.failed_samples += 1
            return
        if missing:
            self.actual_missing_samples += 1
            if eligible:
                record = self.directory(parent, create=True)
                if record is not None and not record['disabled']:
                    record['missing'] += 1
        else:
            self.actual_existing_samples += 1
            record = self.directory(parent, create=True)
            if record is not None and not record['disabled']:
                record['disabled'] = True
                self.evictable.pop(parent, None)
                self.disabled_directories += 1

    def snapshot(self):
        return {'policy': dict(self.policy),
                'tracked_directories': len(self.directories),
                'disabled_directories': self.disabled_directories,
                'saturated_disabled': self.saturated_disabled,
                'actual_missing_samples': self.actual_missing_samples,
                'actual_existing_samples': self.actual_existing_samples,
                'failed_samples': self.failed_samples,
                'delegated_is_missing_evidence': False,
                'delegated_result_meaning': 'candidate_for_worker_verification'}


class PolicyController:
    """Update eligibility only, retaining every observed-existing directory.

    Reloading never executes in a request thread. The directory budget,
    sampling interval and missing-evidence threshold cannot change online.
    A rejected update leaves the last validated policy and all state intact.
    """

    def __init__(self, policy):
        self.policy = validate_policy(policy)
        self.states = weakref.WeakKeyDictionary()
        self.mutex = threading.Lock()
        self.revision = 0
        self.reload_error = None

    def state_for(self, service):
        with self.mutex:
            state = self.states.get(service)
            if state is None:
                state = _ServiceState(dict(self.policy))
                self.states[service] = state
            return state

    def update(self, policy):
        policy = validate_policy(policy)
        with self.mutex:
            if any(policy[key] != self.policy[key]
                   for key in policy if key != 'max_file_bytes'):
                raise ValueError('only max_file_bytes may be hot updated')
            if policy != self.policy:
                for state in self.states.values():
                    with state.mutex:
                        state.policy = dict(policy)
                self.policy = policy
                self.revision += 1
            self.reload_error = None

    def report_error(self, exc):
        with self.mutex:
            self.reload_error = type(exc).__name__ + ': ' + str(exc)

    def snapshot(self):
        with self.mutex:
            return {'revision': self.revision, 'reload_error': self.reload_error,
                    'hot_fields': ['max_file_bytes']}

    def current_policy(self):
        with self.mutex:
            return dict(self.policy)


class PolicyReloader:
    """Bounded background polling; no policy-file I/O on the probe path."""

    def __init__(self, controller, reader, interval=5.0):
        if not callable(reader) or not 1 <= interval <= 60:
            raise ValueError('policy reader and interval within 1..60s required')
        self.controller, self.reader, self.interval = controller, reader, interval
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name='probe-policy', daemon=True)

    def reload_once(self):
        try:
            self.controller.update(self.reader())
        except (OSError, ValueError, TypeError, KeyError) as exc:
            self.controller.report_error(exc)

    def _run(self):
        while not self.stop.wait(self.interval):
            self.reload_once()

    def start(self):
        self.thread.start()

    def close(self):
        self.stop.set()
        # A delayed shared-filesystem read must not delay service shutdown.
        self.thread.join(timeout=.2)


def installed_controller():
    controller = getattr(probe_module.ProbeService.probe,
                         '_small_probe_delegate_controller', None)
    if controller is None:
        raise RuntimeError('delegation policy is not installed')
    return controller


def _delegated_probe(service, entry):
    """Same slot/cancellation/accounting envelope as ProbeService.probe.

    Called while the directory policy lock is held, so an observed existing
    result disables subsequent delegations before they can return candidates.
    """
    if not service.fs_slots.acquire(blocking=False):
        with service.mutex:
            service.counts['busy'] += 1
        raise probe_module.ProbeError(503, 'metadata slots busy')
    began = time.monotonic()
    with service.mutex:
        service.counts['requests'] += 1
        service.inflight += 1
        service.peak_inflight = max(service.peak_inflight, service.inflight)
    try:
        if service.stopped():
            raise probe_module.ProbeError(503, 'STOP requested')
        candidate = dict(entry)
        candidate.pop('_refresh_source', None)
        if service.stopped():
            raise probe_module.ProbeError(503, 'STOP requested')
        with service.mutex:
            service.counts['delegated_small_files'] += 1
        return {'ok': True, 'missing': True, 'reason': 'missing',
                'entry': candidate, 'precheck_delegated': True}
    except probe_module.ProbeError:
        with service.mutex:
            service.counts['errors'] += 1
        raise
    finally:
        with service.mutex:
            service.inflight -= 1
            service.counts['elapsed_milliseconds'] += int(
                (time.monotonic() - began) * 1000)
        service.fs_slots.release()


def install(policy):
    """Wrap only probe/health; existing source and worker safety stay intact."""
    policy = validate_policy(policy)
    original_probe = probe_module.ProbeService.probe
    original_health = probe_module.ProbeService.health
    installed = getattr(original_probe, '_small_probe_delegate_policy', None)
    if installed is not None:
        if installed != policy:
            raise RuntimeError('cannot change an installed delegation policy')
        return dict(policy)
    controller = PolicyController(policy)

    def wrapped_probe(service, entry):
        rel = probe_module.phase3_relative(entry)
        parent = os.path.dirname(rel)
        state = controller.state_for(service)
        with state.mutex:
            if _eligible(entry, state.policy):
                record = state.directory(parent, create=True)
                if (record is not None and not state.saturated_disabled and
                        not record['disabled'] and
                        record['missing'] >= state.policy['min_missing']):
                    record['cursor'] += 1
                    if record['cursor'] % state.policy['recheck_every']:
                        return _delegated_probe(service, entry)
        try:
            result = original_probe(service, entry)
        except Exception:
            with state.mutex:
                state.failed_samples += 1
            raise
        with state.mutex:
            state.observed(parent, _eligible(entry, state.policy), result)
        return result

    def wrapped_health(service):
        result = original_health(service)
        state = controller.state_for(service)
        with state.mutex:
            result['small_probe_delegate'] = state.snapshot()
        result['small_probe_delegate']['runtime'] = controller.snapshot()
        return result

    wrapped_probe._small_probe_delegate_policy = dict(policy)
    wrapped_probe._small_probe_delegate_controller = controller
    probe_module.ProbeService.probe = wrapped_probe
    probe_module.ProbeService.health = wrapped_health
    return dict(policy)
