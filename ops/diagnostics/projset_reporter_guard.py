#!/usr/bin/env python3
"""Guard a resumed migration reporter's counters and shared upload cadence.

This module is not activated by the running reporter.  A caller must obtain a
fresh summary for the exact run while this guard owns its reporter lock, and
must separately verify any controller/status handoff before publishing it.
The locks are advisory and depend on the shared filesystem's lock semantics.
"""
import fcntl
import json
import math
import os
from pathlib import Path
import time
import uuid


INTEGER_METRICS = (
    'audit/已核对文件及链接', 'audit/已核对项', 'queue/已完成目录',
    'queue/已发现目录', 'repair/已接入路径', 'repair/已处理路径',
    'repair/补传操作累计',
)
REAL_METRICS = ('audit/已核对文件_TB', 'repair/补传_TB')
PROGRESS_METRICS = INTEGER_METRICS + REAL_METRICS
HEARTBEAT_METRIC = 'nodes/在线'


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _real(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def _values(data):
    if not isinstance(data, dict):
        raise ValueError('invalid metric mapping')
    result = {}
    for key in PROGRESS_METRICS:
        value = data.get(key)
        if not (_integer(value) if key in INTEGER_METRICS else _real(value)):
            raise ValueError('missing or invalid progress metric')
        result[key] = value
    return result


def _sample_matches(sample, values):
    if not isinstance(sample, dict):
        raise ValueError('invalid rate sample')
    for key in ('scan_time', 'repair_time'):
        if not _real(sample.get(key)) or sample[key] == 0:
            raise ValueError('invalid rate timestamp')
    for key in ('files_links', 'file_bytes', 'copied_operations', 'copied_bytes'):
        if not _integer(sample.get(key)):
            raise ValueError('invalid rate counter')
    pairs = ((sample['files_links'], values['audit/已核对文件及链接']),
             (sample['file_bytes'] / 1e12, values['audit/已核对文件_TB']),
             (sample['copied_operations'], values['repair/补传操作累计']),
             (sample['copied_bytes'] / 1e12, values['repair/补传_TB']))
    if any(left != right for left, right in pairs):
        raise ValueError('rate sample and progress metrics differ')


def cloud_anchor(summary):
    """Extract all required per-metric high waters and the greatest run step."""
    if not isinstance(summary, dict):
        raise ValueError('invalid cloud summary')
    values = {}
    for key in PROGRESS_METRICS + (HEARTBEAT_METRIC,):
        item = summary.get(key)
        if not isinstance(item, dict) or not _integer(item.get('step')):
            raise ValueError('cloud metric or step unavailable')
        values[key] = item.get('value')
    if not _real(values.pop(HEARTBEAT_METRIC)):
        raise ValueError('invalid cloud heartbeat')
    values = _values(values)
    steps = [item['step'] for item in summary.values()
             if isinstance(item, dict) and _integer(item.get('step'))]
    if not steps:
        raise ValueError('cloud step unavailable')
    return {'step': max(steps), 'watermark': values}


def _read(path):
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError('state is not a mapping')
    return value


def _atomic(path, value):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('x') as stream:
            json.dump(value, stream, ensure_ascii=True, separators=(',', ':'))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


class ReporterGuard:
    """Hold the existing reporter lock and reserve before invoking a sender.

    Cloud anchoring, state reservation, and the common cadence gate are
    fail-closed.  A sender exception leaves its step and high water reserved.
    The caller must not send through any other path while using this guard.
    """
    def __init__(self, work, run_path, clock=time.time, interval=120):
        self.work = Path(work)
        self.run_path = run_path
        if (not isinstance(run_path, str) or len(run_path.split('/')) != 3 or
                any(not x or x in ('.', '..') for x in run_path.split('/')) or
                not _integer(interval) or interval < 120):
            raise ValueError('invalid run path or cadence')
        self.clock = clock
        self.interval = interval
        self.state_path = self.work / 'swanlab.json'
        self.cadence_path = self.work.parent / 'attention-swanlab-cadence.json'
        self.reporter_lock_path = self.work / 'reporter.lock'
        self.cadence_lock_path = self.work.parent / 'attention-swanlab-cadence.lock'
        self.lock = None
        self.anchor = None
        self.recovery_not_before = 0

    def __enter__(self):
        self.lock = self.reporter_lock_path.open('a+b')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.lock.close()
            self.lock = None
            raise
        return self

    def __exit__(self, *_):
        if self.lock is not None:
            self.lock.close()
            self.lock = None

    def _held(self):
        if self.lock is None or self.lock.closed:
            raise RuntimeError('reporter lock is not held')

    def _state(self):
        state = _read(self.state_path)
        if (state.get('run_path') != self.run_path or
                state.get('run_id') != self.run_path.split('/')[-1] or
                not _integer(state.get('step')) or not _real(state.get('enqueued_at')) or
                state['enqueued_at'] == 0 or not _integer(state.get('interval_seconds')) or
                state['interval_seconds'] < 120):
            raise ValueError('local reporter identity or cadence is invalid')
        return state

    def anchor_cloud(self, fetch_summary):
        """Read this run's cloud summary only after acquiring the reporter lock."""
        self._held()
        if not callable(fetch_summary):
            raise ValueError('cloud fetcher is required')
        state = self._state()
        cloud = cloud_anchor(fetch_summary(self.run_path))
        watermark = cloud['watermark']
        guard = state.get('progress_guard')
        if guard is not None:
            if not isinstance(guard, dict) or guard.get('schema') != 1:
                raise ValueError('invalid persisted progress guard')
            local = _values(guard.get('watermark'))
            watermark = {k: max(watermark[k], local[k]) for k in PROGRESS_METRICS}
        sample = state.get('last_sample')
        if isinstance(sample, dict):
            local_keys = {'files_links': ('audit/已核对文件及链接', 1),
                          'file_bytes': ('audit/已核对文件_TB', 1e12),
                          'copied_operations': ('repair/补传操作累计', 1),
                          'copied_bytes': ('repair/补传_TB', 1e12)}
            for source, (metric, divisor) in local_keys.items():
                value = sample.get(source)
                if value is None:
                    continue
                if not _integer(value):
                    raise ValueError('invalid local progress sample')
                watermark[metric] = max(watermark[metric], value / divisor if divisor != 1 else value)
        now = self.clock()
        if not _real(now):
            raise ValueError('invalid clock')
        interval = max(self.interval, state['interval_seconds'])
        self.recovery_not_before = now + interval if state.get('state') == 'sending' else 0
        self.anchor = {'step': max(state['step'], cloud['step']), 'watermark': watermark}
        return {'step': self.anchor['step'], 'watermark': dict(watermark)}

    def publish(self, metrics, sample, sender):
        """Reserve cadence and a monotonic step, then call sender(metrics, step)."""
        self._held()
        if self.anchor is None:
            raise RuntimeError('cloud anchor is required')
        if not isinstance(metrics, dict) or not isinstance(sample, dict):
            raise ValueError('invalid metrics or sample')
        metrics, sample = dict(metrics), dict(sample)
        current = _values(metrics)
        _sample_matches(sample, current)
        if not callable(sender):
            raise ValueError('invalid sender')
        with self.cadence_lock_path.open('a+b') as gate:
            fcntl.flock(gate, fcntl.LOCK_EX)
            state = self._state()
            cadence = _read(self.cadence_path)
            last = cadence.get('enqueued_at')
            prior_interval = cadence.get('interval_seconds')
            now = self.clock()
            if (not _real(last) or last == 0 or not _real(now) or last > now + 60 or
                    state['enqueued_at'] > now + 60 or not _integer(prior_interval) or
                    prior_interval < 120):
                raise ValueError('shared cadence is unknown or in the future')
            interval = max(self.interval, state['interval_seconds'], prior_interval)
            if now < max(last + interval, state['enqueued_at'] + interval,
                         self.recovery_not_before):
                return {'sent': False, 'reason': 'cadence'}
            last_sample = state.get('last_sample')
            if isinstance(last_sample, dict):
                for field in ('scan_time', 'repair_time'):
                    prior = last_sample.get(field)
                    if prior is not None and (not _real(prior) or sample[field] < prior):
                        return {'sent': False, 'reason': 'sample_time_regressed'}
            previous = state.get('progress_guard')
            watermark = dict(self.anchor['watermark'])
            if previous is not None:
                if not isinstance(previous, dict) or previous.get('schema') != 1:
                    raise ValueError('invalid persisted progress guard')
                old = _values(previous.get('watermark'))
                watermark = {k: max(watermark[k], old[k]) for k in PROGRESS_METRICS}
            if any(current[k] < watermark[k] for k in PROGRESS_METRICS):
                return {'sent': False, 'reason': 'progress_regressed'}
            step = max(state['step'], self.anchor['step']) + 1
            # Reserve the common cadence first. If the next write fails there
            # is a conservative delay, but no upload with an unreserved step.
            _atomic(self.cadence_path, {'enqueued_at': now, 'interval_seconds': interval})
            state.update(state='sending', step=step, enqueued_at=now,
                         interval_seconds=interval,
                         progress_guard={'schema': 1, 'watermark': current})
            _atomic(self.state_path, state)
            self.anchor = {'step': step, 'watermark': current}
            try:
                sender(metrics, step)
            finally:
                finished = self.clock()
                if not _real(finished) or finished < now:
                    raise ValueError('clock regressed during send')
                _atomic(self.cadence_path, {'enqueued_at': finished, 'interval_seconds': interval})
            state.update(state='streaming', updated=finished, last_sample=sample)
            _atomic(self.state_path, state)
            self.recovery_not_before = 0
            return {'sent': True, 'step': step}
