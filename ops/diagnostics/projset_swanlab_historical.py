#!/usr/bin/env python3
"""Add explicitly historical ETA scenarios to an existing audit reporter.

The original reporter retains its run ID, step history and shared cadence
gate. This wrapper waits for its existing reporter lock before starting.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import types
import uuid


def historical_metrics(scan, monitor, reference, now):
    values = {'estimate20/历史目录情景数据有效': 0, 'estimate20/历史文件情景数据有效': 0}
    if (not isinstance(reference, dict) or reference.get('schema') != 1 or
            reference.get('kind') != 'historical_unique_index_approximation' or
            reference.get('excluded') != 'eai_pwm/datasets/Sekai'):
        return values
    dirs, files = reference.get('directories'), reference.get('files_links')
    if not all(isinstance(x, int) and not isinstance(x, bool) and x > 0 for x in (dirs, files)):
        return values
    values['reference/首轮历史目录路径_万项'] = dirs / 10000
    values['reference/首轮历史文件及链接路径_万项'] = files / 10000
    if (not isinstance(scan, dict) or scan.get('state') != 'running' or
            not isinstance(monitor, dict) or monitor.get('audit_state') != 'running'):
        return values
    rates, sample = monitor.get('rates'), monitor.get('sample')
    if not isinstance(rates, dict) or not isinstance(sample, dict):
        return values
    scan_at, monitor_at = scan.get('updated'), monitor.get('updated')
    sample_at, started, interval = sample.get('audit_updated'), scan.get('started'), rates.get('audit_updated_seconds')
    if not (all(isinstance(x, (int, float)) and math.isfinite(x)
                for x in (scan_at, monitor_at, sample_at, started, interval, now)) and
            -60 <= now - scan_at <= 300 and -60 <= now - monitor_at <= 45 * 60 and
            -60 <= now - sample_at <= 45 * 60 and 600 <= interval <= 45 * 60 and
            started <= sample_at - interval):
        return values
    counts = scan.get('counts', {})
    if not isinstance(counts, dict):
        return values
    ordinary, links = counts.get('files', 0), counts.get('symlinks', 0)
    combined = (ordinary + links if all(isinstance(x, (int, float)) and math.isfinite(x) and x >= 0
                                        for x in (ordinary, links)) else None)
    for current, total, key, label, valid in (
            (scan.get('done_dirs'), dirs, 'audit_directories_per_hour',
             'estimate20/历史目录规模假设_剩余天', 'estimate20/历史目录情景数据有效'),
            (combined, files, 'audit_files_links_per_hour',
             'estimate20/历史文件规模假设_剩余天', 'estimate20/历史文件情景数据有效')):
        rate = rates.get(key)
        if (isinstance(current, (int, float)) and math.isfinite(current) and 0 <= current < total and
                isinstance(rate, (int, float)) and math.isfinite(rate) and rate > 0):
            estimate = (total - current) / rate / 24
            if math.isfinite(estimate):
                values[label], values[valid] = estimate, 1
    return values


def outbox_metrics(monitor, now):
    """Show the last sampled outbox backlog, not a real-time queue count."""
    values = {'monitor/待接入事件探测数据有效': 0}
    if not isinstance(monitor, dict) or not isinstance(now, (int, float)) or not math.isfinite(now):
        return values
    check, updated = monitor.get('outbox_probe'), monitor.get('updated')
    if not isinstance(check, dict):
        return values
    sampled = check.get('at')
    if (not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
                for v in (updated, sampled)) or
            not (-60 <= now - updated <= 45 * 60 and -60 <= now - sampled <= 45 * 60)):
        return values
    values['monitor/待接入事件采样距今_分钟'] = max(0, now - sampled) / 60
    pending, cap, cursor, maximum = (check.get(k) for k in
                                     ('pending_events_capped', 'pending_cap', 'cursor', 'max_id'))
    if (check.get('state') == 'ok' and
            all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in (pending, cursor, maximum)) and
            cap == 10001 and pending <= cap and maximum >= cursor):
        values['repair/上次巡检待接入事件_上限10001'] = pending
        values['monitor/待接入事件探测数据有效'] = 1
    return values


def load_blocking_reporter(work):
    """Load the deployed reporter with a blocking lock and owner observation."""
    code = work / 'code/projset_full_audit.py'
    raw = code.read_bytes()
    manifest = json.loads((work / 'manifest.json').read_text())
    if hashlib.sha256(raw).hexdigest() != manifest.get('code_sha256'):
        raise ValueError('deployed audit code hash mismatch')
    source = raw.decode('utf-8')
    marker = 'fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)'
    if source.count(marker) != 1:
        raise ValueError('reporter lock pattern changed')
    source = source.replace(marker, 'fcntl.flock(lock, fcntl.LOCK_EX); _historical_record_owner(work)', 1)
    module = types.ModuleType('projset_reporting_only')
    module.__file__ = str(code)
    exec(compile(source, str(code), 'exec'), module.__dict__)
    original = module.monitor_values

    def record_owner(owner_work):
        # This is advisory recovery metadata. A failed observation must not
        # prevent the original reporter from continuing to publish metrics.
        try:
            state = Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()
            path = owner_work / 'historical-reporter/owner.json'
            tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
            with tmp.open('x') as stream:
                json.dump({'host': socket.gethostname(), 'pid': os.getpid(), 'start_ticks': int(state[19])},
                          stream, ensure_ascii=True, separators=(',', ':'))
                stream.flush(); os.fsync(stream.fileno())
            os.replace(tmp, path)
        except (OSError, ValueError, IndexError):
            pass

    def augmented(scan, monitor, now, repair=None):
        values = original(scan, monitor, now, repair)
        values.update(outbox_metrics(monitor, now))
        try:
            reference = json.loads((work / 'monitor20/historical-reference.json').read_text())
            values.update(historical_metrics(scan, monitor, reference, now))
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            values.update({'estimate20/历史目录情景数据有效': 0, 'estimate20/历史文件情景数据有效': 0})
        return values

    module.monitor_values = augmented
    original_repair_values = getattr(module, 'repair_values', None)
    if callable(original_repair_values):
        def augmented_repair(state):
            values = original_repair_values(state)
            retries = state.get('outbox_busy_retries') if isinstance(state, dict) else None
            if isinstance(retries, int) and not isinstance(retries, bool) and retries >= 0:
                values['repair/当前主控队列锁重试次数'] = retries
            return values
        module.repair_values = augmented_repair
    module._historical_record_owner = record_owner
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    args = parser.parse_args()
    work = args.work.resolve()
    manifest = json.loads((work / 'historical-reporter/manifest.json').read_text())
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != manifest.get('code_sha256'):
        raise ValueError('historical reporter code hash mismatch')
    return load_blocking_reporter(work).reporter(work)


if __name__ == '__main__':
    raise SystemExit(main())
