#!/usr/bin/env python3
"""Twenty-minute, durable health and throughput monitor for audit + repair.

The monitor checks real processes and mount identities as well as heartbeats.
It may restore a missing guardian or restart a verified stalled controller;
it never replaces target data and never restarts based only on low throughput.
"""
import argparse
import concurrent.futures
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import uuid

RUN = Path('/share-mixed/project/.hcr-copy-projset-20261102T144640Z-4316ae')
INTERVAL = 1200
STALE = 300
LEADER_NODES = ('cpu-master-0', 'cpu-worker-0')


def atomic(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, ensure_ascii=True, separators=(',', ':'))
        stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def summarize(audit, repair, previous, original):
    """Rates and estimates are deliberately separated by their measurement."""
    c = audit.get('counts', {})
    sample = {'audit_updated': audit.get('updated', 0),
              'files_links': c.get('files', 0) + c.get('symlinks', 0),
              'logical_bytes': c.get('file_bytes', 0),
              'entries': c.get('entries', 0), 'done_dirs': audit.get('done_dirs', 0),
              'discovered_dirs': audit.get('discovered_dirs', 0),
              'pending_dirs': audit.get('pending_dirs', 0),
              'active_dirs': audit.get('active_dirs', 0),
              'retry_dirs': audit.get('retry_dirs', 0),
              'repair_updated': repair.get('updated', 0),
              'copied_operations': repair.get('copied_operations', 0),
              'copied_bytes': repair.get('copied_bytes', 0),
              'repair_pending': repair.get('pending', 0),
              'repair_active': repair.get('active', 0),
              'repair_retry': repair.get('retry', 0),
              'audit_nodes_updated': audit.get('updated', 0),
              'repair_nodes_updated': repair.get('updated', 0),
              'audit_nodes': {
                  node: {'files_links': counts.get('files', 0) + counts.get('symlinks', 0),
                         'logical_bytes': counts.get('file_bytes', 0),
                         'entries': counts.get('entries', 0),
                         'done_dirs': counts.get('done_dirs', 0)}
                  for node, counts in audit.get('node_counts', {}).items()},
              'repair_nodes': {
                  node: {'done': counts.get('done', 0),
                         'copied_operations': counts.get('copied_operations', 0),
                         'copied_bytes': counts.get('copied_bytes', 0)}
                  for node, counts in repair.get('nodes', {}).items()}}
    out = {'sample': sample, 'rates': {}, 'node_rates': {}, 'estimates': {},
           'note': 'audit TB is logical file size from metadata, not bytes read; baseline is first-pass cumulative, not a source total'}
    if not previous:
        return out
    for time_key, fields in (
        ('audit_updated', (('files_links', 'audit_files_links_per_hour', 1),
                           ('logical_bytes', 'audit_logical_TB_per_hour', 1e12),
                           ('entries', 'audit_entries_per_hour', 1),
                           ('done_dirs', 'audit_directories_per_hour', 1))),
        ('repair_updated', (('copied_operations', 'repair_operations_per_hour', 1),
                            ('copied_bytes', 'repair_written_TB_per_hour', 1e12)))):
        elapsed = sample[time_key] - previous.get(time_key, 0)
        if elapsed <= 0 or not previous.get(time_key):
            continue
        out['rates'][time_key + '_seconds'] = elapsed
        for key, label, divisor in fields:
            delta = sample[key] - previous.get(key, sample[key])
            if delta >= 0:
                out['rates'][label] = delta * 3600 / elapsed / divisor
    for component, fields in (
        ('audit', (('files_links', 'files_links_per_hour', 1),
                   ('logical_bytes', 'logical_TB_per_hour', 1e12),
                   ('entries', 'entries_per_hour', 1),
                   ('done_dirs', 'directories_per_hour', 1))),
        ('repair', (('done', 'paths_per_hour', 1),
                    ('copied_operations', 'copied_operations_per_hour', 1),
                    ('copied_bytes', 'copied_TB_per_hour', 1e12)))):
        time_key, node_key = component + '_nodes_updated', component + '_nodes'
        elapsed = sample[time_key] - previous.get(time_key, 0)
        prior_nodes = previous.get(node_key, {})
        if elapsed <= 0 or not previous.get(time_key) or not prior_nodes:
            continue
        node_rates = {}
        for node, values in sample[node_key].items():
            if node not in prior_nodes:
                continue
            node_rates[node] = {}
            for key, label, divisor in fields:
                if key not in prior_nodes[node]:
                    continue
                delta = values[key] - prior_nodes[node][key]
                if delta >= 0:
                    node_rates[node][label] = delta * 3600 / elapsed / divisor
        out['node_rates'][component] = {'seconds': elapsed, 'nodes': node_rates}
    baseline = original.get('counts', {})
    file_total = baseline.get('processed', 0)
    byte_total = baseline.get('copied_bytes', 0) + baseline.get('skipped_bytes', 0)
    for current, total, rate_key, prefix, divisor in (
        (sample['files_links'], file_total, 'audit_files_links_per_hour', 'files', 1),
        (sample['logical_bytes'], byte_total, 'audit_logical_TB_per_hour', 'logical_TB', 1e12)):
        rate = out['rates'].get(rate_key, 0)
        # An exhausted historical baseline is not proof the live scan is done.
        if total > current and rate > 0:
            out['estimates'][prefix + '_hours'] = (total - current) / divisor / rate
            out['estimates'][prefix + '_baseline'] = total / divisor
    return out


def historical_scenarios(audit, rates, reference, now):
    """Project historical path counts, never presenting them as source totals."""
    result = {'available': False, 'sample_valid': False,
              'note': 'historical scheduled paths are not a current source inventory; '
                      'forecasts assume a similar scope and an unchanged rate'}
    if (not isinstance(reference, dict) or reference.get('schema') != 1 or
            reference.get('kind') != 'historical_unique_index_approximation' or
            reference.get('excluded') != 'eai_pwm/datasets/Sekai'):
        return result
    directories, files_links = reference.get('directories'), reference.get('files_links')
    if not all(isinstance(x, int) and not isinstance(x, bool) and x > 0 for x in (directories, files_links)):
        return result
    result.update(available=True, directories_reference=directories, files_links_reference=files_links,
                  excluded=reference['excluded'], reference_created=reference.get('created'))
    if not isinstance(audit, dict) or audit.get('state') != 'running' or not isinstance(rates, dict):
        return result
    updated, started, interval = audit.get('updated'), audit.get('started'), rates.get('audit_updated_seconds')
    if not (all(isinstance(x, (int, float)) and math.isfinite(x) for x in (updated, started, interval, now)) and
            -60 <= now - updated <= 300 and INTERVAL / 2 <= interval <= 45 * 60 and
            started <= updated - interval):
        return result
    result['sample_valid'] = True
    counts = audit.get('counts', {})
    if not isinstance(counts, dict):
        return result
    ordinary, links = counts.get('files', 0), counts.get('symlinks', 0)
    combined = (ordinary + links if all(isinstance(x, (int, float)) and math.isfinite(x) and x >= 0
                                        for x in (ordinary, links)) else None)
    for current, total, rate_key, label in (
            (audit.get('done_dirs'), directories, 'audit_directories_per_hour', 'directories_remaining_hours'),
            (combined, files_links, 'audit_files_links_per_hour', 'files_links_remaining_hours')):
        rate = rates.get(rate_key)
        if (isinstance(current, (int, float)) and math.isfinite(current) and 0 <= current < total and
                isinstance(rate, (int, float)) and math.isfinite(rate) and rate > 0):
            result[label] = (total - current) / rate
    return result


def parse_nfs_mountstats(text, roots):
    """Read cumulative per-mount RPC counters without issuing filesystem I/O."""
    wanted = dict(roots)
    result = {}
    current = None
    for line in text.splitlines():
        if line.startswith('device '):
            current = None
            for name, root in wanted.items():
                marker = ' mounted on ' + root + ' with fstype nfs'
                if marker in line:
                    current = {'device': line.split(' mounted on ', 1)[0][7:], 'ops': {}}
                    result[name] = current
                    break
            continue
        if current is None:
            continue
        parts = line.strip().split()
        if not parts:
            continue
        if parts[0] == 'age:' and len(parts) >= 2:
            try:
                current['age_seconds'] = int(parts[1])
            except ValueError:
                pass
        elif parts[0].endswith(':') and parts[0][:-1] in ('GETATTR', 'LOOKUP', 'READDIRPLUS', 'READ', 'WRITE', 'COMMIT') and len(parts) >= 9:
            try:
                values = [int(v) for v in parts[1:9]]
            except ValueError:
                continue
            current['ops'][parts[0][:-1]] = {
                'requests': values[0], 'transmissions': values[1],
                'queue_ms': values[5], 'rtt_ms': values[6], 'execution_ms': values[7]}
    return result


def nfs_rates(current, previous):
    """Compare one node's counters, dropping resets, remounts and empty windows."""
    rates = {}
    if not isinstance(current, dict) or not isinstance(previous, dict):
        return rates
    elapsed = current.get('at', 0) - previous.get('at', 0)
    if elapsed <= 0:
        return rates
    for mount, now in current.get('mounts', {}).items():
        old = previous.get('mounts', {}).get(mount)
        if (not isinstance(old, dict) or now.get('device') != old.get('device') or
                'age_seconds' not in now or 'age_seconds' not in old or
                now['age_seconds'] < old['age_seconds']):
            continue
        for operation, counters in now.get('ops', {}).items():
            prior = old.get('ops', {}).get(operation)
            keys = ('requests', 'transmissions', 'queue_ms', 'rtt_ms', 'execution_ms')
            if not isinstance(prior, dict) or not all(k in counters and k in prior for k in keys):
                continue
            delta = {key: counters[key] - prior[key] for key in keys}
            count = delta['requests']
            if count <= 0 or any(value < 0 for value in delta.values()):
                continue
            rates.setdefault(mount, {})[operation] = {
                'requests': count, 'requests_per_second': count / elapsed,
                'mean_queue_ms': delta['queue_ms'] / count,
                'mean_rtt_ms': delta['rtt_ms'] / count,
                'mean_execution_ms': delta['execution_ms'] / count,
                'extra_transmissions': max(0, delta['transmissions'] - count),
                'seconds': elapsed}
    return rates


def sample_source_cursors(pid, proc_root=Path('/proc')):
    """Sample open source-directory cookies, without opening source data.

    Linux directory positions are opaque NFS cookies: only equality is useful.
    Hash paths before persisting observations, and tolerate descriptor races.
    """
    proc = proc_root / str(pid)
    result = {'pid': pid, 'start_ticks': None, 'complete': False, 'directories': {}}

    def start_ticks():
        return int((proc / 'stat').read_text().rsplit(') ', 1)[1].split()[19])

    try:
        start = start_ticks()
        descriptors = list((proc / 'fd').iterdir())
    except (OSError, ValueError, IndexError):
        return result
    complete = True
    directories = {}
    for descriptor in descriptors:
        try:
            target = os.readlink(descriptor)
        except OSError:
            complete = False
            continue
        if not target.startswith('/share-mixed/projset/'):
            continue
        try:
            fields = dict(line.split(':', 1) for line in
                          (proc / 'fdinfo' / descriptor.name).read_text().splitlines() if ':' in line)
            if not int(fields['flags'].strip(), 8) & os.O_DIRECTORY:
                continue
            position = int(fields['pos'].strip())
            if os.readlink(descriptor) != target:
                complete = False
                continue
        except (OSError, ValueError, KeyError):
            complete = False
            continue
        identity = hashlib.sha256(os.fsencode(target)).hexdigest()
        directories.setdefault(identity, set()).add(position)
    try:
        if start_ticks() != start:
            return result
    except (OSError, ValueError, IndexError):
        return result
    result.update(start_ticks=start, complete=complete,
                  directories={key: sorted(values) for key, values in directories.items()})
    return result


def cursor_observations(current, previous, counts, old_counts):
    """Classify observed activity; lack of evidence is never a hang verdict."""
    observations = {'observed': 0, 'not_observed': 0, 'not_comparable': 0, 'nodes': {}}
    for node, now in current.items():
        old = previous.get(node) if isinstance(previous, dict) else None
        new_count = counts.get(node, {}).get('done_dirs')
        old_count = old_counts.get(node, {}).get('done_dirs') if isinstance(old_counts, dict) else None
        status, reasons = 'not_comparable', []
        if (isinstance(now, dict) and isinstance(old, dict) and
                now.get('pid') is not None and now.get('pid') == old.get('pid') and
                now.get('start_ticks') is not None and now.get('start_ticks') == old.get('start_ticks') and
                isinstance(new_count, int) and isinstance(old_count, int) and new_count >= old_count):
            if new_count > old_count:
                reasons.append('completed_directories')
            new_dirs, old_dirs = now.get('directories'), old.get('directories')
            if (now.get('complete') and old.get('complete') and
                    isinstance(new_dirs, dict) and isinstance(old_dirs, dict)):
                if set(new_dirs) != set(old_dirs):
                    reasons.append('open_directories_changed')
                if any(new_dirs[key] != old_dirs[key] for key in new_dirs.keys() & old_dirs.keys()):
                    reasons.append('directory_cookie_changed')
                if reasons:
                    status = 'observed'
                elif new_dirs and old_dirs:
                    status = 'not_observed'
            elif reasons:
                status = 'observed'
        observations[status] += 1
        observations['nodes'][node] = {'status': status, 'reasons': reasons}
    return observations


def probe(audit, repair, node):
    scripts = {'audit': str(audit / 'code/projset_full_audit.py'),
               'repair': str(repair / 'code/projset_audit_repair.py')}
    works = {'audit': str(audit), 'repair': str(repair)}
    result = {'node': node, 'host': socket.gethostname(), 'roles': {
        x: {'workers': [], 'controllers': [], 'guardians': []} for x in scripts}}
    result['reporter'] = {'wrappers': [], 'guardians': []}
    result['source_ino'] = os.stat('/share-mixed/projset').st_ino
    result['target_ino'] = os.stat('/share-mixed/project').st_ino
    mounts = Path('/proc/self/mountinfo').read_text().splitlines()
    for key, root in (('source', '/share-mixed/projset'), ('target', '/share-mixed/project')):
        rows = [line for line in mounts if line.split()[4] == root]
        result[key + '_mount'] = rows[0].split(' - ', 1)[1].split()[:2] if len(rows) == 1 else None
    try:
        result['nfs'] = {'mounts': parse_nfs_mountstats(Path('/proc/self/mountstats').read_text(),
                         (('source', '/share-mixed/projset'), ('target', '/share-mixed/project'))),
                         'at': time.time()}
    except (OSError, ValueError):
        result['nfs'] = None
    try:
        events = dict(x.split()[:2] for x in Path('/sys/fs/cgroup/memory.events').read_text().splitlines())
        result['memory_events'] = {k: int(events.get(k, 0)) for k in ('oom', 'oom_kill')}
    except (OSError, ValueError):
        result['memory_events'] = None
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = [os.fsdecode(x) for x in (proc / 'cmdline').read_bytes().split(b'\0') if x]
            if not args:
                continue
            pid = int(proc.name)
            fields = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()
            state = fields[0]
            for key, script in scripts.items():
                if script in args and works[key] in args:
                    if 'worker' in args and node in args:
                        result['roles'][key]['workers'].append({'pid': pid, 'state': state})
                    elif 'controller' in args:
                        result['roles'][key]['controllers'].append({'pid': pid, 'state': state})
                if str(Path(works[key]) / 'code/guardian.sh') in args and works[key] in args:
                    result['roles'][key]['guardians'].append({'pid': pid, 'state': state})
            if str(audit / 'historical-reporter/code/projset_swanlab_historical.py') in args and str(audit) in args:
                result['reporter']['wrappers'].append({'pid': pid, 'state': state,
                                                       'start_ticks': int(fields[19])})
            if str(audit / 'historical-reporter/guardian.sh') in args and str(audit) in args:
                result['reporter']['guardians'].append({'pid': pid, 'state': state})
        except (OSError, ValueError, IndexError):
            continue
    workers = result['roles']['audit']['workers']
    result['audit_cursor'] = sample_source_cursors(workers[0]['pid']) if len(workers) == 1 else None
    return result


def stop_stalled(audit, repair, component, pid, observed_updated):
    work = audit if component == 'audit' else repair
    if (work / 'STOP').exists():
        raise ValueError('controller deliberately stopping')
    code = work / ('code/projset_full_audit.py' if component == 'audit' else 'code/projset_audit_repair.py')
    status = json.loads((work / 'status.json').read_text())
    if (status.get('state') != 'running' or status.get('controller_pid') != pid or
            status.get('updated') != observed_updated or time.time() - observed_updated < STALE):
        raise ValueError('heartbeat or active controller changed')
    proc = Path('/proc') / str(pid)
    args = [os.fsdecode(x) for x in (proc / 'cmdline').read_bytes().split(b'\0') if x]
    state = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()[0]
    if str(code) not in args or str(work) not in args or 'controller' not in args or state == 'D':
        raise ValueError('process identity changed or process is in uninterruptible I/O')
    if (work / 'STOP').exists():
        raise ValueError('controller deliberately stopping')
    os.kill(pid, signal.SIGTERM)
    return {'component': component, 'pid': pid, 'signal': 'SIGTERM'}


def reporter_restart_candidate(swanlab, owner, probes, topology, now):
    """Return the exact stalled reporter only when both hosts are verified."""
    if not isinstance(swanlab, dict) or swanlab.get('state') not in ('streaming', 'sending', 'retrying'):
        return None
    updated, enqueued = swanlab.get('updated'), swanlab.get('enqueued_at')
    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0
               for v in (updated, enqueued)) or now - max(updated, enqueued) < STALE:
        return None
    if not isinstance(owner, dict):
        return None
    pid, start = owner.get('pid'), owner.get('start_ticks')
    if not all(isinstance(v, int) and not isinstance(v, bool) and v > 1 for v in (pid, start)):
        return None
    hosts = topology.get('hosts', {})
    node = next((n for n in LEADER_NODES if hosts.get(n) == owner.get('host')), None)
    if node is None:
        return None
    alternate = next(n for n in LEADER_NODES if n != node)
    primary, standby = probes.get(node, {}), probes.get(alternate, {})
    if not (primary.get('identity_ok') and standby.get('identity_ok')):
        return None
    primary_reporter, standby_reporter = primary.get('reporter', {}), standby.get('reporter', {})
    exact = [p for p in primary_reporter.get('wrappers', [])
             if p.get('pid') == pid and p.get('start_ticks') == start and p.get('state') in ('R', 'S')]
    if not (len(exact) == 1 and primary_reporter.get('guardians') and
            standby_reporter.get('guardians') and standby_reporter.get('wrappers')):
        return None
    return {'node': node, 'pid': pid, 'start_ticks': start, 'updated': updated, 'enqueued_at': enqueued}


def stop_stalled_reporter(audit, pid, start_ticks, observed_updated, observed_enqueued):
    """Recheck status and exact process identity on its host before signalling."""
    if (audit / 'historical-reporter/STOP').exists():
        raise ValueError('reporter deliberately stopped')
    state = json.loads((audit / 'swanlab.json').read_text())
    owner = json.loads((audit / 'historical-reporter/owner.json').read_text())
    if (state.get('state') not in ('streaming', 'sending', 'retrying') or
            state.get('updated') != observed_updated or state.get('enqueued_at') != observed_enqueued or
            time.time() - max(observed_updated, observed_enqueued) < STALE or
            owner != {'host': socket.gethostname(), 'pid': pid, 'start_ticks': start_ticks}):
        raise ValueError('reporter state or owner changed')
    proc = Path('/proc') / str(pid)
    args = [os.fsdecode(x) for x in (proc / 'cmdline').read_bytes().split(b'\0') if x]
    fields = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()
    if (str(audit / 'historical-reporter/code/projset_swanlab_historical.py') not in args or
            str(audit) not in args or fields[0] not in ('R', 'S') or int(fields[19]) != start_ticks):
        raise ValueError('reporter identity changed or process is not interruptible')
    if (audit / 'historical-reporter/STOP').exists():
        raise ValueError('reporter deliberately stopped')
    os.kill(pid, signal.SIGTERM)
    return {'component': 'swanlab_reporter', 'pid': pid, 'signal': 'SIGTERM'}


def lease_reference(path):
    """Load a small immutable list of directory IDs and expected attempts."""
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError):
        return {'state': 'invalid_reference'}
    entries = data.get('tracked') if isinstance(data, dict) and data.get('schema') == 1 else None
    if not isinstance(entries, list) or not 1 <= len(entries) <= 64:
        return {'state': 'invalid_reference'}
    result, seen = [], set()
    for entry in entries:
        if not isinstance(entry, dict):
            return {'state': 'invalid_reference'}
        ident, attempt = entry.get('id'), entry.get('attempt')
        if (not isinstance(ident, int) or isinstance(ident, bool) or ident <= 0 or ident in seen or
                not isinstance(attempt, int) or isinstance(attempt, bool) or attempt <= 0):
            return {'state': 'invalid_reference'}
        result.append({'id': ident, 'expected_attempt': attempt}); seen.add(ident)
    return {'state': 'ok', 'tracked': result}


def read_tracked_leases(db, reference):
    """Read only explicitly tracked rows; never emit the underlying paths."""
    if not isinstance(reference, dict) or reference.get('state') != 'ok':
        return reference
    tracked = reference['tracked']
    ids = [item['id'] for item in tracked]
    rows = db.execute('select id,rel,state,node,attempt,updated from dirs where id in (' +
                      ','.join('?' for _ in ids) + ')', ids).fetchall()
    by_id = {row[0]: row for row in rows}
    items = []
    for item in tracked:
        row = by_id.get(item['id'])
        result = dict(item)
        if row is None:
            result['row_missing'] = True
        else:
            _, rel, state, node, attempt, updated = row
            result.update(state=state, node=node, attempt=attempt, updated=updated)
            if state == 1 and attempt == item['expected_attempt']:
                try:
                    rel = json.loads(rel)
                    if isinstance(rel, str) and rel:
                        result['path_digest'] = hashlib.sha256(os.fsencode('/share-mixed/projset/' + rel)).hexdigest()
                except (TypeError, ValueError, UnicodeError):
                    pass
        items.append(result)
    return {'state': 'ok', 'items': items}


def classify_tracked_leases(reading, current, previous, now):
    """Summarize tracked attempts; unchanged cursors do not establish a hang."""
    if not isinstance(reading, dict) or reading.get('state') != 'ok':
        return {'state': reading.get('state', 'unavailable') if isinstance(reading, dict) else 'unavailable'}
    entries = reading.get('items')
    if (not isinstance(entries, list) or not 1 <= len(entries) <= 64 or
            any(not isinstance(item, dict) or not isinstance(item.get('id'), int) or
                not isinstance(item.get('expected_attempt'), int) for item in entries)):
        return {'state': 'invalid_reading'}
    result = {'state': 'ok', 'active_original': 0, 'completed_original': 0,
              'other_attempt_or_state': 0, 'unknown': 0, 'items': [],
              'note': 'NFS cursor changes indicate observed activity, not percentage or proof of a hang'}
    for entry in entries:
        item = {'id': entry['id'], 'expected_attempt': entry['expected_attempt']}
        state, attempt = entry.get('state'), entry.get('attempt')
        if entry.get('row_missing') or state is None or attempt is None:
            classification = 'unknown'
        elif attempt != entry['expected_attempt']:
            classification = 'other_attempt_or_state'
        elif state == 2:
            classification = 'completed_original'
        elif state == 1:
            classification = 'active_original'
            node, updated, key = entry.get('node'), entry.get('updated'), entry.get('path_digest')
            item['node'] = node
            if isinstance(updated, (int, float)) and math.isfinite(updated):
                item['lease_age_seconds'] = max(0, now - updated)
            old = previous.get(node) if isinstance(previous, dict) else None
            new = current.get(node) if isinstance(current, dict) else None
            if (key and isinstance(old, dict) and isinstance(new, dict) and old.get('complete') and new.get('complete') and
                    old.get('pid') is not None and old.get('pid') == new.get('pid') and
                    old.get('start_ticks') is not None and old.get('start_ticks') == new.get('start_ticks')):
                old_dirs, new_dirs = old.get('directories'), new.get('directories')
                if isinstance(old_dirs, dict) and isinstance(new_dirs, dict):
                    before, after = old_dirs.get(key), new_dirs.get(key)
                    if before is not None and after is not None:
                        item['cursor_change_observed'] = before != after
        else:
            classification = 'other_attempt_or_state'
        item['classification'] = classification
        result[classification] += 1
        result['items'].append(item)
    return result


def probe_outbox(audit, cursor, timeout=2):
    """Read a bounded event backlog; an unavailable DB is never an empty one."""
    if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 0:
        raise ValueError('invalid outbox cursor')
    at = time.time()
    try:
        db = sqlite3.connect('file:' + str(audit / 'audit.sqlite3') + '?mode=ro', uri=True, timeout=timeout)
        try:
            maximum, pending = db.execute('select (select coalesce(max(id),0) from repair_outbox), '
                '(select count(*) from (select id from repair_outbox where id>? limit 10001))', (cursor,)).fetchone()
            reference = lease_reference(audit / 'monitor20' / 'long-lease-watch.json')
            if reference is not None:
                try:
                    leases = read_tracked_leases(db, reference)
                except sqlite3.Error as error:
                    leases = {'state': 'unavailable', 'error_type': type(error).__name__}
            else:
                leases = None
        finally:
            db.close()
        result = {'state': 'ok', 'at': at, 'cursor': cursor, 'max_id': maximum,
                  'pending_events_capped': pending, 'pending_cap': 10001}
        if leases is not None:
            result['long_leases'] = leases
        return result
    except sqlite3.OperationalError as error:
        code = getattr(error, 'sqlite_errorcode', None)
        busy = ((isinstance(code, int) and code & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)) or
                (code is None and str(error).lower() in ('database is locked', 'database table is locked',
                                                        'database schema is locked')))
        return {'state': 'busy' if busy else 'error', 'at': at, 'cursor': cursor,
                'error_type': type(error).__name__}


def bounded_outbox_probe(run, work, cursor):
    """Isolate a possibly uninterruptible NFS read from the monitor process."""
    if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 0:
        return {'state': 'invalid_cursor', 'at': time.time()}
    args = [sys.executable, '-S', '-B', str(work / 'code/projset_monitor20.py'),
            'outbox-probe', '--run', str(run), '--work', str(work), '--cursor', str(cursor)]
    try:
        proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, text=True, start_new_session=True)
        try:
            stdout, _ = proc.communicate(timeout=8)
        except subprocess.TimeoutExpired:
            # A killed D-state reader may linger in the kernel, but it cannot
            # block the monitor or mutate either queue once its I/O returns.
            proc.kill()
            return {'state': 'timeout', 'at': time.time(), 'cursor': cursor}
        if proc.returncode != 0:
            return {'state': 'error', 'at': time.time(), 'cursor': cursor}
        value = json.loads(stdout)
        return value if isinstance(value, dict) else {'state': 'error', 'at': time.time(), 'cursor': cursor}
    except (OSError, ValueError):
        return {'state': 'error', 'at': time.time(), 'cursor': cursor}


def outbox_probe_problem(check, previous):
    """Flag a persistent backlog without treating a transient lock as empty."""
    if not isinstance(check, dict):
        return 'outbox_probe_error'
    if check.get('state') in ('error', 'timeout', 'invalid_cursor'):
        return 'outbox_probe_' + check['state']
    if check.get('state') != 'ok':
        return None
    if check.get('max_id', 0) < check.get('cursor', 0):
        return 'outbox_cursor_ahead_of_source'
    if (isinstance(previous, dict) and previous.get('state') == 'ok' and
            previous.get('pending_events_capped', 0) > 0 and check.get('pending_events_capped', 0) > 0 and
            previous.get('cursor') == check.get('cursor')):
        return 'outbox_not_advancing_since_previous_sample'
    return None


CLOUD_PROGRESS_METRICS = {
    'audit/已核对文件及链接': ('files_links', 1),
    'audit/已核对文件_TB': ('file_bytes', 1e12),
    'repair/补传操作累计': ('copied_operations', 1),
    'repair/补传_TB': ('copied_bytes', 1e12),
}
CLOUD_SPEED_METRICS = ('speed20/复核文件及链接_项每小时', 'speed20/复核逻辑容量_TB每小时')


def probe_swanlab_cloud(run_path, api_factory=None):
    """Read heartbeat and progress from the cloud without sending metrics."""
    at = time.time()
    if (not isinstance(run_path, str) or
            not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', run_path)):
        return {'state': 'invalid_run_path', 'at': at}
    try:
        if api_factory is None:
            for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY', 'ALL_PROXY', 'all_proxy'):
                os.environ.pop(key, None)
            secrets = Path('/share-mixed/project/eai_pwm/.secrets')
            proxy = (secrets / 'swanlab_proxy').read_text().strip()
            os.environ.update({key: proxy for key in ('https_proxy', 'http_proxy', 'HTTPS_PROXY', 'HTTP_PROXY')})
            os.environ['SWANLAB_API_KEY'] = (secrets / 'swanlab_api_key').read_text().strip()
            import swanlab
            api_factory = swanlab.Api
        summary = api_factory().run(run_path).summary()
        metric = summary['nodes/在线']
        step, online = metric.get('step'), metric.get('value')
        if (not isinstance(step, int) or isinstance(step, bool) or step < 0 or
                not isinstance(online, (int, float)) or isinstance(online, bool) or not math.isfinite(online)):
            return {'state': 'invalid_response', 'at': time.time()}
        metrics = {}
        for name in tuple(CLOUD_PROGRESS_METRICS) + CLOUD_SPEED_METRICS:
            item = summary.get(name)
            if not isinstance(item, dict):
                metrics[name] = None
                continue
            metric_step, value = item.get('step'), item.get('value')
            if (not isinstance(metric_step, int) or isinstance(metric_step, bool) or metric_step < 0 or
                    not isinstance(value, (int, float)) or isinstance(value, bool) or
                    not math.isfinite(value) or value < 0):
                metrics[name] = None
            else:
                metrics[name] = {'step': metric_step, 'value': value}
        return {'state': 'ok', 'at': time.time(), 'step': step, 'online': online, 'metrics': metrics}
    except Exception as error:
        # Never serialize exception messages: HTTP errors may contain secrets.
        return {'state': 'error', 'at': time.time(), 'error_type': type(error).__name__}


def bounded_cloud_probe(run, work, reporter_python, run_path):
    """Keep slow SDK, proxy and network I/O outside the monitor process."""
    if not isinstance(reporter_python, str) or not isinstance(run_path, str):
        return {'state': 'invalid_configuration', 'at': time.time()}
    args = [reporter_python, '-B', str(work / 'code/projset_monitor20.py'), 'cloud-probe',
            '--run', str(run), '--work', str(work), '--run-path', run_path]
    try:
        proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, text=True, start_new_session=True)
        try:
            stdout, _ = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            return {'state': 'timeout', 'at': time.time()}
        if proc.returncode != 0:
            return {'state': 'error', 'at': time.time()}
        result = json.loads(stdout)
        return result if isinstance(result, dict) else {'state': 'error', 'at': time.time()}
    except (OSError, ValueError):
        return {'state': 'error', 'at': time.time()}


def cloud_probe_problem(probe, local):
    """Alert on missing cloud samples; never kill an SDK that may buffer them."""
    if not isinstance(probe, dict) or probe.get('state') != 'ok':
        return 'swanlab_cloud_probe_unavailable'
    if not isinstance(local, dict):
        return 'swanlab_local_state_unavailable'
    cloud_step, local_step = probe.get('step'), local.get('step')
    if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in (cloud_step, local_step)):
        return 'swanlab_step_unavailable'
    return 'swanlab_cloud_lag_at_least_four_steps' if local_step - cloud_step >= 4 else None


def cloud_progress_problems(probe, local):
    """Check committed progress dimensions without restarting a buffered SDK."""
    if not isinstance(probe, dict) or probe.get('state') != 'ok' or not isinstance(local, dict):
        return []
    sample = local.get('last_sample')
    metrics = probe.get('metrics')
    if not isinstance(sample, dict) or not isinstance(metrics, dict):
        return []
    cloud_step, local_step = probe.get('step'), local.get('step')
    if not all(isinstance(v, int) and not isinstance(v, bool) and v >= 0 for v in (cloud_step, local_step)):
        return []
    problems = []
    for name, (field, divisor) in CLOUD_PROGRESS_METRICS.items():
        expected = sample.get(field)
        if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
            continue
        if name.startswith('repair/') and not sample.get('repair_time'):
            continue
        metric = metrics.get(name)
        if not isinstance(metric, dict):
            problems.append({'component': 'swanlab_reporter', 'problem': 'cloud_progress_metric_unavailable',
                             'metric': name})
            continue
        metric_step, value = metric.get('step'), metric.get('value')
        if (not isinstance(metric_step, int) or isinstance(metric_step, bool) or metric_step < 0 or
                not isinstance(value, (int, float)) or isinstance(value, bool) or
                not math.isfinite(value) or value < 0):
            problems.append({'component': 'swanlab_reporter', 'problem': 'cloud_progress_metric_unavailable',
                             'metric': name})
        elif cloud_step - metric_step >= 4:
            problems.append({'component': 'swanlab_reporter', 'problem': 'cloud_progress_metric_lag',
                             'metric': name, 'step': metric_step, 'heartbeat_step': cloud_step})
        elif (local.get('state') == 'streaming' and metric_step == local_step and
              not math.isclose(value, expected / divisor, rel_tol=1e-12, abs_tol=1e-12)):
            problems.append({'component': 'swanlab_reporter', 'problem': 'cloud_progress_value_mismatch',
                             'metric': name, 'step': metric_step})
    return problems


def inspect_once(run, work, previous):
    audit, repair = work.parent, work.parent / 'repair'
    os.environ['PROJSET_SSH_TOPOLOGY'] = str(run / 'recovery/ssh.json')
    recovery = module('monitor_recovery', run / 'recovery/code/projset_recovery_copy.py')
    topology = json.loads((run / 'recovery/ssh.json').read_text())
    config = json.loads((run / 'config.json').read_text())
    original = json.loads((run / 'status.json').read_text())
    a = json.loads((audit / 'status.json').read_text())
    r = json.loads((repair / 'status.json').read_text())
    result = summarize(a, r, previous, original)
    result.update(updated=time.time(), audit_state=a.get('state'), repair_state=r.get('state'),
                  audit_age_seconds=time.time()-a.get('updated', 0),
                  repair_age_seconds=time.time()-r.get('updated', 0), actions=[], problems=[])
    try:
        reference = json.loads((work / 'historical-reference.json').read_text())
    except (OSError, ValueError):
        reference = None
    result['historical_scenarios'] = historical_scenarios(a, result['rates'], reference, result['updated'])

    def fetch(node):
        command = ('python3 -S -B ' + shlex.quote(str(work / 'code/projset_monitor20.py')) +
                   ' probe --run ' + shlex.quote(str(run)) + ' --work ' + shlex.quote(str(work)) +
                   ' --node ' + shlex.quote(node))
        try:
            p = subprocess.run(recovery.ssh(node, command), capture_output=True, text=True, timeout=15)
            if p.returncode != 0:
                return {'node': node, 'error': 'ssh_or_probe_failed', 'rc': p.returncode}
            actual = json.loads(p.stdout)
            expected = config['nodes'][node]
            actual['identity_ok'] = (actual['host'] == topology['hosts'][node] and
                                     actual['source_ino'] == expected['source_ino'] and
                                     actual['target_ino'] == expected['target_ino'] and
                                     actual['source_mount'] == expected['source_mount'] and
                                     actual['target_mount'] == expected['target_mount'])
            return actual
        except (OSError, subprocess.TimeoutExpired, ValueError, KeyError) as error:
            return {'node': node, 'error': type(error).__name__}

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        probes = list(executor.map(fetch, topology['hosts']))
    by_node = {x['node']: x for x in probes}
    result['probes'] = probes
    result['sample']['nfs_nodes'] = {
        p['node']: p['nfs'] for p in probes if p.get('identity_ok') and p.get('nfs')}
    result['sample']['audit_cursor_nodes'] = {
        p['node']: p.get('audit_cursor') for p in probes if p.get('identity_ok')}
    result['cursor_observations'] = cursor_observations(
        result['sample']['audit_cursor_nodes'],
        previous.get('audit_cursor_nodes', {}) if isinstance(previous, dict) else {},
        result['sample']['audit_nodes'], previous.get('audit_nodes', {}) if isinstance(previous, dict) else {})
    prior_nfs = previous.get('nfs_nodes', {}) if isinstance(previous, dict) else {}
    result['nfs_rates'] = {
        node: rates for node, current in result['sample']['nfs_nodes'].items()
        if (rates := nfs_rates(current, prior_nfs.get(node)))}
    # The repair process may keep retrying while healthy heartbeats continue.
    # An independently bounded, read-only probe makes that situation visible.
    result['outbox_probe'] = bounded_outbox_probe(run, work, r.get('outbox_cursor'))
    result['sample']['outbox_probe'] = result['outbox_probe']
    check = result['outbox_probe']
    if isinstance(check, dict) and 'long_leases' in check:
        result['long_lease_watch'] = classify_tracked_leases(
            check['long_leases'], result['sample']['audit_cursor_nodes'],
            previous.get('audit_cursor_nodes', {}) if isinstance(previous, dict) else {}, result['updated'])
    old_check = previous.get('outbox_probe', {}) if isinstance(previous, dict) else {}
    if problem := outbox_probe_problem(check, old_check):
        result['problems'].append({'component': 'repair', 'problem': problem})
    for p in probes:
        if p.get('error') or not p.get('identity_ok'):
            result['problems'].append({'node': p['node'], 'problem': p.get('error', 'identity_mismatch')})
        elif p.get('memory_events') and p['memory_events'].get('oom_kill', 0):
            result['problems'].append({'node': p['node'], 'problem': 'oom_kill_nonzero',
                                       'count': p['memory_events']['oom_kill']})
        else:
            for component, state in (('audit', a), ('repair', r)):
                if state.get('state') == 'running' and not p['roles'][component]['workers']:
                    result['problems'].append({'node': p['node'], 'problem': component + '_worker_missing'})

    # Restore missing watchdogs only on a directly verified host/mount. An
    # existing controller can continue; the new guardian waits on its lock.
    for component, state, folder in (('audit', a, audit), ('repair', r, repair)):
        if state.get('state') in ('finished', 'stopped') or (folder / 'STOP').exists():
            continue
        for node in LEADER_NODES:
            p = by_node.get(node, {})
            if not p.get('identity_ok') or p['roles'][component]['guardians']:
                continue
            guard = folder / 'code/guardian.sh'
            args = [str(run), str(folder)] + ([str(audit)] if component == 'repair' else [])
            logfile = work / ('guardian-restart-' + component + '-' + node + '.log')
            command = ('nohup /bin/sh ' + shlex.quote(str(guard)) + ' ' +
                       ' '.join(shlex.quote(x) for x in args) + ' >' + shlex.quote(str(logfile)) +
                       ' 2>&1 </dev/null & echo $!')
            try:
                launched = subprocess.run(recovery.ssh(node, command), capture_output=True, text=True, timeout=15)
                result['actions'].append({'action': 'restore_guardian', 'component': component,
                                          'node': node, 'rc': launched.returncode,
                                          'pid': launched.stdout.strip()})
            except (OSError, subprocess.TimeoutExpired) as error:
                result['problems'].append({'node': node, 'problem': 'guardian_restart_' + type(error).__name__})

    # Keep the existing single-writer reporter and its standby alive without
    # restarting either data controller. STOP remains an operator fence.
    reporter_dir = audit / 'historical-reporter'
    if ((reporter_dir / 'guardian.sh').exists() and (reporter_dir / 'manifest.json').exists() and
            not (reporter_dir / 'STOP').exists()):
        try:
            swanlab_state = json.loads((audit / 'swanlab.json').read_text())
        except (OSError, ValueError):
            swanlab_state = {}
            result['problems'].append({'component': 'swanlab_reporter', 'problem': 'status_unavailable'})
        if swanlab_state.get('state') != 'finished':
            result['cloud_probe'] = bounded_cloud_probe(run, work, topology.get('reporter_python'),
                                                        swanlab_state.get('run_path'))
            result['sample']['cloud_probe'] = result['cloud_probe']
            if problem := cloud_probe_problem(result['cloud_probe'], swanlab_state):
                result['problems'].append({'component': 'swanlab_reporter', 'problem': problem})
            result['problems'].extend(cloud_progress_problems(result['cloud_probe'], swanlab_state))
            for node in LEADER_NODES:
                p = by_node.get(node, {})
                if not p.get('identity_ok') or p.get('reporter', {}).get('guardians'):
                    continue
                guard, log = reporter_dir / 'guardian.sh', work / ('guardian-restart-swanlab-' + node + '.log')
                command = ('nohup /bin/sh ' + shlex.quote(str(guard)) + ' ' +
                           shlex.quote(str(audit)) + ' ' + shlex.quote(topology['reporter_python']) +
                           ' >' + shlex.quote(str(log)) + ' 2>&1 </dev/null & echo $!')
                try:
                    launched = subprocess.run(recovery.ssh(node, command), capture_output=True, text=True, timeout=15)
                    result['actions'].append({'action': 'restore_guardian', 'component': 'swanlab_reporter',
                                              'node': node, 'rc': launched.returncode,
                                              'pid': launched.stdout.strip()})
                except (OSError, subprocess.TimeoutExpired) as error:
                    result['problems'].append({'node': node, 'problem': 'swanlab_guardian_' + type(error).__name__})
            updated, enqueued = swanlab_state.get('updated'), swanlab_state.get('enqueued_at')
            stale = (swanlab_state.get('state') in ('streaming', 'sending', 'retrying') and
                     all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0
                         for v in (updated, enqueued)) and time.time() - max(updated, enqueued) >= STALE)
            if stale:
                try:
                    owner = json.loads((reporter_dir / 'owner.json').read_text())
                except (OSError, ValueError):
                    owner = None
                candidate = reporter_restart_candidate(swanlab_state, owner, by_node, topology, time.time())
                if candidate is None:
                    result['problems'].append({'component': 'swanlab_reporter',
                                               'problem': 'stale_without_safe_restart_evidence'})
                else:
                    time.sleep(10)
                    command = ('python3 -S -B ' + shlex.quote(str(work / 'code/projset_monitor20.py')) +
                               ' stop-stalled-reporter --run ' + shlex.quote(str(run)) +
                               ' --work ' + shlex.quote(str(work)) + ' --pid ' + str(candidate['pid']) +
                               ' --observed-start-ticks ' + str(candidate['start_ticks']) +
                               ' --observed-updated ' + repr(candidate['updated']) +
                               ' --observed-enqueued ' + repr(candidate['enqueued_at']))
                    try:
                        stopped = subprocess.run(recovery.ssh(candidate['node'], command),
                                                 capture_output=True, text=True, timeout=15)
                        result['actions'].append({'action': 'restart_stalled_reporter',
                            'node': candidate['node'], 'rc': stopped.returncode,
                            'detail': (stopped.stdout if stopped.returncode == 0 else stopped.stderr).strip()[-300:]})
                        if stopped.returncode:
                            result['problems'].append({'component': 'swanlab_reporter',
                                                       'problem': 'stalled_restart_not_confirmed'})
                    except (OSError, subprocess.TimeoutExpired) as error:
                        result['problems'].append({'component': 'swanlab_reporter',
                                                   'problem': 'stalled_restart_' + type(error).__name__})

    # A stale heartbeat plus the exact live PID and a standby guardian are
    # required; a slow count or a single SSH failure never triggers a restart.
    for component, state in (('audit', a), ('repair', r)):
        folder = audit if component == 'audit' else repair
        if (folder / 'STOP').exists():
            continue
        if state.get('state') != 'running' or time.time()-state.get('updated', 0) < STALE:
            continue
        node = next((n for n, host in topology['hosts'].items() if host == state.get('controller_host')), None)
        alternate = next((n for n in LEADER_NODES if n != node), None)
        p, standby = by_node.get(node, {}), by_node.get(alternate, {})
        pid = state.get('controller_pid')
        exact = [x for x in p.get('roles', {}).get(component, {}).get('controllers', []) if x['pid'] == pid]
        if not (p.get('identity_ok') and standby.get('identity_ok') and
                standby['roles'][component]['guardians'] and exact and exact[0]['state'] != 'D'):
            result['problems'].append({'component': component, 'problem': 'stale_without_safe_restart_evidence'})
            continue
        time.sleep(10)
        command = ('python3 -S -B ' + shlex.quote(str(work / 'code/projset_monitor20.py')) +
                   ' stop-stalled --run ' + shlex.quote(str(run)) + ' --work ' + shlex.quote(str(work)) +
                   ' --component ' + shlex.quote(component) + ' --pid ' + str(pid) +
                   ' --observed-updated ' + repr(state['updated']))
        try:
            p2 = subprocess.run(recovery.ssh(node, command), capture_output=True, text=True, timeout=15)
            result['actions'].append({'action': 'restart_stalled_controller', 'component': component,
                                      'node': node, 'rc': p2.returncode,
                                      'detail': p2.stdout.strip()[-300:] if p2.returncode == 0 else p2.stderr.strip()[-300:]})
        except (OSError, subprocess.TimeoutExpired) as error:
            result['problems'].append({'component': component, 'problem': 'restart_' + type(error).__name__})
    result['state'] = ('finished' if a.get('state') == r.get('state') == 'finished' else
                       'attention' if result['problems'] else 'running')
    return result


def run_monitor(run, work):
    manifest = json.loads((work / 'manifest.json').read_text())
    if hashlib.sha256((work / 'code/projset_monitor20.py').read_bytes()).hexdigest() != manifest['code_sha256']:
        raise ValueError('monitor code changed')
    with (work / 'monitor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        while True:
            try:
                old = json.loads((work / 'status.json').read_text())
            except (OSError, ValueError):
                old = {}
            if old.get('state') == 'finished':
                return 0
            next_check = old.get('updated', 0) + INTERVAL
            while time.time() < next_check:
                if (work / 'STOP').exists():
                    return 0
                time.sleep(min(30, max(0, next_check-time.time())))
            try:
                result = inspect_once(run, work, old.get('sample'))
                rates, previous_rates = result.get('rates', {}), old.get('rates', {})
                criteria = ('audit_files_links_per_hour', 'audit_logical_TB_per_hour',
                            'audit_directories_per_hour')
                if (result.get('audit_state') == 'running' and
                    all(k in rates and previous_rates.get(k, 0) > 0 and
                        rates[k] < .25 * previous_rates[k] for k in criteria)):
                    result['problems'].append({'component': 'audit',
                        'problem': 'three_throughput_dimensions_below_25_percent_of_prior_interval'})
                    result['state'] = 'attention'
                result.update(interval_seconds=INTERVAL, monitor_host=socket.gethostname(), monitor_pid=os.getpid())
                atomic(work / 'status.json', result)
                with (work / 'history.jsonl').open('a') as stream:
                    stream.write(json.dumps(result, ensure_ascii=True, separators=(',', ':')) + '\n')
                    stream.flush(); os.fsync(stream.fileno())
                if result['state'] == 'finished':
                    return 0
            except Exception as error:
                atomic(work / 'error.json', {'updated': time.time(), 'error': type(error).__name__ + ': ' + str(error)})
                time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('monitor', 'probe', 'outbox-probe', 'cloud-probe',
                                         'stop-stalled', 'stop-stalled-reporter'))
    parser.add_argument('--run', type=Path, default=RUN)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--node')
    parser.add_argument('--component', choices=('audit', 'repair'))
    parser.add_argument('--pid', type=int)
    parser.add_argument('--observed-updated', type=float)
    parser.add_argument('--observed-enqueued', type=float)
    parser.add_argument('--observed-start-ticks', type=int)
    parser.add_argument('--cursor', type=int)
    parser.add_argument('--run-path')
    args = parser.parse_args()
    audit = args.work.parent
    repair = audit / 'repair'
    if args.mode == 'probe':
        print(json.dumps(probe(audit, repair, args.node), ensure_ascii=True))
    elif args.mode == 'outbox-probe':
        print(json.dumps(probe_outbox(audit, args.cursor), ensure_ascii=True))
    elif args.mode == 'cloud-probe':
        print(json.dumps(probe_swanlab_cloud(args.run_path), ensure_ascii=True))
    elif args.mode == 'stop-stalled':
        print(json.dumps(stop_stalled(audit, repair, args.component, args.pid, args.observed_updated)))
    elif args.mode == 'stop-stalled-reporter':
        print(json.dumps(stop_stalled_reporter(audit, args.pid, args.observed_start_ticks,
                                               args.observed_updated, args.observed_enqueued)))
    else:
        return run_monitor(args.run.resolve(), args.work.resolve())
    return 0


if __name__ == '__main__':
    sys.exit(main())
