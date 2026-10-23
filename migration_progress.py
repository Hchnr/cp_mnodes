#!/usr/bin/env python3
"""Lightweight migration monitor; reads only small status JSON files.

Aggregate counters in progress.json must count unique paths, not repeated sync
rounds. Supported aggregate schema::

  {"inventory_complete": false,
   "phases": {"1": {"discovered_files": 10, "discovered_bytes": 1024,
                       "verified_files": 8, "verified_bytes": 800,
                       "inventory_complete": true}},
   "current_scan": {"phase": 1, "stage": "p1-r1", "files": 10, "bytes": 1024},
   "totals": {"discovered_files": 10, "discovered_bytes": 1024,
              "verified_files": 8, "verified_bytes": 800}}

`totals` is optional; otherwise present per-phase counters are summed once per
phase. Node verified_* counters are displayed separately because they may count
the same path again in catch-up rounds. No queues or manifests are scanned.
"""

import argparse
import collections
import datetime as dt
import json
import pathlib
import re
import sys
import time
import unicodedata
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from migration_nodes import load_nodes

try:
    from migration_io import read_json as immutable_read_json
except ImportError:
    immutable_read_json = None


DEFAULT_POINTER = pathlib.Path('/share-mixed/project/.hcr-copy-monitor/active.json')
FIELDS = ('discovered_files', 'discovered_bytes', 'verified_files', 'verified_bytes')
PHASE_NAMES = {'1': 'home/hcr', '2': 'repos/cosmos-framework-pwm', '3': '其余 eai_pwm',
               '4': '/share/project → /share-mixed/project'}
STATUS_NAMES = {'scanning': '扫描并复制', 'copying': '复制', 'finalizing': '整理元数据',
                'producer_paused': '生产端故障，已停止供给',
                'preparing_resume': '恢复检查点', 'starting': '启动', 'phase_complete': '阶段完成',
                'waiting_resume_index': '等待后台恢复索引',
                'complete': '完成', 'complete_with_changes': '完成（有待处理变化）',
                'paused': '已暂停', 'paused_training': '训练保护暂停',
                'training_paused': '训练保护暂停',
                'payload_round_complete': '本轮文件完成', 'waiting_hardlinks': '等待硬链接依赖',
                'stopped': '已停止', 'failed': '失败', 'unknown': '未知'}
try:
    SHANGHAI = ZoneInfo('Asia/Shanghai')
except ZoneInfoNotFoundError:
    SHANGHAI = dt.timezone(dt.timedelta(hours=8), 'Asia/Shanghai')


def read_json(path, warnings, required=False):
    try:
        if immutable_read_json is not None:
            value = immutable_read_json(path)
        else:
            with open(path, encoding='utf-8') as f:
                value = json.load(f)
        if not isinstance(value, dict):
            raise ValueError('JSON root must be an object')
        return value
    except (OSError, ValueError) as exc:
        if required or not isinstance(exc, FileNotFoundError):
            warnings.append(str(path) + ': ' + str(exc))
        return {}


def number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0 else None


def age(timestamp, wall_time):
    try:
        if isinstance(timestamp, (int, float)):
            epoch = timestamp
        else:
            parsed = dt.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            epoch = parsed.timestamp()
        return max(0, wall_time - epoch)
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def aggregates(progress):
    phases = progress.get('phases', {})
    if not isinstance(phases, dict):
        phases = {}
    supplied = progress.get('totals', {})
    if not isinstance(supplied, dict):
        supplied = {}
    totals = {}
    for key in FIELDS:
        direct = number(supplied.get(key))
        if direct is None:
            direct = number(progress.get(key))
        if direct is None:
            values = [number(p.get(key)) for p in phases.values() if isinstance(p, dict)]
            present = [v for v in values if v is not None]
            direct = sum(present) if present else None
        totals[key] = direct
    # Merely finishing one phase does not establish the whole migration's size.
    complete = progress.get('inventory_complete') is True or supplied.get('inventory_complete') is True
    if not complete:
        complete = all(isinstance(phases.get(str(p)), dict)
                       and phases[str(p)].get('inventory_complete') is True for p in (1, 2, 3))
    if progress.get('inventory_exact') is False:
        complete = False
    totals['inventory_complete'] = complete
    total, done = totals['discovered_bytes'], totals['verified_bytes']
    totals['percent'] = 100 * done / total if complete and total and done is not None and done <= total else None
    return totals


def pending_extension(plan):
    if not isinstance(plan, dict):
        return None
    if (str(plan.get('priority')) == '4' and plan.get('status') in ('queued', 'running')
            and str(plan.get('source', '')).rstrip('/') == '/share/project'
            and str(plan.get('destination', '')).rstrip('/') == '/share-mixed/project'):
        return plan
    return None


def phases_with_extension(phases, plan):
    result = dict(phases)
    extension = pending_extension(plan)
    if extension:
        result.setdefault('4', {'status': extension['status'],
                                'source': extension['source'],
                                'destination': extension['destination'],
                                'exclude': extension.get('exclude', ['eai_pwm']),
                                'inventory_complete': False, 'planned': True})
    return result


def publication_probe_baseline(root, config, state, warnings):
    """Read this producer handover's frozen counters, never mutate live rates.

    Only the pipelined entrypoint resets these process-local probe counters.
    Its immutable handover bundle provides the preceding observation count;
    this is not a unique-file ledger and must not enter endpoint error rates.
    """
    publication = state.get('task_publication')
    if not isinstance(publication, dict) or publication.get('scope') != 'warm-recovery-only':
        return None
    result = {'complete': False, 'counts': {}, 'operation': None}
    try:
        control = root.resolve()
        supplied = state.get('foreground_handover')
        if not isinstance(supplied, str) or not pathlib.Path(supplied).is_absolute():
            raise ValueError('缺少有效的 foreground_handover 路径')
        handover_path = pathlib.Path(supplied).resolve()
        bundle = handover_path.parent
        matched = re.fullmatch(r'pipelined-producer-([0-9a-f]{32})', bundle.name)
        if (handover_path.name != 'handover.json' or bundle.parent != control
                or matched is None):
            raise ValueError('交接路径不是当前 control 内的发布池交接目录')
        operation = matched.group(1)
        result['operation'] = operation
        checkpoint_path = bundle / 'frozen-checkpoint.json'
        # Also reject symlink escapes for the canonical files and immutable
        # snapshot directories before asking migration_io to read a version.
        for path in (handover_path, checkpoint_path):
            for candidate in (path, path.parent / '.snapshots' / path.name):
                if not candidate.resolve().is_relative_to(control):
                    raise ValueError('交接或检查点路径逃逸到当前 control 外')
        local_warnings = []
        handover = read_json(handover_path, local_warnings, required=True)
        checkpoint = read_json(checkpoint_path, local_warnings, required=True)
        if local_warnings:
            raise ValueError('; '.join(local_warnings))
        if (handover.get('control') != str(control)
                or handover.get('stage') != 'p3-r3'
                or handover.get('operation') != operation
                or checkpoint.get('version') != 1
                or checkpoint.get('status') != 'frozen_checkpoint'
                or checkpoint.get('control') != str(control)
                or checkpoint.get('bundle') != str(bundle)
                or checkpoint.get('operation') != operation
                or checkpoint.get('handover') != handover):
            raise ValueError('交接/冻结检查点的 control、operation 或 bundle 证据不一致')
        frozen = checkpoint.get('frozen_state')
        if not isinstance(frozen, dict) or frozen.get('stage') != 'p3-r3' or frozen.get('recovery_warm') is not True:
            raise ValueError('冻结检查点不是当前恢复队列')
        if frozen.get('run_id') is not None and frozen.get('run_id') != config.get('run_id'):
            raise ValueError('冻结检查点的 run_id 不匹配')
        counts = frozen.get('distributed_target_probe', {}).get('counts', {})
        existing = counts.get('remote_existing') if isinstance(counts, dict) else None
        if type(existing) is not int or existing < 0:
            raise ValueError('冻结检查点缺少有效的 remote_existing 历史命中次数')
        result.update(complete=True, counts={'remote_existing': existing}, checkpoint=str(checkpoint_path))
    except (OSError, ValueError, TypeError, AttributeError, RuntimeError) as exc:
        result['error'] = str(exc)
        warnings.append('历史目标存在检查基线不可用，累计命中仅显示已知下限：' + str(exc))
    return result


def snapshot(control=None, pointer=DEFAULT_POINTER):
    warnings = []
    if control is None:
        active = read_json(pointer, warnings, required=True)
        control = active.get('control')
    if not isinstance(control, (str, pathlib.Path)) or not str(control):
        return {'control': None, 'warnings': warnings or ['未找到当前复制任务'], 'nodes': [],
                'overall': aggregates({}), 'new_copied_bytes': 0, 'seed_progress': {}}
    root = pathlib.Path(control)
    config = read_json(root / 'config.json', warnings, required=True)
    state = read_json(root / 'state.json', warnings, required=True)
    probe_baseline = publication_probe_baseline(root, config, state, warnings)
    seed_progress = read_json(root / 'seed-progress.json', warnings)
    if not seed_progress and isinstance(state.get('seed_progress'), dict):
        seed_progress = dict(state['seed_progress'])
        for key, value in (('run_id', state.get('run_id')), ('started', state.get('started')),
                           ('updated', state.get('updated')), ('resume_from', state.get('resumed_from'))):
            if value is not None:
                seed_progress.setdefault(key, value)
    progress = read_json(root / 'progress.json', warnings)
    inventory = read_json(root / 'inventory.json', warnings)
    baseline = read_json(root / 'inventory-baseline.json', warnings)
    metrics = read_json(root / 'metrics.json', warnings)
    recovery_baseline = read_json(root / 'recovery-baseline.json', warnings)
    if recovery_baseline and (recovery_baseline.get('control') != str(root)
            or recovery_baseline.get('run_id') != config.get('run_id')
            or recovery_baseline.get('resume_from') != config.get('resume_from')):
        warnings.append('恢复累计基线不属于当前运行，拒绝合并计数')
        recovery_baseline = {'invalid': True}
    extension_plan = read_json(root / 'extension-plan.json', warnings)
    progress = dict(progress)
    phases = {str(k): dict(v) for k, v in progress.get('phases', {}).items()
              if isinstance(v, dict)}
    recovery_warm = state.get('recovery_warm') is True
    # Warm receipts occupy p3-r3 solely as a recovery queue. The live producer
    # may publish them into phase3 counters before formal rounds start. Retain
    # the actual queue counters separately, without treating them as a formal
    # round or adding them again to the priorities' confirmed copies.
    warm_phase = dict(phases.get('3', {})) if recovery_warm else {}
    formal_p3 = state.get('phases', {}).get('3', {})
    formal_p3_rounds = formal_p3.get('rounds', []) if isinstance(formal_p3, dict) else []
    formal_p3_confirmed = any(isinstance(record, dict) and
                              isinstance(record.get('summary'), dict) and
                              number(record['summary'].get('verified_files')) is not None
                              for record in formal_p3_rounds)
    exclude_warm_phase = (recovery_warm or ('warm_summary' in state and not formal_p3_confirmed))
    if exclude_warm_phase and '3' in phases:
        phases['3'].update(verified_files=0, verified_bytes=0)
    producer_complete = {p for p, value in phases.items()
                         if value.get('inventory_complete') is True}
    for phase_id, phase in phases.items():
        # A separate, slower inventory must not make a completed copy walk
        # appear never to have traversed this tree. Keep both observations.
        phase['copy_scan_complete'] = phase_id in producer_complete
    for key, inv in inventory.get('phases', {}).items():
        if not isinstance(inv, dict):
            continue
        phase = phases.setdefault(str(key), {})
        phase['independent_inventory'] = {field: inv[field] for field in
                                          ('complete', 'exact', 'completed_units', 'unit_count', 'errors')
                                          if field in inv}
        exact_complete = ((inv.get('inventory_complete') is True or inv.get('complete') is True)
                          and inv.get('exact') is not False and not (number(inv.get('errors')) or 0))
        for field, aliases in (('discovered_files', ('discovered_files', 'files')),
                               ('discovered_bytes', ('discovered_bytes', 'bytes', 'logical_bytes'))):
            values = [number(inv.get(alias)) for alias in aliases]
            if not exact_complete:
                values.append(number(phase.get(field)))
            values = [v for v in values if v is not None]
            if values:
                phase[field] = max(values)
        if (inv.get('inventory_complete') is True or inv.get('complete') is True) and inv.get('exact') is not False:
            phase['inventory_complete'] = True
        if inv.get('exact') is False or (number(inv.get('errors')) or 0) > 0:
            phase['inventory_complete'] = False
        phase['inventory_updated'] = inv.get('updated')
    baseline_phases = baseline.get('phases', {})
    for key, old_phase in baseline_phases.items():
        if not isinstance(old_phase, dict):
            continue
        phase = phases.setdefault(str(key), {})
        if str(key) in producer_complete or phase.get('inventory_complete') is True:
            continue
        for field in ('discovered_files', 'discovered_bytes'):
            prior = number(old_phase.get(field))
            if prior is not None:
                phase[field] = max(number(phase.get(field)) or 0, prior)
                phase['historical_inventory_lower_bound'] = True
    progress['phases'] = phases
    if (inventory.get('inventory_complete') is True or inventory.get('complete') is True) and inventory.get('exact') is not False:
        progress['inventory_complete'] = True
    elif inventory.get('exact') is False:
        progress['inventory_complete'] = False
        progress['inventory_exact'] = False
    supplied_totals = dict(progress.get('totals', {}))
    if exclude_warm_phase:
        for field in ('verified_files', 'verified_bytes'):
            supplied_totals[field] = sum(number(p.get(field)) or 0 for p in phases.values())
    for field in ('discovered_files', 'discovered_bytes'):
        values = [number(p.get(field)) for p in phases.values()]
        values = [v for v in values if v is not None]
        if values:
            supplied_totals[field] = sum(values)
    progress['totals'] = supplied_totals
    training_health = read_json(root / 'training-health.json', warnings)
    broker = read_json(root / 'broker.json', warnings)
    wall_time = time.time()
    stage_name = state.get('stage', progress.get('stage'))
    nodes = []
    for value in load_nodes(root, warnings):
        active = value.get('active', {})
        nodes.append({'host': value.get('host'),
                      'threads': value.get('threads', config.get('threads')),
                      'active_threads': len(active) if isinstance(active, (dict, list)) else None,
                      'counts': value.get('counts', {}), 'bytes': number(value.get('bytes')),
                      'streamed_bytes': number(value.get('streamed_bytes')),
                      'verified_files': number(value.get('verified_files')),
                      'verified_bytes': number(value.get('verified_bytes')),
                      'stage': value.get('stages', {}).get(stage_name, {})
                               if isinstance(value.get('stages'), dict) else {},
                      'heartbeat_age_seconds': age(value.get('updated'), wall_time),
                      'ended': value.get('ended'), 'last_error': value.get('last_error'),
                      'components': value.get('components', [])})
    overall = aggregates(progress)
    background = []
    for job in state.get('background_metadata', []):
        if not isinstance(job, dict):
            continue
        item = dict(job)
        if item.get('kind') == 'directories' and isinstance(item.get('stage'), str):
            stage = item['stage']
            for label in (stage, stage + '-manifest'):
                metadata = read_json(root / 'metadata' / label / 'progress.json', warnings)
                if metadata:
                    item['progress'] = {key: metadata[key] for key in
                                        ('status', 'directories', 'total_directories', 'created_checked',
                                         'threads', 'errors', 'updated', 'stopped') if key in metadata}
                    break
        background.append(item)
    shared_progress = read_json(root / 'metadata' / 'shared-ancestors' / 'progress.json', warnings)
    if shared_progress:
        background.append({'kind': 'directories', 'stage': 'shared-ancestors',
                           'status': 'done' if shared_progress.get('status') == 'complete' else 'running',
                           'progress': shared_progress})
    current_scan = progress.get('current_scan') or state.get('scan') or {}
    if not isinstance(current_scan, dict):
        current_scan = {}
    current_stage = progress.get('current_stage', {})
    if not isinstance(current_stage, dict):
        current_stage = {}
    if not current_stage and any(n['stage'] for n in nodes):
        current_stage = {key: sum(number(n['stage'].get(key)) or 0 for n in nodes
                                  if isinstance(n['stage'], dict))
                         for key in ('verified_files', 'verified_bytes', 'copied_bytes')}
    # The scanner may spend minutes walking directory or hardlink-only regions
    # before publishing another aggregate. Refresh just the current phase from
    # its unique stage counters, never cumulative counters from earlier rounds.
    phase_key = str(state.get('phase', progress.get('phase')))
    phase_progress = progress.get('phases', {}).get(phase_key, {})
    if isinstance(phase_progress, dict) and not recovery_warm:
        for key in ('verified_files', 'verified_bytes'):
            live = number(current_stage.get(key))
            published = number(phase_progress.get(key))
            if live is not None and published is not None and overall.get(key) is not None:
                overall[key] += max(0, live - published)
                phase_progress[key] = max(live, published)
        total, done = overall['discovered_bytes'], overall['verified_bytes']
        overall['percent'] = (100 * done / total if overall['inventory_complete']
                              and total and done is not None and done <= total else None)
    current_run_overall = dict(overall)
    if pending_extension(extension_plan):
        overall = dict(overall, inventory_complete=False, percent=None,
                       scope='priorities_1_to_4_known_lower_bound')
        phases = phases_with_extension(phases, extension_plan)
    resume_index = state.get('resume_index', {})
    cumulative_evidence = {}
    if recovery_warm and isinstance(resume_index, dict):
        historical_files = number(resume_index.get('verified_files'))
        historical_symlinks = number(resume_index.get('verified_symlinks')) or 0
        historical_bytes = number(resume_index.get('verified_bytes'))
        current_entries = max(number(warm_phase.get('verified_files')) or 0,
                              number(current_stage.get('verified_files')) or 0)
        current_bytes = max(number(warm_phase.get('verified_bytes')) or 0,
                            number(current_stage.get('verified_bytes')) or 0)
        if historical_files is not None and historical_bytes is not None:
            historical_entries = historical_files + historical_symlinks
            cumulative_evidence = {
                'exact': False,
                'historical_verified_files': historical_files,
                'historical_verified_symlinks': historical_symlinks,
                'historical_verified_entries': historical_entries,
                'historical_verified_bytes': historical_bytes,
                'current_verified_entries': current_entries,
                'current_verified_bytes': current_bytes,
                # Until the foreground receipts are merged by relative path, the
                # set/version intersection is unknown. These bound accumulated
                # version evidence, not the latest dataset's path capacity.
                'unique_entries_lower_bound': max(historical_entries, current_entries),
                'unique_entries_upper_bound': historical_entries + current_entries,
                'unique_bytes_lower_bound': max(historical_bytes, current_bytes),
                'unique_bytes_upper_bound': historical_bytes + current_bytes,
                'reason': '历史索引与恢复队列存在路径/版本重叠；边界仅表示累计证据，不代表当前路径容量',
            }
    view = {'control': str(root), 'source': config.get('source'), 'destination': config.get('destination'),
            'extension_plan': extension_plan, 'current_run_overall': current_run_overall,
            'status': state.get('status', progress.get('status', 'unknown')),
            'phase': state.get('phase', progress.get('phase')),
            'pipeline': state.get('pipeline', False), 'background_metadata': background,
            'recovery_warm': recovery_warm,
            'recovery_label': state.get('recovery_label'),
            'resume_index_building': state.get('resume_index_building') is True,
            'resume_index_error': state.get('resume_index_error'),
            'recovery_warm_progress': warm_phase,
            'round': state.get('round'), 'scan_complete': state.get('scan_complete', False),
            'completed_rounds': {str(p): len(value.get('rounds', []))
                                 for p, value in state.get('phases', {}).items()
                                 if isinstance(value, dict)},
            'scan_entries_per_second': config.get('scan_entries_per_second', 1000),
            'parallel_scan_threads': state.get('parallel_scan_threads', config.get('parallel_scan_threads')),
            'parallel_target_threads': state.get('parallel_target_threads', config.get('parallel_target_threads')),
            'target_filter_inflight': state.get('target_filter_inflight', config.get('target_filter_inflight')),
            'target_filter': state.get('target_filter', {}),
            'distributed_target_nodes': state.get('distributed_target_nodes', 0),
            'distributed_target_slots_per_node': config.get('target_probe_slots_per_node'),
            'distributed_target_probe': state.get('distributed_target_probe', {}),
            'distributed_target_probe_baseline': probe_baseline,
            'recovery_replay': state.get('recovery_replay', {}),
            'task_publication': state.get('task_publication', {}),
            'producer_error': state.get('producer_error'),
            'producer_nofile': state.get('producer_nofile'),
            'scan_batch_files': config.get('scan_batch_files'),
            'stage': stage_name,
            'updated': max((value for value in (progress.get('updated'), state.get('updated'),
                                               seed_progress.get('updated'),
                                               metrics.get('updated_at'), metrics.get('updated')) if value),
                           key=lambda value: sample_epoch(value) or 0, default=None),
            'sampled_at': dt.datetime.now(dt.timezone.utc).isoformat(),
            'overall': overall, 'current_scan': current_scan,
            'phases': phases, 'inventory': inventory, 'metrics': metrics,
            'metrics_sample_age_seconds': age(metrics.get('sample_time'), wall_time),
            'rate_history': metrics.get('history', metrics.get('samples', [])),
            'current_stage': current_stage,
            'training_health': {k: training_health[k] for k in
                                ('state', 'reason', 'observed_avg_step_seconds',
                                 'logmtime', 'log_mtime', 'log_mtime_utc', 'updated')
                                if k in training_health},
            'broker': {k: broker[k] for k in ('pid', 'host', 'started', 'updated', 'state', 'status')
                       if k in broker},
            'resume_index': resume_index,
            'recovery_baseline': recovery_baseline,
            'cumulative_evidence': cumulative_evidence,
            'seed_progress': seed_progress,
            'new_copied_bytes': (sum(n['bytes'] for n in nodes)
                                 if nodes and all(n['bytes'] is not None for n in nodes) else None),
            'streamed_bytes': (sum(n['streamed_bytes'] for n in nodes)
                               if nodes and all(n['streamed_bytes'] is not None for n in nodes)
                               else number(metrics.get('streamed_bytes'))),
            'rate_bytes_per_second': None, 'eta_seconds': None,
            'nodes': nodes, 'warnings': warnings}
    view['copy_work_progress'] = copy_work_progress(view)
    return view


def copy_work_progress(view):
    """Summarize observed work without inventing a unique-path ledger.

    Existing probes count successful existence observations, not verified
    bytes. Warm discovered counters count published work, unlike inventory
    totals. Worker acknowledgements can lag/replay, so their queue difference
    is only an estimate and a negative difference is unknown, never zero.
    """
    nodes = view.get('nodes', [])
    copied = [number(n.get('counts', {}).get('copied')) for n in nodes]
    copied_files = sum(copied) if copied and all(n is not None for n in copied) else None
    probe_counts = view.get('distributed_target_probe', {}).get('counts', {})
    existing_checks = number(probe_counts.get('remote_existing'))
    existing_lower_bound = False
    probe_baseline = view.get('distributed_target_probe_baseline')
    if isinstance(probe_baseline, dict):
        previous = number(probe_baseline.get('counts', {}).get('remote_existing'))
        if probe_baseline.get('complete') is True and previous is not None:
            if existing_checks is None:
                existing_checks = previous
                existing_lower_bound = True
            else:
                existing_checks += previous
        else:
            # A missing checkpoint is not a historical count of zero.
            existing_lower_bound = existing_checks is not None
    pending = {'files': None, 'bytes': None}
    skew = False
    if view.get('recovery_warm'):
        queued = view.get('recovery_warm_progress', {})
        completed = view.get('current_stage', {})
        for field in pending:
            total = number(queued.get('discovered_' + field))
            done = number(completed.get('verified_' + field))
            if total is not None and done is not None:
                if total >= done:
                    pending[field] = total - done
                else:
                    skew = True
    result = {'copied_files': copied_files,
            'copied_bytes': number(view.get('new_copied_bytes')) if nodes else None,
            'existing_checks': existing_checks,
            'existing_checks_lower_bound': existing_lower_bound,
            'existing_bytes': None,
            'pending_files': pending['files'], 'pending_bytes': pending['bytes'],
            'pending_estimated': view.get('recovery_warm') is True,
            'snapshot_skew': skew}
    baseline = view.get('recovery_baseline')
    if baseline:
        previous = baseline.get('work', {})
        for field in ('copied_files', 'copied_bytes'):
            old, current = number(previous.get(field)), number(result[field])
            result['cumulative_' + field] = old + (current or 0) if old is not None else None
        result['historical_baseline_present'] = True
        old_existing = number(previous.get('existing_checks'))
        if old_existing is not None:
            result['existing_checks'] = old_existing + (result['existing_checks'] or 0)
        else:
            result['existing_checks_lower_bound'] = result['existing_checks'] is not None
    return result


def sample_epoch(value):
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return dt.datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
    except (ValueError, TypeError, OverflowError):
        return None


def normalize_sample(sample):
    if not isinstance(sample, dict):
        return None
    epoch = sample_epoch(first(sample, 'sample_time', 'timestamp', 'time', 'sampled_at', 'updated'))
    if epoch is None:
        return None
    nodebytes = first(sample, 'nodebytes', 'node_bytes', 'new_copied_bytes', 'copied_bytes')
    if isinstance(nodebytes, dict):
        nodebytes = sum(number(v) or 0 for v in nodebytes.values())
    return {'time': epoch, 'copied_bytes': number(nodebytes),
            'streamed_bytes': number(sample.get('streamed_bytes')),
            'verified_bytes': number(first(sample, 'verified_bytes', 'unique_verified_bytes')),
            'verified_files': number(first(sample, 'verified_files', 'unique_verified_files'))}


def remaining_scan_work(view):
    """Model serial mandatory scans separately from copied/verified bytes."""
    rate = number(view.get('scan_entries_per_second'))
    if not rate:
        return None
    expected = {'1': 3, '2': 3, '3': 2}
    finished = {p: int(number(view.get('completed_rounds', {}).get(p)) or 0) for p in expected}
    current_phase = str(view.get('phase'))
    current_round = int(number(view.get('round')) or 0)
    incomplete = (current_phase in expected and current_round > 0
                  and not view.get('scan_complete'))
    if current_phase in expected and current_round > 0:
        finished[current_phase] = max(finished[current_phase], current_round - 1 +
                                      int(bool(view.get('scan_complete'))))
    if view.get('status') in ('complete', 'complete_with_changes'):
        finished = dict(expected)
        incomplete = False
    inventory_phases = view.get('inventory', {}).get('phases', {})
    floor_entries = estimated_entries = 0
    details = {}
    for phase, rounds in expected.items():
        inv = inventory_phases.get(phase, {})
        if inv.get('files') is not None:
            entries = sum(number(inv.get(k)) or 0 for k in ('files', 'dirs', 'symlinks'))
        else:
            entries = ((number(inv.get('discovered_files')) or
                        number(view.get('phases', {}).get(phase, {}).get('discovered_files')) or 0)
                       + (number(inv.get('dirs')) or 0))
        remaining_rounds = max(0, rounds - finished[phase])
        current_incomplete = incomplete and phase == current_phase and current_round <= rounds
        future_rounds = max(0, remaining_rounds - int(current_incomplete))
        future_entries = future_rounds * entries
        estimated = future_entries
        if current_incomplete:
            scan = view.get('current_scan', {})
            matching = scan.get('phase') in (None, phase, int(phase))
            processed = ((number(scan.get('files')) or number(scan.get('discovered_files')) or 0)
                         + (number(scan.get('dirs')) or 0)) if matching else 0
            estimated += max(0, entries - processed)
        floor_entries += future_entries
        estimated_entries += estimated
        details[phase] = {'expected_rounds': rounds, 'completed_scans': finished[phase],
                          'remaining_full_future_rounds': future_rounds,
                          'entries_per_round': entries, 'estimated_remaining_entries': estimated}
    return {'entries_per_second': rate, 'phases': details,
            'future_full_scan_entries': floor_entries,
            'estimated_remaining_entries': estimated_entries,
            'future_full_scan_floor_seconds': floor_entries / rate,
            'estimated_seconds': estimated_entries / rate}


class Rate:
    def __init__(self):
        self.previous = None
        self.samples = collections.deque()
        self.control = None

    def add(self, view, monotonic=None):
        tick = time.time() if monotonic is None else monotonic
        if self.control != view.get('control'):
            self.samples.clear()
            self.control = view.get('control')
        overall = view['overall']
        history = view.get('rate_history', [])
        if isinstance(history, dict):
            history = list(history.values())
        merged = {sample['time']: sample for sample in self.samples}
        for raw in history if isinstance(history, list) else []:
            sample = normalize_sample(raw)
            if sample is not None and sample['time'] <= tick + 5:
                merged[sample['time']] = sample
        merged[tick] = {'time': tick, 'copied_bytes': number(view.get('new_copied_bytes')),
                        'streamed_bytes': number(view.get('streamed_bytes')),
                        'verified_bytes': number(overall.get('verified_bytes')),
                        'verified_files': number(overall.get('verified_files'))}
        ordered = sorted(merged.values(), key=lambda s: s['time'])
        cutoff = tick - 305
        self.samples = collections.deque(s for s in ordered if s['time'] >= cutoff)
        view['rates'] = {}
        for name, window in (('1m', 60), ('5m', 300)):
            relevant = [s for s in self.samples if s['time'] >= tick - window]
            rate = {'seconds': 0, 'copied_bytes_per_second': None,
                    'streamed_bytes_per_second': None,
                    'verified_bytes_per_second': None, 'verified_files_per_second': None}
            if len(relevant) >= 2:
                start, end = relevant[0], relevant[-1]
                seconds = end['time'] - start['time']
                rate['seconds'] = seconds
                for key in ('copied_bytes', 'streamed_bytes', 'verified_bytes', 'verified_files'):
                    first_value, last_value = start.get(key), end.get(key)
                    if seconds > 0 and first_value is not None and last_value is not None and last_value >= first_value:
                        rate[key + '_per_second'] = (last_value - first_value) / seconds
            view['rates'][name] = rate
        smooth = view['rates']['5m']
        view['rate_bytes_per_second'] = smooth['copied_bytes_per_second']
        view['eta_seconds'] = None
        view['eta_range_seconds'] = None
        view['eta_finish_range'] = None
        status = str(view.get('status', ''))
        health_state = str(view.get('training_health', {}).get('state', ''))
        view['eta_suspended'] = (status in ('stopped', 'failed') or 'paus' in status
                                 or 'paus' in health_state)
        view['minimum_remaining_bytes'] = None
        view['minimum_remaining_seconds'] = None
        scan_model = remaining_scan_work(view)
        view['scan_model'] = scan_model
        total, done = overall.get('discovered_bytes'), overall.get('verified_bytes')
        remaining = max(0, total - done) if total is not None and done is not None else None
        view['minimum_remaining_bytes'] = remaining
        verified_rate = smooth['verified_bytes_per_second']
        file_rate = smooth['verified_files_per_second']
        copied_rate = smooth['copied_bytes_per_second']
        streamed_rate = smooth['streamed_bytes_per_second']
        payload_rate = streamed_rate if streamed_rate is not None else copied_rate
        payload_delta = max((copied_rate or 0), (streamed_rate or 0)) * smooth['seconds']
        representative = smooth['seconds'] >= 60 and payload_delta >= 64 * 1024**2
        historical = bool(view.get('resume_index'))
        if status == 'finalizing':
            reason = '当前由协调器整理目录和硬链接元数据，复制 worker 等待；此阶段不按传输速度估时'
        elif not payload_delta and historical:
            reason = '正在复核历史副本，尚无新增复制速度样本；暂不推算复制剩余时间'
        elif not representative:
            reason = '新增复制速度样本不足（需至少60秒及64 MiB）；暂不推算复制剩余时间'
        else:
            reason = '已有新增复制样本，结合唯一核验速度和必需扫描估算'
        view['eta_confidence'] = {'representative_copy_sample': representative,
                                  'sample_seconds': smooth['seconds'],
                                  'new_payload_sample_bytes': payload_delta, 'reason': reason}
        estimates = []
        if (remaining is not None and representative and verified_rate and verified_rate > 0
                and payload_rate and payload_rate > 0):
            # Fast metadata-only revalidation of old copies cannot stand in
            # for throughput of bytes that still need a new destination copy.
            estimates.append(remaining / min(verified_rate, payload_rate))
        total_files, done_files = overall.get('discovered_files'), overall.get('verified_files')
        # File completion frequency during large-file transfers is not a
        # representative service rate for millions of small files elsewhere.
        # Metadata traversal is modeled by mandatory scans instead; further
        # finalization overhead remains explicitly unmodeled.
        byte_estimate_ready = (remaining == 0 or
                               (remaining is not None and verified_rate and verified_rate > 0
                                and payload_rate and payload_rate > 0))
        if estimates and representative and byte_estimate_ready:
            duration = max(estimates)
            if overall['inventory_complete'] and not view['eta_suspended']:
                view['minimum_remaining_seconds'] = duration
                if scan_model is not None:
                    duration = max(duration, scan_model['estimated_seconds'])
                view['eta_seconds'] = duration
                view['eta_range_seconds'] = [duration, duration * 1.5]
                view['eta_finish_range'] = [tick + duration, tick + duration * 1.5]
        if (overall['inventory_complete'] and not view['eta_suspended']
                and scan_model is not None and scan_model['estimated_seconds'] > 0
                and remaining == 0
                and view['eta_range_seconds'] is None):
            duration = scan_model['estimated_seconds']
            view['eta_seconds'] = duration
            view['eta_range_seconds'] = [duration, duration * 1.5]
            view['eta_finish_range'] = [tick + duration, tick + duration * 1.5]
            view['eta_scan_only'] = True
        if (overall['inventory_complete'] and remaining == 0 and total_files == done_files
                and (scan_model is None or scan_model['estimated_seconds'] == 0)):
            view['eta_seconds'] = 0
            view['eta_range_seconds'] = [0, 0]
            view['eta_finish_range'] = [tick, tick]
        metrics_age = number(view.get('metrics_sample_age_seconds'))
        view['eta_metrics_stale'] = metrics_age is not None and metrics_age > 30
        if view['eta_metrics_stale']:
            view['eta_seconds'] = None
            view['eta_range_seconds'] = None
            view['eta_finish_range'] = None
            view['minimum_remaining_seconds'] = None
            view['eta_confidence']['reason'] = f'速率采样已延迟 {metrics_age:.0f} 秒；等待新样本后再估算完成时间'
        metadata_pending = bool(view.get('pipeline')) and (
            status == 'finalizing' or (remaining == 0 and any(
                job.get('status') in ('running', 'queued') for job in view.get('background_metadata', []))))
        view['eta_background_metadata_pending'] = metadata_pending
        if metadata_pending:
            view['eta_seconds'] = None
            view['eta_range_seconds'] = None
            view['eta_finish_range'] = None
            view['minimum_remaining_seconds'] = None
            view['eta_confidence']['reason'] = '目录与硬链接正在后台并行收尾；剩余时间尚需元数据处理样本，请查看后台任务进度'
        if pending_extension(view.get('extension_plan')):
            view.setdefault('current_run_overall', dict(overall))
            overall.update(percent=None, inventory_complete=False,
                           scope='priorities_1_to_4_known_lower_bound')
            view['eta_seconds'] = None
            view['eta_range_seconds'] = None
            view['eta_finish_range'] = None
            view['minimum_remaining_seconds'] = None
            view['eta_confidence']['reason'] = '第4项尚未纳入规模清单；全任务完成比例和预计完成时间暂不能计算'
        self.previous = (self.control, tick, view.get('new_copied_bytes'), done,
                         total, overall['inventory_complete'])


def size(value):
    if value is None:
        return '未知'
    value = float(value)
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'):
        if value < 1024 or unit == 'PiB':
            return f'{value:.1f} {unit}'
        value /= 1024


def count(value):
    return '未知' if value is None else f'{int(value):,}'


def duration(seconds):
    if seconds is None:
        return '未知'
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours}h {minutes}m {seconds}s'


def display_time(value):
    if value is None:
        return '未知'
    try:
        if isinstance(value, (int, float)):
            parsed = dt.datetime.fromtimestamp(value, dt.timezone.utc)
        else:
            parsed = dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(SHANGHAI).strftime('%Y-%m-%d %H:%M:%S 北京时间')
    except (ValueError, TypeError, OverflowError, OSError):
        return str(value)


def first(mapping, *names):
    return next((mapping[name] for name in names if mapping.get(name) is not None), None)


def display_width(value):
    return sum(0 if unicodedata.combining(c) else
               2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1
               for c in str(value))


def aligned_table(headers, rows, right_align=()):
    cells = [[str(cell) for cell in row] for row in [headers] + list(rows)]
    widths = [max(display_width(row[column]) for row in cells)
              for column in range(len(headers))]
    result = []
    for row in cells:
        padded = []
        for column, cell in enumerate(row):
            spaces = ' ' * (widths[column] - display_width(cell))
            padded.append(spaces + cell if column in right_align else cell + spaces)
        result.append(' | '.join(padded).rstrip())
    return result


def seed_progress_lines(seed):
    """Describe observed recovery work without treating it as new copy proof."""
    if not isinstance(seed, dict) or not seed:
        return ['历史回执恢复：等待发布实际处理计数']
    names = {'enumerating': '枚举历史回执', 'reading_receipts': '读取并合并历史回执',
             'finalizing': '整理恢复索引', 'publishing': '发布恢复索引',
             'importing': '合并恢复索引', 'complete': '恢复索引已完成'}
    lines = ['历史回执恢复：' + names.get(seed.get('status'), '正在恢复历史回执索引'),
             f"实际处理：{count(seed.get('processed_receipts', seed.get('receipts')))} 份回执；"
             f"当前已发现：{count(seed.get('receipt_paths_discovered'))} 份",
             f"读取成功：{count(seed.get('receipts'))} 份；"
             f"已处理结果：{count(seed.get('results'))} 条；问题：{count(seed.get('issues'))}"]
    if number(seed.get('verified_files')) is not None:
        lines.append(f"已恢复索引的有效历史文件记录：{count(seed['verified_files'])} 文件 / "
                     f"{size(seed.get('verified_bytes'))}（待本轮重新确认）")
    controls = seed.get('controls')
    current = seed.get('current_control')
    total = number(seed.get('control_count'))
    if isinstance(controls, list) and current in controls:
        position = controls.index(current) + 1
        lines.append(f"祖先控制目录：当前第 {position} 代，共 {count(total)} 代；{pathlib.Path(current).name}")
    elif total is not None:
        lines.append(f"祖先控制目录：共 {count(total)} 代" +
                     ('；当前 ' + pathlib.Path(current).name if isinstance(current, str) else ''))
    elif isinstance(current, str):
        lines.append('当前祖先控制目录：' + current)
    receipt = seed.get('current_receipt')
    if isinstance(receipt, str):
        try:
            receipt = str(pathlib.Path(receipt).relative_to(current)) if isinstance(current, str) else receipt
        except ValueError:
            pass
        lines.append('当前回执：' + receipt)
    if number(seed.get('elapsed_seconds')) is not None:
        lines.append('恢复已用时：' + duration(seed['elapsed_seconds']))
    if number(seed.get('rows_processed')) is not None:
        lines.append(f"索引导入：已处理 {count(seed['rows_processed'])} 行；"
                     f"新增 {count(seed.get('rows_inserted'))} 行；"
                     f"补充所有权 {count(seed.get('ownership_upgraded'))} 行")
    lines.append('以上为实际回执处理量；不代表本轮已重新校验的副本或全量规模。')
    return lines


def node_progress_lines(view):
    rows = []
    for node in view.get('nodes', []):
        label = '-'.join(str(node.get('host', '-')).split('-')[-2:])
        heartbeat = number(node.get('heartbeat_age_seconds'))
        heartbeat = f'{heartbeat:.0f}s' if heartbeat is not None else '未知'
        counters = node.get('counts', {})
        counters = ', '.join(f'{k}={v}' for k, v in sorted(counters.items())) if isinstance(counters, dict) else '-'
        rows.append([label, f"{count(node.get('active_threads'))}/{count(node.get('threads'))}",
                     size(node.get('bytes')), count(node.get('verified_files')),
                     size(node.get('verified_bytes')), heartbeat, counters])
    if not rows:
        return ['尚无节点心跳。']
    active = sum(number(n.get('active_threads')) or 0 for n in view['nodes'])
    threads = sum(number(n.get('threads')) or 0 for n in view['nodes'])
    lines = [f"合计 {len(rows)} 个节点 · 活跃 {count(active)}/{count(threads)} 线程", '']
    lines.extend(aligned_table(['节点', '活跃/总线程', '本次新复制', '核验次数',
                                '核验字节', '心跳', '结果计数'], rows,
                               right_align=(1, 2, 3, 4, 5)))
    return lines


def copy_speed_lines(view):
    """Show recent logical copy rates only with enough fresh telemetry."""
    metric_age = number(view.get('metrics_sample_age_seconds'))
    stale = view.get('eta_metrics_stale') is True or (metric_age is not None and metric_age > 30)
    rows = []
    for key, label in (('1m', '近1分钟'), ('5m', '近5分钟')):
        rate = view.get('rates', {}).get(key, {})
        seconds = number(rate.get('seconds'))
        if stale:
            copied = streamed = '已过期'
        elif seconds is None or seconds < 30:
            copied = streamed = '采样中'
        elif metric_age is None:
            copied = streamed = '未知'
        else:
            copied_rate = number(rate.get('copied_bytes_per_second'))
            streamed_rate = number(rate.get('streamed_bytes_per_second'))
            copied = size(copied_rate) + '/s' if copied_rate is not None else '未知'
            streamed = size(streamed_rate) + '/s' if streamed_rate is not None else '未知'
        rows.append([label, f'{seconds:.0f}s' if seconds is not None else '未知', copied, streamed])
    return ['复制速度（逻辑字节）：'] + aligned_table(
        ['窗口', '有效采样', '新复制完成', '传输写入'], rows, right_align=(1, 2, 3))


def render(view, details=False):
    if details:
        return render_details(view)
    work = copy_work_progress(view)
    status = view.get('status') or 'unknown'
    status_text = STATUS_NAMES.get(status, status)
    if (status == 'scanning'
            and view.get('recovery_replay', {}).get('phase') == 'inventory_replay'):
        status_text = '恢复清单（尚未恢复新任务供给）'
    data_age = age(view.get('updated'), time.time())
    age_text = f'{data_age:.0f}s' if data_age is not None else '未知'
    lines = ['=' * 96,
             f"复制进度开始  |  {display_time(view.get('sampled_at') or view.get('updated'))}",
             '-' * 96,
             f"状态：{status_text}  |  数据年龄：{age_text}",
             '', '【整体进度】']
    existing = work['existing_checks']
    existing_text = count(existing) + ' 次命中' if existing is not None else '未知'
    if work.get('existing_checks_lower_bound'):
        existing_text = '至少 ' + existing_text + '（已知下限）'
    rows = [
        ['本次新复制', count(work['copied_files']) + ' 项', size(work['copied_bytes'])],
        ['已探明无需新复制（目标已存在）',
         existing_text, '未知（未记录）'],
        ['已探明待复制/确认（估算，含在途）',
         '约 ' + count(work['pending_files']) + ' 项' if work['pending_files'] is not None else
         ('同步中' if work['snapshot_skew'] else '未知'),
         '约 ' + size(work['pending_bytes']) if work['pending_bytes'] is not None else
         ('同步中' if work['snapshot_skew'] else '未知')],
    ]
    if work.get('historical_baseline_present'):
        rows[0][0] = '本次恢复新复制'
        rows.insert(0, ['已记录累计新复制（含旧任务）',
                       count(work['cumulative_copied_files']) + ' 项',
                       size(work['cumulative_copied_bytes'])])
    lines.extend('  ' + line for line in aligned_table(['分类', '数量', '逻辑字节'], rows, right_align=(1, 2)))
    lines.append('')
    lines.extend('  ' + line for line in copy_speed_lines(view))
    lines.append('  已存在仅做存在检查，待正式内容核验；待复制量按队列差值估算，含重试。')
    if not view.get('overall', {}).get('inventory_complete'):
        suffix = '；第4项尚未纳入' if pending_extension(view.get('extension_plan')) else ''
        lines.append('  范围：本次已探明部分，扫描未结束' + suffix + '。')
    if work['snapshot_skew']:
        lines.append('  部分队列与完成快照尚未同步，未将负差显示为零。')
    problems = list(view.get('warnings', []))
    if status == 'producer_paused' and view.get('producer_error'):
        error_lines = str(view['producer_error']).strip().splitlines()
        problems.insert(0, '生产端错误：' + error_lines[-1])
    if view.get('resume_index_error'):
        problems.append('恢复索引异常：' + str(view['resume_index_error']))
    for node in view.get('nodes', []):
        if node.get('last_error'):
            problems.append('-'.join(str(node.get('host', '-')).split('-')[-2:]) + '：' + str(node['last_error']))
        heartbeat = number(node.get('heartbeat_age_seconds'))
        if node.get('ended') or (heartbeat is not None and heartbeat > 180):
            problems.append('-'.join(str(node.get('host', '-')).split('-')[-2:]) + '：进程已结束或心跳延迟')
    if data_age is not None and data_age > 60:
        problems.append(f'整体进度已 {data_age:.0f}s 未更新')
    for problem in problems[:3]:
        lines.append('  注意：' + str(problem).replace('\n', ' ')[:180])
    if len(problems) > 3:
        lines.append(f'  另有 {len(problems) - 3} 条异常，使用 --details 查看。')
    lines.extend(['', '【节点明细】'])
    lines.extend('  ' + line for line in node_progress_lines(view))
    return '\n'.join(lines)


def render_details(view):
    overall = view['overall']
    status = view.get('status') or 'unknown'
    status_text = ('后台并行收尾' if view.get('pipeline') and status == 'finalizing'
                   else STATUS_NAMES.get(status, status))
    extension = pending_extension(view.get('extension_plan'))
    if extension and status in ('complete', 'complete_with_changes'):
        prefix = '前三项完成' if status == 'complete' else '前三项收尾完成（有待处理变化）'
        status_text = prefix + ('；第4项等待验收后接续' if extension['status'] == 'queued'
                                else '；第4项正在独立复制')
    data_age = age(view.get('updated'), time.time())
    age_text = f'{data_age:.0f}s' if data_age is not None else '尚无进度时间记录'
    lines = ['=' * 96,
             f"复制进度开始  |  {display_time(view.get('sampled_at') or view.get('updated'))}",
             '-' * 96,
             (f"状态：{status_text}  |  恢复续传队列（非正式第3轮）" if view.get('recovery_warm') else
              f"状态：{status_text}  |  优先级 {view.get('phase') or '-'} "
              f"{PHASE_NAMES.get(str(view.get('phase')), '-')}  |  第{view.get('round') or '-'}轮"),
             f"数据年龄：{age_text}"]

    def section(title, content):
        if content:
            lines.extend(['', '【' + title + '】'])
            lines.extend('  ' + line for line in content)

    # Keep operational warnings above the long progress and node tables.
    alerts = []
    health = view.get('training_health', {})
    health_state = str(health.get('state', ''))
    if status in ('failed', 'stopped') or 'paus' in status or 'paus' in health_state:
        alerts.append('- 任务状态：' + status_text + '；暂停提供完成时间估算。')
    if health.get('reason') and health_state not in ('watching', 'healthy', 'running'):
        alerts.append('- 训练保护：' + str(health['reason']).replace('\n', ' ')[:220])
    if view.get('resume_index_error'):
        alerts.append('- 后台恢复索引异常：' + str(view['resume_index_error']).replace('\n', ' ')[:350])
    metrics_age = number(view.get('metrics_sample_age_seconds'))
    if metrics_age is not None and metrics_age > 30:
        alerts.append(f'- 速率样本延迟 {metrics_age:.0f}s；历史均速仅供参考，等待新样本后恢复 ETA。')
    alerts.extend('- ' + str(message) for message in view.get('warnings', []))
    for node in view['nodes']:
        label = '-'.join(node['host'].split('-')[-2:])
        if node.get('last_error'):
            alerts.append('- ' + label + ' 最近错误记录：' + str(node['last_error']).replace('\n', ' ')[:220])
        if node.get('ended'):
            alerts.append('- ' + label + ' 工作进程已结束：' + display_time(node['ended']))
        heartbeat = number(node.get('heartbeat_age_seconds'))
        if heartbeat is not None and heartbeat > 180:
            alerts.append(f'- {label} 心跳已延迟 {heartbeat:.0f}s。')
    probe = view.get('distributed_target_probe', {})
    unhealthy_probes = []
    for endpoint, counters in probe.get('endpoints', {}).items() if isinstance(probe, dict) else []:
        requests = number(counters.get('requests')) if isinstance(counters, dict) else None
        errors = number(counters.get('remote_errors')) if isinstance(counters, dict) else None
        if requests and requests >= 100 and errors is not None and errors / requests >= .5:
            unhealthy_probes.append((errors / requests, endpoint, int(requests)))
    for error_ratio, endpoint, requests in sorted(unhealthy_probes, reverse=True)[:3]:
        alerts.append(f'- 目标检查端点 {endpoint} 错误率 {error_ratio:.1%}（{count(requests)} 次请求）；'
                      '该端点会触发重试并拖慢任务供给。')
    active = sum(number(n.get('active_threads')) or 0 for n in view['nodes'])
    threads = sum(number(n.get('threads')) or 0 for n in view['nodes'])
    if (view.get('recovery_warm') and threads and active / threads < .25
            and number(view.get('target_filter', {}).get('checked')) is not None):
        alerts.append(f'- 当前复制线程活跃 {count(active)}/{count(threads)}；'
                      '需结合待领取任务量、目标检查延迟和存储 I/O 判断原因；仅凭活跃率无法确定瓶颈。')
    section('需要关注', alerts)

    qualifier = '总量已清点' if overall['inventory_complete'] else '仍在清点，当前为下限'
    summary = [f"已确认副本：{count(overall['verified_files'])} 文件 / {size(overall['verified_bytes'])}",
               f"已发现规模：{count(overall['discovered_files'])} 文件 / "
               f"{size(overall['discovered_bytes'])}（{qualifier}）"]
    if overall['percent'] is not None:
        summary.append(f"完成比例：{overall['percent']:.2f}%（按唯一文件字节）")
    elif overall['inventory_complete']:
        summary.append('完成比例：未知，当前完成量与清单需再次核对')
    else:
        summary.append('完成比例：未知，待全量清单确定后计算')
    if overall['inventory_complete']:
        eta_range = view.get('eta_range_seconds')
        finish = view.get('eta_finish_range')
        if eta_range and finish:
            summary.extend([f"预计剩余：{duration(eta_range[0])} — {duration(eta_range[1])}",
                            f"预计完成：{display_time(finish[0])}",
                            f"        至 {display_time(finish[1])}",
                            '  - 取字节复制与必需扫描耗时的较大值，另留 50% 参考余量。',
                            '  - 当前大文件速度不外推所有小文件；训练保护暂停会顺延。'])
        elif view.get('eta_suspended'):
            summary.append('预计完成：复制已暂停，恢复并取得稳定速率后重新计算')
        else:
            summary.append('预计完成：暂不能可靠估算')
            summary.append('  - ' + view.get('eta_confidence', {}).get('reason',
                           '尚无可靠新增复制速度样本，暂不推算复制剩余时间'))
    else:
        summary.extend(['预计完成：待全量清单确定，当前阶段速度不外推其他目录',
                        f"待本轮确认：至少 {size(view.get('minimum_remaining_bytes'))}",
                        '  - 待确认内容可能已有副本，不等于还需要新增复制的数据量。'])
        if not view.get('eta_confidence', {}).get('representative_copy_sample'):
            summary.append('  - ' + view.get('eta_confidence', {}).get('reason',
                           '新增复制速度正在采样，暂不推算复制剩余时间'))
    if extension:
        summary = [f"已确认副本（优先级1–4合计）：{count(overall.get('verified_files'))} 文件 / {size(overall.get('verified_bytes'))}",
                   f"已发现规模：至少 {count(overall.get('discovered_files'))} 文件 / {size(overall.get('discovered_bytes'))}",
                   '完成比例：未知，第4项尚未扫描，全部范围的清单尚未确定。',
                   '预计完成：待全量清单确定后估算。',
                   '计量范围：1–4统一统计；eai_pwm由1–3覆盖，计量一次。']
    evidence = view.get('cumulative_evidence', {})
    if evidence:
        summary = [
            f"历史 + 本次累计工作量（含重叠）："
            f"{count(evidence.get('unique_entries_upper_bound'))} 项 / "
            f"{size(evidence.get('unique_bytes_upper_bound'))}",
            f"  历史唯一成功证据：{count(evidence.get('historical_verified_entries'))} 项 / "
            f"{size(evidence.get('historical_verified_bytes'))}",
            f"  本次恢复队列已确认：{count(evidence.get('current_verified_entries'))} 项 / "
            f"{size(evidence.get('current_verified_bytes'))}",
            f"累计版本证据范围：{count(evidence.get('unique_entries_lower_bound'))} — "
            f"{count(evidence.get('unique_entries_upper_bound'))} 项 / "
            f"{size(evidence.get('unique_bytes_lower_bound'))} — "
            f"{size(evidence.get('unique_bytes_upper_bound'))}",
            f"正式轮次计数：{count(overall.get('verified_files'))} 文件 / "
            f"{size(overall.get('verified_bytes'))}（恢复预热不计入正式八轮）",
            f"当前已发现规模：至少 {count(overall.get('discovered_files'))} 文件 / "
            f"{size(overall.get('discovered_bytes'))}",
            '以上范围仅描述历史与本次版本证据，不代表目标当前容量或本轮完成量。',
            '完成比例：未知；当前路径完成量尚待合并核验，且第4项尚未扫描。',
            '预计完成：待当前路径核验及全量清单确定后估算。',
        ]
    section('整体进度与完成预期', summary)

    if evidence:
        historical_files = evidence.get('historical_verified_files')
        historical_symlinks = evidence.get('historical_verified_symlinks')
        cumulative = [
            f"历史唯一成功证据：{count(historical_files)} 文件"
            f" + {count(historical_symlinks)} 符号链接 / {size(evidence.get('historical_verified_bytes'))}",
            f"本次恢复队列已确认：{count(evidence.get('current_verified_entries'))} 项 / "
            f"{size(evidence.get('current_verified_bytes'))}",
            f"累计版本证据范围：{count(evidence.get('unique_entries_lower_bound'))} — "
            f"{count(evidence.get('unique_entries_upper_bound'))} 项 / "
            f"{size(evidence.get('unique_bytes_lower_bound'))} — "
            f"{size(evidence.get('unique_bytes_upper_bound'))}",
            "  - 下界取两侧较大证据量；上界按两侧版本证据全部不重叠计算。",
            "  - 版本证据范围不是当前数据容量；当前路径完成量须按相对路径及源版本合并核验。",
            f"恢复后实际新写入：{size(view.get('new_copied_bytes'))}",
        ]
        section('断点续传与累计证据', cumulative)

    phases = phases_with_extension(view.get('phases', {}), view.get('extension_plan'))
    phase_lines = []
    phase_ids = ('1', '2', '3', '4') if '4' in phases else ('1', '2', '3')
    for phase_id in phase_ids:
        phase = phases.get(phase_id, {})
        current = '  ← 当前' if phase_id == str(view.get('phase')) and not view.get('recovery_warm') else ''
        phase_lines.append(f"{phase_id}. {PHASE_NAMES[phase_id]}{current}")
        if phase_id == '4' and extension and phase.get('planned'):
            state_text = ('排队，等待1–3全部校验及元数据收尾完成' if extension['status'] == 'queued'
                          else '运行中，等待独立复制任务发布进度')
            phase_lines.extend(['   状  态：' + state_text,
                                '   已确认：尚未开始' if extension['status'] == 'queued' else '   已确认：尚未发布',
                                '   规  模：未知，尚未扫描',
                                '   范  围：排除已由1–3覆盖的顶层 eai_pwm；其余内容全部复制'])
            continue
        if not phase:
            phase_lines.append('   尚未发布清点与本轮核验数据')
            continue
        complete = phase.get('inventory_complete') is True
        copy_scan_complete = phase.get('copy_scan_complete') is True
        independent = phase.get('independent_inventory', {})
        if complete:
            label = '规模清点完成，在线观测值'
        elif copy_scan_complete:
            label = '当前观测规模，待最终补扫确认'
        else:
            label = '扫描未结束，当前为已发现下限'
        verified_text = (f"{count(phase.get('verified_files'))} 文件 / {size(phase.get('verified_bytes'))}"
                         if phase.get('verified_files') is not None else '本轮尚未开始核验')
        discovered_text = (f"{count(phase.get('discovered_files'))} 文件 / {size(phase.get('discovered_bytes'))}"
                           if phase.get('discovered_files') is not None else '尚未覆盖此阶段')
        phase_lines.extend(['   已确认：' + verified_text,
                            '   规  模：' + discovered_text + '（' + label + '）'])
        if 'copy_scan_complete' in phase or independent:
            scan_text = '复制清单至少完整扫描过一轮' if copy_scan_complete else '复制清单尚未完整扫描'
            if independent:
                inventory_text = ('独立清点完成' if independent.get('complete')
                                  else '独立清点未结束')
                inventory_text += (f" {count(independent.get('completed_units'))}/"
                                   f"{count(independent.get('unit_count'))} 单元")
                if independent.get('complete') and independent.get('exact') is False:
                    inventory_text += '（含未解决差异）'
                if number(independent.get('errors')):
                    inventory_text += f"；读取错误 {count(independent['errors'])}"
                scan_text += '；' + inventory_text
            phase_lines.append('   扫  描：' + scan_text)
    section('各优先级进度', phase_lines)

    if view.get('recovery_warm'):
        current = ['任务队列：恢复续传队列（非正式第3轮）',
                   '恢复队列独立于正式八轮；本队列处理量不计入正式轮次完成量。']
        if view.get('recovery_label'):
            current.append(str(view['recovery_label']))
    elif status == 'preparing_resume':
        current = ['任务轮次：尚未开始，先恢复历史回执索引']
    else:
        current = [f"任务轮次：{view.get('stage') or '-'}（"
                   f"{'扫描完成' if view.get('scan_complete') else '扫描未结束'}）"]
    if status == 'preparing_resume' or view.get('resume_index_building'):
        current.extend(seed_progress_lines(view.get('seed_progress', {})))
    scan = view.get('current_scan', {})
    if scan:
        current.append(f"扫描已发现：{count(scan.get('files', scan.get('discovered_files')))} 文件 / "
                       f"{size(scan.get('bytes', scan.get('discovered_bytes')))}")
    stage = view.get('current_stage', {})
    if isinstance(stage, dict) and stage:
        label = '恢复队列已确认' if view.get('recovery_warm') else '本轮已确认'
        current.append(f"{label}：{count(stage.get('verified_files'))} 文件 / "
                       f"{size(stage.get('verified_bytes'))}")
    current.append(f"恢复后新复制：{size(view['new_copied_bytes'])}")
    if view.get('parallel_scan_threads'):
        cap = view.get('scan_entries_per_second')
        limit_text = f"{count(cap)} 条目/s" if cap else '无固定条目速率上限'
        current.extend([f"扫描配置：{count(view['parallel_scan_threads'])} 个目录线程，"
                        f"每批最多 {count(view.get('scan_batch_files'))} 文件",
                        '          ' + limit_text])
    if view.get('parallel_target_threads'):
        current.append(f"目标存在性检查：{count(view['parallel_target_threads'])} 线程；"
                       f"最多 {count(view.get('target_filter_inflight'))} 个在途检查")
        if view.get('distributed_target_nodes'):
            current.append(f"  分布在 {count(view['distributed_target_nodes'])} 个节点；"
                           f"每节点最多 {count(view.get('distributed_target_slots_per_node'))} 路只读元数据检查")
        filtering = view.get('target_filter', {})
        if filtering:
            current.append(f"  已检查 {count(filtering.get('checked'))}；"
                           f"发现缺失 {count(filtering.get('missing'))}；"
                           f"当前在途 {count(filtering.get('inflight'))}")
    section('当前轮次与配置', current)

    rate_rows = []
    rate_notes = []
    for name, label in (('1m', '近1分钟'), ('5m', '近5分钟')):
        rate = view.get('rates', {}).get(name, {})
        seconds = rate.get('seconds', 0)
        if seconds >= 30:
            payload = size(rate.get('copied_bytes_per_second')) + '/s'
            streamed = rate.get('streamed_bytes_per_second')
            streamed_text = size(streamed) + '/s' if streamed is not None else '计数未启用'
            verified = size(rate.get('verified_bytes_per_second')) + '/s'
            fps = rate.get('verified_files_per_second')
            fps_text = f'{fps:.1f}' if fps is not None else '未知'
            rate_rows.append([label, f'{seconds:.0f}s', streamed_text, payload, verified, fps_text])
        else:
            rate_rows.append([label, f'{seconds:.0f}s', '-', '-', '-', '-'])
            rate_notes.append(f'- {label}正在积累稳定样本，至少需 30s。')
    speed = aligned_table(['窗口', '有效采样', '传输写入', '新复制完成', '唯一核验', '核验文件/s'],
                          rate_rows, right_align=(1, 2, 3, 4, 5))
    section('近期平均速度', speed + rate_notes)

    background = []
    if view.get('pipeline'):
        jobs = view.get('background_metadata', [])
        for kind, label in (('hardlinks', '硬链接',), ('directories', '目录元数据')):
            counts = collections.Counter(job.get('status', 'queued') for job in jobs if job.get('kind') == kind)
            background.append(f"{label}：运行 {counts['running']} / 排队 {counts['queued']} / 完成 {counts['done']}")
        for job in jobs:
            detail = job.get('progress')
            if job.get('kind') != 'directories' or not detail:
                continue
            background.extend([f"  - {job.get('stage', '-')} · {detail.get('status', job.get('status', '-'))}",
                               f"    目录检查 {count(detail.get('created_checked'))}/{count(detail.get('total_directories'))}"
                               f"；元数据处理 {count(detail.get('directories'))}/{count(detail.get('total_directories'))}",
                               f"    {count(detail.get('threads'))} 线程；错误 {count(detail.get('errors'))}"])
        if status == 'finalizing':
            background.extend(['- 文件复制轮次已结束，目录与硬链接仍在并行收尾。',
                               '  整体完成须等待后台任务和共享祖先目录收尾结束。'])
    scan_model = view.get('scan_model')
    if scan_model is not None and overall['inventory_complete']:
        background.extend(['后续必需扫描：优先级 1/2 各 3 轮，优先级 3 共 2 轮',
                           f"  - 后续完整轮次至少 {duration(scan_model['future_full_scan_floor_seconds'])}",
                           f"  - 连同当前余量约 {duration(scan_model['estimated_seconds'])}"
                           f"（扫描上限 {count(scan_model['entries_per_second'])} 项/s）"])
        rounds = ', '.join(f"优先级{p} {info['completed_scans']}/{info['expected_rounds']}轮"
                           for p, info in scan_model['phases'].items())
        background.extend(['  - 已完成：' + rounds,
                           '  - 权限、硬链接和最终变化检查耗时尚未计准，可能进一步延长。'])
    section('后台收尾与后续检查', background)

    node_rows = []
    for node in view['nodes']:
        host = node['host'].split('-')
        label = '-'.join(host[-2:])
        heartbeat = node['heartbeat_age_seconds']
        heartbeat = f'{heartbeat:.0f}s' if heartbeat is not None else '未知'
        counts = node['counts']
        counters = ', '.join(f'{k}={v}' for k, v in sorted(counts.items())) if isinstance(counts, dict) else '-'
        node_rows.append([label, f"{node['active_threads']}/{node['threads']}", size(node['bytes']),
                          count(node['verified_files']), size(node['verified_bytes']), heartbeat, counters])
    if node_rows:
        active = sum(number(n.get('active_threads')) or 0 for n in view['nodes'])
        threads = sum(number(n.get('threads')) or 0 for n in view['nodes'])
        node_lines = [f"合计 {len(node_rows)} 个节点 · 活跃 {count(active)}/{count(threads)} 线程", '']
        node_lines.extend(aligned_table(['节点', '活跃/总线程', '恢复后新复制', '核验次数',
                                         '核验字节', '心跳', '结果计数'], node_rows,
                                        right_align=(1, 2, 3, 4, 5)))
    else:
        node_lines = ['尚无节点心跳。']
    section('节点明细', node_lines)

    protection = []
    if health:
        step = number(health.get('observed_avg_step_seconds'))
        step_text = f'{step:.2f}s' if step is not None else '未知'
        logmtime = first(health, 'logmtime', 'log_mtime', 'log_mtime_utc')
        protection.extend([f"训练状态：{health.get('state', '未知')}；平均每步 {step_text}",
                           f"训练日志：{display_time(logmtime)}"])
        if health.get('reason'):
            protection.append('  - ' + str(health['reason']).replace('\n', ' ')[:130])
    resume = view.get('resume_index', {})
    if isinstance(resume, dict) and resume:
        files = first(resume, 'verified_files', 'previous_verified_files', 'indexed_files', 'files', 'rows')
        payload = first(resume, 'verified_bytes', 'previous_verified_bytes', 'indexed_bytes', 'bytes')
        protection.extend([f"恢复基线：历史成功副本 {count(number(files))} 文件 / {size(number(payload))}",
                           '  - 待本轮逐项确认，与总体完成量重叠，不能相加。'])
    protection.extend(['统计口径：总体按唯一文件计量；节点核验次数和字节可包含多轮复核。',
                       '          传输写入与新复制完成为不同计数，均不能当作物理存储吞吐。',
                       f"源路径：{view.get('source') or '-'}",
                       f"目标路径：{view.get('destination') or '-'}"])
    section('训练保护、恢复基线与统计口径', protection)
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--control', help='控制目录；省略时跟随稳定 active.json 指针')
    parser.add_argument('--watch', nargs='?', const=5.0, type=float, metavar='SECONDS',
                        help='持续刷新，默认每 5 秒')
    parser.add_argument('--json', action='store_true', help='输出一次 JSON')
    parser.add_argument('--details', action='store_true', help='显示阶段、历史证据、配置及后台任务详情')
    args = parser.parse_args(argv)
    if args.json and args.watch is not None:
        parser.error('--json 输出一次，不能与 --watch 同用')
    if args.watch is not None and args.watch <= 0:
        parser.error('--watch 必须大于 0')
    rate = Rate()
    try:
        while True:
            view = snapshot(args.control)
            rate.add(view)
            if args.json:
                print(json.dumps(view, ensure_ascii=False, indent=2))
            else:
                if args.watch is not None and sys.stdout.isatty():
                    print('\033[2J\033[H', end='')
                print(render(view, details=args.details), flush=True)
            if args.watch is None:
                return 0 if view.get('control') else 1
            time.sleep(args.watch)
            if not sys.stdout.isatty():
                print()
    except (KeyboardInterrupt, BrokenPipeError):
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
