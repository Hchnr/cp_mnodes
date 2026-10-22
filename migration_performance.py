"""Pure performance evaluation; this module performs no I/O or actions.

Call evaluate_performance with an explicit epoch ``now`` and persist its
``next_state`` for the next observation. Nodes are current physical-node
records, normally migration_nodes.load_nodes output. A caller must supply the
actual direct STOP result via ``stop`` or state['stop'].

Dense metrics supply byte history. Copied/revalidated file counts and seed
progress need dense, real samples as well: either pass tagged history or call
this evaluator every <=30 seconds and retain next_state. Unique verified_files
is deliberately never substituted for copied_files.
"""

import copy
import datetime
import math
import statistics


DEFAULT_POLICY = {
    'window_seconds': 300.0, 'max_age_seconds': 30.0, 'max_gap_seconds': 30.0,
    'future_tolerance_seconds': 5.0, 'expected_nodes': 12,
    'history_seconds': 1800.0, 'active_size_min_samples': 16,
    'large_size_bytes': 64 * 1024**2, 'small_size_bytes': 1024**2,
    'inflight_large_bytes': 1024**3, 'tail_running_max': 32,
    'large_low_Bps': 6_000_000_000.0, 'large_recovery_Bps': 8_000_000_000.0,
    'small_low_FPS': 1000.0, 'small_recovery_FPS': 1500.0,
    'revalidation_low_FPS': 1000.0, 'revalidation_recovery_FPS': 1500.0,
    'seed_reading_low_RPS': 100.0, 'seed_reading_recovery_RPS': 200.0,
    'seed_finalizing_low_FPS': 5000.0, 'seed_finalizing_recovery_FPS': 10000.0,
    'seed_importing_low_RPS': 5000.0, 'seed_importing_recovery_RPS': 10000.0,
}
TERMINAL = {'complete', 'complete_with_changes', 'failed', 'stopped', 'paused'}


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def epoch(value):
    if number(value):
        return float(value)
    if isinstance(value, str):
        try:
            parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                return None
            return parsed.timestamp()
        except (ValueError, OverflowError):
            pass
    return None


def _policy(value):
    result = dict(DEFAULT_POLICY)
    unknown = set(value or {}) - set(result) - {'workload'}
    if unknown:
        raise ValueError('unknown performance policy keys: ' + ','.join(sorted(unknown)))
    result.update(value or {})
    if not number(result['window_seconds']) or result['window_seconds'] < 300:
        raise ValueError('performance windows must cover at least 300 seconds')
    for key in ('max_age_seconds', 'max_gap_seconds', 'expected_nodes', 'history_seconds'):
        if not number(result[key]) or result[key] <= 0:
            raise ValueError('policy value must be positive: ' + key)
    for low, target in (('large_low_Bps', 'large_recovery_Bps'),
                        ('small_low_FPS', 'small_recovery_FPS'),
                        ('revalidation_low_FPS', 'revalidation_recovery_FPS'),
                        ('seed_reading_low_RPS', 'seed_reading_recovery_RPS'),
                        ('seed_finalizing_low_FPS', 'seed_finalizing_recovery_FPS'),
                        ('seed_importing_low_RPS', 'seed_importing_recovery_RPS')):
        if not number(result[low]) or not number(result[target]) or not 0 < result[low] <= result[target]:
            raise ValueError('require positive recovery target >= low threshold: ' + low)
    return result


def _samples(container):
    if not isinstance(container, dict):
        return []
    history = container.get('samples', container.get('history', []))
    result = [dict(s) for s in history if isinstance(s, dict)] if isinstance(history, list) else []
    current = dict(container)
    current.pop('samples', None)
    current.pop('history', None)
    if epoch(current.get('sample_time', current.get('updated'))) is not None:
        current.setdefault('sample_time', current.get('updated'))
        result.append(current)
    return result


def _mode_tag(sample):
    mode = sample.get('performance_mode', sample.get('mode'))
    if mode == 'revalidation':
        mode = 'copy_revalidation'
    if mode is None and sample.get('status') in ('enumerating', 'reading_receipts', 'finalizing', 'publishing', 'importing'):
        mode = 'seed_' + sample['status']
    if mode == 'copy' and sample.get('workload') in ('large', 'small'):
        mode += '_' + sample['workload']
    return mode


def _qualified(points, context, since, scope_run=None):
    """Keep only the final continuous suffix with a known matching context."""
    ordered = sorted((p for p in points if epoch(p.get('sample_time', p.get('updated'))) is not None),
                     key=lambda p: epoch(p.get('sample_time', p.get('updated'))))
    result = []
    for original in ordered:
        p = dict(original)
        stamp = epoch(p.get('sample_time', p.get('updated')))
        explicit_mode = _mode_tag(p)
        inferred = stamp >= since
        run = p.get('run_id', scope_run)
        stage = p.get('stage', p.get('current_stage', context['stage'] if inferred else None))
        mode = explicit_mode or (context['mode'] if inferred else None)
        # Stage is deliberately None for seed modes; current metric objects
        # are not allowed to lend their stage to all older untagged history.
        stage_known = ('stage' in p or 'current_stage' in p or inferred or mode and mode.startswith('seed_'))
        if run == context['run_id'] and stage_known and stage == context['stage'] and mode == context['mode']:
            p['sample_time'] = stamp
            if 'nodebytes' not in p and 'copied_bytes' in p:
                p['nodebytes'] = p['copied_bytes']
            if 'indexed_files' not in p and number(p.get('verified_files')) and number(p.get('verified_symlinks')):
                p['indexed_files'] = p['verified_files'] + p['verified_symlinks']
            if mode == 'seed_importing' and 'imported_files' not in p and number(p.get('rows_processed')):
                p['imported_files'] = p['rows_processed']
            result.append(p)
        else:
            result = []
    return result


def _window(points, fields, now, policy, not_before=None):
    points = [p for p in points if all(number(p.get(f)) and p[f] >= 0 for f in fields)]
    unique = {}
    for point in points:
        stamp = point['sample_time']
        if stamp in unique and any(unique[stamp][f] != point[f] for f in fields):
            return {'valid': False, 'reason': 'conflicting_sample', 'timestamp': stamp}
        unique[stamp] = point
    points = [unique[t] for t in sorted(unique)]
    if not points:
        return {'valid': False, 'reason': 'insufficient_samples', 'points': 0}
    last = points[-1]
    age = now - last['sample_time']
    if age < -policy['future_tolerance_seconds']:
        return {'valid': False, 'reason': 'future_sample', 'age_seconds': age}
    if age > policy['max_age_seconds']:
        return {'valid': False, 'reason': 'stale_samples', 'age_seconds': age}
    cutoff = last['sample_time'] - policy['window_seconds']
    anchors = [i for i, p in enumerate(points) if p['sample_time'] <= cutoff]
    if not anchors:
        return {'valid': False, 'reason': 'insufficient_samples', 'points': len(points),
                'available_seconds': last['sample_time'] - points[0]['sample_time']}
    points = points[anchors[-1]:]
    first = points[0]
    duration = last['sample_time'] - first['sample_time']
    gap = max(b['sample_time'] - a['sample_time'] for a, b in zip(points, points[1:]))
    result = {'valid': True, 'start': first['sample_time'], 'end': last['sample_time'],
              'seconds': duration, 'points': len(points), 'max_gap_seconds': gap,
              'age_seconds': max(0, age)}
    if gap > policy['max_gap_seconds']:
        return dict(result, valid=False, reason='sample_gap')
    if not_before is not None and first['sample_time'] < not_before:
        return dict(result, valid=False, reason='recovery_window_not_post_incident')
    if any(b[f] < a[f] for a, b in zip(points, points[1:]) for f in fields):
        return dict(result, valid=False, reason='counter_reset')
    result['delta'] = {f: last[f] - first[f] for f in fields}
    result['rates'] = {f: result['delta'][f] / duration for f in fields}
    return result


def _nodes(nodes, now, policy, run_id):
    reasons, sizes = [], []
    copied = revalidated = 0
    counters_known = True
    if not isinstance(nodes, list) or len(nodes) != policy['expected_nodes']:
        reasons.append('node_count_uncertain')
        nodes = nodes if isinstance(nodes, list) else []
    hosts = [n.get('host') for n in nodes if isinstance(n, dict)]
    if len(hosts) != len(nodes) or any(not isinstance(h, str) for h in hosts) or len(set(hosts)) != len(hosts):
        reasons.append('node_identity_uncertain')
    for node in nodes:
        if not isinstance(node, dict):
            continue
        components = node.get('components') or [node]
        if not isinstance(components, list):
            reasons.append('node_identity_uncertain')
            components = [node]
        for component in components:
            if not isinstance(component, dict):
                reasons.append('node_identity_uncertain')
                continue
            stamp = epoch(component.get('updated', component.get('started')))
            if stamp is None or now - stamp > policy['max_age_seconds']:
                reasons.append('stale_nodes')
            elif stamp - now > policy['future_tolerance_seconds']:
                reasons.append('future_node_timestamp')
            if component.get('ended') or component.get('thread_error') or component.get('tuning_error'):
                reasons.append('unhealthy_nodes')
        if node.get('run_id', run_id) != run_id or node.get('identity_verified') is False:
            reasons.append('node_identity_uncertain')
        counts = node.get('counts')
        if not isinstance(counts, dict) or any(not number(counts.get(k, 0)) or counts.get(k, 0) < 0
                                                for k in ('copied', 'verified', 'skipped')):
            counters_known = False
        else:
            copied += counts.get('copied', 0)
            revalidated += counts.get('verified', 0) + counts.get('skipped', 0)
        active = node.get('active', {})
        for entry in active.values() if isinstance(active, dict) else []:
            if isinstance(entry, dict) and not entry.get('awaiting_receipt') and number(entry.get('size')) and entry['size'] >= 0:
                sizes.append(entry['size'])
    return sorted(set(reasons)), sizes, ({'copied_files': copied, 'revalidated_files': revalidated}
                                        if counters_known else {})


def _node_baselines(nodes):
    result = {}
    for node in nodes if isinstance(nodes, list) else []:
        if not isinstance(node, dict) or not isinstance(node.get('host'), str):
            continue
        components = node.get('components') or [node]
        components = components if isinstance(components, list) else [node]
        # Worker snapshots expose ``started`` and an instance identifier;
        # /proc probes may additionally supply start_ticks. Include all of
        # them so a recycled PID never lends the old process's counters to a
        # recovery window.
        identities = [[str(c.get('lane', i)), c.get('pid'), c.get('start_ticks'),
                       c.get('started'), c.get('instance')]
                      for i, c in enumerate(components) if isinstance(c, dict)]
        counts = node.get('counts') if isinstance(node.get('counts'), dict) else {}
        values = {'counts.' + k: v for k, v in counts.items() if number(v)}
        values.update({k: node[k] for k in ('bytes', 'streamed_bytes') if number(node.get(k))})
        result[node['host']] = {'identities': identities, 'counters': values}
    return result


def _readback_evidence(nodes, now, policy):
    """Optional probe evidence; never infer SHA progress from a file size."""
    result = []
    for node in nodes if isinstance(nodes, list) else []:
        value = node.get('readback_progress', {}) if isinstance(node, dict) else {}
        if not isinstance(value, dict):
            continue
        stamp = epoch(value.get('sample_time'))
        if (stamp is not None and 0 <= now - stamp <= policy['max_age_seconds']
                and number(value.get('fd_count')) and value['fd_count'] >= 1
                and number(value.get('bytes_delta')) and value['bytes_delta'] > 0
                and number(value.get('seconds')) and value['seconds'] > 0):
            result.append({'host': node.get('host'), 'fd_count': value['fd_count'],
                           'Bps': value['bytes_delta'] / value['seconds'], 'sample_time': stamp})
    return result


def _delayed_node_metadata(nodes, now, policy):
    """Describe delayed heartbeat publications, without claiming process death."""
    result = []
    for node in nodes if isinstance(nodes, list) else []:
        if not isinstance(node, dict):
            continue
        components = node.get('components') or [node]
        for component in components if isinstance(components, list) else [node]:
            if not isinstance(component, dict):
                continue
            stamp = epoch(component.get('updated', component.get('started')))
            if stamp is None or now - stamp > policy['max_age_seconds']:
                result.append({'host': node.get('host'), 'lane': component.get('lane'),
                               'pid': component.get('pid'),
                               'age_seconds': now - stamp if stamp is not None else None})
    return result


def evaluate_performance(state, metrics, nodes, seed_progress=None, previous=None, policy=None, *, now, stop=False):
    """Return measurements, a latched incident, diagnostics and next_state.

    Only ``optimize`` with ``armed=True`` authorizes a performance review.
    ``trigger`` retains an existing incident through uncertain/stopped data;
    it never authorizes stopping/restarting copy. No action is performed here.
    """
    if not number(now):
        raise ValueError('now must be a finite epoch supplied by the caller')
    policy = _policy(policy)
    state, metrics = state or {}, metrics or {}
    seed = seed_progress or state.get('seed_progress') or {}
    old = (previous or {}).get('next_state', previous or {})
    old = copy.deepcopy(old)
    run_id, stage = state.get('run_id'), state.get('stage')
    status = state.get('status')
    reasons, diagnostics, evidence = [], [], []
    if not isinstance(run_id, str) or not run_id:
        reasons.append('missing_run_id')
    state_time = epoch(state.get('updated'))
    if state_time is None or now - state_time > policy['max_age_seconds']:
        reasons.append('stale_state')
    elif state_time - now > policy['future_tolerance_seconds']:
        reasons.append('future_state_timestamp')
    node_reasons, sizes, counts = _nodes(nodes, now, policy, run_id)
    reasons.extend(node_reasons)
    current_baselines = _node_baselines(nodes)
    reset_at = old.get('last_reset_at') if old.get('run_id') == run_id else None
    if old.get('run_id') == run_id and not node_reasons:
        for host, baseline in current_baselines.items():
            before = old.get('node_baselines', {}).get(host)
            if before is None:
                continue
            if before.get('identities') != baseline['identities']:
                reasons.append('node_process_changed')
                reset_at = now
            if any(value < before.get('counters', {}).get(key, value)
                   for key, value in baseline['counters'].items()):
                reasons.append('counter_reset')
                reset_at = now
    if stop or state.get('stop') or state.get('STOP') or status in TERMINAL or metrics.get('status') == 'stopped':
        reasons.append('stopped_or_terminal')
    if state.get('training_protection') or state.get('measurement_uncertain') or metrics.get('measurement_uncertain'):
        reasons.append('measurement_uncertain')
    scope_run = metrics.get('run_id')
    control = metrics.get('control')
    if scope_run is None and isinstance(control, str) and control.rstrip('/').rsplit('/', 1)[-1] == '.' + str(run_id):
        scope_run = run_id
    if scope_run is not None and scope_run != run_id:
        reasons.append('metrics_run_mismatch')

    metric_points = _samples(metrics)
    prior_observations = old.get('observations', []) if old.get('run_id') == run_id else []
    if status == 'preparing_resume':
        mode = 'seed_' + str(seed.get('status', 'unknown'))
        stage = None
    elif status in ('scanning', 'copying') and isinstance(stage, str):
        hint = policy.get('workload', state.get('workload', metrics.get('workload')))
        recent = sorted((p for p in metric_points if number(p.get('nodebytes', p.get('copied_bytes')))
                         and number(p.get('streamed_bytes'))), key=lambda p: epoch(p.get('sample_time')) or 0)
        has_copy = (len(recent) > 1 and (recent[-1].get('nodebytes', recent[-1].get('copied_bytes')) >
                    recent[0].get('nodebytes', recent[0].get('copied_bytes'))
                    or recent[-1]['streamed_bytes'] > recent[0]['streamed_bytes']))
        if hint not in ('large', 'small', 'revalidation'):
            if has_copy and len(sizes) >= policy['active_size_min_samples']:
                median = statistics.median(sizes)
                hint = ('large' if median >= policy['large_size_bytes'] else
                        'small' if median <= policy['small_size_bytes'] else None)
            if hint is None and prior_observations and counts:
                first = prior_observations[-1]
                if (counts['copied_files'] == first.get('copied_files') and
                        counts['revalidated_files'] > first.get('revalidated_files', counts['revalidated_files'])
                        and sizes and max(sizes) <= policy['small_size_bytes']):
                    hint = 'revalidation'
        mode = 'copy_' + hint if hint in ('large', 'small', 'revalidation') else 'copy_unclassified'
    else:
        mode = 'waiting_or_metadata'
    context = {'run_id': run_id, 'stage': stage, 'mode': mode}
    same_context = all(old.get(k) == v for k, v in context.items())
    since = old.get('context_since', now) if same_context else now
    explicit_since = epoch(state.get('performance_context_since'))
    if explicit_since is not None:
        since = max(since if same_context else explicit_since, explicit_since)
    incident = copy.deepcopy(old.get('incident', {'active': False}))
    observation = dict(context, sample_time=now, **counts)
    observations = [copy.deepcopy(p) for p in prior_observations
                    if isinstance(p, dict) and epoch(p.get('sample_time')) is not None
                    and epoch(p['sample_time']) >= now - policy['history_seconds']]
    if not node_reasons and counts:
        observations = [p for p in observations if p['sample_time'] != now] + [observation]
    seed_points = _samples(seed)
    if old.get('run_id') == run_id:
        seed_points = copy.deepcopy(old.get('seed_samples', [])) + seed_points
    for p in seed_points:
        if 'run_id' not in p:
            p['run_id'] = seed.get('run_id', run_id)
    compact_seed, seen_seed = [], {}
    for p in seed_points:
        stamp = epoch(p.get('sample_time', p.get('updated')))
        if stamp is None or stamp < now - policy['history_seconds']:
            continue
        compact = {k: p[k] for k in ('run_id', 'stage', 'mode', 'performance_mode', 'status',
                   'processed_receipts', 'receipt_paths_discovered', 'indexed_files',
                   'verified_files', 'verified_symlinks', 'imported_files', 'rows_processed') if k in p}
        compact['sample_time'] = stamp
        key = (str(compact.get('run_id')), str(compact.get('stage')), _mode_tag(compact), stamp)
        prior = seen_seed.setdefault(key, [])
        if compact not in prior:
            compact_seed.append(compact)
            prior.append(compact)
    seed_points = sorted(compact_seed, key=lambda p: p['sample_time'])
    windows, rates = {}, {}
    threshold = target = unit = None
    low = recovered = None

    if mode.startswith('copy_'):
        qualified = _qualified(metric_points, context, since, scope_run)
        if reset_at is not None:
            qualified = [p for p in qualified if p['sample_time'] >= reset_at]
        windows['bytes'] = _window(qualified, ('nodebytes', 'streamed_bytes'), now, policy)
        file_points = _qualified(metric_points + observations, context, since, scope_run)
        if reset_at is not None:
            file_points = [p for p in file_points if p['sample_time'] >= reset_at]
        if mode == 'copy_large':
            window = windows['bytes']
            threshold, target, unit = policy['large_low_Bps'], policy['large_recovery_Bps'], 'B/s'
            if window['valid']:
                rates.update(completed_Bps=window['rates']['nodebytes'], streamed_Bps=window['rates']['streamed_bytes'])
                rates['effective_Bps'] = max(rates['completed_Bps'], rates['streamed_Bps'])
                low, recovered = rates['effective_Bps'] < threshold, rates['effective_Bps'] >= target
                if low and rates['streamed_Bps'] == 0 and sizes and max(sizes) >= policy['inflight_large_bytes']:
                    readback = _readback_evidence(nodes, now, policy)
                    if readback:
                        reasons.append('inflight_readback_progress')
                        evidence.append({'measured_readback': readback})
                        # A short /proc probe proves that verification is
                        # advancing; it cannot prove a sustained healthy
                        # five-minute rate. In particular, a few bytes per
                        # second must not silence diagnosis indefinitely.
                        if sum(p['Bps'] for p in readback) < threshold:
                            diagnostics.append('inspect_low_readback_throughput')
                    else:
                        reasons.append('inflight_unobservable')
                        diagnostics.append('inspect_readback_bytes_cpu_and_file_fds')
        elif mode in ('copy_small', 'copy_revalidation'):
            counter = 'copied_files' if mode == 'copy_small' else 'revalidated_files'
            windows['files'] = window = _window(file_points, (counter,), now, policy)
            prefix = 'small' if mode == 'copy_small' else 'revalidation'
            threshold, target, unit = policy[prefix + '_low_FPS'], policy[prefix + '_recovery_FPS'], 'files/s'
            if window['valid']:
                rates[counter + '_per_second'] = window['rates'][counter]
                low = window['rates'][counter] < threshold
                recovered = window['rates'][counter] >= target
                if (mode == 'copy_revalidation' and low and sizes
                        and max(sizes) >= policy['inflight_large_bytes']):
                    reasons.append('inflight_unobservable')
                    diagnostics.append('inspect_readback_bytes_cpu_and_file_fds')
                if mode == 'copy_small' and windows['bytes']['valid']:
                    byte_rate = max(windows['bytes']['rates'].values())
                    rates['effective_Bps'] = byte_rate
                    if byte_rate >= policy['large_low_Bps']:
                        low = False
                        evidence.append('healthy_byte_throughput_suppresses_small_file_low')
                elif mode == 'copy_small':
                    reasons.append(windows['bytes']['reason'])
        else:
            window = windows['bytes']
            reasons.append('workload_unclassified')
        if not window['valid']:
            reasons.append(window['reason'])
        queue = state.get('queue', {})
        queue_time = epoch(queue.get('sample_time')) if isinstance(queue, dict) else None
        if (queue_time is not None and 0 <= now - queue_time <= policy['max_age_seconds']
                and queue.get('stage') == stage and state.get('scan_complete') is True
                and queue.get('pending') == 0 and number(queue.get('running'))
                and queue['running'] <= policy['tail_running_max']):
            reasons.append('tail_workload_not_cluster_copy')
        elif (queue_time is not None and 0 <= now - queue_time <= policy['max_age_seconds']
              and queue.get('stage') == stage and state.get('scan_complete') is False
              and queue.get('pending') == 0 and queue.get('running') == 0
              and windows['bytes']['valid'] and not any(windows['bytes']['delta'].values())):
            diagnostics.append('inspect_scanner_task_supply')
    elif mode.startswith('seed_'):
        qualified = _qualified(seed_points, context, since, run_id)
        if mode == 'seed_reading_receipts':
            field, threshold, target, unit = ('processed_receipts', policy['seed_reading_low_RPS'],
                                             policy['seed_reading_recovery_RPS'], 'receipts/s')
        elif mode == 'seed_finalizing':
            field, threshold, target, unit = ('indexed_files', policy['seed_finalizing_low_FPS'],
                                             policy['seed_finalizing_recovery_FPS'], 'files/s')
        elif mode == 'seed_importing':
            field, threshold, target, unit = ('imported_files', policy['seed_importing_low_RPS'],
                                             policy['seed_importing_recovery_RPS'], 'rows/s')
            # Both producer versions are supported, but conflicting aliases
            # cannot be silently interpreted as a reliable rate.
            if any(number(p.get('imported_files')) and number(p.get('rows_processed'))
                   and p['imported_files'] != p['rows_processed'] for p in qualified):
                reasons.append('seed_counter_conflict')
                diagnostics.append('inspect_seed_counter_identity')
        else:
            field = 'receipt_paths_discovered'
            reasons.append('seed_phase_has_no_rate_threshold')
        windows['seed'] = window = _window(qualified, (field,), now, policy)
        if window['valid']:
            rates[field + '_per_second'] = window['rates'][field]
            if threshold is not None:
                low, recovered = window['rates'][field] < threshold, window['rates'][field] >= target
            elif window['delta'][field] == 0:
                diagnostics.append('progress_unobservable')
        else:
            reasons.append(window['reason'])
    else:
        window = {'valid': False, 'reason': 'no_active_rate_workload'}
        reasons.append('no_active_rate_workload')

    if 'stale_nodes' in reasons:
        # Fresh metrics may still prove a non-zero lower-bound byte rate.
        # Delayed heartbeats make the total uncertain, rather than zero. Ask
        # for a metadata/process probe immediately instead of suppressing all
        # diagnosis until another full five-minute period has elapsed.
        diagnostics.append('inspect_node_metadata_latency')
        evidence.append({'delayed_node_metadata': _delayed_node_metadata(nodes, now, policy),
                         'rate_interpretation': 'incomplete_metadata_not_zero_throughput'})

    reasons = sorted(set(reasons))
    armed = not reasons and low is not None
    # An oscillating/unclassified workload must not reset the telemetry timer
    # forever. A genuinely usable window clears it below; changing workload
    # alone does not establish usable evidence.
    first_awaiting = old.get('first_awaiting_since', now) if old.get('run_id') == run_id else now
    if not number(first_awaiting):
        first_awaiting = now
    if armed:
        first_awaiting = None
        if low:
            if not incident.get('active'):
                incident = dict(active=True, triggered_at=window['end'], mode=mode, run_id=run_id,
                                stage=stage, trigger_rate=copy.deepcopy(rates), threshold=threshold)
            incident['last_low_at'] = window['end']
        elif recovered and incident.get('active'):
            floor = max(incident.get('triggered_at', now), old.get('last_stop_at') or float('-inf'),
                        old.get('last_disruption_at') or float('-inf'))
            if window['start'] >= floor:
                incident.update(active=False, recovered_at=window['end'], recovery_mode=mode,
                                recovery_rate=copy.deepcopy(rates), recovery_target=target)
            else:
                evidence.append('recovery_requires_full_post_incident_window')
    elif (now - first_awaiting >= policy['window_seconds'] and set(reasons) & {
            'insufficient_samples', 'sample_gap', 'stale_samples', 'stale_nodes', 'stale_state',
            'missing_run_id', 'metrics_run_mismatch', 'counter_reset', 'measurement_uncertain',
            'conflicting_sample', 'future_sample', 'future_node_timestamp', 'future_state_timestamp',
            'node_identity_uncertain', 'node_count_uncertain', 'node_process_changed', 'workload_unclassified',
            'seed_counter_conflict'}
            and 'stopped_or_terminal' not in reasons):
        diagnostics.append('telemetry_insufficient')
    if 'stopped_or_terminal' in reasons:
        diagnostics = []
    # Metric-history gaps and insufficient history already invalidate their
    # own rolling windows. Directly observed stale/changed identities also
    # require a full later window before clearing an existing incident.
    disruptions = {'stale_nodes', 'stale_state', 'stale_samples', 'missing_run_id',
                   'metrics_run_mismatch', 'counter_reset', 'measurement_uncertain',
                   'conflicting_sample', 'future_sample', 'future_node_timestamp',
                   'future_state_timestamp', 'node_identity_uncertain',
                   'node_count_uncertain', 'node_process_changed', 'seed_counter_conflict'}
    disruption_at = (now if set(reasons) & disruptions else old.get('last_disruption_at'))
    next_state = dict(context, context_since=since, observations=observations, seed_samples=seed_points,
                      incident=incident, first_awaiting_since=first_awaiting,
                      node_baselines=current_baselines if not node_reasons else old.get('node_baselines', {}),
                      last_reset_at=reset_at, last_disruption_at=disruption_at,
                      last_stop_at=now if 'stopped_or_terminal' in reasons else old.get('last_stop_at'),
                      last_invalid_at=now if not armed else old.get('last_invalid_at'), last_evaluated_at=now)
    return {'version': 1, **context, 'evaluated_at': now, 'armed': armed, 'reason_codes': reasons,
            'window': window, 'windows': windows, 'rates': rates,
            'low_threshold': threshold, 'recovery_target': target, 'unit': unit,
            'below_threshold': low if armed else None, 'recovery_target_met': recovered if armed else None,
            'trigger': bool(incident.get('active')), 'optimize': bool(armed and incident.get('active')),
            'incident': copy.deepcopy(incident), 'diagnostics': sorted(set(diagnostics)),
            'diagnostic_trigger': bool(diagnostics), 'evidence': evidence,
            'constraints': {'stop_copy_allowed': False, 'restart_copy_allowed': False,
                            'deploy_allowed': False, 'evaluation_only': True},
            'policy': policy, 'next_state': next_state}
