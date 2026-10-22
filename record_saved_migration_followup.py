"""Record a normal, still-open incident followup from saved evidence only.

No SSH, live control access, signals or runtime changes. Any changed safety or
incident condition raises for separate review instead of manufacturing normal
status or recovery. Local JSON history is versioned through migration_io.
"""
import argparse
import datetime
import json
from pathlib import Path

from migration_io import read_json, write_json


BASE = Path(__file__).resolve().parent
PROC_BASELINE = BASE / 'cache_age_canary_profile_20261021T2003_after.json'
ARGV_BASELINE = BASE / 'cache_age_canary_proc_20261021T2003_after.json'
READY_EPOCH = 1792613514.037288


def require(value, reason):
    if not value:
        raise ValueError(reason + '; inspect the actual evidence separately')


def record(summary_path, canary_path):
    summary_path, canary_path = Path(summary_path), Path(canary_path)
    active = read_json(BASE / 'active_migration.json')
    s = read_json(summary_path)
    profile = read_json(Path(s['proc_only_profile']['path']))
    lanes = read_json(canary_path)
    baseline = read_json(PROC_BASELINE)
    require(s['control'] == profile['control'] == lanes['control'] == active['control'], 'Control mismatch')
    require(profile['job'] == lanes['job'] == active['job'], 'Cluster mismatch')
    require(Path(profile['baseline']).resolve() == PROC_BASELINE, 'Unexpected worker identity baseline')
    require(profile.get('full_argv_bound_to_baseline') is True
            and Path(profile.get('argv_baseline', '')).resolve() == ARGV_BASELINE,
            'Full worker argv was not checked against the authorized post-canary evidence')
    require(s['recovery_warm'] is True and s['pipeline_stages'] == []
            and s['status'] not in ('complete', 'complete_with_changes', 'failed', 'stopped'),
            'Warm/formal or terminal stage changed; review the phase transition')
    require(s['stop'] is False and lanes['stop'] is False, 'STOP changed or missing')
    require(s['training_state'] == 'watching', 'Training protection changed')
    age = s['checked_at'] - datetime.datetime.fromisoformat(s['training_updated']).timestamp()
    require(0 <= age < 90, 'Training protection observation is not fresh')
    require(s['draining_hosts'] == [] and s['duplicate_leased_owners'] == []
            and s['nonempty_issue_files'] == [], 'Drain, duplicate owner or nonempty issue requires review')
    require(s['inspection_warnings'] == ['inventory.json: FileNotFoundError'], 'Inspection warnings changed')
    require(s['nodes_seen'] == 12 and len(s['node_components']) == 24, 'Incomplete worker census')
    require(sum(x['threads'] for x in s['node_components']) == 1536, 'Copy thread budget changed')
    require(sum(x['io_bytes_per_second'] for x in s['node_components']) == 239966445568, 'I/O allowance changed')
    require(all(not x.get(key) for x in s['node_components'] for key in
                ('ended', 'thread_error', 'last_error', 'tuning_error', 'inspection_warning')), 'Worker error or exit')
    require(profile['valid_processes'] == 24 and profile['rate_complete'] is True
            and profile['errors'] == [], 'Actual worker identities or process observation incomplete')
    require(profile['no_nfs_json_or_payload_reads'] is True and profile['mutation_performed'] is False,
            'Unexpected process observer type or mutation')
    require(s['role_identity_changes'] == [], 'Running role identity changed')
    for role, (pid, start) in {'producer': (1824893, '500686447'),
                              'broker': (166974, '500376938'), 'metrics': (166975, '500376939'),
                              'performance': (982825, '500522783')}.items():
        require(s['processes'][role]['pid'] == pid and str(s['processes'][role]['start_ticks']) == start,
                role + ' identity changed')
    require(s['processes']['producer']['threads'] == 385, 'Producer thread census changed')
    require(s['all_stage_queues']['complete'] is True and s['all_stage_queues']['errors'] == []
            and s['issue_census']['complete'] is True and s['issue_census']['errors'] == [], 'Queue or issue census incomplete')
    observer = s['performance_lanes']['copy']
    require(observer['incident']['active'] is True, 'Observer incident changed; evaluate recovery explicitly')
    node = next(x for x in baseline['nodes'] if x['node'] == 'worker-5')
    previous_counter = node['snapshots_after']['primary']['counters']['broker_retries']
    require(previous_counter == 76706, 'Canary baseline counter changed')
    require(lanes['lanes']['primary']['identity']['pid'] == 1881537
            and str(lanes['lanes']['primary']['identity']['start_ticks']) == '4105099', 'Canary identity changed')
    current_counter = lanes['lanes']['primary']['node']['broker_retries']
    require(type(current_counter) is int and current_counter >= previous_counter, 'Canary counter reset or missing')
    floor = max(READY_EPOCH, observer['incident']['triggered_at'],
                observer.get('last_disruption_at') or 0, observer.get('last_stop_at') or 0)
    window = observer.get('window') or {}
    full = bool(observer['armed'] is True and window.get('valid') is True
                and isinstance(window.get('start'), (int, float))
                and isinstance(window.get('end'), (int, float))
                and window['end'] - window['start'] >= 300 and window['start'] >= floor)
    followup = {'inspection': str(summary_path), 'checked_at': s['checked_at'], 'observer': observer,
                'proc_profile': s['proc_only_profile']['path'], 'canary_lanes': str(canary_path),
                'full_argv_bound_to_baseline': True, 'argv_baseline': str(ARGV_BASELINE),
                'lane_counter_deltas': lanes['comparison_to_previous'],
                'canary_extra_retries_since_post_change_baseline': current_counter - previous_counter,
                'current_control': {key: s[key] for key in
                    ('state_age_seconds', 'metrics_sample_age_seconds', 'queue', 'claim_counts',
                     'draining_hosts', 'training_state', 'target_filter', 'copied_bytes', 'streamed_bytes')},
                'training_health_age_seconds': age, 'all_stage_queues': s['all_stage_queues'],
                'issue_census': s['issue_census'], 'endpoint_window_seconds': s['endpoint_window_seconds'],
                'endpoint_rates': s['endpoint_rates'], 'post_disruption_full_window': full,
                'required_recovery_window_start': floor, 'recovery_verified': False,
                'runtime_mutation_performed': False,
                'interpretation': 'Saved evidence confirms continuing work,24 authorized processes with unchanged allowances, fresh training watching, STOP false, no drain, duplicate lease or nonempty issues. Raw observer mode/window and counters are retained. The real incident remains active; a complete post-disruption window alone is not recovery. Short logical process rates, mixed endpoints and different file mixtures do not establish physical throughput or controlled speed gains. No running role changed.'}
    incident_path = BASE / 'performance_incident_20261021T1933.json'
    incident = read_json(incident_path)
    episode_path = BASE / 'cache_age_episode_20261021T2003.json'
    episode = read_json(episode_path)
    require(incident['control'] == episode['control'] == active['control'],
            'Incident or canary episode belongs to another control')
    existing = [item for item in incident['followups'] if item.get('inspection') == str(summary_path)]
    require(not existing or all(item['checked_at'] == s['checked_at'] for item in existing),
            'Inspection path reused with different evidence')
    if not existing:
        incident['followups'].append(followup)
        write_json(incident_path, incident)
    episode.update(last_observation=str(summary_path), last_observed_at=s['checked_at'],
                   post_disruption_full_window=full, required_recovery_window_start=floor,
                   observer_mode=observer['mode'], observer_incident_active=True,
                   latest_lane_progress=lanes['comparison_to_previous'],
                   canary_additional_broker_retries=current_counter - previous_counter,
                   latest_canary_broker_retries_delta=lanes['comparison_to_previous']['primary']['deltas']['broker_retries'],
                   canary_retry_counter_baseline=previous_counter, canary_retry_counter_current=current_counter,
                   canary_retry_baseline=str(PROC_BASELINE), recovery_verified=False)
    write_json(episode_path, episode)
    return {'saved_followup': str(summary_path), 'warm': s['recovery_warm'],
            'checked': (s.get('target_filter') or {}).get('checked'),
            'copied_TB': s['copied_bytes'] / 1e12, 'observer_mode': observer['mode'],
            'post_disruption_full_window': full, 'recovery_verified': False,
            'canary_extra_retries_total': current_counter - previous_counter,
            'active': sum(x['active_threads'] for x in s['node_components']),
            'runtime_changed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('summary', type=Path)
    parser.add_argument('--canary', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(record(args.summary, args.canary)))


if __name__ == '__main__':
    main()
