"""Summarize saved inspections without contacting or changing the live run."""
import argparse
import datetime
import json
from pathlib import Path

from migration_io import read_json, write_json


def age_at(checked_at, value):
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    return checked_at - value


def summarize(path, previous=None, profile=None, proc_profile=None):
    current = read_json(path)
    state, metrics = current['state'], current['metrics']
    checked = current['checked_at']
    result = {
        'inspection': str(path), 'checked_at': checked,
        'control': metrics.get('control'), 'status': state.get('status'),
        'stage': state.get('stage'), 'recovery_warm': state.get('recovery_warm'),
        'scan_complete': state.get('scan_complete'), 'stop': current['stop'],
        'state_age_seconds': age_at(checked, state.get('updated')),
        'metrics_sample_age_seconds': age_at(checked, metrics.get('sample_time')),
        'nodes_seen': metrics.get('nodes_seen'), 'metrics_warnings': metrics.get('warnings'),
        'target_filter': state.get('target_filter'),
        'copied_bytes': metrics.get('copied_bytes'),
        'streamed_bytes': metrics.get('streamed_bytes'),
        'verified_files': metrics.get('verified_files'),
        'queue': current['queue'], 'claim_counts': current['claim_counts'],
        'all_stage_queues': current.get('all_stage_queues'),
        'issue_census': current.get('issue_census'),
        'draining_hosts': current['broker_control'].get('draining_hosts'),
        'duplicate_leased_owners': current['duplicate_leased_owners'],
        'training_state': current['training'].get('state'),
        'training_updated': current['training'].get('updated'),
        'processes': {role: {key: proc.get(key) for key in
                             ('pid', 'start_ticks', 'state', 'threads', 'error')}
                      for role, proc in current['processes'].items()},
        'helpers': current['helpers'], 'seed': current.get('seed'),
        'performance': current.get('performance'),
        'performance_lanes': current.get('performance_lanes'),
        'nonempty_issue_files': [item for item in current['issues'] if item.get('bytes')],
        'pipeline_stages': list(current['pipeline'].get('stages', {})),
        'directory_job_names': list(current['pipeline'].get('directory_jobs', {})),
        'inspection_warnings': current.get('warnings'),
        'notes': [
            'Warm counters and an interim state are not final acceptance of the four scopes.',
            'Endpoint and proc byte rates are logical process/application I/O, not physical storage bandwidth.',
            'Different file mixtures and intervals do not establish a controlled speed comparison.',
            'This program reads saved evidence only and performs no live deployment, drain, or restart.',
        ],
    }
    if 'node_components' in current:
        result['node_components'] = [dict(component,
            heartbeat_age_seconds=age_at(checked, component.get('updated')))
            for component in current['node_components']]
        result['node_component_count'] = len(result['node_components'])
    if previous:
        old = read_json(previous)
        if old['metrics'].get('control') != metrics.get('control'):
            raise ValueError('cannot compare endpoint counters across different controls')
        interval = metrics['sample_time'] - old['metrics']['sample_time']
        deltas = {key: metrics[key] - old['metrics'][key]
                  for key in ('copied_bytes', 'streamed_bytes', 'verified_files')}
        result.update(previous_inspection=str(previous), endpoint_window_seconds=interval,
                      endpoint_counter_delta=deltas,
                      endpoint_rates=({key: value / interval for key, value in deltas.items()}
                                      if interval > 0 and all(x >= 0 for x in deltas.values())
                                      else None),
                      role_identity_changes=[role for role, proc in current['processes'].items()
                          if role in old['processes'] and any(
                              proc.get(key) != old['processes'][role].get(key)
                              for key in ('pid', 'start_ticks', 'argv'))])
    if profile:
        evidence = read_json(profile)
        if evidence.get('control') != metrics.get('control'):
            raise ValueError('process profile belongs to a different control')
        total = evidence['aggregate']['total']
        result['profile'] = {
            'path': str(profile), 'started_at': evidence['started_at'],
            'finished_at': evidence['finished_at'], 'errors': evidence['errors'],
            'valid_processes': total['valid_rate_processes'],
            'active': total['active'], 'threads': total['threads'],
            'unhealthy_physical_nodes': total['unhealthy_physical_nodes'],
            'logical_read_bytes_per_second': total['io_bytes_per_second']['rchar'],
            'logical_write_bytes_per_second': total['io_bytes_per_second']['wchar'],
            'counter_delta': total['counter_delta'],
        }
    if proc_profile:
        evidence = read_json(proc_profile)
        if evidence.get('control') != metrics.get('control'):
            raise ValueError('proc-only profile belongs to a different control')
        result['proc_only_profile'] = {'path': str(proc_profile), **{
            key: evidence.get(key) for key in
            ('started_epoch', 'finished_epoch', 'valid_processes', 'rate_complete',
             'baseline', 'argv_baseline', 'full_argv_bound_to_baseline',
             'logical_io_bytes_per_second', 'errors', 'no_nfs_json_or_payload_reads',
             'mutation_performed')}}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inspection', type=Path)
    parser.add_argument('--previous', type=Path)
    parser.add_argument('--profile', type=Path)
    parser.add_argument('--proc-profile', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = summarize(args.inspection, args.previous, args.profile, args.proc_profile)
    write_json(args.output, result)
    print(json.dumps({key: result.get(key) for key in (
        'status', 'stage', 'stop', 'state_age_seconds', 'metrics_sample_age_seconds',
        'nodes_seen', 'queue', 'draining_hosts', 'duplicate_leased_owners',
        'training_state', 'endpoint_window_seconds', 'endpoint_rates', 'role_identity_changes')}
        | {'saved': str(args.output), 'nonempty_issue_files': len(result['nonempty_issue_files'])}))


if __name__ == '__main__':
    main()
