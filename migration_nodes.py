"""Read primary/supplement snapshots as one record per physical copy node.

Each daemon remains the sole publisher of its own immutable node stream.
Totals sum disjoint owner lanes; liveness keeps each component visible.
"""
import collections
import pathlib

from migration_io import read_json


SUM_FIELDS = ('bytes', 'streamed_bytes', 'verified_bytes', 'verified_files',
              'completed_batches', 'broker_retries')
_LAST_GOOD = {}


def _sum_mapping(values):
    result = collections.Counter()
    for value in values:
        if isinstance(value, dict):
            result.update({key: count for key, count in value.items()
                           if isinstance(count, (int, float)) and not isinstance(count, bool)})
    return dict(result)


def merge_node(primary, supplement=None):
    if supplement is None:
        result = dict(primary)
        result['components'] = [_component(primary, 'primary')]
        return result
    if primary.get('host') != supplement.get('host'):
        raise ValueError('primary and supplement physical host mismatch')
    if primary.get('pid') == supplement.get('pid'):
        raise ValueError('two node lanes must have distinct process IDs')
    result = dict(primary)
    components = (primary, supplement)
    result['components'] = [_component(primary, 'primary'), _component(supplement, 'extra64')]
    result['pids'] = [value.get('pid') for value in components]
    result['threads'] = sum(value.get('threads', 0) for value in components)
    result['io_bytes_per_second'] = sum(value.get('io_bytes_per_second', 0) for value in components)
    for field in SUM_FIELDS:
        result[field] = sum(value.get(field, 0) for value in components)
    result['counts'] = _sum_mapping(value.get('counts') for value in components)
    active = {}
    for lane, value in zip(('primary', 'extra64'), components):
        for index, entry in value.get('active', {}).items():
            # Lane prefixes also safely handle historical supplement snapshots
            # that exposed local indices rather than physical owner offsets.
            active[lane + ':' + str(index)] = dict(entry)
    result['active'] = active
    stages = {}
    for value in components:
        for stage, counters in value.get('stages', {}).items():
            target = stages.setdefault(stage, {})
            for key, count in counters.items():
                if key == 'counts':
                    target[key] = _sum_mapping((target.get(key), count))
                elif isinstance(count, (int, float)) and not isinstance(count, bool):
                    target[key] = target.get(key, 0) + count
    result['stages'] = stages
    # Never hide a stalled/failed lane behind another lane's fresh heartbeat.
    stamps = [value.get('updated', value.get('started')) for value in components]
    stamps = [stamp for stamp in stamps if stamp]
    if stamps:
        result['updated'] = min(stamps)
        result['latest_component_updated'] = max(stamps)
    failures = [value.get('last_error') for value in components if value.get('last_error')]
    result['last_error'] = '\n'.join(failures) if failures else None
    result['thread_error'] = any(value.get('thread_error') for value in components)
    ended = [value.get('ended') for value in components if value.get('ended')]
    result['ended'] = min(ended) if ended else None
    tuning = [value.get('tuning_error') for value in components if value.get('tuning_error')]
    if tuning:
        result['tuning_error'] = '; '.join(tuning)
    else:
        result.pop('tuning_error', None)
    return result


def _component(value, lane):
    result = {key: value.get(key) for key in
              ('host', 'pid', 'started', 'updated', 'ended', 'threads', 'io_bytes_per_second',
               'thread_error', 'last_error', 'tuning_error', 'instance', 'inspection_warning')}
    result['lane'] = lane
    result['active_threads'] = len(value.get('active', {}))
    return result


def load_nodes(control, warnings=None):
    """Return physical nodes; missing supplement is valid during deployment."""
    control = pathlib.Path(control)
    primary = {}
    supplemental = {}
    for folder, target in (('nodes', primary), ('nodes-extra', supplemental)):
        for path in (control / folder).glob('*.json'):
            try:
                value = read_json(path)
                if not isinstance(value, dict) or not isinstance(value.get('host'), str):
                    raise ValueError('invalid node snapshot')
                if value['host'] in target:
                    raise ValueError('duplicate physical host within lane')
                target[value['host']] = value
                _LAST_GOOD[str(path)] = value
            except (OSError, ValueError) as exc:
                detail = str(path) + ': ' + str(exc)
                if warnings is not None:
                    warnings.append(detail)
                previous = _LAST_GOOD.get(str(path))
                if previous is not None:
                    value = dict(previous)
                    value['inspection_warning'] = detail
                    target[value['host']] = value
                else:
                    target[path.stem] = {
                        'host': path.stem, 'pid': None, 'threads': 0,
                        'started': '1970-01-01T00:00:00+00:00',
                        'updated': '1970-01-01T00:00:00+00:00',
                        'thread_error': True, 'last_error': 'existing node stream unreadable: ' + detail,
                        'inspection_warning': detail, 'active': {}}
    result = []
    for host, value in sorted(primary.items()):
        result.append(merge_node(value, supplemental.pop(host, None)))
    # A supplement without the primary's durable stream is an unhealthy node,
    # rather than a silent disappearance of outstanding auxiliary leases.
    for host, value in sorted(supplemental.items()):
        node = dict(value)
        node['components'] = [_component(value, 'extra64')]
        node['thread_error'] = True
        node['last_error'] = 'primary node stream missing for live supplement'
        result.append(node)
    return result
