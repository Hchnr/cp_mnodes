#!/usr/bin/env python3
"""Paced, static distributed metadata inventory. Never reads source file data.

prepare_inventory(source, control, nodes=12, rate=100) partitions each priority
tree and the remainder at two directory levels. run_node(control, index, rate)
walks assigned units with one thread. aggregate_inventory(control) combines
completed immutable unit results without scanning source, queues or manifests.

files counts regular paths; bytes is their logical size, including every
hardlinked path. allocated_bytes is per-path allocation; unique_allocated_bytes
deduplicates regular inodes, including hardlinks across nodes. Symlinks are
counted separately and never followed. Any observation error makes exact false.
This is a live inventory, not a filesystem snapshot.
"""

import argparse
import collections
import datetime
import errno
import hashlib
import json
import os
import pathlib
import stat
import time


SOURCE = '/share/project/eai_pwm'
PRIORITY = {1: 'home/hcr', 2: 'repos/cosmos-framework-pwm'}
COUNTERS = ('files', 'bytes', 'allocated_bytes', 'allocated_bytes_nonhardlink',
            'dirs', 'symlinks', 'unsupported', 'errors', 'hardlink_files')


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def read_json(path):
    try:
        from migration_io import read_json as reader
    except ImportError:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return reader(path)


def write_json(path, value):
    from migration_io import write_json as writer
    return writer(pathlib.Path(path), value)


class InventoryStopped(RuntimeError):
    pass


class Pace:
    def __init__(self, rate, control=None):
        if not 0 < rate <= 100:
            raise ValueError('inventory rate must be > 0 and <= 100 entries/s/node')
        self.period = 1.0 / rate
        self.next = time.monotonic()
        self.control = pathlib.Path(control) if control else None
        self.last_stop_check = 0

    def entry(self):
        tick = time.monotonic()
        if self.control and tick - self.last_stop_check >= 1:
            self.last_stop_check = tick
            if (self.control / 'STOP').exists() or (self.control / 'inventory' / 'STOP').exists():
                raise InventoryStopped('inventory STOP requested')
        if tick < self.next:
            time.sleep(self.next - tick)
        self.next = max(self.next, time.monotonic()) + self.period


def components(rel):
    if rel == '':
        return []
    if not isinstance(rel, str) or os.path.isabs(rel) or '\x00' in rel:
        raise ValueError('invalid relative path')
    parts = rel.split('/')
    if any(x in ('', '.', '..') for x in parts):
        raise ValueError('unsafe relative path')
    return parts


def open_directory(root, rel=''):
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, 'O_CLOEXEC', 0)
    fd = open_directory_at('/', flags)
    try:
        for part in [x for x in os.path.abspath(root).split('/') if x] + components(rel):
            child = open_directory_at(part, flags, fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def open_directory_at(name, flags=None, parent=None):
    if flags is None:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, 'O_CLOEXEC', 0)
    noatime = getattr(os, 'O_NOATIME', 0)
    try:
        return os.open(name, flags | noatime, dir_fd=parent)
    except OSError as exc:
        if not noatime or exc.errno not in (errno.EPERM, errno.EACCES, errno.EINVAL):
            raise
        return os.open(name, flags, dir_fd=parent)


def empty_stats():
    return {key: 0 for key in COUNTERS}


def prepare_inventory(source, control, nodes=12, rate=100):
    """Enumerate only two levels per phase, then publish disjoint static units."""
    if nodes < 1:
        raise ValueError('nodes must be positive')
    pace = Pace(rate, control)
    source = os.path.abspath(source)
    if os.path.commonpath((os.path.realpath(source), os.path.realpath(control))) == os.path.realpath(source):
        raise ValueError('inventory control must be outside the source tree')
    root = pathlib.Path(control) / 'inventory'
    for name in ('assignments', 'units', 'nodes'):
        (root / name).mkdir(parents=True, exist_ok=True)
    units, direct, errors = [], collections.defaultdict(list), []

    def issue(rel, exc):
        errors.append({'rel': rel, 'detail': str(exc), 'errno': getattr(exc, 'errno', None)})

    def unit(phase, kind, rel='', entries=None):
        identity = json.dumps([phase, kind, rel, entries], ensure_ascii=True, separators=(',', ':'))
        uid = hashlib.sha256(identity.encode()).hexdigest()[:24]
        record = {'id': uid, 'phase': phase, 'kind': kind, 'rel': rel}
        if entries is not None:
            record['entries'] = entries
        # Hash assignment is stable; no cross-node claim or shared lock needed.
        record['node_index'] = int(uid, 16) % nodes
        units.append(record)

    def split(rel, phase, depth):
        fd = None
        try:
            pace.entry()
            fd = open_directory(source, rel)
            direct[phase].append({'rel': rel, 'directory_only': True})
            with os.scandir(fd) as it:
                for child in it:
                    child_rel = rel + '/' + child.name if rel else child.name
                    if phase == 3 and child_rel in PRIORITY.values():
                        continue
                    pace.entry()
                    try:
                        st = child.stat(follow_symlinks=False)
                        if stat.S_ISDIR(st.st_mode):
                            if depth > 1:
                                split(child_rel, phase, depth - 1)
                            else:
                                unit(phase, 'tree', child_rel)
                        else:
                            direct[phase].append({'rel': child_rel, 'directory_only': False})
                    except OSError as exc:
                        issue(child_rel, exc)
        except OSError as exc:
            issue(rel, exc)
        finally:
            if fd is not None:
                os.close(fd)

    try:
        for phase, rel in PRIORITY.items():
            split(rel, phase, 2)
        split('', 3, 2)
    except InventoryStopped:
        stopped = {'status': 'stopped', 'complete': False, 'exact': False,
                   'stopped': now(), 'unit_count': len(units), 'errors': errors}
        write_json(root / 'prepare-state.json', stopped)
        return stopped
    for phase, entries in direct.items():
        for offset in range(0, len(entries), 256):
            unit(phase, 'entries', entries=entries[offset:offset + 256])
    plan = {'version': 1, 'source': source, 'nodes': nodes, 'rate': rate,
            'created': now(), 'units': units, 'errors': errors,
            'exact': not errors, 'unit_count': len(units)}
    for index in range(nodes):
        write_json(root / 'assignments' / (str(index) + '.json'),
                   {'source': source, 'node_index': index,
                    'units': [u for u in units if u['node_index'] == index]})
    write_json(root / 'plan.json', plan)
    return {'unit_count': len(units), 'nodes': nodes, 'exact': not errors,
            'errors': errors, 'units_per_node': dict(collections.Counter(u['node_index'] for u in units))}


def inventory_unit(source, unit, pace, progress=None):
    stats, links, issues = empty_stats(), {}, []
    last_report = [time.monotonic()]

    def issue(rel, exc):
        stats['errors'] += 1
        if len(issues) < 100:
            issues.append({'rel': rel, 'detail': str(exc), 'errno': getattr(exc, 'errno', None)})

    def record(rel, st):
        if stat.S_ISREG(st.st_mode):
            stats['files'] += 1
            stats['bytes'] += st.st_size
            allocation = getattr(st, 'st_blocks', 0) * 512
            stats['allocated_bytes'] += allocation
            if st.st_nlink > 1:
                stats['hardlink_files'] += 1
                links[str(st.st_ino)] = allocation
            else:
                stats['allocated_bytes_nonhardlink'] += allocation
        elif stat.S_ISDIR(st.st_mode):
            stats['dirs'] += 1
        elif stat.S_ISLNK(st.st_mode):
            stats['symlinks'] += 1
        else:
            stats['unsupported'] += 1
        if progress and time.monotonic() - last_report[0] >= 5:
            progress(dict(stats))
            last_report[0] = time.monotonic()

    def walk(rel):
        frames = []

        def push(fd, name):
            try:
                frames.append((fd, os.scandir(fd), name))
            except OSError as exc:
                os.close(fd)
                issue(name, exc)

        try:
            pace.entry()
            fd = open_directory(source, rel)
            record(rel, os.fstat(fd))
            push(fd, rel)
            while frames:
                fd, iterator, parent = frames[-1]
                try:
                    child = next(iterator)
                except StopIteration:
                    iterator.close(); os.close(fd); frames.pop()
                    continue
                except OSError as exc:
                    issue(parent, exc)
                    iterator.close(); os.close(fd); frames.pop()
                    continue
                name = parent + '/' + child.name if parent else child.name
                pace.entry()
                try:
                    st = child.stat(follow_symlinks=False)
                    if stat.S_ISDIR(st.st_mode):
                        child_fd = open_directory_at(child.name, parent=fd)
                        actual = os.fstat(child_fd)
                        if (actual.st_dev, actual.st_ino) != (st.st_dev, st.st_ino):
                            os.close(child_fd)
                            raise OSError('directory changed while opening')
                        record(name, actual)
                        push(child_fd, name)
                    else:
                        record(name, st)
                except OSError as exc:
                    issue(name, exc)
        except OSError as exc:
            issue(rel, exc)
        finally:
            for fd, iterator, _ in reversed(frames):
                iterator.close(); os.close(fd)

    if unit['kind'] == 'tree':
        walk(unit['rel'])
    else:
        for entry in unit['entries']:
            pace.entry()
            rel = entry['rel']; parts = components(rel); fd = None
            try:
                fd = open_directory(source, '/'.join(parts[:-1]))
                st = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False) if parts else os.fstat(fd)
                if stat.S_ISDIR(st.st_mode) != entry['directory_only']:
                    raise OSError('entry type changed since partitioning')
                record(rel, st)
            except OSError as exc:
                issue(rel, exc)
            finally:
                if fd is not None:
                    os.close(fd)
    return {'unit_id': unit['id'], 'phase': unit['phase'], 'stats': stats,
            'hardlink_allocations': links, 'unique_allocated_bytes':
            stats['allocated_bytes_nonhardlink'] + sum(links.values()),
            'errors': issues, 'exact': stats['errors'] == 0, 'complete': True, 'finished': now()}


def run_node(control, node_index, rate=100):
    root = pathlib.Path(control) / 'inventory'
    plan = read_json(root / 'assignments' / (str(node_index) + '.json'))
    pace = Pace(rate, control)
    phases = {str(p): empty_stats() for p in (1, 2, 3)}
    state = {'node_index': node_index, 'started': now(), 'completed_units': 0,
             'assigned_units': len(plan['units']), 'phases': phases, 'complete': False,
             'rate_limit_entries_per_second': rate}

    def publish(current=None):
        state.update(updated=now(), current_unit=current)
        write_json(root / 'nodes' / (str(node_index) + '.json'), state)

    publish()
    try:
        pace.entry()  # Honour STOP even when this node has no assigned units.
        for unit in plan['units']:
            publish({'id': unit['id'], 'phase': unit['phase'], 'rel': unit['rel'], 'stats': empty_stats()})
            def ongoing(stats):
                publish({'id': unit['id'], 'phase': unit['phase'], 'rel': unit['rel'], 'stats': stats})
            result = inventory_unit(plan['source'], unit, pace, ongoing)
            result['node_index'] = node_index
            write_json(root / 'units' / (unit['id'] + '.json'), result)
            for key in COUNTERS:
                phases[str(unit['phase'])][key] += result['stats'][key]
            state['completed_units'] += 1
            publish()
    except InventoryStopped:
        state.update(status='stopped', complete=False, exact=False, stopped=now())
        publish(state.get('current_unit'))
        return state
    state.update(complete=True, exact=all(p['errors'] == 0 for p in phases.values()), finished=now())
    publish()
    return state


def aggregate_inventory(control, cache=None, cancel_event=None):
    root = pathlib.Path(control) / 'inventory'
    plan = read_json(root / 'plan.json')
    def cancelled():
        if cancel_event is not None and cancel_event.is_set():
            raise InventoryStopped('inventory aggregation cancelled')
    cancelled()
    # Completed unit files are immutable for this plan. Never probe every
    # missing planned result: read_json retries would serialize long delays.
    cache = {} if cache is None else cache
    identity = (str(root), plan.get('created'), plan.get('unit_count'))
    if cache.get('identity') != identity:
        cache.clear()
        cache.update(identity=identity, results={})
    results = cache['results']
    by_id = {unit['id']: unit for unit in plan['units']}
    try:
        names = [p.name for p in (root / 'units').iterdir()
                 if p.name.endswith('.json') and not p.name.startswith('.')]
    except FileNotFoundError:
        names = []
    for name in names:
        cancelled()
        uid = name[:-5]
        if uid not in by_id or uid in results:
            continue
        try:
            result = read_json(root / 'units' / name)
        except FileNotFoundError:
            continue
        if result.get('complete'):
            results[uid] = result
    phases = {str(p): empty_stats() for p in (1, 2, 3)}
    links = {str(p): {} for p in (1, 2, 3)}
    all_links = {}; completed = 0; exact = plan['exact']
    planned = collections.Counter(str(u['phase']) for u in plan['units'])
    completed_by_phase = collections.Counter()
    phase_exact = {str(p): True for p in (1, 2, 3)}
    for error in plan['errors']:
        rel = error.get('rel', '')
        phase = next((str(p) for p, prefix in PRIORITY.items()
                      if rel == prefix or rel.startswith(prefix + '/')), '3')
        phase_exact[phase] = False
    unit_totals = {}
    for uid, result in results.items():
        cancelled()
        unit = by_id[uid]
        completed += 1
        exact = exact and result['exact']
        phase = str(unit['phase'])
        completed_by_phase[phase] += 1
        phase_exact[phase] = phase_exact[phase] and result['exact']
        for key in COUNTERS:
            phases[phase][key] += result['stats'][key]
        links[phase].update(result['hardlink_allocations'])
        all_links.update(result['hardlink_allocations'])
        unit_totals[unit['id']] = {'phase': unit['phase'], **result['stats'],
                                  'unique_allocated_bytes': result['unique_allocated_bytes'],
                                  'exact': result['exact']}
    # In-flight units contribute lower bounds, never duplicate a committed unit.
    partial = set()
    for index in range(plan['nodes']):
        cancelled()
        try:
            node = read_json(root / 'nodes' / (str(index) + '.json'))
        except (OSError, ValueError):
            continue
        current = node.get('current_unit')
        if not isinstance(current, dict):
            continue
        uid = current.get('id'); phase = str(current.get('phase'))
        if uid in unit_totals or uid in partial or phase not in phases:
            continue
        partial.add(uid)
        for key in COUNTERS:
            phases[phase][key] += current.get('stats', {}).get(key, 0)
    complete = completed == len(plan['units'])
    totals = {key: sum(p[key] for p in phases.values()) for key in COUNTERS}
    totals['unique_allocated_bytes'] = totals['allocated_bytes_nonhardlink'] + sum(all_links.values())
    for phase, counters in phases.items():
        counters['unique_allocated_bytes'] = counters['allocated_bytes_nonhardlink'] + sum(links[phase].values())
        counters.update(completed_units=completed_by_phase[phase], unit_count=planned[phase],
                        complete=completed_by_phase[phase] == planned[phase],
                        exact=completed_by_phase[phase] == planned[phase] and phase_exact[phase],
                        inventory_complete=completed_by_phase[phase] == planned[phase] and phase_exact[phase],
                        discovered_files=counters['files'] + counters['symlinks'],
                        discovered_bytes=counters['bytes'])
    return {'updated': now(), 'complete': complete, 'exact': complete and exact,
            'inventory_complete': complete and exact,
            'completed_units': completed, 'unit_count': len(plan['units']),
            'partial_units': len(partial),
            'phases': phases, 'totals': totals, 'units': unit_totals,
            'planning_errors': plan['errors']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--node-index', type=int)
    parser.add_argument('--rate', type=float, default=100)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--source', default=SOURCE)
    parser.add_argument('--nodes', type=int, default=12)
    parser.add_argument('--aggregate', action='store_true')
    args = parser.parse_args()
    if args.prepare:
        result = prepare_inventory(args.source, args.control, args.nodes, args.rate)
    elif args.aggregate:
        result = aggregate_inventory(args.control)
    elif args.node_index is not None:
        result = run_node(args.control, args.node_index, args.rate)
    else:
        parser.error('choose --prepare, --aggregate, or --node-index')
    print(json.dumps(result, ensure_ascii=True))


if __name__ == '__main__':
    main()
