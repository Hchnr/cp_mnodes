#!/usr/bin/env python3
"""Read-only remote preflight and bounded selection from an existing manifest.

The only output written by this program is a local JSON selection. The remote
program disables bytecode writes and only reads metadata/control records.
"""
import argparse
import json
import pathlib

import migration_launch as transport
from migration_io import read_json


REMOTE = r'''
import sys
sys.dont_write_bytecode = True
import collections, datetime, json, os, pathlib, socket, stat, subprocess, time
params = json.loads(PARAMETERS)
c = pathlib.Path(params['control'])
sys.path.insert(0, str(c))
from migration_io import read_json
src = pathlib.Path(params['source'])
manifest = c / 'manifests' / 'p1-r1.jsonl'
now = time.time()
MiB = 1024**2
warnings = []
def optional(path):
    try:
        return read_json(path)
    except (OSError, ValueError, TypeError) as exc:
        warnings.append(str(path) + ': ' + type(exc).__name__)
        return None
def command(args):
    try:
        run = subprocess.run(args, capture_output=True, text=True, timeout=10)
        return {'returncode': run.returncode, 'stdout': run.stdout[:30000],
                'stderr': run.stderr[:2000]}
    except (OSError, subprocess.SubprocessError) as exc:
        return {'error': type(exc).__name__}
cp_version = command(['cp', '--version'])
cp_help = command(['cp', '--help'])
rsync_version = command(['rsync', '--version'])
rsync_help = command(['rsync', '--help'])
state = optional(c / 'state.json')
health = optional(c / 'training-health.json')
node = optional(c / 'nodes' / (socket.gethostname() + '.json'))
cfg = optional(c / 'config.json') or {}
stop = (c / 'STOP').exists()
preflight = {
    'hostname': socket.gethostname(), 'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'stop_present': stop, 'state_status': (state or {}).get('status'),
    'state_stage': (state or {}).get('stage'), 'training_health': health,
    'node_threads': (node or {}).get('threads'),
    'node_active_threads': len((node or {}).get('active', {})),
    'node_ended': (node or {}).get('ended'),
    'configured_threads': cfg.get('threads'), 'configured_io_bytes_per_second': cfg.get('io_bytes_per_second'),
    'cp_version': cp_version, 'rsync_version': rsync_version,
    'cp_flags': {flag: flag in cp_help.get('stdout', '') for flag in ['--reflink', '--sparse', '--preserve']},
    'rsync_flags': {flag: flag in rsync_help.get('stdout', '') for flag in ['--whole-file', '--sparse', '--acls', '--xattrs', '--fsync']},
    'source_mount': command(['findmnt', '-T', str(src), '-rn', '-o', 'TARGET,SOURCE,FSTYPE']),
    'destination_mount': command(['findmnt', '-T', params['destination'], '-rn', '-o', 'TARGET,SOURCE,FSTYPE']),
}
if stop:
    print(json.dumps({'ok': False, 'reason': 'migration STOP present', 'preflight': preflight,
                      'source': str(src), 'control': str(c), 'node': params['node'], 'warnings': warnings}))
    raise SystemExit(0)

stats = collections.Counter()
large_candidates = []
small_candidates = []
seen = set()
forbidden_parts = {'.git', 'outputs', 'logs', 'wandb', 'tmp', 'temp', '.tmp', '__pycache__', '.cache'}
def eligible_path(rel):
    if not isinstance(rel, str) or not rel or rel.startswith('/') or '\x00' in rel:
        return False
    parts = rel.split('/')
    if any(part in ('', '.', '..') or part.lower() in forbidden_parts or 'codex' in part.lower() for part in parts):
        return False
    name = parts[-1].lower()
    return not (name.startswith('.migration-') or name.endswith(('.tmp', '.partial', '.part', '.lock', '~')))

def inspect(entry, category):
    stats[category + '_stat_attempts'] += 1
    rel = entry['rel']
    try:
        st = os.stat(src / rel, follow_symlinks=False)
    except OSError:
        stats['stat_error'] += 1
        return None
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
        stats['nonregular_or_hardlinked'] += 1
        return None
    if max(st.st_mtime, st.st_ctime) > now - params['min_age_seconds']:
        stats['recently_changed'] += 1
        return None
    if any(key not in entry or getattr(st, 'st_' + key) != entry[key]
           for key in ('ino', 'size', 'mtime_ns', 'ctime_ns')):
        stats['manifest_mismatch'] += 1
        return None
    # A tiny amount of last-block rounding is normal. Reject sparse extents.
    if st.st_blocks * 512 < st.st_size:
        stats['sparse'] += 1
        return None
    result = dict(entry)
    result.update({key: getattr(st, 'st_' + key) for key in
                   ('dev', 'ino', 'size', 'mode', 'uid', 'gid', 'nlink', 'mtime_ns', 'ctime_ns', 'atime_ns')})
    result['allocated_bytes'] = st.st_blocks * 512
    return result

byte_limit = params['manifest_limit_bytes']
with open(manifest, 'rb') as handle:
    while handle.tell() < byte_limit:
        remaining = byte_limit - handle.tell()
        raw = handle.readline(remaining)
        if not raw:
            stats['manifest_eof'] = 1
            break
        stats['manifest_bytes_read'] += len(raw)
        if not raw.endswith(b'\n'):
            stats['partial_line_skipped'] += 1
            break
        stats['manifest_records_read'] += 1
        try:
            entry = json.loads(raw)
        except (ValueError, TypeError):
            stats['invalid_json'] += 1
            continue
        if entry.get('kind') != 'file' or entry.get('nlink') != 1:
            continue
        rel = entry.get('rel')
        if rel in seen or not eligible_path(rel):
            continue
        size = entry.get('size', -1)
        if max(entry.get('mtime_ns', 0), entry.get('ctime_ns', 0)) > int((now - params['min_age_seconds']) * 1e9):
            continue
        category = None
        if 16*MiB <= size <= 256*MiB and stats['large_stat_attempts'] < 512:
            category = 'large'
        elif 1024 <= size <= 64*1024 and stats['small_stat_attempts'] < 2048:
            category = 'small'
        if category is None:
            continue
        seen.add(rel)
        checked = inspect(entry, category)
        if checked is not None:
            (large_candidates if category == 'large' else small_candidates).append(checked)
        # Collect enough candidates to stratify large files without scanning
        # beyond the stated fixed byte budget.
        if len(large_candidates) >= 128 and len(small_candidates) >= 1024:
            stats['candidate_targets_reached'] = 1
            break

# Interleave size buckets. Each chosen entry remains a full copy_entry record.
buckets = [[], [], [], []]
for entry in large_candidates:
    size = entry['size']
    bucket = 0 if size < 32*MiB else 1 if size < 64*MiB else 2 if size < 128*MiB else 3
    buckets[bucket].append(entry)
large = []
large_bytes = 0
while len(large) < 64 and any(buckets):
    made_progress = False
    for bucket in buckets:
        if bucket:
            entry = bucket.pop(0)
            if large_bytes + entry['size'] <= 8*1024**3 and len(large) < 64:
                large.append(entry)
                large_bytes += entry['size']
                made_progress = True
    if not made_progress:
        break
small = []
small_bytes = 0
for entry in small_candidates:
    if len(small) >= 1024:
        break
    if small_bytes + entry['size'] <= 64*MiB:
        small.append(entry)
        small_bytes += entry['size']

summary = {'large': {'files': len(large), 'bytes': large_bytes, 'candidate_files': len(large_candidates)},
           'small': {'files': len(small), 'bytes': small_bytes, 'candidate_files': len(small_candidates)}}
ok = len(large) >= 16 and len(small) >= 64 and not stop
result = {'ok': ok, 'source': str(src), 'destination': params['destination'],
          'control': str(c), 'node': params['node'], 'job': params['job'],
          'manifest': str(manifest), 'min_age_seconds': params['min_age_seconds'],
          'large': large, 'small': small, 'summary': summary, 'statistics': dict(stats),
          'preflight': preflight, 'warnings': warnings}
print(json.dumps(result, ensure_ascii=True))
'''


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--active', default=str(pathlib.Path(__file__).parent / 'active_migration.json'))
    parser.add_argument('--output', default=str(pathlib.Path(__file__).parent / 'benchmark_selection.json'))
    parser.add_argument('--node', default='worker-10')
    parser.add_argument('--manifest-mib', type=int, default=384)
    parser.add_argument('--min-age-seconds', type=int, default=600)
    args = parser.parse_args(argv)
    cfg = read_json(args.active)
    transport.JOB = cfg['job']
    params = {key: cfg[key] for key in ('source', 'destination', 'control', 'job')}
    params.update(node=args.node, manifest_limit_bytes=args.manifest_mib * 1024**2,
                  min_age_seconds=args.min_age_seconds)
    code = REMOTE.replace('PARAMETERS', repr(json.dumps(params)), 1)
    _, output = transport.ssh(args.node, code)
    selected = json.loads(output)
    selected['workloads'] = {
        name: {'entries': selected.get(name, []),
               'bytes': selected.get('summary', {}).get(name, {}).get('bytes', 0)}
        for name in ('large', 'small')}
    preflight = selected.get('preflight', {})
    selected['tools'] = {
        name: {'version': preflight.get(name + '_version'),
               'flags': preflight.get(name + '_flags')}
        for name in ('cp', 'rsync')}
    # An allowlist was used for config fields above; also guard accidental
    # token leakage if an external control status unexpectedly contained one.
    token = cfg.get('broker_token')
    if isinstance(token, str) and token and token in json.dumps(selected):
        raise RuntimeError('private token appeared in benchmark selection')
    path = pathlib.Path(args.output)
    path.write_text(json.dumps(selected, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'ok': selected.get('ok'), 'output': str(path.resolve()),
                      'node': selected.get('node'), 'summary': selected.get('summary'),
                      'statistics': selected.get('statistics'), 'preflight': selected.get('preflight'),
                      'warnings': selected.get('warnings')}, ensure_ascii=False))
    return 0 if selected.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
