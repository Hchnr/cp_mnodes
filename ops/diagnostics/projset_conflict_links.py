#!/usr/bin/env python3
"""Read-only, scope-restricted resolution of conflicting symlink paths.

This does not change links or infer that separate source/destination trees have
identical content. Resolution stops at a link outside the allowed roots.
"""
import argparse
import collections
import csv
import importlib.util
import json
import os
from pathlib import Path
import stat
import time
import uuid


class OutsideScope(ValueError):
    pass


class ChangedPath(ValueError):
    pass


def absolute_target(path, roots):
    """Pick an allowed prefix without collapsing '..' across symlinks."""
    if not path.startswith('/'):
        raise ValueError('absolute path required')
    parts = [part for part in path.split('/') if part]
    for root in sorted(roots, key=len, reverse=True):
        prefix = [part for part in root.split('/') if part]
        if parts[:len(prefix)] == prefix:
            return root, parts[len(prefix):]
    raise OutsideScope('absolute link leaves permitted roots')


def resolve(native, roots, initial_root, rel, *, delay=0.0, max_links=40, max_components=512):
    """Resolve links one component at a time; all operations remain beneath roots."""
    if initial_root not in roots or not rel or rel.startswith('/') or '\x00' in rel:
        raise ValueError('invalid initial path')
    remaining = collections.deque(rel.split('/'))
    resolved = []
    root = initial_root
    links = steps = 0
    final_stat = None
    while remaining:
        part = remaining.popleft()
        if part in ('', '.'):
            continue
        if part == '..':
            if not resolved:
                raise OutsideScope('relative link leaves permitted root')
            resolved.pop()
            final_stat = None
            continue
        steps += 1
        if steps > max_components:
            raise ValueError('component limit reached')
        if delay:
            time.sleep(delay)
        parent, name = native.open_parent_readonly(root, '/'.join(resolved + [part]))
        try:
            before = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                links += 1
                if links > max_links:
                    raise ValueError('symlink limit reached')
                target = os.readlink(name, dir_fd=parent)
                after = os.stat(name, dir_fd=parent, follow_symlinks=False)
                identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                if identity(before) != identity(after):
                    raise ChangedPath('symlink changed while inspected')
                if target.startswith('/'):
                    root, prefix = absolute_target(target, roots)
                    resolved = []
                else:
                    prefix = target.split('/')
                remaining.extendleft(reversed(prefix))
                final_stat = None
                continue
            if remaining and not stat.S_ISDIR(before.st_mode):
                raise NotADirectoryError('non-directory path component')
            resolved.append(part)
            final_stat = before
        finally:
            os.close(parent)
    if final_stat is None:
        fd = (native._open_absolute_directory(root) if not resolved else
              native.open_parent_readonly(root, '/'.join(resolved) + '/_')[0])
        try:
            final_stat = os.fstat(fd)
        finally:
            os.close(fd)
    return {'root': root, 'relative': '/'.join(resolved),
            'path': os.path.join(root, *resolved), 'dev': final_stat.st_dev,
            'ino': final_stat.st_ino, 'mode': final_stat.st_mode, 'links_followed': links}


def compare(source, target, source_root, target_root):
    if 'error' in source or 'error' in target:
        return 'unresolved'
    if source['path'] == target['path']:
        if (source['dev'], source['ino']) == (target['dev'], target['ino']):
            return 'same_current_object'
        return 'changed_during_comparison'
    if (source['root'] == source_root and target['root'] == target_root and
            source['relative'] == target['relative']):
        return 'corresponding_relative_paths_content_unverified'
    return 'different_resolved_paths_content_unverified'


def atomic(path, value):
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('x') as stream:
        json.dump(value, stream, ensure_ascii=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--node', required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--delay', type=float, default=0.005)
    args = parser.parse_args()
    if not 0 <= args.delay <= 0.1:
        parser.error('delay must be between 0 and 0.1 seconds')
    run = args.run.resolve()
    module_path = run / 'recovery/code/projset_recovery_copy.py'
    spec = importlib.util.spec_from_file_location('frozen_recovery', module_path)
    recovery = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(recovery)
    cfg = recovery.read(run / 'config.json')
    recovery.validate(run, cfg, args.node)
    roots = (cfg['source'], cfg['target'])
    with args.input.open(encoding='utf-8-sig', newline='') as stream:
        selected = [row for row in csv.DictReader(stream) if row['kind'] == 'symlink']
    started = time.time()
    results = []
    for row in selected:
        pair = {'rel': row['relative_path'], 'historical_reason': row['reason']}
        for name, root in zip(('source', 'target'), roots):
            try:
                pair[name] = resolve(recovery.native, roots, root, row['relative_path'], delay=args.delay)
            except (OSError, ValueError) as error:
                pair[name] = {'error': type(error).__name__, 'errno': getattr(error, 'errno', None)}
        pair['comparison'] = compare(pair['source'], pair['target'], *roots)
        results.append(pair)
    summary = {'node': args.node, 'started': started, 'updated': time.time(),
               'input': str(args.input), 'total': len(results),
               'categories': dict(collections.Counter(row['comparison'] for row in results)),
               'scope': 'read-only resolution within configured source and target roots; separate trees are not content-verified'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic(args.output, {'summary': summary, 'results': results})
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == '__main__':
    main()
