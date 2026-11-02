#!/usr/bin/env python3
"""Prepare a fair streaming supplement; actual start remains an operator action."""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import uuid

import boost
import rm24

HERE = Path(__file__).resolve().parent
POINTER = HERE/'runs/tail-boost-current.json'
TERMINAL = {'supplement_complete', 'supplement_finished_with_errors', 'stopped_or_error'}


def other_supplements(original):
    """Never touch the first supplement or overwrite its current-run pointer."""
    protected = []
    for path in sorted((HERE/'runs').glob('*/config.json')):
        cfg = rm24.load(path)
        if cfg.get('supplement_of') != str(original) or not (path.parent/'start-claim.json').exists():
            continue
        status = rm24.load(path.parent/'status.json')
        if 'tail_scopes' in cfg and status['state'] not in TERMINAL:
            raise ValueError('an existing streaming supplement must finish or be stopped first')
        # Keep even finished seeds protected: do not revisit their subtrees.
        for task in cfg.get('seed_tasks', ()):
            for name in task['names']:
                rm24.component(name)
                rel = '/'.join(filter(None, (task['parent'], name)))
                protected.append(rm24.TARGET+'/'+rel)
    return sorted(set(protected))


def prepare(per_node, max_nodes):
    original = rm24.RUN.resolve()
    cfg = rm24.load(original/'config.json')
    if (cfg['target'], cfg['root_inode']) != (rm24.TARGET, rm24.ROOT_INODE):
        raise ValueError('original scope differs')
    status = rm24.load(original/'status.json')
    if status['state'] != 'rm_running' or time.time()-status['updated'] > 30:
        raise ValueError('original run is not freshly observed running')
    protected = other_supplements(original)
    observations = boost.observe(original)
    idle = [r['node'] for r in observations if r['rm_count'] == 0][:max_nodes]
    if not idle:
        raise ValueError('no idle nodes available')
    roots = sorted({p for row in observations for p in row['roots']})
    opened = sorted({p for row in observations for p in row['opened']})
    root = rm24.Root(rm24.TARGET, rm24.ROOT_INODE)
    scopes = []
    preview = []
    scans = 0
    try:
        for path in roots:
            if not path.startswith(root.path+'/'):
                raise ValueError('observed scope outside source')
            rel = path[len(root.path)+1:]
            for part in rel.split('/'):
                rm24.component(part)
            try:
                info = os.stat(path, follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode):
                    continue
                with root.parent(rel, info.st_ino):
                    pass
            except FileNotFoundError:
                continue
            if any(rel == s['rel'] or rel.startswith(s['rel']+'/') or s['rel'].startswith(rel+'/')
                   for s in scopes):
                raise ValueError('observed original scopes overlap; inspect its current phase')
            scopes.append(dict(rel=rel, ino=info.st_ino))
        if not scopes:
            raise ValueError('no live original directory scopes')
        stream = rm24.cooperative_plan(root, scopes, protected, opened)
        deadline = time.monotonic()+20
        try:
            for item in stream:
                if item['kind'] == 'task':
                    preview.append(item)
                else:
                    scans += 1
                if len(preview) >= 96 or scans >= 512 or time.monotonic() >= deadline:
                    break
        finally:
            stream.close()
        if not preview:
            raise ValueError('bounded read-only preview found no supplementary work')
    finally:
        root.close()
    run = HERE/'runs'/('tail-boost-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:6])
    rm24.prepare(run, per_node, execution_nodes=idle, supplement_of=original,
                 tail_scopes=scopes, blocked_paths=protected, busy_paths=opened)
    rm24.atomic(run/'observations.json', observations)
    rm24.atomic(run/'preview.json', dict(tasks=preview, scanned_directories=scans,
        note='Read-only sample only; start streams a fresh frontier, not these preview jobs.'))
    rm24.atomic(POINTER, dict(run=str(run), original=str(original)))
    print(json.dumps(dict(prepared=str(run), idle_nodes=idle,
        additional_rm_max=len(idle)*per_node, original_rm_observed=sum(r['rm_count'] for r in observations),
        original_directory_scopes=len(scopes), preview_tasks=len(preview),
        streaming=True, started=False, cooperative_overlap=True)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'start', 'status', 'stop'))
    parser.add_argument('--per-node', type=int, default=2)
    parser.add_argument('--max-nodes', type=int, default=12)
    args = parser.parse_args()
    if not 1 <= args.per_node <= 8 or not 1 <= args.max_nodes <= 24:
        parser.error('per-node must be 1..8; max-nodes must be 1..24')
    if args.action == 'prepare':
        prepare(args.per_node, args.max_nodes)
        return
    current = rm24.load(POINTER)
    run = Path(current['run'])
    if args.action == 'status':
        for title, path in [('original', Path(current['original'])), ('streaming_supplement', run)]:
            print(title, json.dumps(rm24.load(path/'status.json')))
        return
    raise SystemExit(subprocess.call([sys.executable, '-S', '-B', '-u', str(run/'rm24.py'),
                                     args.action, '--run', str(run)]))


if __name__ == '__main__':
    main()
