#!/usr/bin/env python3
"""Freeze the read-only combined monitor; do not alter an active copy bundle."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import uuid

from migration_io import read_json, write_json
from migration_priority4_worker import ScopeGuard


MODULES = ('migration_parallel_progress.py', 'migration_progress.py', 'migration_nodes.py',
           'migration_io.py', 'migration_priority4_worker.py', 'migration_state_cache.py')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, required=True)
    args = parser.parse_args(argv)
    control = args.control.absolute()
    cfg = read_json(control / 'config.json')
    ScopeGuard(str(control), cfg)
    predecessor = Path(cfg['parallel_with_control'])
    plan = read_json(predecessor / 'extension-plan.json')
    if (plan.get('parallel_execution', {}).get('control') != str(control) or
            plan['parallel_execution'].get('mode') != 'independent_concurrent_p4'):
        raise ValueError('P4 is not the registered parallel run')
    bundle = control / ('monitor-parallel-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    root = Path(__file__).resolve().parent
    hashes = {}
    for name in MODULES:
        source = root / name
        hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
        shutil.copyfile(source, bundle / name)
        if hashlib.sha256((bundle / name).read_bytes()).hexdigest() != hashes[name]:
            raise ValueError('monitor source changed while freezing')
    wrapper = Path('/share-mixed/project/.hcr-copy-monitor/progress.py')
    shutil.copyfile(wrapper, bundle / 'original-progress.py')
    write_json(bundle / 'source-sha256.json', hashes)
    write_json(bundle / 'deployment.json', dict(control=str(control), predecessor=str(predecessor),
               data_roles_changed=False, pointer_changed=False, wrapper_backup=str(bundle / 'original-progress.py')))
    plan['parallel_execution']['monitor_bundle'] = str(bundle)
    write_json(predecessor / 'extension-plan.json', plan)
    print(json.dumps(dict(monitor_bundle=str(bundle), data_roles_changed=False, pointer_changed=False)))


if __name__ == '__main__':
    main()
