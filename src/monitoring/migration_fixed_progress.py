"""Read-only fixed-manifest monitor dispatch; does not touch old active.json."""
import argparse
import hashlib
import json
from pathlib import Path
import runpy
import sys
import time


POINTER = Path('/share-mixed/project/.hcr-copy-monitor/fixed-active.json')


def dispatch(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if '--control' in argv or any(arg.startswith('--control=') for arg in argv):
        return False  # Explicit historical controls remain inspectable.
    for attempt in range(3):
        try:
            value = json.loads(POINTER.read_text())
            break
        except FileNotFoundError:
            if attempt == 2:
                return False
            list(POINTER.parent.iterdir())
    if not value.get('enabled'):
        return False
    control = Path(value['control'])
    if (control.parent != Path('/share-mixed/project')
            or not control.name.startswith('.hcr-copy-fixed-') or control.is_symlink()):
        raise ValueError('invalid fixed-manifest monitor control')
    cfg = json.loads((control / 'config.json').read_text())
    for name, checksum in cfg['code'].items():
        if Path(name).name != name:
            raise ValueError('invalid frozen code name')
        with open(control / 'code' / name, 'rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != checksum:
                raise ValueError('frozen monitor/engine changed: ' + name)
    sys.path.insert(0, str(control / 'code'))
    module = runpy.run_path(str(control / 'code/migration_manifest_copy.py'), run_name='_fixed_monitor')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--watch', nargs='?', type=float, const=5)
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--details', action='store_true')
    args = parser.parse_args(argv)
    while True:
        if args.json:
            nodes = {}
            for node in cfg['shards']:
                try:
                    nodes[node] = module['read_json'](control / 'nodes' / node / 'progress.json')
                except FileNotFoundError:
                    nodes[node] = None
            print(json.dumps(dict(control=str(control), files=cfg['files'], bytes=cfg['bytes'],
                                  manifest_sha256=cfg['manifest_sha256'], nodes=nodes)), flush=True)
        else:
            module['monitor'](control)
        if args.watch is None or args.json:
            return True
        time.sleep(max(1, args.watch))
