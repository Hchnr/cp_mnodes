#!/usr/bin/env python3
"""Build a flat compatibility bundle; never deploy or start copying implicitly."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
STANDALONE_TESTS = frozenset({
    'test_migration_fresh_resume',
    'test_migration_seed_handover',
    'test_migration_seed_handover_guards',
})


def sources(root):
    result = {}
    for directory in ('src', 'ops', 'tests'):
        for path in sorted((root / directory).rglob('*.py')):
            if '__pycache__' in path.parts:
                continue
            if path.name in result:
                raise ValueError(f'duplicate bundle filename: {path.name}')
            result[path.name] = path
    return result


def build(root=ROOT, destination=None):
    # A fresh bundle avoids stale modules and never edits a deployed bundle.
    items = sources(root)
    if not items:
        raise ValueError('no source files found')
    if destination is None:
        cache = root / '.build'
        cache.mkdir(exist_ok=True)
        output = Path(tempfile.mkdtemp(prefix='runtime-', dir=cache))
    else:
        output = Path(destination).resolve()
        output.mkdir(parents=True, exist_ok=False)
    manifest = {}
    for name, path in items.items():
        data = path.read_bytes()
        with (output / name).open('xb') as stream:
            stream.write(data)
        (output / name).chmod(path.stat().st_mode & 0o777)
        manifest[name] = {'source': path.relative_to(root).as_posix(),
                          'sha256': hashlib.sha256(data).hexdigest()}
    (output / 'SOURCE_MAP.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return output


def run_tests(output, arguments):
    prefix = [sys.executable, '-B', '-m', 'unittest']
    if arguments:
        return subprocess.call(prefix + arguments, cwd=output)
    # Historical tests install process-global engine adapters. Isolate modules
    # so one module cannot change another module's filesystem implementation.
    modules = sorted(path.stem for path in output.glob('test_*.py'))
    if not modules:
        raise ValueError('bundle contains no tests')
    failed = []
    for module in modules:
        print(f'Test module: {module}', flush=True)
        command = ([sys.executable, '-B', str(output / (module + '.py'))]
                   if module in STANDALONE_TESTS else prefix + [module])
        if subprocess.call(command, cwd=output):
            failed.append(module)
    print(f'Test modules: {len(modules)}, failed: {len(failed)}', flush=True)
    if failed:
        print('Failed modules: ' + ', '.join(failed), file=sys.stderr)
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('list', help='list logical entry names and source paths')
    bundle = commands.add_parser('build', help='freeze sources; execute nothing')
    bundle.add_argument('--output', type=Path)
    test = commands.add_parser('test', help='run unittest in a fresh flat bundle')
    test.add_argument('args', nargs=argparse.REMAINDER)
    run = commands.add_parser('run', help='explicitly execute an entry; review its safety requirements first')
    run.add_argument('entry')
    run.add_argument('args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command == 'list':
        for name, path in sorted(sources(ROOT).items()):
            print(f'{name}\t{path.relative_to(ROOT)}')
        return 0
    if args.command == 'run' and args.entry not in sources(ROOT):
        parser.error('unknown entry; use list to find available names')
    output = build(destination=getattr(args, 'output', None))
    print(f'Bundle: {output}', file=sys.stderr)
    if args.command == 'build':
        print(output)
        return 0
    rest = args.args
    if rest[:1] == ['--']:
        rest = rest[1:]
    if args.command == 'test':
        return run_tests(output, rest)
    command = [sys.executable, '-B', str(output / args.entry)] + rest
    return subprocess.call(command, cwd=output)


if __name__ == '__main__':
    raise SystemExit(main())
