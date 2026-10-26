"""Validate the directory census in stdin memory and owned Linux /tmp fixtures."""
import datetime
import hashlib
import json
from pathlib import Path

from migration_io import read_json, write_json
import migration_launch as transport


def main():
    base = Path(__file__).resolve().parent
    names = ('migration_io', 'migration_control_observation', 'test_migration_control_observation')
    sources = {name: (base / (name + '.py')).read_text() for name in names}
    active = read_json(base / 'active_migration.json')
    transport.JOB = active['job']
    code = """import sys
sys.dont_write_bytecode = True
import io,json,types,unittest
sources = SOURCES
for name, source in sources.items():
    module = types.ModuleType(name)
    module.__file__ = '/tmp/control-census-memory-fixture/' + name + '.py'
    sys.modules[name] = module
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules['test_migration_control_observation'])
stream = io.StringIO()
result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
print(json.dumps({'tests_run':result.testsRun,'failures':len(result.failures),
                  'errors':len(result.errors),'skipped':len(result.skipped),
                  'successful':result.wasSuccessful(),'log':stream.getvalue()}))
""".replace('SOURCES', repr(sources), 1)
    _, output = transport.ssh('master-0', code, owner=active['ssh_owner'])
    result = json.loads(output)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    log = base / ('control_observation_linux_test_' + stamp + '.log')
    log.write_text(result.pop('log'))
    validation = {**result, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'node': 'master-0', 'job': active['job'], 'log': str(log),
                  'source_sha256': {name: hashlib.sha256(source.encode()).hexdigest()
                                    for name, source in sources.items()},
                  'fixture_storage': 'stdin modules and owned temporary directories only',
                  'migration_payload_or_control_accessed': False,
                  'running_roles_changed': False, 'production_code_deployed': False,
                  'full_P4_engine_integration': False,
                  'note': '13 independent monitor-census fixtures; not added to the132 P4 component fixtures.'}
    write_json(base / 'control_observation_linux_validation.json', validation)
    print(json.dumps({**result, 'log': str(log), 'running_roles_changed': False}))
    if not result['successful'] or result['tests_run'] != 13 or result['skipped']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
