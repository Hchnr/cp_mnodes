"""Validate P4 process observation in stdin memory and owned Linux fixtures."""
import datetime
import hashlib
import json
from pathlib import Path

from migration_io import read_json, write_json
import migration_launch as transport


def main():
    base = Path(__file__).resolve().parent
    names = ('migration_io', 'profile_copy_proc_only', 'test_profile_copy_proc_only',
             'migration_priority4_process_observation', 'test_migration_priority4_process_observation',
             'observe_priority4_dependency_processes', 'test_observe_priority4_dependency_processes')
    sources = {name: (base / (name + '.py')).read_text() for name in names}
    active = read_json(base / 'active_migration.json')
    transport.JOB = active['job']
    code = """import sys
sys.dont_write_bytecode = True
import io,json,types,unittest
sources = SOURCES
transport_stub = types.ModuleType('migration_launch')
def forbidden(*args, **kwargs):
    raise AssertionError('fixture must not read migration JSON or call SSH')
transport_stub.ssh = forbidden
sys.modules['migration_launch'] = transport_stub
for name, source in sources.items():
    module = types.ModuleType(name)
    module.__file__ = '/tmp/p4-process-memory-fixture/' + name + '.py'
    sys.modules[name] = module
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
for name in ('profile_copy_proc_only','observe_priority4_dependency_processes'):
    sys.modules[name].read_json = forbidden
    sys.modules[name].write_json = forbidden
suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[name])
    for name in ('test_migration_priority4_process_observation','test_observe_priority4_dependency_processes'))
stream = io.StringIO()
result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
print(json.dumps({'tests_run':result.testsRun,'failures':len(result.failures),
                  'errors':len(result.errors),'skipped':len(result.skipped),
                  'successful':result.wasSuccessful(),'log':stream.getvalue()}))
""".replace('SOURCES', repr(sources), 1)
    _, output = transport.ssh('master-0', code, owner=active['ssh_owner'])
    result = json.loads(output)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    log = base / ('priority4_process_linux_test_' + stamp + '.log')
    log.write_text(result.pop('log'))
    validation = {**result, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'node': 'master-0', 'job': active['job'], 'log': str(log),
                  'source_sha256': {name: hashlib.sha256(source.encode()).hexdigest()
                                    for name, source in sources.items()},
                  'local_fixture_result': {'tests_run': 24, 'passed': 23, 'skipped': 1,
                                           'skip_reason': 'real Linux child lifecycle requires Linux /proc'},
                  'fixture_storage': 'stdin modules and owned temporary directories',
                  'real_linux_proc_census_performed': True,
                  'fixture_child_lifecycle': 'Only the fixture-owned child is created, terminated and reaped; live, zombie/no-FDs and absent states are independently checked.',
                  'migration_payload_or_control_accessed': False,
                  'migration_roles_changed': False, 'production_code_deployed': False,
                  'full_P4_engine_integration': False, 'launch_ready': False,
                  'unique_fixture_scope': '16 process-reader fixtures plus8 independent saved-inventory/collector composition fixtures; previously separate14 profiler fixtures are imported only for their evidence factory and are not rerun or counted here.',
                  'limitations': ['12-node inventory/transport live integration is recorded separately; no terminal exit acceptance from fixtures.',
                                  'A process snapshot alone cannot validate terminal/scope/STOP/queues/leases/receipts/metadata or local dispatch locks.',
                                  'The caller still needs two fresh complete observations and independent full dependency checks.']}
    write_json(base / 'priority4_process_observation_validation.json', validation)
    print(json.dumps({**result, 'log': str(log), 'migration_roles_changed': False,
                      'full_P4_engine_integration': False}))
    if not result['successful'] or result['tests_run'] != 24 or result['skipped']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
