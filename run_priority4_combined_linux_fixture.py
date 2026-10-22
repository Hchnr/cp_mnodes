"""Run only the read-only proof fixtures in memory and owned /tmp on master.

No migration root/control or running role is opened, deployed, or modified.
Production dependency checks and full P4 integration remain separate.
"""
import datetime
import hashlib
import json
from pathlib import Path

from migration_io import read_json, write_json
import migration_launch as transport


BASE = Path(__file__).resolve().parent
MODULES = (
    'migration_worker', 'migration_scan', 'migration_io', 'migration_state_cache',
    'migration_priority4_worker', 'migration_run', 'migration_worker128_run',
    'migration_priority4_file_metadata', 'migration_priority4_hardlink_proof',
    'test_migration_priority4_worker', 'test_migration_priority4_hardlink_proof',
    'test_migration_priority4_file_metadata', 'test_migration_priority4_combined_proof',
)


def main():
    sources = {name: (BASE / (name + '.py')).read_text() for name in MODULES}
    active = read_json(BASE / 'active_migration.json')
    transport.JOB = active['job']
    code = """import sys
sys.dont_write_bytecode = True
import io,json,types,unittest
sources = SOURCES
for name, source in sources.items():
    module = types.ModuleType(name)
    module.__file__ = '/tmp/p4-memory-fixture/' + name + '.py'
    sys.modules[name] = module
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
suite = unittest.TestSuite()
for name in ('test_migration_priority4_hardlink_proof', 'test_migration_priority4_file_metadata',
             'test_migration_priority4_combined_proof'):
    suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[name]))
stream = io.StringIO()
result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
print(json.dumps({'tests_run': result.testsRun, 'failures': len(result.failures),
                  'errors': len(result.errors), 'skipped': len(result.skipped),
                  'successful': result.wasSuccessful(), 'log': stream.getvalue()}))
""".replace('SOURCES', repr(sources), 1)
    _, output = transport.ssh('master-0', code, owner=active['ssh_owner'])
    result = json.loads(output)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    log = BASE / ('priority4_combined_proof_linux_test_' + stamp + '.log')
    log.write_text(result.pop('log'))
    validation = dict(
        result, at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        node='master-0', job=active['job'],
        source_sha256={name: hashlib.sha256(source.encode()).hexdigest()
                       for name, source in sources.items()},
        fixture_storage='payload and real flock files within owned /tmp temporary trees; modules only in stdin memory',
        fixture_lock_redirected_to_temp_tree=True,
        migration_payload_or_control_accessed=False, production_deployed=False,
        running_roles_changed=False, publication_ready=False,
        validation_scope='16 content/ownership +14 FD metadata +10 combined Linux proof fixtures',
        new_unique_combined_fixtures=10, prior_fixtures_revalidated_linux=30,
        earlier_failed_log='work/priority4_combined_proof_linux_test_first.log',
        limitations=['Verification only; atime restoration, other exposed ACL models and final publication checks remain required.',
                     'Current SHA plus bounded stat/metadata observations are not an atomic snapshot against arbitrary online writers.'],
        log=str(log),
    )
    write_json(BASE / 'priority4_combined_proof_validation.json', validation)
    print(json.dumps({**result, 'log': str(log), 'deployed': False}))
    if not result['successful'] or result['tests_run'] != 40 or result['skipped']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
