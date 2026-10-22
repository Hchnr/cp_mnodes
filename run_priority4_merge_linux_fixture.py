"""Run scoped P4 resume/merge fixtures in stdin memory and owned Linux /tmp."""
import ast
import datetime
import hashlib
import json
from pathlib import Path

from migration_io import read_json, write_json
import migration_launch as transport


def fixture_sources(base):
    sources = {}
    pending = ['test_migration_priority4_index_merge', 'test_migration_priority4_resume']
    while pending:
        name = pending.pop()
        if name in sources:
            continue
        path = base / (name + '.py')
        source = path.read_text()
        sources[name] = source
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Import):
                names = [alias.name.split('.')[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module.split('.')[0]]
            else:
                continue
            pending.extend(n for n in names if (base / (n + '.py')).is_file())
    return sources


def main():
    base = Path(__file__).resolve().parent
    sources = fixture_sources(base)
    active = read_json(base / 'active_migration.json')
    transport.JOB = active['job']
    code = '''import sys
sys.dont_write_bytecode=True
import importlib.abc,importlib.util,io,json,pathlib,platform,sqlite3,tempfile,unittest
sources=SOURCES
class MemoryLoader(importlib.abc.MetaPathFinder,importlib.abc.Loader):
 def find_spec(self,fullname,path=None,target=None):
  if fullname in sources:return importlib.util.spec_from_loader(fullname,self)
 def create_module(self,spec):return None
 def exec_module(self,module):
  module.__file__='/tmp/hcr-p4-merge-memory/'+module.__name__+'.py'
  exec(compile(sources[module.__name__],module.__file__,'exec'),module.__dict__)
sys.meta_path.insert(0,MemoryLoader())
import migration_io
actual_read,actual_write=migration_io.read_json,migration_io.write_json
def fixture_path(path):
 p=pathlib.Path(path).absolute();root=pathlib.Path(tempfile.gettempdir()).resolve()
 try:parts=p.relative_to(root).parts
 except ValueError:raise AssertionError('fixture JSON outside owned /tmp: '+str(p))
 if not parts or not parts[0].startswith(('p4-resume-fixture-','hcr-p4-merge-fixture-')):
  raise AssertionError('fixture JSON outside owned fixture tree: '+str(p))
 return p
def safe_read(path):return actual_read(fixture_path(path))
def safe_write(path,value):return actual_write(fixture_path(path),value)
migration_io.read_json,migration_io.write_json=safe_read,safe_write
names=('test_migration_priority4_index_merge','test_migration_priority4_resume')
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(n) for n in names)
stream=io.StringIO();result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
print(json.dumps({'tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
 'skipped':len(result.skipped),'successful':result.wasSuccessful(),'log':stream.getvalue(),
 'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,'platform':platform.system()}))
'''.replace('SOURCES', repr(sources), 1)
    _, output = transport.ssh('master-0', code, owner=active['ssh_owner'])
    result = json.loads(output)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    log = base / ('priority4_merge_linux_test_' + stamp + '.log')
    log.write_text(result.pop('log'))
    validation = {**result, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  'node': 'master-0', 'job': active['job'], 'log': str(log),
                  'source_sha256': {name: hashlib.sha256(source.encode()).hexdigest()
                                    for name, source in sources.items()},
                  'fixture_scope': '16 independent merge fixtures plus14 existing scoped resume regressions; 30 total, no duplicate unique count for regressions.',
                  'fixture_storage': 'stdin modules and fixture-owned temporary directories; JSON reads/writes restricted to fixture roots',
                  'new_unique_fixtures': 16, 'existing_fixture_regressions': 14,
                  'production_code_deployed': False, 'migration_payload_or_control_accessed': False,
                  'migration_roles_changed': False, 'full_P4_engine_integration': False,
                  'launch_ready': False,
                  'limitations': ['The primitive accepts supplied dedicated SQLite connections; runtime file identity, no-follow local paths and the coordinator lock still require independent implementation and validation.',
                                  'No runtime connection is opened by the primitive and no existing migration database is stamped, merged, adopted, deleted or rebuilt.',
                                  'The eventual caller must provide current STOP/configuration checks and complete dependency/dispatch acceptance before integration.']}
    write_json(base / 'priority4_index_merge_validation.json', validation)
    print(json.dumps({**result, 'log': str(log), 'migration_roles_changed': False,
                      'full_P4_engine_integration': False}))
    if not result['successful'] or result['tests_run'] != 30 or result['skipped']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
