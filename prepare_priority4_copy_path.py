#!/usr/bin/env python3
"""Prepare and test one immutable P4 copy-path canary; NEVER launch copy.

No broker control POST, configuration/role update or process signal exists in
this program. The candidate still refuses to start without a separately
authorized, fully drained lane checkpoint. Native tests use private fixtures.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

from migration_io import read_json, write_json
from migration_priority4_worker import ScopeGuard
from migration_priority4_path_worker import ADDITIONS, SCOPE


CONTROL = Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053')
BASELINE = Path('/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a/fast-p3-scan-466d08d7ea874d91a73534fb5caabac5')
NODE = 'worker-10'
TESTS = ('test_migration_priority4_path_worker', 'test_migration_priority4_worker',
         'test_migration_worker_directory_fastpath', 'test_migration_worker_native_open',
         'test_migration_progress_checkpoint')


def native(code):
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        '-o', 'ForwardAgent=no', '-o', 'ForwardX11=no', '-o', 'ServerAliveInterval=5',
        '-o', 'ServerAliveCountMax=2', NODE, 'python3 -u -'],
        input=code, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=210, check=True)
    return json.loads(result.stdout)


PREFLIGHT = '''import sys,json,socket,time
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,BASELINE)
from migration_io import read_json
from migration_scan_boundary import require_identity
host=socket.gethostname()
assert host=='job-2dadf47a-b549-47bb-a267-d56dc877aec9-worker-10'
baseline=read_json(Path(BASELINE)/'copy-verification-worker-10.json')
actors=[require_identity(x) for x in baseline['identities']]
assert len(actors)==4
control=Path(CONTROL)
assert not (control/'STOP').exists()
progress=read_json(control/'nodes-extra'/(host+'.json'))
target=[x for x in actors if str(control) in x['argv'] and 'supplement' in x['argv']]
assert len(target)==1 and target[0]['pid']==progress['pid']
assert not progress.get('ended') and not progress.get('thread_error')
selected={k:progress[k] for k in ('pid','bytes','streamed_bytes','verified_files','verified_bytes','counts','stages','run_started','completed_batches','broker_retries')}
print(json.dumps(dict(host=host,verified_at=time.time(),actors=actors,
  target=target[0],progress=selected,signals_sent=0,broker_control_changes=0)))
'''


def preflight():
    return native('BASELINE=' + repr(str(BASELINE)) + '\nCONTROL=' + repr(str(CONTROL)) + '\n' + PREFLIGHT)


def prepare():
    if CONTROL.is_symlink():
        raise ValueError('real independent P4 control required')
    cfg = read_json(CONTROL / 'config.json')
    ScopeGuard(str(CONTROL), cfg)
    if cfg['job'] != '2dadf47a-b549-47bb-a267-d56dc877aec9':
        raise ValueError('exact verified P4 cluster required')
    before = preflight()
    original_path = CONTROL / 'source-sha256.json'
    original_raw = original_path.read_bytes()
    original = json.loads(original_raw)
    root_sha = hashlib.sha256(original_raw).hexdigest()
    bundle = CONTROL / ('p4-copy-path-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parent
    hashes = {}
    for name in sorted(set(original) | (ADDITIONS - {'path-open-policy.json'})):
        if Path(name).name != name or name in ('.', '..'):
            raise ValueError('invalid frozen source name')
        source = CONTROL / name if name in original else repository / name
        if source.is_symlink() or not source.is_file():
            raise ValueError('regular frozen source required')
        payload = source.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if name in original and digest != original[name]:
            raise ValueError('original P4 source hash differs')
        with open(bundle / name, 'xb') as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        (bundle / name).chmod(0o444)
        hashes[name] = digest
    policy = dict(scope=SCOPE, control=str(CONTROL), run_id=cfg['run_id'], job=cfg['job'],
        node=NODE, lane='supplement', directory_strategy='native_readonly',
        predecessor_manifest_sha256=root_sha)
    write_json(bundle / 'path-open-policy.json', policy)
    (bundle / 'path-open-policy.json').chmod(0o444)
    hashes['path-open-policy.json'] = hashlib.sha256((bundle / 'path-open-policy.json').read_bytes()).hexdigest()
    write_json(bundle / 'source-sha256.json', hashes)
    (bundle / 'source-sha256.json').chmod(0o444)
    fixtures = bundle / ('native-tests-' + uuid.uuid4().hex)
    fixtures.mkdir(mode=0o700)
    test_hashes = {}
    for name in TESTS:
        filename = name + '.py'
        shutil.copyfile(repository / filename, fixtures / filename)
        (fixtures / filename).chmod(0o444)
        test_hashes[filename] = hashlib.sha256((fixtures / filename).read_bytes()).hexdigest()
    write_json(fixtures / 'test-source-sha256.json', test_hashes)
    write_json(bundle / 'preparation-before.json', before)
    code = '''import sys,os,json,subprocess,time,hashlib
from pathlib import Path
sys.dont_write_bytecode=True
bundle=Path(BUNDLE);fixtures=Path(FIXTURES)
sys.path.insert(0,str(bundle))
import migration_priority4_path_worker as candidate
candidate.validate_bundle(Path(CONTROL),bundle,'supplement')
candidate.validate_imports(bundle)
with open(bundle/'native-tests.log','xb') as output:
 completed=subprocess.run([sys.executable,'-m','unittest']+TESTS,cwd=fixtures,
   env=dict(os.environ,PYTHONPATH=str(bundle),PYTHONDONTWRITEBYTECODE='1'),
   stdout=output,stderr=subprocess.STDOUT,timeout=180)
print(json.dumps(dict(status='prepared_native_tests_passed' if completed.returncode==0 else 'prepared_native_tests_failed',
  verified_at=time.time(),returncode=completed.returncode,copy_processes_launched=0,signals_sent=0,
  broker_control_changes=0,log=str(bundle/'native-tests.log'))))
'''
    parameters = dict(BUNDLE=str(bundle), FIXTURES=str(fixtures), CONTROL=str(CONTROL), TESTS=list(TESTS))
    proof = native('\n'.join(k + '=' + repr(v) for k, v in parameters.items()) + '\n' + code)
    after = preflight()
    if ([{k:a[k] for k in ('pid','start_ticks','argv')} for a in before['actors']]
            != [{k:a[k] for k in ('pid','start_ticks','argv')} for a in after['actors']]):
        raise RuntimeError('existing node copy identities changed; no apply or relaunch')
    write_json(bundle / 'preparation-after.json', after)
    record = dict(status=proof['status'], bundle=str(bundle), policy=policy,
        native_test_proof=proof, source_manifest_sha256=root_sha,
        preparation_entry_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        copy_processes_launched=0, signals_sent=0, broker_control_changes=0,
        node_copy_identities_unchanged=True, authorization_for_copy_replacement=False,
        created=time.time())
    write_json(bundle / 'preparation.json', record)
    print(json.dumps(record), flush=True)
    if proof['returncode']:
        raise RuntimeError('isolated native tests failed; copy is untouched, inspect prepared evidence')
    return record


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    prepare()
