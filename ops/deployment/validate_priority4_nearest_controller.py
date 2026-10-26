#!/usr/bin/env python3
"""Freeze/native-test a controller validation pack, NEVER stage authority.

Does not call transaction.stage/run/remote, contact broker control, drain,
signal or launch copy. Actual transaction staging remains a separate action
that requires the user's second explicit permission.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import time
import uuid

import deploy_priority4_nearest_canary as deployment
import prepare_priority4_nearest_worker as preparation
from migration_cached_worker import _check_progress
from migration_io import read_json, write_json


TESTS = ('test_deploy_priority4_nearest_canary', 'test_deploy_priority4_path_canary',
         'test_rollout_cache_age', 'test_rollout_cache_cluster',
         'test_migration_progress_checkpoint', 'test_audit_priority4_source')
FIXTURE_DEPENDENCIES = ('test_migration_worker_native_open', 'test_migration_worker_directory_fastpath')


def fixture_names():
    return tuple(dict.fromkeys(TESTS + FIXTURE_DEPENDENCIES))


def validate():
    before = preparation.preflight()
    bundle = deployment.BUNDLE
    if read_json(bundle / 'preparation.json')['status'] != 'prepared_native_tests_passed':
        raise RuntimeError('validated second candidate required; no authority is created')
    deployment.transaction.freeze_verify(bundle)
    directory = bundle / ('controller-validation-' + uuid.uuid4().hex)
    directory.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parent
    hashes = {}
    for name in deployment.FILES:
        source = bundle / name if name == 'migration_io.py' else repository / name
        digest = preparation.candidate.proof.digest_file(source)
        with open(directory / name, 'xb') as output:
            output.write(source.read_bytes())
            output.flush()
            os.fsync(output.fileno())
        (directory / name).chmod(0o444)
        hashes[name] = preparation.candidate.proof.digest_file(directory / name)
        if hashes[name] != digest:
            raise RuntimeError('controller source changed during freezing')
    write_json(directory / 'source-sha256.json', hashes)
    fixtures = directory / 'native-tests'
    fixtures.mkdir(mode=0o700)
    test_hashes = {}
    for name in fixture_names():
        filename = name + '.py'
        shutil.copyfile(repository / filename, fixtures / filename)
        (fixtures / filename).chmod(0o444)
        test_hashes[filename] = preparation.candidate.proof.digest_file(fixtures / filename)
    write_json(fixtures / 'test-source-sha256.json', test_hashes)
    write_json(directory / 'validation-before.json', before)
    code = '''import sys,os,json,time,subprocess
from pathlib import Path
sys.path[:0]=[DIRECTORY,BUNDLE]
import deploy_priority4_nearest_canary as deployment
directory=Path(DIRECTORY)
assert not (directory/'authorization.json').exists()
assert not (directory/'result.json').exists()
with deployment.configured() as transaction:
 transaction.freeze_verify(directory);transaction.freeze_verify(Path(BUNDLE))
 cfg=transaction.read_json(transaction.CONTROL/'config.json')
 baseline=transaction.read_json(Path(BUNDLE)/'preparation-after.json')
 transaction.verify_rollout_policy(cfg,transaction.native_identity(baseline['target']))
 assert not transaction.CHECKPOINT.exists()
 assert not (transaction.CHECKPOINT.parent/'.snapshots'/transaction.CHECKPOINT.name).exists()
 command=transaction.child_command(transaction.CHECKPOINT,'validation-only',sys.executable)
 assert command[2]==str(Path(BUNDLE)/'migration_priority4_nearest_worker.py')
with open(directory/'native-tests.log','xb') as output:
 result=subprocess.run([sys.executable,'-B','-m','unittest']+TESTS,cwd=FIXTURES,
  env=dict(os.environ,PYTHONPATH=DIRECTORY+os.pathsep+BUNDLE,PYTHONDONTWRITEBYTECODE='1'),
  stdout=output,stderr=subprocess.STDOUT,timeout=120)
assert not (directory/'authorization.json').exists()
print(json.dumps(dict(returncode=result.returncode,verified_at=time.time(),authorization_present=False,
 copy_processes_launched=0,signals_sent=0,broker_control_changes=0,log=str(directory/'native-tests.log'))))
'''
    proof = preparation.native(preparation.parameters(dict(DIRECTORY=str(directory),
        BUNDLE=str(bundle), FIXTURES=str(fixtures), TESTS=list(TESTS))) + code)
    after = preparation.preflight()
    if ([{k:x[k] for k in ('pid','start_ticks','argv')} for x in before['actors']]
            != [{k:x[k] for k in ('pid','start_ticks','argv')} for x in after['actors']]):
        raise RuntimeError('existing node identity changed during validation; no apply')
    _check_progress(before['progress'], after['progress'])
    if before['config_sha256'] != after['config_sha256']:
        raise RuntimeError('configuration changed during validation; no apply')
    write_json(directory / 'validation-after.json', after)
    record = dict(status='controller_native_tests_passed' if proof['returncode']==0 else 'controller_native_tests_failed',
        directory=str(directory), bundle=str(bundle), native_test_proof=proof,
        validation_entry_sha256=preparation.candidate.proof.digest_file(Path(__file__)),
        authorization_created=False, runtime_changes=0, copy_processes_launched=0,
        signals_sent=0, broker_control_changes=0, counters_monotonic=True,
        node_copy_identities_unchanged=True, created=time.time())
    write_json(directory / 'validation.json', record)
    print(json.dumps(record), flush=True)
    if proof['returncode']:
        raise RuntimeError('private native controller tests failed; no authority or copy change')
    return record


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    validate()
