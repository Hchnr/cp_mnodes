#!/usr/bin/env python3
"""Freeze/test the second P4 canary without authorizing or launching copy.

Only private source/fixture/log/preparation files are written. Native process
identities and the prior bundle are read/verified before and after tests. No
checkpoint, role/configuration change, broker POST, drain or signal is issued.
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
from migration_cached_worker import _check_progress
from migration_priority4_worker import ScopeGuard
import migration_priority4_nearest_worker as candidate


CONTROL = Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053')
PREVIOUS = CONTROL / candidate.PREVIOUS_NAME
BASELINE = PREVIOUS / candidate.PREVIOUS_CONTROLLER / 'after-copy-audit.json'
HELPERS = '/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a/fast-p3-scan-466d08d7ea874d91a73534fb5caabac5'
TESTS = ('test_migration_priority4_nearest_worker', 'test_migration_parent_frontier',
         'test_migration_priority4_path_worker', 'test_migration_priority4_worker',
         'test_migration_worker_directory_fastpath', 'test_migration_worker_native_open',
         'test_migration_progress_checkpoint')


def native(code):
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
        '-o', 'ForwardAgent=no', '-o', 'ForwardX11=no', '-o', 'ServerAliveInterval=5',
        '-o', 'ServerAliveCountMax=2', 'worker-10', 'python3', '-B', '-u', '-'],
        input=code, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
    if result.returncode:
        raise RuntimeError('native preparation failed, no apply: ' + result.stderr)
    return json.loads(result.stdout)


def parameters(values):
    return '\n'.join(key + '=' + repr(value) for key, value in values.items()) + '\n'


def preparation_record(bundle, policy, test_proof):
    return dict(status='prepared_native_tests_passed' if test_proof['returncode']==0 else 'prepared_native_tests_failed',
        bundle=str(bundle), policy=policy, native_test_proof=test_proof,
        preparation_entry_sha256=candidate.proof.digest_file(Path(__file__)),
        copy_processes_launched=0, signals_sent=0, broker_control_changes=0,
        node_copy_identities_unchanged=True, counters_monotonic=True,
        authorization_for_copy_replacement=False, created=time.time())


def preflight():
    code = '''import sys,json,time,socket,hashlib,datetime,os,stat
from pathlib import Path
sys.path[:0]=[PREVIOUS,HELPERS]
from migration_io import read_json
from migration_scan_boundary import require_identity
from migration_cached_worker import _check_progress
import migration_priority4_path_worker as proof
control=Path(CONTROL); host=socket.gethostname()
assert host=='job-2dadf47a-b549-47bb-a267-d56dc877aec9-worker-10'
base=read_json(Path(BASELINE)); row=next(x for x in base['nodes'] if x['node']=='worker-10')
actors=[]
for item in row['identities']:
 expected=dict(pid=item['pid'],start_ticks=str(item.get('start_ticks',item.get('starttime_ticks'))),argv=item['argv'])
 actors.append(require_identity(expected))
assert len(actors)==4
cfg,_=proof.validate_bundle(control,Path(PREVIOUS),'supplement')
state=read_json(control/'state.json'); assert (state['status'],state['stage'])==('scanning','p4-r1')
assert not (control/'STOP').exists()
n=read_json(control/'nodes-extra'/(host+'.json'))
assert not n.get('thread_error') and not n.get('ended')
assert 0<=time.time()-datetime.datetime.fromisoformat(n['updated']).timestamp()<=15
target=next(x for x in actors if x['pid']==n['pid'])
target=dict(pid=target['pid'],starttime_ticks=target['start_ticks'],argv=target['argv'])
assert target==EXPECTED
assert n['threads']==64 and n['io_bytes_per_second']==cfg['supplement_io_bytes_per_second']
assert os.getpriority(os.PRIO_PROCESS,n['pid'])==0
named=Path(LOG).lstat(); assert stat.S_ISREG(named.st_mode)
for fd in ('1','2'):
 path=Path('/proc')/str(n['pid'])/'fd'/fd
 assert os.readlink(path)==LOG
 actual=path.stat(); assert (actual.st_dev,actual.st_ino)==(named.st_dev,named.st_ino)
 flags=int(next(line.split()[1] for line in (Path('/proc')/str(n['pid'])/'fdinfo'/fd).read_text().splitlines() if line.startswith('flags:')),8)
 assert flags&os.O_ACCMODE==os.O_WRONLY and flags&os.O_APPEND
progress={key:n[key] for key in ('pid','host','bytes','streamed_bytes','verified_files','verified_bytes','counts','stages','run_started','completed_batches','broker_retries')}
_check_progress(progress,progress)
print(json.dumps(dict(host=host,verified_at=time.time(),actors=actors,target=target,progress=progress,
 threads=n['threads'],io_bytes_per_second=n['io_bytes_per_second'],
 predecessor_stdout_log=LOG,
 config_sha256=hashlib.sha256(json.dumps(cfg,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
 signals_sent=0,copy_processes_launched=0,broker_control_changes=0)))
'''
    return native(parameters(dict(CONTROL=str(CONTROL), PREVIOUS=str(PREVIOUS),
        BASELINE=str(BASELINE), HELPERS=HELPERS,
        LOG=str(PREVIOUS / candidate.PREVIOUS_CONTROLLER / 'worker.log'),
        EXPECTED=candidate.expected_predecessor(CONTROL, '2dadf47a-b549-47bb-a267-d56dc877aec9'))) + code)


def prepare():
    if CONTROL.is_symlink() or PREVIOUS.is_symlink():
        raise ValueError('real current P4 control and first-canary bundle required')
    cfg = read_json(CONTROL / 'config.json')
    ScopeGuard(str(CONTROL), cfg)
    if cfg['job'] != '2dadf47a-b549-47bb-a267-d56dc877aec9':
        raise ValueError('exact current cluster required')
    before = preflight()
    policy = candidate.policy_for(CONTROL, cfg)
    if before['config_sha256'] != policy['config_sha256']:
        raise RuntimeError('native/development configuration differs; no preparation')
    previous = candidate.proof.static_json(PREVIOUS / 'source-sha256.json')
    bundle = CONTROL / ('p4-copy-nearest-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parent
    hashes = {}
    for name in sorted(set(previous) | (candidate.ADDITIONS - {candidate.POLICY})):
        source = PREVIOUS / name if name in previous else repository / name
        digest = candidate.proof.digest_file(source)
        if Path(name).name != name or (name in previous and digest != previous[name]):
            raise ValueError('inherited source manifest differs')
        with open(bundle / name, 'xb') as output:
            output.write(source.read_bytes())
            output.flush()
            os.fsync(output.fileno())
        (bundle / name).chmod(0o444)
        hashes[name] = candidate.proof.digest_file(bundle / name)
        if hashes[name] != digest:
            raise RuntimeError('source changed during freezing')
    write_json(bundle / candidate.POLICY, policy)
    (bundle / candidate.POLICY).chmod(0o444)
    hashes[candidate.POLICY] = candidate.proof.digest_file(bundle / candidate.POLICY)
    write_json(bundle / 'source-sha256.json', hashes)
    (bundle / 'source-sha256.json').chmod(0o444)
    write_json(bundle / 'preparation-before.json', before)
    fixtures = bundle / ('native-tests-' + uuid.uuid4().hex)
    fixtures.mkdir(mode=0o700)
    test_hashes = {}
    for name in TESTS:
        filename = name + '.py'
        shutil.copyfile(repository / filename, fixtures / filename)
        (fixtures / filename).chmod(0o444)
        test_hashes[filename] = candidate.proof.digest_file(fixtures / filename)
    write_json(fixtures / 'test-source-sha256.json', test_hashes)
    code = '''import sys,os,json,subprocess,time
from pathlib import Path
sys.path.insert(0,BUNDLE)
import migration_priority4_nearest_worker as candidate
candidate.validate_bundle(Path(CONTROL),Path(BUNDLE),'supplement')
candidate.validate_imports(Path(BUNDLE))
with open(Path(BUNDLE)/'native-tests.log','xb') as output:
 result=subprocess.run([sys.executable,'-B','-m','unittest']+TESTS,cwd=FIXTURES,
  env=dict(os.environ,PYTHONPATH=BUNDLE,PYTHONDONTWRITEBYTECODE='1'),stdout=output,stderr=subprocess.STDOUT,timeout=120)
print(json.dumps(dict(returncode=result.returncode,verified_at=time.time(),signals_sent=0,
 copy_processes_launched=0,broker_control_changes=0,log=str(Path(BUNDLE)/'native-tests.log'))))
'''
    test_proof = native(parameters(dict(BUNDLE=str(bundle), FIXTURES=str(fixtures),
        CONTROL=str(CONTROL), TESTS=list(TESTS))) + code)
    after = preflight()
    if ([{key:x[key] for key in ('pid','start_ticks','argv')} for x in before['actors']]
            != [{key:x[key] for key in ('pid','start_ticks','argv')} for x in after['actors']]):
        raise RuntimeError('protected node identity changed; no apply')
    _check_progress(before['progress'], after['progress'])
    if after['config_sha256'] != before['config_sha256']:
        raise RuntimeError('configuration changed during preparation')
    write_json(bundle / 'preparation-after.json', after)
    record = preparation_record(bundle, policy, test_proof)
    write_json(bundle / 'preparation.json', record)
    print(json.dumps(record), flush=True)
    if test_proof['returncode']:
        raise RuntimeError('native fixture tests failed; copy remains untouched')
    return record


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    prepare()
