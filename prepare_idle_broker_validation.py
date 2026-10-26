#!/usr/bin/env python3
"""Freeze and validate the broker-client fix beside, never inside, live code.

Run on the bound master only. This preparation may launch private tests and a
bounded synthetic benchmark, but never copy daemons or production requests.
It has no signal, drain, role-write or configuration-write operation.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid

from migration_io import read_json, write_json


CONTROL = Path('/share-mixed/project/.hcr-copy-20261023T141342Z-2dad-d7de938a')
JOB = '2dadf47a-b549-47bb-a267-d56dc877aec9'
AUDIT = Path('/share-mixed/project/.hcr-copy-p4-20261024T015159Z-2dad-26b053') / (
    'p4-copy-nearest-cd98275b19f14511b72dcbbe523da05e') / (
    'controller-e19eacb9908a418281b91b981f33d5d4/expected-after-copy-audit.json')
FILES = ('migration_run.py', 'migration_worker128_run.py', 'migration_worker.py',
         'migration_scan.py', 'migration_io.py', 'test_migration_broker_client.py',
         'benchmark_idle_broker_client.py', 'prepare_idle_broker_validation.py')
PROTECTED = {281673: '78319084', 5834: '73233216', 1856078: '78573242',
             66325: '78960563', 5837: '73233236', 3744900: '77412740', 6304: '73249613'}


def identity(pid):
    path = Path('/proc') / str(pid)
    fields = (path / 'stat').read_text().rsplit(') ', 1)[1].split()
    if fields[0] in ('Z', 'X'):
        raise RuntimeError('protected actor is not live: ' + str(pid))
    return dict(pid=pid, start_ticks=fields[19],
        argv=(path / 'cmdline').read_bytes().rstrip(b'\0').decode().split('\0'))


def before_identities():
    record = read_json(AUDIT)
    row = next(item for item in record['nodes'] if item['node'] == 'master-0')
    values = []
    for item in row['identities']:
        actual = identity(item['pid'])
        if (actual['start_ticks'] != str(item.get('start_ticks', item.get('starttime_ticks')))
                or actual['argv'] != item['argv']):
            raise RuntimeError('copy identity differs before private validation')
        values.append(actual)
    if len(values) != 4:
        raise RuntimeError('exactly four master copy identities required')
    for pid, start in PROTECTED.items():
        actual = identity(pid)
        if actual['start_ticks'] != start:
            raise RuntimeError('protected role differs before private validation')
        values.append(actual)
    return values


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    args = parser.parse_args(argv)
    if socket.gethostname() != 'job-' + JOB + '-master-0':
        raise RuntimeError('private validation is bound to the current master')
    cfg = read_json(CONTROL / 'config.json')
    if cfg.get('job') != JOB or (CONTROL / 'STOP').exists():
        raise RuntimeError('control/job/STOP changed')
    before = before_identities()
    bundle = CONTROL / ('idle-broker-validation-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    hashes = {}
    for name in FILES:
        source = args.repo / name
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        shutil.copyfile(source, bundle / name)
        if hashlib.sha256((bundle / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError('source changed while freezing: ' + name)
        hashes[name] = digest
    write_json(bundle / 'source-sha256.json', hashes)
    write_json(bundle / 'before-identities.json', before)
    print(json.dumps(dict(bundle=str(bundle), phase='private_validation_only')), flush=True)
    started = time.time()
    commands = [
        ['nice', '-n', '10', sys.executable, '-B', '-m', 'unittest', '-v', 'test_migration_broker_client'],
        ['nice', '-n', '10', sys.executable, '-B', 'benchmark_idle_broker_client.py',
         '--output', str(bundle / 'client-cpu.json'), '--threads', '4', '--requests-per-thread', '8',
         '--idle-wait', '1.5']]
    results = []
    for index, command in enumerate(commands):
        completed = subprocess.run(command, cwd=bundle, capture_output=True, text=True, timeout=110)
        log = completed.stdout + completed.stderr
        write_json(bundle / ('private-result-' + str(index) + '.json'),
                   dict(returncode=completed.returncode, output=log, command=command))
        print(log, flush=True)
        if completed.returncode:
            raise RuntimeError('private validation failed; no production mutation performed')
        results.append(dict(returncode=completed.returncode, command=command))
    after = [identity(item['pid']) for item in before]
    if after != before:
        raise RuntimeError('protected identity changed during private validation')
    for name, digest in hashes.items():
        if hashlib.sha256((bundle / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError('frozen source changed during private validation')
    result = dict(status='validated_not_deployed', bundle=str(bundle), started=started,
        finished=time.time(), protected_identities_unchanged=True, before=before, after=after,
        private_results=results, copy_processes_started=0, signals_sent=0,
        production_requests=0, configuration_changes=0, drain_changes=0)
    write_json(bundle / 'preparation.json', result)
    print(json.dumps({k: v for k, v in result.items() if k not in ('before', 'after', 'private_results')}), flush=True)
    return result


if __name__ == '__main__':
    main()
