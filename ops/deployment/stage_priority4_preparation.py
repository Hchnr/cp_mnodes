#!/usr/bin/env python3
"""Stage a NEW immutable readonly P4 preparer; no runtime actor is stopped."""
import argparse
import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

from migration_io import read_json, write_json
from migration_scan_boundary import identity, require_identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, required=True)
    parser.add_argument('--kind', choices=('assignments',), required=True)
    args = parser.parse_args()
    control = args.control.resolve()
    cfg, state = read_json(control / 'config.json'), read_json(control / 'state.json')
    if (cfg['control'] != str(control) or control.parent != Path('/share-mixed/project')
            or socket.gethostname() != 'job-' + cfg['job'] + '-master-0'
            or cfg['migration_scope'] != 'project_priority4'
            or state['stage'] != 'p4-r1' or state['status'] != 'scanning'):
        raise ValueError('actual independent P4 scanning master required')
    producer = identity(3745648)
    expected = ['/usr/bin/python3', '-u', str(control / 'migration_priority4_run.py'), '--control', str(control)]
    if producer['argv'] != expected or producer['start_ticks'] != '77412873':
        raise ValueError('original P4 producer identity differs')
    producer = {key: producer[key] for key in ('pid', 'start_ticks', 'argv')}
    bundle = control / ('assignment-preparation-' + uuid.uuid4().hex)
    bundle.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parent
    files = ['prepare_priority4_assignments.py', 'test_prepare_priority4_assignments.py',
             'migration_priority4_worker.py', 'migration_state_cache.py',
             'migration_scan_boundary.py', 'migration_unix_queues.py', 'migration_io.py']
    hashes = {}
    for name in files:
        payload = (repository / name).read_bytes()
        with open(bundle / name, 'xb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        (bundle / name).chmod(0o444)
        hashes[name] = hashlib.sha256(payload).hexdigest()
    write_json(bundle / 'source-sha256.json', hashes)
    write_json(bundle / 'intent.json', dict(control=str(control), producer=producer,
        runtime_manifest_sha256=hashlib.sha256((control / 'source-sha256.json').read_bytes()).hexdigest(),
        authorizes_handover=False, staged=time.time()))
    subprocess.run(['/usr/bin/python3', '-m', 'unittest', 'test_prepare_priority4_assignments'],
                   cwd=bundle, check=True, timeout=30)
    require_identity(producer)
    with open(bundle / 'controller.log', 'xb') as output:
        process = subprocess.Popen(['/usr/bin/python3', '-u', str(bundle / files[0]),
            '--directory', str(bundle)], stdin=subprocess.DEVNULL, stdout=output,
            stderr=subprocess.STDOUT, start_new_session=True, cwd=bundle)
    actual = identity(process.pid)
    write_json(bundle / 'controller-role.json', dict(pid=process.pid, identity=actual,
                launched=time.time(), process_signals_sent=0))
    print(dict(bundle=str(bundle), process=actual), flush=True)


if __name__ == '__main__':
    main()
