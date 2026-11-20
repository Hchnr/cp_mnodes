#!/usr/bin/env python3
"""Install or roll back a staged scheduler while the audit is fenced."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import time
import uuid


STAGE = Path(__file__).resolve().parent
WORK = STAGE.parent
RUN = WORK.parent.parent
FENCE = STAGE.name
EXPECTED_NEW_SHA = '5c89288da331bbf7e3877cc0652a21a3f97e174fd783d2a66fe1453be0884483'


def validate_node(topology, config):
    expected = config['nodes']['cpu-worker-0']
    if (socket.gethostname() != topology['hosts']['cpu-worker-0'] or
            os.stat('/share-mixed/projset').st_ino != expected['source_ino'] or
            os.stat('/share-mixed/project').st_ino != expected['target_ino']):
        raise RuntimeError('node or mount identity changed')


def digest(value):
    return hashlib.sha256(value).hexdigest()


def rollback_path(stage):
    return Path('/var/tmp') / ('projset-audit-rollback-' + stage.name) / 'audit.sqlite3'


def replace(path, data, mode=0o644):
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def install(rollback=False):
    topology = json.loads((RUN / 'recovery/ssh.json').read_text())
    config = json.loads((RUN / 'config.json').read_text())
    validate_node(topology, config)
    if (WORK / 'STOP').read_text().strip() != FENCE:
        raise RuntimeError('upgrade fence is not present')
    old_code = (STAGE / 'old-code.py').read_bytes()
    new_code = (STAGE / 'new-code.py').read_bytes()
    old_manifest = json.loads((STAGE / 'old-manifest.json').read_text())
    old_sha = digest(old_code)
    if old_manifest.get('code_sha256') != old_sha or digest(new_code) != EXPECTED_NEW_SHA:
        raise RuntimeError('staged code or manifest changed')
    ready = json.loads((STAGE / 'snapshot-ready.json').read_text())
    local = rollback_path(STAGE)
    info = local.lstat()
    if (ready.get('state') != 'ready' or ready.get('quick_check') != 'ok' or
            ready.get('host') != socket.gethostname() or ready.get('local') != str(local) or
            not stat.S_ISREG(info.st_mode) or info.st_size != ready.get('size') or
            not isinstance(ready.get('sha256'), str) or len(ready['sha256']) != 64):
        raise RuntimeError('verified local rollback snapshot unavailable')
    desired_tuning = (STAGE / 'tuning.json').read_bytes()
    if json.loads(desired_tuning) != {'version': 1, 'default': {'threads': 8, 'prefetch': 2}}:
        raise RuntimeError('unexpected staged tuning')
    with (WORK / 'controller.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (WORK / 'STOP').read_text().strip() != FENCE:
            raise RuntimeError('upgrade fence changed')
        current_code = (WORK / 'code/projset_full_audit.py').read_bytes()
        current_manifest = json.loads((WORK / 'manifest.json').read_text())
        actual_sha = current_manifest.pop('code_sha256', None)
        baseline = dict(old_manifest)
        baseline.pop('code_sha256', None)
        if (digest(current_code) not in (old_sha, EXPECTED_NEW_SHA) or
                actual_sha not in (old_sha, EXPECTED_NEW_SHA) or current_manifest != baseline):
            raise RuntimeError('running code or manifest changed unexpectedly')
        new_manifest = dict(old_manifest)
        new_manifest['code_sha256'] = old_sha if rollback else EXPECTED_NEW_SHA
        tuning_path = WORK / 'tuning.json'
        existing_tuning = tuning_path.read_bytes() if tuning_path.exists() else None
        if not rollback and existing_tuning not in (None, desired_tuning):
            raise RuntimeError('existing tuning differs from staged tuning')
        # STOP and the controller lock prevent either half of this pair from
        # being executed if the process is interrupted between replacements.
        replace(WORK / 'code/projset_full_audit.py', old_code if rollback else new_code)
        replace(WORK / 'manifest.json', (json.dumps(new_manifest, indent=2) + '\n').encode())
        if rollback:
            # Preserve an operator's later online adjustment. The old
            # controller ignores this file and it remains available for the
            # next upgrade.
            if existing_tuning == desired_tuning:
                tuning_path.unlink()
        else:
            replace(tuning_path, desired_tuning)
        result = {'state': 'rolled_back' if rollback else 'installed', 'at': time.time(),
                  'code_sha256': new_manifest['code_sha256'], 'snapshot': str(local)}
        replace(STAGE / 'install-state.json', (json.dumps(result, sort_keys=True) + '\n').encode(), 0o600)
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rollback', action='store_true')
    install(parser.parse_args().rollback)
