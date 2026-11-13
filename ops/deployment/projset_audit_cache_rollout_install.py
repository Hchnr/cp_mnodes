#!/usr/bin/env python3
"""Install the live cache setting only while the audit is fenced and backed up."""
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
OLD_SHA = '5c89288da331bbf7e3877cc0652a21a3f97e174fd783d2a66fe1453be0884483'
NEW_SHA = '48ab55dd95fafbc19d7cad98909253a4bc49fd746f1cc3c5d971bf72d6eae625'
OLD_TUNING = {'version': 1, 'default': {'threads': 10, 'prefetch': 2},
              'nodes': {'cpu-worker-2': {'threads': 13}}}
NEW_TUNING = dict(OLD_TUNING, controller={'sqlite_cache_mib': 512})


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate_node(topology, config):
    expected = config['nodes']['cpu-worker-0']
    if (socket.gethostname() != topology['hosts']['cpu-worker-0'] or
            os.stat('/share-mixed/projset').st_ino != expected['source_ino'] or
            os.stat('/share-mixed/project').st_ino != expected['target_ino']):
        raise RuntimeError('node or mount identity changed')


def rollback_path(stage):
    return Path('/var/tmp') / ('projset-audit-rollback-' + stage.name) / 'audit.sqlite3'


def replace(path, data, mode=0o644):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def install(rollback=False):
    validate_node(json.loads((RUN / 'recovery/ssh.json').read_text()),
                  json.loads((RUN / 'config.json').read_text()))
    if (WORK / 'STOP').read_text().strip() != FENCE:
        raise RuntimeError('upgrade fence is not present')
    old_code = (STAGE / 'old-code.py').read_bytes()
    new_code = (STAGE / 'new-code.py').read_bytes()
    old_manifest = json.loads((STAGE / 'old-manifest.json').read_text())
    if (digest(old_code) != OLD_SHA or digest(new_code) != NEW_SHA or
            old_manifest.get('code_sha256') != OLD_SHA):
        raise RuntimeError('staged code or manifest changed')
    ready = json.loads((STAGE / 'snapshot-ready.json').read_text())
    local = rollback_path(STAGE)
    info = local.lstat()
    if (ready.get('state') != 'ready' or ready.get('quick_check') != 'ok' or
            ready.get('host') != socket.gethostname() or ready.get('local') != str(local) or
            not stat.S_ISREG(info.st_mode) or info.st_size != ready.get('size') or
            not isinstance(ready.get('sha256'), str) or len(ready['sha256']) != 64):
        raise RuntimeError('verified local rollback snapshot unavailable')
    with (WORK / 'controller.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (WORK / 'STOP').read_text().strip() != FENCE:
            raise RuntimeError('upgrade fence changed')
        current_code = (WORK / 'code/projset_full_audit.py').read_bytes()
        current_manifest = json.loads((WORK / 'manifest.json').read_text())
        current_sha = current_manifest.pop('code_sha256', None)
        baseline = dict(old_manifest)
        baseline.pop('code_sha256')
        current_tuning = json.loads((WORK / 'tuning.json').read_text())
        if (digest(current_code) not in (OLD_SHA, NEW_SHA) or current_sha not in (OLD_SHA, NEW_SHA) or
                current_manifest != baseline or current_tuning not in (OLD_TUNING, NEW_TUNING)):
            raise RuntimeError('running code, manifest or tuning changed unexpectedly')
        desired_sha = OLD_SHA if rollback else NEW_SHA
        desired_tuning = OLD_TUNING if rollback else NEW_TUNING
        new_manifest = dict(old_manifest, code_sha256=desired_sha)
        # The fence and lock prevent either half of an interrupted replacement
        # from being executed. A retry repairs an incomplete replacement.
        replace(WORK / 'code/projset_full_audit.py', old_code if rollback else new_code)
        replace(WORK / 'manifest.json', (json.dumps(new_manifest, indent=2) + '\n').encode())
        replace(WORK / 'tuning.json', (json.dumps(desired_tuning, sort_keys=True) + '\n').encode())
        result = {'state': 'rolled_back' if rollback else 'installed', 'at': time.time(),
                  'code_sha256': desired_sha, 'snapshot': str(local)}
        replace(STAGE / 'install-state.json', (json.dumps(result, sort_keys=True) + '\n').encode(), 0o600)
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rollback', action='store_true')
    install(parser.parse_args().rollback)
