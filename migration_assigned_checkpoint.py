"""Assignment-only adoption cache prepared while the old producer runs.

The full immutable task ledger remains the audit authority. A digest-bound,
primitive-only local set plus a small complete-task delta avoids parsing its
millions of historical entries during the supply handover.
"""
import gc
import hashlib
import os
from pathlib import Path
import pickle

from migration_io import read_json


def digest_file(path):
    value = hashlib.sha256()
    with open(path, 'rb') as stream:
        for piece in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            value.update(piece)
    return value.hexdigest()


class PrimitiveUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        raise ValueError('assignment checkpoint globals are forbidden')


def load_checkpoint(control, cfg, handover):
    control = Path(control).resolve()
    descriptor = Path(handover['assignment_checkpoint']).resolve()
    if not descriptor.is_relative_to(control):
        raise ValueError('assignment checkpoint escapes control')
    meta = read_json(descriptor)
    if (meta.get('version') not in (1, 2) or meta.get('control') != str(control)
            or meta.get('run_id') != cfg['run_id']
            or meta.get('base_ledger') != handover.get('assigned_tasks_path')
            or meta.get('prefix_sha256') != handover.get('assignment_prefix_sha256')):
        raise ValueError('assignment checkpoint identity differs')
    cache = Path(meta['cache'])
    info = cache.lstat()
    if (not cache.is_absolute() or cache.is_symlink() or info.st_uid != os.getuid()
            or info.st_mode & 0o077 or digest_file(cache) != meta['cache_sha256']):
        raise ValueError('assignment cache mode, owner or SHA differs')
    delta = Path(handover['assignment_delta_path']).resolve()
    if (not delta.is_relative_to(control)
            or digest_file(delta) != handover['assignment_delta_sha256']):
        raise ValueError('assignment delta SHA/path differs')
    enabled = gc.isenabled()
    gc.disable()
    try:
        with open(cache, 'rb', buffering=8*1024**2) as stream:
            cached = PrimitiveUnpickler(stream).load()
        task_ids = None
        if meta['version'] == 2:
            if type(cached) is not dict or set(cached) != {'paths', 'task_ids'}:
                raise ValueError('assignment cache fields differ')
            assigned, task_ids = cached['paths'], cached['task_ids']
            if (type(task_ids) is not set or len(task_ids) != meta['prefix_tasks']
                    or any(type(i) is not int or i < 0 for i in task_ids)
                    or max(task_ids, default=-1) != meta['maximum_id']):
                raise ValueError('assignment cache task IDs differ')
        else:
            assigned = cached
        if type(assigned) is not set or len(assigned) != meta['assigned_paths']:
            raise ValueError('assignment cache container/count differs')
        gc.freeze()
    finally:
        if enabled:
            gc.enable()
    return assigned, meta['maximum_id'], delta, task_ids
