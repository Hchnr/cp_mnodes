"""Read-only directory census for control queues and issue-file names/sizes.

No JSON or payload file is opened. Anchored directory FDs refuse symlinks and
detect path replacement. Errors are explicit, never empty/zero observations.
These are sequential observations, not an atomic broker/filesystem snapshot.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import re
import stat
import time


class _Census:
    def __init__(self, control):
        self.control = Path(control)
        self.errors = []

    def error(self, path, exc):
        self.errors.append({'path': str(path), 'error': type(exc).__name__,
                            'detail': str(exc)})

    @contextmanager
    def directory(self, name, parent=None, path=None, optional=False):
        path = Path(name) if path is None else path
        fd = None
        try:
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, 'O_CLOEXEC', 0)
            try:
                fd = os.open(name, flags, dir_fd=parent)
            except FileNotFoundError:
                if not optional:
                    raise
                yield None
                return
            before = os.fstat(fd)
            yield fd
            after = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISDIR(after.st_mode) or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
                raise RuntimeError('directory path exchanged during control census')
        except OSError as exc:
            self.error(path, exc)
            if fd is None:
                yield None
        except RuntimeError as exc:
            self.error(path, exc)
        finally:
            if fd is not None:
                os.close(fd)

    def names(self, fd):
        with os.scandir(fd) as entries:
            return sorted(entry.name for entry in entries)

    def queue_files(self, fd, path):
        names = self.names(fd)
        return {'json_files': sum(name.endswith('.json') for name in names),
                'other_entries': [name for name in names if name != '.snapshots' and not name.endswith('.json')]}


def observe_stage_queues(control):
    census = _Census(control)
    started = time.time()
    stages = {}
    with census.directory(control) as root:
        if root is not None:
            with census.directory('stages', root, Path(control) / 'stages') as stage_root:
                if stage_root is not None:
                    for stage in census.names(stage_root):
                        if stage == '.snapshots':
                            continue
                        path = Path(control) / 'stages' / stage
                        record = {'pending': None, 'running': None, 'other_entries': [], 'errors': []}
                        stages[stage] = record
                        begin_errors = len(census.errors)
                        if not re.fullmatch(r'p[1-4]-r[1-3]', stage):
                            census.error(path, ValueError('unrecognized stage directory'))
                        with census.directory(stage, stage_root, path) as stage_fd:
                            if stage_fd is not None:
                                with census.directory('pending', stage_fd, path / 'pending') as pending:
                                    if pending is not None:
                                        count = 0
                                        shard_names = census.names(pending)
                                        expected = {str(i) for i in range(64)}
                                        extras = set(shard_names) - expected - {'.snapshots'}
                                        record['other_entries'].extend('pending/' + name for name in sorted(extras))
                                        for shard in range(64):
                                            with census.directory(str(shard), pending, path / 'pending' / str(shard)) as shard_fd:
                                                if shard_fd is not None:
                                                    value = census.queue_files(shard_fd, path / 'pending' / str(shard))
                                                    count += value['json_files']
                                                    record['other_entries'].extend('pending/%d/%s' % (shard, name) for name in value['other_entries'])
                                        record['pending'] = count
                                with census.directory('running', stage_fd, path / 'running') as running:
                                    if running is not None:
                                        value = census.queue_files(running, path / 'running')
                                        record['running'] = value['json_files']
                                        expected = {str(i) for i in range(64)}
                                        for name in value['other_entries']:
                                            if name not in expected:
                                                record['other_entries'].append('running/' + name)
                                                continue
                                            shard_path = path / 'running' / name
                                            with census.directory(name, running, shard_path) as shard_fd:
                                                if shard_fd is not None:
                                                    shard = census.queue_files(shard_fd, shard_path)
                                                    record['running'] += shard['json_files']
                                                    record['other_entries'].extend(
                                                        'running/%s/%s' % (name, entry)
                                                        for entry in shard['other_entries'])
                        record['errors'] = list(census.errors[begin_errors:])
                        if record['errors']:
                            record['pending'] = record['running'] = None
    empty = not census.errors and all(record['pending'] == 0 and record['running'] == 0
                                      and not record['other_entries'] for record in stages.values())
    return {'started_at': started, 'finished_at': time.time(), 'stages': stages,
            'complete': not census.errors, 'errors': census.errors,
            'all_queues_empty': empty,
            'atomic_snapshot': False, 'payload_or_json_opened': False}


def observe_issue_files(control):
    census = _Census(control)
    started = time.time()
    files = []

    def issue(fd, name, path, kind):
        try:
            value = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISREG(value.st_mode):
                raise ValueError('issue entry is not a regular file')
            files.append({'path': str(path), 'bytes': value.st_size,
                          'mtime': value.st_mtime, 'kind': kind})
        except (OSError, ValueError) as exc:
            census.error(path, exc)

    with census.directory(control) as root:
        if root is not None:
            for name in census.names(root):
                if name.endswith('-issues.jsonl'):
                    issue(root, name, Path(control) / name, 'stage_or_finalizer_jsonl')
            with census.directory('metadata', root, Path(control) / 'metadata', optional=True) as metadata:
                if metadata is not None:
                    for label in census.names(metadata):
                        if label == '.snapshots':
                            continue
                        path = Path(control) / 'metadata' / label
                        with census.directory(label, metadata, path) as job:
                            if job is None:
                                continue
                            for name in census.names(job):
                                if name.endswith('issues.jsonl'):
                                    issue(job, name, path / name, 'metadata_jsonl')
                            with census.directory('issues', job, path / 'issues', optional=True) as issues:
                                if issues is not None:
                                    for name in census.names(issues):
                                        if name.endswith('.json'):
                                            issue(issues, name, path / 'issues' / name, 'metadata_issue_batch_json')
    return {'started_at': started, 'finished_at': time.time(), 'files': files,
            'complete': not census.errors, 'errors': census.errors,
            'atomic_snapshot': False, 'payload_or_json_opened': False}
