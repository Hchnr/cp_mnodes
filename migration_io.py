"""JSON publication without replacing an inode that another client may read.

Mutable logical paths use immutable, never-pruned version files. Canonical JSON
is created once for older tooling and then remains unchanged. All participating
readers must use ``read_json``. Each logical path should have one publisher.
"""

import errno
import json
import os
import pathlib
import re
import time
import uuid


_MUTABLE_NAMES = frozenset(('state.json', 'progress.json', 'training-health.json',
                            'active.json', 'inventory.json', 'metrics.json',
                            'metrics-history.json'))
_VERSION = re.compile(r'^[0-9]{20}-[0-9a-f]{32}\.json$')
_HOUR = re.compile(r'^[0-9]{10}$')
_MINUTE = re.compile(r'^[0-5][0-9]$')


def _snapshot_dir(path):
    return path.parent / '.snapshots' / path.name


def _is_mutable(path):
    path = pathlib.Path(path)
    return (path.name in _MUTABLE_NAMES or path.parent.name == 'nodes' or
            _snapshot_dir(path).exists() or path.exists() or path.is_symlink())


def _latest_snapshot(directory):
    """Return a relative version path, skipping empty new hour/minute buckets.

    Root-level flat versions remain readable. New writes are partitioned by
    UTC hour and minute, so routine reads list at most a few small directories.
    """
    flat = None
    hours = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if _VERSION.fullmatch(entry.name) and (flat is None or entry.name > flat):
                flat = entry.name
            elif _HOUR.fullmatch(entry.name):
                hours.append(entry.name)
    bucketed = None
    for hour in sorted(hours, reverse=True):
        try:
            with os.scandir(directory / hour) as entries:
                minutes = sorted((entry.name for entry in entries
                                  if _MINUTE.fullmatch(entry.name)), reverse=True)
        except (FileNotFoundError, NotADirectoryError):
            continue
        for minute in minutes:
            newest = None
            try:
                with os.scandir(directory / hour / minute) as entries:
                    for entry in entries:
                        if _VERSION.fullmatch(entry.name) and (newest is None or entry.name > newest):
                            newest = entry.name
            except (FileNotFoundError, NotADirectoryError):
                continue
            if newest is not None:
                bucketed = pathlib.Path(hour) / minute / newest
                break
        if bucketed is not None:
            break
    if flat is not None and (bucketed is None or flat > bucketed.name):
        return pathlib.Path(flat)
    return bucketed


def _read_file(path):
    with open(path, encoding='utf-8') as handle:
        return json.load(handle)


def read_json(path):
    """Read latest immutable version, or a never-versioned legacy/static file.

    Once the snapshot directory exists, canonical JSON is never opened, even
    while the directory is empty. Brief retries cover publication visibility
    delays; errors remain visible to callers rather than using stale canonical
    data. Immutable versions are never unlinked by this module.
    """
    path = pathlib.Path(path)
    directory = _snapshot_dir(path)
    snapshot_mode = False
    last_error = None
    for attempt in range(3):
        try:
            try:
                newest = _latest_snapshot(directory)
                snapshot_mode = True
            except FileNotFoundError:
                if not snapshot_mode:
                    return _read_file(path)
                raise
            if newest is None:
                raise FileNotFoundError(errno.ENOENT, 'no published JSON snapshot', str(directory))
            return _read_file(directory / newest)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.02 * (attempt + 1))
    raise last_error


def _publish_unique(path, payload):
    """Publish at a new unique path; its final inode is never replaced later."""
    temporary = path.with_name('.' + path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with open(temporary, 'x', encoding='utf-8') as handle:
            handle.write(payload)
        os.rename(temporary, path)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def write_json(path, data, *, ensure_parent=True):
    """Publish JSON and return its actual immutable path.

    Known mutable paths use snapshots from their first write. Rewriting any
    other existing path upgrades it to snapshots instead of replacing it.
    Queue/config files first published at absent paths keep their usual paths.
    A caller that already prepared a trusted parent may skip its redundant
    mkdir; publication then fails normally if that parent is missing.
    """
    path = pathlib.Path(path)
    if ensure_parent:
        path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=True) + '\n'
    directory = _snapshot_dir(path)
    if not _is_mutable(path):
        _publish_unique(path, payload)
        return path
    directory.mkdir(parents=True, exist_ok=True)
    latest = _latest_snapshot(directory)
    version = time.time_ns()
    if latest is not None:
        version = max(version, int(latest.name[:20]) + 1)
    bucket = directory / time.strftime('%Y%m%d%H', time.gmtime(version // 10**9)) / time.strftime(
        '%M', time.gmtime(version // 10**9))
    bucket.mkdir(parents=True, exist_ok=True)
    published = bucket / (str(version).zfill(20) + '-' + uuid.uuid4().hex + '.json')
    _publish_unique(published, payload)
    # Compatibility file is written once and is never unlinked or replaced.
    # Participating readers already see the snapshot directory and ignore it.
    try:
        with open(path, 'x', encoding='utf-8') as handle:
            handle.write(payload)
    except FileExistsError:
        pass
    return published
