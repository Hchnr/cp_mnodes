"""Durable, grouped scheduling acknowledgements, never data-copy receipts.

Original discovery files remain immutable in ready/. A group may be recorded
only after its probe/publication barrier. On adoption, a group is reusable only
when its predecessor assignment basis AND every subsequently allocated task ID
through its barrier still exist in the adopted pending/claim ledger. Otherwise
its hints stay eligible for re-offering; an acknowledgement cannot hide a lost
assignment or stand in for final content verification.
"""
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import uuid


SCOPE = 'frontier-input-publication-receipts-v1'


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii')


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish_unique(path, record):
    """Fully write/fsync before atomic no-replace publication, then sync parent."""
    payload = _bytes({'record': record, 'sha256': hashlib.sha256(_bytes(record)).hexdigest()})
    temporary = path.parent / ('.' + uuid.uuid4().hex + '.tmp')
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        _sync_directory(path.parent)
    finally:
        # Only our private temporary link, never a candidate or final receipt.
        temporary.unlink(missing_ok=True)


def _identifier(value):
    return (isinstance(value, str) and len(value) == 32
            and all(c in '0123456789abcdef' for c in value))


class InputReceipts:
    def __init__(self, owner):
        self.control = owner.control
        self.run_id = owner.cfg['run_id']
        self.hosts = frozenset(owner.cfg['copy_hosts'])
        if type(getattr(owner, 'adopted_task_ids', None)) is not set:
            raise ValueError('group acknowledgements require the complete adopted task ID set')
        self.task_ids = owner.adopted_task_ids
        self.ordered_ids = sorted(self.task_ids)
        self.floor = owner.next_task_id
        if (type(self.floor) is not int or self.floor < 0
                or any(type(i) is not int or not 0 <= i < self.floor for i in self.task_ids)):
            raise ValueError('invalid adopted task IDs or allocation floor')
        self.root = self.control / 'frontier-input' / 'acknowledgements-v1'
        self.bases, self.groups = self.root / 'bases', self.root / 'groups'
        self.root.mkdir(parents=True, exist_ok=True)
        self.bases.mkdir(exist_ok=True)
        self.groups.mkdir(exist_ok=True)
        baseline = self.root / 'baseline.json'
        if not baseline.exists():
            if any(self.groups.iterdir()) or any(self.bases.iterdir()):
                raise ValueError('group acknowledgement baseline is missing')
            previous = owner.state.get('priority_inputs', {})
            publish_unique(baseline, self._record('baseline',
                files=previous.get('files_consumed', 0), entries=previous.get('entries_consumed', 0)))
            _sync_directory(self.root.parent)
        initial = self.read(baseline, 'baseline')
        if any(type(initial.get(k)) is not int or initial[k] < 0 for k in ('files', 'entries')):
            raise ValueError('invalid group acknowledgement baseline')
        self.base_files, self.base_entries = initial['files'], initial['entries']
        self.known = {}  # Path -> raw candidate entry count; mutated by owner only.
        self.entries = self.replay_groups = self.loaded_groups = 0
        basis_cache = {}
        with os.scandir(self.groups) as stream:
            paths = []
            for entry in stream:
                if not entry.name.endswith('.json'):
                    continue
                if not _identifier(entry.name[:-5]) or not entry.is_file(follow_symlinks=False):
                    raise ValueError('invalid group acknowledgement file')
                paths.append(Path(entry.path))
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix='input-ack-restore') as pool:
            for record in pool.map(lambda p: self.read(p, 'group'), paths):
                refs = self._validate_group(record)
                basis = record['basis']
                if basis not in basis_cache:
                    basis_cache[basis] = self._basis_backed(basis, record['floor'])
                backed, floor = basis_cache[basis]
                if floor != record['floor']:
                    raise ValueError('acknowledgement basis floor changed')
                if not backed or not self._range_backed(record['floor'], record['barrier']):
                    self.replay_groups += 1
                    continue
                self.accept(refs)
                self.loaded_groups += 1
        self.basis = uuid.uuid4().hex
        publish_unique(self.bases / (self.basis + '.json'), self._record('basis',
            floor=self.floor, task_ids=self.ordered_ids))

    def _record(self, kind, **values):
        return dict(scope=SCOPE, control=str(self.control), run_id=self.run_id,
                    kind=kind, **values)

    def read(self, path, kind):
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            value = json.load(stream)
        if (not isinstance(value, dict) or set(value) != {'record', 'sha256'}
                or not isinstance(value['record'], dict)
                or hashlib.sha256(_bytes(value['record'])).hexdigest() != value['sha256']):
            raise ValueError('input acknowledgement checksum differs')
        record = value['record']
        if any(record.get(k) != v for k, v in self._record(kind).items()):
            raise ValueError('input acknowledgement identity differs')
        return record

    def _range_backed(self, lower, upper):
        if upper < lower:
            return upper == lower - 1
        return (bisect_right(self.ordered_ids, upper) - bisect_left(self.ordered_ids, lower)
                == upper - lower + 1)

    def _basis_backed(self, basis, floor):
        try:
            record = self.read(self.bases / (basis + '.json'), 'basis')
        except FileNotFoundError:
            return False, floor
        ids = record.get('task_ids')
        if (type(record.get('floor')) is not int or record['floor'] != floor or not isinstance(ids, list)
                or any(type(i) is not int or not 0 <= i < floor for i in ids)
                or any(b <= a for a, b in zip(ids, ids[1:]))):
            raise ValueError('invalid acknowledgement assignment basis')
        return self.task_ids.issuperset(ids), floor

    def _validate_group(self, record):
        if (not _identifier(record.get('basis')) or type(record.get('floor')) is not int
                or record['floor'] < 0 or type(record.get('barrier')) is not int
                or record['barrier'] < record['floor'] - 1
                or not isinstance(record.get('files'), list)
                or not 1 <= len(record['files']) <= 4096):
            raise ValueError('invalid input acknowledgement group')
        refs, seen = [], set()
        for row in record['files']:
            if (not isinstance(row, list) or len(row) != 3
                    or not isinstance(row[0], str) or row[0] not in self.hosts
                    or not isinstance(row[1], str) or Path(row[1]).name != row[1]
                    or not row[1].endswith('.json') or row[1].startswith('.')
                    or '\x00' in row[1] or type(row[2]) is not int or not 0 <= row[2] <= 512):
                raise ValueError('invalid input acknowledgement reference')
            path = self.control / 'frontier-input' / row[0] / 'ready' / row[1]
            if path in seen:
                raise ValueError('duplicate candidate within acknowledgement')
            seen.add(path)
            refs.append((path, row[2]))
        return refs

    def commit(self, rows, barrier):
        """Background file I/O only; caller has already established the barrier."""
        record = self._record('group', basis=self.basis, floor=self.floor,
            barrier=barrier, files=[[row[0].parent.parent.name, row[0].name, row[3]] for row in rows])
        refs = self._validate_group(record)
        if any(row[0] != ref[0] for row, ref in zip(rows, refs)):
            raise ValueError('acknowledgement source escaped this control')
        path = self.groups / (uuid.uuid4().hex + '.json')
        publish_unique(path, record)
        return path

    def accept(self, refs):
        for path, count in refs:
            previous = self.known.get(path)
            if previous is not None and previous != count:
                raise ValueError('immutable candidate entry count changed')
            if previous is None:
                self.known[path] = count
                self.entries += count

    @property
    def files(self):
        return self.base_files + len(self.known)

    @property
    def total_entries(self):
        return self.base_entries + self.entries
