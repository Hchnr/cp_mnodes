"""UNDEPLOYED, payload-read-only P4 hardlink content/ownership verifier.

This is not a finalizer: it never creates links, replaces files, creates target
directories, changes metadata, or writes an index. verify_with_metadata binds
Linux POSIX ACL/xattr observations to the current checksums through the same
scope-checked FDs. Publication still requires dependency checks, remaining
metadata/atime validation, immediate rechecks, and final inode snapshots.
A returned proof alone never authorizes publication.

Use only in a dedicated, serial coordinator helper after predecessor acceptance.
The original SHA reader charges all source/target reads to one 32 MiB/s budget.
"""
import contextlib
import copy
import fcntl
import os
from pathlib import Path
import re
import stat
import threading

import migration_worker as engine
from migration_priority4_worker import ScopeGuard, components


HASH_BUDGET = 32 * 1024 * 1024
_SHA = re.compile(r'[0-9a-fA-F]{64}\Z')
_PROCESS_SESSION = threading.Lock()


def digest(value):
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise ValueError('hardlink proof requires a SHA256 digest')
    return value.lower()


def proof_scope(proof, control, cfg, rel, cross_eai=False):
    if not isinstance(proof, dict) or proof.get('kind') != 'file':
        raise ValueError('hardlink proof must describe a regular file')
    expected = (cfg['source'], cfg['destination'], rel)
    if cross_eai:
        if proof.get('control') not in cfg['covered_by_controls']:
            raise ValueError('cross-eai canonical proof is outside covered controls')
        expected = (cfg['source'] + '/eai_pwm', cfg['destination'] + '/eai_pwm',
                    rel[len('eai_pwm/'):])
    elif proof.get('control') != str(control):
        raise ValueError('P4 proof must be refreshed into the current control')
    if (proof.get('source_root'), proof.get('destination_root'), proof.get('rel')) != expected:
        raise ValueError('hardlink proof root/relative path scope differs')
    for name in ('source', 'destination'):
        record = proof.get(name)
        if not isinstance(record, dict) or type(record.get('ino')) is not int:
            raise ValueError('hardlink proof lacks an inode identity')
    digest(proof.get('sha256'))


@contextlib.contextmanager
def regular_file(root, rel, optional=False):
    parent = fd = None
    try:
        try:
            parent, name = engine._open_parent(root, rel)
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=parent)
        except FileNotFoundError:
            if not optional:
                raise
            yield None
            return
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('hardlink endpoint is not a regular file')
        if engine._identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != engine._identity(before):
            raise RuntimeError('hardlink path changed before observation')
        yield {'root': root, 'rel': rel, 'parent': parent, 'name': name,
               'fd': fd, 'before': before}
    finally:
        for value in (fd, parent):
            if value is not None:
                os.close(value)


def stable(endpoint):
    before = endpoint['before']
    if (engine._identity(os.fstat(endpoint['fd'])) != engine._identity(before) or
            engine._identity(os.stat(endpoint['name'], dir_fd=endpoint['parent'],
                                    follow_symlinks=False)) != engine._identity(before)):
        raise RuntimeError('hardlink endpoint changed during checksum')
    # Reopen the original spelling without following links. An anchored FD
    # alone must not certify a parent which has since moved or been exchanged.
    parent, name = engine._open_parent(endpoint['root'], endpoint['rel'])
    try:
        first, current = os.fstat(endpoint['parent']), os.fstat(parent)
        if (first.st_dev, first.st_ino) != (current.st_dev, current.st_ino):
            raise RuntimeError('hardlink parent binding changed during checksum')
        if engine._identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != engine._identity(before):
            raise RuntimeError('hardlink path binding changed during checksum')
    finally:
        os.close(parent)


class HardlinkProofSession:
    def __init__(self, control, cfg, stop=None):
        self.control = Path(control)
        self.cfg = copy.deepcopy(cfg)
        self.guard = ScopeGuard(str(control), self.cfg)
        self.stop = stop or threading.Event()
        self.lock = None
        self.serial = threading.Lock()

    def is_set(self):
        return self.stop.is_set() or (self.control / 'STOP').exists()

    def __enter__(self):
        if self.lock is not None:
            raise RuntimeError('hardlink proof session is already open')
        if not _PROCESS_SESSION.acquire(blocking=False):
            raise BlockingIOError('another hardlink proof session owns this process budget')
        lock = None
        try:
            lock = open('/tmp/' + self.guard.run_id + '-p4-hardlinks.lock', 'a')
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            if lock is not None:
                lock.close()
            _PROCESS_SESSION.release()
            raise
        self.lock = lock
        self.saved_rate, self.saved_cancel = engine._io_limiter.rate, engine._cancel_event
        engine.configure_rate_limit(HASH_BUDGET)
        engine.configure_cancel_event(self)
        return self

    def __exit__(self, *args):
        try:
            engine.configure_cancel_event(self.saved_cancel)
            engine.configure_rate_limit(self.saved_rate)
        finally:
            self.lock.close()
            self.lock = None
            _PROCESS_SESSION.release()

    def verify(self, entry, canonical_rel, canonical_proof, alias_proof=None):
        if self.lock is None:
            raise RuntimeError('a locked dedicated proof session is required')
        with self.serial:
            return self._verify(entry, canonical_rel, canonical_proof, alias_proof)

    def verify_with_metadata(self, entry, canonical_rel, canonical_proof, alias_proof=None):
        """Observe content and POSIX metadata together; never authorize linking."""
        if self.lock is None:
            raise RuntimeError('a locked dedicated proof session is required')
        with self.serial:
            return self._verify(entry, canonical_rel, canonical_proof, alias_proof,
                                observe_metadata=True)

    def _verify(self, entry, canonical_rel, canonical_proof, alias_proof,
                observe_metadata=False):
        self.guard.assert_config(self.cfg)
        if entry.get('kind') != 'file':
            raise ValueError('hardlink alias must be a regular file')
        rel = self.guard.assert_path(entry.get('rel'))
        parts = components(canonical_rel)
        cross_eai = parts[0] == 'eai_pwm'
        if cross_eai:
            if len(parts) < 2:
                raise ValueError('canonical file cannot be the eai_pwm root')
        else:
            self.guard.assert_path(canonical_rel)
        if rel == canonical_rel or entry.get('hardlink_to', canonical_rel) != canonical_rel:
            raise ValueError('hardlink alias/canonical relationship differs')
        proof_scope(canonical_proof, self.control, self.cfg, canonical_rel, cross_eai)
        if alias_proof is not None:
            proof_scope(alias_proof, self.control, self.cfg, rel)
        if self.is_set():
            raise engine.CopyCancelled('STOP requested during hardlink verification')
        with contextlib.ExitStack() as stack:
            source = stack.enter_context(regular_file(self.cfg['source'], rel))
            canonical_source = stack.enter_context(regular_file(self.cfg['source'], canonical_rel))
            canonical_target = stack.enter_context(regular_file(self.cfg['destination'], canonical_rel))
            alias_target = stack.enter_context(regular_file(self.cfg['destination'], rel, optional=True))
            source_stat = source['before']
            engine._check_scanned(source_stat, entry)
            if engine._identity(source_stat) != engine._identity(canonical_source['before']):
                raise ValueError('current source files are not the same hardlink group')
            if source_stat.st_nlink < 2:
                raise ValueError('source hardlink group no longer has two names')
            # Device numbers in historical receipts can differ by client
            # mount. Ownership requires the actual inode AND a fresh SHA.
            if (canonical_source['before'].st_ino != canonical_proof['source']['ino'] or
                    canonical_target['before'].st_ino != canonical_proof['destination']['ino']):
                raise ValueError('canonical proof inode changed')
            candidate_owned = bool(alias_target is not None and alias_proof and
                                   alias_proof.get('owned') is True and
                                   alias_proof['destination']['ino'] == alias_target['before'].st_ino)
            metadata_before = {}
            if observe_metadata:
                import migration_priority4_file_metadata as metadata
                # Capture attributes before reading payloads as well as after.
                # An unowned separate alias is preserved without reading its
                # content or attributes. The source canonical has the same
                # current inode as source and is independently path-checked.
                observed = [('source', source), ('canonical_destination', canonical_target)]
                if candidate_owned:
                    observed.append(('candidate_alias', alias_target))
                for name, endpoint in observed:
                    metadata_before[name] = metadata.snapshot(endpoint['fd'], stop=self)
            source_hash = engine._hash_fd(source['fd'])
            canonical_hash = engine._hash_fd(canonical_target['fd'])
            if source_hash != canonical_hash or source_hash != digest(canonical_proof['sha256']):
                raise ValueError('current source/target SHA differs from canonical proof')
            alias_hash = None
            owned = False
            if alias_target is None:
                action = 'missing_alias'
            else:
                same_target = ((alias_target['before'].st_dev, alias_target['before'].st_ino) ==
                               (canonical_target['before'].st_dev, canonical_target['before'].st_ino))
                alias_hash = (canonical_hash if same_target else
                              engine._hash_fd(alias_target['fd']) if candidate_owned else None)
                owned = bool(candidate_owned and digest(alias_proof['sha256']) == alias_hash)
                action = 'already_linked' if same_target else 'owned_alias' if owned else 'preserve_unowned_alias'
            metadata_observation = None
            if observe_metadata:
                comparison = metadata.compare(source['fd'], canonical_target['fd'], stop=self)
                after = {'source': comparison['source'],
                         'canonical_destination': comparison['destination']}
                if candidate_owned:
                    after['candidate_alias'] = metadata.snapshot(alias_target['fd'], stop=self)
                for name, before in metadata_before.items():
                    if metadata._stable_observation(before) != metadata._stable_observation(after[name]):
                        raise metadata.MetadataChanged('hardlink metadata changed across checksum: ' + name)
                if not comparison['equivalent_observed_metadata']:
                    raise ValueError('canonical source/target metadata differs: ' +
                                     ', '.join(comparison['differences']))
                metadata_observation = {
                    'canonical': comparison,
                    # A previously owned alias can contain older data and
                    # attributes. Record its stable current state, without
                    # requiring it to equal the desired canonical metadata.
                    'candidate_alias': after.get('candidate_alias'),
                    'bound_to_current_checksum': True,
                    'remaining': ['other ACL models if exposed by the filesystem',
                                  'owned publication atime restoration and verification',
                                  'immediate rechecks and final alias inode/metadata snapshots'],
                }
            for endpoint in (source, canonical_source, canonical_target, alias_target):
                if endpoint is not None:
                    stable(endpoint)
            if alias_target is None:
                with regular_file(self.cfg['destination'], rel, optional=True) as appeared:
                    if appeared is not None:
                        raise RuntimeError('hardlink alias appeared during checksum')
            if self.is_set():
                raise engine.CopyCancelled('STOP requested after hardlink checksum')
            return {'rel': rel, 'canonical_rel': canonical_rel, 'cross_eai': cross_eai,
                    'sha256': source_hash, 'alias_sha256': alias_hash,
                    'source_identity': engine._record(source_stat),
                    'canonical_source_identity': engine._record(canonical_source['before']),
                    'canonical_destination_identity': engine._record(canonical_target['before']),
                    'alias_destination_identity': (engine._record(alias_target['before'])
                                                   if alias_target is not None else None),
                    'alias_owned': owned, 'alias_action': action,
                    'verification_only': True, 'publication_performed': False,
                    'publication_ready': False, 'metadata_validation_required': True,
                    'metadata_observation': metadata_observation,
                    'hash_budget_bytes_per_second': HASH_BUDGET}
