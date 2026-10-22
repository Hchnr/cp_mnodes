"""Local real-file hardlink proof fixtures; no live run or remote paths used."""
import builtins
import hashlib
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

import migration_priority4_hardlink_proof as proof
import migration_priority4_worker as scope
import migration_worker as engine
from test_migration_priority4_worker import config, CONTROL, PREDECESSOR


class Priority4HardlinkProofTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='p4-hardlink-proof-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.source, self.destination = self.base / 'source', self.base / 'destination'
        self.source.mkdir(); self.destination.mkdir()
        self.cfg = config()
        real_open = builtins.open
        lock_name = '/tmp/' + self.cfg['run_id'] + '-p4-hardlinks.lock'
        def fixture_open(file, *args, **kwargs):
            if isinstance(file, (str, os.PathLike)) and os.fspath(file) == lock_name:
                file = self.base / 'proof-session.lock'
            return real_open(file, *args, **kwargs)
        # Keep the real flock implementation, but place its dedicated file
        # inside this fixture's temporary tree so cleanup includes the lock.
        patcher = mock.patch.object(builtins, 'open', side_effect=fixture_open)
        patcher.start(); self.addCleanup(patcher.stop)
        self.original_open_root = engine._open_root
        def mapped(root, create=False):
            actual = {scope.SOURCE: self.source, scope.DESTINATION: self.destination}.get(str(root))
            if actual is None:
                raise AssertionError('unexpected root')
            return self.original_open_root(str(actual), create=create)
        patcher = mock.patch.object(engine, '_open_root', mapped)
        patcher.start(); self.addCleanup(patcher.stop)
        self.stop = threading.Event()
        # STOP is exercised through the same cancellation event in these local
        # fixtures; production also directly checks the exact control/STOP.
        stopped = mock.patch.object(Path, 'exists', autospec=True,
                                    side_effect=lambda p: False if str(p) == CONTROL + '/STOP'
                                    else os.path.exists(p))
        stopped.start(); self.addCleanup(stopped.stop)

    def group(self, canonical='eai_pwm/data/canonical', alias='other/data/alias', data=b'fixture-data'):
        a, c = self.source / alias, self.source / canonical
        a.parent.mkdir(parents=True, exist_ok=True); c.parent.mkdir(parents=True, exist_ok=True)
        c.write_bytes(data); os.link(c, a)
        target = self.destination / canonical
        target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(data)
        record = {'kind': 'file', 'control': PREDECESSOR if canonical.startswith('eai_pwm/') else CONTROL,
                  'source_root': scope.SOURCE + ('/eai_pwm' if canonical.startswith('eai_pwm/') else ''),
                  'destination_root': scope.DESTINATION + ('/eai_pwm' if canonical.startswith('eai_pwm/') else ''),
                  'rel': canonical[len('eai_pwm/'):] if canonical.startswith('eai_pwm/') else canonical,
                  'source': engine._record(c.stat()), 'destination': engine._record(target.stat()),
                  'sha256': hashlib.sha256(data).hexdigest()}
        entry = dict(engine._record(a.stat()), kind='file', rel=alias, hardlink_to=canonical)
        return entry, record

    def verify(self, entry, canonical, **kwargs):
        with proof.HardlinkProofSession(CONTROL, self.cfg, self.stop) as session:
            return session.verify(entry, entry['hardlink_to'], canonical, **kwargs)

    def alias(self, entry, body=b'fixture-data'):
        path = self.destination / entry['rel']
        path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
        return {'kind': 'file', 'control': CONTROL, 'source_root': scope.SOURCE,
                'destination_root': scope.DESTINATION, 'rel': entry['rel'], 'owned': True,
                'source': {k: v for k, v in entry.items() if k not in ('rel', 'kind', 'hardlink_to')},
                'destination': engine._record(path.stat()), 'sha256': hashlib.sha256(body).hexdigest()}

    def test_cross_eai_fresh_sha_never_creates_missing_alias_or_parents(self):
        entry, canonical = self.group()
        with mock.patch.object(engine, '_hash_fd', wraps=engine._hash_fd) as reader:
            value = self.verify(entry, canonical)
        self.assertEqual(reader.call_count, 2)
        self.assertEqual(value['alias_action'], 'missing_alias')
        self.assertTrue(value['cross_eai'])
        self.assertFalse(value['publication_ready'])
        self.assertFalse((self.destination / 'other').exists())
        self.assertEqual((self.destination / entry['hardlink_to']).stat().st_nlink, 1)

    def test_same_scope_hidden_and_nested_eai_groups_are_valid(self):
        entry, canonical = self.group(canonical='.hidden/eai_pwm/canonical')
        value = self.verify(entry, canonical)
        self.assertFalse(value['cross_eai'])
        self.assertEqual(value['sha256'], canonical['sha256'])

    def test_existing_owned_alias_requires_inode_and_current_sha_together(self):
        entry, canonical = self.group(); owned = self.alias(entry, b'older-owned-copy')
        value = self.verify(entry, canonical, alias_proof=owned)
        self.assertEqual(value['alias_action'], 'owned_alias')
        self.assertTrue(value['alias_owned'])
        self.assertEqual((self.destination / entry['rel']).read_bytes(), b'older-owned-copy')
        self.assertNotEqual(value['alias_destination_identity']['ino'], value['canonical_destination_identity']['ino'])
        for changed in [dict(owned, destination=dict(owned['destination'], ino=-1)),
                        dict(owned, sha256='0' * 64), dict(owned, owned=False)]:
            self.assertEqual(self.verify(entry, canonical, alias_proof=changed)['alias_action'],
                             'preserve_unowned_alias')

    def test_identical_existing_unowned_file_is_not_adopted_or_replaced(self):
        entry, canonical = self.group(); self.alias(entry)
        path = self.destination / entry['rel']; before = path.stat().st_ino
        with mock.patch.object(engine, '_hash_fd', wraps=engine._hash_fd) as reader:
            value = self.verify(entry, canonical)
        self.assertEqual(reader.call_count, 2)  # No payload read of an unowned alias.
        self.assertEqual(value['alias_action'], 'preserve_unowned_alias')
        self.assertFalse(value['alias_owned']); self.assertEqual(path.stat().st_ino, before)

    def test_existing_same_inode_alias_is_observed_without_claiming_ownership(self):
        entry, canonical = self.group()
        path = self.destination / entry['rel']; path.parent.mkdir(parents=True)
        os.link(self.destination / entry['hardlink_to'], path)
        value = self.verify(entry, canonical)
        self.assertEqual(value['alias_action'], 'already_linked')
        self.assertFalse(value['alias_owned'])

    def test_stale_sha_or_changed_canonical_inode_cannot_certify_content(self):
        entry, canonical = self.group()
        for bad in [dict(canonical, sha256='0' * 64),
                    dict(canonical, destination=dict(canonical['destination'], ino=-1))]:
            with self.assertRaises(ValueError): self.verify(entry, bad)
        (self.destination / entry['hardlink_to']).write_bytes(b'new-contents')
        with self.assertRaises(ValueError): self.verify(entry, canonical)

    def test_actual_source_hardlink_membership_is_required(self):
        entry, canonical = self.group(); path = self.source / entry['rel']
        path.unlink(); path.write_bytes(b'fixture-data')
        current = dict(engine._record(path.stat()), kind='file', rel=entry['rel'],
                       hardlink_to=entry['hardlink_to'])
        with self.assertRaises(ValueError): self.verify(current, canonical)

    def test_covered_root_pair_and_relative_prefix_cannot_be_confused(self):
        entry, canonical = self.group()
        for bad in [dict(canonical, control='/unrelated'), dict(canonical, source_root=scope.SOURCE),
                    dict(canonical, rel='eai_pwm/' + canonical['rel']), dict(canonical, sha256=None)]:
            with mock.patch.object(engine, '_open_parent') as opened:
                with self.assertRaises(ValueError): self.verify(entry, bad)
                opened.assert_not_called()

    def test_reserved_alias_canonical_and_foreign_alias_proof_are_rejected_before_io(self):
        entry, canonical = self.group()
        for rel in ['eai_pwm/alias', '.hcr-copy-monitor/alias', '../escape']:
            with mock.patch.object(engine, '_open_parent') as opened:
                with self.assertRaises(ValueError): self.verify(dict(entry, rel=rel), canonical)
                opened.assert_not_called()
        with proof.HardlinkProofSession(CONTROL, self.cfg, self.stop) as session:
            with self.assertRaises(ValueError):
                session.verify(entry, '.hcr-copy-monitor/file', canonical)
        owned = self.alias(entry)
        with self.assertRaises(ValueError):
            self.verify(entry, canonical, alias_proof=dict(owned, control=PREDECESSOR))

    def test_symlink_parent_and_leaf_are_never_followed(self):
        entry, canonical = self.group()
        target = self.destination / entry['hardlink_to']; target.unlink()
        target.symlink_to(self.source / entry['hardlink_to'])
        with self.assertRaises(OSError): self.verify(entry, canonical)
        target.unlink(); target.parent.rmdir()
        target.parent.symlink_to(self.source / 'eai_pwm/data', target_is_directory=True)
        with self.assertRaises(OSError): self.verify(entry, canonical)

    def test_change_after_a_checksum_invalidates_the_observation(self):
        entry, canonical = self.group(); original = engine._hash_fd; calls = []
        def mutate(fd):
            value = original(fd); calls.append(fd)
            if len(calls) == 2:
                # A same-size rewrite can retain the observed timestamps on
                # a coarse-resolution fixture filesystem. Change size too so
                # this tests the documented inode/size/time identity guard.
                (self.destination / entry['hardlink_to']).write_bytes(b'new-contents-after-checksum')
            return value
        with mock.patch.object(engine, '_hash_fd', side_effect=mutate):
            with self.assertRaisesRegex(RuntimeError, 'changed during checksum'):
                self.verify(entry, canonical)

    def test_alias_appearing_during_hashing_is_not_reported_missing(self):
        entry, canonical = self.group(); original = engine._hash_fd; calls = []
        def appear(fd):
            value = original(fd); calls.append(fd)
            if len(calls) == 2: self.alias(entry, b'new-unowned-data')
            return value
        with mock.patch.object(engine, '_hash_fd', side_effect=appear):
            with self.assertRaisesRegex(RuntimeError, 'appeared during checksum'):
                self.verify(entry, canonical)
        self.assertEqual((self.destination / entry['rel']).read_bytes(), b'new-unowned-data')

    def test_parent_exchange_after_hashing_is_rejected(self):
        entry, canonical = self.group(); original = engine._hash_fd; calls = []
        def move(fd):
            value = original(fd); calls.append(fd)
            if len(calls) == 2:
                parent = (self.destination / entry['hardlink_to']).parent
                parent.rename(parent.with_name('moved')); parent.mkdir()
                (parent / 'canonical').write_bytes(b'fixture-data')
            return value
        with mock.patch.object(engine, '_hash_fd', side_effect=move):
            with self.assertRaisesRegex(RuntimeError, 'parent binding changed'):
                self.verify(entry, canonical)

    def test_mount_device_number_from_another_client_is_not_an_inode_change(self):
        entry, canonical = self.group()
        entry['dev'] = -100
        canonical['source']['dev'] = -200
        canonical['destination']['dev'] = -300
        self.assertEqual(self.verify(entry, canonical)['alias_action'], 'missing_alias')

    def test_stop_during_hashing_cancels_and_restores_the_process_budget(self):
        entry, canonical = self.group(); original = engine._hash_fd; saved = engine._io_limiter.rate
        def stop(fd):
            value = original(fd); self.stop.set(); return value
        with mock.patch.object(engine, '_hash_fd', side_effect=stop):
            with self.assertRaises(engine.CopyCancelled): self.verify(entry, canonical)
        self.assertEqual(engine._io_limiter.rate, saved)

    def test_stop_lock_budget_and_closed_session_contract(self):
        entry, canonical = self.group(); self.stop.set()
        with self.assertRaises(engine.CopyCancelled): self.verify(entry, canonical)
        self.stop.clear(); saved = engine._io_limiter.rate
        with proof.HardlinkProofSession(CONTROL, self.cfg, self.stop) as session:
            self.assertEqual(engine._io_limiter.rate, 32 * 1024 * 1024)
            with self.assertRaises(BlockingIOError):
                with proof.HardlinkProofSession(CONTROL, self.cfg): pass
            value = session.verify(entry, entry['hardlink_to'], canonical)
            self.assertFalse(value['publication_performed'])
        self.assertEqual(engine._io_limiter.rate, saved)
        with self.assertRaises(RuntimeError): session.verify(entry, entry['hardlink_to'], canonical)


if __name__ == '__main__':
    unittest.main()
