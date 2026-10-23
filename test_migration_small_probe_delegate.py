import concurrent.futures
import hashlib
import os
import stat
import threading
import unittest
from unittest import mock

import migration_small_probe_delegate as delegation
import migration_target_probe as probe
import migration_worker as worker
from migration_distributed_target_filter import DistributedTargetFilterResume
from migration_scan import _stat_fields
import test_migration_target_probe as fixtures


POLICY = {'scope': delegation.SCOPE, 'max_file_bytes': 65536,
          'min_missing': 16, 'recheck_every': 64, 'max_directories': 4096}


class DelegationContractTests(fixtures.TargetProbeTests):
    def setUp(self):
        super().setUp()
        for owner, name in ((probe.ProbeService, 'probe'),
                            (probe.ProbeService, 'health')):
            patch = mock.patch.object(owner, name, getattr(owner, name))
            patch.start()
            self.addCleanup(patch.stop)
        delegation.install(POLICY)

    def entry(self, rel='data/file', body=b'fresh source bytes'):
        source = self.file(self.source, rel, body)
        return {'rel': rel, 'kind': 'file', **_stat_fields(source.stat())}

    def warm(self, entry=None, count=16):
        if entry is None:
            entry = self.entry()
        for _ in range(count):
            result = self.service.probe(entry)
            self.assertTrue(result['missing'])
            self.assertNotIn('precheck_delegated', result)
        return entry

    def policy_health(self):
        return self.service.health()['small_probe_delegate']

    def test_threshold_sampling_and_accounting_are_actual_not_candidates(self):
        entry = self.warm(count=15)
        self.assertNotIn('precheck_delegated', self.service.probe(entry))
        entry['_refresh_source'] = False
        for _ in range(63):
            result = self.service.probe(entry)
            self.assertTrue(result['precheck_delegated'])
            self.assertNotIn('_refresh_source', result['entry'])
        self.assertNotIn('precheck_delegated', self.service.probe(entry))
        health = self.service.health()
        self.assertEqual(health['counts']['missing'], 17)
        self.assertEqual(health['counts']['delegated_small_files'], 63)
        self.assertEqual(health['counts']['requests'], 80)
        self.assertEqual(health['small_probe_delegate']['actual_missing_samples'], 17)
        self.assertFalse(health['small_probe_delegate']['delegated_is_missing_evidence'])
        self.assertFalse((self.destination / 'data').exists())

    def test_existing_sample_permanently_disables_parent(self):
        entry = self.warm()
        self.file(self.destination, 'data/file', b'already present')
        for _ in range(63):
            self.assertTrue(self.service.probe(entry)['precheck_delegated'])
        result = self.service.probe(entry)
        self.assertFalse(result['missing'])
        self.assertEqual(self.policy_health()['disabled_directories'], 1)
        (self.destination / 'data/file').unlink()
        for _ in range(100):
            self.assertNotIn('precheck_delegated', self.service.probe(entry))

    def test_existing_ineligible_entry_also_disables_parent(self):
        entry = self.entry()
        self.file(self.destination, 'data/old', b'old target')
        self.assertFalse(self.service.probe({'rel': 'data/old'})['missing'])
        for _ in range(80):
            self.assertNotIn('precheck_delegated', self.service.probe(entry))
        self.assertEqual(self.policy_health()['disabled_directories'], 1)

    def test_ineligible_metadata_is_never_delegated(self):
        entry = self.warm()
        cases = [dict(entry, kind='symlink'), dict(entry, size=65537),
                 dict(entry, size=-1), dict(entry, mode=stat.S_IFDIR | 0o755),
                 dict(entry, _refresh_source=True),
                 dict(entry, known={'sha256': 'f' * 64}),
                 dict(entry, known=[])]
        for field in delegation._STAT_INTEGERS:
            absent = dict(entry)
            del absent[field]
            cases += [absent, dict(entry, **{field: True}),
                      dict(entry, **{field: '123'})]
        for case in cases:
            with self.subTest(case=case):
                self.assertNotIn('precheck_delegated', self.service.probe(case))
        self.assertEqual(self.service.health()['counts'].get('delegated_small_files', 0), 0)

    def test_ineligible_missing_does_not_warm_a_parent(self):
        entry = self.entry()
        for _ in range(20):
            self.service.probe({'rel': 'data/file'})
        self.assertNotIn('precheck_delegated', self.service.probe(entry))
        self.assertEqual(self.policy_health()['actual_missing_samples'], 21)

    def test_failed_real_probe_never_counts_as_missing_or_warmth(self):
        entry = self.warm(count=15)
        with mock.patch.object(probe, '_open_parent', side_effect=PermissionError('denied')):
            with self.assertRaises(probe.ProbeError):
                self.service.probe(entry)
        self.assertEqual(self.policy_health()['actual_missing_samples'], 15)
        self.assertEqual(self.policy_health()['failed_samples'], 1)
        self.assertNotIn('precheck_delegated', self.service.probe(entry))
        self.assertTrue(self.service.probe(entry)['precheck_delegated'])

    def test_delegated_candidate_still_hashes_existing_target_and_skips(self):
        body = b'already copied identical payload'
        entry = self.warm(self.entry(body=body))
        target = self.file(self.destination, 'data/file', body)
        inode = target.stat().st_ino
        candidate = self.service.probe(entry)
        self.assertTrue(candidate['precheck_delegated'])
        with mock.patch.object(worker, '_hash_fd', wraps=worker._hash_fd) as hashes:
            receipt = worker.copy_entry(self.source, self.destination,
                                        candidate['entry'], 'delegation-fixture')
        self.assertEqual(receipt['status'], 'skipped')
        self.assertEqual(receipt['sha256'], hashlib.sha256(body).hexdigest())
        self.assertEqual(receipt['bytes'], 0)
        self.assertEqual(hashes.call_count, 2)
        self.assertEqual(target.stat().st_ino, inode)
        self.assertEqual(target.read_bytes(), body)
        self.assertIn('source_identity', receipt)
        self.assertIn('dst_stat', receipt)

    def test_existing_producer_protocol_accepts_candidate_without_metadata_change(self):
        entry = self.warm()
        entry['_refresh_source'] = False
        entry['known'] = {}
        candidate = self.service.probe(entry)
        checked = DistributedTargetFilterResume._checked_entry(candidate, entry)
        self.assertEqual(checked, {key: value for key, value in entry.items()
                                   if key != '_refresh_source'})
        self.assertIsNot(checked, candidate['entry'])

    def test_delegated_conflicting_target_is_not_overwritten(self):
        entry = self.warm(self.entry(body=b'AAAA'))
        target = self.file(self.destination, 'data/file', b'BBBB')
        inode = target.stat().st_ino
        candidate = self.service.probe(entry)
        with mock.patch.object(worker, '_hash_fd', wraps=worker._hash_fd) as hashes:
            receipt = worker.copy_entry(self.source, self.destination,
                                        candidate['entry'], 'delegation-fixture')
        self.assertEqual(receipt['status'], 'conflict')
        self.assertEqual(receipt['bytes'], 0)
        self.assertEqual(hashes.call_count, 2)
        self.assertEqual(target.stat().st_ino, inode)
        self.assertEqual(target.read_bytes(), b'BBBB')

    def test_delegated_stale_source_is_still_changed_not_copied(self):
        entry = self.warm()
        (self.source / 'data/file').write_bytes(b'changed since scan; longer data')
        candidate = self.service.probe(entry)
        self.assertTrue(candidate['precheck_delegated'])
        receipt = worker.copy_entry(self.source, self.destination,
                                    candidate['entry'], 'delegation-fixture')
        self.assertEqual(receipt['status'], 'changed')
        self.assertEqual(receipt['bytes'], 0)
        self.assertFalse((self.destination / 'data/file').exists())

    def test_delegation_obeys_busy_limit_and_stop_before_after(self):
        entry = self.warm()
        self.assertTrue(self.service.fs_slots.acquire(blocking=False))
        self.assertTrue(self.service.fs_slots.acquire(blocking=False))
        try:
            with self.assertRaises(probe.ProbeError) as caught:
                self.service.probe(entry)
            self.assertEqual(caught.exception.status, 503)
            self.assertEqual(self.service.health()['counts']['busy'], 1)
            self.assertEqual(self.service.health()['counts']['requests'], 16)
        finally:
            self.service.fs_slots.release()
            self.service.fs_slots.release()
        for checks in ([True], [False, True]):
            with mock.patch.object(self.service, 'stopped', side_effect=checks), \
                    mock.patch.object(probe, '_open_parent') as opened:
                with self.assertRaises(probe.ProbeError) as caught:
                    self.service.probe(entry)
                self.assertEqual(caught.exception.status, 503)
                opened.assert_not_called()
        health = self.service.health()
        self.assertEqual(health['inflight'], 0)
        self.assertEqual(health['counts'].get('delegated_small_files', 0), 0)
        self.assertEqual(health['counts']['errors'], 2)

    def test_unauthorized_delegatable_request_never_reaches_probe(self):
        entry = self.warm()
        server = self.server()
        status, _ = self.request(server, {'entry': entry}, auth=False)
        self.assertEqual(status, 401)
        self.assertEqual(self.service.health()['counts']['requests'], 16)

    def test_concurrent_delegation_sampling_and_slot_cleanup(self):
        entry = self.warm()
        self.service.fs_slots = threading.BoundedSemaphore(64)
        self.service.slots = 64
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            results = list(pool.map(lambda _: self.service.probe(entry), range(256)))
        self.assertEqual(sum(bool(r.get('precheck_delegated')) for r in results), 252)
        health = self.service.health()
        self.assertEqual(health['counts']['delegated_small_files'], 252)
        self.assertEqual(health['counts']['missing'], 20)
        self.assertEqual(health['inflight'], 0)
        self.assertLessEqual(health['peak_inflight'], 64)

    def test_hot_threshold_update_keeps_warmth_existing_disables_and_service(self):
        entry = self.warm()
        self.file(self.destination, 'old/file', b'already there')
        self.service.probe({'rel': 'old/file'})
        controller = delegation.installed_controller()
        state = controller.state_for(self.service)
        identity = self.service.identity
        large = dict(entry, size=128 * 1024)
        self.assertNotIn('precheck_delegated', self.service.probe(large))
        controller.update(dict(POLICY, max_file_bytes=1024 * 1024))
        self.assertTrue(self.service.probe(large)['precheck_delegated'])
        self.assertIs(controller.state_for(self.service), state)
        self.assertTrue(state.directories['old']['disabled'])
        self.assertIs(self.service.identity, identity)
        self.assertNotIn('precheck_delegated',
                         self.service.probe(dict(large, rel='old/new')))
        self.assertEqual(self.policy_health()['runtime']['revision'], 1)
        controller.update(POLICY)
        self.assertNotIn('precheck_delegated', self.service.probe(large))
        self.assertTrue(state.directories['old']['disabled'])

    def test_policy_reloader_rejects_bad_updates_without_changing_valid_policy(self):
        self.warm()
        controller = delegation.installed_controller()
        reader = mock.Mock(return_value=dict(POLICY, max_file_bytes=1024 * 1024))
        reloader = delegation.PolicyReloader(controller, reader)
        reloader.reload_once()
        self.assertEqual(self.policy_health()['policy']['max_file_bytes'], 1024 * 1024)
        for invalid in (dict(POLICY, max_file_bytes=1024 * 1024 + 1),
                        dict(POLICY, min_missing=32),
                        dict(POLICY, max_directories=1),
                        dict(POLICY, recheck_every=1), None):
            reader.return_value = invalid
            reloader.reload_once()
            health = self.policy_health()
            self.assertIsNotNone(health['runtime']['reload_error'])
            self.assertEqual(health['policy']['max_file_bytes'], 1024 * 1024)
            self.assertEqual(health['runtime']['revision'], 1)
        reader.side_effect = OSError('temporary unavailable policy')
        reloader.reload_once()
        self.assertIn('OSError', self.policy_health()['runtime']['reload_error'])
        reader.side_effect = None
        reader.return_value = dict(POLICY, max_file_bytes=1024 * 1024)
        reloader.reload_once()
        self.assertIsNone(self.policy_health()['runtime']['reload_error'])
        reader.assert_called()

    def test_large_delegated_candidate_still_skips_and_preserves_conflicts(self):
        delegation.installed_controller().update(dict(POLICY, max_file_bytes=1024 * 1024))
        body = b'a' * (128 * 1024)
        entry = self.warm(self.entry(body=body))
        target = self.file(self.destination, 'data/file', body)
        inode = target.stat().st_ino
        candidate = self.service.probe(entry)
        self.assertTrue(candidate['precheck_delegated'])
        receipt = worker.copy_entry(self.source, self.destination,
                                    candidate['entry'], 'large-delegate-fixture')
        self.assertEqual(receipt['status'], 'skipped')
        self.assertEqual(receipt['bytes'], 0)
        self.assertEqual(target.stat().st_ino, inode)
        target.write_bytes(b'b' * len(body))
        candidate = self.service.probe(entry)
        receipt = worker.copy_entry(self.source, self.destination,
                                    candidate['entry'], 'large-delegate-fixture')
        self.assertEqual(receipt['status'], 'conflict')
        self.assertEqual(target.read_bytes(), b'b' * len(body))

    def test_policy_reload_and_probes_can_run_concurrently(self):
        entry = self.warm()
        controller = delegation.installed_controller()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.service.probe, entry) for _ in range(64)]
            for i in range(30):
                controller.update(dict(POLICY, max_file_bytes=(1024 * 1024 if i % 2 else 65536)))
            for future in futures:
                self.assertTrue(future.result(timeout=5)['missing'])
        self.assertEqual(self.service.health()['inflight'], 0)
        self.assertEqual(self.policy_health()['tracked_directories'], 1)

    def test_delayed_policy_reader_does_not_block_copy_candidate_requests(self):
        entry = self.warm()
        entered, release = threading.Event(), threading.Event()
        def delayed_reader():
            entered.set()
            release.wait(5)
            return dict(POLICY, max_file_bytes=1024 * 1024)
        reloader = delegation.PolicyReloader(delegation.installed_controller(), delayed_reader)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            reading = pool.submit(reloader.reload_once)
            self.assertTrue(entered.wait(2))
            try:
                candidate = pool.submit(self.service.probe, entry).result(timeout=2)
                self.assertTrue(candidate['precheck_delegated'])
            finally:
                release.set()
            reading.result(timeout=2)

    def test_reloader_background_start_and_close(self):
        reader = mock.Mock(return_value=dict(POLICY, max_file_bytes=1024 * 1024))
        reloader = delegation.PolicyReloader(delegation.installed_controller(), reader, interval=1)
        reloader.start()
        try:
            self.assertTrue(reloader.thread.is_alive())
        finally:
            reloader.close()
        self.assertFalse(reloader.thread.is_alive())
        with self.assertRaises(ValueError):
            delegation.PolicyReloader(delegation.installed_controller(), reader, interval=0)


class PolicyAndBoundTests(unittest.TestCase):
    def test_policy_rejects_unknown_scope_fields_types_and_unsafe_bounds(self):
        invalid = [None, {}, dict(POLICY, scope='all-files'),
                   dict(POLICY, extra=True), dict(POLICY, max_file_bytes=1024 * 1024 + 1),
                   dict(POLICY, min_missing=15), dict(POLICY, recheck_every=65),
                   dict(POLICY, max_directories=4097)]
        for key in delegation._INTEGER_LIMITS:
            invalid += [dict(POLICY, **{key: True}), dict(POLICY, **{key: -1})]
        for policy in invalid:
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                delegation.validate_policy(policy)
        self.assertEqual(delegation.validate_policy(POLICY), POLICY)

    def test_lru_bound_and_permanent_existing_disable_survive_eviction_pressure(self):
        state = delegation._ServiceState(dict(POLICY, max_directories=2))
        missing = {'ok': True, 'missing': True}
        existing = {'ok': True, 'missing': False}
        state.observed('a', True, missing)
        state.observed('b', True, missing)
        state.directory('a')
        state.observed('c', True, missing)
        self.assertEqual(set(state.directories), {'a', 'c'})
        state.observed('a', True, existing)
        for i in range(20):
            state.observed('fresh-' + str(i), True, missing)
            self.assertTrue(state.directories['a']['disabled'])
            self.assertEqual(len(state.directories), 2)
        state.observed('second-existing', True, existing)
        self.assertEqual(state.disabled_directories, 2)
        state.observed('new-parent', True, missing)
        self.assertTrue(state.saturated_disabled)
        self.assertEqual(len(state.directories), 2)
        self.assertTrue(state.directories['a']['disabled'])

    def test_malformed_or_delegated_results_never_become_real_missing(self):
        state = delegation._ServiceState(POLICY)
        for result in (None, {}, {'ok': False, 'missing': True},
                       {'ok': True, 'missing': 1},
                       {'ok': True, 'missing': True, 'precheck_delegated': True}):
            state.observed('p', True, result)
        self.assertEqual(state.actual_missing_samples, 0)
        self.assertEqual(state.actual_existing_samples, 0)
        self.assertEqual(state.failed_samples, 5)
        self.assertEqual(len(state.directories), 0)


if __name__ == '__main__':
    unittest.main()
