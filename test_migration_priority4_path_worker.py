import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_io
import migration_priority4_path_worker as canary
import migration_worker
import migration_worker128_run as auxiliary
from migration_state_cache import StateCache
from test_migration_priority4_worker import config
from test_migration_priority4_worker import CONTROL as FIXTURE_CONTROL


HOST = 'job-fixture-worker-10'


class Priority4PathBundleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='p4-path-bundle-fixture-')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.bundle = self.control / 'p4-copy-path-fixture'
        self.bundle.mkdir()
        names = {'migration_priority4_worker.py', 'migration_run.py', 'migration_worker.py',
                 'migration_worker128_run.py', 'migration_io.py', 'migration_state_cache.py'}
        self.base = {}
        for name in names:
            data = ('# unchanged ' + name).encode()
            (self.control / name).write_bytes(data)
            (self.bundle / name).write_bytes(data)
            self.base[name] = hashlib.sha256(data).hexdigest()
        (self.control / 'source-sha256.json').write_text(json.dumps(self.base))
        for name in canary.ADDITIONS - {'path-open-policy.json'}:
            (self.bundle / name).write_text('# fixture new launcher component')
        self.cfg = dict(config(), job='fixture')
        self.policy = dict(scope=canary.SCOPE, control=str(self.control),
            run_id=self.cfg['run_id'], job='fixture', node='worker-10', lane='supplement',
            directory_strategy='native_readonly',
            predecessor_manifest_sha256=canary.digest_file(self.control / 'source-sha256.json'))
        self.freeze()
        patches = (mock.patch.object(canary.io, 'read_json', return_value=self.cfg),
                   mock.patch.object(canary.original, 'ScopeGuard'),
                   mock.patch.object(canary.socket, 'gethostname', return_value=HOST))
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def freeze(self):
        (self.bundle / 'path-open-policy.json').write_text(json.dumps(self.policy))
        names = set(self.base) | canary.ADDITIONS
        values = {name: canary.digest_file(self.bundle / name) for name in names}
        (self.bundle / 'source-sha256.json').write_text(json.dumps(values))

    def validate(self, lane='supplement'):
        return canary.validate_bundle(self.control, self.bundle, lane)

    def test_only_exact_p4_canary_node_and_lane_are_accepted(self):
        self.assertEqual(self.validate(), (self.cfg, self.policy))
        with self.assertRaises(canary.StartupProgressError):
            self.validate('primary')
        with mock.patch.object(canary.socket, 'gethostname', return_value='job-fixture-worker-9'):
            with self.assertRaises(canary.StartupProgressError):
                self.validate()

    def test_unrelated_engine_change_is_forbidden_even_with_new_candidate_hash(self):
        path = self.bundle / 'migration_worker.py'
        path.write_text('# changed data engine')
        self.freeze()
        with self.assertRaises(canary.StartupProgressError):
            self.validate()

    def test_original_hash_rewrite_cannot_change_the_declared_predecessor(self):
        path = self.control / 'migration_worker.py'
        path.write_text('# changed original')
        self.base[path.name] = canary.digest_file(path)
        (self.control / 'source-sha256.json').write_text(json.dumps(self.base))
        (self.bundle / path.name).write_bytes(path.read_bytes())
        self.freeze()
        with self.assertRaises(canary.StartupProgressError):
            self.validate()

    def test_unknown_strategy_and_versioned_policy_are_rejected(self):
        self.policy['directory_strategy'] = 'destination_opath'
        self.freeze()
        with self.assertRaises(canary.StartupProgressError):
            self.validate()
        self.policy['directory_strategy'] = 'native_readonly'
        self.freeze()
        (self.bundle / '.snapshots/path-open-policy.json').mkdir(parents=True)
        with self.assertRaises(canary.StartupProgressError):
            self.validate()


class Priority4PathStartupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='p4-path-startup-fixture-')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.bundle = self.control / 'p4-copy-path-fixture'
        self.bundle.mkdir()
        self.checkpoint = self.bundle / 'cache-age-rollout' / (HOST + '-supplement.json')
        self.node = self.control / 'nodes-extra' / (HOST + '.json')
        self.operation = 'fixture-operation'
        self.progress = dict(host=HOST, pid=12345, active={}, run_started='original-run',
            bytes=111, streamed_bytes=112, verified_bytes=111, verified_files=7,
            completed_batches=4, broker_retries=2, counts={'copied': 7},
            stages={'p4-r1': dict(copied_bytes=111, verified_files=7, counts={'copied': 7})})
        self.expected = dict(pid=12345, starttime_ticks='777',
            argv=['python3', str(self.control / 'migration_priority4_worker.py'),
                  'supplement', '--control', str(self.control)])
        self.record = dict(control=str(self.control), migration_scope=canary.SCOPE,
            directory_strategy='native_readonly', user_authorization=canary.AUTHORIZATION,
            operation=self.operation, host=HOST, lane='supplement', status='replacing',
            lane_leases={}, expected=self.expected, idle=self.progress)
        patch = mock.patch.object(canary.socket, 'gethostname', return_value=HOST)
        patch.start()
        self.addCleanup(patch.stop)

    def read(self, path):
        if Path(path) == self.checkpoint:
            return copy.deepcopy(self.record)
        if Path(path) == self.node:
            return copy.deepcopy(self.progress)
        if Path(path) == self.control / 'config.json':
            return config()
        if Path(path) == self.control / 'state.json':
            return dict(stage='p4-r1', phase=4, round=1, status='scanning')
        raise FileNotFoundError(str(path))

    def guard(self):
        return canary.checkpoint_identity(self.control, self.bundle, self.checkpoint,
                                          self.operation, 'supplement', self.read)

    def test_actual_first_node_read_preserves_all_history_and_refuses_regression(self):
        guard, expected = self.guard()
        self.assertEqual(expected, self.expected)
        self.progress['bytes'] = 0
        with self.assertRaises(canary.StartupProgressError):
            guard.read_json(self.node)
        self.assertFalse(guard.consumed)
        self.progress['bytes'] = 111
        self.assertEqual(guard.read_json(self.node), self.progress)
        self.assertTrue(guard.consumed)

    def test_authorization_lease_and_scope_failures_never_start_worker(self):
        for change in (dict(user_authorization='producer-only permission'),
                       dict(migration_scope='wrong'), dict(lane_leases={'p4-r1': 1}),
                       dict(operation='other-operation'), dict(directory_strategy='destination_opath')):
            previous = self.record
            self.record = dict(previous, **change)
            with self.subTest(change=change), self.assertRaises(canary.StartupProgressError):
                self.guard()
            self.record = previous

    def test_missing_checkpoint_and_ambiguous_exit_fail_closed(self):
        with self.assertRaises(FileNotFoundError):
            canary.checkpoint_identity(self.control, self.bundle, self.checkpoint,
                self.operation, 'supplement', lambda _: (_ for _ in ()).throw(FileNotFoundError()))
        for current in (dict(start_ticks='777', state='R', argv=['']),
                        dict(start_ticks='778', state='Z', argv=[''])):
            with mock.patch.object(canary, 'identity', return_value=current):
                with self.assertRaises(canary.StartupProgressError):
                    canary.require_predecessor_exited(self.expected)
        with mock.patch.object(canary, 'identity', return_value=dict(start_ticks='777', state='Z', argv=[''])):
            canary.require_predecessor_exited(self.expected)

    def test_launcher_uses_original_p4_scope_wrapper_and_restores_aliases(self):
        saved_reader, saved_opener = migration_io.read_json, migration_worker._directory_opener
        def delegate(argv):
            self.assertEqual(argv[0], 'supplement')
            self.assertIs(migration_worker._directory_opener, canary.native.open_parent_readonly)
            self.assertEqual(migration_io.read_json(self.node), self.progress)
            raise RuntimeError('fixture worker exit')
        with mock.patch.object(canary, '__file__', str(self.bundle / 'migration_priority4_path_worker.py')), \
                mock.patch.object(canary, 'validate_bundle'), \
                mock.patch.object(canary, 'validate_imports'), \
                mock.patch.object(canary, 'require_predecessor_exited'), \
                mock.patch.object(migration_io, 'read_json', self.read), \
                mock.patch.object(canary.original, 'main', side_effect=delegate):
            with self.assertRaisesRegex(RuntimeError, 'fixture worker exit'):
                canary.main(['supplement', '--control', str(self.control),
                    '--progress-checkpoint', str(self.checkpoint), '--progress-operation', self.operation])
            self.assertEqual(migration_io.read_json, self.read)
        self.assertIs(migration_io.read_json, saved_reader)
        self.assertIs(migration_worker._directory_opener, saved_opener)

    def exercise_original_dispatch(self, regress):
        """Use the actual legacy P4 wrapper/cache, not a mocked delegate."""
        original_guard = canary.original.ScopeGuard(FIXTURE_CONTROL, config())
        observed = []
        class ImmediateCache(StateCache):
            def start(self):
                self.refresh_once()
                return self
        def worker(control, threads):
            # Exactly the exception boundary used by both original lanes.
            # A bad first checkpoint must not enter their empty-history path.
            if regress:
                self.progress['verified_bytes'] = 0
            try:
                previous = auxiliary.read_json(self.node)
            except (OSError, ValueError):
                previous = {}
            observed.append(previous)
            self.assertEqual(previous, self.progress)
            self.assertEqual(threads, 64)
            self.assertIs(migration_worker._directory_opener, canary.native.open_parent_readonly)
            blocked = auxiliary.process_entry(config(), dict(id=1, op='copy', phase=4, round=1),
                                              dict(rel='eai_pwm/protected', kind='file'))
            self.assertTrue(blocked['scope_violation'])
        reader, opener = migration_io.read_json, migration_worker._directory_opener
        delegate, alias = auxiliary.process_entry, auxiliary.read_json
        with mock.patch.object(canary, '__file__', str(self.bundle / 'migration_priority4_path_worker.py')), \
                mock.patch.object(canary, 'validate_bundle'), \
                mock.patch.object(canary, 'validate_imports'), \
                mock.patch.object(canary, 'require_predecessor_exited'), \
                mock.patch.object(migration_io, 'read_json', self.read), \
                mock.patch.object(canary.original, 'ScopeGuard', return_value=original_guard), \
                mock.patch.object(canary.original, 'StateCache', ImmediateCache), \
                mock.patch.object(auxiliary, 'worker', side_effect=worker):
            args = ['supplement', '--control', str(self.control),
                    '--progress-checkpoint', str(self.checkpoint), '--progress-operation', self.operation]
            if regress:
                with self.assertRaises(canary.StartupProgressError):
                    canary.main(args)
                self.assertEqual(observed, [])
            else:
                self.assertEqual(canary.main(args), 0)
                self.assertEqual(observed, [self.progress])
        self.assertIs(migration_io.read_json, reader)
        self.assertIs(migration_worker._directory_opener, opener)
        self.assertIs(auxiliary.process_entry, delegate)
        self.assertIs(auxiliary.read_json, alias)

    def test_actual_original_cache_and_lane_alias_inherit_history(self):
        self.exercise_original_dispatch(False)

    def test_actual_original_zero_history_fallback_cannot_catch_regression(self):
        self.exercise_original_dispatch(True)


if __name__ == '__main__':
    unittest.main()
