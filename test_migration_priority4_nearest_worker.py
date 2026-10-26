import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

import migration_io
import migration_parent_frontier
import migration_priority4_nearest_worker as candidate
import migration_worker
import migration_worker128_run as auxiliary
from migration_state_cache import StateCache
from test_migration_priority4_worker import config, CONTROL as FIXTURE_CONTROL
import test_migration_worker_native_open as native_safety


HOST = 'job-fixture-worker-10'


class NearestBundleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='nearest-bundle-fixture-')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.previous = self.control / candidate.PREVIOUS_NAME
        self.bundle = self.control / 'p4-copy-nearest-fixture'
        self.previous.mkdir()
        self.bundle.mkdir()
        self.cfg = dict(config(), job='fixture')
        original = {}
        for name in ('migration_priority4_worker.py', 'migration_run.py', 'migration_worker.py',
                     'migration_worker128_run.py', 'migration_io.py', 'migration_state_cache.py'):
            payload = ('# original ' + name).encode()
            (self.control / name).write_bytes(payload)
            (self.previous / name).write_bytes(payload)
            original[name] = hashlib.sha256(payload).hexdigest()
        (self.control / 'source-sha256.json').write_text(json.dumps(original))
        for name in candidate.proof.ADDITIONS - {'path-open-policy.json'}:
            (self.previous / name).write_text('# unchanged first-canary helper ' + name)
        old_policy = dict(scope=candidate.proof.SCOPE, control=str(self.control),
            run_id=self.cfg['run_id'], job='fixture', node='worker-10', lane='supplement',
            directory_strategy='native_readonly',
            predecessor_manifest_sha256=candidate.proof.digest_file(self.control / 'source-sha256.json'))
        (self.previous / 'path-open-policy.json').write_text(json.dumps(old_policy))
        names = set(original) | candidate.proof.ADDITIONS
        previous = {n: candidate.proof.digest_file(self.previous / n) for n in names}
        (self.previous / 'source-sha256.json').write_text(json.dumps(previous))
        for name in names:
            shutil.copyfile(self.previous / name, self.bundle / name)
        for name in candidate.ADDITIONS - {candidate.POLICY}:
            (self.bundle / name).write_text('# nearest-only addition ' + name)
        self.policy = candidate.policy_for(self.control, self.cfg)
        self.freeze()
        for patch in (mock.patch.object(candidate.proof.io, 'read_json', return_value=self.cfg),
                      mock.patch.object(candidate.proof.original, 'ScopeGuard'),
                      mock.patch.object(candidate.socket, 'gethostname', return_value=HOST)):
            patch.start()
            self.addCleanup(patch.stop)

    def freeze(self):
        (self.bundle / candidate.POLICY).write_text(json.dumps(self.policy))
        names = set(candidate.proof.static_json(self.previous / 'source-sha256.json')) | candidate.ADDITIONS
        manifest = {n: candidate.proof.digest_file(self.bundle / n) for n in names}
        (self.bundle / 'source-sha256.json').write_text(json.dumps(manifest))

    def test_exact_predecessor_and_node_are_bound(self):
        self.assertEqual(candidate.validate_bundle(self.control, self.bundle, 'supplement'),
                         (self.cfg, self.policy))
        for field, value in (('pid', 123), ('starttime_ticks', '999'), ('argv', ['different'])):
            previous = copy.deepcopy(self.policy)
            self.policy['predecessor'][field] = value
            self.freeze()
            with self.assertRaises(candidate.StartupProgressError):
                candidate.validate_bundle(self.control, self.bundle, 'supplement')
            self.policy = previous
        self.freeze()
        with mock.patch.object(candidate.socket, 'gethostname', return_value='job-fixture-worker-9'):
            with self.assertRaises(candidate.StartupProgressError):
                candidate.validate_bundle(self.control, self.bundle, 'supplement')

    def test_inherited_engine_and_first_canary_cannot_be_rewritten(self):
        for name in ('migration_worker.py', 'migration_priority4_path_worker.py'):
            saved = (self.bundle / name).read_bytes()
            (self.bundle / name).write_text('# changed inherited file')
            self.freeze()
            with self.assertRaises(candidate.StartupProgressError):
                candidate.validate_bundle(self.control, self.bundle, 'supplement')
            (self.bundle / name).write_bytes(saved)
        self.freeze()
        candidate.validate_bundle(self.control, self.bundle, 'supplement')

    def test_wrong_strategy_and_versioned_policy_are_rejected(self):
        self.policy['directory_strategy'] = 'frontier_path_anchor'
        self.freeze()
        with self.assertRaises(candidate.StartupProgressError):
            candidate.validate_bundle(self.control, self.bundle, 'supplement')

        self.policy = candidate.policy_for(self.control, self.cfg)
        self.freeze()
        (self.bundle / '.snapshots' / candidate.POLICY).mkdir(parents=True)
        with self.assertRaises(candidate.StartupProgressError):
            candidate.validate_bundle(self.control, self.bundle, 'supplement')

    def test_configuration_drift_is_not_an_implicit_tuning_change(self):
        self.cfg['supplement_io_bytes_per_second'] += 1
        with self.assertRaises(candidate.StartupProgressError):
            candidate.validate_bundle(self.control, self.bundle, 'supplement')


class NearestStartupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='nearest-startup-fixture-')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.bundle = self.control / 'p4-copy-nearest-fixture'
        self.bundle.mkdir()
        self.checkpoint = self.bundle / 'cache-age-rollout' / (HOST + '-supplement.json')
        self.node = self.control / 'nodes-extra' / (HOST + '.json')
        self.operation = 'fixture-second-operation'
        self.cfg = dict(config(), job='fixture')
        self.policy = {'predecessor': candidate.expected_predecessor(self.control, 'fixture')}
        self.progress = dict(host=HOST, pid=candidate.PREVIOUS_PID, active={}, run_started='same-run',
            bytes=100, streamed_bytes=101, verified_bytes=100, verified_files=7, completed_batches=3,
            broker_retries=2, counts={'copied': 7},
            stages={'p4-r1': dict(copied_bytes=100, counts={'copied': 7}, verified_files=7)})
        self.record = dict(control=str(self.control), migration_scope=candidate.SCOPE,
            directory_strategy=candidate.STRATEGY, user_authorization=candidate.AUTHORIZATION,
            operation=self.operation, host=HOST, lane='supplement', status='replacing',
            lane_leases={}, expected=copy.deepcopy(self.policy['predecessor']), idle=copy.deepcopy(self.progress))
        patch = mock.patch.object(candidate.socket, 'gethostname', return_value=HOST)
        patch.start()
        self.addCleanup(patch.stop)

    def read(self, path):
        path = Path(path)
        if path == self.checkpoint:
            return copy.deepcopy(self.record)
        if path == self.node:
            return copy.deepcopy(self.progress)
        if path == self.control / 'config.json':
            return copy.deepcopy(self.cfg)
        if path == self.control / 'state.json':
            return dict(stage='p4-r1', phase=4, round=1, status='scanning')
        raise FileNotFoundError(str(path))

    def guard(self):
        return candidate.checkpoint_identity(self.control, self.bundle, self.checkpoint,
            self.operation, 'supplement', self.read, self.policy)

    def test_first_node_read_cannot_zero_or_change_run_started(self):
        guard, expected = self.guard()
        self.assertEqual(expected, self.policy['predecessor'])
        for change in (dict(bytes=0), dict(run_started='different'), dict(pid=123), dict(active={'0': {}})):
            saved = self.progress
            self.progress = dict(saved, **change)
            with self.assertRaises(candidate.StartupProgressError):
                guard.read_json(self.node)
            self.assertFalse(guard.consumed)
            self.progress = saved
        self.assertEqual(guard.read_json(self.node), self.progress)
        self.assertTrue(guard.consumed)

    def test_first_authorization_and_wrong_operation_or_leases_are_insufficient(self):
        for change in (dict(user_authorization=candidate.proof.AUTHORIZATION),
                       dict(operation='old-operation'), dict(migration_scope=candidate.proof.SCOPE),
                       dict(lane_leases={'p4-r1': 1}), dict(directory_strategy='native_readonly')):
            saved = self.record
            self.record = dict(saved, **change)
            with self.assertRaises(candidate.StartupProgressError):
                self.guard()
            self.record = saved

    def test_live_predecessor_does_not_reach_original_worker(self):
        original = migration_worker._open_parent
        with mock.patch.object(candidate, '__file__', str(self.bundle / candidate.ENTRY)), \
                mock.patch.object(candidate, 'validate_bundle', return_value=(self.cfg, self.policy)), \
                mock.patch.object(candidate, 'validate_imports'), \
                mock.patch.object(migration_io, 'read_json', self.read), \
                mock.patch.object(candidate.proof, 'identity', return_value={
                    'state': 'S', 'start_ticks': candidate.PREVIOUS_START}), \
                mock.patch.object(candidate.proof.original, 'main') as worker:
            with self.assertRaises(candidate.StartupProgressError):
                candidate.main(['supplement', '--control', str(self.control),
                    '--progress-checkpoint', str(self.checkpoint), '--progress-operation', self.operation])
            worker.assert_not_called()
        self.assertIs(migration_worker._open_parent, original)

    def exercise_actual_wrapper(self, regress):
        scope = candidate.proof.original.ScopeGuard(FIXTURE_CONTROL, config())
        observed = []
        class ImmediateCache(StateCache):
            def start(self):
                self.refresh_once()
                return self
        def worker(control, threads):
            if regress:
                self.progress['verified_bytes'] = 0
            try:
                previous = auxiliary.read_json(self.node)
            except (OSError, ValueError):
                previous = {}
            observed.append(previous)
            self.assertEqual(previous, self.progress)
            self.assertEqual(threads, 64)
            adapter = migration_worker._open_parent
            self.assertIsInstance(adapter, migration_parent_frontier.FrontierParentOpener)
            self.assertIs(adapter.opener, candidate.proof.native.open_parent_readonly)
            self.assertIs(adapter.anchor_opener, candidate.proof.native.open_parent_readonly)
            blocked = auxiliary.process_entry(config(), dict(id=1, op='copy', phase=4, round=1),
                                              dict(rel='eai_pwm/protected', kind='file'))
            self.assertTrue(blocked['scope_violation'])
        reader, opener, parent = migration_io.read_json, migration_worker._directory_opener, migration_worker._open_parent
        delegate, alias = auxiliary.process_entry, auxiliary.read_json
        with mock.patch.object(candidate, '__file__', str(self.bundle / candidate.ENTRY)), \
                mock.patch.object(candidate, 'validate_bundle', return_value=(self.cfg, self.policy)), \
                mock.patch.object(candidate, 'validate_imports'), \
                mock.patch.object(candidate.proof, 'require_predecessor_exited'), \
                mock.patch.object(migration_io, 'read_json', self.read), \
                mock.patch.object(candidate.proof.original, 'ScopeGuard', return_value=scope), \
                mock.patch.object(candidate.proof.original, 'StateCache', ImmediateCache), \
                mock.patch.object(auxiliary, 'worker', side_effect=worker):
            args = ['supplement', '--control', str(self.control), '--progress-checkpoint',
                    str(self.checkpoint), '--progress-operation', self.operation]
            if regress:
                with self.assertRaises(candidate.StartupProgressError):
                    candidate.main(args)
                self.assertEqual(observed, [])
            else:
                self.assertEqual(candidate.main(args), 0)
                self.assertEqual(observed, [self.progress])
        self.assertIs(migration_io.read_json, reader)
        self.assertIs(migration_worker._directory_opener, opener)
        self.assertIs(migration_worker._open_parent, parent)
        self.assertIs(auxiliary.process_entry, delegate)
        self.assertIs(auxiliary.read_json, alias)

    def test_actual_wrapper_inherits_history_and_keeps_scope_and_readonly_strategy(self):
        self.exercise_actual_wrapper(False)

    def test_actual_zero_history_fallback_cannot_catch_bad_checkpoint(self):
        self.exercise_actual_wrapper(True)


class NearestCopySafetyTests(native_safety.NativeCopySafetyTests):
    def setUp(self):
        super().setUp()
        original = migration_worker._open_parent
        self.addCleanup(setattr, migration_worker, '_open_parent', original)
        migration_worker._open_parent = migration_parent_frontier.FrontierParentOpener(
            self.destination, original, check=migration_worker._check_cancel)


if __name__ == '__main__':
    unittest.main()
