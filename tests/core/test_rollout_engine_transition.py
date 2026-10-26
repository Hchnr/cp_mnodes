"""Local-only proofs for the narrowly scoped worker-engine transition gate."""
import copy
import datetime
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

import rollout_cache_age as rollout


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class EngineTransitionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.control = Path(self.temporary.name).resolve()
        self.previous, self.proposed = (self.control / name for name in ('previous', 'proposed'))
        for folder in (self.previous, self.proposed):
            folder.mkdir()
            for name in rollout.ENGINE_FILES:
                (folder / name).write_text('# unchanged ' + name + '\n')
            (folder / 'migration_cached_worker.py').write_text('# wrapper\n')
        self.manifest = self.proposed / 'engine-transition.json'

    def prepare(self):
        (self.proposed / 'migration_worker.py').write_text('# open-first worker\n')
        data = {'scope': 'directory-open-first-v1', 'control': str(self.control),
                'bundle': str(self.proposed),
                'old_sha256': digest(self.previous / 'migration_worker.py'),
                'new_sha256': digest(self.proposed / 'migration_worker.py')}
        self.manifest.write_text(json.dumps(data))
        return data

    def validate(self, manifest=None):
        return rollout.validate_engine_transition(self.previous, self.proposed, manifest, self.control)

    def test_default_retains_six_file_equality(self):
        self.assertIsNone(self.validate())
        for name in rollout.ENGINE_FILES:
            path = self.proposed / name
            original = path.read_text()
            path.write_text('# changed\n')
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, 'engine differs'):
                self.validate()
            path.write_text(original)

    def test_manifest_permits_only_bound_complete_worker_source(self):
        data = self.prepare()
        proof = self.validate(self.manifest)
        self.assertEqual(proof['manifest_sha256'], digest(self.manifest))
        self.assertEqual(proof['old_sha256'], data['old_sha256'])
        self.assertEqual(proof['new_sha256'], data['new_sha256'])
        self.assertEqual(proof['manifest_path'], str(self.manifest))
        for name in rollout.ENGINE_FILES:
            if name == 'migration_worker.py':
                continue
            path = self.proposed / name
            original = path.read_text()
            path.write_text('# unrelated upgrade\n')
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, 'engine differs'):
                self.validate(self.manifest)
            path.write_text(original)

    def test_path_launcher_transition_requires_equal_engine_and_bound_launcher_policy(self):
        policy = self.proposed / 'worker-directory-policy.json'
        policy.write_text(json.dumps({'scope': 'destination-opath-checkpoint-v1'}))
        (self.proposed / 'migration_linux_open.py').write_text('# path helper\n')
        data = {'scope': 'destination-opath-checkpoint-v1', 'control': str(self.control),
                'bundle': str(self.proposed),
                'old_sha256': digest(self.previous / 'migration_worker.py'),
                'new_sha256': digest(self.proposed / 'migration_worker.py'),
                'launcher_sha256': {name: digest(self.proposed / name) for name in (
                    'migration_cached_worker.py', 'migration_linux_open.py', 'worker-directory-policy.json')}}
        self.manifest.write_text(json.dumps(data))
        proof = self.validate(self.manifest)
        self.assertEqual(proof['scope'], data['scope'])
        for name in rollout.ENGINE_FILES + tuple(data['launcher_sha256']):
            path = self.proposed / name
            old = path.read_bytes()
            path.write_bytes(old + b'# changed\n')
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                self.validate(self.manifest)
            path.write_bytes(old)

    def test_path_launcher_does_not_permit_even_an_explicit_worker_change(self):
        data = self.prepare()
        data['scope'] = 'destination-opath-checkpoint-v1'
        self.manifest.write_text(json.dumps(data))
        with self.assertRaisesRegex(RuntimeError, 'complete data engine unchanged'):
            self.validate(self.manifest)

    def test_path_launcher_missing_progress_proof_prevents_first_signal(self):
        (self.proposed / 'worker-directory-policy.json').write_text(
            json.dumps({'scope': 'destination-opath-checkpoint-v1'}))
        (self.proposed / 'migration_linux_open.py').write_text('# path helper\n')
        self.manifest.write_text(json.dumps({
            'scope': 'destination-opath-checkpoint-v1', 'control': str(self.control),
            'bundle': str(self.proposed),
            'old_sha256': digest(self.previous / 'migration_worker.py'),
            'new_sha256': digest(self.proposed / 'migration_worker.py'),
            'launcher_sha256': {name: digest(self.proposed / name) for name in (
                'migration_cached_worker.py', 'migration_linux_open.py', 'worker-directory-policy.json')}}))
        calls, _ = self.invoke_until_signal(self.remote_fixture(), 'startup checkpoint')
        self.assertEqual(calls, 0)

    def test_wrong_scope_control_bundle_and_partial_or_wrong_hash_rejected(self):
        data = self.prepare()
        invalid = ({'scope': 'arbitrary-upgrade'}, {'control': str(self.control) + '/elsewhere'},
                   {'bundle': str(self.previous)}, {'old_sha256': 'a' * 63},
                   {'old_sha256': 'A' * 64}, {'old_sha256': 'a' * 64},
                   {'new_sha256': 'b' * 64}, {'new_sha256': data['old_sha256']})
        for patch in invalid:
            self.manifest.write_text(json.dumps(dict(data, **patch)))
            with self.subTest(patch=patch), self.assertRaises(RuntimeError):
                self.validate(self.manifest)

    def test_outside_or_escaping_manifest_rejected(self):
        self.prepare()
        outside = self.control / 'outside.json'
        outside.write_bytes(self.manifest.read_bytes())
        escaping = self.proposed / 'escaping.json'
        escaping.symlink_to(outside)
        for path in (outside, escaping):
            with self.subTest(path=path), self.assertRaisesRegex(RuntimeError, 'inside'):
                self.validate(path)

    def test_engine_source_symlink_escape_rejected(self):
        self.prepare()
        path = self.proposed / 'migration_scan.py'
        path.unlink()
        path.symlink_to(self.previous / path.name)
        with self.assertRaisesRegex(RuntimeError, 'escapes'):
            self.validate(self.manifest)

    def test_proposed_or_previous_bundle_outside_control_rejected(self):
        self.prepare()
        foreign = self.control.parent / 'outside-engine-bundle'
        for previous, proposed in ((foreign, self.proposed), (self.previous, foreign)):
            with self.subTest(previous=previous, proposed=proposed), self.assertRaisesRegex(RuntimeError, 'inside'):
                rollout.validate_engine_transition(previous, proposed, self.manifest, self.control)

    def test_real_previous_engine_at_control_root_is_allowed(self):
        self.prepare()
        for name in rollout.ENGINE_FILES:
            (self.control / name).write_bytes((self.previous / name).read_bytes())
        (self.control / 'migration_cached_worker.py').write_text('# original root wrapper\n')
        proof = rollout.validate_engine_transition(self.control, self.proposed,
                                                    self.manifest, self.control)
        self.assertEqual(proof['old_sha256'], digest(self.control / 'migration_worker.py'))
        self.assertEqual(proof['bundle'], str(self.proposed))

    def test_proposed_engine_cannot_replace_control_root(self):
        self.prepare()
        with self.assertRaisesRegex(RuntimeError, 'inside'):
            rollout.validate_engine_transition(self.previous, self.control,
                                                self.manifest, self.control)

    def test_full_source_changes_after_manifest_rejected_on_either_side(self):
        self.prepare()
        for folder in (self.previous, self.proposed):
            path = folder / 'migration_worker.py'
            original = path.read_bytes()
            path.write_bytes(original + b'# unbound tail\n')
            with self.subTest(folder=folder), self.assertRaisesRegex(RuntimeError, 'SHA256 differs'):
                self.validate(self.manifest)
            path.write_bytes(original)

    def remote_fixture(self, *, mutate_after_checkpoint=None, manifest=True):
        run_id = 'engine-transition-fixture-' + uuid.uuid4().hex
        lock = Path('/tmp/' + run_id + '-primary-cache-age-replace.lock')
        self.addCleanup(lambda: lock.unlink(missing_ok=True))
        identity = {'pid': 10, 'state': 'S', 'starttime_ticks': '2',
                    'argv': ['python3', str(self.previous / 'migration_cached_worker.py'),
                             'primary', '--control', str(self.control)]}
        now = time.time()
        snapshot = {'pid': 10, 'updated': datetime.datetime.fromtimestamp(now - 1,
                      datetime.timezone.utc).isoformat(), 'active': {}}
        record = {'operation': 'fixture', 'expected': identity, 'lane_leases': {},
                  'status': 'idle_verified', 'barrier': now - 5}
        cfg = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
               'run_id': run_id, 'broker_url': 'http://fixture', 'broker_token': 'fixture-only'}
        args = SimpleNamespace(control=str(self.control), host='fixture-host', lane='primary',
                               action='replace', operation='fixture', max_age=30,
                               wrapper=str(self.proposed / 'migration_cached_worker.py'),
                               engine_manifest=str(self.manifest) if manifest else None,
                               gate_timeout=5)
        saved = []
        def read(path):
            path = Path(path)
            if path.name == 'config.json':
                return cfg
            if path.name == 'state.json':
                return {'status': 'copying'}
            if path.parent.name == 'nodes':
                return snapshot
            return record
        def save(path, value):
            saved.append(copy.deepcopy(value))
            if mutate_after_checkpoint:
                mutate_after_checkpoint()
        opener = SimpleNamespace(open=lambda *args, **kw: io.StringIO(json.dumps(
            {'draining_hosts': ['fixture-host']})))
        return args, saved, read, save, opener, identity

    def invoke_until_signal(self, fixture, expected_error):
        args, saved, read, save, opener, identity = fixture
        with mock.patch.object(rollout, 'read_json', side_effect=read), \
                mock.patch.object(rollout, 'write_json', side_effect=save), \
                mock.patch.object(rollout.socket, 'gethostname', lambda: 'fixture-host'), \
                mock.patch.object(rollout.urllib.request, 'build_opener', lambda *args: opener), \
                mock.patch.object(rollout, 'proc_identity', lambda _pid: identity), \
                mock.patch.object(rollout, 'payload_fds', lambda *args: []), \
                mock.patch.object(rollout.resource, 'getrlimit', lambda _: (65536, 65536)), \
                mock.patch.object(rollout.os, 'kill', side_effect=RuntimeError('signal sentinel')) as kill, \
                mock.patch.object(rollout.subprocess, 'Popen') as launch:
            with self.assertRaisesRegex(RuntimeError, expected_error):
                rollout.remote(args)
            launch.assert_not_called()
            return kill.call_count, saved

    def test_allowed_transition_digest_recorded_before_one_signal(self):
        self.prepare()
        calls, saved = self.invoke_until_signal(self.remote_fixture(), 'signal sentinel')
        self.assertEqual(calls, 1)
        self.assertEqual(saved[-1]['engine_transition']['manifest_sha256'], digest(self.manifest))

    def test_manifest_digest_rechecked_after_checkpoint_before_signal(self):
        self.prepare()
        def change():
            self.manifest.write_bytes(self.manifest.read_bytes() + b'\n')
        calls, saved = self.invoke_until_signal(self.remote_fixture(mutate_after_checkpoint=change),
                                                'manifest changed before signal')
        self.assertEqual(calls, 0)
        self.assertTrue(saved)

    def test_engine_rechecked_after_checkpoint_before_signal(self):
        self.prepare()
        def change():
            (self.proposed / 'migration_io.py').write_text('# unexpected upgrade\n')
        calls, _ = self.invoke_until_signal(self.remote_fixture(mutate_after_checkpoint=change),
                                            'engine differs')
        self.assertEqual(calls, 0)

    def test_even_matching_unrelated_change_after_checkpoint_prevents_signal(self):
        self.prepare()
        def change():
            for folder in (self.previous, self.proposed):
                (folder / 'migration_scan.py').write_text('# unexpected shared upgrade\n')
        calls, _ = self.invoke_until_signal(self.remote_fixture(mutate_after_checkpoint=change),
                                            'manifest changed before signal')
        self.assertEqual(calls, 0)

    def test_missing_manifest_keeps_old_rejection_before_signal(self):
        self.prepare()
        calls, saved = self.invoke_until_signal(self.remote_fixture(manifest=False), 'engine differs')
        self.assertEqual(calls, 0)
        self.assertEqual(saved, [])

    def test_manifest_cli_requires_wrapper_before_remote_action(self):
        args = ['rollout_cache_age.py', '--action', 'hold', '--control', str(self.control),
                '--engine-manifest', str(self.manifest)]
        with mock.patch.object(rollout.sys, 'argv', args), \
                mock.patch.object(rollout.sys, 'stderr', io.StringIO()), \
                mock.patch.object(rollout, 'remote') as remote:
            with self.assertRaises(SystemExit):
                rollout.main()
            remote.assert_not_called()

    def test_manifest_cli_reaches_remote_as_explicit_option(self):
        args = ['rollout_cache_age.py', '--action', 'hold', '--control', str(self.control),
                '--engine-manifest', str(self.manifest),
                '--wrapper', str(self.proposed / 'migration_cached_worker.py')]
        with mock.patch.object(rollout.sys, 'argv', args), mock.patch.object(rollout, 'remote') as remote:
            rollout.main()
        self.assertEqual(remote.call_args.args[0].engine_manifest, str(self.manifest))


if __name__ == '__main__':
    unittest.main()
