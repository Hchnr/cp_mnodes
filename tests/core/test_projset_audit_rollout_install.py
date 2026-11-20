import contextlib
import fcntl
import importlib.util
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    'audit_rollout_install', ROOT / 'ops/deployment/projset_audit_rollout_install.py')
install = importlib.util.module_from_spec(spec)
spec.loader.exec_module(install)


class AuditRolloutInstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.run = self.root / 'run'
        self.work = self.run / 'recovery/audit'
        self.stage = self.work / 'upgrade-test'
        self.stage.mkdir(parents=True)
        (self.work / 'code').mkdir()
        (self.run / 'recovery/ssh.json').write_text('{}')
        (self.run / 'config.json').write_text('{}')
        (self.work / 'STOP').write_text(self.stage.name + '\n')
        self.old, self.new = b'old controller\n', b'new controller\n'
        self.old_sha, self.new_sha = install.digest(self.old), install.digest(self.new)
        self.old_manifest = {'code_sha256': self.old_sha, 'nodes': 16, 'reporter': True}
        (self.stage / 'old-code.py').write_bytes(self.old)
        (self.stage / 'new-code.py').write_bytes(self.new)
        (self.stage / 'old-manifest.json').write_text(json.dumps(self.old_manifest))
        (self.stage / 'tuning.json').write_text(json.dumps(
            {'version': 1, 'default': {'threads': 8, 'prefetch': 2}}))
        (self.work / 'code/projset_full_audit.py').write_bytes(self.old)
        (self.work / 'manifest.json').write_text(json.dumps(self.old_manifest))
        self.backup = self.root / 'rollback.sqlite3'
        self.backup.write_bytes(b'checked snapshot')
        (self.stage / 'snapshot-ready.json').write_text(json.dumps({'state': 'ready', 'quick_check': 'ok',
            'host': socket.gethostname(), 'local': str(self.backup), 'size': self.backup.stat().st_size,
            'sha256': '0' * 64}))
        self.patches = mock.patch.multiple(install, STAGE=self.stage, WORK=self.work, RUN=self.run,
                                           FENCE=self.stage.name, EXPECTED_NEW_SHA=self.new_sha)
        self.patches.start()
        self.identity = mock.patch.object(install, 'validate_node')
        self.identity.start()
        self.path = mock.patch.object(install, 'rollback_path', return_value=self.backup)
        self.path.start()

    def tearDown(self):
        self.path.stop(); self.identity.stop(); self.patches.stop(); self.tmp.cleanup()

    def test_install_and_rollback_preserve_fence_and_manifest(self):
        with contextlib.redirect_stdout(io.StringIO()):
            install.install()
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.new)
        manifest = json.loads((self.work / 'manifest.json').read_text())
        self.assertEqual(manifest, dict(self.old_manifest, code_sha256=self.new_sha))
        self.assertTrue((self.work / 'tuning.json').exists())
        self.assertTrue((self.work / 'STOP').exists())
        with contextlib.redirect_stdout(io.StringIO()):
            install.install(rollback=True)
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)
        self.assertEqual(json.loads((self.work / 'manifest.json').read_text()), self.old_manifest)
        self.assertFalse((self.work / 'tuning.json').exists())
        self.assertTrue((self.work / 'STOP').exists())

    def test_retry_repairs_an_interrupted_pair_of_code_and_manifest(self):
        (self.work / 'code/projset_full_audit.py').write_bytes(self.new)
        with contextlib.redirect_stdout(io.StringIO()):
            install.install()
        self.assertEqual(json.loads((self.work / 'manifest.json').read_text())['code_sha256'], self.new_sha)

    def test_refuses_active_controller_or_changed_tuning(self):
        with (self.work / 'controller.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                install.install()
        (self.work / 'tuning.json').write_text('{"custom":true}')
        with self.assertRaisesRegex(RuntimeError, 'existing tuning'):
            install.install()
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)

    def test_missing_or_wrong_snapshot_never_installs(self):
        self.backup.unlink()
        with self.assertRaises(FileNotFoundError):
            install.install()
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)

    def test_rollback_preserves_later_online_tuning(self):
        with contextlib.redirect_stdout(io.StringIO()):
            install.install()
        tuned = b'{"version":1,"default":{"threads":7,"prefetch":2}}\n'
        (self.work / 'tuning.json').write_bytes(tuned)
        with contextlib.redirect_stdout(io.StringIO()):
            install.install(rollback=True)
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)
        self.assertEqual((self.work / 'tuning.json').read_bytes(), tuned)


if __name__ == '__main__':
    unittest.main()
