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
    'audit_cache_rollout_install', ROOT / 'ops/deployment/projset_audit_cache_rollout_install.py')
install = importlib.util.module_from_spec(spec)
spec.loader.exec_module(install)


class CacheRolloutInstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.run = root / 'run'
        self.work = self.run / 'recovery/audit'
        self.stage = self.work / 'upgrade-test'
        self.stage.mkdir(parents=True)
        (self.work / 'code').mkdir()
        (self.run / 'recovery/ssh.json').write_text('{}')
        (self.run / 'config.json').write_text('{}')
        (self.work / 'STOP').write_text(self.stage.name + '\n')
        self.old, self.new = b'old controller\n', b'new controller\n'
        old_sha, new_sha = install.digest(self.old), install.digest(self.new)
        self.manifest = {'code_sha256': old_sha, 'nodes': 16, 'reporter': True}
        (self.stage / 'old-code.py').write_bytes(self.old)
        (self.stage / 'new-code.py').write_bytes(self.new)
        (self.stage / 'old-manifest.json').write_text(json.dumps(self.manifest))
        (self.work / 'code/projset_full_audit.py').write_bytes(self.old)
        (self.work / 'manifest.json').write_text(json.dumps(self.manifest))
        (self.work / 'tuning.json').write_text(json.dumps(install.OLD_TUNING))
        self.backup = root / 'rollback.sqlite3'
        self.backup.write_bytes(b'checked snapshot')
        (self.stage / 'snapshot-ready.json').write_text(json.dumps({'state': 'ready', 'quick_check': 'ok',
            'host': socket.gethostname(), 'local': str(self.backup), 'size': self.backup.stat().st_size,
            'sha256': '0' * 64}))
        self.patches = mock.patch.multiple(install, STAGE=self.stage, WORK=self.work, RUN=self.run,
                                           FENCE=self.stage.name, OLD_SHA=old_sha, NEW_SHA=new_sha)
        self.patches.start()
        self.identity = mock.patch.object(install, 'validate_node')
        self.identity.start()
        self.path = mock.patch.object(install, 'rollback_path', return_value=self.backup)
        self.path.start()

    def tearDown(self):
        self.path.stop(); self.identity.stop(); self.patches.stop(); self.tmp.cleanup()

    def test_install_and_rollback_keep_existing_worker_tuning(self):
        with contextlib.redirect_stdout(io.StringIO()):
            install.install()
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.new)
        self.assertEqual(json.loads((self.work / 'tuning.json').read_text()), install.NEW_TUNING)
        self.assertTrue((self.work / 'STOP').exists())
        with contextlib.redirect_stdout(io.StringIO()):
            install.install(rollback=True)
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)
        self.assertEqual(json.loads((self.work / 'manifest.json').read_text()), self.manifest)
        self.assertEqual(json.loads((self.work / 'tuning.json').read_text()), install.OLD_TUNING)

    def test_retry_repairs_interrupted_code_manifest_pair(self):
        (self.work / 'code/projset_full_audit.py').write_bytes(self.new)
        with contextlib.redirect_stdout(io.StringIO()):
            install.install()
        self.assertEqual(json.loads((self.work / 'manifest.json').read_text())['code_sha256'], install.NEW_SHA)
        self.assertEqual(json.loads((self.work / 'tuning.json').read_text()), install.NEW_TUNING)

    def test_refuses_unexpected_tuning_before_changing_anything(self):
        (self.work / 'tuning.json').write_text('{"custom":true}')
        with self.assertRaisesRegex(RuntimeError, 'tuning changed'):
            install.install()
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)
        self.assertEqual(json.loads((self.work / 'manifest.json').read_text()), self.manifest)

    def test_refuses_active_controller_or_missing_snapshot(self):
        with (self.work / 'controller.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                install.install()
        self.backup.unlink()
        with self.assertRaises(FileNotFoundError):
            install.install()
        self.assertEqual((self.work / 'code/projset_full_audit.py').read_bytes(), self.old)


if __name__ == '__main__':
    unittest.main()
