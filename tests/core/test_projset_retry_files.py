import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock


REPO = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


retry = load('projset_retry_files', REPO / 'ops/copy/projset_retry_files.py')
recovery = load('test_recovery_for_retry', REPO / 'ops/copy/projset_recovery_copy.py')


class RetryTests(unittest.TestCase):
    def test_transient_estale_is_retried_and_recorded_once(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, target = root / 'source', root / 'target'
            source.mkdir(); target.mkdir()
            (source / 'file').write_bytes(b'payload')
            run = root / 'run'
            output = run / 'recovery' / 'retry'
            output.mkdir(parents=True)
            (run / 'config.json').write_text(json.dumps({
                'source': str(source), 'target': str(target), 'run_id': 'test',
            }))
            entry = dict(recovery.engine._record((source / 'file').stat()),
                         rel='file', kind='file')
            manifest = output / 'manifest.json'
            manifest.write_text(json.dumps({'entries': [entry]}))
            digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
            original = recovery.engine.copy_entry
            attempts = []

            def transient(*args, **kwargs):
                attempts.append(1)
                if len(attempts) == 1:
                    return {'rel': 'file', 'status': 'error',
                            'detail': 'OSError: [Errno 116] Stale file handle'}
                return original(*args, **kwargs)

            try:
                with mock.patch.object(recovery, 'validate'), \
                     mock.patch.object(recovery.engine, 'copy_entry', side_effect=transient):
                    retry.run_worker(run, 'node', manifest, digest, output, 10**9, recovery)
                receipts = retry.latest_receipts(output / 'receipts.jsonl')
                self.assertEqual(receipts['file']['status'], 'copied')
                self.assertEqual(receipts['file']['estale_retries'], 1)
                self.assertEqual((target / 'file').read_bytes(), b'payload')
                self.assertEqual(len(attempts), 2)
            finally:
                recovery.engine.configure_rate_limit(0)

    def test_retry_preserves_conflicts_and_resumes_verified_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, target = root / 'source', root / 'target'
            source.mkdir()
            target.mkdir()
            (source / 'first').write_bytes(b'first payload')
            (source / 'second').write_bytes(b'second payload')
            (target / 'second').write_bytes(b'different data')
            run = root / 'run'
            output = run / 'recovery' / 'retry'
            output.mkdir(parents=True)
            (run / 'config.json').write_text(json.dumps({
                'source': str(source), 'target': str(target), 'run_id': 'test',
            }))
            entries = [dict(recovery.engine._record((source / name).stat()),
                            rel=name, kind='file') for name in ('first', 'second')]
            manifest = output / 'manifest.json'
            manifest.write_text(json.dumps({'entries': entries}))
            digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
            old_source, old_target, old_validate = recovery.SOURCE, recovery.TARGET, recovery.validate
            recovery.SOURCE, recovery.TARGET = str(source), str(target)
            recovery.validate = lambda *args: None
            try:
                retry.run_worker(run, 'node', manifest, digest, output, 10**9, recovery)
                self.assertEqual((target / 'first').read_bytes(), b'first payload')
                self.assertEqual((target / 'second').read_bytes(), b'different data')
                status = json.loads((output / 'status.json').read_text())
                self.assertEqual(status['state'], 'finished_with_errors')
                self.assertEqual(status['counts']['conflict'], 1)

                # A crash can leave a partial final line; resume the durable prefix.
                with (output / 'receipts.jsonl').open('ab') as stream:
                    stream.write(b'{"rel":"partial')
                (target / 'second').unlink()
                # Older frozen recovery modules do not expose the retry helper.
                with mock.patch.object(recovery, 'copy_entry_with_estale_retry', None):
                    retry.run_worker(run, 'node', manifest, digest, output, 10**9, recovery)
                status = json.loads((output / 'status.json').read_text())
                self.assertEqual(status['state'], 'finished')
                self.assertEqual(status['counts']['copied'], 2)
                self.assertEqual((target / 'second').read_bytes(), b'second payload')
                self.assertEqual(len(retry.latest_receipts(output / 'receipts.jsonl')), 2)
            finally:
                recovery.SOURCE, recovery.TARGET, recovery.validate = old_source, old_target, old_validate
                recovery.engine.configure_rate_limit(0)

    def test_manifest_rejects_tampering_and_unsafe_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'manifest.json'
            path.write_text(json.dumps({'entries': [{'rel': '../outside', 'kind': 'file', 'size': 1}]}))
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(ValueError):
                retry.read_manifest(path, digest)
            with self.assertRaises(ValueError):
                retry.read_manifest(path, '0' * 64)

    def test_supervisor_restarts_a_crashed_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            args = SimpleNamespace(output=output, run=output, node='node',
                                   manifest=output / 'manifest.json',
                                   manifest_sha256='a' * 64, rate=1024)
            calls = []

            def launch(*unused_args, **unused_kwargs):
                calls.append(1)
                rc = -9 if len(calls) == 1 else 0
                if rc == 0:
                    (output / 'status.json').write_text(json.dumps({'state': 'finished'}))
                return SimpleNamespace(pid=100 + len(calls), returncode=rc, poll=lambda: rc)

            with mock.patch.object(retry.subprocess, 'Popen', side_effect=launch), \
                 mock.patch.object(retry.time, 'sleep'):
                self.assertEqual(retry.supervise(args), 0)
            status = json.loads((output / 'supervisor.json').read_text())
            self.assertEqual(status['state'], 'finished')
            self.assertEqual(status['attempt'], 2)


if __name__ == '__main__':
    unittest.main()
