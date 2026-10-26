import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import repo
from repo import build, sources


class BundleTests(unittest.TestCase):
    def test_exact_bytes_and_flat_imports(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'src/core').mkdir(parents=True)
            data = b'VALUE = 42\n'
            (root / 'src/core/example.py').write_bytes(data)
            output = build(root)
            self.assertEqual((output / 'example.py').read_bytes(), data)
            self.assertIn(hashlib.sha256(data).hexdigest(), (output / 'SOURCE_MAP.json').read_text())

    def test_duplicates_fail_before_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ('src', 'ops'):
                (root / name).mkdir()
                (root / name / 'same.py').touch()
            with self.assertRaises(ValueError):
                sources(root)
            self.assertFalse((root / '.build').exists())

    def test_existing_destination_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'src').mkdir()
            (root / 'src/a.py').touch()
            with self.assertRaises(FileExistsError):
                build(root, root / 'src')

    def test_test_runner_propagates_exit_and_bundle_cwd(self):
        with mock.patch.object(repo, 'build', return_value=Path('/bundle')), \
                mock.patch.object(repo.subprocess, 'call', return_value=7) as call, \
                mock.patch.object(repo.sys, 'argv', ['repo.py', 'test', 'test_example']):
            self.assertEqual(repo.main(), 7)
            self.assertEqual(call.call_args.kwargs['cwd'], Path('/bundle'))
            self.assertEqual(call.call_args.args[0][-1], 'test_example')

    def test_build_never_executes(self):
        with mock.patch.object(repo, 'build', return_value=Path('/bundle')), \
                mock.patch.object(repo.subprocess, 'call') as call, \
                mock.patch.object(repo.sys, 'argv', ['repo.py', 'build']):
            self.assertEqual(repo.main(), 0)
            call.assert_not_called()

    def test_unknown_entry_rejected_before_build(self):
        with mock.patch.object(repo, 'sources', return_value={}), \
                mock.patch.object(repo, 'build') as builder, \
                mock.patch.object(repo.sys, 'argv', ['repo.py', 'run', '../bad.py']):
            with self.assertRaises(SystemExit):
                repo.main()
            builder.assert_not_called()

    def test_default_suite_isolates_modules_and_reports_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'test_a.py').touch()
            (root / 'test_b.py').touch()
            with mock.patch.object(repo.subprocess, 'call', side_effect=[1, 0]) as call:
                self.assertEqual(repo.run_tests(root, []), 1)
                self.assertEqual(call.call_count, 2)
                self.assertEqual(call.call_args_list[0].args[0][-1], 'test_a')
                self.assertEqual(call.call_args_list[1].args[0][-1], 'test_b')

    def test_empty_suite_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                repo.run_tests(Path(temporary), [])

    def test_known_standalone_fixture_runs_as_script(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / 'test_migration_fresh_resume.py'
            fixture.touch()
            with mock.patch.object(repo.subprocess, 'call', return_value=0) as call:
                self.assertEqual(repo.run_tests(root, []), 0)
                self.assertEqual(call.call_args.args[0][-1], str(fixture))
                self.assertNotIn('unittest', call.call_args.args[0])


if __name__ == '__main__':
    unittest.main()
