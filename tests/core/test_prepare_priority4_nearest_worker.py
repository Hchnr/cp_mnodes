import hashlib
from pathlib import Path
import subprocess
import unittest
from unittest import mock

import prepare_priority4_nearest_worker as preparation


class NearestPreparationTests(unittest.TestCase):
    def test_summary_uses_real_path_hash_and_never_grants_replacement_authority(self):
        for code, status in ((0, 'prepared_native_tests_passed'), (1, 'prepared_native_tests_failed')):
            record = preparation.preparation_record(Path('/fixture/bundle'), {'scope': 'fixture'},
                                                    {'returncode': code})
            self.assertEqual(record['status'], status)
            self.assertEqual(record['preparation_entry_sha256'],
                             hashlib.sha256(Path(preparation.__file__).read_bytes()).hexdigest())
            self.assertFalse(record['authorization_for_copy_replacement'])
            self.assertEqual((record['signals_sent'], record['copy_processes_launched'],
                              record['broker_control_changes']), (0, 0, 0))

    def test_native_failure_cannot_be_reinterpreted_as_success(self):
        failed = subprocess.CompletedProcess(['ssh'], 1, '', 'fixture remote failure')
        with mock.patch.object(preparation.subprocess, 'run', return_value=failed):
            with self.assertRaisesRegex(RuntimeError, 'no apply'):
                preparation.native('print(1)')

    def test_native_is_exact_target_python_stdin_and_has_bounded_timeout(self):
        success = subprocess.CompletedProcess(['ssh'], 0, '{"fixture": true}', '')
        with mock.patch.object(preparation.subprocess, 'run', return_value=success) as run:
            self.assertEqual(preparation.native('fixture_code'), {'fixture': True})
        argv = run.call_args.args[0]
        self.assertEqual(argv[-5:], ['worker-10', 'python3', '-B', '-u', '-'])
        self.assertIn('ForwardAgent=no', argv)
        self.assertIn('ForwardX11=no', argv)
        self.assertEqual(run.call_args.kwargs['input'], 'fixture_code')
        self.assertEqual(run.call_args.kwargs['timeout'], 180)


if __name__ == '__main__':
    unittest.main()
