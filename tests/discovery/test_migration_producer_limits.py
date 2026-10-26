import resource
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

import migration_pipelined_target_filter as producer


class ProducerLimitTests(unittest.TestCase):
    def test_real_ssh_style_limit_allows_more_than_1024_open_files(self):
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard != resource.RLIM_INFINITY and hard < 65536:
            self.skipTest('test process has an intentionally lower hard limit')
        code = '''import os, resource
resource.setrlimit(resource.RLIMIT_NOFILE, (1024, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
from migration_pipelined_target_filter import ensure_producer_nofile
limits = ensure_producer_nofile()
assert limits[0] == resource.RLIM_INFINITY or limits[0] >= 65536
fds=[]
try:
    for _ in range(1536):
        fds.append(os.open('/dev/null', os.O_RDONLY))
    assert len(fds) > 1024
finally:
    for fd in fds:
        os.close(fd)
print('low-limit launch and 1536 simultaneous descriptors verified')
'''
        result = subprocess.run([sys.executable, '-c', code],
                                cwd=Path(__file__).resolve().parent,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('1536 simultaneous descriptors verified', result.stdout)

    def test_ssh_default_is_raised_and_verified(self):
        with mock.patch.object(resource, 'getrlimit', side_effect=[(1024, 1048576), (65536, 1048576)]), \
                mock.patch.object(resource, 'setrlimit') as setter:
            self.assertEqual(producer.ensure_producer_nofile(), (65536, 1048576))
            setter.assert_called_once_with(resource.RLIMIT_NOFILE, (65536, 1048576))

    def test_higher_or_infinite_soft_limit_is_preserved(self):
        for limits in ((131072, 1048576), (resource.RLIM_INFINITY, resource.RLIM_INFINITY)):
            with self.subTest(limits=limits), mock.patch.object(resource, 'getrlimit', return_value=limits), \
                    mock.patch.object(resource, 'setrlimit') as setter:
                self.assertEqual(producer.ensure_producer_nofile(), limits)
                setter.assert_not_called()

    def test_low_hard_limit_fails_before_ancestor_constructor(self):
        with mock.patch.object(resource, 'getrlimit', return_value=(1024, 4096)), \
                mock.patch.object(producer.DistributedTargetFilterResume, '__init__') as ancestor:
            with self.assertRaisesRegex(RuntimeError, 'hard limit'):
                producer.PipelinedTargetFilterResume('/unused', '/unused')
            ancestor.assert_not_called()

    def test_failed_raise_and_failed_readback_are_not_ignored(self):
        with mock.patch.object(resource, 'getrlimit', return_value=(1024, 1048576)), \
                mock.patch.object(resource, 'setrlimit', side_effect=PermissionError('denied')):
            with self.assertRaises(PermissionError):
                producer.ensure_producer_nofile()
        with mock.patch.object(resource, 'getrlimit', return_value=(1024, 1048576)), \
                mock.patch.object(resource, 'setrlimit'):
            with self.assertRaisesRegex(RuntimeError, 'did not reach'):
                producer.ensure_producer_nofile()


if __name__ == '__main__':
    unittest.main()
