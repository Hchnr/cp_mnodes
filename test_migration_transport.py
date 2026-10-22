"""Local SSH routing fixtures; subprocess is mocked and no SSH is launched."""

import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


WORK = Path(__file__).absolute().parent
sys.path.insert(0, str(WORK))
import migration_launch as transport
from migration_io import write_json


OLD = '0a326937-3eb4-4da4-8b70-3a50586e1976'
NEW = '1e56d871-526b-4036-950b-d11340c193ba'


class TransportTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(dir=WORK, prefix='ssh-route-fixture-')
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name)
        for patch in (mock.patch.object(transport, 'BASE', self.base),
                      mock.patch.object(transport, 'JOB', NEW),
                      mock.patch.object(transport, 'OWNER', None)):
            patch.start()
            self.addCleanup(patch.stop)

    def active(self, job, owner=None):
        value = {'job': job}
        if owner is not None:
            value['ssh_owner'] = owner
        write_json(self.base / 'active_migration.json', value)

    def expected(self, job, owner, node='master-0'):
        return f'job-{job}-{node}.{owner}.baai-eai_pwm.ch1-calc1.job@ssh.platform-multi.baai.ac.cn'

    def test_latest_matching_active_owner_is_ybai(self):
        self.active(OLD, 'hechenrui')
        self.active(NEW, 'ybai')
        # Canonical compatibility JSON stays old; route must use read_json.
        self.assertEqual(json.loads((self.base / 'active_migration.json').read_text())['job'], OLD)
        self.assertEqual(transport.ssh_destination('master-0'), self.expected(NEW, 'ybai'))

    def test_new_active_owner_does_not_leak_to_old_job(self):
        self.active(NEW, 'ybai')
        self.assertEqual(transport.ssh_destination('master-0', job=OLD), self.expected(OLD, 'hechenrui'))

    def test_legacy_active_without_owner_keeps_hechenrui(self):
        self.active(OLD)
        transport.JOB = OLD
        self.assertEqual(transport.ssh_destination('master-0'), self.expected(OLD, 'hechenrui'))

    def test_explicit_owner_supports_preflight_while_active_is_old(self):
        self.active(OLD)
        transport.OWNER = 'ybai'
        self.assertEqual(transport.ssh_destination('master-0'), self.expected(NEW, 'ybai'))

    def test_explicit_argument_wins_and_does_not_change_active(self):
        self.active(OLD)
        transport.OWNER = 'hechenrui'
        before = (self.base / 'active_migration.json').read_bytes()
        self.assertEqual(transport.ssh_destination('worker-10', owner='ybai'),
                         self.expected(NEW, 'ybai', 'worker-10'))
        self.assertEqual((self.base / 'active_migration.json').read_bytes(), before)

    def test_missing_active_uses_legacy_default(self):
        self.assertEqual(transport.ssh_destination('master-0', job=OLD), self.expected(OLD, 'hechenrui'))

    def test_invalid_owner_refuses_subprocess(self):
        transport.OWNER = 'bad owner'
        with mock.patch.object(transport.subprocess, 'run') as run:
            with self.assertRaises(ValueError):
                transport.ssh('master-0', 'pass')
            run.assert_not_called()

    def test_ssh_command_uses_current_owner_and_original_protocol(self):
        self.active(NEW, 'ybai')
        with mock.patch.object(transport.subprocess, 'run',
                               return_value=SimpleNamespace(returncode=0, stdout='ok\n', stderr='')) as run:
            self.assertEqual(transport.ssh('worker-10', 'print("ok")'), ('worker-10', 'ok'))
        args, kwargs = run.call_args
        self.assertEqual(args[0][-2:], [self.expected(NEW, 'ybai', 'worker-10'), 'python3 -'])
        self.assertEqual(args[0][args[0].index('-p') + 1], '2222')
        self.assertEqual(kwargs['input'], 'print("ok")')
        self.assertEqual(kwargs['timeout'], 90)


if __name__ == '__main__':
    unittest.main()
