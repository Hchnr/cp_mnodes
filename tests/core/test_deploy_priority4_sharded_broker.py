"""P4-specific handover guards; no production roles or files are touched."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import deploy_sharded_broker as deploy
from test_deploy_sharded_broker import BrokerHandoverGuards


class Priority4HandoverGuards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='p4-broker-handover-')
        self.addCleanup(self.tmp.cleanup)
        self.control = Path(self.tmp.name) / 'control'
        self.bundle = self.control / 'new-bundle'
        self.bundle.mkdir(parents=True)
        self.entry = self.control / 'migration_priority4_broker.py'
        self.entry.write_text('# frozen fixture\n')
        self.path = self.bundle / 'expected-current-broker.json'
        self.expected = dict(control=str(self.control), pid=123, starttime_ticks='456',
                             argv=['/usr/bin/python3', '-u', str(self.entry), '--control',
                                   str(self.control), '--port', '37654'])

    def publish_expected(self, value=None):
        self.path.write_text(json.dumps(self.expected if value is None else value))

    def test_p4_requires_explicit_identity_and_its_own_port_entry(self):
        with self.assertRaisesRegex(ValueError, 'frozen explicit identity'):
            deploy.expected_broker(self.control, priority4=True)
        self.publish_expected()
        self.assertEqual(deploy.expected_broker(self.control, self.path, priority4=True)['pid'], 123)
        with self.assertRaises(ValueError):
            deploy.expected_broker(self.control, self.path)
        for bad in (self.expected['argv'][:-1] + ['37651'],
                    self.expected['argv'][:2] + [str(self.control / 'migration_broker.py')] +
                    self.expected['argv'][3:]):
            self.publish_expected(dict(self.expected, argv=bad))
            with self.assertRaises(ValueError):
                deploy.expected_broker(self.control, self.path, priority4=True)

    def test_staged_p4_source_requires_its_inherited_scope_modules(self):
        manifest = {}
        for name in ('migration_priority4_broker_sharded.py', 'migration_priority4_broker.py',
                     'migration_broker.py'):
            content = '# fixture module\n'
            (self.bundle / name).write_text(content)
            manifest[name] = hashlib.sha256(content.encode()).hexdigest()
        path = self.bundle / 'source-sha256.json'
        path.write_text(json.dumps(manifest))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(deploy.staged_source(self.bundle, digest, priority4=True).name,
                         'migration_priority4_broker_sharded.py')
        with self.assertRaises(RuntimeError):
            deploy.staged_source(self.bundle, digest)
        (self.bundle / 'migration_priority4_broker.py').write_text('# changed\n')
        with self.assertRaisesRegex(RuntimeError, 'source file binding'):
            deploy.staged_source(self.bundle, digest, priority4=True)

    def test_all_protected_producers_copies_helpers_must_remain_exactly_live(self):
        values = [dict(pid=200 + i, starttime_ticks=str(i + 1), argv=['python3', 'role-' + str(i)])
                  for i in range(6)]
        expected = dict(self.expected, protected_processes=values)
        live = {x['pid']: dict(x, state='S') for x in values}
        with mock.patch.object(deploy, 'identity', side_effect=live.get):
            self.assertEqual(deploy.protected_identities(expected), values)
            for bad in (None, values[:5], values[:5] + values[:1]):
                with self.assertRaises(ValueError):
                    deploy.protected_identities(dict(expected, protected_processes=bad))
            for change in (dict(live[200], state='Z'), dict(live[200], starttime_ticks='reused'),
                           dict(live[200], argv=['changed'])):
                live[200] = change
                with self.assertRaises(RuntimeError):
                    deploy.protected_identities(expected)
            live[200] = dict(values[0], state='S')
            with self.assertRaises(ValueError):
                deploy.protected_identities(dict(expected, protected_processes=values[:5] +
                                                [dict(values[5], pid=123)]))


class InheritedPayloadGuards(BrokerHandoverGuards):
    def test_persisted_json_checkpoint_can_be_validated_against_live_sqlite(self):
        before = json.loads(json.dumps(deploy.database_snapshot(self.path)))
        self.db.execute("UPDATE claims SET status='done' WHERE id=10")
        self.db.commit()
        deploy.validate_preserved_database(before, deploy.database_snapshot(self.path))

    def test_lease_payload_and_done_monotonicity_are_preserved(self):
        before = deploy.database_snapshot(self.path)
        self.db.execute("UPDATE claims SET payload='changed' WHERE id=10")
        self.db.commit()
        with self.assertRaisesRegex(RuntimeError, 'payload was changed'):
            deploy.validate_preserved_database(before, deploy.database_snapshot(self.path))
        self.db.execute("UPDATE claims SET payload='{}' WHERE id=10")
        self.db.execute("UPDATE claims SET status='leased' WHERE id=11")
        self.db.commit()
        with self.assertRaisesRegex(RuntimeError, 'not preserved'):
            deploy.validate_preserved_database(before, deploy.database_snapshot(self.path))


if __name__ == '__main__':
    unittest.main()
