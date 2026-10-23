"""Local lease-preservation guards; never touch a production process/run."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

import deploy_sharded_broker as deploy


class BrokerHandoverGuards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='broker-handover-guard-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'broker.sqlite'
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,owner TEXT,lease TEXT,'
                        'status TEXT,payload TEXT,running TEXT,PRIMARY KEY(stage,id))')
        self.db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?,?)',
                        ('p3-r3', 10, 'node-1', 'lease10', 'leased', '{}', '/old/running/task10'))
        self.db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?,?)',
                        ('p3-r3', 11, 'node-2', 'lease11', 'done', '{}', '/old/running/task11'))
        self.db.commit()

    def test_legacy_leases_survive_while_completion_and_new_claims_continue(self):
        before = deploy.database_snapshot(self.path)
        self.db.execute("UPDATE claims SET status='done' WHERE id=10")
        self.db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?,?)',
                        ('p3-r3', 12, 'node-3', 'lease12', 'leased', '{}', '/new/running/12/task12'))
        self.db.commit()
        after = deploy.database_snapshot(self.path)
        deploy.validate_preserved_database(before, after)
        self.assertEqual(after['counts'], {'done': 2, 'leased': 1})

    def test_changed_owner_lease_or_old_absolute_running_path_is_rejected(self):
        before = deploy.database_snapshot(self.path)
        for field in ('owner', 'lease', 'running'):
            with self.subTest(field=field):
                self.db.execute('SAVEPOINT fixture')
                self.db.execute('UPDATE claims SET ' + field + "='changed' WHERE id=10")
                self.db.commit()
                with self.assertRaisesRegex(RuntimeError, 'inherited lease'):
                    deploy.validate_preserved_database(before, deploy.database_snapshot(self.path))
                self.db.execute('UPDATE claims SET owner=?,lease=?,running=? WHERE id=10',
                                ('node-1', 'lease10', '/old/running/task10'))
                self.db.commit()

    def test_removed_claim_or_replaced_inode_is_rejected(self):
        before = deploy.database_snapshot(self.path)
        changed = dict(before, inode=before['inode'] + 1)
        with self.assertRaises(RuntimeError):
            deploy.validate_preserved_database(before, changed)
        self.db.execute('DELETE FROM claims WHERE id=10')
        self.db.commit()
        with self.assertRaises(RuntimeError):
            deploy.validate_preserved_database(before, deploy.database_snapshot(self.path))

    def test_full_identity_and_staged_source_cannot_silently_change(self):
        actual = deploy.identity(os.getpid())
        self.assertTrue(deploy.same_process(actual, actual))
        for changed in (dict(actual, pid=actual['pid'] + 1),
                        dict(actual, starttime_ticks='0'), dict(actual, argv=['unknown'])):
            self.assertFalse(deploy.same_process(actual, changed))
        self.assertFalse(deploy.same_process(dict(actual, state='Z'), actual))
        bundle = self.root / 'bundle'
        bundle.mkdir()
        manifest = {}
        for name in ('migration_broker_sharded.py', 'migration_broker_parallel.py', 'migration_broker.py'):
            content = '# fixture module\n'
            (bundle / name).write_text(content)
            manifest[name] = hashlib.sha256(content.encode()).hexdigest()
        path = bundle / 'source-sha256.json'
        path.write_text(json.dumps(manifest))
        binding = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(deploy.staged_source(bundle, binding), bundle / 'migration_broker_sharded.py')
        (bundle / 'migration_broker.py').write_text('# changed\n')
        with self.assertRaisesRegex(RuntimeError, 'source file binding'):
            deploy.staged_source(bundle, binding)

    def test_default_identity_keeps_the_original_broker_binding(self):
        expected = deploy.expected_broker(self.root)
        self.assertEqual(expected['pid'], 166974)
        self.assertEqual(expected['starttime_ticks'], '500376938')
        self.assertEqual(expected['control'], str(self.root))
        self.assertEqual(expected['argv'][2], str(self.root / 'migration_broker_parallel.py'))

    def test_frozen_new_identity_binds_full_argv_control_and_pid_reuse(self):
        control = self.root / 'control'
        old_bundle, new_bundle = control / 'old-bundle', control / 'new-bundle'
        old_bundle.mkdir(parents=True)
        new_bundle.mkdir()
        entry = old_bundle / 'migration_broker_sharded.py'
        entry.write_text('# frozen old broker\n')
        path = new_bundle / 'expected-current-broker.json'
        value = {'control': str(control), 'pid': 127101, 'starttime_ticks': '507800965',
                 'argv': ['/usr/bin/python3', '-u', str(entry), '--control', str(control),
                          '--port', '37651']}
        path.write_text(json.dumps(value))
        expected = deploy.expected_broker(control, path)
        self.assertEqual(expected['pid'], 127101)
        self.assertEqual(expected['identity_sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertTrue(deploy.same_process(dict(value, state='S'), expected))
        self.assertFalse(deploy.same_process(dict(value, state='S', starttime_ticks='507800966'), expected))
        self.assertFalse(deploy.same_process(dict(value, state='S', argv=value['argv'][:-1]+['1']), expected))
        for changed in (dict(value, control=str(self.root)), dict(value, pid=True),
                        dict(value, argv=value['argv'][:4]+[str(self.root)]+value['argv'][5:]),
                        dict(value, argv=['/usr/bin/python3', '-u', str(self.root/'unrelated.py'),
                                          '--control', str(control), '--port', '37651'])):
            with self.subTest(changed=changed):
                path.write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    deploy.expected_broker(control, path)
        outside = self.root / 'outside-identity.json'
        outside.write_text(json.dumps(value))
        with self.assertRaises(ValueError):
            deploy.expected_broker(control, outside)


if __name__ == '__main__':
    unittest.main()
