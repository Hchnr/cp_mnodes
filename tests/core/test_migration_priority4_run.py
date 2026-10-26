"""P4 coordinator fixtures stay in private temporary trees/local databases."""
import contextlib
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
import uuid

import migration_io
import migration_priority4_run as p4
import migration_priority4_scan as scanner
import migration_priority4_worker as scope
import migration_worker as engine
from migration_run import process_entry
from test_migration_priority4_worker import config, CONTROL


class Priority4CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='p4-coordinator-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.control = self.root / 'control'
        self.control.mkdir()
        self.source, self.destination = self.root / 'source', self.root / 'destination'
        self.source.mkdir(); self.destination.mkdir()
        self.cfg = dict(config(), job='fixture', training_watchdog=False,
                        parallel_with_control=config()['covered_by_controls'][0])
        self.guard = scope.ScopeGuard(CONTROL, self.cfg)
        record = dict(version=1, mode='independent_concurrent_p4', control=str(self.control),
                      run_id=self.cfg['run_id'], job='fixture', source=self.cfg['source'],
                      destination=self.cfg['destination'], exclude=['eai_pwm'],
                      predecessor_control=self.cfg['parallel_with_control'],
                      existing_roles_unchanged=True, mount_root_metadata='preserve_existing',
                      verified=True, user_authorization='允许先验证 P4 独立隔离，再并行搬运')
        for name, value in [('config.json', self.cfg), ('isolation.json', record),
                            ('state.json', dict(status='starting', stage=None, phases={}))]:
            migration_io.write_json(self.control / name, value)
        self.local = self.root / 'local'; self.local.mkdir()
        original_connect = sqlite3.connect
        def connect(path, *args, **kwargs):
            path = str(path)
            prefix = '/tmp/' + self.cfg['run_id']
            if path.startswith('file://' + prefix):
                path = 'file://' + str(self.local) + path[len('file:///tmp'):]
            elif path.startswith('file:' + prefix):
                path = 'file:' + str(self.local) + path[len('file:/tmp'):]
            elif path.startswith(prefix):
                path = str(self.local) + path[len('/tmp'):]
            return original_connect(path, *args, **kwargs)
        # Broker path validation is mapped separately, not replaced by a live
        # /tmp database. All connections and fixture payloads remain private.
        self.connect = connect
        self.patches = [mock.patch.object(p4, 'ScopeGuard', return_value=self.guard),
                        mock.patch.object(p4, 'validate_mounts'),
                        mock.patch.object(p4.sqlite3, 'connect', side_effect=connect)]
        for patch in self.patches:
            patch.start(); self.addCleanup(patch.stop)
        self.run = p4.Priority4Coordinator(self.control)
        self.addCleanup(self.run.db.close)

    def make_broker(self):
        # The private redirect affects Path validation as well as SQLite open.
        path = self.local / (self.cfg['run_id'] + '-broker.sqlite')
        with self.connect(path) as db:
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,status TEXT,payload TEXT)')
        original_is_file = Path.is_file
        original_is_symlink = Path.is_symlink
        broker_name = '/tmp/' + self.cfg['run_id'] + '-broker.sqlite'
        a = mock.patch.object(Path, 'is_file', lambda p: True if str(p) == broker_name else original_is_file(p))
        b = mock.patch.object(Path, 'is_symlink', lambda p: False if str(p) == broker_name else original_is_symlink(p))
        a.start(); b.start(); self.addCleanup(a.stop); self.addCleanup(b.stop)
        return path

    def put(self, rel, data=b'payload'):
        path = self.source / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return dict(engine._record(path.stat()), rel=rel, kind='file')

    def scan(self):
        real_scan = scanner.scan_phase
        def mapped(source, *args, **kwargs):
            return real_scan(self.source, *args, **kwargs)
        with mock.patch.object(p4, 'scan_phase', side_effect=mapped):
            return self.run.scan(4, 1)

    def test_missing_or_mismatched_authorization_fails_before_index(self):
        value = migration_io.read_json(self.control / 'isolation.json')
        for key, bad in [('verified', False), ('mode', 'successor'), ('run_id', 'other'),
                         ('exclude', []), ('user_authorization', 'not approved'),
                         ('existing_roles_unchanged', False)]:
            with self.subTest(key=key):
                migration_io.write_json(self.control / 'isolation.json', dict(value, **{key: bad}))
                with self.assertRaises(ValueError):
                    p4.validate_authorization(self.control, self.cfg)
        migration_io.write_json(self.control / 'isolation.json', value)

    def test_cross_scope_resume_and_watchdog_are_rejected(self):
        for values in [dict(resume_from='/old-eai'), dict(training_watchdog=True)]:
            with self.assertRaises(ValueError):
                p4.validate_authorization(self.control, dict(self.cfg, **values))

    def test_missing_local_broker_does_not_publish_tasks(self):
        self.put('other/a')
        with self.assertRaisesRegex(RuntimeError, 'lease recovery'):
            self.scan()
        self.assertEqual(self.run.archives('p4-r1'), [])

    def test_real_inventory_archives_before_publish_and_exactly_excludes_eai(self):
        self.make_broker()
        self.put('other/a'); self.put('eai_pwm/private'); self.put('other/eai_pwm/keep')
        stage, manifest, stats = self.scan()
        self.assertEqual(stats['files'], 2)
        archives = self.run.archives(stage)
        self.assertEqual(len(archives), 1)
        task = migration_io.read_json(archives[0])
        pending = self.control / 'stages' / stage / 'pending' / '0' / '0.json'
        self.assertEqual(migration_io.read_json(pending), task)
        self.assertEqual({e['rel'] for e in task['entries']}, {'other/a', 'other/eai_pwm/keep'})
        self.assertFalse(any(json.loads(line)['rel'].startswith('eai_pwm') for line in manifest.read_text().splitlines()))
        self.assertEqual(self.run.state['task_publication']['pending'], 0)

    def test_producer_rescan_reuses_archived_paths_and_continues_ids(self):
        self.make_broker(); self.put('other/a')
        self.scan()
        self.put('other/b')
        self.scan()
        tasks = [migration_io.read_json(path) for path in self.run.archives('p4-r1')]
        self.assertEqual([t['id'] for t in tasks], [0, 1])
        self.assertEqual([e['rel'] for t in tasks for e in t['entries']], ['other/a', 'other/b'])

    def test_archive_without_pending_is_republished_with_same_id(self):
        self.make_broker(); self.put('other/a'); self.scan()
        pending = self.control / 'stages/p4-r1/pending/0/0.json'
        pending.unlink()
        paths, next_id = self.run.recover_assignments('p4-r1', 1)
        self.assertEqual(paths, {'other/a'}); self.assertEqual(next_id, 1)
        self.assertEqual(migration_io.read_json(pending)['id'], 0)

    def test_missing_broker_claim_with_durable_receipt_fails_closed(self):
        self.make_broker(); self.put('other/a'); self.scan()
        migration_io.write_json(self.control / 'stages/p4-r1/done/0/0.json', {'id': 0, 'results': []})
        with self.assertRaisesRegex(RuntimeError, 'lease recovery'):
            self.run.recover_assignments('p4-r1', 1)

    def test_mismatched_broker_payload_is_not_reassigned(self):
        dbfile = self.make_broker(); self.put('other/a'); self.scan()
        with self.connect(dbfile) as db:
            db.execute('INSERT INTO claims VALUES (?,?,?,?)', ('p4-r1', 0, 'done', '{}'))
        with self.assertRaisesRegex(ValueError, 'payload differs'):
            self.run.recover_assignments('p4-r1', 1)

    def test_eai_hardlink_target_is_rejected_before_original_finalizer(self):
        manifest = self.control / 'manifests' / 'p4-r1-fixture.jsonl'
        manifest.parent.mkdir()
        manifest.write_text(json.dumps(dict(rel='other/a', kind='file', hardlink_to='eai_pwm/secret')) + '\n')
        with mock.patch.object(p4.Coordinator, 'finalize') as delegate:
            with self.assertRaises(ValueError):
                self.run.finalize(manifest)
            delegate.assert_not_called()

    def test_mount_root_directory_never_reaches_original_directory_metadata(self):
        manifest = self.control / 'manifests' / 'p4-r1-fixture.jsonl'
        manifest.parent.mkdir()
        manifest.write_text(json.dumps(dict(rel='', kind='directory')) + '\n')
        with mock.patch.object(p4.Coordinator, 'finalize', return_value={}) as original, \
                mock.patch.object(p4, 'finalize_directories', return_value={'directories': 0, 'errors': 0}):
            result = self.run.finalize(manifest)
            original.assert_called_once_with(manifest, include_directories=False)
            self.assertEqual(result['mount_root_metadata'], 'preserved_existing')

    def test_scanned_payload_uses_original_sha_copy_skip_and_conflict(self):
        self.make_broker(); self.put('other/a'); self.put('other/conflict', b'new')
        (self.destination / 'other').mkdir()
        (self.destination / 'other/conflict').write_bytes(b'old-unowned')
        self.scan()
        task = migration_io.read_json(self.run.archives('p4-r1')[0])
        cfg = dict(self.cfg, source=str(self.source), destination=str(self.destination))
        results = [process_entry(cfg, task, entry) for entry in task['entries']]
        by_path = {r['rel']: r for r in results}
        self.assertEqual(by_path['other/a']['status'], 'copied')
        self.assertEqual(by_path['other/conflict']['status'], 'conflict')
        self.assertEqual((self.destination / 'other/conflict').read_bytes(), b'old-unowned')
        a = next(e for e in task['entries'] if e['rel'] == 'other/a')
        self.assertEqual(process_entry(cfg, task, a)['status'], 'skipped')

    def test_two_round_pipeline_copies_once_then_verifies_without_touching_eai(self):
        self.make_broker()
        self.put('other/a'); self.put('eai_pwm/never', b'covered')
        real_scan = scanner.scan_phase
        copied = []
        def mapped(source, *args, **kwargs):
            return real_scan(self.source, *args, **kwargs)
        def consume(stage):
            for path in self.run.archives(stage):
                task = migration_io.read_json(path)
                cfg = dict(self.cfg, source=str(self.source), destination=str(self.destination))
                results = [process_entry(cfg, task, entry) for entry in task['entries']]
                copied.extend(r for r in results if r['status'] == 'copied')
                migration_io.write_json(self.control / 'stages' / stage / 'done' /
                                        str(task['id'] % 64) / path.name,
                                        dict(id=task['id'], results=results))
        with mock.patch.object(p4, 'scan_phase', side_effect=mapped), \
                mock.patch.object(self.run, 'drain', side_effect=consume), \
                mock.patch.object(p4, 'finalize_directories', return_value={'directories': 1, 'errors': 0}):
            self.run.run()
        rounds = self.run.state['phases']['4']['rounds']
        self.assertEqual([r['stage'] for r in rounds], ['p4-r1', 'p4-r2'])
        self.assertEqual(len(copied), 1)
        self.assertEqual(rounds[1]['summary'].get('verified'), 1)
        self.assertEqual(self.run.state['status'], 'complete')
        self.assertEqual(self.run.state['completion_scope'], 'priority4_only')
        self.assertTrue(self.run.state['predecessor_acceptance_still_required'])
        self.assertFalse((self.destination / 'eai_pwm').exists())

    def test_stop_does_not_signal_or_modify_other_control(self):
        (self.control / 'STOP').touch()
        with mock.patch('os.kill') as kill:
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                self.run.run()
            kill.assert_not_called()


if __name__ == '__main__':
    unittest.main()
