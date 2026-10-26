"""Owned local SQLite fixtures for P4 merge isolation, interleavings and STOP."""
import copy
import json
from pathlib import Path
import sqlite3
import stat
import tempfile
import unittest

import migration_priority4_index_merge as merge
from migration_priority4_worker import ScopeGuard


def fixture_guard(name='current'):
    run = 'hcr-copy-p4-fixture-' + name
    return ScopeGuard('/share-mixed/project/.' + run, {
        'run_id': run, 'source': '/share/project', 'destination': '/share-mixed/project',
        'migration_scope': 'project_priority4', 'exclude': ['eai_pwm'],
        'covered_by_controls': ['/share-mixed/project/.hcr-copy-fixture-eai'],
        'protected_destination_relatives': ['.hcr-copy-fixture-reserved'],
        'threads': 64, 'copy_threads_per_node': 128, 'supplement_threads': 64,
        'io_bytes_per_second': 10_000_000_000, 'supplement_io_bytes_per_second': 10_000_000_000})


def row(rel, *, owned=1, ino=10, digest='a' * 64, mode=stat.S_IFREG | 0o640):
    ident = {'dev': 1, 'ino': ino, 'size': 7, 'mode': mode, 'uid': 123,
             'gid': 456, 'mtime_ns': 1, 'ctime_ns': 1}
    return (rel, json.dumps({**ident, 'ino': ino + 1000}), json.dumps(ident), owned, digest)


class Priority4IndexMergeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir='/tmp', prefix='hcr-p4-merge-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.guard = fixture_guard()
        self.source = self.db('candidate')
        self.live = self.db('live')
        merge.initialize_empty_live_index(self.live, self.guard)

    def db(self, name):
        db = sqlite3.connect(self.base / (name + '.sqlite'))
        self.addCleanup(db.close)
        db.execute(merge.FILES_SQL)
        db.commit()
        return db

    def candidate(self, rows, *, issues=0, warnings=0):
        self.source.executemany('INSERT INTO files VALUES (?,?,?,?,?)', rows)
        self.source.commit()
        merge.seal_receipt_candidate(self.source, self.guard, issues=issues, metadata_warnings=warnings)

    def rows(self, db=None):
        return (db or self.live).execute('SELECT * FROM files ORDER BY rel').fetchall()

    def perform(self, **kwargs):
        return merge.merge_candidate(self.source, self.live, self.guard,
                                     check_current=kwargs.pop('check_current', lambda: None), **kwargs)

    def test_ownership_upgrade_and_older_content_never_replace_fresh_rows(self):
        fresh = [row('same', owned=0), row('changed', owned=0, ino=20, digest='b' * 64)]
        self.live.executemany('INSERT INTO files VALUES (?,?,?,?,?)', fresh)
        self.live.commit()
        self.candidate([row('same', owned=1), row('changed', owned=1), row('new')])
        source_before = self.rows(self.source)
        result = self.perform(batch_size=1)
        rows = {r[0]: r for r in self.rows()}
        self.assertEqual([rows[r[0]] for r in fresh], fresh)
        self.assertEqual(rows['new'], row('new'))
        self.assertEqual(self.rows(self.source), source_before)
        self.assertEqual((result['rows_inserted'], result['rows_ignored'], result['existing_rows_updated']), (1, 2, 0))
        self.assertFalse(result['launch_ready'])

    def test_idempotent_retry_keeps_identical_rows_and_ownership(self):
        self.candidate([row('a'), row('b')])
        first = self.perform(batch_size=1)
        before = self.rows()
        second = self.perform(batch_size=1)
        self.assertEqual(first['rows_inserted'], 2)
        self.assertEqual((second['rows_inserted'], second['rows_ignored']), (0, 2))
        self.assertEqual(self.rows(), before)

    def test_fresh_writer_can_commit_between_batches_and_wins_same_path(self):
        self.candidate([row('a'), row('z')])
        writer = sqlite3.connect(self.base / 'live.sqlite', timeout=0.1)
        self.addCleanup(writer.close)
        fresh = row('z', owned=0, ino=77, digest='c' * 64)
        def progress(report):
            if report['committed_batches'] == 1:
                writer.execute('INSERT INTO files VALUES (?,?,?,?,?)', fresh)
                writer.commit()
        result = self.perform(batch_size=1, progress=progress)
        self.assertEqual(self.rows(), [row('a'), fresh])
        self.assertEqual((result['rows_inserted'], result['rows_ignored']), (1, 1))

    def test_stop_before_first_write_leaves_both_databases_untouched(self):
        self.candidate([row('a')])
        before = self.rows(self.source)
        def stop():
            raise RuntimeError('STOP')
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            self.perform(check_current=stop)
        with self.assertRaisesRegex(ValueError, 'checker must raise'):
            self.perform(check_current=lambda: False)
        self.assertEqual(self.rows(), [])
        self.assertEqual(self.rows(self.source), before)
        self.assertFalse(self.live.in_transaction)
        self.assertFalse(self.source.in_transaction)

    def test_stop_after_insert_before_commit_rolls_back_current_batch(self):
        self.candidate([row('a')])
        observed_uncommitted = []
        def stop_on_row():
            if self.live.in_transaction and self.rows():
                observed_uncommitted.append(True)
                raise RuntimeError('STOP before commit')
        with self.assertRaisesRegex(RuntimeError, 'STOP before commit'):
            self.perform(check_current=stop_on_row)
        self.assertEqual(observed_uncommitted, [True])
        self.assertEqual(self.rows(), [])
        self.assertFalse(self.live.in_transaction)

    def test_interrupted_merge_retains_committed_batches_and_can_resume(self):
        self.candidate([row('a'), row('b')])
        def interrupted(report):
            raise RuntimeError('observation interrupted')
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            self.perform(batch_size=1, progress=interrupted)
        self.assertEqual(self.rows(), [row('a')])
        resumed = self.perform(batch_size=1)
        self.assertEqual(self.rows(), [row('a'), row('b')])
        self.assertEqual((resumed['rows_inserted'], resumed['rows_ignored']), (1, 1))

    def test_unsealed_or_other_run_index_is_refused_without_adoption(self):
        self.source.execute('INSERT INTO files VALUES (?,?,?,?,?)', row('a'))
        self.source.commit()
        with self.assertRaises(ValueError):
            self.perform()
        merge.seal_receipt_candidate(self.source, fixture_guard('other'), issues=0, metadata_warnings=0)
        with self.assertRaisesRegex(ValueError, 'scope/provenance'):
            self.perform()
        self.assertEqual(self.rows(), [])

    def test_populated_existing_index_cannot_be_stamped_or_rebuilt(self):
        db = self.db('unbound')
        sentinel = row('fresh', owned=0)
        db.execute('INSERT INTO files VALUES (?,?,?,?,?)', sentinel)
        db.commit()
        with self.assertRaisesRegex(ValueError, 'populated'):
            merge.initialize_empty_live_index(db, self.guard)
        self.assertEqual(self.rows(db), [sentinel])
        self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [('files',)])

    def test_scope_marker_cannot_be_overwritten_and_issue_candidates_are_refused(self):
        self.candidate([row('a')], issues=1)
        with self.assertRaisesRegex(ValueError, 'candidate issue'):
            self.perform()
        with self.assertRaises(ValueError):
            merge.initialize_empty_live_index(self.live, fixture_guard('other'))
        self.assertEqual(self.rows(), [])

    def test_metadata_warning_candidate_is_not_silently_accepted(self):
        self.candidate([row('a')], warnings=1)
        with self.assertRaisesRegex(ValueError, 'candidate issue'):
            self.perform()
        self.assertEqual(self.rows(), [])

    def test_protected_path_refuses_entire_batch_but_other_home_and_nested_eai_are_legal(self):
        allowed = ['.hidden/a', 'home/hcr/a', 'repos/cosmos-framework-pwm/a', 'other/eai_pwm/a']
        self.candidate([*(row(r) for r in allowed), row('eai_pwm/forbidden')])
        with self.assertRaisesRegex(ValueError, 'covered or reserved'):
            self.perform(batch_size=100)
        self.assertEqual(self.rows(), [])
        # A candidate fixed by a future validated builder must be independently
        # sealed; this fixture only demonstrates that valid relatives pass.
        for rel in allowed:
            merge._row(row(rel), self.guard)

    def test_malformed_checksum_identity_type_and_ownership_are_refused(self):
        bad = [row('a', digest=None), row('a', owned=2), row('a', mode=stat.S_IFDIR | 0o700)]
        broken = list(row('a')); ident = json.loads(broken[2]); ident['ino'] = True
        broken[2] = json.dumps(ident); bad.append(tuple(broken))
        for value in bad:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    merge._row(value, self.guard)
        self.assertEqual(self.rows(), [])

    def test_hidden_main_or_temp_trigger_cannot_upgrade_existing_ownership(self):
        self.candidate([row('a')])
        sentinel = row('fresh', owned=0)
        self.live.execute('INSERT INTO files VALUES (?,?,?,?,?)', sentinel)
        self.live.commit()
        for temp, name in [('', 'sqlitex_hidden'), ('TEMP ', 'hidden_temp')]:
            self.live.execute('CREATE ' + temp + 'TRIGGER ' + name + ' AFTER INSERT ON main.files '
                              'BEGIN UPDATE files SET owned=1; END')
            self.live.commit()
            with self.assertRaises(ValueError):
                self.perform()
            self.assertEqual(self.rows(), [sentinel])
            self.live.execute('DROP TRIGGER ' + name)
            self.live.commit()
        # A writable_schema rename can hide the schema row behind SQLite's
        # reserved prefix without invalidating its already compiled trigger.
        # Exercise both enabled and disabled writable_schema on separate
        # connections, since a forged schema must never be reused afterward.
        for writable in (True, False):
            with self.subTest(reserved_prefix=True, writable_schema=writable):
                db = self.db('forged-' + str(writable))
                merge.initialize_empty_live_index(db, self.guard)
                db.execute('INSERT INTO files VALUES (?,?,?,?,?)', sentinel)
                db.commit()
                db.execute('CREATE TRIGGER cached_hidden AFTER INSERT ON files '
                           'BEGIN UPDATE files SET owned=1; END')
                db.commit()
                db.execute('PRAGMA writable_schema=ON')
                db.execute("UPDATE sqlite_master SET name='sqlite_hidden' "
                           "WHERE type='trigger' AND name='cached_hidden'")
                db.commit()
                if not writable:
                    db.execute('PRAGMA writable_schema=OFF')
                with self.assertRaises(ValueError):
                    merge.merge_candidate(self.source, db, self.guard, check_current=lambda: None)
                self.assertEqual(self.rows(db), [sentinel])
                self.assertFalse(db.in_transaction)

    def test_schema_constraint_or_attached_database_cannot_change_insert_semantics(self):
        self.candidate([row('a')])
        self.live.execute("ATTACH ':memory:' AS unrelated")
        with self.assertRaisesRegex(ValueError, 'attached'):
            self.perform()
        self.live.execute('DETACH unrelated')
        db = sqlite3.connect(':memory:'); self.addCleanup(db.close)
        db.execute(merge.FILES_SQL[:-1] + ', CHECK(owned=0))')
        with self.assertRaisesRegex(ValueError, 'constraints'):
            merge.initialize_empty_live_index(db, self.guard)
        self.assertEqual(self.rows(), [])

    def test_caller_transaction_and_same_database_are_refused_without_rollback(self):
        self.candidate([row('a')])
        self.live.execute('INSERT INTO files VALUES (?,?,?,?,?)', row('caller'))
        with self.assertRaisesRegex(ValueError, 'caller transaction'):
            self.perform()
        self.assertTrue(self.live.in_transaction)
        self.assertEqual(self.rows(), [row('caller')])
        self.live.rollback()
        second = sqlite3.connect(self.base / 'live.sqlite'); self.addCleanup(second.close)
        with self.assertRaisesRegex(ValueError, 'path must differ'):
            merge.merge_candidate(second, self.live, self.guard, check_current=lambda: None)

    def test_symlink_rows_keep_proof_flags_and_binding_change_prevents_next_batch(self):
        linked = row('alias', digest=None, mode=stat.S_IFLNK | 0o777)
        self.candidate([linked, row('z')])
        original = copy.deepcopy(self.guard.protected)
        def changed(report):
            self.guard.protected = self.guard.protected | {'new-reserved'}
        with self.assertRaisesRegex(ValueError, 'scope/provenance'):
            self.perform(batch_size=1, progress=changed)
        self.assertEqual(self.rows(), [linked])
        self.guard.protected = original
        resumed = self.perform(batch_size=1)
        self.assertEqual(resumed['rows_inserted'], 1)
        self.assertEqual(self.rows(), [linked, row('z')])


if __name__ == '__main__':
    unittest.main()
