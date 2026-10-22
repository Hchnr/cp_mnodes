"""Same-scope P4 recovery fixtures; local temporary controls and SQLite only."""
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import unittest
from unittest import mock

import migration_priority4_resume as resume
import migration_priority4_index_merge as merge
import migration_priority4_worker as worker
from migration_io import read_json, write_json
from test_migration_priority4_worker import config
from test_migration_resume_index import identity, result, receipt


class Priority4ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir='/tmp', prefix='p4-resume-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.control = self.new_control('current')
        self.target = self.base / 'index.sqlite'
        # Test controls are under local /tmp. Retain all real config/scope
        # checks while mapping only their location to the production namespace.
        patcher = mock.patch.object(resume, 'ScopeGuard',
            lambda path, cfg: worker.ScopeGuard(worker.DESTINATION + '/.' + cfg['run_id'], cfg))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.guard_patcher = patcher

    def new_control(self, name, previous=None):
        path = self.base / ('.hcr-copy-p4-fixture-' + name)
        path.mkdir()
        cfg = config()
        cfg['run_id'] = path.name[1:]
        if previous is not None:
            cfg['resume_from'] = str(previous)
        write_json(path / 'config.json', cfg)
        return path

    def add(self, values, *, control=None, stage='p4-r1', day=1, name='0'):
        path = (control or self.control) / 'stages' / stage / 'done/0' / (name + '.json')
        receipt(path, values, day=day)
        return path

    def build(self, **kwargs):
        return resume.build_resume_index(self.control, self.target, read_workers=4, **kwargs)

    def rows(self):
        with sqlite3.connect(self.target) as db:
            return {row[0]: row for row in db.execute('SELECT * FROM files')}

    def assert_no_candidate(self):
        self.assertFalse(self.target.exists())
        self.assertFalse(any('p4-scope-' in p.name for p in self.base.iterdir()))

    def test_same_root_history_reuses_only_same_inode_and_sha_creation_proofs(self):
        old = self.new_control('old')
        cfg = read_json(self.control / 'config.json')
        cfg['resume_from'] = str(old)
        write_json(self.control / 'config.json', cfg)
        self.add([result('owned', ino=11), result('inode-changed', ino=21),
                  result('sha-changed', ino=31)], control=old)
        self.add([result('owned', status='verified', ino=11, stamp=2),
                  result('inode-changed', status='verified', ino=22),
                  result('sha-changed', status='verified', ino=31, digest='b' * 64),
                  result('replay', ino=41, replay_reused_copy=True)], stage='p4-r3', day=2)
        summary = self.build()
        rows = self.rows()
        self.assertEqual(rows['owned'][3], 1)
        for rel in ['inode-changed', 'sha-changed', 'replay']:
            self.assertEqual(rows[rel][3], 0)
        self.assertEqual(summary['scope_validated_controls'], 2)
        self.assertEqual(summary['scope'], 'project_priority4')
        self.assertEqual(summary['verified_files'], 4)
        self.assertEqual(summary['controls'], [str(old), str(self.control)])
        # The actual scoped builder's candidate can merge into a separately
        # initialized empty same-run P4 index, preserving a newer fresh row.
        guard = resume.ScopeGuard(str(self.control), read_json(self.control / 'config.json'))
        with sqlite3.connect(self.base / 'live.sqlite') as live, sqlite3.connect(self.target) as candidate:
            live.execute(merge.FILES_SQL)
            live.commit()
            merge.initialize_empty_live_index(live, guard)
            fresh = ('owned', rows['owned'][1], rows['owned'][2], 0, 'c' * 64)
            live.execute('INSERT INTO files VALUES (?,?,?,?,?)', fresh)
            live.commit()
            merged = merge.merge_candidate(candidate, live, guard, check_current=lambda: None, batch_size=2)
            self.assertEqual(live.execute('SELECT * FROM files WHERE rel=?', ('owned',)).fetchone(), fresh)
            self.assertEqual((merged['rows_inserted'], merged['rows_ignored']), (3, 1))

    def test_link_target_can_reference_eai_without_entering_it(self):
        self.add([result('alias', kind='symlink', sha256=None, target='eai_pwm',
                         source_identity=identity(200, mode=stat.S_IFLNK | 0o777),
                         dst_stat=identity(201, mode=stat.S_IFLNK | 0o777))])
        summary = self.build()
        self.assertEqual(summary['verified_symlinks'], 1)
        self.assertEqual(self.rows()['alias'][3], 1)

    def test_nested_eai_other_home_repos_and_hidden_paths_remain_in_scope(self):
        paths = ['other/eai_pwm/a', 'home/hcr/a', 'repos/cosmos-framework-pwm/a', '.hidden/a']
        self.add([result(path, ino=100 + i) for i, path in enumerate(paths)])
        self.build()
        self.assertEqual(set(self.rows()), set(paths))

    def test_reserved_or_eai_receipt_prevents_publication(self):
        for i, rel in enumerate(['eai_pwm/a', '.hcr-copy-monitor/a',
                                 '.hcr-copy-fixture-eai/a', self.control.name + '/a']):
            with self.subTest(rel=rel):
                self.add([result('ordinary'), result(rel)], name='reserved')
                with self.assertRaises(ValueError):
                    self.build()
                self.assert_no_candidate()

    def test_original_eai_ancestor_is_refused_before_builder_opens_database(self):
        old = self.new_control('wrong-root')
        cfg = read_json(old / 'config.json')
        cfg.update(source='/share/project/eai_pwm', destination='/share-mixed/project/eai_pwm')
        write_json(old / 'config.json', cfg)
        cfg = read_json(self.control / 'config.json')
        cfg['resume_from'] = str(old)
        write_json(self.control / 'config.json', cfg)
        with mock.patch.object(resume, 'parallel_build') as builder:
            with self.assertRaises(ValueError):
                self.build()
            builder.assert_not_called()
        self.assert_no_candidate()

    def test_foreign_phase_receipt_tree_is_refused_before_database(self):
        self.add([result('relative-eai-key')], stage='p3-r1')
        with mock.patch.object(resume, 'parallel_build') as builder:
            with self.assertRaisesRegex(ValueError, 'non-P4 stage'):
                self.build()
            builder.assert_not_called()
        self.assert_no_candidate()

    def test_missing_config_relative_resume_and_cycle_are_refused(self):
        cfg = read_json(self.control / 'config.json')
        cfg['resume_from'] = 'relative-ancestor'
        write_json(self.control / 'config.json', cfg)
        with self.assertRaisesRegex(ValueError, 'absolute'):
            self.build()
        cfg['resume_from'] = str(self.control)
        write_json(self.control / 'config.json', cfg)
        with self.assertRaisesRegex(ValueError, 'cycle'):
            self.build()
        missing = self.base / '.hcr-copy-p4-no-config'
        missing.mkdir()
        with self.assertRaises(FileNotFoundError):
            resume.build_resume_index(missing, self.target)
        self.assert_no_candidate()

    def test_stopped_ancestor_proof_can_be_read_without_resuming_its_processes(self):
        (self.control / 'STOP').touch()
        self.add([result('file')])
        self.build()
        self.assertTrue((self.control / 'STOP').exists())
        self.assertEqual(self.rows()['file'][3], 1)

    def test_existing_live_index_and_symlink_are_never_replaced(self):
        self.target.write_bytes(b'live-index-sentinel')
        with self.assertRaises(FileExistsError):
            self.build()
        self.assertEqual(self.target.read_bytes(), b'live-index-sentinel')
        self.target.unlink()
        other = self.base / 'other'
        other.write_bytes(b'other')
        self.target.symlink_to(other)
        with self.assertRaises(FileExistsError):
            self.build()
        self.assertEqual(other.read_bytes(), b'other')

    def test_nonlocal_target_and_symlink_parent_are_refused_without_mutation(self):
        with self.assertRaises(ValueError):
            resume.local_target('/share-mixed/project/nonlocal-index.sqlite')
        escape = self.base / 'escape'
        escape.symlink_to(Path.home(), target_is_directory=True)
        with self.assertRaises(ValueError):
            resume.local_target(escape / 'p4-resume-not-created.sqlite')
        self.assert_no_candidate()

    def test_callback_abort_removes_unpublished_candidate_and_preserves_receipts(self):
        path = self.add([result('file')])
        before = read_json(path)
        def abort(progress):
            raise RuntimeError('fixture abort')
        with self.assertRaisesRegex(RuntimeError, 'fixture abort'):
            self.build(progress_callback=abort)
        self.assert_no_candidate()
        self.assertEqual(read_json(path), before)

    def test_scope_change_during_rebuild_is_detected_before_publication(self):
        self.add([result('file')])
        original = read_json(self.control / 'config.json')
        for status in ('reading_receipts', 'publishing'):
            with self.subTest(status=status):
                write_json(self.control / 'config.json', original)
                changed = []
                def mutate(progress):
                    if progress['status'] == status and not changed:
                        cfg = read_json(self.control / 'config.json')
                        cfg['destination'] = '/home-mixed'
                        write_json(self.control / 'config.json', cfg)
                        changed.append(True)
                with self.assertRaises(ValueError):
                    self.build(progress_callback=mutate)
                self.assert_no_candidate()

    def test_callback_cannot_replace_a_target_created_during_build(self):
        self.add([result('file')])
        def competing_publish(progress):
            if progress['status'] == 'publishing':
                self.target.write_bytes(b'competing-index')
        with self.assertRaises(FileExistsError):
            self.build(progress_callback=competing_publish)
        self.assertEqual(self.target.read_bytes(), b'competing-index')
        self.assertFalse(any('p4-scope-' in p.name for p in self.base.iterdir()))

    def test_bad_config_and_nonproduction_control_are_not_allowed_by_real_guard(self):
        self.guard_patcher.stop()
        with self.assertRaises(ValueError):
            self.build()
        self.assert_no_candidate()


if __name__ == '__main__':
    unittest.main()
