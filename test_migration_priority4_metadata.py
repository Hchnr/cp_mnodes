"""P4 directory guards, durable receipts and real local metadata fixtures."""
import json
import os
from pathlib import Path
import stat
import tempfile
import threading
import unittest
from unittest import mock

import migration_io
import migration_metadata as original
import migration_priority4_metadata as meta
import migration_priority4_worker as p4
import migration_worker as engine
from test_migration_priority4_worker import CONTROL, config


class Priority4DirectoryGuards(unittest.TestCase):
    def setUp(self):
        self.cfg = config()
        self.guard = p4.ScopeGuard(CONTROL, self.cfg)
        self.stop = threading.Event()

    def entry(self, rel):
        return {'kind': 'directory', 'rel': rel}

    def test_mount_root_never_reaches_mkdir_or_metadata_delegate(self):
        with mock.patch.object(meta, 'original_mkdir') as mkdir, \
             mock.patch.object(meta, 'original_metadata') as metadata:
            for rel in ['', '.']:
                for operation in [meta._mkdir, meta._metadata]:
                    self.assertEqual(operation(self.cfg, self.entry(rel), self.stop, self.guard)['status'],
                                     'preserved_mount_root')
            mkdir.assert_not_called()
            metadata.assert_not_called()

    def test_reserved_and_unsafe_paths_record_errors_without_delegate_io(self):
        with mock.patch.object(meta, 'original_mkdir') as mkdir, \
             mock.patch.object(meta, 'original_metadata') as metadata:
            for rel in ['eai_pwm', 'eai_pwm/a', '.hcr-copy-monitor',
                        '.hcr-copy-fixture-eai/a', '.hcr-copy-p4-fixture-123',
                        '../outside', '/absolute', 'a/../b']:
                for operation in [meta._mkdir, meta._metadata]:
                    result = operation(self.cfg, self.entry(rel), self.stop, self.guard)
                    self.assertEqual(result['status'], 'error')
                    self.assertTrue(result['scope_violation'])
            mkdir.assert_not_called()
            metadata.assert_not_called()

    def test_other_home_repos_and_nested_eai_paths_are_delegated(self):
        with mock.patch.object(meta, 'original_metadata', return_value={'status': 'applied'}) as delegate:
            for rel in ['home/hcr', 'repos/cosmos-framework-pwm', 'other/eai_pwm', '.hidden']:
                self.assertEqual(meta._metadata(self.cfg, self.entry(rel), self.stop, self.guard),
                                 {'status': 'applied'})
            self.assertEqual(delegate.call_count, 4)

    def test_existing_stop_skips_every_operation(self):
        self.stop.set()
        with mock.patch.object(meta, 'original_mkdir') as delegate:
            self.assertEqual(meta._mkdir(self.cfg, self.entry('ordinary'), self.stop, self.guard)['status'],
                             'stopped')
            delegate.assert_not_called()

    def test_non_directory_and_changed_scope_are_rejected(self):
        with mock.patch.object(meta, 'original_metadata') as delegate:
            entry = {'kind': 'file', 'rel': 'ordinary'}
            self.assertTrue(meta._metadata(self.cfg, entry, self.stop, self.guard)['scope_violation'])
            cfg = dict(self.cfg, source='/home')
            self.assertTrue(meta._metadata(cfg, self.entry('ordinary'), self.stop, self.guard)['scope_violation'])
            delegate.assert_not_called()

    def test_original_shared_ancestors_unchanged(self):
        self.assertEqual(original.SHARED_ANCESTORS, frozenset(('', 'home', 'repos')))
        self.assertEqual(meta.SHARED_ANCESTORS, frozenset(('',)))

    def test_manifest_and_result_namespace_reject_original_scope_and_control_files(self):
        control = Path(CONTROL)
        for manifest in [control / 'manifests/p1-r3.jsonl', control / 'manifests/p4-r4.jsonl',
                         control / 'manifests/p4-r1.json', control / 'other/p4-r1.jsonl',
                         Path('/share/project/p4-r1.jsonl')]:
            with self.assertRaises(ValueError):
                meta._manifest_path(control, manifest)
        for result in [control / 'state.json', control / 'config.json',
                       Path('/share-mixed/project/eai_pwm/result.json'), control / 'pipeline-results/file.py']:
            with self.assertRaises(ValueError):
                meta._result_path(control, 'p4-r3', result)
        self.assertEqual(meta._manifest_path(control, control / 'manifests/p4-r3-combined-hash.jsonl').stem,
                         'p4-r3-combined-hash')


class Priority4DirectoryIntegration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.control = self.base / 'control'
        self.control.mkdir()
        self.source = self.base / 'source'
        self.destination = self.base / 'destination'
        self.source.mkdir()
        self.destination.mkdir(mode=0o750)
        self.cfg = config()
        migration_io.write_json(self.control / 'config.json', self.cfg)
        self.manifest = self.control / 'manifests/p4-r3.jsonl'
        self.manifest.parent.mkdir()
        self.calls = []
        real_open_root = engine._open_root
        def mapped(root, create=False):
            root = str(root)
            for prefix, local in [(p4.SOURCE, self.source), (p4.DESTINATION, self.destination)]:
                if root == prefix or root.startswith(prefix + '/'):
                    return real_open_root(str(local) + root[len(prefix):], create=create)
            raise AssertionError('unexpected filesystem root: ' + root)
        def track_mkdir(cfg, entry, stop):
            self.calls.append(('mkdir', entry['rel']))
            return original._mkdir(cfg, entry, stop)
        def track_metadata(cfg, entry, stop):
            self.calls.append(('metadata', entry['rel']))
            return original._metadata(cfg, entry, stop)
        for patcher in [mock.patch.object(engine, '_open_root', mapped),
                        mock.patch.object(original, '_open_root', mapped),
                        mock.patch.object(meta, 'ScopeGuard', lambda _, cfg: p4.ScopeGuard(CONTROL, cfg)),
                        mock.patch.object(meta, 'original_mkdir', track_mkdir),
                        mock.patch.object(meta, 'original_metadata', track_metadata)]:
            patcher.start()
            self.addCleanup(patcher.stop)

    def mkdir(self, rel, mode=0o711):
        p = self.source / rel
        p.mkdir(parents=True, exist_ok=True)
        p.chmod(mode)
        return p

    def records(self, paths):
        return [dict(engine._record((self.source / rel).lstat()), rel=rel, kind='directory')
                for rel in paths]

    def manifest_records(self, records):
        self.manifest.write_text(''.join(json.dumps(r) + '\n' for r in records))

    def manifest_paths(self, paths):
        self.manifest_records(self.records(paths))

    def receipts(self, label):
        result = []
        for path in (self.control / 'metadata' / label / 'receipts').glob('*.json'):
            result.extend(migration_io.read_json(path)['results'])
        return result

    def test_actual_directory_metadata_depth_barriers_and_root_preservation(self):
        for rel in ['home/hcr', 'repos/cosmos-framework-pwm', 'other/eai_pwm', 'empty']:
            self.mkdir(rel)
        paths = ['', 'home', 'home/hcr', 'repos', 'repos/cosmos-framework-pwm',
                 'other', 'other/eai_pwm', 'empty']
        self.manifest_paths(paths)
        before = self.destination.stat()
        summary = meta.finalize_directories(self.control, self.manifest, threads=4)
        self.assertEqual(summary['status'], 'complete')
        self.assertEqual(summary['deferred'], 1)
        self.assertEqual(summary['directories'], 7)
        events = [kind for kind, _ in self.calls]
        self.assertLess(max(i for i, kind in enumerate(events) if kind == 'mkdir'),
                        min(i for i, kind in enumerate(events) if kind == 'metadata'))
        depths = [rel.count('/') + 1 for kind, rel in self.calls if kind == 'metadata']
        self.assertEqual(depths, sorted(depths, reverse=True))
        root_result = meta.finalize_shared_ancestors(self.control, [self.manifest], threads=4)
        self.assertEqual(root_result['preserved_mount_root_operations'], 2)
        self.assertEqual(root_result['directories'], 0)
        after = self.destination.stat()
        self.assertEqual((before.st_mode, before.st_uid, before.st_gid),
                         (after.st_mode, after.st_uid, after.st_gid))
        self.assertNotIn('', [rel for _, rel in self.calls])
        for rel in paths[1:]:
            self.assertEqual(stat.S_IMODE((self.source / rel).stat().st_mode),
                             stat.S_IMODE((self.destination / rel).stat().st_mode))
        self.assertTrue((self.destination / 'empty').is_dir())
        self.assertEqual(len(self.receipts('shared-ancestors')), 2)

    def test_protected_destination_has_durable_issue_and_is_not_touched(self):
        self.mkdir('ordinary')
        self.mkdir('eai_pwm')
        (self.destination / 'eai_pwm').mkdir(mode=0o750)
        before = (self.destination / 'eai_pwm').stat()
        self.manifest_paths(['ordinary', 'eai_pwm'])
        summary = meta.finalize_directories(self.control, self.manifest, threads=2)
        self.assertEqual(summary['errors'], 1)
        self.assertTrue(any(r.get('scope_violation') for r in self.receipts('p4-r3')))
        after = (self.destination / 'eai_pwm').stat()
        self.assertEqual((before.st_mode, before.st_mtime_ns, before.st_ctime_ns),
                         (after.st_mode, after.st_mtime_ns, after.st_ctime_ns))
        self.assertNotIn('eai_pwm', [rel for _, rel in self.calls])
        self.assertEqual(migration_io.read_json(self.control / 'metadata/p4-r3/result.json')['errors'], 1)

    def test_existing_stop_creates_no_destination_directories(self):
        self.mkdir('ordinary')
        self.manifest_paths(['ordinary'])
        (self.control / 'STOP').touch()
        summary = meta.finalize_directories(self.control, self.manifest)
        self.assertEqual(summary['status'], 'stopped')
        self.assertEqual(self.calls, [])
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_changed_source_inode_is_reported_without_creating_destination(self):
        self.mkdir('changed')
        records = self.records(['changed'])
        (self.source / 'changed').rename(self.source / 'old-inode')
        self.mkdir('changed')
        self.manifest_records(records)
        summary = meta.finalize_directories(self.control, self.manifest)
        self.assertEqual(summary['source_changed'], 1)
        self.assertFalse((self.destination / 'changed').exists())

    def test_destination_symlink_parent_cannot_modify_covered_tree(self):
        self.mkdir('alias/child')
        (self.destination / 'eai_pwm').mkdir()
        os.symlink('eai_pwm', self.destination / 'alias')
        self.manifest_paths(['alias/child'])
        summary = meta.finalize_directories(self.control, self.manifest)
        self.assertEqual(summary['errors'], 1)
        self.assertFalse((self.destination / 'eai_pwm/child').exists())

    def test_unsafe_manifest_fails_before_any_directory_operation(self):
        self.manifest_records([{'rel': '../escape', 'kind': 'directory'}])
        with self.assertRaises(ValueError):
            meta.finalize_directories(self.control, self.manifest)
        self.assertEqual(self.calls, [])
        result = migration_io.read_json(self.control / 'metadata/p4-r3/result.json')
        self.assertEqual(result['status'], 'failed')

    def test_non_root_deferred_metadata_cannot_be_adopted(self):
        self.manifest_paths([''])
        migration_io.write_json(self.control / 'metadata/p4-r3/result.json',
                                {'deferred_entries': [{'rel': 'home', 'kind': 'directory'}]})
        with self.assertRaisesRegex(RuntimeError, 'non-mount-root'):
            meta.finalize_shared_ancestors(self.control, [self.manifest])
        self.assertEqual(self.calls, [])

    def test_duplicate_finalizer_cannot_acquire_local_lock(self):
        self.manifest_paths([''])
        with meta._exclusive(self.control):
            with self.assertRaises(BlockingIOError):
                meta.finalize_directories(self.control, self.manifest)
        self.assertFalse((self.control / 'metadata').exists())


if __name__ == '__main__':
    unittest.main()
