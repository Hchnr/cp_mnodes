"""Private fixtures: prepared prefixes never authorize a production cutover."""
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

import migration_warm_scan_checkpoint as checkpoint
from migration_io import write_json
from migration_parallel_scan import scan_phase
from migration_scan import _stat_fields
from migration_warm_scan_checkpoint import Prefix, read_prefix, directory_identity


def directory(rel, ino=1):
    return {'kind': 'directory', 'rel': rel, 'mode': stat.S_IFDIR | 0o755,
            'dev': 1, 'ino': ino, 'mtime_ns': 10, 'ctime_ns': 20}


def leaf(rel):
    return {'kind': 'file', 'rel': rel, 'mode': stat.S_IFREG | 0o644,
            'dev': 1, 'ino': 99, 'size': 5, 'nlink': 1}


class PrefixTests(unittest.TestCase):
    def test_prefix_alone_cannot_be_used_as_a_directory_completion_proof(self):
        prefix = Prefix()
        prefix.add(directory(''))
        prefix.add(directory('done'))
        prefix.add(leaf('done/a'))
        with self.assertRaisesRegex(ValueError, 'prefix alone'):
            prefix.pruning_candidates(set())

    def test_keep_unentered_empty_active_error_directories_and_all_their_ancestors(self):
        prefix = Prefix()
        entries = [directory(''), directory('finished'), leaf('finished/a'),
                   directory('parent'), directory('parent/empty'),
                   directory('parent/active'), leaf('parent/active/already_seen'),
                   directory('parent/finished'), leaf('parent/finished/a'),
                   directory('failed'), leaf('failed/a'),
                   {'kind': 'error', 'rel': 'failed/b', 'operation': 'stat', 'error': 'fixture'}]
        for entry in entries:
            prefix.add(entry)
        result = prefix.pruning_candidates({'parent/active'}, quiescent_and_flushed=True)
        self.assertEqual(set(result), {'finished', 'parent/finished'})

    def test_tail_keeps_all_possible_unoffered_entries_but_not_secondary_hardlinks(self):
        prefix = Prefix()
        prefix.add(directory(''))
        for i in range(700):
            prefix.add(leaf('f' + str(i)))
        prefix.add(dict(leaf('secondary'), hardlink_to='f0', nlink=2))
        self.assertEqual(len(prefix.tail), 512)
        self.assertEqual(prefix.tail[0]['rel'], 'f188')
        self.assertEqual(prefix.tail[-1]['rel'], 'f699')
        self.assertEqual(prefix.scanner.stats['files'], 701)

    def test_complete_prefix_boundary_is_exact_and_append_can_be_caught_up(self):
        prefix = Prefix()
        first = json.dumps(directory('')).encode() + b'\n'
        second = json.dumps(leaf('a')).encode() + b'\n'
        stream = io.BytesIO(first + second)
        checksum = hashlib.sha256()
        offset = read_prefix(stream, len(first) + len(second) - 1, prefix, checksum)
        self.assertEqual(offset, len(first))
        self.assertEqual(stream.tell(), len(first))
        self.assertEqual(checksum.hexdigest(), hashlib.sha256(first).hexdigest())
        offset = read_prefix(stream, len(first + second), prefix, checksum)
        self.assertEqual(offset, len(first + second))
        self.assertEqual(prefix.records, 2)
        self.assertEqual(checksum.hexdigest(), hashlib.sha256(first + second).hexdigest())

    def test_every_cut_of_a_directory_walk_keeps_every_unobserved_subtree(self):
        tree = {'': ['a', 'b', 'empty'], 'a': ['a/d', 'a/file'],
                'a/d': ['a/d/1', 'a/d/2'], 'b': ['b/1', 'b/2'], 'empty': []}
        all_files = {child for children in tree.values() for child in children if child not in tree}
        prefix = Prefix()
        prefix.add(directory(''))
        active, observed = set(), set()

        def check_cut():
            completed = prefix.pruning_candidates(active, quiescent_and_flushed=True)
            for rel in all_files - observed:
                self.assertFalse(any(rel.startswith(d + '/') for d in completed),
                                 (rel, completed, active))

        def walk(rel):
            active.add(rel)
            for child in tree[rel]:
                entry = directory(child) if child in tree else leaf(child)
                prefix.add(entry)
                if child not in tree:
                    observed.add(child)
                check_cut()
            active.remove(rel)
            check_cut()  # Owner may close the parent before pending children enter.
            for child in tree[rel]:
                if child in tree:
                    walk(child)

        check_cut()
        walk('')
        self.assertEqual(observed, all_files)
        self.assertEqual(set(prefix.pruning_candidates(set(), quiescent_and_flushed=True)),
                         {'a', 'a/d', 'b'})

    def test_malformed_complete_records_duplicates_and_unsafe_paths_fail_visibly(self):
        for data in (b'{bad}\n', b'[]\n'):
            with self.assertRaises(ValueError):
                read_prefix(io.BytesIO(data), len(data), Prefix(), hashlib.sha256())
        prefix = Prefix()
        prefix.add(directory(''))
        prefix.add(leaf('a'))
        with self.assertRaisesRegex(ValueError, 'repeats'):
            prefix.add(leaf('a'))
        with self.assertRaises(ValueError):
            prefix.add(leaf('../outside'))
        with self.assertRaises(ValueError):
            directory_identity(dict(directory('b'), ino=True))


class PreparationGuardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name) / '.checkpoint-fixture'
        self.control.mkdir()
        self.host = 'job-fixture-master-0'
        self.cfg = dict(control=str(self.control), run_id='checkpoint-fixture',
            job='fixture', master_host=self.host, cold_frontier_v1=True,
            source='/share/project/eai_pwm', destination='/share-mixed/project/eai_pwm')
        write_json(self.control/'config.json', self.cfg)
        self.state = dict(status='scanning', stage='p3-r3', recovery_warm=True,
            recovery_label='旧清单供给结束；补充当前源目录变化')
        write_json(self.control/'state.json', self.state)
        write_json(self.control/'pipeline.json', dict(stages={}))
        self.argv = ['/fixture/python3', '-u',
            str(self.control/'migration_cold_frontier.py'), 'run', '--control', str(self.control),
            '--warm-tasks', str(self.control/'warm-tasks.jsonl')]
        role = dict(pid=42, start_ticks='123', argv=self.argv)
        write_json(self.control/'recovery-roles'/(self.host+'-producer.json'), role)
        hashes = {}
        for name in ('migration_cold_frontier.py', 'migration_process_scan.py',
                     'migration_parallel_scan.py', 'migration_scan.py'):
            (self.control/name).write_bytes(name.encode())
            hashes[name] = hashlib.sha256(name.encode()).hexdigest()
        write_json(self.control/'source-sha256.json', hashes)
        self.expected = dict(scope=checkpoint.SCOPE, control=str(self.control),
            run_id=self.cfg['run_id'], job='fixture',
            producer=dict(pid=42, start_ticks='123', argv=self.argv),
            frozen_runtime_manifest_sha256=hashlib.sha256(
                (self.control/'source-sha256.json').read_bytes()).hexdigest())
        patch = mock.patch.object(checkpoint.socket, 'gethostname', return_value=self.host)
        patch.start(); self.addCleanup(patch.stop)
        patch = mock.patch.object(checkpoint, 'process_identity',
            return_value=dict(pid=42, start_ticks='123', argv=self.argv, state='S'))
        self.identity = patch.start(); self.addCleanup(patch.stop)

    def test_exact_current_runtime_can_be_checked_without_writing(self):
        before = {str(p): p.read_bytes() for p in self.control.rglob('*') if p.is_file()}
        cfg, _, actual = checkpoint.validate_runtime(self.control, self.expected)
        self.assertEqual(cfg, self.cfg)
        self.assertEqual(actual['pid'], 42)
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.control.rglob('*') if p.is_file()})

    def test_fast_successor_requires_exact_active_bundle_hash_and_scan_lineage(self):
        bundle=self.control/'fast-p3-scan-fixture';bundle.mkdir()
        entry=bundle/'migration_fast_cold_resume.py';entry.write_text('# frozen fixture\n')
        write_json(bundle/'source-sha256.json',{entry.name:hashlib.sha256(entry.read_bytes()).hexdigest()})
        argv=['/usr/bin/python3','-u',str(entry),'--control',str(self.control),'--directory',str(bundle)]
        state=dict(self.state,recovery_label='fixture fast scan',foreground_handover=str(bundle/'handover.json'),
                   source_scan_acceleration={'processes':32})
        write_json(self.control/'state.json',state)
        write_json(self.control/'recovery-roles'/(self.host+'-producer.json'),dict(pid=42,start_ticks='123',argv=argv))
        self.identity.return_value=dict(pid=42,start_ticks='123',argv=argv,state='S')
        expected=dict(self.expected,entry_kind='fast-p3-scan',source_bundle=str(bundle),source_reader_count=32,
                      producer=dict(pid=42,start_ticks='123',argv=argv),
                      frozen_active_manifest_sha256=hashlib.sha256((bundle/'source-sha256.json').read_bytes()).hexdigest())
        checkpoint.validate_runtime(self.control,expected)
        for change in ({'source_reader_count':64},{'source_bundle':str(self.control/'other')},
                       {'frozen_active_manifest_sha256':'0'*64}):
            with self.assertRaises(ValueError):checkpoint.validate_runtime(self.control,dict(expected,**change))
        entry.write_text('# mutated fixture\n')
        with self.assertRaises(ValueError):checkpoint.validate_runtime(self.control,expected)

    def test_pid_reuse_and_rehashed_mutated_runtime_do_not_match_original_intent(self):
        self.identity.return_value = dict(pid=42, start_ticks='124', argv=self.argv, state='S')
        with self.assertRaisesRegex(RuntimeError, 'identity differs'):
            checkpoint.validate_runtime(self.control, self.expected)
        self.identity.return_value['start_ticks'] = '123'
        path = self.control/'migration_scan.py'
        path.write_bytes(b'changed')
        hashes = json.loads((self.control/'source-sha256.json').read_bytes())
        hashes[path.name] = hashlib.sha256(b'changed').hexdigest()
        write_json(self.control/'source-sha256.json', hashes)
        # validate_runtime intentionally binds the static raw manifest as well
        # as its entries. Replacing it together with a source cannot repin it.
        (self.control/'source-sha256.json').write_text(json.dumps(hashes))
        with self.assertRaisesRegex(ValueError, 'preparation intent'):
            checkpoint.validate_runtime(self.control, self.expected)

    def test_no_preparation_after_the_source_scan_or_with_stop(self):
        write_json(self.control/'state.json', dict(self.state, scan_complete=True))
        with self.assertRaisesRegex(RuntimeError, 'no longer'):
            checkpoint.validate_runtime(self.control, self.expected)
        (self.control/'STOP').touch()
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            checkpoint.validate_runtime(self.control, self.expected)

    def test_memory_and_scratch_checks_use_finite_cgroup_headroom(self):
        gib = 1024**3
        def contents(path, *args, **kwargs):
            if path.name == 'memory.max':
                return str(100*gib)
            return 'anon '+str(95*gib)+'\n'
        with mock.patch.object(Path, 'exists', return_value=True), \
             mock.patch.object(Path, 'read_text', autospec=True, side_effect=contents):
            with self.assertRaisesRegex(RuntimeError, 'cgroup'):
                checkpoint.resource_budget(2*gib)
        with mock.patch.object(Path, 'exists', return_value=False), \
             mock.patch.object(checkpoint.shutil, 'disk_usage', return_value=mock.Mock(free=1)):
            with self.assertRaisesRegex(RuntimeError, 'scratch'):
                checkpoint.resource_budget(1)


class DirectoryPruningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        for rel in ('done/a', 'unfinished/old', 'unfinished/new'):
            p = self.source / rel
            p.parent.mkdir(exist_ok=True)
            p.write_bytes(b'payload')
        self.manifest = self.root / 'inventory.jsonl'
        self.prefix = Prefix()
        for rel, kind in (('', 'directory'), ('done', 'directory'), ('done/a', 'file'),
                          ('unfinished', 'directory'), ('unfinished/old', 'file')):
            self.prefix.add({'rel': rel, 'kind': kind, **_stat_fields((self.source / rel).stat())})
        self.manifest.write_text(''.join(json.dumps(e) + '\n' for e in self.prefix.tail))
        self.emitted = []

    def test_current_error_for_a_seeded_directory_cannot_be_suppressed(self):
        def fail_walk(*args,**kwargs):
            consume=args[5]
            consume(dict(rel='unfinished',kind='error',operation='open_directory',error='fixture stale inode'))
            return {}
        with mock.patch('migration_process_scan.walk',side_effect=fail_walk):
            stats=self.scan()
        self.assertEqual(stats['errors'],1)
        self.assertFalse(stats['scan_complete'])
        self.assertTrue(any(json.loads(line).get('kind')=='error' for line in self.manifest.read_text().splitlines()))

    def scan(self, **overrides):
        root = self.source.stat()
        kwargs = dict(processes=2, threads_per_process=2, process_batch_size=4,
                      batch_files=2, initial_seeded_paths=self.prefix.paths,
                      append_manifest=True, seed_stats=self.prefix.scanner.stats,
                      skip_seeded_nondirectories=True,
                      completed_directories=self.prefix.pruning_candidates(
                          {'unfinished'}, quiescent_and_flushed=True),
                      expected_root_identity=(root.st_dev, root.st_ino))
        kwargs.update(overrides)
        return scan_phase(self.source, 3, self.emitted.extend, self.manifest, **kwargs)

    def test_same_identity_completed_subtree_is_skipped_and_unfinished_is_continued(self):
        result = self.scan()
        self.assertEqual([e['rel'] for e in self.emitted], ['unfinished/new'])
        self.assertEqual(result['process_readers']['pruned_completed_directories'], 1)
        self.assertEqual(result['files'], 3)

    def test_changed_completed_directory_is_reopened_and_new_children_are_not_hidden(self):
        (self.source / 'done' / 'later').write_bytes(b'new')
        old = self.prefix.directories['done']
        os.utime(self.source / 'done', ns=(old[2] + 10000000, old[2] + 10000000))
        result = self.scan()
        self.assertEqual({e['rel'] for e in self.emitted}, {'done/later', 'unfinished/new'})
        self.assertEqual(result['process_readers']['pruned_completed_directories'], 0)

    def test_pruning_requires_append_seed_stats_processes_and_exact_root_identity(self):
        for kwargs in ({'processes': 0}, {'append_manifest': False},
                       {'seed_stats': None}, {'expected_root_identity': None},
                       {'completed_directories': {'../outside': (1, 2, 3, 4)}}):
            with self.assertRaises(ValueError):
                self.scan(**kwargs)
        root = self.source.stat()
        before = self.manifest.read_bytes()
        with self.assertRaises(OSError):
            self.scan(expected_root_identity=(root.st_dev, root.st_ino + 1))
        self.assertEqual(self.manifest.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
