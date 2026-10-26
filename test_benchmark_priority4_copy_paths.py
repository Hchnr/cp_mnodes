import hashlib
import os
from pathlib import Path
import tempfile
import unittest

import benchmark_priority4_copy_paths as bench
import migration_linux_open as helper
import migration_worker as engine


class FullCopyPathBenchmarkTests(unittest.TestCase):
    def test_every_strategy_keeps_bytes_metadata_and_source_identity(self):
        with tempfile.TemporaryDirectory(prefix='p4-copy-path-benchmark-fixture-') as name:
            root = Path(name)
            source = root / 'source'
            (source / 'a/b/c').mkdir(parents=True)
            path = source / 'a/b/c/f'
            content = b'bounded-copy-benchmark\n' * 100
            path.write_bytes(content)
            path.chmod(0o640)
            entry = dict(rel='a/b/c/f', kind='file', **engine._record(path.stat()))
            hashes = {entry['rel']: hashlib.sha256(content).hexdigest()}
            before = engine._identity(path.stat())
            engine.configure_cancel_event(None)
            engine.configure_rate_limit(0)
            for index, mode in enumerate(('legacy', 'native_readonly', 'destination_opath')):
                target = root / ('target-' + str(index))
                target.mkdir()
                value = bench.run_trial(engine, helper, str(source), target, [entry], hashes,
                                        mode, 'fixture', lambda: None, threads=2)
                self.assertEqual((value['files'], value['bytes']), (1, len(content)))
                self.assertEqual((target / entry['rel']).read_bytes(), content)
                self.assertEqual(engine._identity(path.stat()), before)

    def test_changed_source_and_nonempty_target_fail_before_copy(self):
        with tempfile.TemporaryDirectory(prefix='p4-copy-path-benchmark-fixture-') as name:
            root = Path(name)
            source = root / 'source'
            source.mkdir()
            path = source / 'f'
            path.write_bytes(b'original')
            entry = dict(rel='f', kind='file', **engine._record(path.stat()))
            path.write_bytes(b'changed')
            with self.assertRaises(ValueError):
                bench.stable_source(engine, str(source), entry)
            target = root / 'target'
            target.mkdir()
            (target / 'user-data').write_bytes(b'preserve')
            with self.assertRaises(ValueError):
                bench.run_trial(engine, helper, str(source), target, [entry], {},
                                'legacy', 'fixture', lambda: None)
            self.assertEqual((target / 'user-data').read_bytes(), b'preserve')

    def test_opath_is_destination_parent_only_and_never_root_descriptor(self):
        class Helper:
            open_parent_path = staticmethod(lambda *args: ('path', args))
            open_parent_readonly = staticmethod(lambda *args: ('readonly', args))
        fn = bench.directory_opener('destination_opath', '/destination', Helper)
        self.assertEqual(fn('/destination', 'a/f')[0], 'path')
        self.assertEqual(fn('/source', 'a/f')[0], 'readonly')
        self.assertEqual(fn('/destination', '__directory_open_root__')[0], 'readonly')
        with self.assertRaises(ValueError):
            bench.directory_opener('unsafe', '/destination', Helper)


if __name__ == '__main__':
    unittest.main()
