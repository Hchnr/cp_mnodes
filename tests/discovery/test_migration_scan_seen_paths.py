import gc
import json
import os
from pathlib import Path
import platform
import tempfile
import unittest
from unittest import mock

import migration_parallel_scan as scan


class SeenPathKeysTests(unittest.TestCase):
    def test_membership_and_duplicates_match_set_without_storing_file_evidence(self):
        keys = scan._new_seen_paths()
        expected = set()
        paths = ['', 'plain', 'deep/file', 'unicode-目录', 'line\nbreak', 'plain']
        for path in paths:
            self.assertEqual(path in keys, path in expected)
            keys[path] = None
            expected.add(path)
        self.assertEqual(set(keys), expected)
        self.assertTrue(all(value is None for value in keys.values()))
        if platform.python_implementation() == 'CPython':
            self.assertFalse(gc.is_tracked(keys))
            gc.collect(2)
            self.assertFalse(gc.is_tracked(keys))

    def test_real_scan_retains_manifest_task_metadata_and_hardlink_semantics(self):
        with tempfile.TemporaryDirectory(prefix='seen-path-keys-') as temporary:
            base = Path(temporary)
            source = base / 'source'
            source.mkdir()
            (source / 'nested').mkdir()
            (source / 'nested/original').write_bytes(b'fixture-data')
            os.link(source / 'nested/original', source / 'alias')
            os.symlink('nested/original', source / 'link')
            (source / 'unicode-目录').write_bytes(b'fixture')
            (source / 'line\nbreak').write_bytes(b'fixture')
            manifest = base / 'manifest.jsonl'
            keys = scan._new_seen_paths()
            emitted = []
            with mock.patch.object(scan, '_new_seen_paths', return_value=keys):
                stats = scan.scan_phase(source, 3, emitted.extend, manifest,
                    workers=2, batch_files=2)
            rows = [json.loads(line) for line in manifest.read_text().splitlines()]
            self.assertEqual(len(rows), len({entry['rel'] for entry in rows}))
            self.assertEqual(set(keys), {entry['rel'] for entry in rows})
            self.assertEqual(stats['files'], 4)
            self.assertEqual(stats['hardlinks'], 1)
            self.assertEqual(stats['symlinks'], 1)
            self.assertTrue(stats['scan_complete'])
            self.assertEqual(len(emitted), 4)
            for entry in emitted:
                self.assertIn('ino', entry)
                self.assertIn('mtime_ns', entry)
            if platform.python_implementation() == 'CPython':
                self.assertFalse(gc.is_tracked(keys))


if __name__ == '__main__':
    unittest.main()
