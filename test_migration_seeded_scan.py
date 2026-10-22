import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from migration_parallel_scan import _seeded_nondirectory, scan_phase
from migration_scan import _stat_fields


class SeededScanTests(unittest.TestCase):
    def test_only_known_nondirectories_are_skipped(self):
        child = mock.Mock()
        child.is_dir.return_value = False
        self.assertFalse(_seeded_nondirectory(child, 'new', {'old'}))
        child.is_dir.assert_not_called()
        self.assertTrue(_seeded_nondirectory(child, 'old', {'old'}))
        child.is_dir.assert_called_once_with(follow_symlinks=False)
        child.is_dir.return_value = True
        self.assertFalse(_seeded_nondirectory(child, 'old', {'old'}))
        child.is_dir.side_effect = OSError('changed')
        self.assertFalse(_seeded_nondirectory(child, 'old', {'old'}))

    def test_seeded_file_now_directory_is_still_walked(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            source = base / 'source'
            source.mkdir()
            old = source / 'old'
            old.write_bytes(b'old')
            entry = {'rel': 'old', 'kind': 'file', **_stat_fields(old.stat())}
            old.unlink()
            old.mkdir()
            (old / 'fresh').write_bytes(b'new')
            emitted = []
            stats = scan_phase(source, 3, emitted.extend, base / 'manifest.jsonl',
                               workers=2, batch_files=1, resume_records=[entry],
                               skip_seeded_nondirectories=True)
            self.assertEqual([e['rel'] for e in emitted], ['old/fresh'])
            self.assertEqual(stats['files'], 2)
            records = [json.loads(s) for s in (base / 'manifest.jsonl').read_text().splitlines()]
            self.assertEqual(sum(e['rel'] == 'old' for e in records), 1)

    def test_seeded_leaf_not_emitted_again_and_new_leaf_is_emitted(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            source = base / 'source'
            source.mkdir()
            (source / 'old').write_bytes(b'old')
            (source / 'new').write_bytes(b'new')
            entry = {'rel': 'old', 'kind': 'file', **_stat_fields((source / 'old').stat())}
            emitted = []
            stats = scan_phase(source, 3, emitted.extend, base / 'manifest.jsonl',
                               workers=2, batch_files=1, resume_records=[entry],
                               skip_seeded_nondirectories=True)
            self.assertEqual([e['rel'] for e in emitted], ['new'])
            self.assertEqual(stats['files'], 2)


if __name__ == '__main__':
    unittest.main()
