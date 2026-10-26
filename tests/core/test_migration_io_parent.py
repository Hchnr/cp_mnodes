from pathlib import Path
import tempfile
import unittest
from unittest import mock

from migration_io import read_json, write_json


class PreparedParentPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_default_still_creates_missing_parents(self):
        target = self.root / 'not-yet' / 'nested' / 'queue.json'
        write_json(target, {'id': 1})
        self.assertEqual(read_json(target), {'id': 1})

    def test_prepared_parent_first_publication_uses_no_mkdir(self):
        target = self.root / 'queue.json'
        with mock.patch.object(Path, 'mkdir', side_effect=AssertionError('unexpected parent mkdir')):
            published = write_json(target, {'id': 1}, ensure_parent=False)
        self.assertEqual(published, target)
        self.assertEqual(read_json(target), {'id': 1})

    def test_missing_prepared_parent_raises_and_is_not_created(self):
        target = self.root / 'missing' / 'queue.json'
        with self.assertRaises(FileNotFoundError):
            write_json(target, {'id': 1}, ensure_parent=False)
        self.assertFalse(target.parent.exists())

    def test_rewrites_preserve_canonical_inode_and_latest_immutable_snapshot(self):
        target = self.root / 'queue.json'
        write_json(target, {'id': 1, 'version': 'initial'}, ensure_parent=False)
        first_inode = target.stat().st_ino
        first_bytes = target.read_bytes()
        published = write_json(target, {'id': 1, 'version': 'retry'}, ensure_parent=False)
        self.assertNotEqual(published, target)
        self.assertTrue(published.is_file())
        self.assertEqual(target.stat().st_ino, first_inode)
        self.assertEqual(target.read_bytes(), first_bytes)
        self.assertEqual(read_json(target), {'id': 1, 'version': 'retry'})
        newer = write_json(target, {'id': 1, 'version': 'second retry'}, ensure_parent=False)
        self.assertNotEqual(published, newer)
        self.assertTrue(published.exists())
        self.assertEqual(read_json(target), {'id': 1, 'version': 'second retry'})


if __name__ == '__main__':
    unittest.main()
