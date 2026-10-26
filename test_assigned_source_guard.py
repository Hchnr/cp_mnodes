import hashlib
import io
import json
from pathlib import Path
import stat
import tempfile
import unittest

from migration_assigned_source_guard import AssignedSourceGuard, sample_manifest
from prepare_assigned_source_guard import build


def file(rel='data/a', ino=10, **values):
    return dict(rel=rel, ino=ino, kind='file', nlink=1, mode=stat.S_IFREG | 0o644, **values)


class AssignedSourceGuardTests(unittest.TestCase):
    def test_match_requires_exact_path_current_inode_and_regular_type(self):
        guard = AssignedSourceGuard()
        guard.add(file())
        self.assertTrue(guard.matches('data/a', 10, True))
        for args in (('data/b', 10, True), ('data/a', 11, True), ('data/a', 10, False),
                     ('data/a', True, True), ('data/a', 10, 1)):
            self.assertFalse(guard.matches(*args))

    def test_multilink_unknown_metadata_and_inode_aliases_never_qualify(self):
        for changed in ({'nlink': 2}, {'nlink': None}, {'nlink': True}, {'kind': 'symlink'},
                        {'mode': stat.S_IFLNK | 0o777}):
            guard = AssignedSourceGuard()
            entry = file();entry.update(changed);guard.add(entry)
            guard.add(file())
            self.assertFalse(guard.matches('data/a', 10, True))
        guard = AssignedSourceGuard();guard.add(file());guard.add(file('other/b'))
        guard.add(file())
        self.assertFalse(guard.matches('data/a', 10, True))

    def test_digest_task_scope_and_gap_are_bound_before_a_cache_can_be_used(self):
        row = dict(id=0, phase=3, round=3, entries=[file()])
        raw = (json.dumps(row) + '\n').encode()
        meta = dict(prefix_tasks=1, prefix_sha256=hashlib.sha256(raw).hexdigest())
        guard, counts = build(io.BytesIO(raw), 1, meta)
        self.assertEqual(counts['entries'], 1)
        self.assertTrue(guard.matches('data/a', 10, True))
        for bad in (dict(row, id=1), dict(row, phase=1), dict(row, round=2)):
            payload = (json.dumps(bad) + '\n').encode()
            with self.assertRaises(ValueError):
                build(io.BytesIO(payload), 1, dict(meta, prefix_sha256=hashlib.sha256(payload).hexdigest()))
        with self.assertRaises(ValueError):build(io.BytesIO(raw), 1, dict(meta, prefix_sha256='wrong'))
        with self.assertRaises(ValueError):build(io.BytesIO(raw[:-1]), 1, meta)

    def test_tail_audit_is_bounded_and_never_treats_an_incomplete_line_as_evidence(self):
        guard = AssignedSourceGuard();guard.add(file())
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'manifest'
            p.write_text(json.dumps(file()) + '\n' + '{"rel":"unfinished')
            result = sample_manifest(p, guard)
        self.assertEqual(result['assigned_single_inode_matches'], 1)
        self.assertFalse(result['metadata_inventory_proof'])


if __name__ == '__main__':
    unittest.main()
