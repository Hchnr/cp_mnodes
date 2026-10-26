import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from recover_paused_producer import claim_delta, prefix_copy


class PausedRecoveryTests(unittest.TestCase):
    def database(self, rows):
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,status TEXT,payload TEXT)')
        for task_id, status, payload_id in rows:
            payload = {'id': payload_id, 'phase': 3, 'round': 3, 'op': 'sync',
                       'entries': [{'rel': 'other/'+str(task_id), 'kind': 'file'}]}
            db.execute('INSERT INTO claims VALUES (?,?,?,?)',
                       ('p3-r3', task_id, status, json.dumps(payload)))
        return db

    def test_complete_suffix_covers_every_id_and_keeps_payload(self):
        db = self.database([(0, 'done', 0), (3, 'done', 3), (4, 'done', 4)])
        records = list(claim_delta(db, 3, 5))
        self.assertEqual([json.loads(line)['id'] for line in records], [3, 4])
        self.assertTrue(all(line.endswith(b'\n') for line in records))

    def test_holes_unfinished_foreign_ids_and_mismatched_payloads_reject(self):
        cases = ([(3, 'done', 3), (5, 'done', 5)],
                 [(3, 'leased', 3), (4, 'done', 4)],
                 [(3, 'done', 3), (4, 'done', 4), (5, 'done', 5)],
                 [(3, 'done', 9), (4, 'done', 4)], [])
        for rows in cases:
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                list(claim_delta(self.database(rows), 3, 5))

    def test_original_prefix_is_copied_verbatim_and_sha_checked(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'ledger'
            original = b'{"id": 0, "entries": []}\n'
            path.write_bytes(original)
            out = io.BytesIO()
            digest, size = prefix_copy(path, out, hashlib.sha256(original).hexdigest(), len(original))
            self.assertEqual(out.getvalue(), original)
            self.assertEqual(digest.hexdigest(), hashlib.sha256(original).hexdigest())
            self.assertEqual(size, len(original))
            for expected_sha, expected_bytes in (('0'*64, len(original)),
                                                  (digest.hexdigest(), len(original)+1)):
                with self.assertRaises(RuntimeError):
                    prefix_copy(path, io.BytesIO(), expected_sha, expected_bytes)


if __name__ == '__main__':
    unittest.main()
