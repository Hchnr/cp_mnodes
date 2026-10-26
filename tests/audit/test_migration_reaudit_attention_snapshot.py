import gzip
import json
from pathlib import Path
import tempfile
import unittest

from migration_reaudit_attention_snapshot import changed_lines,match_tasks


class SnapshotTests(unittest.TestCase):
    def test_only_closed_changed_receipts_and_exclusions(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'receipts.jsonl'
            rows=[dict(id=1,status='copied'),dict(id=2,status='changed'),dict(id=3,status='changed')]
            path.write_bytes(b''.join((json.dumps(r)+'\n').encode() for r in rows)+b'{"id":4,"status":"changed"')
            events=changed_lines(path,{2},64)
            self.assertEqual([e['receipt']['id'] for e in events],[3])
            with path.open('rb') as stream:
                stream.seek(events[0]['byte_offset']);self.assertEqual(json.loads(stream.readline()),rows[2])
            self.assertEqual(changed_lines(path,set(),1)[0]['receipt']['id'],2)

    def test_exact_id_path_size_binding(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'shard.gz';task=dict(id=1,entry=dict(rel='file',size=12))
            with gzip.open(path,'wt') as stream:stream.write(json.dumps(task)+'\n')
            events=[dict(receipt=dict(id=1,rel='file',expected_bytes=12))]
            match_tasks(path,events);self.assertEqual(events[0]['task'],task)
            events[0]['receipt']['rel']='different'
            with self.assertRaisesRegex(ValueError,'differs'):match_tasks(path,events)

    def test_task_outside_shard_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'shard.gz'
            with gzip.open(path,'wt') as stream:stream.write(json.dumps(dict(id=1,entry=dict(rel='file',size=12)))+'\n')
            with self.assertRaisesRegex(ValueError,'outside'):
                match_tasks(path,[dict(receipt=dict(id=2,rel='file',expected_bytes=12))])


if __name__=='__main__':unittest.main()
