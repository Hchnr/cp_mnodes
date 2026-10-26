import json
from pathlib import Path
import tempfile
import unittest

from deploy_prepared_producer import unresolved_cancelled
from migration_io import write_json
from migration_prepared_resume import digest_file


class RetryHandoverTests(unittest.TestCase):
    def test_reserved_retries_are_not_cancelled_again_during_next_handover(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            control=Path(tmp)
            evidence=control/'retry.json'
            entries=[dict(rel='retried',size=1),dict(rel='not-offered',size=2)]
            write_json(evidence,{'entries':entries})
            previous=dict(next_task_id=100,cancelled_retry_path=str(evidence),
                          cancelled_retry_sha256=digest_file(evidence))
            delta=control/'delta.jsonl'
            delta.write_text(''.join(json.dumps(row)+'\n' for row in [
                {'id':5,'entries':entries},  # Original cancelled tasks remain in claims.
                {'id':103,'entries':[entries[0]]},  # Already copied OR still leased.
            ]))
            self.assertEqual(unresolved_cancelled(previous,delta,control),[entries[1]])

    def test_unchanged_retry_evidence_is_required(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmp:
            control=Path(tmp);path=control/'retry.json'
            write_json(path,{'entries':[]})
            previous=dict(next_task_id=100,cancelled_retry_path=str(path),
                          cancelled_retry_sha256='0'*64)
            with self.assertRaisesRegex(RuntimeError,'evidence changed'):
                unresolved_cancelled(previous,control/'unused-delta',control)

    def test_no_retry_contract_needs_no_extra_receipt_or_delta_reads(self):
        self.assertEqual(unresolved_cancelled({},Path('/not-used'),Path('/tmp')),[])


if __name__=='__main__':
    unittest.main()
