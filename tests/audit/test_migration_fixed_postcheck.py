import gzip
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import migration_fixed_postcheck as check


class PostcheckTests(unittest.TestCase):
    def fixture(self, folder, *, sha='a'*64, unchanged=True, status='complete', receipts_extra=False):
        root=Path(folder);shard=root/'shard.gz'
        task=dict(id=0,entry=dict(rel='safe/file',size=7))
        with gzip.open(shard,'wt') as stream:stream.write(json.dumps(task)+'\n')
        cfg=dict(job_kind='files',files=1,bytes=7,
                 shards=dict(node=dict(path=str(shard),files=1,sha256='bound-by-entry-validate')))
        state=dict(pid=2147483647,start='0',status=status,active=0)
        receipt=dict(id=0,rel='safe/file',status='copied',sha256=sha)
        receipts={0:receipt}
        if receipts_extra:receipts[1]=dict(receipt,id=1)
        validate=mock.Mock();resume=mock.Mock(return_value=unchanged)
        base=SimpleNamespace(read_json=lambda path:cfg if path.name=='config.json' else state,
                             load_receipts=lambda folder:receipts,resume_valid=resume)
        return SimpleNamespace(base=base,validate=validate),resume

    def run_fixture(self, **kwargs):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        entry,resume=self.fixture(temp.name,**kwargs)
        with mock.patch.dict(sys.modules,{'migration_reaudit_copy':entry}),mock.patch.object(sys,'path',sys.path[:]):
            result=check.run(temp.name)
        entry.validate.assert_called_once()
        return result,resume

    def test_closed_identity_and_sha_evidence_pass(self):
        result,resume=self.run_fixture()
        self.assertEqual(result['status'],'verified_closed_batch_identities')
        self.assertEqual(result['verified_logical_bytes'],7)
        self.assertFalse(result['whole_tree_completion_proven'])
        self.assertFalse(result['payload_read'])
        self.assertFalse(result['target_mutated'])
        resume.assert_called_once()

    def test_missing_sha_cannot_pass_even_with_unchanged_stat(self):
        result,resume=self.run_fixture(sha=None)
        self.assertEqual(result['status'],'closed_batch_needs_reconciliation')
        self.assertEqual(result['counts']['missing_sha_evidence'],1)
        self.assertEqual(result['verified_logical_bytes'],0)
        resume.assert_not_called()

    def test_changed_identity_is_not_trusted(self):
        result,_=self.run_fixture(unchanged=False)
        self.assertEqual(result['counts']['source_or_target_identity_changed'],1)
        self.assertEqual(result['verified_logical_bytes'],0)

    def test_unclosed_worker_refused(self):
        with self.assertRaisesRegex(ValueError,'shard not fully closed'):
            self.run_fixture(status='copying')

    def test_extra_receipt_refused(self):
        with self.assertRaisesRegex(ValueError,'receipt/shard exact coverage differs'):
            self.run_fixture(receipts_extra=True)


if __name__=='__main__':unittest.main()
