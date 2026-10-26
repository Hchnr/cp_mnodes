import gzip
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import migration_manifest_copy as base
import migration_reaudit_attention_retry as retry


class RetryTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name);self.source=self.root/'source';self.target=self.root/'target'
        self.source.mkdir();self.target.mkdir();p=self.source/'file';p.write_bytes(b'old')
        entry=base.validate_row(dict(rel='file',kind='file',source=base.engine._record(p.stat()),reason='target_absent'))
        self.task=dict(id=7,entry=entry);p.write_bytes(b'current source')
        self.cfg=dict(source=str(self.source),target=str(self.target),run_id='attention-test',job_kind='files',bytes=3)

    def fixture(self, receipt_status='changed'):
        control=self.root/'control';node=control/'nodes/node';node.mkdir(parents=True)
        shard=control/'shard.gz'
        with gzip.open(shard,'wt') as stream:stream.write(json.dumps(self.task)+'\n')
        self.cfg['shards']=dict(node=dict(path=str(shard)))
        (control/'config.json').write_text(json.dumps(self.cfg))
        receipt=dict(id=7,rel='file',status=receipt_status,expected_bytes=3)
        line=(json.dumps(receipt)+'\n').encode();original=node/'receipts-old.jsonl';original.write_bytes(line)
        folder=self.root/'attention';folder.mkdir();manifest=folder/'tasks.gz'
        import hashlib
        event=dict(node='node',task=self.task,receipt=receipt,original_receipt=str(original),byte_offset=0,
                   line_sha256=hashlib.sha256(line).hexdigest())
        with gzip.open(manifest,'wt') as stream:stream.write(json.dumps(event)+'\n')
        summary=dict(control=str(control),config_sha256=base.digest(control/'config.json'),manifest=str(manifest),
                     sha256=base.digest(manifest),completed_attention=1)
        snapshot=folder/'snapshot.json';snapshot.write_text(json.dumps(summary))
        return snapshot,folder/'retry',original,line

    def execute(self, snapshot,output,prior_refreshed_bytes=0):
        entry=SimpleNamespace(base=base,validate=mock.Mock())
        with mock.patch.dict(sys.modules,{'migration_reaudit_copy':entry}),mock.patch.object(sys,'path',sys.path[:]):
            retry.run(snapshot,output,prior_refreshed_bytes)
        return json.loads((output/'result.json').read_text())

    def test_completed_changed_task_retries_latest_version_once(self):
        snapshot,output,original,line=self.fixture()
        result=self.execute(snapshot,output)
        self.assertEqual(result['counts']['identity_verified'],1)
        self.assertEqual((self.target/'file').read_bytes(),b'current source')
        self.assertEqual(original.read_bytes(),line)
        self.assertFalse(result['whole_tree_completion_proven'])
        self.assertTrue(result['same_main_paths_not_additional_unique_files'])

    def test_existing_target_never_dispatched_or_changed(self):
        snapshot,output,_,_=self.fixture();(self.target/'file').write_bytes(b'keep')
        result=self.execute(snapshot,output)
        self.assertEqual(result['accepted_files'],0)
        self.assertEqual((self.target/'file').read_bytes(),b'keep')
        self.assertEqual(result['observations'][0]['not_dispatched_reason'],'existing_target_retained')

    def test_successful_old_task_is_not_retryable(self):
        snapshot,output,_,_=self.fixture(receipt_status='copied')
        with self.assertRaisesRegex(ValueError,'only completed source-changed'):self.execute(snapshot,output)
        self.assertFalse((self.target/'file').exists())

    def test_exact_fifty_tb_is_rejected_before_copy(self):
        self.cfg['bytes']=50_000_000_000_000-len(b'current source')
        snapshot,output,_,_=self.fixture()
        with self.assertRaisesRegex(ValueError,'exceeds threshold'):self.execute(snapshot,output)
        self.assertFalse((self.target/'file').exists())

    def test_prior_refreshed_versions_count_toward_fifty_tb_gate(self):
        self.cfg['bytes']=50_000_000_000_000-len(b'current source')-5
        snapshot,output,_,_=self.fixture()
        with self.assertRaisesRegex(ValueError,'exceeds threshold'):self.execute(snapshot,output,5)
        self.assertFalse((self.target/'file').exists())

    def test_negative_prior_budget_refused(self):
        snapshot,output,_,_=self.fixture()
        with self.assertRaisesRegex(ValueError,'nonnegative'):self.execute(snapshot,output,-1)

    def test_raw_is_rejected_before_source_access(self):
        self.task['entry']['rel']='eai_pwm/datasets/raw/file'
        with self.assertRaises(ValueError):retry.refresh_candidate(base,self.cfg,self.task)

    def test_source_symlink_parent_is_not_followed(self):
        outside=self.root/'outside';outside.mkdir();(outside/'file').write_bytes(b'outside')
        (self.source/'link').symlink_to(outside,target_is_directory=True);self.task['entry']['rel']='link/file'
        with self.assertRaises(OSError):retry.refresh_candidate(base,self.cfg,self.task)


if __name__=='__main__':unittest.main()
