import os
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_manifest_copy as base
import migration_namespace_postcheck as check
import migration_reaudit_copy as copy


class NamespacePostcheckTests(unittest.TestCase):
    def test_complete_status_is_not_actual_exit(self):
        actor=dict(host='local',pid=1,start='1');state=dict(actor,status='complete',active=0)
        self.assertEqual(check.original_actor_state(state,actor,False),'waiting_for_actual_original_exit')
        self.assertEqual(check.original_actor_state(state,actor,True),'ready_for_audit')

    def test_handoff_never_uses_local_pid_absence_as_remote_exit(self):
        actor=dict(host='local',pid=1,start='1');state=dict(host='remote',pid=1,start='1',status='complete',active=0)
        self.assertEqual(check.original_actor_state(state,actor,True),'original_actor_replaced_or_handed_off')

    def test_drained_handoff_prefix_is_not_full_shard(self):
        actor=dict(host='local',pid=1,start='1');state=dict(actor,status='stopped',active=0)
        self.assertEqual(check.original_actor_state(state,actor,True),'original_actor_exited_without_full_shard')

    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.source=self.root/'source';self.target=self.root/'target'
        self.source.mkdir();self.target.mkdir();self.cfg=dict(source=str(self.source),target=str(self.target),run_id='postcheck-fixture')

    def fixture(self,kind='directory'):
        path=self.source/'item'
        if kind=='directory':path.mkdir()
        else:path.symlink_to('literal/target')
        row=dict(rel='item',kind=kind,source=base.engine._record(path.lstat()),reason='target_absent')
        if kind=='symlink':row['link_target']='literal/target'
        task=dict(id=1,entry=copy.namespace_entry(row));receipt=copy.namespace_one(self.cfg,task)
        self.assertEqual(receipt['status'],'copied')
        return task,receipt

    def inspect(self,task,receipt):return check.inspect_group(base,self.cfg,[task],{1:receipt})[0]

    def test_directory_is_checked_without_reading_payload_or_mutating(self):
        task,receipt=self.fixture();before=(self.target/'item').stat()
        with mock.patch.object(os,'read',side_effect=AssertionError('payload read')), \
                mock.patch.object(os,'mkdir',side_effect=AssertionError('mutation')):
            row=self.inspect(task,receipt)
        self.assertEqual(row['status'],'path_identity_verified')
        self.assertEqual((self.target/'item').stat().st_ctime_ns,before.st_ctime_ns)

    def test_new_source_child_is_not_claimed_closed(self):
        task,receipt=self.fixture();(self.source/'item/new').write_text('new')
        # tmp filesystems may coarsen consecutive metadata writes into one
        # tick; make the observed directory version change deterministic.
        old=task['entry']['identity'];os.utime(self.source/'item',ns=(old['atime_ns'],old['mtime_ns']+1_000_000_000))
        self.assertEqual(self.inspect(task,receipt)['status'],'source_directory_version_changed')

    def test_missing_target_is_reported(self):
        task,receipt=self.fixture();(self.target/'item').rmdir()
        self.assertEqual(self.inspect(task,receipt)['status'],'path_check_error')

    def test_replaced_target_is_not_trusted(self):
        task,receipt=self.fixture();(self.target/'item').rename(self.target/'old');(self.target/'item').mkdir()
        self.assertEqual(self.inspect(task,receipt)['status'],'target_inode_changed')

    def test_symlink_text_and_identity_verified(self):
        task,receipt=self.fixture('symlink')
        self.assertEqual(self.inspect(task,receipt)['status'],'path_identity_verified')
        (self.target/'item').unlink();(self.target/'item').symlink_to('changed')
        self.assertNotEqual(self.inspect(task,receipt)['status'],'path_identity_verified')

    def test_old_failed_copy_cannot_pass_as_verified(self):
        task,receipt=self.fixture();receipt['status']='changed'
        self.assertEqual(self.inspect(task,receipt)['status'],'copy_attention')

    def test_closed_original_shard_has_exact_coverage_and_no_receiver_claim(self):
        task,receipt=self.fixture();control=self.root/'control';node=control/'nodes/node';node.mkdir(parents=True)
        shard=control/'shard.gz'
        with gzip.open(shard,'wt') as stream:stream.write(json.dumps(task)+'\n')
        (node/'receipts-test.jsonl').write_text(json.dumps(receipt)+'\n')
        (control/'config.json').write_text('{}')
        cfg=dict(self.cfg,shards=dict(node=dict(path=str(shard),files=1,sha256=base.digest(shard))))
        state=dict(status='complete',active=0,submitted=1,host='fixture',pid=123,start='1',
            counts=dict(processed=1,copied=1,completed_bytes=0,copied_bytes=0))
        output=node/'post-copy-audit-original';output.mkdir()
        result=check.audit_closed(base,control,cfg,'node',state,output,{},actor_kind='original_local')
        self.assertEqual(result['counts']['path_identity_verified'],1)
        self.assertTrue(result['exact_receipt_coverage']);self.assertTrue(result['actual_actor_terminal'])
        self.assertFalse(result['actual_receiver_terminal']);self.assertFalse(result['whole_tree_completion_proven'])


if __name__=='__main__':unittest.main()
