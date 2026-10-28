from pathlib import Path
import unittest
from unittest import mock

import migration_reaudit_handoff_runtime as runtime
import migration_file_rollout as rollout
from migration_file_handoff_postcheck import receipt_status


class FileHandoffTests(unittest.TestCase):
    def approval(self):
        control=Path('/shared/files')
        nodes=['master-0']+['worker-'+str(i) for i in range(11)]
        return control,dict(kind='explicit-24-node-host-migration-v1',
            directive='24节点已经可以连接，没问题的就把任务迁移到24个节点',sequential=True,
            allowed=[dict(control=str(control),node=n,target_node='worker-'+str(i+11))
                     for i,n in enumerate(nodes)])

    def test_exact_file_mapping(self):
        control,approval=self.approval()
        runtime.validate_file_approval(approval,control,'master-0','worker-11')
        runtime.validate_file_approval(approval,control,'worker-10','worker-22')
        for node,target in [('master-0','worker-12'),('worker-11','worker-22')]:
            with self.assertRaises(ValueError):runtime.validate_file_approval(approval,control,node,target)
        with self.assertRaises(ValueError):runtime.validate_file_approval(approval,Path('/other'),'master-0','worker-11')

    def test_partial_or_parallel_authority_rejected(self):
        control,approval=self.approval();approval['sequential']=False
        with self.assertRaises(ValueError):runtime.validate_file_approval(approval,control,'master-0','worker-11')
        control,approval=self.approval();approval['allowed'].pop()
        with self.assertRaises(ValueError):runtime.validate_file_approval(approval,control,'master-0','worker-11')

    def test_exact_file_counts_and_bytes(self):
        report=dict(missing=dict(file=dict(paths=10,logical_bytes=100)))
        runtime.validate_coverage(dict(files=10,bytes=100),report,True)
        for cfg in [dict(files=9,bytes=100),dict(files=10,bytes=99)]:
            with self.assertRaises(ValueError):runtime.validate_coverage(cfg,report,True)

    def test_remote_mount_or_actor_mismatch_fails_closed(self):
        cfg=dict(cluster='cluster',source_root_ino=1,target_root_ino=2)
        result=dict(host='job-cluster-worker-11',boot='boot',source=1,target=2,mounts=True,actors=[])
        with mock.patch.object(rollout,'remote',return_value=result):
            self.assertEqual(rollout.probe('worker-11',Path('/control'),cfg),result)
        for key,value in [('host','wrong'),('source',3),('mounts',False),('actors',['123'])]:
            with mock.patch.object(rollout,'remote',return_value=dict(result,**{key:value})):
                with self.assertRaises(ValueError):rollout.probe('worker-11',Path('/control'),cfg)

    def test_postcheck_requires_sha_and_current_identity(self):
        base=mock.Mock();task=dict(entry=dict(rel='a',size=10));cfg={}
        receipt=dict(rel='a',status='copied',sha256='a'*64)
        base.resume_valid.return_value=True
        self.assertEqual(receipt_status(base,cfg,task,receipt),'unchanged_verified_copy_identity')
        base.resume_valid.return_value=False
        self.assertEqual(receipt_status(base,cfg,task,receipt),'source_or_target_identity_changed')
        self.assertEqual(receipt_status(base,cfg,task,dict(receipt,sha256='bad')),'missing_sha_evidence')
        self.assertEqual(receipt_status(base,cfg,task,dict(receipt,status='changed')),'copy_attention')

    def test_file_receiver_does_not_install_namespace_copy(self):
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            control=Path(temp);folder=control/'handoffs'/'master-0';folder.mkdir(parents=True)
            (control/'nodes'/'master-0').mkdir(parents=True)
            path=folder/'plan.json';path.write_text('{}')
            old=dict(pid=123,start='1',host='dev')
            plan=dict(node='master-0',source_host='dev',source_process=dict(pid=123,start='1'))
            prefix=dict(prefix_tasks=2)
            seal=dict(plan_sha256=runtime.base.digest(path),source_process=plan['source_process'],
                      actual_source_terminal=True,closed_progress=old,receipts={},prefix=prefix)
            cfg=dict(job_kind='files',shards={'master-0':dict(path='fixture')})
            original=runtime.base.copy_one
            with mock.patch.object(runtime,'validate',return_value=(plan,control,cfg)), \
                    mock.patch.object(runtime,'read',return_value=seal), \
                    mock.patch.object(runtime.base,'load_receipts',return_value={}), \
                    mock.patch.object(runtime,'verify_prefix',return_value=prefix), \
                    mock.patch.object(runtime.base,'validate_worker'), \
                    mock.patch.object(runtime.base,'worker') as worker:
                runtime.receiver(path)
                worker.assert_called_once_with(control,'master-0',checkpoint=old)
                self.assertIs(runtime.base.copy_one,original)


if __name__=='__main__':unittest.main()
