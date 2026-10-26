from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_reaudit_handoff_runtime as runtime


class RuntimeTests(unittest.TestCase):
    def test_batch_approval_exact_scope_and_sequence(self):
        control=Path('/fixture/namespace')
        approval=dict(answer='允许，逐个交接剩余目录分片',sequential=True,
            allowed=[dict(control=str(control),node=n) for n in
                     ['master-0']+['worker-'+str(i) for i in range(1,11)]])
        runtime.validate_approval(approval,control,'worker-10')
        for node in ('worker-0','worker-11'):
            with self.assertRaises(ValueError):runtime.validate_approval(approval,control,node)
        with self.assertRaises(ValueError):runtime.validate_approval(approval,Path('/fixture/files'),'master-0')
        approval['sequential']=False
        with self.assertRaises(ValueError):runtime.validate_approval(approval,control,'master-0')

    def test_wrong_receiver_boot_refused_before_data_operation(self):
        control=Path('/fixture/control');path=control/'handoffs/worker-0/plan.json'
        plan=dict(kind='single-namespace-shard-host-handoff-v1',control=str(control),node='worker-0',
                  config_sha256='cfg',source_host='dev',source_boot='source-boot',receiver_host='worker',receiver_boot='right-boot')
        cfg=dict(job_kind='namespace',execution_mode='local',execution_host='dev',execution_boot_id='source-boot')
        def read(p):
            return plan if p==path else cfg if p.name=='config.json' else dict(config_sha256='cfg')
        with mock.patch.object(runtime.base,'read_json',side_effect=read), \
                mock.patch.object(runtime.base,'digest',return_value='cfg'), \
                mock.patch.object(runtime,'identity',return_value=dict(host='worker',boot='wrong-boot')), \
                mock.patch.object(runtime.base,'worker') as worker:
            with self.assertRaisesRegex(ValueError,'host/boot differs'):runtime.validate(path,True)
        worker.assert_not_called()

    def test_missing_source_exit_proof_cannot_start_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp);path=folder/'plan.json';path.write_text('{}')
            plan=dict(source_process=dict(pid=123,start='1'));seal=dict(plan_sha256=runtime.base.digest(path),
                source_process=plan['source_process'],actual_source_terminal=False)
            with mock.patch.object(runtime,'validate',return_value=(plan,folder,{})), \
                    mock.patch.object(runtime,'read',return_value=seal),mock.patch.object(runtime.base,'worker') as worker:
                with self.assertRaisesRegex(ValueError,'source exit proof differs'):runtime.receiver(path)
            worker.assert_not_called()
            self.assertFalse((folder/'receiver-copy-started.json').exists())


if __name__=='__main__':unittest.main()
