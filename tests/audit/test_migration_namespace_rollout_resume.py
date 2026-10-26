from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from migration_namespace_rollout_resume import check_pristine


class ResumeTests(unittest.TestCase):
    def fixture(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);control=Path(temp.name)
        folder=control/'namespace-rollout-v2';folder.mkdir();nodes=['master-0','worker-1']
        old=dict(pid=111,start='1',host='dev',boot='boot')
        actor=dict(pid=33,start='3',host='dev',status='copying')
        plan=dict(control=str(control),nodes=nodes,approval='/approval',approval_sha256='approval',
            files_control='/files',file_actors={})
        records={folder/'plan.json':plan,folder/'progress.json':dict(status='inspection_required',completed=[]),
            folder/'driver-instance.json':old,folder/'driver-spawn.json':old,
            control/'config.json':dict(job_kind='namespace',execution_mode='local',execution_host='dev',execution_boot_id='boot'),
            control/'handoffs/worker-0/plan.json':dict(config_sha256='cfg'),'/approval':{}}
        for n in nodes:
            records[control/'nodes'/n/'progress.json']=actor
            records[control/'nodes'/n/'launch.json']=actor
        api=SimpleNamespace(read=lambda p:records[p],NODES=nodes,
            identity=lambda:dict(pid=222,start='2',host='dev',boot='boot'),
            process_terminal=mock.Mock(side_effect=lambda p:p['pid']==111),
            base=SimpleNamespace(digest=lambda p:'approval' if str(p)=='/approval' else 'cfg'),
            validate_approval=mock.Mock(),assert_files_unchanged=mock.Mock())
        return control,folder,api,records

    def test_only_pristine_failure_can_continue(self):
        _,folder,api,_=self.fixture();proof=check_pristine(api,folder)
        self.assertTrue(proof['all_handoff_folders_absent']);api.assert_files_unchanged.assert_called_once()

    def test_existing_staged_evidence_refused(self):
        control,folder,api,_=self.fixture();(control/'handoffs/master-0').mkdir(parents=True)
        with self.assertRaisesRegex(ValueError,'existing handoff'):check_pristine(api,folder)

    def test_live_old_driver_refused(self):
        _,folder,api,_=self.fixture();api.process_terminal.return_value=False;api.process_terminal.side_effect=None
        with self.assertRaisesRegex(ValueError,'actually exited'):check_pristine(api,folder)

    def test_completed_handoff_count_refused(self):
        _,folder,api,records=self.fixture();records[folder/'progress.json']['completed']=['master-0']
        with self.assertRaisesRegex(ValueError,'pristine'):check_pristine(api,folder)


if __name__=='__main__':unittest.main()
