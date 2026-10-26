import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_namespace_rollout as rollout


class RolloutTests(unittest.TestCase):
    def test_remote_preflight_failure_never_drains_or_spawns(self):
        with tempfile.TemporaryDirectory() as temp:
            control=Path(temp);folder=control/'namespace-rollout-v2';folder.mkdir()
            (control/'config.json').write_text('{}')
            (folder/'plan.json').write_text(json.dumps(dict(control=str(control),
                files_control='/fixture/files',file_actors={},nodes=['master-0'])))
            with mock.patch.object(rollout,'assert_files_unchanged'), \
                    mock.patch.object(rollout,'remote',side_effect=TimeoutError('SSH unavailable')), \
                    mock.patch.object(rollout,'spawn') as spawn:
                with self.assertRaises(TimeoutError):rollout.run(folder)
            spawn.assert_not_called()
            self.assertFalse((control/'handoffs').exists())
            progress=json.loads((folder/'progress.json').read_text())
            self.assertEqual(progress['status'],'inspection_required')
            self.assertEqual(progress['completed'],[])

    def test_file_actor_identity_change_is_refused(self):
        with mock.patch.object(rollout,'file_identities',return_value={'node':{'pid':2}}):
            with self.assertRaisesRegex(ValueError,'identities changed'):
                rollout.assert_files_unchanged(Path('/fixture'),{'node':{'pid':1}})


if __name__=='__main__':unittest.main()
