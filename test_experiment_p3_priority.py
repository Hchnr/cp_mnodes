import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

import experiment_p3_priority as trial


class P3PriorityTests(unittest.TestCase):
    def test_restore_owns_only_baseline_and_trial_values(self):
        self.assertEqual(trial.restoration_value(0),10)
        self.assertEqual(trial.restoration_value(10),10)
        self.assertIsNone(trial.restoration_value(5))

    def test_reused_pid_never_receives_priority_update(self):
        with mock.patch.object(trial,'require_identity',side_effect=RuntimeError('changed')), \
                mock.patch.object(os,'setpriority') as update:
            self.assertEqual(trial.restore({'pid':1})['status'],'target_exited_or_changed_no_write')
            update.assert_not_called()

    def test_external_operator_priority_is_not_overwritten(self):
        with mock.patch.object(trial,'require_identity'), \
                mock.patch.object(os,'getpriority',return_value=5), \
                mock.patch.object(os,'setpriority') as update:
            self.assertEqual(trial.restore({'pid':1})['status'],'external_priority_change_no_write')
            update.assert_not_called()

    def test_watchdog_restores_when_controller_pipe_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            r,w=os.pipe();os.close(w)
            try:
                with mock.patch.object(trial.signal,'signal'), \
                        mock.patch.object(trial,'restore',return_value={'status':'restored'}) as restore:
                    trial.watchdog({'pid':1},r,Path(directory)/'result.json',time.monotonic()+1)
                    restore.assert_called_once_with({'pid':1})
            finally:os.close(r)


if __name__=='__main__':unittest.main()
