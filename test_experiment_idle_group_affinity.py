import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

import experiment_idle_group_affinity as trial


class GroupAffinityTests(unittest.TestCase):
    def test_priority_groups_are_disjoint_but_no_other_workers_are_admitted(self):
        original=set(range(192))
        self.assertEqual(trial.trial_mask(original,5873,True),[0,1,2,3])
        self.assertEqual(trial.trial_mask(original,5879,True),[0,1,2,3])
        self.assertEqual(trial.trial_mask(original,3737241,True),[4,5,6,7])
        self.assertEqual(trial.trial_mask(original,3741388,True),[4,5,6,7])
        self.assertEqual(trial.trial_mask(original,3737241,False),[0,1,2,3])

    def test_restore_attempts_every_worker_even_if_one_restoration_fails(self):
        plans=[{'expected':{'pid':i}} for i in range(4)]
        with mock.patch.object(trial,'restore',side_effect=[RuntimeError('fixture'),
                {'status':'restored'},{'status':'restored'},{'status':'restored'}]) as restore:
            value=trial.restore_all(plans)
        self.assertEqual(restore.call_count,4)
        self.assertEqual(value['status'],'inspect_restoration')

    def test_watchdog_checks_new_work_and_restores_all_without_waiting_for_controller(self):
        r,w=os.pipe()
        try:
            with tempfile.TemporaryDirectory() as directory, \
                    mock.patch.object(trial.signal,'signal'), \
                    mock.patch.object(trial.select,'select',return_value=([],[],[])), \
                    mock.patch.object(trial,'group_guard',side_effect=RuntimeError('new lease')), \
                    mock.patch.object(trial,'restore_all',return_value={'status':'restored'}) as restore:
                trial.watchdog([],[],r,Path(directory)/'result.json',time.monotonic()+10)
                restore.assert_called_once_with([])
        finally:os.close(r);os.close(w)

    def test_watchdog_restores_all_on_controller_eof(self):
        r,w=os.pipe();os.close(w)
        try:
            with tempfile.TemporaryDirectory() as directory, \
                    mock.patch.object(trial.signal,'signal'), \
                    mock.patch.object(trial,'restore_all',return_value={'status':'restored'}) as restore:
                trial.watchdog([],[],r,Path(directory)/'result.json',time.monotonic()+10)
                restore.assert_called_once_with([])
        finally:os.close(r)

    def test_watchdog_restores_all_on_deadline(self):
        r,w=os.pipe()
        try:
            with tempfile.TemporaryDirectory() as directory, \
                    mock.patch.object(trial.signal,'signal'), \
                    mock.patch.object(trial,'restore_all',return_value={'status':'restored'}) as restore:
                trial.watchdog([],[],r,Path(directory)/'result.json',time.monotonic()-.1)
                restore.assert_called_once_with([])
        finally:os.close(r);os.close(w)


if __name__=='__main__':unittest.main()
