import copy
import unittest

import observe_worker_canary as observe


class WindowTests(unittest.TestCase):
    def setUp(self):
        node = dict(pid=1, started='process', run_started='run', counts={'copied': 10},
                    bytes=1000, streamed_bytes=2000, verified_files=10, verified_bytes=1000,
                    completed_batches=1, broker_retries=0, active=64, awaiting=0)
        self.before = dict(monotonic=100, stage='p3-r3', status='scanning', gc_seconds=1,
                           nodes={'lane': node})
        self.after = copy.deepcopy(self.before)
        self.after['monotonic'] = 120
        self.after['nodes']['lane'].update(counts={'copied': 20}, bytes=2000,
                                           streamed_bytes=4000, verified_files=20, verified_bytes=2000)

    def test_per_lane_and_cluster_deltas_use_same_window(self):
        result = observe.compare(self.before, self.after)
        self.assertEqual(result['files_per_second'], .5)
        self.assertEqual(result['mean_copied_bytes'], 100)
        self.assertEqual(result['active'], 64)
        self.assertEqual(result['lanes']['lane']['bytes'], 1000)

    def test_zero_copy_window_is_not_invented_as_progress(self):
        self.after['nodes'] = copy.deepcopy(self.before['nodes'])
        result = observe.compare(self.before, self.after)
        self.assertEqual(result['GiB_per_second'], 0)
        self.assertIsNone(result['mean_copied_bytes'])

    def test_regressed_counter_or_identity_rejected(self):
        for patch in ({'pid': 2}, {'started': 'new'}, {'run_started': 'new'},
                      {'bytes': 999}, {'counts': {}}, {'broker_retries': -1}):
            after = copy.deepcopy(self.after)
            after['nodes']['lane'].update(patch)
            with self.subTest(patch=patch), self.assertRaises(RuntimeError):
                observe.compare(self.before, after)

    def test_stage_and_time_discontinuity_rejected(self):
        for patch in ({'stage': 'next'}, {'status': 'complete'}, {'monotonic': 100}):
            with self.subTest(patch=patch), self.assertRaises(RuntimeError):
                observe.compare(self.before, dict(self.after, **patch))


if __name__ == '__main__':
    unittest.main()
