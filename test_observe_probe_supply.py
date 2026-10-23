import copy
import unittest

from observe_probe_supply import compare


class SupplyMeasurementTests(unittest.TestCase):
    def samples(self):
        a = {'monotonic': 10, 'nodes': {
            'node': {'pid': 1, 'started': 'start', 'counts': {'copied': 10},
                     'bytes': 1000, 'streamed_bytes': 1000, 'retries': 0,
                     'batches': 1, 'active': 1, 'awaiting': 0}},
             'probes': {'endpoint': {'pid': 2, 'starttime_ticks': '2',
                                    'counts': {'missing': 1, 'delegated_small_files': 1},
                                    'small_probe_delegate': {'delegated_is_missing_evidence': False}}},
             'state': {'stage': 'p3-r3', 'status': 'scanning',
                       'target_filter': {'checked': 1}},
             'procs': {'3': {'starttime_ticks': '3', 'cpu_seconds': 1}}}
        b = copy.deepcopy(a)
        b['monotonic'] = 20
        b['nodes']['node'].update(bytes=2000, streamed_bytes=2000, counts={'copied': 20})
        b['probes']['endpoint']['counts'] = {'missing': 2, 'delegated_small_files': 10}
        b['state']['target_filter']['checked'] = 11
        return a, b

    def test_rates_and_delegation_evidence_stay_separate(self):
        a, b = self.samples()
        result = compare(a, b)
        self.assertEqual(result['files_per_second'], 1)
        self.assertEqual(result['mean_copied_bytes'], 100)
        self.assertEqual(result['probes'][0]['delta']['missing'], 1)
        self.assertEqual(result['probes'][0]['delta']['delegated_small_files'], 9)
        self.assertTrue(result['protected_copy_identities_stable'])

    def test_changed_identities_stages_or_negative_counters_are_not_zero_speed(self):
        for mutate in (
            lambda b: b['nodes']['node'].update(pid=9),
            lambda b: b['probes']['endpoint'].update(starttime_ticks='9'),
            lambda b: b['nodes']['node'].update(bytes=0),
            lambda b: b['probes']['endpoint']['counts'].update(missing=0),
            lambda b: b['state'].update(stage='p1-r1'),
            lambda b: b.update(monotonic=10),
        ):
            a, b = self.samples()
            mutate(b)
            with self.assertRaises((ValueError, RuntimeError)):
                compare(a, b)


if __name__ == '__main__':
    unittest.main()
