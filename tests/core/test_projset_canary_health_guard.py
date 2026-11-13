#!/usr/bin/env python3
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'ops/diagnostics'))
import projset_canary_health_guard as module


def state(t, f, b, d):
    return {'updated': t, 'counts': {'files': f, 'symlinks': 0, 'file_bytes': b}, 'done_dirs': d}


class ThroughputGuardTests(unittest.TestCase):
    def setUp(self):
        self.rates = {'audit_files_links_per_hour': 3600, 'audit_logical_TB_per_hour': 3.6,
                      'audit_directories_per_hour': 360}
        self.g = module.Guard(self.rates)

    def test_all_three_slow_causes_stop(self):
        self.g.observe(state(100, 0, 0, 0))
        x = self.g.observe(state(190, 10, 10_000_000_000, 1))
        self.assertTrue(x['all_below_threshold'])

    def test_only_small_files_does_not_cause_stop(self):
        self.g.observe(state(100, 0, 0, 0))
        x = self.g.observe(state(190, 90, 100, 9))
        self.assertFalse(x['all_below_threshold'])

    def test_duplicate_time_and_sliding_window(self):
        self.g.observe(state(100, 0, 0, 0))
        self.assertIsNone(self.g.observe(state(100, 0, 0, 0)))
        self.assertIsNone(self.g.observe(state(150, 50, 50_000_000_000, 5)))
        self.assertFalse(self.g.observe(state(190, 90, 90_000_000_000, 9))['all_below_threshold'])
        self.assertEqual(self.g.observe(state(250, 150, 150_000_000_000, 15))['seconds'], 100)

    def test_invalid_and_regressing_values_fail_closed(self):
        for bad in (0, -1, True, float('nan')):
            rates = dict(self.rates, audit_files_links_per_hour=bad)
            with self.assertRaises(ValueError): module.Guard(rates)
        self.g.observe(state(100, 10, 100, 1))
        with self.assertRaises(ValueError): self.g.observe(state(101, 9, 100, 1))


if __name__ == '__main__': unittest.main()
