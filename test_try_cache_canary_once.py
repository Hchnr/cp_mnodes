import copy
import datetime
import unittest
from try_cache_canary_once import SHA, eligible, observe_once


class ReadOnlySelection(unittest.TestCase):
    def setUp(self):
        self.now = 1800000000.0
        iso = datetime.datetime.fromtimestamp(self.now, datetime.timezone.utc).isoformat()
        self.expected = {'pid': 123, 'start_ticks': '456', 'argv': ['copy', 'supplement']}
        self.master = dict(stop=False, status='scanning', training_state='watching', helper_sha256=SHA,
                           owner_count=64, leased_by_stage={}, host_draining=False, helpers=[],
                           at=self.now, state_updated=iso, training_updated=iso)
        self.worker = dict(self.expected, stop=False, process_state='S', active=0, payload_fds=0,
                           threads=64, budget=10000000000, ended=None, thread_error=None,
                           at=self.now, updated=iso, helpers=[])

    def test_current_empty_identity_dispatches_once(self):
        calls = []
        result = observe_once(lambda: (self.master, self.worker), lambda: calls.append(1) or {'status': 'waiting_payload'},
                              self.expected, 10000000000, wait_seconds=45, clock=lambda: self.now)
        self.assertEqual(calls, [1])
        self.assertTrue(result['controller_invoked'])

    def test_observe_only_never_dispatches_even_when_idle(self):
        calls = []
        result = observe_once(lambda: (self.master, self.worker), lambda: calls.append(1),
                              self.expected, 10000000000, wait_seconds=45, clock=lambda: self.now,
                              allow_dispatch=False)
        self.assertEqual(result['status'], 'eligible_read_only')
        self.assertFalse(result['controller_invoked'])
        self.assertFalse(calls)

    def test_every_safety_field_fails_closed(self):
        cases = [('master', 'stop', True), ('worker', 'stop', True), ('master', 'status', 'complete'),
                 ('master', 'training_state', 'stopped'), ('master', 'helper_sha256', 'old'),
                 ('master', 'owner_count', 63), ('master', 'leased_by_stage', {'p1-r1': 1}),
                 ('master', 'host_draining', True), ('master', 'helpers', [987]), ('worker', 'helpers', [987]),
                 ('worker', 'pid', 124), ('worker', 'start_ticks', '457'), ('worker', 'argv', ['different']),
                 ('worker', 'process_state', 'Z'), ('worker', 'process_state', None),
                 ('worker', 'process_state', 'T'), ('worker', 'active', 1), ('worker', 'payload_fds', 1),
                 ('worker', 'threads', 128), ('worker', 'budget', 20000000000), ('worker', 'ended', 'done'),
                 ('worker', 'thread_error', 'error'), ('master', 'at', self.now - 6),
                 ('worker', 'at', self.now + 1), ('worker', 'updated', 'invalid'),
                 ('master', 'state_updated', '1970-01-01T00:00:00+00:00'),
                 ('master', 'training_updated', '1970-01-01T00:00:00+00:00')]
        for side, key, value in cases:
            with self.subTest(side=side, key=key):
                master, worker = copy.deepcopy(self.master), copy.deepcopy(self.worker)
                (master if side == 'master' else worker)[key] = value
                self.assertFalse(eligible(master, worker, self.expected, 10000000000, self.now))
                calls = []
                observe_once(lambda: (master, worker), lambda: calls.append(1), self.expected,
                             10000000000, wait_seconds=0, clock=lambda: self.now)
                self.assertFalse(calls)

    def test_missing_fields_reject(self):
        for side in ('master', 'worker'):
            original = getattr(self, side)
            for key in original:
                master, worker = copy.deepcopy(self.master), copy.deepcopy(self.worker)
                del (master if side == 'master' else worker)[key]
                with self.subTest(side=side, key=key):
                    self.assertFalse(eligible(master, worker, self.expected, 10000000000, self.now))

    def test_future_or_stale_heartbeat_rejects(self):
        for delta in (-4, 1):
            self.worker['updated'] = datetime.datetime.fromtimestamp(self.now + delta, datetime.timezone.utc).isoformat()
            self.assertFalse(eligible(self.master, self.worker, self.expected, 10000000000, self.now))

    def test_watchdog_nfs_check_timestamp_is_not_worker_heartbeat(self):
        def ago(seconds):
            return datetime.datetime.fromtimestamp(self.now - seconds, datetime.timezone.utc).isoformat()
        self.master['state_updated'] = ago(31)
        self.master['training_updated'] = ago(41)
        self.assertTrue(eligible(self.master, self.worker, self.expected, 10000000000, self.now))
        self.master['state_updated'] = ago(61)
        self.assertFalse(eligible(self.master, self.worker, self.expected, 10000000000, self.now))
        self.master['state_updated'] = ago(31)
        self.master['training_updated'] = ago(91)
        self.assertFalse(eligible(self.master, self.worker, self.expected, 10000000000, self.now))

    def test_busy_then_idle_one_dispatch(self):
        busy = dict(self.worker, active=1)
        samples = iter([(self.master, busy), (self.master, self.worker)])
        calls = []
        observe_once(lambda: next(samples), lambda: calls.append(1), self.expected, 10000000000,
                     wait_seconds=45, clock=lambda: self.now, sleep=lambda _: None)
        self.assertEqual(calls, [1])

    def test_ambiguous_dispatch_never_retries(self):
        calls = []
        def dispatch():
            calls.append(1)
            raise RuntimeError('transport outcome unknown')
        with self.assertRaisesRegex(RuntimeError, 'unknown'):
            observe_once(lambda: (self.master, self.worker), dispatch, self.expected,
                         10000000000, wait_seconds=45, clock=lambda: self.now)
        self.assertEqual(calls, [1])

    def test_readonly_probe_failure_never_dispatches(self):
        calls = []
        def probe():
            raise OSError('NFS read failed')
        with self.assertRaises(OSError):
            observe_once(probe, lambda: calls.append(1), self.expected, 10000000000, wait_seconds=0)
        self.assertFalse(calls)


if __name__ == '__main__':
    unittest.main()
