"""Local-only safety fixtures for an explicitly chosen idle cache-age lane."""
import copy
import datetime
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

import rollout_cache_age as rollout

IDENTITY = {'pid': 1, 'starttime_ticks': '1', 'argv': ['worker']}
VISIBLE = {'identity': IDENTITY, 'record_expected': IDENTITY,
           'active_count': 0, 'payload_fd_count': 0, 'record_status': 'idle_verified',
           'record_operation': 'fixture-operation', 'checkpoint_protocol': 2}
GATE = {'ready': True, 'operation': 'fixture-operation', 'checkpoint_protocol': 2}


class CacheAgeTests(unittest.TestCase):
    def test_exact_owner_ranges_cover_all_stages_and_exclude_other_lane(self):
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.execute('CREATE TABLE claims(stage TEXT,owner TEXT,status TEXT)')
        db.executemany('INSERT INTO claims VALUES(?,?,?)', [
            ('p1-r1', 'host-0', 'leased'), ('p3-r3', 'host-63', 'leased'),
            ('p3-r1', 'host-64', 'leased'), ('p2-r3', 'host-127', 'leased'),
            ('p2-r1', 'host-60', 'done'), ('p3-r3', 'other-host-63', 'leased'),
            ('p1-r1', 'host-128', 'leased')])
        self.assertEqual(rollout.lane_leases(db, 'host', 'primary'), {'p1-r1': 1, 'p3-r3': 1})
        self.assertEqual(rollout.lane_leases(db, 'host', 'supplement'), {'p3-r1': 1, 'p2-r3': 1})
        self.assertEqual(len(rollout.lane_owners('host', 'primary')), 64)
        self.assertEqual(len(rollout.lane_owners('host', 'supplement')), 64)

    def test_idle_gate_rejects_active_stale_ended_or_wrong_pid(self):
        stamp = datetime.datetime.fromtimestamp(100, datetime.timezone.utc).isoformat()
        empty = {'pid': 10, 'updated': stamp, 'active': {}}
        self.assertTrue(rollout.idle_sample(empty, {'pid': 10}, 99, now=101))
        for patch in ({'pid': 11}, {'active': {'owner': {'bytes': 1}}}, {'ended': stamp},
                      {'thread_error': 'bad'}, {'updated': '1970-01-01T00:00:00+00:00'}):
            self.assertFalse(rollout.idle_sample(dict(empty, **patch), {'pid': 10}, 99, now=101))
        self.assertFalse(rollout.idle_sample(empty, {'pid': 10}, 99, now=120))
        self.assertFalse(rollout.same_identity({'pid': 10, 'starttime_ticks': 1, 'argv': ['a']},
                                              {'pid': 10, 'starttime_ticks': 2, 'argv': ['a']}))

    def test_counter_history_budget_independent_check_preserves_nested_stages(self):
        before = {'run_started': 'same', 'bytes': 10, 'streamed_bytes': 20, 'verified_files': 4,
                  'counts': {'copied': 2}, 'stages': {'p3-r3': {'verified_files': 4}}}
        after = dict(copy.deepcopy(before), bytes=11)
        rollout.counter_check(before, after)
        for change in ({'bytes': 9}, {'counts': {}}, {'stages': {}}, {'run_started': 'different'}):
            with self.assertRaises(RuntimeError):
                rollout.counter_check(before, dict(after, **change))

    def backend(self, overrides=None):
        calls = []
        overrides = overrides or {}
        def call(node, action, **kwargs):
            calls.append((node, action, kwargs))
            value = overrides.get(action, {
                'inspect': VISIBLE,
                'drain': {'draining': True}, 'gate': GATE,
                'replace': {'ready': True}, 'release': {'released': True},
                'observe': {'drain_owned_current': True, 'draining': False, 'status': 'complete'}}[action])
            if callable(value):
                return value()
            if isinstance(value, BaseException):
                raise value
            return copy.deepcopy(value)
        return call, calls

    def test_canary_changes_only_one_lane_and_releases_own_drain(self):
        call, calls = self.backend()
        result = rollout.roll_one(call, 'worker-0', 'supplement')
        self.assertEqual(result['status'], 'complete')
        self.assertEqual([c[1] for c in calls], ['inspect', 'drain', 'gate', 'inspect', 'replace', 'release'])
        self.assertEqual([c[0] for c in calls if c[1] == 'replace'], ['worker-0'])
        self.assertTrue(all(c[2]['lane'] == 'supplement' for c in calls))

    def test_busy_lane_timeout_retains_payload_and_other_lane_is_never_replaced(self):
        call, calls = self.backend({'gate': {'ready': False, 'reason': 'active payload'}})
        result = rollout.roll_one(call, 'worker-0', 'primary')
        self.assertEqual(result['status'], 'waiting_payload')
        self.assertNotIn('replace', [c[1] for c in calls])
        self.assertEqual(calls[-1][1], 'release')

    def test_preexisting_drain_is_never_released(self):
        call, calls = self.backend({'drain': {'skipped': True, 'reason': 'preexisting drain'}})
        self.assertEqual(rollout.roll_one(call, 'worker-0', 'primary')['status'], 'skipped')
        self.assertEqual([c[1] for c in calls], ['inspect', 'drain'])

    def test_drain_response_loss_observes_operation_intent_then_restores_only_own_drain(self):
        call, calls = self.backend({'drain': TimeoutError('lost POST response')})
        with self.assertRaises(TimeoutError):
            rollout.roll_one(call, 'worker-0', 'primary')
        self.assertEqual([c[1] for c in calls], ['inspect', 'drain', 'observe', 'release'])
        call, calls = self.backend({'drain': TimeoutError('lost skip response'),
                                   'observe': {'drain_owned_current': False}})
        with self.assertRaises(TimeoutError):
            rollout.roll_one(call, 'worker-0', 'primary')
        self.assertNotIn('release', [c[1] for c in calls])

    def test_replacement_response_loss_adopts_checkpoint_without_repeating_replace(self):
        inspections = iter([VISIBLE, VISIBLE, {'replacement_ready': True, 'identity': {'pid': 2}}])
        call, calls = self.backend({'replace': TimeoutError('lost replacement response'),
                                   'inspect': lambda: next(inspections)})
        result = rollout.roll_one(call, 'worker-0', 'supplement')
        self.assertEqual(result['status'], 'complete')
        self.assertTrue(result['replacement']['adopted_after_rpc_loss'])
        self.assertEqual(sum(c[1] == 'replace' for c in calls), 1)
        self.assertEqual(sum(c[1] == 'release' for c in calls), 1)

    def test_uncertain_replacement_keeps_drain_for_identity_inspection(self):
        call, calls = self.backend({'replace': TimeoutError('unknown')})
        with self.assertRaisesRegex(RuntimeError, 'inspection'):
            rollout.roll_one(call, 'worker-0', 'supplement')
        self.assertEqual(sum(c[1] == 'replace' for c in calls), 1)
        self.assertNotIn('release', [c[1] for c in calls])

    def test_release_response_loss_is_observed_not_blindly_repeated(self):
        call, calls = self.backend({'release': TimeoutError('lost release response')})
        result = rollout.roll_one(call, 'worker-0', 'primary')
        self.assertTrue(result['release']['adopted_after_rpc_loss'])
        self.assertEqual(sum(c[1] == 'release' for c in calls), 1)
        self.assertEqual(calls[-1][1], 'observe')

    def test_node_invisible_stale_or_foreign_checkpoint_never_dispatches_replace(self):
        for changes in ({'record_status': None, 'record_operation': None, 'record_expected': None},
                        {'record_status': 'draining'}, {'record_operation': 'another-operation'},
                        {'checkpoint_protocol': 1}, {'record_expected': None}):
            with self.subTest(changes=changes):
                inspections = iter([VISIBLE, dict(VISIBLE, **changes)])
                call, calls = self.backend({'inspect': lambda: next(inspections)})
                result = rollout.roll_one(call, 'worker-2', 'primary')
                self.assertEqual(result['status'], 'waiting_payload')
                self.assertNotIn('replace', [c[1] for c in calls])
                self.assertEqual(calls[-1][1], 'release')

    def test_identity_payload_or_active_drift_at_visibility_check_releases_without_term(self):
        for changes in ({'identity': dict(IDENTITY, pid=2)},
                        {'identity': dict(IDENTITY, starttime_ticks='2')},
                        {'identity': dict(IDENTITY, argv=['another-worker'])},
                        {'record_expected': dict(IDENTITY, pid=2)},
                        {'active_count': 1}, {'payload_fd_count': 1}):
            with self.subTest(changes=changes):
                inspections = iter([VISIBLE, dict(VISIBLE, **changes)])
                call, calls = self.backend({'inspect': lambda: next(inspections)})
                self.assertEqual(rollout.roll_one(call, 'worker-2', 'primary')['status'], 'waiting_payload')
                self.assertNotIn('replace', [c[1] for c in calls])
                self.assertEqual(calls[-1][1], 'release')

    def test_visibility_rpc_failure_is_read_only_and_releases_our_drain(self):
        def inspect():
            if not hasattr(inspect, 'called'):
                inspect.called = True
                return copy.deepcopy(VISIBLE)
            raise TimeoutError('checkpoint visibility RPC timed out')
        call, calls = self.backend({'inspect': inspect})
        result = rollout.roll_one(call, 'worker-2', 'primary')
        self.assertEqual(result['status'], 'waiting_payload')
        self.assertIn('TimeoutError', result['visibility_error'])
        self.assertNotIn('replace', [c[1] for c in calls])
        self.assertEqual(calls[-1][1], 'release')

    def test_old_gate_protocol_and_incomplete_identity_fail_closed(self):
        call, calls = self.backend({'gate': {'ready': True}})
        self.assertEqual(rollout.roll_one(call, 'worker-2', 'primary')['status'], 'waiting_payload')
        self.assertNotIn('replace', [c[1] for c in calls])
        for bad in ({'identity': {'pid': 1}}, {'identity': None}, None):
            self.assertFalse(rollout.visible_idle_checkpoint(VISIBLE, GATE, bad))

    def test_old_node_helper_protocol_is_rejected_before_any_drain(self):
        call, calls = self.backend({'inspect': {'identity': IDENTITY}})
        self.assertEqual(rollout.roll_one(call, 'worker-2', 'primary')['status'], 'skipped')
        self.assertEqual([c[1] for c in calls], ['inspect'])

    def test_missing_remote_checkpoint_raises_before_term_or_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory).resolve()
            run_id = 'cache-age-fixture-' + uuid.uuid4().hex
            cfg = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                   'run_id': run_id}
            a = SimpleNamespace(control=str(control), host='fixture-host', lane='primary', action='replace',
                                operation='fixture', max_age=30, wrapper=None, gate_timeout=5)
            def read(path):
                if Path(path).name == 'config.json': return cfg
                raise FileNotFoundError('NFS checkpoint not visible on worker')
            try:
                with mock.patch.object(rollout, 'read_json', side_effect=read), \
                        mock.patch.object(rollout.socket, 'gethostname', lambda: 'fixture-host'), \
                        mock.patch.object(rollout.os, 'kill') as kill, \
                        mock.patch.object(rollout.subprocess, 'Popen') as launch:
                    with self.assertRaises(FileNotFoundError): rollout.remote(a)
                    kill.assert_not_called(); launch.assert_not_called()
            finally:
                Path('/tmp/' + run_id + '-primary-cache-age-replace.lock').unlink(missing_ok=True)

    def test_remote_stop_guard_prevents_term_and_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory).resolve()
            (control / 'STOP').write_text('fixture stop\n')
            run_id = 'cache-age-fixture-' + uuid.uuid4().hex
            cfg = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                   'run_id': run_id}
            a = SimpleNamespace(control=str(control), host='fixture-host', lane='primary', action='replace',
                                operation='fixture', max_age=30, wrapper=None, gate_timeout=5)
            record = {'operation': 'fixture', 'status': 'idle_verified', 'lane_leases': {}}
            def read(path):
                return cfg if Path(path).name == 'config.json' else record
            try:
                with mock.patch.object(rollout, 'read_json', side_effect=read), \
                        mock.patch.object(rollout.socket, 'gethostname', lambda: 'fixture-host'), \
                        mock.patch.object(rollout.os, 'kill') as kill, \
                        mock.patch.object(rollout.subprocess, 'Popen') as launch:
                    with self.assertRaisesRegex(RuntimeError, 'STOP'):
                        rollout.remote(a)
                    kill.assert_not_called()
                    launch.assert_not_called()
            finally:
                Path('/tmp/' + run_id + '-primary-cache-age-replace.lock').unlink(missing_ok=True)

    def test_master_gate_accepts_two_fresh_empty_samples_while_other_lane_keeps_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory).resolve()
            run_id = 'cache-age-fixture-' + uuid.uuid4().hex
            db_path = Path('/tmp') / (run_id + '-broker.sqlite')
            with sqlite3.connect(db_path) as db:
                db.execute('CREATE TABLE claims(stage TEXT,owner TEXT,status TEXT)')
                db.execute("INSERT INTO claims VALUES ('p3-r3','fixture-host-64','leased')")
            cfg = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                   'run_id': run_id, 'broker_url': 'http://fixture', 'broker_token': 'fixture-only'}
            a = SimpleNamespace(control=str(control), host='fixture-host', lane='primary', action='gate',
                                operation='fixture', max_age=30, wrapper=None, gate_timeout=5)
            now = time.time()
            record = {'operation': 'fixture', 'expected': {'pid': 10}, 'barrier': now - 5}
            samples = iter([{'pid': 10, 'updated': datetime.datetime.fromtimestamp(now - seconds,
                                datetime.timezone.utc).isoformat(), 'active': {}} for seconds in (2, 1)])
            def read(path):
                path = Path(path)
                if path.name == 'config.json':
                    return cfg
                if path.name == 'state.json':
                    return {'status': 'copying'}
                if path.parent.name == 'nodes':
                    return next(samples)
                return record
            opener = SimpleNamespace(open=lambda *args, **kw: io.StringIO(json.dumps(
                {'draining_hosts': ['fixture-host'], 'leased_by_host': {'fixture-host': 1}})))
            saved = []
            try:
                with mock.patch.object(rollout, 'read_json', side_effect=read), \
                        mock.patch.object(rollout, 'write_json', side_effect=lambda path, value: saved.append(dict(value))), \
                        mock.patch.object(rollout.urllib.request, 'build_opener', lambda *args: opener), \
                        mock.patch.object(rollout.time, 'sleep', lambda _s: None):
                    rollout.remote(a)
                self.assertEqual(saved[-1]['status'], 'idle_verified')
                self.assertEqual(saved[-1]['samples'], 2)
                self.assertEqual(saved[-1]['lane_leases'], {})
                with sqlite3.connect(db_path) as db:
                    self.assertEqual(rollout.lane_leases(db, 'fixture-host', 'supplement'), {'p3-r3': 1})
            finally:
                db_path.unlink(missing_ok=True)

    def test_payload_descriptor_prevents_term_even_with_empty_heartbeat_and_zero_lane_leases(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory).resolve()
            run_id = 'cache-age-fixture-' + uuid.uuid4().hex
            cfg = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                   'run_id': run_id, 'broker_url': 'http://fixture', 'broker_token': 'fixture-only'}
            identity = {'pid': 10, 'state': 'S', 'starttime_ticks': '2',
                        'argv': ['python3', str(control / 'migration_cached_worker.py'), 'primary',
                                 '--control', str(control)]}
            now = time.time()
            record = {'operation': 'fixture', 'expected': identity, 'lane_leases': {},
                      'status': 'idle_verified', 'barrier': now - 5}
            snapshot = {'pid': 10, 'updated': datetime.datetime.fromtimestamp(now - 1,
                              datetime.timezone.utc).isoformat(), 'active': {}}
            a = SimpleNamespace(control=str(control), host='fixture-host', lane='primary', action='replace',
                                operation='fixture', max_age=30, wrapper=None, gate_timeout=5)
            def read(path):
                path = Path(path)
                if path.name == 'config.json':
                    return cfg
                if path.name == 'state.json':
                    return {'status': 'copying'}
                if path.parent.name == 'nodes':
                    return snapshot
                return record
            opener = SimpleNamespace(open=lambda *args, **kw: io.StringIO(json.dumps(
                {'draining_hosts': ['fixture-host']})))
            try:
                with mock.patch.object(rollout, 'read_json', side_effect=read), \
                        mock.patch.object(rollout.socket, 'gethostname', lambda: 'fixture-host'), \
                        mock.patch.object(rollout.urllib.request, 'build_opener', lambda *args: opener), \
                        mock.patch.object(rollout, 'proc_identity', lambda _pid: identity), \
                        mock.patch.object(rollout, 'payload_fds', lambda *args: [{'fd': '33'}]), \
                        mock.patch.object(rollout.os, 'kill') as kill, \
                        mock.patch.object(rollout.subprocess, 'Popen') as launch:
                    with self.assertRaisesRegex(RuntimeError, 'payload descriptors'):
                        rollout.remote(a)
                    kill.assert_not_called()
                    launch.assert_not_called()
            finally:
                Path('/tmp/' + run_id + '-primary-cache-age-replace.lock').unlink(missing_ok=True)


if __name__ == '__main__':
    unittest.main()
