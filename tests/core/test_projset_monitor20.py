import copy
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import types
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('projset_monitor20', ROOT / 'ops/diagnostics/projset_monitor20.py')
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


class Monitor20Tests(unittest.TestCase):
    def test_historical_scenarios_use_distinct_scope_and_current_rates(self):
        reference = {'schema': 1, 'kind': 'historical_unique_index_approximation',
                     'excluded': 'eai_pwm/datasets/Sekai', 'directories': 1000,
                     'files_links': 10000, 'created': 1}
        audit = {'state': 'running', 'started': 800, 'updated': 2200, 'done_dirs': 200,
                 'counts': {'files': 100, 'symlinks': 30}}
        rates = {'audit_updated_seconds': 1200, 'audit_directories_per_hour': 100,
                 'audit_files_links_per_hour': 1800}
        result = monitor.historical_scenarios(audit, rates, reference, 2220)
        self.assertTrue(result['available'])
        self.assertTrue(result['sample_valid'])
        self.assertEqual(result['directories_reference'], 1000)
        self.assertEqual(result['files_links_reference'], 10000)
        self.assertEqual(result['directories_remaining_hours'], 8)
        self.assertAlmostEqual(result['files_links_remaining_hours'], 9870 / 1800)
        self.assertIn('not a current source inventory', result['note'])

    def test_historical_scenarios_reject_stale_zero_or_exhausted_inputs(self):
        reference = {'schema': 1, 'kind': 'historical_unique_index_approximation',
                     'excluded': 'eai_pwm/datasets/Sekai', 'directories': 1000, 'files_links': 10000}
        audit = {'state': 'running', 'started': 800, 'updated': 2200, 'done_dirs': 1000,
                 'counts': {'files': 100, 'symlinks': 0}}
        rates = {'audit_updated_seconds': 1200, 'audit_directories_per_hour': 100,
                 'audit_files_links_per_hour': 0}
        result = monitor.historical_scenarios(audit, rates, reference, 2220)
        self.assertTrue(result['sample_valid'])
        self.assertNotIn('directories_remaining_hours', result)
        self.assertNotIn('files_links_remaining_hours', result)
        self.assertFalse(monitor.historical_scenarios(audit, rates, reference, 2600)['sample_valid'])
        audit['started'] = 2000
        self.assertFalse(monitor.historical_scenarios(audit, rates, reference, 2220)['sample_valid'])
        self.assertFalse(monitor.historical_scenarios(audit, rates, {}, 2220)['available'])
        reference['directories'] = True
        self.assertFalse(monitor.historical_scenarios(audit, rates, reference, 2220)['available'])

    def test_directory_cursors_filter_paths_and_collapse_duplicate_descriptors(self):
        with tempfile.TemporaryDirectory() as temp:
            proc = Path(temp) / '123'
            (proc / 'fd').mkdir(parents=True)
            (proc / 'fdinfo').mkdir()
            (proc / 'stat').write_text('123 (worker) S ' + '0 ' * 18 + '456 0\n')
            for fd, target, flags, position in (
                    ('3', '/share-mixed/projset/one', os.O_DIRECTORY, 20),
                    ('4', '/share-mixed/projset/one', os.O_DIRECTORY, 20),
                    ('5', '/share-mixed/projset/one', os.O_DIRECTORY, 30),
                    ('6', '/share-mixed/projset/file', 0, 40),
                    ('7', '/share-mixed/project/target', os.O_DIRECTORY, 50),
                    ('8', '/share-mixed/projset', os.O_DIRECTORY, 60)):
                (proc / 'fd' / fd).symlink_to(target)
                (proc / 'fdinfo' / fd).write_text(f'pos:\t{position}\nflags:\t{flags:o}\n')
            result = monitor.sample_source_cursors(123, Path(temp))
            digest = hashlib.sha256(b'/share-mixed/projset/one').hexdigest()
            self.assertEqual(result, {'pid': 123, 'start_ticks': 456, 'complete': True,
                                      'directories': {digest: [20, 30]}})
            (proc / 'fdinfo' / '3').unlink()
            self.assertFalse(monitor.sample_source_cursors(123, Path(temp))['complete'])
            self.assertFalse(monitor.sample_source_cursors(999, Path(temp))['complete'])

    def test_cursor_activity_never_calls_unobserved_nodes_hung(self):
        def cursor(pid=1, start=5, dirs=None, complete=True):
            return {'pid': pid, 'start_ticks': start, 'complete': complete,
                    'directories': {'x': [10]} if dirs is None else dirs}
        old = {node: cursor() for node in ('cookie', 'opened', 'done', 'same',
                                           'restart', 'pid_reused', 'reset', 'incomplete', 'empty')}
        current = {
            'cookie': cursor(dirs={'x': [9]}), # NFS cookies are not monotonic.
            'opened': cursor(dirs={'y': [10]}),
            'done': cursor(complete=False),
            'same': cursor(),
            'restart': cursor(pid=2),
            'pid_reused': cursor(start=6),
            'reset': cursor(),
            'incomplete': cursor(complete=False),
            'empty': cursor(dirs={}),
            'missing': None,
        }
        old['empty'] = cursor(dirs={})
        previous_counts = {node: {'done_dirs': 10} for node in old}
        counts = {node: {'done_dirs': 10} for node in current}
        counts['done']['done_dirs'] = 11
        counts['reset']['done_dirs'] = 9
        result = monitor.cursor_observations(current, old, counts, previous_counts)
        self.assertEqual((result['observed'], result['not_observed'], result['not_comparable']), (3, 1, 6))
        self.assertEqual(result['nodes']['cookie']['reasons'], ['directory_cookie_changed'])
        self.assertEqual(result['nodes']['opened']['reasons'], ['open_directories_changed'])
        self.assertEqual(result['nodes']['done']['reasons'], ['completed_directories'])
        self.assertEqual(result['nodes']['same']['status'], 'not_observed')

    def test_mountstats_parses_only_requested_nfs_mounts_and_rpc_counters(self):
        text = '''device server:/source mounted on /share-mixed/projset with fstype nfs statvers=1.1
\tage: 1000
\tper-op statistics
\tWRITE: 100 101 0 100 20 200 4000 4200 0
\tREADDIRPLUS: 20 20 0 10 20 1 40 41 0
device server:/target mounted on /share-mixed/project with fstype nfs statvers=1.1
\tage: 1001
\tWRITE: 40 40 0 0 0 12 80 92 0
device /dev/sda mounted on /other with fstype ext4
\tWRITE: 999 999 0 0 0 0 0 0 0
'''
        mounts = monitor.parse_nfs_mountstats(text, (('source', '/share-mixed/projset'),
                                                       ('target', '/share-mixed/project')))
        self.assertEqual(mounts['source']['device'], 'server:/source')
        self.assertEqual(mounts['source']['age_seconds'], 1000)
        self.assertEqual(mounts['source']['ops']['WRITE']['requests'], 100)
        self.assertEqual(mounts['source']['ops']['WRITE']['rtt_ms'], 4000)
        self.assertEqual(mounts['target']['ops']['WRITE']['execution_ms'], 92)
        self.assertEqual(set(mounts), {'source', 'target'})

    def test_nfs_interval_uses_counter_deltas_and_discards_remounts(self):
        def sample(at, age, requests, rtt, execution, device='server:/target'):
            return {'at': at, 'mounts': {'target': {'device': device, 'age_seconds': age,
                    'ops': {'WRITE': {'requests': requests, 'transmissions': requests,
                                      'queue_ms': 10, 'rtt_ms': rtt, 'execution_ms': execution}}}}}
        first = sample(100, 1000, 100, 4000, 4100)
        last = sample(120, 1020, 110, 9000, 9300)
        rate = monitor.nfs_rates(last, first)['target']['WRITE']
        self.assertEqual(rate['requests'], 10)
        self.assertEqual(rate['requests_per_second'], .5)
        self.assertEqual(rate['mean_rtt_ms'], 500)
        self.assertEqual(rate['mean_execution_ms'], 520)
        self.assertEqual(monitor.nfs_rates(sample(120, 5, 110, 9000, 9300), first), {})
        self.assertEqual(monitor.nfs_rates(sample(120, 1020, 110, 9000, 9300, 'other'), first), {})
        self.assertEqual(monitor.nfs_rates(sample(120, 1020, 99, 9000, 9300), first), {})
        self.assertEqual(monitor.nfs_rates(last, {}), {})

    def test_rates_distinguish_logical_scan_from_actual_repair(self):
        audit = {'updated': 220, 'counts': {'files': 120, 'symlinks': 10,
                 'file_bytes': 3_000_000_000_000, 'entries': 170}, 'done_dirs': 25}
        repair = {'updated': 220, 'copied_operations': 6, 'copied_bytes': 600_000_000_000}
        previous = {'audit_updated': 100, 'files_links': 70, 'logical_bytes': 1_000_000_000_000,
                    'entries': 110, 'done_dirs': 20, 'repair_updated': 100,
                    'copied_operations': 2, 'copied_bytes': 200_000_000_000}
        original = {'counts': {'processed': 1_030, 'copied_bytes': 6_000_000_000_000}}
        result = monitor.summarize(audit, repair, previous, original)
        self.assertEqual(result['rates']['audit_files_links_per_hour'], 1800)
        self.assertEqual(result['rates']['audit_logical_TB_per_hour'], 60)
        self.assertEqual(result['rates']['repair_operations_per_hour'], 120)
        self.assertAlmostEqual(result['rates']['repair_written_TB_per_hour'], 12)
        self.assertEqual(result['estimates']['files_hours'], .5)
        self.assertAlmostEqual(result['estimates']['logical_TB_hours'], .05)

    def test_reset_or_exhausted_baseline_never_fakes_a_finish(self):
        audit = {'updated': 200, 'counts': {'files': 2, 'file_bytes': 20}}
        previous = {'audit_updated': 100, 'files_links': 100, 'logical_bytes': 100}
        result = monitor.summarize(audit, {}, previous, {'counts': {'processed': 1}})
        self.assertNotIn('audit_files_links_per_hour', result['rates'])
        self.assertNotIn('files_hours', result['estimates'])

    def test_per_node_rates_use_their_own_sample_times_and_separate_repair(self):
        audit = {'updated': 220, 'pending_dirs': 12, 'active_dirs': 4, 'retry_dirs': 2,
                 'node_counts': {
                     'n1': {'files': 120, 'symlinks': 10, 'file_bytes': 3_000_000_000_000,
                            'entries': 170, 'done_dirs': 25},
                     'new': {'files': 20, 'file_bytes': 100}}}
        repair = {'updated': 220, 'pending': 3, 'active': 1, 'retry': 2,
                  'nodes': {'n1': {'done': 8, 'copied_operations': 6,
                                   'copied_bytes': 600_000_000_000}}}
        previous = {'audit_nodes_updated': 100, 'repair_nodes_updated': 160,
                    'audit_nodes': {'n1': {'files_links': 70, 'logical_bytes': 1_000_000_000_000,
                                          'entries': 110, 'done_dirs': 20}},
                    'repair_nodes': {'n1': {'done': 3, 'copied_operations': 2,
                                            'copied_bytes': 200_000_000_000}}}
        result = monitor.summarize(audit, repair, previous, {})
        self.assertEqual(result['sample']['pending_dirs'], 12)
        self.assertEqual(result['sample']['repair_retry'], 2)
        audit_rates = result['node_rates']['audit']['nodes']
        self.assertEqual(audit_rates['n1']['files_links_per_hour'], 1800)
        self.assertEqual(audit_rates['n1']['logical_TB_per_hour'], 60)
        self.assertEqual(audit_rates['n1']['directories_per_hour'], 150)
        self.assertNotIn('new', audit_rates)
        repair_rates = result['node_rates']['repair']['nodes']['n1']
        self.assertEqual(repair_rates['paths_per_hour'], 300)
        self.assertEqual(repair_rates['copied_operations_per_hour'], 240)
        self.assertAlmostEqual(repair_rates['copied_TB_per_hour'], 24)

    def test_per_node_reset_and_missing_baseline_do_not_create_false_rates(self):
        audit = {'updated': 200, 'node_counts': {'n1': {'files': 5, 'file_bytes': 10}}}
        previous = {'audit_nodes_updated': 100,
                    'audit_nodes': {'n1': {'files_links': 10, 'logical_bytes': 20}}}
        result = monitor.summarize(audit, {}, previous, {})
        rates = result['node_rates']['audit']['nodes']['n1']
        self.assertNotIn('files_links_per_hour', rates)
        self.assertNotIn('logical_TB_per_hour', rates)
        old_format = monitor.summarize(audit, {}, {'audit_updated': 100}, {})
        self.assertNotIn('audit', old_format['node_rates'])

    def test_stalled_restart_refuses_fresh_heartbeat_and_wrong_process(self):
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp) / 'audit'; audit.mkdir()
            repair = audit / 'repair'; repair.mkdir()
            status = audit / 'status.json'
            fresh = time.time()
            status.write_text(json.dumps({'state': 'running', 'controller_pid': os.getpid(), 'updated': fresh}))
            with mock.patch.object(monitor.os, 'kill') as kill:
                with self.assertRaises(ValueError):
                    monitor.stop_stalled(audit, repair, 'audit', os.getpid(), fresh)
                kill.assert_not_called()
            stale = time.time() - 600
            status.write_text(json.dumps({'state': 'running', 'controller_pid': os.getpid(), 'updated': stale}))
            with mock.patch.object(monitor.os, 'kill') as kill:
                with self.assertRaises(ValueError):
                    monitor.stop_stalled(audit, repair, 'audit', os.getpid(), stale)
                kill.assert_not_called()

    def test_stalled_restart_targets_only_verified_interruptible_controller(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            audit = root / 'audit'; audit.mkdir()
            repair = audit / 'repair'; repair.mkdir()
            fake_proc = root / 'proc'; fake_pid = 12345
            proc = fake_proc / str(fake_pid); proc.mkdir(parents=True)
            code = audit / 'code/projset_full_audit.py'
            (proc / 'cmdline').write_bytes(b'python3\0' + os.fsencode(code) + b'\0controller\0--work\0' +
                                           os.fsencode(audit) + b'\0')
            (proc / 'stat').write_text(f'{fake_pid} (python3) S 1\n')
            stale = time.time() - 600
            (audit / 'status.json').write_text(json.dumps({'state': 'running',
                'controller_pid': fake_pid, 'updated': stale}))

            def path_for(value):
                return fake_proc if value == '/proc' else Path(value)

            with mock.patch.object(monitor, 'Path', side_effect=path_for), \
                    mock.patch.object(monitor.os, 'kill') as kill:
                result = monitor.stop_stalled(audit, repair, 'audit', fake_pid, stale)
                self.assertEqual(result['signal'], 'SIGTERM')
                kill.assert_called_once_with(fake_pid, monitor.signal.SIGTERM)
                kill.reset_mock()
                (proc / 'stat').write_text(f'{fake_pid} (python3) D 1\n')
                with self.assertRaises(ValueError):
                    monitor.stop_stalled(audit, repair, 'audit', fake_pid, stale)
                kill.assert_not_called()

    def test_stalled_restart_respects_stop_fence_before_and_during_check(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            audit = root / 'audit'; audit.mkdir()
            repair = audit / 'repair'; repair.mkdir()
            fake_proc = root / 'proc'; fake_pid = 12345
            proc = fake_proc / str(fake_pid); proc.mkdir(parents=True)
            stale = time.time() - 600

            def path_for(value):
                return fake_proc if value == '/proc' else Path(value)

            for component, work, code_name in (
                    ('audit', audit, 'code/projset_full_audit.py'),
                    ('repair', repair, 'code/projset_audit_repair.py')):
                with self.subTest(component=component):
                    code = work / code_name
                    (proc / 'cmdline').write_bytes(b'python3\0' + os.fsencode(code) + b'\0controller\0--work\0' +
                                                   os.fsencode(work) + b'\0')
                    (proc / 'stat').write_text(f'{fake_pid} (python3) S 1\n')
                    (work / 'status.json').write_text(json.dumps({'state': 'running',
                        'controller_pid': fake_pid, 'updated': stale}))
                    (work / 'STOP').touch()
                    with mock.patch.object(monitor, 'Path', side_effect=path_for), \
                            mock.patch.object(monitor.os, 'kill') as kill:
                        with self.assertRaisesRegex(ValueError, 'deliberately stopping'):
                            monitor.stop_stalled(audit, repair, component, fake_pid, stale)
                        kill.assert_not_called()
                    (work / 'STOP').unlink()
                    with mock.patch.object(monitor, 'Path', side_effect=path_for), \
                            mock.patch.object(Path, 'exists', side_effect=[False, True]), \
                            mock.patch.object(monitor.os, 'kill') as kill:
                        with self.assertRaisesRegex(ValueError, 'deliberately stopping'):
                            monitor.stop_stalled(audit, repair, component, fake_pid, stale)
                        kill.assert_not_called()

    def test_reporter_restart_requires_stale_exact_owner_and_verified_standby(self):
        now = time.time()
        state = {'state': 'streaming', 'updated': now - 800, 'enqueued_at': now - 801}
        owner = {'host': 'host-a', 'pid': 123, 'start_ticks': 456}
        topology = {'hosts': {'cpu-master-0': 'host-a', 'cpu-worker-0': 'host-b'}}
        probes = {
            'cpu-master-0': {'identity_ok': True, 'reporter': {'guardians': [{'pid': 3}],
                'wrappers': [{'pid': 123, 'state': 'S', 'start_ticks': 456}]}},
            'cpu-worker-0': {'identity_ok': True, 'reporter': {'guardians': [{'pid': 4}],
                'wrappers': [{'pid': 789, 'state': 'S', 'start_ticks': 42}]}}}
        candidate = monitor.reporter_restart_candidate(state, owner, probes, topology, now)
        self.assertEqual(candidate['node'], 'cpu-master-0')
        self.assertEqual(candidate['pid'], 123)
        for change in ('fresh', 'recent_enqueue', 'new_pid', 'wrong_start', 'uninterruptible',
                       'no_standby', 'unverified_standby', 'finished'):
            s, o, p = copy.deepcopy(state), copy.deepcopy(owner), copy.deepcopy(probes)
            if change == 'fresh': s['updated'] = now - 1
            elif change == 'recent_enqueue': s['enqueued_at'] = now - 1
            elif change == 'new_pid': o['pid'] = 124
            elif change == 'wrong_start': o['start_ticks'] = 457
            elif change == 'uninterruptible': p['cpu-master-0']['reporter']['wrappers'][0]['state'] = 'D'
            elif change == 'no_standby': p['cpu-worker-0']['reporter']['wrappers'] = []
            elif change == 'unverified_standby': p['cpu-worker-0']['identity_ok'] = False
            elif change == 'finished': s['state'] = 'finished'
            with self.subTest(change=change):
                self.assertIsNone(monitor.reporter_restart_candidate(s, o, p, topology, now))

    def test_outbox_probe_distinguishes_backlog_from_lock_and_caps_count(self):
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp)
            db = sqlite3.connect(audit / 'audit.sqlite3')
            db.execute('create table repair_outbox(id integer primary key, rel text, data text)')
            db.executemany('insert into repair_outbox values(?,?,?)', ((i, '', '') for i in range(1, 10004)))
            db.commit()
            first = monitor.probe_outbox(audit, 0, timeout=.05)
            self.assertEqual((first['state'], first['max_id'], first['pending_events_capped']), ('ok', 10003, 10001))
            last = monitor.probe_outbox(audit, 10002, timeout=.05)
            self.assertEqual(last['pending_events_capped'], 1)
            db.execute('begin exclusive')
            blocked = monitor.probe_outbox(audit, 0, timeout=.05)
            self.assertEqual(blocked['state'], 'busy')
            self.assertNotIn('pending_events_capped', blocked)
            db.rollback(); db.close()
            with self.assertRaises(ValueError):
                monitor.probe_outbox(audit, -1)

    def test_tracked_leases_preserve_original_attempts_and_hide_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp); work = audit / 'monitor20'; work.mkdir()
            (work / 'long-lease-watch.json').write_text(json.dumps({'schema': 1, 'tracked': [
                {'id': 1, 'attempt': 2}, {'id': 2, 'attempt': 2},
                {'id': 3, 'attempt': 2}, {'id': 4, 'attempt': 2}]}))
            db = sqlite3.connect(audit / 'audit.sqlite3')
            db.execute('create table repair_outbox(id integer primary key, rel text, data text)')
            db.execute('create table dirs(id integer primary key,rel text,state integer,node text,attempt integer,updated real)')
            db.executemany('insert into dirs values(?,?,?,?,?,?)', [
                (1, json.dumps('private/long'), 1, 'n1', 2, 100),
                (2, json.dumps('private/done'), 2, None, 2, 110),
                (3, json.dumps('private/retried'), 1, 'n2', 3, 120)])
            db.commit(); db.close()
            result = monitor.probe_outbox(audit, 0)
            reading = result['long_leases']
            self.assertEqual(reading['state'], 'ok')
            self.assertNotIn('private/', json.dumps(reading))
            key = hashlib.sha256(os.fsencode('/share-mixed/projset/private/long')).hexdigest()
            before = {'n1': {'pid': 8, 'start_ticks': 9, 'complete': True, 'directories': {key: [10]}}}
            after = {'n1': {'pid': 8, 'start_ticks': 9, 'complete': True, 'directories': {key: [20]}}}
            watched = monitor.classify_tracked_leases(reading, after, before, 220)
            self.assertEqual((watched['active_original'], watched['completed_original'],
                              watched['other_attempt_or_state'], watched['unknown']), (1, 1, 1, 1))
            self.assertTrue(watched['items'][0]['cursor_change_observed'])
            self.assertEqual(watched['items'][0]['lease_age_seconds'], 120)
            self.assertNotIn('private/', json.dumps(watched))
            self.assertNotIn('path_digest', json.dumps(watched))
            after['n1']['start_ticks'] = 10
            self.assertNotIn('cursor_change_observed',
                             monitor.classify_tracked_leases(reading, after, before, 220)['items'][0])

    def test_invalid_tracked_reference_does_not_change_outbox_result(self):
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp); work = audit / 'monitor20'; work.mkdir()
            db = sqlite3.connect(audit / 'audit.sqlite3')
            db.execute('create table repair_outbox(id integer primary key,rel text,data text)')
            db.commit(); db.close()
            (work / 'long-lease-watch.json').write_text(json.dumps({'schema': 1, 'tracked': [
                {'id': 1, 'attempt': 2}, {'id': 1, 'attempt': 2}]}))
            result = monitor.probe_outbox(audit, 0)
            self.assertEqual(result['state'], 'ok')
            self.assertEqual(result['pending_events_capped'], 0)
            self.assertEqual(result['long_leases']['state'], 'invalid_reference')
            (work / 'long-lease-watch.json').write_text(json.dumps(
                {'schema': 1, 'tracked': [{'id': 1, 'attempt': 2}]}))
            unavailable = monitor.probe_outbox(audit, 0)
            self.assertEqual(unavailable['state'], 'ok')
            self.assertEqual(unavailable['long_leases']['state'], 'unavailable')
            self.assertEqual(monitor.classify_tracked_leases({'state': 'ok', 'items': [None]}, {}, {}, 1)['state'],
                             'invalid_reading')

    def test_bounded_outbox_probe_does_not_wait_for_stuck_reader(self):
        process = mock.Mock()
        process.communicate.side_effect = subprocess.TimeoutExpired('probe', 8)
        with mock.patch.object(monitor.subprocess, 'Popen', return_value=process):
            result = monitor.bounded_outbox_probe(Path('/run'), Path('/work'), 12)
        self.assertEqual(result['state'], 'timeout')
        process.kill.assert_called_once()

    def test_bounded_outbox_probe_runs_real_isolated_reader(self):
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp) / 'audit'; work = audit / 'monitor20'
            (work / 'code').mkdir(parents=True)
            shutil.copyfile(ROOT / 'ops/diagnostics/projset_monitor20.py', work / 'code/projset_monitor20.py')
            db = sqlite3.connect(audit / 'audit.sqlite3')
            db.execute('create table repair_outbox(id integer primary key, rel text, data text)')
            db.execute("insert into repair_outbox values(1,'path','{}')")
            db.commit()
            self.assertEqual(monitor.bounded_outbox_probe(Path('/run'), work, 0)['pending_events_capped'], 1)
            db.execute('begin exclusive')
            try:
                self.assertEqual(monitor.bounded_outbox_probe(Path('/run'), work, 0)['state'], 'busy')
            finally:
                db.rollback(); db.close()

    def test_outbox_backlog_alert_requires_two_comparable_stalled_samples(self):
        previous = {'state': 'ok', 'cursor': 10, 'max_id': 12, 'pending_events_capped': 2}
        self.assertEqual(monitor.outbox_probe_problem(previous, previous),
                         'outbox_not_advancing_since_previous_sample')
        advanced = dict(previous, cursor=11, pending_events_capped=1)
        self.assertIsNone(monitor.outbox_probe_problem(advanced, previous))
        self.assertIsNone(monitor.outbox_probe_problem({'state': 'busy'}, previous))
        self.assertEqual(monitor.outbox_probe_problem({'state': 'timeout'}, previous), 'outbox_probe_timeout')
        self.assertEqual(monitor.outbox_probe_problem(dict(previous, max_id=9), previous),
                         'outbox_cursor_ahead_of_source')

    def test_swanlab_cloud_probe_reads_heartbeat_and_sanitizes_errors(self):
        class Run:
            def summary(self):
                return {'nodes/在线': {'step': 100, 'value': 16}}
        class Api:
            def run(self, path):
                self.path = path
                return Run()
        result = monitor.probe_swanlab_cloud('account/project/run123', api_factory=Api)
        self.assertEqual((result['state'], result['step'], result['online']), ('ok', 100, 16))
        self.assertEqual(monitor.probe_swanlab_cloud('bad/path/extra/segment', api_factory=Api)['state'],
                         'invalid_run_path')
        def failed():
            raise RuntimeError('sensitive error detail')
        error = monitor.probe_swanlab_cloud('account/project/run123', api_factory=failed)
        self.assertEqual(error['error_type'], 'RuntimeError')
        self.assertNotIn('sensitive error detail', json.dumps(error))

    def test_swanlab_cloud_lag_alert_is_separate_from_reporter_restart(self):
        local = {'state': 'streaming', 'step': 104}
        self.assertIsNone(monitor.cloud_probe_problem({'state': 'ok', 'step': 103}, local))
        self.assertEqual(monitor.cloud_probe_problem({'state': 'ok', 'step': 100}, local),
                         'swanlab_cloud_lag_at_least_four_steps')
        self.assertEqual(monitor.cloud_probe_problem({'state': 'timeout'}, local),
                         'swanlab_cloud_probe_unavailable')
        process = mock.Mock()
        process.communicate.side_effect = subprocess.TimeoutExpired('cloud-probe', 20)
        with mock.patch.object(monitor.subprocess, 'Popen', return_value=process), \
                mock.patch.object(monitor.os, 'kill') as kill:
            result = monitor.bounded_cloud_probe(Path('/run'), Path('/work'), '/python', 'a/b/c')
            self.assertEqual(result['state'], 'timeout')
            process.kill.assert_called_once()
            kill.assert_not_called()

    def test_cloud_probe_exposes_progress_and_speed_and_rejects_bad_values(self):
        class Run:
            def summary(self):
                return {'nodes/在线': {'step': 104, 'value': 16},
                        'audit/已核对文件及链接': {'step': 104, 'value': 123},
                        'audit/已核对文件_TB': {'step': 104, 'value': 1.25},
                        'repair/补传操作累计': {'step': 104, 'value': -1},
                        'speed20/复核文件及链接_项每小时': {'step': 104, 'value': 12.5}}
        class Api:
            def run(self, path): return Run()
        result = monitor.probe_swanlab_cloud('account/project/run123', api_factory=Api)
        self.assertEqual(result['metrics']['audit/已核对文件及链接'], {'step': 104, 'value': 123})
        self.assertEqual(result['metrics']['speed20/复核文件及链接_项每小时'], {'step': 104, 'value': 12.5})
        self.assertIsNone(result['metrics']['repair/补传操作累计'])
        self.assertIsNone(result['metrics']['repair/补传_TB'])

    def test_cloud_progress_comparison_handles_lag_and_uncommitted_samples(self):
        local = {'state': 'streaming', 'step': 104, 'last_sample': {'files_links': 123,
                 'file_bytes': 1_250_000_000_000, 'repair_time': 1000,
                 'copied_operations': 20, 'copied_bytes': 4_000_000_000}}
        metric_values = {'audit/已核对文件及链接': 123, 'audit/已核对文件_TB': 1.25,
                         'repair/补传操作累计': 20, 'repair/补传_TB': 0.004}
        probe = {'state': 'ok', 'step': 104,
                 'metrics': {key: {'step': 104, 'value': value} for key, value in metric_values.items()}}
        self.assertEqual(monitor.cloud_progress_problems(probe, local), [])
        mismatch = copy.deepcopy(probe)
        mismatch['metrics']['audit/已核对文件_TB']['value'] = 1.20
        self.assertEqual(monitor.cloud_progress_problems(mismatch, local)[0]['problem'],
                         'cloud_progress_value_mismatch')
        sending = dict(local, state='sending')
        self.assertEqual(monitor.cloud_progress_problems(mismatch, sending), [])
        behind = copy.deepcopy(probe)
        behind['metrics']['repair/补传_TB']['step'] = 100
        self.assertEqual(monitor.cloud_progress_problems(behind, local)[0]['problem'],
                         'cloud_progress_metric_lag')
        missing = copy.deepcopy(probe)
        missing['metrics']['audit/已核对文件及链接'] = None
        self.assertEqual(monitor.cloud_progress_problems(missing, local)[0]['problem'],
                         'cloud_progress_metric_unavailable')
        stale_local = dict(local, step=105)
        self.assertEqual(monitor.cloud_progress_problems(mismatch, stale_local), [])
        no_repair = copy.deepcopy(missing)
        no_repair['metrics']['audit/已核对文件及链接'] = probe['metrics']['audit/已核对文件及链接']
        no_repair['metrics']['repair/补传_TB'] = None
        no_repair_local = copy.deepcopy(local); no_repair_local['last_sample']['repair_time'] = 0
        self.assertEqual(monitor.cloud_progress_problems(no_repair, no_repair_local), [])

    def test_inspection_reports_cloud_lag_without_restarting_buffered_reporter(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); run = root / 'run'; audit = root / 'audit'
            work = audit / 'monitor20'; repair = audit / 'repair'
            (run / 'recovery').mkdir(parents=True); work.mkdir(parents=True); repair.mkdir()
            reporter = audit / 'historical-reporter'; reporter.mkdir()
            (reporter / 'guardian.sh').write_text('fixture')
            (reporter / 'manifest.json').write_text('{}')
            nodes = ('cpu-master-0', 'cpu-worker-0')
            topology = {'hosts': {n: 'host-' + n for n in nodes}, 'reporter_python': '/python'}
            mounts = {'source_ino': 1, 'target_ino': 2,
                      'source_mount': ['nfs', 'source'], 'target_mount': ['nfs', 'target']}
            (run / 'recovery/ssh.json').write_text(json.dumps(topology))
            (run / 'config.json').write_text(json.dumps({'nodes': {n: mounts for n in nodes}}))
            (run / 'status.json').write_text('{"counts":{}}')
            now = time.time()
            (audit / 'status.json').write_text(json.dumps({'state': 'running', 'updated': now,
                'started': now - 3600, 'counts': {}, 'node_counts': {}}))
            (repair / 'status.json').write_text(json.dumps({'state': 'running', 'updated': now,
                'outbox_cursor': 0, 'nodes': {}}))
            (audit / 'swanlab.json').write_text(json.dumps({'state': 'streaming', 'updated': now,
                'enqueued_at': now, 'step': 100, 'run_path': 'a/b/c'}))
            roles = {component: {'workers': [{'pid': 1}], 'controllers': [], 'guardians': [{'pid': 2}]}
                     for component in ('audit', 'repair')}
            def fake_run(args, **kwargs):
                node = args[0]
                probe = dict(mounts, node=node, host=topology['hosts'][node], roles=roles,
                             reporter={'guardians': [{'pid': 3}], 'wrappers': [{'pid': 4}]})
                return types.SimpleNamespace(returncode=0, stdout=json.dumps(probe), stderr='')
            recovery = types.SimpleNamespace(ssh=lambda node, command: [node, command])
            with mock.patch.object(monitor, 'module', return_value=recovery), \
                    mock.patch.object(monitor.subprocess, 'run', side_effect=fake_run), \
                    mock.patch.object(monitor, 'bounded_outbox_probe', return_value={'state':'ok',
                        'at':now, 'cursor':0, 'max_id':0, 'pending_events_capped':0}), \
                    mock.patch.object(monitor, 'bounded_cloud_probe', return_value={'state':'ok',
                        'at':now, 'step':95, 'online':16}), \
                    mock.patch.object(monitor.os, 'kill') as kill:
                result = monitor.inspect_once(run, work, None)
            self.assertIn({'component':'swanlab_reporter', 'problem':'swanlab_cloud_lag_at_least_four_steps'},
                          result['problems'])
            self.assertEqual(result['cloud_probe']['step'], 95)
            self.assertFalse(any(x.get('action') == 'restart_stalled_reporter' for x in result['actions']))
            kill.assert_not_called()
            local = {'state': 'streaming', 'updated': now, 'enqueued_at': now, 'step': 100,
                     'run_path': 'a/b/c', 'last_sample': {'files_links': 123, 'file_bytes': 1_250_000_000_000,
                     'repair_time': now, 'copied_operations': 20, 'copied_bytes': 4_000_000_000}}
            (audit / 'swanlab.json').write_text(json.dumps(local))
            values = {'audit/已核对文件及链接': 123, 'audit/已核对文件_TB': 1.20,
                      'repair/补传操作累计': 20, 'repair/补传_TB': 0.004}
            cloud = {'state': 'ok', 'at': now, 'step': 100, 'online': 16,
                     'metrics': {k: {'step': 100, 'value': v} for k, v in values.items()}}
            with mock.patch.object(monitor, 'module', return_value=recovery), \
                    mock.patch.object(monitor.subprocess, 'run', side_effect=fake_run), \
                    mock.patch.object(monitor, 'bounded_outbox_probe', return_value={'state':'ok',
                        'at':now, 'cursor':0, 'max_id':0, 'pending_events_capped':0}), \
                    mock.patch.object(monitor, 'bounded_cloud_probe', return_value=cloud), \
                    mock.patch.object(monitor.os, 'kill') as kill:
                result = monitor.inspect_once(run, work, None)
            self.assertIn({'component': 'swanlab_reporter', 'problem': 'cloud_progress_value_mismatch',
                           'metric': 'audit/已核对文件_TB', 'step': 100}, result['problems'])
            self.assertFalse(any(x.get('action') == 'restart_stalled_reporter' for x in result['actions']))
            kill.assert_not_called()

    def test_inspection_does_not_restart_controllers_behind_stop_fences(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); run = root / 'run'; audit = root / 'audit'
            work = audit / 'monitor20'; repair = audit / 'repair'
            (run / 'recovery').mkdir(parents=True); work.mkdir(parents=True); repair.mkdir()
            nodes = ('cpu-master-0', 'cpu-worker-0')
            topology = {'hosts': {n: 'host-' + n for n in nodes}, 'reporter_python': '/python'}
            mounts = {'source_ino': 1, 'target_ino': 2,
                      'source_mount': ['nfs', 'source'], 'target_mount': ['nfs', 'target']}
            (run / 'recovery/ssh.json').write_text(json.dumps(topology))
            (run / 'config.json').write_text(json.dumps({'nodes': {n: mounts for n in nodes}}))
            (run / 'status.json').write_text('{"counts":{}}')
            stale = time.time() - 900
            (audit / 'status.json').write_text(json.dumps({'state': 'running', 'updated': stale,
                'started': stale - 3600, 'counts': {}, 'node_counts': {},
                'controller_host': topology['hosts'][nodes[0]], 'controller_pid': 17}))
            (repair / 'status.json').write_text(json.dumps({'state': 'running', 'updated': stale,
                'outbox_cursor': 0, 'nodes': {}, 'controller_host': topology['hosts'][nodes[0]],
                'controller_pid': 18}))
            (audit / 'STOP').touch(); (repair / 'STOP').touch()
            def fake_run(args, **kwargs):
                node, command = args
                self.assertNotIn(' stop-stalled ', command)
                roles = {component: {'workers': [{'pid': 1}], 'guardians': [{'pid': 2}],
                                     'controllers': ([{'pid': pid, 'state': 'S'}] if node == nodes[0] else [])}
                         for component, pid in (('audit', 17), ('repair', 18))}
                probe = dict(mounts, node=node, host=topology['hosts'][node], roles=roles, reporter={})
                return types.SimpleNamespace(returncode=0, stdout=json.dumps(probe), stderr='')
            recovery = types.SimpleNamespace(ssh=lambda node, command: [node, command])
            with mock.patch.object(monitor, 'module', return_value=recovery), \
                    mock.patch.object(monitor.subprocess, 'run', side_effect=fake_run), \
                    mock.patch.object(monitor.time, 'sleep') as sleep, \
                    mock.patch.object(monitor, 'bounded_outbox_probe', return_value={'state': 'ok',
                        'at': time.time(), 'cursor': 0, 'max_id': 0, 'pending_events_capped': 0}):
                result = monitor.inspect_once(run, work, None)
            self.assertFalse(any(x.get('action') == 'restart_stalled_controller' for x in result['actions']))
            sleep.assert_not_called()

    def test_stalled_reporter_rechecks_owner_process_and_fence_before_signal(self):
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp) / 'audit'; audit.mkdir()
            reporter = audit / 'historical-reporter'; reporter.mkdir()
            procroot = Path(temp) / 'proc'; pid, start = 12345, 98765
            proc = procroot / str(pid); proc.mkdir(parents=True)
            code = reporter / 'code/projset_swanlab_historical.py'
            (proc / 'cmdline').write_bytes(b'python3\0' + os.fsencode(code) + b'\0--work\0' +
                                           os.fsencode(audit) + b'\0')
            fields = ['S'] + ['1'] * 18 + [str(start)]
            (proc / 'stat').write_text(f'{pid} (python3) ' + ' '.join(fields))
            updated, enqueued = time.time() - 900, time.time() - 901
            state = {'state': 'streaming', 'updated': updated, 'enqueued_at': enqueued}
            owner = {'host': monitor.socket.gethostname(), 'pid': pid, 'start_ticks': start}
            (audit / 'swanlab.json').write_text(json.dumps(state))
            (reporter / 'owner.json').write_text(json.dumps(owner))

            def path_for(value):
                return procroot if value == '/proc' else Path(value)

            with mock.patch.object(monitor, 'Path', side_effect=path_for), \
                    mock.patch.object(monitor.os, 'kill') as kill:
                result = monitor.stop_stalled_reporter(audit, pid, start, updated, enqueued)
                self.assertEqual(result['signal'], 'SIGTERM')
                kill.assert_called_once_with(pid, monitor.signal.SIGTERM)
                kill.reset_mock()
                for change in ('changed_heartbeat', 'changed_enqueue', 'owner_reused', 'blocked', 'stop'):
                    state2, owner2, fields2 = dict(state), dict(owner), list(fields)
                    if change == 'changed_heartbeat': state2['updated'] += 1
                    elif change == 'changed_enqueue': state2['enqueued_at'] += 1
                    elif change == 'owner_reused': owner2['start_ticks'] += 1
                    elif change == 'blocked': fields2[0] = 'D'
                    elif change == 'stop': (reporter / 'STOP').touch()
                    (audit / 'swanlab.json').write_text(json.dumps(state2))
                    (reporter / 'owner.json').write_text(json.dumps(owner2))
                    (proc / 'stat').write_text(f'{pid} (python3) ' + ' '.join(fields2))
                    with self.subTest(change=change):
                        with self.assertRaises(ValueError):
                            monitor.stop_stalled_reporter(audit, pid, start, updated, enqueued)
                        kill.assert_not_called()
                (reporter / 'STOP').unlink()
                (audit / 'swanlab.json').write_text(json.dumps(state))
                (reporter / 'owner.json').write_text(json.dumps(owner))
                (proc / 'stat').write_text(f'{pid} (python3) ' + ' '.join(fields))
                with mock.patch.object(Path, 'exists', side_effect=[False, True]):
                    with self.assertRaisesRegex(ValueError, 'deliberately stopped'):
                        monitor.stop_stalled_reporter(audit, pid, start, updated, enqueued)
                    kill.assert_not_called()


if __name__ == '__main__':
    unittest.main()
