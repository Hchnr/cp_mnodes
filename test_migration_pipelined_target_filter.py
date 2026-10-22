"""Local publication/coordinator integration fixtures; never touch live jobs."""

import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest import mock

import migration_pipelined_target_filter as pipelined
from migration_io import read_json, write_json
from migration_overlap_resume import WARM_STAGE
from migration_task_publisher import BoundedTaskPublisher, PublicationError


class PipelinedTargetFilterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='pipelined-target-fixture-')
        self.addCleanup(self.temporary.cleanup)
        self.control = Path(self.temporary.name)
        self.release = threading.Event()
        self.pools = []
        # Register last so blocked writers are released before their temporary
        # directories or SQLite handles are cleaned up after a failed assertion.
        self.addCleanup(self.close_pools)

    def close_pools(self):
        self.release.set()
        for pool in self.pools:
            try:
                pool.close()
            except PublicationError:
                pass

    def make(self, writer=write_json, workers=8, limit=16):
        obj = object.__new__(pipelined.PipelinedTargetFilterResume)
        obj.control = self.control
        obj.cfg = {'run_id': 'local-pipelined-fixture'}
        obj.stage = WARM_STAGE
        obj.state = {'recovery_warm': True, 'status': 'scanning'}
        obj.db = sqlite3.connect(':memory:')
        self.addCleanup(obj.db.close)
        obj.db.execute('CREATE TABLE files (rel TEXT PRIMARY KEY, source TEXT, '
                       'destination TEXT, owned INTEGER, sha256 TEXT)')
        obj.assigned = set()
        obj.next_task_id = 700
        obj.enqueued_batches = 50
        obj.discovered_files = 100
        obj.discovered_bytes = 1000
        obj.phase_progress = {'3': {'discovered_files': 100, 'discovered_bytes': 1000}}
        obj.current_scan = {'files': 100, 'bytes': 1000}
        obj.last_scan_save = time.monotonic()
        obj._check_stop = mock.Mock()
        obj._task_publisher = BoundedTaskPublisher(
            workers=workers, max_pending=limit, writer=writer)
        self.pools.append(obj._task_publisher)
        obj._publication_tasks = {}
        obj._publication_error = None
        obj._publication_failures = []
        obj._publication_successes = 0
        obj._publication_workers = workers
        obj._publication_limit = limit
        obj._publication_enabled = True
        obj._publication_failure_path = None
        obj._publication_finished = False
        obj._publication_last_tuning_check = 0
        obj._publication_tuning_id = None
        obj._publication_tuning_error = None
        return obj

    @staticmethod
    def entry(name, size=10, **fields):
        return dict({'rel': 'other/' + name, 'kind': 'file', 'size': size}, **fields)

    def task_path(self, task_id):
        return self.control / 'stages' / WARM_STAGE / 'pending' / str(task_id % 64) / (str(task_id) + '.json')

    def tuning(self, obj, **updates):
        value = {'control': str(obj.control), 'run_id': obj.cfg['run_id'],
                 'scope': 'warm-publication-v1', 'operation_id': 'a' * 32,
                 'publish_workers': obj._publication_workers,
                 'publish_limit': obj._publication_limit}
        value.update(updates)
        write_json(obj.control / 'producer-tuning.json', value)
        return value

    def await_ready(self, predicate):
        deadline = time.monotonic() + 3
        while not predicate() and time.monotonic() < deadline:
            time.sleep(.002)
        self.assertTrue(predicate(), 'local fixture did not become ready')

    def test_eight_writers_count_only_completed_publications_once(self):
        lock = threading.Lock()
        started = set()

        def writer(path, task):
            with lock:
                started.add(task['id'])
            self.assertTrue(self.release.wait(3))
            return write_json(path, task)

        obj = self.make(writer)
        for number in range(8):
            obj._emit_tasks([self.entry(str(number), number + 1)], 3, 3)
        self.await_ready(lambda: len(started) == 8)
        self.assertEqual(obj.enqueued_batches, 50)
        self.assertEqual(obj.discovered_files, 100)
        self.assertEqual(obj.discovered_bytes, 1000)
        self.assertEqual(obj._publication_snapshot()['pending'], 8)
        self.release.set()
        obj._collect_publications(finish=True)
        self.assertEqual(obj.enqueued_batches, 58)
        self.assertEqual(obj.discovered_files, 108)
        self.assertEqual(obj.discovered_bytes, 1036)
        self.assertEqual(obj.current_scan, {'files': 108, 'bytes': 1036})
        self.assertEqual(obj.phase_progress['3'],
                         {'discovered_files': 108, 'discovered_bytes': 1036})
        self.assertEqual(obj._publication_snapshot()['published_since_handover'], 8)
        self.assertEqual(obj._publication_snapshot()['pending'], 0)
        obj._collect_publications(finish=True)
        self.assertEqual(obj.enqueued_batches, 58)

    def test_paths_and_ids_are_reserved_before_submit_and_duplicate_not_republished(self):
        def writer(path, task):
            self.assertTrue(self.release.wait(3))
            return write_json(path, task)

        obj = self.make(writer)
        original_submit = obj._task_publisher.submit

        def checked_submit(task_id, task, target):
            self.assertEqual(task_id, 700)
            self.assertEqual(obj.next_task_id, 701)
            self.assertEqual(obj.assigned, {'other/one'})
            self.assertIn(task_id, obj._publication_tasks)
            return original_submit(task_id, task, target)

        with mock.patch.object(obj._task_publisher, 'submit', side_effect=checked_submit) as submit:
            obj._emit_tasks([self.entry('one'), self.entry('one', 99)], 3, 3)
            obj._emit_tasks([self.entry('one')], 3, 3)
        submit.assert_called_once()
        self.assertEqual(obj.next_task_id, 701)
        self.release.set()
        obj._collect_publications(finish=True)
        self.assertEqual(len(read_json(self.task_path(700))['entries']), 1)
        self.assertFalse(self.task_path(701).exists())

    def test_known_proof_is_preserved_and_not_inserted_into_original(self):
        obj = self.make()
        source, destination = {'ino': 3}, {'ino': 9}
        obj.db.execute('INSERT INTO files VALUES (?,?,?,?,?)',
                       ('other/known', json.dumps(source), json.dumps(destination), 1, 'abc123'))
        original = self.entry('known', known={'old': 'superseded'})
        untouched = copy.deepcopy(original)
        retained = self.entry('retained', known={'sha256': 'existing-entry-proof'})
        obj._emit_tasks([original, retained], 3, 3)
        obj._collect_publications(finish=True)
        entries = read_json(self.task_path(700))['entries']
        self.assertEqual(entries[0]['known'], {'source': source, 'destination': destination,
                                             'owned': True, 'sha256': 'abc123'})
        self.assertEqual(entries[1]['known'], retained['known'])
        self.assertEqual(original, untouched)

    def test_count_discovery_false_only_counts_successful_batch(self):
        obj = self.make()
        obj._emit_tasks([self.entry('already-discovered', 150)], 3, 3, count_discovery=False)
        obj._collect_publications(finish=True)
        self.assertEqual(obj.enqueued_batches, 51)
        self.assertEqual(obj.discovered_files, 100)
        self.assertEqual(obj.discovered_bytes, 1000)
        self.assertEqual(obj.current_scan, {'files': 100, 'bytes': 1000})

    def test_scan_complete_save_waits_for_all_files_before_parent_save(self):
        entered, completed = threading.Event(), threading.Event()

        def writer(path, task):
            entered.set()
            self.assertTrue(self.release.wait(3))
            result = write_json(path, task)
            completed.set()
            return result

        obj = self.make(writer)
        obj._emit_tasks([self.entry('slow')], 3, 3)
        self.assertTrue(entered.wait(2))
        observed = []
        timer = threading.Timer(.05, self.release.set)
        timer.start()
        try:
            with mock.patch.object(pipelined.DistributedTargetFilterResume, 'save',
                                   autospec=True, side_effect=lambda instance, **updates:
                                   observed.append((completed.is_set(), updates))):
                obj.save(status='copying', scan_complete=True)
        finally:
            timer.join()
        self.assertEqual(len(observed), 1)
        self.assertTrue(observed[0][0])
        self.assertTrue(observed[0][1]['scan_complete'])
        self.assertEqual(observed[0][1]['enqueued_batches'], 51)
        self.assertEqual(observed[0][1]['next_task_id'], 701)
        self.assertEqual(observed[0][1]['task_publication']['pending'], 0)

    def test_failure_run_drains_other_futures_and_saves_paused_unconfirmed_ids(self):
        accepted = threading.Event()
        completed = threading.Event()

        def writer(path, task):
            self.assertTrue(accepted.wait(3))
            if task['id'] == 700:
                raise OSError('fixture first publication failed')
            result = write_json(path, task)
            completed.set()
            return result

        obj = self.make(writer)

        def parent_supplement(instance):
            instance._emit_tasks([self.entry('failed')], 3, 3)
            instance._emit_tasks([self.entry('successful', 20)], 3, 3)
            accepted.set()
            raise OSError('fixture scan abort')

        captured = []
        with mock.patch.object(pipelined.DistributedTargetFilterResume, 'supplement_missing',
                               autospec=True, side_effect=parent_supplement), \
                mock.patch.object(pipelined.DistributedTargetFilterResume, 'save', autospec=True,
                                  side_effect=lambda instance, **updates: captured.append(updates)):
            with self.assertRaises(PublicationError):
                obj.run()
        self.assertTrue(completed.is_set())
        self.assertIsNone(obj._task_publisher)
        self.assertEqual(obj.enqueued_batches, 51)
        self.assertEqual(obj.discovered_files, 101)
        self.assertEqual(obj.discovered_bytes, 1020)
        self.assertEqual(obj.next_task_id, 702)
        self.assertEqual(obj.assigned, {'other/failed', 'other/successful'})
        self.assertEqual(len(captured), 1)
        saved = captured[0]
        self.assertEqual(saved['status'], 'producer_paused')
        self.assertNotIn('error', saved)
        self.assertIn('fixture first publication failed', saved['producer_error'])
        self.assertEqual(saved['next_task_id'], 702)
        record = read_json(saved['task_publication']['failure_record'])
        self.assertEqual(record['next_task_id'], 702)
        self.assertEqual([task['id'] for task in record['unconfirmed']], [700])
        self.assertEqual(record['unconfirmed'][0]['task']['entries'][0]['rel'], 'other/failed')
        self.assertTrue(self.task_path(701).is_file())

    def test_direct_failed_save_drains_inflight_publications_before_paused_state(self):
        entered, completed = threading.Event(), threading.Event()

        def writer(path, task):
            entered.set()
            self.assertTrue(self.release.wait(3))
            result = write_json(path, task)
            completed.set()
            return result

        obj = self.make(writer)
        obj._emit_tasks([self.entry('inflight')], 3, 3)
        self.assertTrue(entered.wait(2))
        observed = []
        timer = threading.Timer(.05, self.release.set)
        timer.start()
        try:
            with mock.patch.object(pipelined.DistributedTargetFilterResume, 'save', autospec=True,
                                   side_effect=lambda instance, **updates:
                                   observed.append((completed.is_set(), updates))):
                obj.save(status='failed', error='fixture scanner failed')
        finally:
            timer.join()
        self.assertTrue(observed[0][0], 'paused state must not precede accepted writes finishing')
        self.assertEqual(observed[0][1]['status'], 'producer_paused')
        self.assertEqual(observed[0][1]['task_publication']['pending'], 0)
        self.assertEqual(obj.next_task_id, 701)

    def test_failed_scan_complete_cannot_publish_scan_complete_marker(self):
        def writer(path, task):
            raise OSError('fixture publication failed')

        obj = self.make(writer)
        # Submit without the final opportunistic harvest so failure scheduling
        # cannot decide whether _emit_tasks or save observes the failure first.
        with mock.patch.object(obj, '_collect_publications'):
            obj._emit_tasks([self.entry('failed')], 3, 3)
        with mock.patch.object(pipelined.DistributedTargetFilterResume, 'save') as parent_save:
            with self.assertRaises(PublicationError):
                obj.save(scan_complete=True)
        parent_save.assert_not_called()
        self.assertEqual(obj.next_task_id, 701)
        self.assertEqual(obj.enqueued_batches, 50)
        self.assertEqual(obj.discovered_files, 100)

    def test_submit_failure_applies_ready_success_and_reserves_uncertain_id(self):
        accepted = threading.Event()

        def writer(path, task):
            self.assertTrue(accepted.wait(3))
            if task['id'] == 700:
                raise OSError('fixture detected by submit')
            return write_json(path, task)

        obj = self.make(writer)
        # Defer coordinator harvest to exercise submit's separate failure path,
        # which may carry prior successes along with the fatal failure.
        with mock.patch.object(obj, '_collect_publications'):
            obj._emit_tasks([self.entry('failed')], 3, 3)
            obj._emit_tasks([self.entry('successful', 20)], 3, 3)
            accepted.set()
            self.await_ready(lambda: all(f.done() for f in obj._task_publisher._pending))
            with self.assertRaises(PublicationError):
                obj._emit_tasks([self.entry('not-accepted', 30)], 3, 3)
        self.assertEqual(obj.enqueued_batches, 51)
        self.assertEqual(obj.discovered_files, 101)
        self.assertEqual(obj.discovered_bytes, 1020)
        self.assertEqual(obj.next_task_id, 703)
        self.assertEqual(set(obj._publication_tasks), {700, 702})
        self.assertEqual(obj.assigned, {'other/failed', 'other/successful', 'other/not-accepted'})
        self.assertFalse(self.task_path(702).exists())
        with mock.patch.object(pipelined.DistributedTargetFilterResume, 'save', autospec=True) as save:
            obj.save(status='failed', error='fixture submit aborted')
        updates = save.call_args.kwargs
        self.assertEqual(updates['status'], 'producer_paused')
        self.assertEqual(updates['next_task_id'], 703)
        record = read_json(updates['task_publication']['failure_record'])
        self.assertEqual({task['id'] for task in record['unconfirmed']}, {700, 702})
        self.assertEqual(obj.enqueued_batches, 51)

    def test_capacity_backpressure_retries_same_reserved_id_not_another_task(self):
        def writer(path, task):
            self.assertTrue(self.release.wait(3))
            return write_json(path, task)

        obj = self.make(writer, workers=1, limit=1)
        obj._emit_tasks([self.entry('one')], 3, 3)
        original_submit = obj._task_publisher.submit
        attempts = []

        def checked_submit(task_id, task, target):
            attempts.append(task_id)
            self.assertEqual(task_id, 701)
            self.assertEqual(obj.next_task_id, 702)
            self.assertEqual(obj.assigned, {'other/one', 'other/two'})
            return original_submit(task_id, task, target)

        timer = threading.Timer(.05, self.release.set)
        timer.start()
        try:
            with mock.patch.object(obj._task_publisher, 'submit', side_effect=checked_submit):
                obj._emit_tasks([self.entry('two')], 3, 3)
        finally:
            timer.join()
        self.assertGreaterEqual(len(attempts), 2)
        obj._collect_publications(finish=True)
        self.assertEqual(obj.enqueued_batches, 52)
        self.assertEqual(obj.next_task_id, 702)
        self.assertEqual(obj.discovered_files, 102)
        self.assertEqual(read_json(self.task_path(701))['entries'][0]['rel'], 'other/two')

    def test_formal_stage_and_closed_publisher_delegate_to_parent_unchanged(self):
        for stage, closed in (('p1-r1', False), (WARM_STAGE, True)):
            with self.subTest(stage=stage, closed=closed):
                obj = self.make()
                obj.stage = stage
                if closed:
                    obj._task_publisher = None
                entries = [self.entry('formal')]
                with mock.patch.object(pipelined.DistributedTargetFilterResume, '_emit_tasks',
                                       autospec=True, return_value='parent-result') as parent:
                    self.assertEqual(obj._emit_tasks(entries, 1, 2, False), 'parent-result')
                parent.assert_called_once_with(obj, entries, 1, 2, False)
                self.assertEqual(obj.next_task_id, 700)
                self.assertEqual(obj.assigned, set())

    def test_invalid_constructor_parameters_rejected_before_parent_side_effects(self):
        cases = [(0, 64), (65, 64), (True, 64), (1.0, 64), (8, 7),
                 (8, 4097), (8, True), (8, 64.0)]
        with mock.patch.object(pipelined.DistributedTargetFilterResume, '__init__',
                               autospec=True) as parent:
            for workers, limit in cases:
                with self.subTest(workers=workers, limit=limit):
                    with self.assertRaises(ValueError):
                        pipelined.PipelinedTargetFilterResume(
                            self.control, self.control / 'unused-handover',
                            publish_workers=workers, publish_limit=limit)
            parent.assert_not_called()

    def test_tuning_checks_every_ten_seconds_and_same_size_does_not_replace_pool(self):
        obj = self.make()
        self.tuning(obj)
        pool = obj._task_publisher
        with mock.patch.object(pipelined, 'read_json', wraps=read_json) as read, \
                mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory:
            for now, expected_reads in ((9.999, 0), (10.0, 1), (19.999, 1), (20.0, 2)):
                with mock.patch.object(pipelined.time, 'monotonic', return_value=now):
                    obj._refresh_publication_tuning()
                self.assertEqual(read.call_count, expected_reads)
            factory.assert_not_called()
        self.assertIs(obj._task_publisher, pool)
        self.assertFalse(pool._closed)
        self.assertEqual(obj._publication_tuning_id, 'a' * 32)
        self.assertIsNone(obj._publication_tuning_error)

    def test_tuning_drains_old_pool_before_replacement_without_resetting_work(self):
        entered, completed = threading.Event(), threading.Event()

        def writer(path, task):
            entered.set()
            self.assertTrue(self.release.wait(3))
            result = write_json(path, task)
            completed.set()
            return result

        obj = self.make(writer)
        obj._emit_tasks([self.entry('before-tuning', 20)], 3, 3)
        self.assertTrue(entered.wait(2))
        old_pool = obj._task_publisher
        self.tuning(obj, publish_workers=16, publish_limit=64)
        observed = []

        def construct(**kwargs):
            observed.append((completed.is_set(), old_pool._shutdown,
                             obj.enqueued_batches, obj.next_task_id,
                             set(obj.assigned), obj.discovered_files, obj.discovered_bytes))
            pool = BoundedTaskPublisher(**kwargs)
            self.pools.append(pool)
            return pool

        timer = threading.Timer(.05, self.release.set)
        timer.start()
        try:
            with mock.patch.object(pipelined, 'BoundedTaskPublisher', side_effect=construct) as factory, \
                    mock.patch.object(pipelined.time, 'monotonic', return_value=100.0):
                obj._refresh_publication_tuning()
        finally:
            timer.join()
        factory.assert_called_once_with(workers=16, max_pending=64)
        self.assertEqual(observed, [(True, True, 51, 701, {'other/before-tuning'}, 101, 1020)])
        self.assertIsNot(obj._task_publisher, old_pool)
        self.assertFalse(obj._publication_finished)
        self.assertEqual(obj._publication_workers, 16)
        self.assertEqual(obj._publication_limit, 64)
        self.assertEqual(obj._publication_successes, 1)
        self.assertEqual(obj._publication_tasks, {})
        obj._emit_tasks([self.entry('after-tuning', 30)], 3, 3)
        obj._collect_publications(finish=True)
        self.assertEqual(obj.next_task_id, 702)
        self.assertEqual(obj.enqueued_batches, 52)
        self.assertEqual(obj.discovered_files, 102)
        self.assertEqual(obj.discovered_bytes, 1050)
        self.assertEqual(obj.assigned, {'other/before-tuning', 'other/after-tuning'})
        self.assertEqual(read_json(self.task_path(701))['entries'][0]['rel'], 'other/after-tuning')

    def test_tuning_rejects_changed_payload_for_same_operation_but_old_pool_stays_healthy(self):
        obj = self.make()
        self.tuning(obj)
        pool = obj._task_publisher
        with mock.patch.object(pipelined.time, 'monotonic', return_value=100.0):
            obj._refresh_publication_tuning()
        self.tuning(obj, publish_workers=16, publish_limit=64)
        with mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory, \
                mock.patch.object(pipelined.time, 'monotonic', return_value=110.0):
            obj._refresh_publication_tuning()
        factory.assert_not_called()
        self.assertIs(obj._task_publisher, pool)
        self.assertFalse(pool._closed)
        self.assertEqual(obj._publication_tuning_id, 'a' * 32)
        self.assertIn('cannot change its payload', obj._publication_tuning_error)
        obj._emit_tasks([self.entry('healthy-old-pool')], 3, 3)
        obj._collect_publications()
        # A fresh operation is allowed, and an unchanged validated payload can
        # clear a prior administrative validation error without opening a pool.
        self.tuning(obj, operation_id='b' * 32)
        with mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory, \
                mock.patch.object(pipelined.time, 'monotonic', return_value=120.0):
            obj._refresh_publication_tuning()
        factory.assert_not_called()
        self.assertEqual(obj._publication_tuning_id, 'b' * 32)
        self.assertIsNone(obj._publication_tuning_error)
        obj._collect_publications(finish=True)
        self.assertEqual(obj.enqueued_batches, 51)

    def test_tuning_barrier_failure_does_not_open_new_pool_or_accept_operation(self):
        accepted = threading.Event()

        def writer(path, task):
            self.assertTrue(accepted.wait(3))
            if task['id'] == 700:
                raise OSError('fixture failed while tuning drained the old pool')
            return write_json(path, task)

        obj = self.make(writer)
        obj._emit_tasks([self.entry('failed')], 3, 3)
        obj._emit_tasks([self.entry('successful', 20)], 3, 3)
        pool = obj._task_publisher
        self.tuning(obj, publish_workers=16, publish_limit=64)
        accepted.set()
        with mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory, \
                mock.patch.object(pipelined.time, 'monotonic', return_value=100.0):
            with self.assertRaises(PublicationError):
                obj._refresh_publication_tuning()
        factory.assert_not_called()
        self.assertIs(obj._task_publisher, pool)
        self.assertTrue(pool._shutdown)
        self.assertTrue(obj._publication_finished)
        self.assertIsNone(obj._publication_tuning_id)
        self.assertEqual((obj._publication_workers, obj._publication_limit), (8, 16))
        self.assertEqual(obj.next_task_id, 702)
        self.assertEqual(obj.enqueued_batches, 51)
        self.assertEqual(obj.discovered_files, 101)
        self.assertEqual(obj.discovered_bytes, 1020)
        self.assertEqual(set(obj._publication_tasks), {700})

    def test_invalid_tuning_is_observable_and_never_changes_healthy_pool(self):
        obj = self.make()
        pool = obj._task_publisher
        invalid = [
            {'control': str(self.control / 'other')}, {'run_id': 'another-run'},
            {'scope': 'formal-publication'}, {'operation_id': None},
            {'operation_id': 'a' * 31}, {'operation_id': 'A' * 32},
            {'operation_id': 'g' * 32}, {'publish_workers': 0},
            {'publish_workers': 65}, {'publish_workers': True},
            {'publish_workers': 8.0}, {'publish_limit': 7},
            {'publish_limit': 4097}, {'publish_limit': True}, {'publish_limit': 16.0},
        ]
        for index, updates in enumerate(invalid):
            with self.subTest(updates=updates):
                self.tuning(obj, **updates)
                with mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory, \
                        mock.patch.object(pipelined.time, 'monotonic', return_value=100 + index * 10):
                    obj._refresh_publication_tuning()
                factory.assert_not_called()
                self.assertIs(obj._task_publisher, pool)
                self.assertFalse(pool._closed)
                self.assertIsNotNone(obj._publication_snapshot()['tuning_error'])
                self.assertIsNone(obj._publication_tuning_id)
                self.assertEqual((obj._publication_workers, obj._publication_limit), (8, 16))
        obj._emit_tasks([self.entry('still-healthy')], 3, 3)
        obj._collect_publications(finish=True)
        self.assertEqual(obj.enqueued_batches, 51)

    def test_tuning_worker_and_capacity_boundaries_are_inclusive(self):
        for workers, limit in ((1, 1), (64, 64), (64, 4096)):
            with self.subTest(workers=workers, limit=limit):
                obj = self.make()
                self.tuning(obj, publish_workers=workers, publish_limit=limit)
                replacements = []

                def construct(**kwargs):
                    pool = BoundedTaskPublisher(**kwargs)
                    self.pools.append(pool)
                    replacements.append(pool)
                    return pool

                with mock.patch.object(pipelined, 'BoundedTaskPublisher', side_effect=construct), \
                        mock.patch.object(pipelined.time, 'monotonic', return_value=100.0):
                    obj._refresh_publication_tuning()
                self.assertEqual(len(replacements), 1)
                self.assertEqual((obj._publication_workers, obj._publication_limit), (workers, limit))
                self.assertIsNone(obj._publication_tuning_error)

    def test_scan_complete_or_closed_pool_never_reads_tuning_or_reopens_pool(self):
        obj = self.make()
        self.tuning(obj, publish_workers=16, publish_limit=64)
        with mock.patch.object(pipelined.DistributedTargetFilterResume, 'save', autospec=True), \
                mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory, \
                mock.patch.object(pipelined, 'read_json', wraps=read_json) as read:
            obj.save(scan_complete=True)
            self.assertTrue(obj._publication_finished)
            obj.save(status='copying')
            obj._refresh_publication_tuning()
            obj._task_publisher = None
            obj._refresh_publication_tuning()
            factory.assert_not_called()
            read.assert_not_called()
        self.assertEqual((obj._publication_workers, obj._publication_limit), (8, 16))

    def test_formal_stage_never_reads_or_applies_warm_tuning(self):
        obj = self.make()
        obj.stage = 'p1-r1'
        self.tuning(obj, publish_workers=16, publish_limit=64)
        with mock.patch.object(pipelined, 'BoundedTaskPublisher') as factory, \
                mock.patch.object(pipelined, 'read_json', wraps=read_json) as read:
            obj._refresh_publication_tuning()
        factory.assert_not_called()
        read.assert_not_called()

    def test_ancestor_constructor_virtual_save_sees_initialized_publication_fields(self):
        observed = []

        def parent_constructor(instance, control, handover):
            self.assertIsNone(instance._task_publisher)
            self.assertFalse(instance._publication_enabled)
            self.assertEqual(instance._publication_tasks, {})
            instance.control = Path(control)
            instance.cfg = {'run_id': 'local-pipelined-fixture'}
            instance.stage = WARM_STAGE
            instance.state = {'recovery_warm': True}
            instance.next_task_id = 19
            instance.enqueued_batches = 15
            instance.save(status='scanning')

        with mock.patch.object(pipelined.DistributedTargetFilterResume, '__init__',
                               autospec=True, side_effect=parent_constructor), \
                mock.patch.object(pipelined.DistributedTargetFilterResume, 'save', autospec=True,
                                  side_effect=lambda instance, **updates: observed.append(updates)):
            obj = pipelined.PipelinedTargetFilterResume(
                self.control, self.control / 'unused-handover', publish_workers=8, publish_limit=64)
            self.pools.append(obj._task_publisher)
        self.assertEqual(observed[0]['status'], 'scanning')
        self.assertGreaterEqual(observed[0]['producer_nofile'][0], 65536)
        self.assertEqual(observed[1]['task_publication']['workers'], 8)
        self.assertEqual(observed[1]['task_publication']['limit'], 64)
        self.assertEqual(observed[1]['next_task_id'], 19)


if __name__ == '__main__':
    unittest.main()
