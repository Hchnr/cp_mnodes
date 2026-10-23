"""Local fixtures only: no migration coordinator, broker, or live copy starts."""

from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

import migration_task_publisher as publisher_module
from migration_io import read_json, write_json
from migration_task_publisher import BoundedTaskPublisher, PublicationError


class TaskPublisherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='task-publisher-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.release = threading.Event()
        # Release blocked fixtures before closing pools, including on assertion
        # failures, so a broken test cannot leave the suite waiting forever.
        self.pools = []
        self.addCleanup(self.clean_pools)

    def clean_pools(self):
        self.release.set()
        for pool in self.pools:
            try:
                pool.close()
            except PublicationError:
                pass

    def pool(self, **kwargs):
        pool = BoundedTaskPublisher(**kwargs)
        self.pools.append(pool)
        return pool

    def target(self, task_id):
        return self.root / 'pending' / str(task_id % 64) / (str(task_id) + '.json')

    def await_ready(self, predicate):
        deadline = time.monotonic() + 3
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.002)
        self.assertTrue(predicate(), 'fixture did not become ready')

    def test_out_of_order_writes_are_independent_and_reported_once(self):
        first_entered, second_written = threading.Event(), threading.Event()

        def writer(path, payload):
            if payload['id'] == 1:
                first_entered.set()
                self.assertTrue(self.release.wait(3))
            result = write_json(path, payload)
            if payload['id'] == 2:
                second_written.set()
            return result

        pool = self.pool(workers=2, max_pending=3, writer=writer)
        self.assertTrue(pool.submit(1, {'id': 1}, self.target(1)))
        self.assertTrue(first_entered.wait(2))
        self.assertTrue(pool.submit(2, {'id': 2}, self.target(2)))
        self.assertTrue(second_written.wait(2))
        ready = pool.harvest(wait=True)
        self.assertEqual([result.task_id for result in ready], [2])
        self.assertEqual(pool.pending_count, 1)
        self.release.set()
        self.assertEqual([result.task_id for result in pool.finish()], [1])
        self.assertEqual(pool.finish(), [])
        self.assertEqual(read_json(self.target(1)), {'id': 1})

    def test_bound_includes_completed_unharvested_results(self):
        completed = threading.Event()

        def writer(path, payload):
            result = write_json(path, payload)
            completed.set()
            return result

        pool = self.pool(workers=1, max_pending=1, writer=writer)
        self.assertTrue(pool.submit(1, {'id': 1}, self.target(1)))
        self.assertTrue(completed.wait(2))
        self.assertFalse(pool.submit(2, {'id': 2}, self.target(2)))
        self.assertEqual(pool.pending_count, 1)
        self.assertFalse(self.target(2).exists())
        self.assertEqual([r.task_id for r in pool.harvest(wait=True)], [1])
        self.assertTrue(pool.submit(2, {'id': 2}, self.target(2)))
        self.assertEqual([r.task_id for r in pool.close()], [2])

    def test_bound_limits_running_plus_queued_writes(self):
        started = threading.Event()

        def writer(path, payload):
            started.set()
            self.assertTrue(self.release.wait(3))
            return write_json(path, payload)

        pool = self.pool(workers=1, max_pending=2, writer=writer)
        self.assertTrue(pool.submit(1, {'id': 1}, self.target(1)))
        self.assertTrue(started.wait(2))
        self.assertTrue(pool.submit(2, {'id': 2}, self.target(2)))
        self.assertFalse(pool.submit(3, {'id': 3}, self.target(3)))
        self.assertEqual(pool.harvest(), [])
        self.assertEqual(pool.pending_count, 2)
        self.release.set()
        self.assertEqual({r.task_id for r in pool.finish()}, {1, 2})

    def test_payload_is_copied_before_submit_returns(self):
        started = threading.Event()

        def writer(path, payload):
            started.set()
            self.assertTrue(self.release.wait(3))
            return write_json(path, payload)

        pool = self.pool(workers=1, max_pending=1, writer=writer)
        original = {'id': 1, 'entries': [{'known': {'source': [1, 2]}}]}
        self.assertTrue(pool.submit(1, original, self.target(1)))
        self.assertTrue(started.wait(2))
        original['entries'][0]['known']['source'].append(3)
        original['entries'].clear()
        self.release.set()
        pool.finish()
        self.assertEqual(read_json(self.target(1)),
                         {'id': 1, 'entries': [{'known': {'source': [1, 2]}}]})

    def test_task_ids_and_paths_cannot_be_reused_after_harvest(self):
        pool = self.pool(workers=1, max_pending=1)
        self.assertTrue(pool.submit(1, {'id': 1}, self.target(1)))
        pool.harvest(wait=True)
        with self.assertRaisesRegex(ValueError, 'reused task ID'):
            pool.submit(1, {'id': 1}, self.target(2))
        with self.assertRaisesRegex(ValueError, 'reused target'):
            pool.submit(2, {'id': 2}, self.target(1))

    def test_existing_target_snapshot_or_dangling_symlink_rejected(self):
        pool = self.pool(workers=1, max_pending=1)
        target = self.target(1)
        target.parent.mkdir(parents=True)
        target.write_text('{"old":true}')
        with self.assertRaises(FileExistsError):
            pool.submit(1, {'id': 1}, target)
        self.assertEqual(target.read_text(), '{"old":true}')
        target = self.target(2)
        snapshot = target.parent / '.snapshots' / target.name
        snapshot.mkdir(parents=True)
        with self.assertRaises(FileExistsError):
            pool.submit(2, {'id': 2}, target)
        target = self.target(3)
        target.parent.mkdir(parents=True)
        target.symlink_to(target.parent / 'missing')
        with self.assertRaises(FileExistsError):
            pool.submit(3, {'id': 3}, target)

    def test_deferred_fresh_check_never_uses_the_main_thread(self):
        owner = threading.get_ident()
        original = publisher_module._fresh_target
        checked = []
        def check(path):
            self.assertNotEqual(threading.get_ident(), owner)
            checked.append(path)
            return original(path)
        pool = self.pool(workers=1,max_pending=2,check_on_submit=False)
        with mock.patch.object(publisher_module,'_fresh_target',side_effect=check):
            self.assertTrue(pool.submit(1,{'id':1},self.target(1)))
            self.assertEqual([r.task_id for r in pool.finish()],[1])
        self.assertEqual(checked,[self.target(1)])

    def test_deferred_checks_preserve_every_collision_guard_and_never_write_existing_path(self):
        for number,kind in enumerate(('file','snapshot','symlink'),1):
            with self.subTest(kind=kind):
                target=self.target(number);target.parent.mkdir(parents=True,exist_ok=True)
                if kind=='file':target.write_text('{"old":true}')
                elif kind=='snapshot':(target.parent/'.snapshots'/target.name).mkdir(parents=True)
                else:target.symlink_to(target.parent/'missing')
                pool=self.pool(workers=1,max_pending=1,check_on_submit=False)
                self.assertTrue(pool.submit(number,{'id':number},target))
                with self.assertRaises(PublicationError) as error:pool.finish()
                self.assertEqual(error.exception.results,())
                self.assertIsInstance(error.exception.failures[0].error,FileExistsError)
                if kind=='file':self.assertEqual(target.read_text(),'{"old":true}')
                elif kind=='snapshot':self.assertFalse(target.exists())
                else:self.assertTrue(target.is_symlink())

    def test_deferred_mode_still_rejects_duplicate_ids_and_targets_before_accepting(self):
        pool=self.pool(workers=1,max_pending=2,check_on_submit=False)
        self.assertTrue(pool.submit(1,{'id':1},self.target(1)))
        with self.assertRaisesRegex(ValueError,'reused task ID'):
            pool.submit(1,{'id':1},self.target(2))
        with self.assertRaisesRegex(ValueError,'reused target'):
            pool.submit(2,{'id':2},self.target(1))

    def test_failure_is_not_success_and_poison_blocks_more_submissions(self):
        failed = threading.Event()

        def writer(path, payload):
            failed.set()
            raise OSError('disk publication failed')

        pool = self.pool(workers=1, max_pending=1, writer=writer)
        pool.submit(1, {'id': 1}, self.target(1))
        self.assertTrue(failed.wait(2))
        self.await_ready(lambda: any(f.done() for f in pool._pending))
        with self.assertRaises(PublicationError) as error:
            pool.harvest()
        self.assertEqual(error.exception.results, ())
        self.assertEqual([f.task_id for f in error.exception.failures], [1])
        self.assertIsInstance(error.exception.failures[0].error, OSError)
        with self.assertRaises(PublicationError):
            pool.submit(2, {'id': 2}, self.target(2))
        self.assertFalse(self.target(2).exists())
        with self.assertRaises(PublicationError) as error:
            pool.finish()
        self.assertEqual(error.exception.results, ())

    def test_submit_detects_already_finished_failure_and_reports_successes(self):
        release_failure = threading.Event()

        def writer(path, payload):
            if payload['id'] == 2:
                self.assertTrue(release_failure.wait(3))
                raise OSError('second write fails')
            return write_json(path, payload)

        pool = self.pool(workers=2, max_pending=3, writer=writer)
        pool.submit(1, {'id': 1}, self.target(1))
        pool.submit(2, {'id': 2}, self.target(2))
        release_failure.set()
        self.await_ready(lambda: all(f.done() for f in pool._pending))
        with self.assertRaises(PublicationError) as error:
            pool.submit(3, {'id': 3}, self.target(3))
        self.assertEqual([r.task_id for r in error.exception.results], [1])
        self.assertEqual([f.task_id for f in error.exception.failures], [2])
        self.assertFalse(self.target(3).exists())

    def test_finish_waits_for_accepted_work_even_after_a_failure(self):
        first_started = threading.Event()
        last_completed = threading.Event()

        def writer(path, payload):
            if payload['id'] == 1:
                first_started.set()
                self.assertTrue(self.release.wait(3))
                raise OSError('first write fails')
            result = write_json(path, payload)
            if payload['id'] == 3:
                last_completed.set()
            return result

        pool = self.pool(workers=1, max_pending=3, writer=writer)
        for task_id in (1, 2, 3):
            self.assertTrue(pool.submit(task_id, {'id': task_id}, self.target(task_id)))
        self.assertTrue(first_started.wait(2))
        timer = threading.Timer(0.1, self.release.set)
        timer.start()
        try:
            with self.assertRaises(PublicationError) as error:
                pool.finish()
        finally:
            timer.join()
        self.assertTrue(last_completed.is_set())
        self.assertEqual({r.task_id for r in error.exception.results}, {2, 3})
        self.assertEqual([f.task_id for f in error.exception.failures], [1])
        self.assertEqual(pool.pending_count, 0)
        with self.assertRaises(PublicationError) as again:
            pool.close()
        self.assertEqual(again.exception.results, ())

    def test_close_waits_and_rejects_later_submit(self):
        started = threading.Event()

        def writer(path, payload):
            started.set()
            self.assertTrue(self.release.wait(3))
            return write_json(path, payload)

        pool = self.pool(workers=1, max_pending=1, writer=writer)
        pool.submit(1, {'id': 1}, self.target(1))
        self.assertTrue(started.wait(2))
        timer = threading.Timer(0.1, self.release.set)
        timer.start()
        try:
            self.assertEqual([r.task_id for r in pool.close()], [1])
        finally:
            timer.join()
        self.assertTrue(self.target(1).exists())
        self.assertEqual(pool.close(), [])
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            pool.submit(2, {'id': 2}, self.target(2))

    def test_unexpected_snapshot_return_fails_closed(self):
        def writer(path, payload):
            return path.parent / '.snapshots' / path.name / 'version.json'

        pool = self.pool(workers=1, max_pending=1, writer=writer)
        pool.submit(1, {'id': 1}, self.target(1))
        with self.assertRaises(PublicationError) as error:
            pool.finish()
        self.assertEqual(error.exception.results, ())
        self.assertIn('snapshot', str(error.exception))

    def test_existing_target_appearing_before_worker_write_is_rejected(self):
        # Occupy the only writer so a second task is accepted but not started.
        first_started = threading.Event()

        def writer(path, payload):
            if payload['id'] == 1:
                first_started.set()
                self.assertTrue(self.release.wait(3))
            return write_json(path, payload)

        pool = self.pool(workers=1, max_pending=2, writer=writer)
        pool.submit(1, {'id': 1}, self.target(1))
        self.assertTrue(first_started.wait(2))
        pool.submit(2, {'id': 2}, self.target(2))
        self.target(2).parent.mkdir(parents=True)
        self.target(2).write_text('{"previous":true}')
        self.release.set()
        with self.assertRaises(PublicationError) as error:
            pool.finish()
        self.assertEqual([r.task_id for r in error.exception.results], [1])
        self.assertEqual([f.task_id for f in error.exception.failures], [2])
        self.assertEqual(self.target(2).read_text(), '{"previous":true}')

    def test_inputs_and_owner_thread_are_validated(self):
        for workers in (0, 129, 1.0, True):
            with self.assertRaises(ValueError):
                BoundedTaskPublisher(workers=workers)
        for bound in (0, 1, 4097, 8.0, True):
            with self.assertRaises(ValueError):
                BoundedTaskPublisher(workers=2, max_pending=bound)
        pool = self.pool(workers=1, max_pending=1)
        for task_id in (-1, '1', True):
            with self.assertRaises(ValueError):
                pool.submit(task_id, {}, self.target(1))
        errors = []

        def wrong_thread():
            try:
                pool.submit(1, {}, self.target(1))
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=wrong_thread)
        thread.start()
        thread.join(2)
        self.assertEqual(len(errors), 1)
        self.assertIn('creating thread', str(errors[0]))


if __name__ == '__main__':
    unittest.main()
