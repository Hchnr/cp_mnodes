"""Pure in-memory scheduling fixtures: no processes, SSH, signals or filesystem work."""
import collections
import queue
import threading
import time
import unittest

from parent_scheduler import LeaseError, ParentQueue


def task(identifier, parent='hot', ino=1, flat=True):
    result = dict(id=str(identifier), parent=parent, ino=ino)
    if flat:
        result['flat_batch'] = True
    return result


class ParentSchedulerTests(unittest.TestCase):
    def scheduler(self, **kwargs):
        return ParentQueue({'a': 'boot1', 'b': 'boot1', 'c': 'boot2'}, **kwargs)

    def test_flat_parent_cap_is_shared_by_nodes_on_one_boot_not_other_boots(self):
        q = self.scheduler(limit=2)
        for n in range(6):
            q.put(task(n))
        first, second = q.claim('a'), q.claim('b')
        with self.assertRaises(queue.Empty):
            q.claim('a')
        q.claim('c')
        q.claim('c')
        with self.assertRaises(queue.Empty):
            q.claim('c')
        self.assertEqual(q.snapshot()['active_by_boot'], {'boot1': 2, 'boot2': 2})
        q.release(first['id'], 'a')
        q.claim('b')
        self.assertEqual(q.qsize(), 1)

    def test_independent_directories_with_same_cwd_do_not_consume_flat_quota(self):
        q = self.scheduler(limit=1)
        for n in range(12):
            q.put(task(n, flat=False))
        for n in range(12):
            q.claim('a' if n % 2 else 'b')
        self.assertTrue(q.empty())
        self.assertEqual(q.snapshot()['active_leases'], 12)
        self.assertEqual(q.snapshot()['active_flat_groups'], 0)

    def test_flat_blockage_does_not_hide_same_parent_independent_tasks(self):
        q = self.scheduler(limit=1)
        q.put(task('flat-first'))
        q.put(task('flat-blocked'))
        for n in range(4):
            q.put(task('directory-'+str(n), flat=False))
        first = q.claim('a')
        self.assertEqual(first['id'], 'flat-first')
        for n in range(4):
            self.assertEqual(q.claim('b')['id'], 'directory-'+str(n))
        self.assertEqual(q.qsize(), 1)
        with self.assertRaises(queue.Empty):
            q.claim('b')
        q.release(first['id'], 'a')
        self.assertEqual(q.claim('b')['id'], 'flat-blocked')

    def test_only_literal_true_enables_quota(self):
        q = self.scheduler(limit=1)
        for n, marker in enumerate((1, 'true', False, None)):
            q.put(dict(task(n, flat=False), flat_batch=marker))
        for _ in range(4):
            q.claim('a')
        self.assertEqual(q.snapshot()['active_flat_groups'], 0)

    def test_parent_round_robin_prevents_hot_parent_starving_cold_parent(self):
        q = self.scheduler(limit=None)
        for n in range(10):
            q.put(task('hot'+str(n)))
        for n in range(3):
            q.put(task('cold'+str(n), parent='cold', ino=2))
        selected = [q.claim('a') for _ in range(6)]
        self.assertEqual([t['parent'] for t in selected], ['hot', 'cold']*3)
        self.assertEqual(q.snapshot()['parent_switches'], 5)

    def test_inode_is_part_of_parent_quota_identity(self):
        q = self.scheduler(limit=1)
        q.put(task('old', ino=1))
        q.put(task('new', ino=2))
        self.assertEqual(q.claim('a')['id'], 'old')
        self.assertEqual(q.claim('b')['id'], 'new')

    def test_total_pending_backpressure_covers_all_parents_and_both_kinds(self):
        q = self.scheduler(maxsize=2)
        q.put(task('one'))
        q.put(task('two', parent='cold', flat=False))
        with self.assertRaises(queue.Full):
            q.put(task('three'), timeout=.01)
        entered, completed = threading.Event(), threading.Event()
        errors = []
        def produce():
            entered.set()
            try:
                q.put(task('three'), timeout=1)
                completed.set()
            except Exception as exc:
                errors.append(exc)
        thread = threading.Thread(target=produce)
        thread.start()
        self.assertTrue(entered.wait(1))
        self.assertFalse(completed.wait(.01))
        q.claim('a')
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(completed.is_set())
        self.assertEqual(q.qsize(), 2)

    def test_pending_all_blocked_is_not_empty_or_finished(self):
        q = self.scheduler(limit=1)
        for n in range(3):
            q.put(task(n))
        q.claim('a')
        finished = threading.Event()
        for producer_finished in (False, True):
            if producer_finished:
                finished.set()
            with self.assertRaises(queue.Empty):
                q.claim('b')
            self.assertFalse(q.empty())
            self.assertFalse(finished.is_set() and q.empty())
        self.assertEqual(q.qsize(), 2)

    def test_release_including_rc1_is_exact_and_double_release_cannot_free_another_task(self):
        q = self.scheduler(limit=1)
        q.put(task('retry-later'))
        q.put(task('next'))
        current = q.claim('a')
        with self.assertRaises(LeaseError):
            q.release(current['id'], 'b')
        self.assertEqual(q.snapshot()['active_leases'], 1)
        # The rc=1 result follows exactly the same release API as rc=0.
        result = dict(id=current['id'], rc=1)
        q.release(result['id'], 'a')
        next_task = q.claim('b')
        with self.assertRaises(LeaseError):
            q.release(current['id'], 'a')
        self.assertEqual(q.snapshot()['active_leases'], 1)
        q.release(next_task['id'], 'b')
        self.assertEqual(q.snapshot()['active_leases'], 0)

    def test_pending_or_active_task_id_cannot_be_enqueued_twice(self):
        q = self.scheduler()
        q.put(task('unique'))
        with self.assertRaises(LeaseError):
            q.put(task('unique'))
        q.claim('a')
        with self.assertRaises(LeaseError):
            q.put(task('unique'))

    def test_release_node_after_exit_only_releases_that_nodes_leases(self):
        q = self.scheduler(limit=2)
        for n in range(4):
            q.put(task(n))
        q.claim('a')
        q.claim('b')
        self.assertEqual(q.release_node('a'), 1)
        self.assertEqual(q.release_node('a'), 0)
        self.assertEqual(q.snapshot()['active_by_boot'], {'boot1': 1})
        q.claim('a')
        self.assertEqual(q.snapshot()['active_by_boot'], {'boot1': 2})

    def test_lower_limit_does_not_cancel_inflight_and_drains_naturally(self):
        q = self.scheduler(limit=4)
        for n in range(6):
            q.put(task(n))
        leases = [q.claim('a') for _ in range(4)]
        self.assertEqual(q.set_limit(1), 4)
        self.assertEqual(q.snapshot()['active_leases'], 4)
        for lease in leases[:-1]:
            q.release(lease['id'], 'a')
            with self.assertRaises(queue.Empty):
                q.claim('b')
        q.release(leases[-1]['id'], 'a')
        self.assertEqual(q.claim('b')['id'], '4')
        with self.assertRaises(queue.Empty):
            q.claim('a')

    def test_disabling_limit_unblocks_flat_work_without_touching_leases(self):
        q = self.scheduler(limit=1)
        for n in range(5):
            q.put(task(n))
        q.claim('a')
        q.set_limit(None)
        for _ in range(4):
            q.claim('b')
        self.assertEqual(q.snapshot()['active_leases'], 5)
        self.assertTrue(q.empty())

    def test_stop_wakes_waiting_claim_and_put_without_releasing_inflight(self):
        q = self.scheduler(limit=1, maxsize=1)
        q.put(task('active'))
        active = q.claim('a')
        q.put(task('pending'))
        caught = []
        def claim():
            try:q.claim('b', timeout=None)
            except queue.Empty:caught.append('claim')
        def put():
            try:q.put(task('waiting'), timeout=None)
            except queue.Full:caught.append('put')
        threads = [threading.Thread(target=f) for f in (claim, put)]
        for thread in threads:thread.start()
        q.stop()
        for thread in threads:
            thread.join(1)
            self.assertFalse(thread.is_alive())
        self.assertCountEqual(caught, ['claim', 'put'])
        self.assertEqual(q.qsize(), 1)
        self.assertEqual(q.snapshot()['active_leases'], 1)
        q.release(active['id'], 'a')
        self.assertEqual(q.snapshot()['active_leases'], 0)

    def test_complete_boot_mapping_unknown_nodes_and_safe_registration(self):
        with self.assertRaises(ValueError):
            ParentQueue({'a': 'boot1'}, nodes=['a', 'b'])
        q = ParentQueue({'a': 'boot1'}, nodes=['a'], limit=1)
        for n in range(3):q.put(task(n))
        with self.assertRaises(KeyError):q.claim('new')
        q.claim('a')
        with self.assertRaises(LeaseError):q.register_node('a', 'different-boot')
        self.assertTrue(q.register_node('new', 'boot2'))
        self.assertFalse(q.register_node('new', 'boot2'))
        self.assertEqual(q.claim('new')['id'], '1')

    def test_thread_races_do_not_duplicate_or_exceed_shared_boot_flat_cap(self):
        mapping = {'n'+str(i): 'boot'+str(i % 2) for i in range(8)}
        q = ParentQueue(mapping, maxsize=16, limit=2)
        done, mutex = threading.Event(), threading.Lock()
        seen, running, errors = [], collections.Counter(), []
        def consume(node):
            while not (done.is_set() and q.empty()):
                try:current = q.claim(node, timeout=.02)
                except queue.Empty:continue
                try:
                    with mutex:
                        running[mapping[node]] += 1
                        if running[mapping[node]] > 2:errors.append('quota exceeded')
                        seen.append(current['id'])
                    time.sleep(.001)
                    with mutex:running[mapping[node]] -= 1
                finally:q.release(current['id'], node)
        threads = [threading.Thread(target=consume, args=(node,)) for node in mapping]
        for thread in threads:thread.start()
        for n in range(120):q.put(task(n), timeout=2)
        done.set()
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(seen), 120)
        self.assertEqual(len(set(seen)), 120)
        self.assertEqual(q.snapshot()['active_leases'], 0)
        self.assertTrue(q.empty())


if __name__ == '__main__':
    unittest.main()
