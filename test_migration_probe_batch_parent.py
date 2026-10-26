"""The full existing metadata contract plus request-local descriptor ownership."""

import concurrent.futures
import os
import threading
import unittest
from unittest import mock

import migration_linux_open as opening
import migration_probe_batch_parent as wrapper
import migration_probe_fast_open as fast
import migration_target_probe as probe
import test_migration_target_probe as fixtures


class BatchParentContractTests(fixtures.TargetProbeTests):
    def setUp(self):
        super().setUp()
        for obj, name in ((probe, '_open_parent'), (probe.ProbeService, 'probe_batch'),
                          (probe.ProbeService, 'health')):
            patch = mock.patch.object(obj, name, getattr(obj, name))
            patch.start()
            self.addCleanup(patch.stop)
        wrapper.install()

    def test_same_parent_opens_once_per_request_but_still_stats_every_leaf(self):
        (self.destination / 'data').mkdir()
        for name in ('a', 'b'):
            (self.destination / 'data' / name).write_bytes(b'fixture')
        entries = [{'rel': 'data/a'}, {'rel': 'data/b'}, {'rel': 'data/a'}]
        with mock.patch.object(opening, 'open_parent_readonly', wraps=opening.open_parent_readonly) as op:
            a = self.service.probe_batch(entries)
            self.assertEqual(op.call_count, 1)
            b = self.service.probe_batch(entries)
            self.assertEqual(op.call_count, 2)
        self.assertEqual(a, b)
        self.assertTrue(all(row['result']['missing'] is False for row in a['results']))
        counts = self.service.health()['counts']
        self.assertEqual(counts['requests'], 6)
        self.assertEqual(counts['existing'], 6)
        self.assertEqual(counts['batch_parent_opens'], 2)
        self.assertEqual(counts['batch_parent_reuses'], 4)

    def test_unrelated_parent_opens_do_not_hold_global_cache_lock(self):
        parents = wrapper.BatchParents(self.service)
        entered = threading.Barrier(2)
        fds = []
        original = opening.open_parent_readonly

        def opened(root, rel):
            entered.wait(timeout=3)
            return original(root, rel)

        for name in ('one', 'two'):
            (self.destination / name).mkdir()
        with mock.patch.object(opening, 'open_parent_readonly', side_effect=opened):
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                fds = list(pool.map(parents.open, ('one/a', 'two/b')))
        for fd, _ in fds:
            os.close(fd)
        parents.close()

    def test_refresh_source_is_not_shared_with_target_parent(self):
        (self.destination / 'data').mkdir()
        (self.source / 'data').mkdir()
        (self.source / 'data/a').write_bytes(b'new')
        entries = [{'rel': 'data/a', '_refresh_source': True}] * 2
        with mock.patch.object(opening, 'open_parent_readonly', wraps=opening.open_parent_readonly) as op:
            result = self.service.probe_batch(entries)
        self.assertEqual(op.call_count, 3)  # one target parent, two fresh source observations
        self.assertTrue(all(r['result']['entry']['size'] == 3 for r in result['results']))

    def test_directory_descriptors_close_after_success_and_failed_parent(self):
        opened = []
        original = opening.open_parent_readonly

        def tracked(*args):
            fd, name = original(*args)
            opened.append(fd)
            return fd, name

        with mock.patch.object(opening, 'open_parent_readonly', side_effect=tracked):
            self.service.probe_batch([{'rel': 'a'}, {'rel': 'absent/a'}])
        self.assertTrue(opened)
        for fd in opened:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_partial_submit_failure_drains_accepted_work_before_parent_close(self):
        entered, finish = threading.Event(), threading.Event()
        original = self.service.batch_pool.submit
        accepted = []

        def submit(fn, entry):
            if accepted:
                entered.wait(timeout=3)
                finish.set()
                raise RuntimeError('fixture executor is closing')

            def delayed(value):
                result = fn(value)
                entered.set()
                finish.wait(timeout=3)
                return result

            future = original(delayed, entry)
            accepted.append(future)
            return future

        with mock.patch.object(self.service.batch_pool, 'submit', side_effect=submit):
            with self.assertRaises(probe.ProbeError) as failure:
                self.service.probe_batch([{'rel': 'a'}, {'rel': 'b'}])
        self.assertEqual(failure.exception.status, 503)
        self.assertTrue(accepted[0].done())
        self.assertIsNone(getattr(wrapper._context, 'parents', None))

    def test_single_probe_is_unchanged_and_health_reports_request_scope(self):
        with mock.patch.object(opening, 'open_parent_readonly', wraps=opening.open_parent_readonly) as op:
            self.service.probe({'rel': 'a'})
            self.assertEqual(op.call_count, 1)
        self.assertEqual(self.service.health()['batch_parent_reuse'], 'request-scoped')
        self.assertNotIn('batch_parent_opens', self.service.health()['counts'])

    def test_main_preserves_policy_reloader_and_cli(self):
        with mock.patch.object(fast, 'main') as main:
            wrapper.main()
        main.assert_called_once_with(installer=wrapper.install)


if __name__ == '__main__':
    unittest.main()
