"""Local concurrency and fail-closed fixtures for worker state caching.

Run with ``python3 -m unittest -v work/test_migration_state_cache.py``.
All fixtures use temporary local controls; none import or start real workers.
"""

import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock


WORK_DIR = Path(__file__).absolute().parent
sys.path.insert(0, str(WORK_DIR))

from migration_state_cache import StateCache
import migration_cached_worker
import migration_io


class ManualClock:
    def __init__(self, value=100.0):
        self.value = value
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def advance(self, seconds):
        with self.lock:
            self.value += seconds


class StateReader:
    """A counted reader whose next I/O can be held outside the cache lock."""

    def __init__(self, state):
        self.state = state
        self.error = None
        self.calls = []
        self.lock = threading.Lock()
        self.block = False
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, path):
        with self.lock:
            self.calls.append(path)
            state, error, block = self.state, self.error, self.block
        if block:
            self.entered.set()
            if not self.release.wait(5):
                raise TimeoutError("test reader was not released")
        if error is not None:
            raise error
        return state

    @property
    def call_count(self):
        with self.lock:
            return len(self.calls)


class ReaderGroup:
    """Bounded wait for many readers; a broken lock cannot hang the suite."""

    def __init__(self, operation, count=64):
        self.results = []
        self.errors = []
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.remaining = count
        self.barrier = threading.Barrier(count + 1)

        def run():
            try:
                self.barrier.wait(timeout=3)
                result = operation()
                with self.lock:
                    self.results.append(result)
            except BaseException as exc:
                with self.lock:
                    self.errors.append(exc)
            finally:
                with self.lock:
                    self.remaining -= 1
                    if self.remaining == 0:
                        self.done.set()

        self.threads = [threading.Thread(target=run, daemon=True)
                        for _ in range(count)]
        for thread in self.threads:
            thread.start()
        self.barrier.wait(timeout=3)

    def join(self):
        for thread in self.threads:
            thread.join(timeout=1)


class StateCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="state-cache-test-", dir=WORK_DIR)
        self.addCleanup(self.temp.cleanup)
        self.control = Path(self.temp.name) / "control"
        self.control.mkdir()
        self.state_path = self.control / "state.json"
        self.clock = ManualClock()

    def cache(self, reader, **kwargs):
        kwargs.setdefault("clock", self.clock)
        result = StateCache(self.state_path, reader, **kwargs)
        self.addCleanup(result.close)
        return result

    @staticmethod
    def refresh_allowing_error(cache):
        # Whether refresh_once surfaces an I/O exception is not part of its API;
        # every subsequent worker read must nevertheless fail closed.
        try:
            cache.refresh_once()
        except Exception:
            pass

    def assert_readers_finished(self, group):
        self.assertTrue(group.done.wait(2), "cached readers blocked on unrelated I/O")
        group.join()
        self.assertEqual(group.errors, [])

    def test_64_readers_share_one_io_and_receive_independent_nested_values(self):
        original = {"stage": "p1-r1", "queue": {"items": ["a", {"n": 1}]}}
        reader = StateReader(original)
        cache = self.cache(reader)
        cache.refresh_once()

        def read_and_mutate():
            for _ in range(25):
                value = cache.read_json(self.state_path)
                self.assertEqual(value["stage"], "p1-r1")
                self.assertEqual(value["queue"]["items"], ["a", {"n": 1}])
                value["queue"]["items"][1]["n"] = 999
                value["queue"]["items"].append("private")
            return value["stage"]

        group = ReaderGroup(read_and_mutate)
        self.assert_readers_finished(group)
        self.assertEqual(len(group.results), 64)
        self.assertEqual(reader.call_count, 1)
        self.assertEqual(cache.read(), original)
        self.assertEqual(original["queue"]["items"], ["a", {"n": 1}])

    def test_round_change_becomes_visible_without_leaking_returned_mutations(self):
        reader = StateReader({"stage": "p1-r1", "nested": {"round": [1]}})
        cache = self.cache(reader)
        cache.refresh_once()
        first = cache.read()
        first["nested"]["round"].append(99)
        reader.state = {"stage": "p1-r2", "nested": {"round": [2]}}
        cache.refresh_once()

        group = ReaderGroup(cache.read)
        self.assert_readers_finished(group)
        self.assertEqual(len(group.results), 64)
        for result in group.results:
            self.assertEqual(result, {"stage": "p1-r2", "nested": {"round": [2]}})
        group.results[0]["nested"]["round"].append(100)
        self.assertEqual(group.results[1]["nested"]["round"], [2])
        self.assertEqual(cache.read()["nested"]["round"], [2])
        self.assertEqual(reader.call_count, 2)

    def test_uninitialized_and_every_refresh_error_fail_closed_then_recover(self):
        reader = StateReader({"stage": "p1-r1"})
        cache = self.cache(reader)
        with self.assertRaises(OSError):
            cache.read()
        self.assertEqual(reader.call_count, 0, "worker reads must not initiate NFS I/O")

        failures = [PermissionError("denied"),
                    json.JSONDecodeError("incomplete", "{", 1),
                    RuntimeError("unexpected reader failure")]
        for number, error in enumerate(failures, 1):
            with self.subTest(error=type(error).__name__):
                reader.error = None
                reader.state = {"stage": f"p1-r{number}"}
                cache.refresh_once()
                self.assertEqual(cache.read(), reader.state)
                reader.error = error
                self.refresh_allowing_error(cache)
                calls = reader.call_count
                with self.assertRaises(OSError):
                    cache.read()
                with self.assertRaises(OSError):
                    cache.read_json(str(self.state_path))
                self.assertEqual(reader.call_count, calls)

        reader.error = None
        reader.state = {"stage": "p2-r1"}
        cache.refresh_once()
        self.assertEqual(cache.read(), {"stage": "p2-r1"})

    def test_successful_snapshot_expires_without_more_io_and_can_recover(self):
        reader = StateReader({"stage": "p1-r1"})
        cache = self.cache(reader, max_age=3.0)
        cache.refresh_once()
        self.clock.advance(3.0)
        self.assertEqual(cache.read(), {"stage": "p1-r1"})
        self.clock.advance(0.01)
        with self.assertRaises(OSError):
            cache.read()
        self.assertEqual(reader.call_count, 1)
        reader.state = {"stage": "p2-r1"}
        cache.refresh_once()
        self.assertEqual(cache.read(), {"stage": "p2-r1"})

    def test_blocked_refresh_does_not_block_64_readers_and_old_value_expires(self):
        reader = StateReader({"stage": "p1-r1"})
        cache = self.cache(reader, max_age=3.0)
        cache.refresh_once()
        reader.state = {"stage": "p1-r2"}
        reader.block = True
        refresh_errors = []

        def refresh():
            try:
                cache.refresh_once()
            except BaseException as exc:
                refresh_errors.append(exc)

        refresher = threading.Thread(target=refresh, daemon=True)
        refresher.start()
        groups = []
        try:
            self.assertTrue(reader.entered.wait(1))
            fresh = ReaderGroup(cache.read)
            groups.append(fresh)
            self.assert_readers_finished(fresh)
            self.assertEqual(fresh.results, [{"stage": "p1-r1"}] * 64)

            self.clock.advance(3.01)

            def read_stale():
                with self.assertRaises(OSError):
                    cache.read()
                return "closed"

            stale = ReaderGroup(read_stale)
            groups.append(stale)
            self.assert_readers_finished(stale)
            self.assertEqual(stale.results, ["closed"] * 64)
            self.assertEqual(reader.call_count, 2)
        finally:
            reader.release.set()
            refresher.join(timeout=2)
            for group in groups:
                group.join()
        self.assertFalse(refresher.is_alive())
        self.assertEqual(refresh_errors, [])
        # A delayed result must not make a snapshot whose I/O started more than
        # max_age ago fresh again. A new, timely read is required to recover.
        with self.assertRaises(OSError):
            cache.read()
        reader.block = False
        cache.refresh_once()
        self.assertEqual(cache.read(), {"stage": "p1-r2"})

    def test_only_exact_lexical_state_path_is_cached_and_other_reads_pass_through(self):
        alias = Path(self.temp.name) / "another-control"
        alias.symlink_to(self.control, target_is_directory=True)
        sentinel = {"uncached": []}
        calls = []

        def reader(path):
            calls.append(path)
            if os.fspath(path) == os.fspath(self.state_path):
                return {"stage": "p1-r1"}
            return sentinel

        cache = self.cache(reader)
        cache.refresh_once()
        self.assertEqual(cache.read_json(str(self.state_path)), {"stage": "p1-r1"})
        relative = os.path.relpath(self.state_path)
        self.assertEqual(cache.read_json(relative), {"stage": "p1-r1"})
        self.assertEqual(len(calls), 1)

        for path in (self.control / "config.json", self.control / "STOP",
                     alias / "state.json", self.control.parent / "other" / "state.json"):
            with self.subTest(path=path):
                result = cache.read_json(path)
                self.assertIs(result, sentinel, "non-state reads must pass through unchanged")
                self.assertIs(calls[-1], path, "delegate the caller's original path object")
        self.assertEqual(len(calls), 5)

    def test_stop_creation_and_removal_and_non_state_json_are_live(self):
        calls = []

        def reader(path):
            calls.append(path)
            return json.loads(Path(path).read_text())

        self.state_path.write_text(json.dumps({"stage": "p1-r1"}))
        config = self.control / "config.json"
        config.write_text(json.dumps({"threads": 64}))
        stop = self.control / "STOP"
        cache = self.cache(reader)
        cache.refresh_once()
        self.assertFalse(stop.exists())
        stop.write_text(json.dumps({"stop": True}))
        self.assertTrue(stop.exists())
        self.assertEqual(cache.read_json(stop), {"stop": True})
        stop.unlink()
        self.assertFalse(stop.exists())
        with self.assertRaises(FileNotFoundError):
            cache.read_json(stop)
        self.assertEqual(cache.read_json(config), {"threads": 64})
        config.write_text(json.dumps({"threads": 128}))
        self.assertEqual(cache.read_json(config), {"threads": 128})
        self.state_path.write_text(json.dumps({"stage": "p2-r1"}))
        self.assertEqual(cache.read(), {"stage": "p1-r1"})
        self.assertEqual(sum(Path(path) == self.state_path for path in calls), 1)

    def test_real_immutable_publication_advances_only_after_state_refresh(self):
        first = {"stage": "p1-r1", "nested": {"round": 1}}
        second = {"stage": "p1-r2", "nested": {"round": 2}}
        first_version = migration_io.write_json(self.state_path, first)
        cache = self.cache(migration_io.read_json)
        cache.refresh_once()
        self.assertEqual(cache.read(), first)
        second_version = migration_io.write_json(self.state_path, second)
        self.assertNotEqual(first_version, second_version)
        self.assertEqual(json.loads(self.state_path.read_text()), first,
                         "canonical compatibility file should remain unchanged")
        self.assertEqual(migration_io.read_json(self.state_path), second)
        self.assertEqual(cache.read(), first)
        cache.refresh_once()
        self.assertEqual(cache.read(), second)
        self.assertTrue(first_version.exists(), "old immutable versions must remain intact")

        config = self.control / "config.json"
        migration_io.write_json(config, {"threads": 64})
        self.assertEqual(cache.read_json(config), {"threads": 64})
        migration_io.write_json(config, {"threads": 128})
        self.assertEqual(json.loads(config.read_text()), {"threads": 64})
        self.assertEqual(cache.read_json(config), {"threads": 128})

    def test_start_refreshes_immediately_in_background_and_close_stops_refreshing(self):
        first = threading.Event()
        second = threading.Event()
        lock = threading.Lock()
        calls = []

        def reader(path):
            with lock:
                calls.append(path)
                count = len(calls)
            first.set()
            if count >= 2:
                second.set()
            return {"stage": "p1-r1"}

        cache = self.cache(reader, refresh_interval=0.05)
        cache.start()
        self.assertTrue(first.wait(0.5), "first refresh should not wait for an interval")
        self.assertTrue(second.wait(1), "background refresh must repeat")
        self.assertEqual(cache.read(), {"stage": "p1-r1"})
        cache.close(timeout=1)
        with lock:
            stopped_count = len(calls)
        threading.Event().wait(0.12)
        with lock:
            self.assertEqual(len(calls), stopped_count)
        with self.assertRaises(OSError):
            cache.read()


class CachedWorkerWrapperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="cached-worker-test-", dir=WORK_DIR)
        self.addCleanup(self.temp.cleanup)
        self.control = Path(self.temp.name) / "control"
        self.control.mkdir()
        self.state_path = self.control / "state.json"
        self.state_path.write_text(json.dumps({"stage": "p1-r1"}))
        self.config_path = self.control / "config.json"
        self.config_path.write_text(json.dumps({"threads": 64}))

    def exercise_wrapper(self, kind, run_error=None):
        calls = []
        caches = []
        run_calls = []

        def original_reader(path):
            calls.append(path)
            return json.loads(Path(path).read_text())

        class SynchronousCache(StateCache):
            # Keep wrapper fixtures deterministic; background behavior is
            # exercised separately with the real start implementation above.
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.closed_by_wrapper = False
                caches.append(self)

            def start(self):
                self.refresh_once()
                return self

            def close(self, timeout=1):
                self.closed_by_wrapper = True
                super().close(timeout=timeout)

        script_name = ("migration_run.py" if kind == "primary"
                       else "migration_worker128_run.py")
        script = str(WORK_DIR / script_name)
        expected_argv = [script] + (["worker"] if kind == "primary" else [])
        expected_argv += ["--control", str(self.control), "--threads", "64", "--nice", "10"]
        previous_argv = sys.argv
        previous_path = list(sys.path)

        def run_path(path, run_name):
            run_calls.append((path, run_name, list(sys.argv)))
            self.assertEqual(path, script)
            self.assertEqual(run_name, "__main__")
            self.assertEqual(sys.argv, expected_argv)
            self.assertEqual(sys.path[0], str(WORK_DIR))
            cache = caches[0]
            self.assertIs(cache.reader, original_reader, "saved reader must avoid dispatcher recursion")
            self.assertIs(migration_io.read_json.__self__, cache)

            self.assertEqual(migration_io.read_json(self.state_path), {"stage": "p1-r1"})
            self.state_path.write_text(json.dumps({"stage": "p2-r1"}))
            for _ in range(64):
                self.assertEqual(migration_io.read_json(str(self.state_path)), {"stage": "p1-r1"})
            self.assertEqual(sum(Path(item) == self.state_path for item in calls), 1)
            self.assertEqual(migration_io.read_json(self.config_path), {"threads": 64})
            self.config_path.write_text(json.dumps({"threads": 128}))
            self.assertEqual(migration_io.read_json(self.config_path), {"threads": 128})

            alias = Path(self.temp.name) / "alias-control"
            alias.symlink_to(self.control, target_is_directory=True)
            self.assertEqual(migration_io.read_json(alias / "state.json"), {"stage": "p2-r1"})
            self.assertEqual(Path(calls[-1]), alias / "state.json")
            stop = self.control / "STOP"
            self.assertFalse(stop.exists())
            stop.write_text(json.dumps({"stop": True}))
            self.assertTrue(stop.exists())
            self.assertEqual(migration_io.read_json(stop), {"stop": True})
            stop.unlink()
            self.assertFalse(stop.exists())
            with self.assertRaises(FileNotFoundError):
                migration_io.read_json(stop)
            if run_error is not None:
                raise run_error

        args = [kind, "--control", str(self.control),
                "--cache-refresh-interval", "0.2", "--cache-max-age", "2.0",
                "--threads", "64", "--nice", "10"]
        with mock.patch.object(migration_io, "read_json", original_reader), \
                mock.patch.object(migration_cached_worker, "StateCache", SynchronousCache), \
                mock.patch.object(migration_cached_worker.runpy, "run_path", side_effect=run_path):
            if run_error is None:
                self.assertEqual(migration_cached_worker.main(args), 0)
            else:
                with self.assertRaises(type(run_error)) as caught:
                    migration_cached_worker.main(args)
                self.assertIs(caught.exception, run_error)
            self.assertIs(migration_io.read_json, original_reader)
        self.assertIs(sys.argv, previous_argv)
        self.assertEqual(sys.path, previous_path)
        self.assertEqual(len(run_calls), 1)
        self.assertEqual(len(caches), 1)
        self.assertTrue(caches[0].closed_by_wrapper)
        self.assertEqual(caches[0].refresh_interval, 0.2)
        self.assertEqual(caches[0].max_age, 2.0)
        with self.assertRaises(OSError):
            caches[0].read()

    def test_primary_keeps_original_worker_and_cli_and_restores_globals(self):
        self.exercise_wrapper("primary")

    def test_supplement_keeps_original_worker_and_cli_and_restores_globals(self):
        self.exercise_wrapper("supplement")

    def test_worker_system_exit_still_closes_cache_and_restores_globals(self):
        self.exercise_wrapper("primary", SystemExit(7))

    def test_invalid_cache_settings_restore_globals_without_starting_worker(self):
        previous_argv = sys.argv
        previous_path = list(sys.path)
        original_reader = migration_io.read_json
        try:
            with mock.patch.object(migration_cached_worker.runpy, "run_path") as run_path:
                with self.assertRaises(ValueError):
                    migration_cached_worker.main([
                        "primary", "--control", str(self.control),
                        "--cache-refresh-interval", "1", "--cache-max-age", "0.5"])
                run_path.assert_not_called()
            self.assertIs(migration_io.read_json, original_reader)
            self.assertIs(sys.argv, previous_argv)
            self.assertEqual(sys.path, previous_path)
        finally:
            # Keep a failure in this regression fixture from contaminating
            # unrelated tests in the same Python interpreter.
            sys.argv = previous_argv
            sys.path[:] = previous_path


if __name__ == "__main__":
    unittest.main()
