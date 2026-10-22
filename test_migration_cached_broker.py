"""Local fixtures only; never bind a broker port or contact remote nodes."""

import errno
import json
from pathlib import Path
import runpy
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock


WORK = Path(__file__).absolute().parent
sys.path.insert(0, str(WORK))
import migration_broker
import migration_cached_broker as wrapper
import migration_io
from migration_state_cache import StateCache


class CachedBrokerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=WORK, prefix='cached-broker-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.control = Path(self.temp.name)
        (self.control / 'state.json').write_text(json.dumps({'status': 'scanning', 'stage': 'p3-r1'}))
        (self.control / 'config.json').write_text(json.dumps({'threads': 64}))

    def exercise_wrapper(self, server_error=None, fresh_import=False):
        calls, caches, runs = [], [], []
        original_argv, original_path = sys.argv, list(sys.path)
        original_legacy_alias = migration_broker.read_json
        original_legacy_class = migration_broker.Broker
        real_run_path = runpy.run_path

        def reader(path):
            calls.append(Path(path))
            return json.loads(Path(path).read_text())

        class FixtureCache(StateCache):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                caches.append(self)

            def start(self):
                self.refresh_once()
                return self

        def pretend_server(path, run_name):
            runs.append((path, run_name, list(sys.argv)))
            self.assertEqual(Path(path), WORK / 'migration_broker_parallel.py')
            self.assertEqual(run_name, '__main__')
            self.assertEqual(sys.argv, [path, '--control', str(self.control), '--port', '38123'])
            cache = caches[0]
            self.assertIs(cache.reader, reader)
            self.assertIs(migration_io.read_json.__self__, cache)
            legacy = sys.modules['migration_broker']
            self.assertIs(legacy.read_json.__self__, cache)
            # Execute the real unchanged definitions, without its __main__
            # branch. This proves its import alias is patched without a port.
            namespace = real_run_path(path, run_name='cached_broker_fixture')
            self.assertIs(namespace['read_json'].__self__, cache)
            self.assertIs(namespace['Broker'].__mro__[1], legacy.Broker)
            state = self.control / 'state.json'
            state.write_text(json.dumps({'status': 'scanning', 'stage': 'p3-r2'}))
            for _ in range(64):
                self.assertEqual(namespace['read_json'](state)['stage'], 'p3-r1')
            self.assertEqual(calls.count(state), 1)
            for name in ('config.json', 'receipt.json', 'node.json'):
                path = self.control / name
                path.write_text(json.dumps({'version': 1}))
                self.assertEqual(namespace['read_json'](path), {'version': 1})
                path.write_text(json.dumps({'version': 2}))
                self.assertEqual(namespace['read_json'](path), {'version': 2})
                self.assertEqual(calls.count(path), 2)
            # Original parallel main rebinds this class; cleanup must restore it.
            legacy.Broker = object()
            if server_error is not None:
                raise server_error

        with mock.patch.dict(sys.modules), \
                mock.patch.object(migration_io, 'read_json', reader), \
                mock.patch.object(wrapper, 'StateCache', FixtureCache), \
                mock.patch.object(wrapper.runpy, 'run_path', side_effect=pretend_server):
            if fresh_import:
                sys.modules.pop('migration_broker', None)
            args = ['--control', str(self.control), '--port', '38123']
            if server_error is None:
                self.assertEqual(wrapper.main(args), 0)
            else:
                with self.assertRaises(type(server_error)) as caught:
                    wrapper.main(args)
                self.assertIs(caught.exception, server_error)
            self.assertIs(migration_io.read_json, reader)
            restored = sys.modules['migration_broker']
            self.assertIs(restored.read_json, reader if fresh_import else original_legacy_alias)
            if not fresh_import:
                self.assertIs(restored.Broker, original_legacy_class)
        self.assertEqual(len(runs), 1)
        self.assertIs(sys.argv, original_argv)
        self.assertEqual(sys.path, original_path)
        with self.assertRaises(OSError):
            caches[0].read()

    def test_cli_aliases_non_state_passthrough_and_cleanup(self):
        self.exercise_wrapper()

    def test_fresh_module_alias_is_restored_to_original_reader(self):
        self.exercise_wrapper(fresh_import=True)

    def test_server_system_exit_restores_aliases_and_globals(self):
        self.exercise_wrapper(SystemExit(7))

    def test_stop_refuses_start_before_cache_or_broker(self):
        (self.control / 'STOP').touch()
        with mock.patch.object(wrapper, 'StateCache') as cache, \
                mock.patch.object(wrapper.runpy, 'run_path') as run:
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                wrapper.main(['--control', str(self.control), '--port', '38123'])
            cache.assert_not_called()
            run.assert_not_called()

    def test_ready_wait_has_bounded_failure_and_observes_new_stop(self):
        class Unready:
            state_path = str(self.control / 'state.json')
            def read(self):
                raise OSError('no fresh state')
        start = time.monotonic()
        with self.assertRaises(OSError) as caught:
            wrapper.wait_until_ready(Unready(), self.control, timeout=0.03)
        self.assertEqual(caught.exception.errno, errno.ETIMEDOUT)
        self.assertLess(time.monotonic() - start, 1)
        timer = threading.Timer(0.01, (self.control / 'STOP').touch)
        timer.start()
        try:
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                wrapper.wait_until_ready(Unready(), self.control, timeout=0.5)
        finally:
            timer.join()

    def test_ready_wait_recovers_from_cold_background_read(self):
        ready = threading.Event()
        def reader(path):
            if not ready.is_set():
                raise FileNotFoundError(path)
            return {'stage': 'p3-r1'}
        with StateCache(self.control / 'state.json', reader, refresh_interval=0.01, max_age=1) as cache:
            timer = threading.Timer(0.02, ready.set)
            timer.start()
            try:
                wrapper.wait_until_ready(cache, self.control, timeout=0.5)
                self.assertEqual(cache.read(), {'stage': 'p3-r1'})
            finally:
                timer.join()


if __name__ == '__main__':
    unittest.main()
