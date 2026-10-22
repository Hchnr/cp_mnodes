"""Local metadata-only probe service fixtures; no remote mounts or deployment."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

import migration_target_probe as probe


class TargetProbeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='target-probe-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.control, self.source, self.destination = (self.root / x for x in ('control', 'source', 'destination'))
        for path in (self.control, self.source, self.destination):
            path.mkdir()
        self.cfg = {'source': probe.SOURCE, 'destination': probe.DESTINATION,
                    'broker_token': 'fixture-private-token', 'run_id': 'fixture-run'}
        self.service = probe.ProbeService(self.control, self.cfg, slots=2)
        # Scope validation has passed for the real production mapping. Only the
        # local fixture substitutes payload roots after construction.
        self.service.source_root = str(self.source)
        self.service.destination_root = str(self.destination)
        self.service.identity = {'pid': os.getpid(), 'host': 'fixture-host', 'endpoint': 'http://fixture'}

    def file(self, root, rel, body=b'fixture'):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return path

    def server(self):
        server = probe.BoundedProbeServer(('127.0.0.1', 0), self.service)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        def cleanup():
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        self.addCleanup(cleanup)
        return server

    def request(self, server, body=None, auth=True, method='POST', route='/probe', headers=None):
        connection = http.client.HTTPConnection(*server.server_address, timeout=5)
        self.addCleanup(connection.close)
        fields = {'Authorization': 'Bearer ' + self.cfg['broker_token']} if auth else {}
        fields.update(headers or {})
        data = json.dumps(body).encode() if isinstance(body, dict) else body
        connection.request(method, route, body=data, headers=fields)
        response = connection.getresponse()
        raw = response.read()
        return response.status, json.loads(raw) if raw else None

    def test_existing_returns_none_and_leaves_source_and_target_unchanged(self):
        source = self.file(self.source, 'data/a', b'original')
        destination = self.file(self.destination, 'data/a', b'different existing target')
        before = (source.read_bytes(), destination.read_bytes(), source.stat().st_mtime_ns, destination.stat().st_mtime_ns)
        result = self.service.probe({'rel': 'data/a', 'kind': 'file', '_refresh_source': True})
        self.assertEqual(result, {'ok': True, 'missing': False, 'entry': None, 'reason': 'existing'})
        after = (source.read_bytes(), destination.read_bytes(), source.stat().st_mtime_ns, destination.stat().st_mtime_ns)
        self.assertEqual(after, before)

    def test_missing_returns_entry_without_creating_any_target_or_parents(self):
        entry = {'rel': 'new/nested/file', 'kind': 'file', 'size': 40}
        self.assertEqual(self.service.probe(entry)['entry'], entry)
        self.assertFalse((self.destination / 'new').exists())

    def test_missing_refreshes_changed_source_identity_and_preserves_known(self):
        source = self.file(self.source, 'data/a', b'new body after old inventory')
        known = {'owned': True, 'sha256': 'a' * 64}
        result = self.service.probe({'rel': 'data/a', 'kind': 'file', 'size': 1,
                                     '_refresh_source': True, 'known': known})
        entry = result['entry']
        self.assertTrue(result['missing'])
        self.assertEqual(entry['size'], source.stat().st_size)
        self.assertEqual(entry['ino'], source.stat().st_ino)
        self.assertEqual(entry['known'], known)
        self.assertNotIn('_refresh_source', entry)
        self.assertFalse((self.destination / 'data').exists())

    def test_source_symlink_metadata_is_preserved_without_following_target(self):
        (self.source / 'link').symlink_to('/outside/need-not-exist')
        result = self.service.probe({'rel': 'link', 'kind': 'file', '_refresh_source': True})
        self.assertEqual(result['entry']['kind'], 'symlink')
        self.assertEqual(result['entry']['target'], '/outside/need-not-exist')
        self.assertEqual(result['entry']['link_target'], '/outside/need-not-exist')

    def test_symlink_target_parent_and_source_parent_are_rejected(self):
        (self.destination / 'unsafe').symlink_to(self.source, target_is_directory=True)
        with self.assertRaises(probe.ProbeError) as error:
            self.service.probe({'rel': 'unsafe/file', 'kind': 'file'})
        self.assertEqual(error.exception.status, 409)
        (self.source / 'unsafe2').symlink_to(self.destination, target_is_directory=True)
        with self.assertRaises(probe.ProbeError):
            self.service.probe({'rel': 'unsafe2/file', 'kind': 'file', '_refresh_source': True})
        self.assertFalse((self.destination / 'unsafe2').exists())

    def test_unsafe_paths_and_priority1_2_are_rejected_before_any_filesystem_probe(self):
        for rel in ('', '.', '..', '../escape', '/absolute', 'a/../b', 'a//b', 'a/./b', 'a\x00b',
                    'home/hcr', 'home/hcr/private', 'repos/cosmos-framework-pwm',
                    'repos/cosmos-framework-pwm/private'):
            with mock.patch.object(probe, '_open_parent') as opener:
                with self.assertRaises(probe.ProbeError) as error:
                    self.service.probe({'rel': rel, 'kind': 'file'})
                self.assertEqual(error.exception.status, 400)
                opener.assert_not_called()
        # Same names below another project-relative prefix remain in phase3.
        self.assertEqual(probe.phase3_relative({'rel': 'other/home/hcr/a'}), 'other/home/hcr/a')

    def test_stop_is_checked_before_metadata_and_after_a_probe(self):
        (self.control / 'STOP').write_text('fixture stop\n')
        with mock.patch.object(probe, '_open_parent') as opener:
            with self.assertRaises(probe.ProbeError) as error:
                self.service.probe({'rel': 'data/a'})
            self.assertEqual(error.exception.status, 503)
            opener.assert_not_called()
        (self.control / 'STOP').unlink()
        with mock.patch.object(self.service, 'stopped', side_effect=[False, True]):
            with self.assertRaises(probe.ProbeError) as error:
                self.service.probe({'rel': 'data/a'})
            self.assertEqual(error.exception.status, 503)

    def test_filesystem_slots_are_bounded_and_busy_requests_do_not_do_metadata(self):
        gate = threading.Event()
        entered = threading.Event()
        original = self.service.stopped
        def wait():
            entered.set()
            gate.wait(3)
            return original()
        with mock.patch.object(self.service, 'stopped', side_effect=wait):
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                first = pool.submit(self.service.probe, {'rel': 'a'})
                self.assertTrue(entered.wait(1))
                second = pool.submit(self.service.probe, {'rel': 'b'})
                deadline = time.monotonic() + 1
                while self.service.inflight < 2 and time.monotonic() < deadline:
                    time.sleep(.001)
                with self.assertRaises(probe.ProbeError) as error:
                    self.service.probe({'rel': 'c'})
                self.assertEqual(error.exception.status, 503)
                self.assertEqual(self.service.peak_inflight, 2)
                gate.set()
                self.assertTrue(first.result()['missing'])
                self.assertTrue(second.result()['missing'])
        self.assertEqual(self.service.inflight, 0)

    def test_http_auth_health_and_body_limit_do_not_expose_private_token(self):
        server = self.server()
        status, value = self.request(server, {'entry': {'rel': 'data/a'}}, auth=False)
        self.assertEqual(status, 401)
        self.assertNotIn(self.cfg['broker_token'], json.dumps(value))
        status, value = self.request(server, method='GET', route='/health')
        self.assertEqual(status, 200)
        self.assertEqual(value['host'], 'fixture-host')
        self.assertNotIn(self.cfg['broker_token'], json.dumps(value))
        status, value = self.request(server, b'x' * (probe.MAX_BODY + 1))
        self.assertEqual(status, 413)
        status, value = self.request(server, {'entry': {'rel': 'data/a'}})
        self.assertEqual(status, 200)
        self.assertTrue(value['missing'])
        status, value = self.request(server, {'entry': {'rel': 'home/hcr/a'}})
        self.assertEqual(status, 400)
        (self.control / 'STOP').write_text('fixture stop\n')
        status, value = self.request(server, {'entry': {'rel': 'data/a'}})
        self.assertEqual(status, 503)

    def test_scope_and_slot_validation_are_fixed(self):
        for cfg in (dict(self.cfg, source='/home'), dict(self.cfg, destination='/tmp/other'),
                    dict(self.cfg, broker_token='')):
            with self.assertRaises(ValueError):
                probe.ProbeService(self.control, cfg)
        for slots in (0, 33, True, 3.0):
            with self.assertRaises(ValueError):
                probe.ProbeService(self.control, self.cfg, slots=slots)


if __name__ == '__main__':
    unittest.main()
