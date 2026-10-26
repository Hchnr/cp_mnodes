"""Private client fixtures; no real broker or migration process is contacted."""
import ast
from concurrent.futures import ThreadPoolExecutor
import io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import ssl
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request

from migration_run import BrokerClient


class BrokerClientTests(unittest.TestCase):
    def client(self):
        return BrokerClient('http://private.invalid:1234/', 'fixture-token')

    def test_claim_and_done_reuse_opener_but_not_requests_or_responses(self):
        responses = [io.BytesIO(b'{"ok":true,"task":null}'),
                     io.BytesIO(b'{"ok":true}')]
        opener = mock.Mock()
        opener.open.side_effect = responses
        payload = {'stage': 'p3-r3', 'id': 12, 'lease': 'same-lease',
                   'receipt': {'results': [{'rel': '中文', 'status': 'copied'}]}}
        with mock.patch.object(urllib.request, 'build_opener', return_value=opener) as build:
            client = self.client()
            self.assertEqual(client.request('GET', '/claim?owner=fixture-0')['task'], None)
            self.assertEqual(client.request('POST', '/done', payload), {'ok': True})
        build.assert_called_once()
        self.assertEqual(build.call_args.args[0].proxies, {})
        claim, done = [call.args[0] for call in opener.open.call_args_list]
        self.assertIsNot(claim, done)
        self.assertEqual(claim.full_url, 'http://private.invalid:1234/claim?owner=fixture-0')
        self.assertIsNone(claim.data)
        self.assertEqual(done.method, 'POST')
        self.assertEqual(done.data, json.dumps(payload, ensure_ascii=True).encode('utf-8'))
        for request in (claim, done):
            self.assertEqual(request.get_header('Authorization'), 'Bearer fixture-token')
            self.assertEqual(request.get_header('Content-type'), 'application/json')
        self.assertTrue(all(call.kwargs == {'timeout': 30} for call in opener.open.call_args_list))
        self.assertTrue(all(response.closed for response in responses))

    def test_each_thread_has_its_own_opener_including_mutable_request_state(self):
        count = 8
        barrier = threading.Barrier(count)
        built = []
        lock = threading.Lock()

        class StatefulOpener:
            def __init__(self):
                self.owner = threading.get_ident()

            def open(self, request, timeout):
                self_identity = threading.get_ident()
                if self_identity != self.owner:
                    raise AssertionError('opener shared between worker threads')
                return io.BytesIO(b'{"ok":true,"task":null}')

        def build(handler):
            value = StatefulOpener()
            with lock:
                built.append(value)
            return value

        client = self.client()

        def run(index):
            barrier.wait(timeout=5)
            for _ in range(10):
                self.assertIsNone(client.request('GET', '/claim?owner=' + str(index))['task'])

        with mock.patch.object(urllib.request, 'build_opener', side_effect=build):
            with ThreadPoolExecutor(max_workers=count) as pool:
                list(pool.map(run, range(count)))
        self.assertEqual(len(built), count)
        self.assertEqual(len({item.owner for item in built}), count)

    def test_different_clients_do_not_share_opener_or_credentials(self):
        first, second = mock.Mock(), mock.Mock()
        first.open.return_value = io.BytesIO(b'{"ok":true}')
        second.open.return_value = io.BytesIO(b'{"ok":true}')
        with mock.patch.object(urllib.request, 'build_opener', side_effect=[first, second]) as build:
            self.client().request('GET', '/claim')
            BrokerClient('http://other.invalid', 'second-token').request('GET', '/claim')
        self.assertEqual(build.call_count, 2)
        self.assertEqual(first.open.call_args.args[0].get_header('Authorization'), 'Bearer fixture-token')
        self.assertEqual(second.open.call_args.args[0].get_header('Authorization'), 'Bearer second-token')

    def test_done_transport_error_propagates_without_hidden_retry_or_recopy(self):
        opener = mock.Mock()
        error = urllib.error.URLError('lost reply after possible commit')
        opener.open.side_effect = [error, io.BytesIO(b'{"ok":true}')]
        payload = {'id': 7, 'lease': 'exact-lease', 'receipt': {'results': []}}
        with mock.patch.object(urllib.request, 'build_opener', return_value=opener) as build:
            client = self.client()
            with self.assertRaises(urllib.error.URLError) as caught:
                client.request('POST', '/done', payload)
            self.assertIs(caught.exception, error)
            self.assertEqual(opener.open.call_count, 1)
            self.assertEqual(client.request('POST', '/done', payload), {'ok': True})
        build.assert_called_once()
        requests = [call.args[0] for call in opener.open.call_args_list]
        self.assertEqual(requests[0].data, requests[1].data)

    def test_invalid_replies_close_response_and_keep_original_validation(self):
        for body in (b'[]', b'null', b'false', b'{"ok":false}', b'{'):
            with self.subTest(body=body):
                response = io.BytesIO(body)
                opener = mock.Mock()
                opener.open.return_value = response
                with mock.patch.object(urllib.request, 'build_opener', return_value=opener):
                    with self.assertRaises(ValueError):
                        self.client().request('GET', '/claim')
                self.assertTrue(response.closed)

    def test_first_construction_failure_can_retry_without_a_poisoned_cache(self):
        opener = mock.Mock()
        opener.open.return_value = io.BytesIO(b'{"task":null}')
        with mock.patch.object(urllib.request, 'build_opener', side_effect=[OSError('temporary'), opener]) as build:
            client = self.client()
            with self.assertRaises(OSError):
                client.request('GET', '/claim')
            self.assertIsNone(client.request('GET', '/claim')['task'])
        self.assertEqual(build.call_count, 2)

    def test_real_urllib_keeps_verified_https_and_proxy_bypass_without_global_patch(self):
        client = self.client()
        # Construct real handlers but replace only network open for this test.
        global_opener = urllib.request._opener
        with mock.patch.object(urllib.request.OpenerDirector, 'open',
                               return_value=io.BytesIO(b'{"ok":true}')):
            client.request('GET', '/claim')
        opener = client._local.opener
        https = [handler for handler in opener.handlers if isinstance(handler, urllib.request.HTTPSHandler)]
        self.assertEqual(len(https), 1)
        # Python 3.10/3.11 initialize the default context on first HTTPS open;
        # Python 3.12 constructs it while building the handler.
        context = https[0]._context
        if context is not None:
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(context.check_hostname)
        self.assertIs(urllib.request._opener, global_opener)
        # An empty ProxyHandler is omitted from the handler chain entirely.
        self.assertFalse(any(isinstance(handler, urllib.request.ProxyHandler) and handler.proxies
                             for handler in opener.handlers))

    def test_real_http_claims_are_not_cached_and_new_task_is_immediately_visible(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append((self.path, self.headers.get('Authorization')))
                value = {'ok': True, 'task': None} if len(requests) == 1 else {
                    'ok': True, 'task': {'id': 9}, 'lease': 'private-lease'}
                body = json.dumps(value).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever,
                                      kwargs={'poll_interval': 0.01}, daemon=True)
            thread.start()
            try:
                client = BrokerClient('http://127.0.0.1:' + str(server.server_port), 'private-token')
                # Invalid environment proxies must never intercept the private broker.
                with mock.patch.dict('os.environ', {'http_proxy': 'http://127.0.0.1:1', 'no_proxy': ''}):
                    self.assertIsNone(client.request('GET', '/claim?owner=private-0')['task'])
                    self.assertEqual(client.request('GET', '/claim?owner=private-0')['task'], {'id': 9})
            finally:
                server.shutdown()
                thread.join(timeout=3)
        self.assertEqual(requests, [('/claim?owner=private-0', 'Bearer private-token')] * 2)

    def test_both_worker_lanes_use_client_without_changing_wait_or_receipt_retry(self):
        root = Path(__file__).resolve().parent
        for name in ('migration_run.py', 'migration_worker128_run.py'):
            with self.subTest(name=name):
                tree = ast.parse((root / name).read_text())
                worker = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'worker')
                code = ast.unparse(worker)
                self.assertIn("BrokerClient(cfg['broker_url'], cfg['broker_token']).request", code)
                self.assertNotIn('build_opener', code)
                self.assertIn('stop.wait(1.5)', code)
                self.assertIn("broker_request('POST', '/done', payload)", code)
                self.assertIn('delay = min(30, delay * 2)', code)


if __name__ == '__main__':
    unittest.main()
