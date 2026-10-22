"""Local HTTP/protocol/concurrency fixtures; no SSH or running migration edits."""
import concurrent.futures
import copy
import http.server
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

import migration_distributed_target_filter as distributed
import migration_target_probe as daemon
from migration_overlap_parallel_filter import ParallelFilterResume


class DistributedTargetFilterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='distributed-target-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.control = Path(self.temp.name).resolve()
        self.cfg = {'broker_token': 'fixture-private-token',
                    'target_probe_endpoints': ['http://127.0.0.1:' + str(19000+i) for i in range(12)]}
        self.entry = {'rel': 'other/file', 'kind': 'file', 'size': 10,
                      'mode': 0o100600, 'ino': 9, 'dev': 3, 'mtime_ns': 50,
                      'ctime_ns': 60, 'uid': 0, 'gid': 0}

    def make(self, **config):
        obj = object.__new__(distributed.DistributedTargetFilterResume)
        obj.control = self.control
        obj.cfg = dict(self.cfg, **config)
        obj._init_probe_client(obj.cfg)
        return obj

    def missing(self, entry=None):
        return {'ok': True, 'missing': True, 'reason': 'missing',
                'entry': {k: v for k, v in (entry or self.entry).items() if k != '_refresh_source'}}

    def test_missing_existing_and_refreshed_wire_preserve_known_proof(self):
        obj = self.make()
        with mock.patch.object(obj, '_http_probe', return_value=self.missing()):
            self.assertEqual(obj._probe_missing(self.entry), self.entry)
        with mock.patch.object(obj, '_http_probe', return_value={
                'ok': True, 'missing': False, 'entry': None, 'reason': 'existing'}):
            self.assertIsNone(obj._probe_missing(self.entry))
        original = dict(self.entry, _refresh_source=True, known={'sha256': 'fixture-proof'})
        fresh = dict(original, size=25, ino=19, mtime_ns=100)
        with mock.patch.object(obj, '_http_probe', return_value=self.missing(fresh)):
            self.assertEqual(obj._probe_missing(original), {k:v for k,v in fresh.items() if k!='_refresh_source'})
        self.assertEqual(obj._probe_snapshot()['counts']['remote_success'], 3)
        self.assertNotIn('fixture-private-token', json.dumps(obj._probe_snapshot()))

    def test_network_failures_retry_distinct_nodes_then_original_local_probe(self):
        obj = self.make()
        attempted = []
        def failed(endpoint, entry, deadline):
            attempted.append(endpoint)
            raise TimeoutError('fixture transport timeout')
        with mock.patch.object(obj, '_http_probe', side_effect=failed), \
                mock.patch.object(obj, '_local_probe_missing', return_value=self.entry) as fallback:
            self.assertEqual(obj._probe_missing(self.entry), self.entry)
        self.assertEqual(len(attempted), 3)
        self.assertEqual(len(set(attempted)), 3)
        fallback.assert_called_once_with(self.entry)
        self.assertEqual(obj._probe_snapshot()['counts']['remote_errors'], 3)
        self.assertEqual(obj._probe_snapshot()['counts']['local_fallbacks'], 1)

    def test_second_endpoint_success_avoids_local_fallback(self):
        obj = self.make()
        with mock.patch.object(obj, '_http_probe', side_effect=[OSError('first node unavailable'), self.missing()]) as rpc, \
                mock.patch.object(obj, '_local_probe_missing') as fallback:
            self.assertEqual(obj._probe_missing(self.entry), self.entry)
        self.assertEqual(rpc.call_count, 2)
        fallback.assert_not_called()
        self.assertNotEqual(rpc.call_args_list[0].args[0], rpc.call_args_list[1].args[0])

    def test_protocol_faults_never_become_existing_or_missing_results(self):
        cases = [None, {}, {'ok': True, 'missing': 1},
                 {'ok': True, 'missing': False, 'entry': None, 'reason': 'unsafe'},
                 self.missing(dict(self.entry, rel='other/wrong-path')),
                 self.missing(dict(self.entry, known={'injected': True})),
                 self.missing(dict(self.entry, size=99))]
        for payload in cases:
            with self.subTest(payload=payload):
                obj = self.make()
                with mock.patch.object(obj, '_http_probe', return_value=payload), \
                        mock.patch.object(obj, '_local_probe_missing', return_value='local-result') as fallback:
                    self.assertEqual(obj._probe_missing(self.entry), 'local-result')
                fallback.assert_called_once()
                self.assertEqual(obj._probe_snapshot()['counts']['remote_errors'], 3)

    def test_stop_before_or_during_remote_request_is_never_bypassed(self):
        obj = self.make()
        (self.control/'STOP').write_text('fixture stop')
        with mock.patch.object(obj, '_http_probe') as rpc, mock.patch.object(obj, '_local_probe_missing') as fallback:
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                obj._probe_missing(self.entry)
        rpc.assert_not_called();fallback.assert_not_called()
        (self.control/'STOP').unlink()
        def stopping(endpoint, entry, deadline):
            (self.control/'STOP').write_text('fixture stop during response')
            return self.missing()
        with mock.patch.object(obj, '_http_probe', side_effect=stopping), mock.patch.object(obj, '_local_probe_missing') as fallback:
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                obj._probe_missing(self.entry)
        fallback.assert_not_called()

    def test_global_deadline_is_not_renewed_for_each_retry(self):
        obj = self.make()
        clock = [10.0]
        def slow(endpoint, entry, deadline):
            self.assertEqual(deadline, 20)
            clock[0] = 20.1
            raise TimeoutError('budget consumed')
        with mock.patch.object(distributed.time, 'monotonic', side_effect=lambda: clock[0]), \
                mock.patch.object(obj, '_http_probe', side_effect=slow) as rpc, \
                mock.patch.object(obj, '_local_probe_missing', return_value=self.entry):
            self.assertEqual(obj._probe_missing(self.entry), self.entry)
        self.assertEqual(rpc.call_count, 1)
        self.assertEqual(obj._probe_snapshot()['counts']['deadline_exhausted'], 1)

    def test_parallel_round_robin_counters_and_threadlocal_opener(self):
        obj = self.make()
        lock = threading.Lock();seen=[];openers={}
        def remote(endpoint, entry, deadline):
            opener = obj._opener()
            with lock:
                seen.append(endpoint)
                previous = openers.setdefault(threading.get_ident(), opener)
                self.assertIs(previous, opener)
            time.sleep(.001)
            return self.missing(entry)
        entries = [dict(self.entry, rel='other/file-'+str(i)) for i in range(384)]
        with mock.patch.object(obj, '_http_probe', side_effect=remote):
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
                results = list(pool.map(obj._probe_missing, entries))
        self.assertEqual(results, entries)
        counts=obj._probe_snapshot()
        self.assertEqual(counts['counts']['probes'], 384)
        self.assertEqual(counts['counts']['remote_success'], 384)
        self.assertEqual({value['requests'] for value in counts['endpoints'].values()}, {32})
        self.assertGreater(len(openers), 1)
        self.assertEqual(len({id(value) for value in openers.values()}), len(openers))

    def test_inherited_handover_assignment_seed_and_run_are_unchanged(self):
        cls=distributed.DistributedTargetFilterResume
        self.assertIs(cls.run, ParallelFilterResume.run)
        self.assertIs(cls.supplement_missing, ParallelFilterResume.supplement_missing)
        self.assertIs(cls.fresh_entry, ParallelFilterResume.fresh_entry)
        seen=[]
        def initialize(obj, control, handover):
            seen.append((control,handover));obj.cfg=self.cfg;obj.state={}
            obj.filter_workers=128;obj.filter_limit=4096
        with mock.patch.object(distributed, 'read_json', return_value=self.cfg), \
                mock.patch.object(ParallelFilterResume, '__init__', initialize), \
                mock.patch.object(ParallelFilterResume, 'save') as save:
            obj=cls(self.control, 'existing-handover.json')
        self.assertEqual(seen,[(self.control,'existing-handover.json')])
        self.assertEqual(obj.filter_workers,256)
        self.assertEqual(obj.filter_limit,4096)
        self.assertEqual(save.call_args.kwargs['distributed_target_nodes'],12)
        self.assertEqual(save.call_args.kwargs['parallel_target_threads'],256)
        self.assertEqual(save.call_args.kwargs['distributed_target_threads'],256)

    def test_client_configuration_validated_before_consuming_handover(self):
        for endpoints in ([], ['http://u:secret@host:123'], ['http://host:123/probe'],
                          ['http://host:123','http://host:123']):
            with mock.patch.object(distributed,'read_json',return_value=dict(self.cfg,target_probe_endpoints=endpoints)), \
                    mock.patch.object(ParallelFilterResume,'__init__') as inherited:
                with self.assertRaises(ValueError):
                    distributed.DistributedTargetFilterResume(self.control,'never-consume')
                inherited.assert_not_called()
        self.assertEqual(distributed.normalize_endpoints({'b':'http://b:1/','a':{'url':'http://a:1'}}),
                         ('http://a:1','http://b:1'))

    def test_real_local_http_wire_auth_payload_and_non200_fallback(self):
        test=self;received=[]
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(inner):
                received.append({'path':inner.path,'authorization':inner.headers.get('Authorization'),
                                 'body':json.loads(inner.rfile.read(int(inner.headers['Content-Length'])))})
                body=json.dumps(test.missing(received[-1]['body']['entry'])).encode()
                status=200 if len(received)==1 else 503
                inner.send_response(status);inner.send_header('Content-Length',str(len(body)));inner.end_headers();inner.wfile.write(body)
            def log_message(inner,*args):pass
        server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            obj=self.make(target_probe_endpoints=['http://127.0.0.1:'+str(server.server_port)])
            self.assertEqual(obj._probe_missing(self.entry),self.entry)
            with mock.patch.object(obj,'_local_probe_missing',return_value='local-fallback'):
                self.assertEqual(obj._probe_missing(self.entry),'local-fallback')
            self.assertEqual(received[0]['path'],'/probe')
            self.assertEqual(received[0]['authorization'],'Bearer fixture-private-token')
            self.assertEqual(received[0]['body'],{'entry':self.entry})
            self.assertEqual(obj._probe_snapshot()['counts']['local_fallbacks'],1)
        finally:
            server.shutdown();server.server_close();thread.join(timeout=2)

    def test_actual_daemon_client_missing_existing_refresh_and_http_error_fallback(self):
        source=self.control/'source';target=self.control/'destination'
        source.mkdir();target.mkdir();(source/'other').mkdir()
        original=source/'other/file';original.write_bytes(b'actual fixture source')
        service=daemon.ProbeService(self.control,dict(self.cfg,source=daemon.SOURCE,destination=daemon.DESTINATION),slots=32)
        service.source_root=str(source);service.destination_root=str(target)
        server=daemon.BoundedProbeServer(('127.0.0.1',0),service)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01},daemon=True);thread.start()
        try:
            obj=self.make(target_probe_endpoints=['http://127.0.0.1:'+str(server.server_port)],
                          source=str(source),destination=str(target))
            entry=dict(self.entry,_refresh_source=True,known={'owned':True,'sha256':'a'*64})
            refreshed=obj._probe_missing(entry)
            self.assertEqual(refreshed['size'],original.stat().st_size)
            self.assertEqual(refreshed['ino'],original.stat().st_ino)
            self.assertEqual(refreshed['known'],entry['known'])
            self.assertFalse((target/'other').exists(),'probe cannot create destination directories')
            (target/'other').mkdir();existing=target/'other/file';existing.write_bytes(b'existing target stays intact')
            self.assertIsNone(obj._probe_missing(entry))
            self.assertEqual(existing.read_bytes(),b'existing target stays intact')
            obj._probe_token='wrong-token-for-http-401'
            missing=dict(self.entry,rel='other/another')
            with mock.patch.object(obj,'_local_probe_missing',wraps=obj._local_probe_missing) as fallback:
                self.assertEqual(obj._probe_missing(missing),missing)
            fallback.assert_called_once_with(missing)
            self.assertEqual(obj._probe_snapshot()['counts']['remote_errors'],1)
            self.assertFalse((target/'other/another').exists())
            self.assertEqual(original.read_bytes(),b'actual fixture source')
        finally:
            server.shutdown();server.server_close();thread.join(timeout=2)


if __name__=='__main__':unittest.main()
