import concurrent.futures
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_probe_processes as processes
from test_migration_target_probe import TargetProbeTests


class ProbeProcessTests(unittest.TestCase):
    def fixture(self):
        f=TargetProbeTests();f.setUp();self.addCleanup(f.doCleanups)
        server=f.server()
        cfg=dict(f.cfg, target_probe_endpoints=['http://127.0.0.1:'+str(server.server_port)],
                 source=str(f.source),destination=str(f.destination))
        return f,cfg

    def test_real_spawn_checks_paths_without_opening_coordinator_or_queue(self):
        f,cfg=self.fixture()
        (f.destination/'already').write_bytes(b'existing')
        entries=[{'rel':name,'kind':'file','size':1} for name in ['already','new1','new2']]
        pool=processes.ProcessProbePool(f.control,cfg,processes=2,threads=2)
        self.addCleanup(pool.shutdown)
        result=pool.submit(entries).result(timeout=20)
        self.assertIsNone(result['entries'][0])
        self.assertEqual([e['rel'] for e in result['entries'][1:]],['new1','new2'])
        self.assertEqual(result['probe_delta']['counts']['probes'],3)
        self.assertEqual(set(p.name for p in f.control.iterdir()),set())
        self.assertFalse((f.destination/'new1').exists())

    def test_spawn_honors_stop_before_metadata_submission(self):
        f,cfg=self.fixture();(f.control/'STOP').write_text('stop')
        pool=processes.ProcessProbePool(f.control,cfg,processes=1,threads=2)
        self.addCleanup(pool.shutdown)
        with self.assertRaisesRegex(RuntimeError,'STOP'):
            pool.submit([{'rel':'new','kind':'file'}]).result(timeout=20)

    def test_invalid_batch_reply_falls_back_to_individually_checked_results(self):
        f,cfg=self.fixture();client=processes._Client(f.control,cfg)
        class Reply:
            def __enter__(self):return self
            def __exit__(self,*args):pass
        entries=[{'rel':'a','kind':'file'},{'rel':'b','kind':'file'}]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as threads, \
                mock.patch.object(processes,'_THREADS',threads), \
                mock.patch.object(client._opener(),'open',return_value=Reply()), \
                mock.patch.object(client,'_response_payload',return_value={'ok':True,'results':[]}), \
                mock.patch.object(client,'_probe_missing',side_effect=lambda e:dict(e,checked=True)) as checked:
            result=client.probe_many(entries)
        self.assertEqual(checked.call_count,2)
        self.assertTrue(all(e['checked'] for e in result))


if __name__=='__main__':unittest.main()
