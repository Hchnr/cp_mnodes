"""Real copy/checksum + existing broker receipt protocol, private local fixtures."""
import hashlib
import json
from pathlib import Path
import unittest
from unittest import mock

import migration_broker_sharded as sharded
import migration_worker as copy_core
from migration_run import process_entry
from migration_scan import _stat_fields
from migration_io import read_json, write_json
from test_broker_parallel import Fixture, STAGE


class LargeWarmBatchTests(unittest.TestCase):
    def test_256_and_512_entry_tasks_copy_verify_and_commit_idempotent_receipts(self):
        for count in (256,512):
            with self.subTest(entries=count):
                fixture=Fixture(tasks=0)
                try:
                    broker=fixture.open(sharded.Broker);fixture.broker=broker
                    source,destination=fixture.control/'source',fixture.control/'destination'
                    source.mkdir();destination.mkdir();entries=[]
                    for i in range(count):
                        rel='data-'+str(i);p=source/rel;p.write_bytes(b'fixture-'+str(i).encode())
                        entries.append({'rel':rel,'kind':'file',**_stat_fields(p.stat())})
                    task={'id':0,'phase':3,'round':1,'op':'sync','entries':entries}
                    write_json(fixture.pending(0),task)
                    claim=broker.claim(STAGE,'fixture-host-0')
                    self.assertEqual(len(claim['task']['entries']),count)
                    cfg={'source':str(source),'destination':str(destination),'run_id':fixture.run_id}
                    with mock.patch.object(copy_core,'_cancel_event',None), \
                            mock.patch.object(copy_core,'_progress_callback',None), \
                            mock.patch.object(copy_core._io_limiter,'consume'):
                        results=[process_entry(cfg,claim['task'],e) for e in claim['task']['entries']]
                    self.assertTrue(all(r['status']=='copied' for r in results))
                    for entry,result in zip(entries,results):
                        raw=(source/entry['rel']).read_bytes()
                        self.assertEqual((destination/entry['rel']).read_bytes(),raw)
                        self.assertEqual(result['sha256'],hashlib.sha256(raw).hexdigest())
                    receipt={'id':0,'host':'fixture-host','thread':0,'results':results}
                    payload={'stage':STAGE,'id':0,'lease':claim['lease'],'receipt':receipt}
                    self.assertLess(len(json.dumps(payload).encode()),64*1024**2)
                    self.assertTrue(broker.done(payload)['ok'])
                    stored=read_json(fixture.done_path(0))
                    self.assertEqual(len(stored['results']),count)
                    self.assertTrue(broker.done(payload)['ok'])
                    self.assertEqual(read_json(fixture.done_path(0)),stored)
                    self.assertIsNone(broker.claim(STAGE,'fixture-host-0')['task'])
                finally:
                    fixture.close()


if __name__=='__main__':
    unittest.main()
