import json
from pathlib import Path
import tempfile
import stat
import time
import types
import unittest
from unittest import mock

import deploy_fast_priority4_source as deploy
from migration_io import write_json


class ProducerHandoverGuards(unittest.TestCase):
    def test_preload_gate_uses_current_inode_length_not_the_old_observed_eof(self):
        meta=dict(manifest='/fixture',manifest_dev=1,manifest_ino=2,offset=100)
        info=types.SimpleNamespace(st_mode=stat.S_IFREG,st_dev=1,st_ino=2,st_size=200)
        self.assertEqual(deploy.source_preload_lag(meta,dict(active_manifest='/fixture'),
            dict(manifest_offset=150,manifest_lag=0),info),50)
        for change in (dict(st_size=17*1024**2),dict(st_ino=3),dict(st_mode=stat.S_IFLNK)):
            changed=types.SimpleNamespace(**dict(vars(info),**change))
            with self.assertRaises(ValueError):
                deploy.source_preload_lag(meta,dict(active_manifest='/fixture'),
                                         dict(manifest_offset=150,manifest_lag=0),changed)
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='p4-source-handover-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.control = Path(self.tmp.name)
        self.bundle = self.control / 'fast-p4-source-fixture'
        self.bundle.mkdir()
        self.audit = self.bundle / 'audit.json'
        self.intent = dict(producer=deploy.PRODUCER, control=str(self.control),
                           runtime_manifest_sha256='fixture', operation='fixture')
        write_json(self.bundle / 'intent.json', self.intent)
        self.native = dict(verified_at=time.time(), signals_sent=0, all_unchanged=True,
            copy_process_count=48, nodes=[dict(node=node, verified_at=time.time())
                for node in ['master-0'] + ['worker-' + str(i) for i in range(11)]])
        write_json(self.audit, self.native)
        self.standby = dict(status='standby_readonly_caught_up', manifest_lag=0,
                            updated=time.time(), tasks=100)
        write_json(self.bundle / 'standby.json', self.standby)
        write_json(self.bundle / 'producer-role.json', dict(pid=321, start_ticks='123', argv=['fixture']))
        patches = [mock.patch.object(deploy, 'CONTROL', self.control),
                   mock.patch.object(deploy, 'validate_original', return_value=({}, dict(next_task_id=100))),
                   mock.patch.object(deploy, 'frozen'), mock.patch.object(deploy, 'require_identity'),
                   mock.patch.object(deploy, 'identity', side_effect=AssertionError('no native actor reads in fixtures'))]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def test_stale_missing_or_partial_native_proof_never_signals(self):
        for change in (dict(verified_at=0), dict(all_unchanged=False), dict(copy_process_count=47),
                       dict(nodes=self.native['nodes'][:-1]), dict(signals_sent=1)):
            write_json(self.audit, dict(self.native, **change))
            with mock.patch.object(deploy.os, 'kill') as kill:
                with self.assertRaises(ValueError):
                    deploy.handover(self.bundle, self.audit)
                kill.assert_not_called()

    def test_uncaught_up_or_stale_standby_never_signals(self):
        for change in (dict(manifest_lag=1), dict(updated=0), dict(tasks=0), dict(tasks=613),
                       dict(status='standby_cache_loaded')):
            write_json(self.bundle / 'standby.json', dict(self.standby, **change))
            with mock.patch.object(deploy.os, 'kill') as kill:
                with self.assertRaises(ValueError):
                    deploy.handover(self.bundle, self.audit)
                kill.assert_not_called()

    def test_durable_claim_prefix_may_lead_the_ten_second_saved_state(self):
        write_json(self.bundle / 'standby.json', dict(self.standby, tasks=101))
        intent = dict(self.intent, preserved_actors=[dict(pid=p, argv=[]) for p in (5834,1856078,281673,5837,3744900)])
        write_json(self.bundle / 'intent.json', intent)
        # Pass the caught-up gate, then fail the independent actor binding.
        with mock.patch.object(deploy.os, 'kill') as kill:
            with self.assertRaisesRegex(ValueError, 'binding differs'):
                deploy.handover(self.bundle, self.audit)
            kill.assert_not_called()

    def test_wrong_bundle_or_audit_location_never_signals(self):
        with mock.patch.object(deploy.os, 'kill') as kill:
            for bundle, audit in ((self.control, self.audit), (self.bundle, self.control / 'audit.json')):
                with self.assertRaises(ValueError):
                    deploy.handover(bundle, audit)
            kill.assert_not_called()


if __name__ == '__main__':
    unittest.main()
