"""Private fixtures only: cold bootstrap, bounded hints and full-scan barriers."""
import concurrent.futures
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid

import migration_cold_frontier as cold
import migration_hint_import as importer
import deploy_recovery_2dad as deployment
from migration_io import read_json,write_json
import test_crash_recovery as fixtures


class ColdTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.CrashProducerTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        cfg=dict(self.fixture.producer.cfg)
        self.fixture.cleanup()
        control=self.fixture.control.parent/'cold';control.mkdir()
        cfg.update(run_id='cold-fixture-'+uuid.uuid4().hex,cold_frontier_v1=True)
        write_json(control/'config.json',cfg)
        warm=control/'warm.jsonl';warm.write_text('')
        with mock.patch('migration_overlap_resume.validate_mounts'):
            p=cold.ColdFrontierResume(control,warm)
        self.p=p;self.fixture.producer=p;self.fixture.run_id=cfg['run_id']
        self.addCleanup(p.feed_reader.shutdown)
        self.addCleanup(p._inputs.close)

    def test_fresh_namespace_has_empty_assignment_basis_and_checked_warm_limits(self):
        self.assertEqual(self.p.next_task_id,0)
        self.assertEqual(self.p.adopted_task_ids,set())
        self.assertEqual(self.p.scan_batch_files,256)
        self.assertEqual(self.p._formal_scan_batch_files,64)
        self.assertFalse(self.p._task_publisher.check_on_submit)
        self.assertEqual(self.p._publication_workers,16)
        self.assertTrue(self.p.combined_manifest.is_relative_to(self.p.control))
        with mock.patch.object(cold.OverlapResume,'start_seed') as seed:
            self.p.start_seed();seed.assert_called_once_with(self.p)

    def test_preserved_queue_is_checked_and_issued_only_once(self):
        entry={'rel':'datasets/cold','kind':'file','size':10}
        self.p.warm_tasks.write_text(json.dumps({'task':{'entries':[entry,entry]}})+'\n')
        self.fixture.fake_pool()
        self.p.enqueue_warm()
        tasks=[read_json(p) for p in self.p.control.glob('stages/p3-r3/pending/*/*.json')]
        self.assertEqual([e['rel'] for t in tasks for e in t['entries']],['datasets/cold'])
        self.assertEqual(self.p.next_task_id,1)

    def test_feed_readers_refuse_wrong_scope_and_never_equate_missing_to_complete(self):
        self.assertTrue(all(v['status']=='waiting' for v in self.p._read_feeds()))
        host=self.p.cfg['copy_hosts'][0]
        path=self.p.control/'frontier-input'/host/'progress.json'
        write_json(path,dict(host=host,run_id='wrong',status='complete'))
        with self.assertRaisesRegex(ValueError,'identity'):
            self.p._read_feeds()

    def test_following_full_scan_and_original_formal_batch_configuration_remain_required(self):
        self.fixture.fake_pool()
        with mock.patch.object(self.p,'feed_frontier') as feed, \
             mock.patch('migration_frontier_resume.scan_phase',return_value={}) as scan:
            self.p.supplement_missing()
        feed.assert_called_once();scan.assert_called_once()
        self.assertEqual(scan.call_args.kwargs['batch_bytes'],1024**3)
        self.assertEqual(self.p.scan_batch_files,64)
        self.assertEqual(self.p.cfg.get('scan_batch_files',64),64)


class HintTests(unittest.TestCase):
    def test_import_is_read_only_on_old_control_and_deprioritization_is_not_copy_proof(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as d:
            root=Path(d);old=root/'old';control=root/'new';old.mkdir();control.mkdir()
            (old/'STOP').touch();oldhost='old-host';host='new-host'
            run_id='import-fixture-'+uuid.uuid4().hex
            scope=dict(source='/share/project/eai_pwm',destination='/share-mixed/project/eai_pwm')
            write_json(old/'config.json',dict(scope,run_id='old',copy_hosts=[oldhost]))
            write_json(control/'config.json',dict(scope,run_id=run_id,copy_hosts=[host],resume_from=str(old)))
            ready=old/'frontier-input'/oldhost/'ready';ready.mkdir(parents=True)
            target=control/'frontier-input'/host/'ready';target.mkdir(parents=True)
            for name in ('new.json','scheduled.json'):
                write_json(ready/name,dict(control=str(old),run_id='old',host=oldhost,
                    entries=[{'rel':'datasets/'+name,'kind':'file','known':{'owned':True}}]))
            before={p.name:p.read_bytes() for p in ready.iterdir()}
            write_json(control/'hint-import-plan.json',dict(control=str(control),run_id=run_id,
                hosts={host:[oldhost]},deprioritized={oldhost:['scheduled.json']}))
            with mock.patch.object(importer.socket,'gethostname',return_value=host), \
                 mock.patch.object(importer,'validate_mounts'):
                importer.run(control)
            values=[read_json(p) for p in target.glob('*.json')]
            self.assertEqual(len(values),1)
            self.assertEqual(values[0]['entries'],[{'rel':'datasets/new.json','kind':'file','_refresh_source':True}])
            self.assertEqual({p.name:p.read_bytes() for p in ready.iterdir()},before)
            status=read_json(target.parent/'import-progress.json')
            self.assertEqual(status['status'],'complete')
            self.assertEqual(status['files_deprioritized'],1)
            self.assertNotIn('copied',status)
            Path('/tmp/'+run_id+'-hint-import.lock').unlink()


class PackagingTests(unittest.TestCase):
    def test_multi_generation_baseline_keeps_ancestors_without_changing_live_counters(self):
        original={'copied_files':20,'copied_bytes':200,'cumulative_copied_files':120,
                  'cumulative_copied_bytes':1200,'existing_checks':30}
        value=deployment.inherited_baseline(original)
        self.assertEqual(value['copied_files'],120)
        self.assertEqual(value['copied_bytes'],1200)
        self.assertEqual(value['last_run_copied_files'],20)
        self.assertEqual(original['copied_files'],20)
        from migration_progress import copy_work_progress
        result=copy_work_progress(dict(nodes=[{'counts':{'copied':3}}],new_copied_bytes=40,
            recovery_baseline={'work':value}))
        self.assertEqual(result['cumulative_copied_files'],123)
        self.assertEqual(result['cumulative_copied_bytes'],1240)
        self.assertEqual(result['copied_files'],3)
        self.assertEqual(result['copied_bytes'],40)
        unknown=deployment.inherited_baseline(dict(original,cumulative_copied_bytes=None))
        self.assertIsNone(unknown['copied_bytes'])

    def test_dynamic_entrypoints_are_explicit_bundle_roots(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as d, \
                mock.patch.object(deployment,'OLD',Path(d)/'no-old-bundle'):
            sources=deployment.source_files(Path(deployment.__file__).parent)
        self.assertTrue({'migration_missing_discovery.py','migration_hint_import.py',
            'migration_cold_frontier.py','migration_broker_sharded.py',
            'migration_cached_worker.py','migration_probe_fast_open.py','migration_metrics.py'}<=set(sources))


if __name__=='__main__':unittest.main()
