"""Local recovery fixtures: never contact nodes or launch migration services."""
import concurrent.futures
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid

import deploy_crash_cluster as deploy
import migration_crash_resume as crash
import migration_progress as progress
import migration_recovery_role as roles
import migration_resume_index_parallel_memory as index
from migration_io import read_json, write_json


class CrashProducerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='crash-recovery-fixture-', dir='/tmp')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.old, self.control = root / 'old', root / 'new'
        self.old.mkdir(); self.control.mkdir()
        (self.old / 'STOP').touch()
        self.run_id = 'crash-fixture-' + uuid.uuid4().hex
        scope = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm'}
        write_json(self.old / 'config.json', dict(scope, run_id='old-fixture'))
        write_json(self.old / 'extension-plan.json', {'priority': 4, 'source': '/share/project',
            'destination': '/share-mixed/project', 'exclude': ['eai_pwm'], 'status': 'queued',
            'depends_on': {'control': str(self.old)}})
        write_json(self.control / 'config.json', dict(scope, run_id=self.run_id,
            resume_from=str(self.old), copy_hosts=['fixture-master'],
            predecessor_isolation={'confirmed': True, 'method': 'fixture', 'old_control': str(self.old)},
            broker_token='fixture-token', target_probe_endpoints=['http://127.0.0.1:40561']))
        self.warm = self.control / 'warm-tasks.jsonl'
        self.warm.write_text('')
        with mock.patch('migration_overlap_resume.validate_mounts'), \
                mock.patch.object(crash, 'ensure_producer_nofile', return_value=(65536, 65536)):
            self.producer = crash.CrashResume(self.control, self.warm)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        p = self.producer
        p._collect_publications(finish=True)
        p.db.close(); p._lock.close()
        p.hardlink_pool.shutdown(); p.directory_pool.shutdown()
        for name in ('index.sqlite', 'resume-candidate.sqlite'):
            for suffix in ('', '-wal', '-shm'):
                Path('/tmp/' + self.run_id + '-' + name + suffix).unlink(missing_ok=True)

    def fake_pool(self, transform=lambda e: e):
        pool = mock.Mock()
        def submit(entries):
            f = concurrent.futures.Future()
            f.set_result({'entries': [transform(e) for e in entries],
                          'probe_delta': {'counts': {}, 'endpoints': {}}})
            return f
        pool.submit.side_effect = submit
        self.producer.probe_pool = pool
        return pool

    def test_fresh_bootstrap_does_not_require_old_local_database_or_pickle(self):
        self.assertTrue(self.producer.live_path.is_file())
        self.assertEqual(self.producer.next_task_id, 0)
        self.assertEqual(read_json(self.control / 'state.json')['workers_expected'], 1)
        self.assertTrue(read_json(self.control / 'state.json')['recovery_warm'])

    def test_existing_target_is_not_published(self):
        self.fake_pool(lambda e: None)
        self.producer.stage = 'p3-r3'
        self.producer._offer([{'rel': 'datasets/existing', 'kind': 'file', 'size': 10}])
        self.producer._drain_probes()
        self.assertEqual(self.producer.next_task_id, 0)

    def test_duplicate_missing_path_is_reserved_once_and_counter_follows_publication(self):
        self.fake_pool()
        crash.queue_dirs(self.control, 'p3-r3')
        entry = {'rel': 'datasets/missing', 'kind': 'file', 'size': 10}
        self.producer._offer([entry, entry])
        self.producer._drain_probes()
        self.producer._collect_publications(finish=True)
        task = read_json(self.control / 'stages/p3-r3/pending/0/0.json')
        self.assertEqual(task['entries'], [entry])
        self.assertEqual(self.producer.enqueued_batches, 1)
        self.assertEqual(self.producer.discovered_files, 1)

    def test_warm_candidates_request_current_source_identity(self):
        pool = self.fake_pool(lambda e: None)
        self.warm.write_text(json.dumps({'task': {'entries': [{'rel': 'datasets/file', 'kind': 'file'}]}}) + '\n')
        self.producer.enqueue_warm()
        self.assertTrue(pool.submit.call_args.args[0][0]['_refresh_source'])

    def test_metadata_mismatched_path_fails_before_publication(self):
        self.fake_pool(lambda e: dict(e, rel='other/path'))
        with self.assertRaisesRegex(ValueError, 'mismatched'):
            self.producer._offer([{'rel': 'datasets/file', 'kind': 'file'}])
        self.assertEqual(self.producer.next_task_id, 0)

    def test_run_selects_fresh_overlap_not_live_handover(self):
        with mock.patch.object(crash, 'ProcessProbePool') as pool, \
                mock.patch.object(crash.OverlapResume, 'run', return_value='fixture') as run:
            self.assertEqual(self.producer.run(), 'fixture')
            run.assert_called_once_with(self.producer)
            pool.return_value.shutdown.assert_called_once()


class ImportAndGuardTests(unittest.TestCase):
    def test_worker9_is_not_in_any_target_or_internal_ssh_command(self):
        self.assertEqual(len(deploy.IPS), 11)
        self.assertNotIn('worker-9', deploy.IPS)
        with self.assertRaises(ValueError):
            deploy.internal_ssh('worker-9', 'hostname')

    def test_dependency_closure_contains_dynamic_worker_entries(self):
        names = deploy.runtime_sources(Path(deploy.__file__).parent)
        for name in ('migration_worker128_run.py', 'migration_run.py', 'migration_linux_open.py',
                     'migration_crash_resume.py', 'migration_resume_index_parallel_memory.py'):
            self.assertIn(name, names)

    def test_import_deduplicates_task_and_path_and_does_not_requeue_completed_receipt(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            old = Path(directory)
            stage = old / 'stages/p3-r3'
            for d in ('pending/0', 'running/0', 'done/0'):
                (stage / d).mkdir(parents=True)
            task = {'id': 64, 'phase': 3, 'round': 3, 'op': 'sync', 'entries': [
                {'rel': 'datasets/done', 'kind': 'file'}, {'rel': 'datasets/retry', 'kind': 'file', 'known': {}}]}
            write_json(stage / 'pending/0/64.json', task)
            write_json(stage / 'running/0/broker--old--64.json', task)
            write_json(stage / 'done/0/64.json', {'id': 64, 'results': [
                {'rel': 'datasets/done', 'status': 'copied'}, {'rel': 'datasets/retry', 'status': 'error'}]})
            rows, counts = deploy.unfinished_tasks(old)
            self.assertEqual(counts['unfinished_paths'], 1)
            self.assertEqual(rows[0]['task']['entries'], [{'rel': 'datasets/retry', 'kind': 'file'}])

    def test_new_role_mapping_retains_cache_age_and_uses_sharded_broker(self):
        cfg = {'crash_recovery_v1': True}
        primary = roles.role_argv('/fixture', 'primary', config=cfg)
        self.assertEqual(primary[-2:], ['--cache-max-age', '30'])
        self.assertTrue(roles.role_argv('/fixture', 'broker', config=cfg)[2].endswith('migration_broker_sharded.py'))
        self.assertEqual(roles.role_argv('/fixture', 'producer', config=cfg)[-2:],
                         ['--warm-tasks', '/fixture/warm-tasks.jsonl'])

    def test_strict_index_enumeration_never_means_permission_denied_is_empty(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory); (root / 'stages').mkdir()
            with mock.patch.object(index.os, 'scandir', side_effect=PermissionError('fixture denied')):
                with self.assertRaises(PermissionError):
                    index._receipt_paths(root, strict=True)

    def test_monitor_adds_recorded_baseline_without_changing_new_run_rate_counter(self):
        view = {'nodes': [{'counts': {'copied': 2}}], 'new_copied_bytes': 20,
                'recovery_baseline': {'work': {'copied_files': 100, 'copied_bytes': 1000, 'existing_checks': 3}},
                'distributed_target_probe': {'counts': {'remote_existing': 4}}}
        work = progress.copy_work_progress(view)
        self.assertEqual(work['copied_bytes'], 20)
        self.assertEqual(work['cumulative_copied_bytes'], 1020)
        self.assertEqual(work['cumulative_copied_files'], 102)
        self.assertEqual(work['existing_checks'], 7)


if __name__ == '__main__':
    unittest.main()
