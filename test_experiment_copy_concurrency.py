"""Local-only hot target changes: no copy processes or remote services run."""
from contextlib import ExitStack, redirect_stdout
import copy
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid

import experiment_copy_concurrency as experiment
from migration_io import read_json, write_json


class ConcurrencyExperimentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='copy-concurrency-test-')
        self.addCleanup(self.tmp.cleanup)
        self.run_id = 'test-concurrency-' + uuid.uuid4().hex
        self.control = Path(self.tmp.name) / ('.' + self.run_id)
        self.control.mkdir()
        self.hosts = ['job-fixture-master-0'] + ['job-fixture-worker-' + str(i) for i in range(11)]
        self.cfg = {'control': str(self.control), 'run_id': self.run_id, 'job': 'fixture',
                    'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm',
                    'threads': 64, 'copy_threads_per_node': 128, 'supplement_threads': 64,
                    'io_bytes_per_second': 10_000_000_000,
                    'supplement_io_bytes_per_second': 10_000_000_000,
                    'master_host': self.hosts[0], 'copy_hosts': self.hosts}
        write_json(self.control / 'config.json', self.cfg)
        write_json(self.control / 'state.json', {'status': 'copying'})
        self.clock = 0
        lock = Path('/tmp') / (self.run_id + '-copy-concurrency.lock')
        self.addCleanup(lambda: lock.unlink(missing_ok=True))

    def snapshot(self, control):
        nodes = {}
        cfg = read_json(control / 'config.json')
        for folder in ('nodes', 'nodes-extra'):
            (control / folder).mkdir(exist_ok=True)
            for i, host in enumerate(self.hosts):
                key = folder + '/' + host + '.json'
                node = {'pid': i + (100 if folder == 'nodes' else 200), 'started': 'unchanged',
                        'run_started': 'unchanged', 'counts': {'copied': int(self.clock)},
                        'bytes': int(self.clock) * 1000, 'streamed_bytes': int(self.clock) * 1100,
                        'verified_files': int(self.clock), 'verified_bytes': int(self.clock) * 1000,
                        'completed_batches': 0, 'broker_retries': 0, 'stages': {},
                        'active': 64, 'awaiting': 0}
                nodes[key] = node
                write_json(control / key, {'threads': 64 if folder == 'nodes' else cfg['supplement_threads']})
        return {'nodes': nodes, 'monotonic': self.clock, 'epoch': self.clock,
                'stage': 'p3-r3', 'status': 'copying', 'gc_seconds': None}

    def sleep(self, seconds):
        self.clock += seconds

    def run_experiment(self, sleep=None):
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(experiment.socket, 'gethostname', return_value=self.hosts[0]))
            stack.enter_context(mock.patch('migration_run.validate_mounts'))
            stack.enter_context(mock.patch.object(experiment, 'snapshot', side_effect=self.snapshot))
            stack.enter_context(mock.patch.object(experiment.time, 'monotonic', side_effect=lambda: self.clock))
            stack.enter_context(mock.patch.object(experiment.time, 'sleep', side_effect=sleep or self.sleep))
            stack.enter_context(mock.patch('os.kill', side_effect=AssertionError('must never signal a copy')))
            stack.enter_context(mock.patch('subprocess.Popen', side_effect=AssertionError('must never launch a copy')))
            stack.enter_context(redirect_stdout(io.StringIO()))
            return experiment.run(self.control, 96, 30, interval=10)

    def test_success_restores_all_original_fields_and_keeps_processes(self):
        output = self.run_experiment()
        self.assertEqual(read_json(self.control / 'config.json'), self.cfg)
        record = read_json(output)
        self.assertEqual(record['status'], 'restored')
        self.assertTrue(record['experiment_completed'])
        self.assertEqual(record['copy_processes_signalled'], 0)
        self.assertEqual(len(record['samples']), 3)
        self.assertTrue(all(row['desired_threads'] == 12 * 96 for row in record['samples']))

    def test_keyboard_interrupt_restores_and_records_incomplete_experiment(self):
        def interrupted(seconds):
            raise KeyboardInterrupt('fixture')
        with self.assertRaises(KeyboardInterrupt):
            self.run_experiment(interrupted)
        self.assertEqual(read_json(self.control / 'config.json'), self.cfg)
        record = read_json(next(self.control.glob('copy-concurrency-*/experiment.json')))
        self.assertEqual(record['status'], 'restored')
        self.assertFalse(record['experiment_completed'])

    def test_unrelated_config_edit_survives_restoration(self):
        def edit(seconds):
            cfg = read_json(self.control / 'config.json')
            cfg['ssh_owner'] = 'new-owner'
            write_json(self.control / 'config.json', cfg)
            self.sleep(seconds)
        self.run_experiment(edit)
        self.assertEqual(read_json(self.control / 'config.json'), dict(self.cfg, ssh_owner='new-owner'))

    def test_foreign_marker_never_overwritten_by_restoration(self):
        def edit(seconds):
            cfg = read_json(self.control / 'config.json')
            cfg[experiment.MARKER] = {'operation': 'foreign'}
            write_json(self.control / 'config.json', cfg)
            raise KeyboardInterrupt('fixture')
        with self.assertRaisesRegex(RuntimeError, 'ownership differs'):
            self.run_experiment(edit)
        cfg = read_json(self.control / 'config.json')
        self.assertEqual(cfg[experiment.MARKER], {'operation': 'foreign'})
        self.assertEqual(cfg['copy_threads_per_node'], 96)
        record = read_json(next(self.control.glob('copy-concurrency-*/experiment.json')))
        self.assertEqual(record['status'], 'restore_needs_inspection')

    def test_validation_rejects_scope_partition_identity_and_budget_drift(self):
        invalid = ({'destination': '/different'}, {'control': '/different'},
                   {'threads': True}, {'copy_threads_per_node': 64},
                   {'supplement_threads': 32}, {'run_id': 'different'},
                   {'copy_hosts': self.hosts[:11]}, {'io_bytes_per_second': 0})
        for change in invalid:
            with self.subTest(change=change), self.assertRaises(ValueError):
                experiment.validate_config(self.control, dict(self.cfg, **change))

    def test_stop_and_foreign_host_prevent_any_target_write(self):
        (self.control / 'STOP').touch()
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            self.run_experiment()
        self.assertEqual(read_json(self.control / 'config.json'), self.cfg)
        with mock.patch.object(experiment.socket, 'gethostname', return_value='another-host'):
            with self.assertRaisesRegex(RuntimeError, 'real recovery master'):
                experiment.run(self.control, 96, 30)

    def test_restore_rejects_extra_or_invalid_thread_fields(self):
        record = {'control': str(self.control), 'run_id': self.run_id,
                  'marker': {'operation': 'ours'}, 'before': {'copy_threads_per_node': 128,
                  'supplement_threads': 64}, 'target': {'copy_threads_per_node': 96, 'supplement_threads': 32}}
        for name, value in (('before', dict(record['before'], broker_token='not-allowed')),
                            ('before', {'copy_threads_per_node': 96, 'supplement_threads': 64}),
                            ('target', {'copy_threads_per_node': True, 'supplement_threads': 32})):
            patch = copy.deepcopy(record);patch[name] = value
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                experiment.restore(self.control, patch)
        self.assertEqual(read_json(self.control / 'config.json'), self.cfg)

    def test_restore_is_noop_if_config_already_original_without_marker(self):
        record = {'control': str(self.control), 'run_id': self.run_id,
                  'marker': {'operation': 'ours'}, 'before': {'copy_threads_per_node': 128,
                  'supplement_threads': 64}, 'target': {'copy_threads_per_node': 96, 'supplement_threads': 32}}
        with mock.patch.object(experiment, 'write_json') as writer:
            self.assertFalse(experiment.restore(self.control, record))
        writer.assert_not_called()


if __name__ == '__main__':
    unittest.main()
