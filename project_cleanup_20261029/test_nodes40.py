"""Forty-node registry checks; private fixtures/mocked transports only."""
import contextlib
import concurrent.futures
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import queue
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('nodes40_fixture_rm24', Path(__file__).with_name('rm24.py'))
rm24 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rm24)


class StopBeforeRoot(Exception):
    pass


class RecordingExecutor:
    capacities = []
    submitted = []

    def __init__(self, max_workers):
        self.capacities.append(max_workers)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def map(self, function, values):
        return [function(value) for value in values]

    def submit(self, function, *args):
        # Never call a real dispatch, SSH, worker, or rm in this test.
        self.submitted.append(args[0])
        result = concurrent.futures.Future()
        result.set_result(None)
        return result


class Nodes40Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='nodes40-private-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        RecordingExecutor.capacities = []
        RecordingExecutor.submitted = []
        for name in ('Popen', 'run'):
            patcher = mock.patch.object(rm24.subprocess, name,
                side_effect=AssertionError('private registry tests must not invoke real processes'))
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_registry_has_24_unchanged_old_aliases_and_16_explicit_extra_aliases(self):
        old = ['master-0']+['worker-'+str(i) for i in range(23)]
        extra = ['extra-master-0']+['extra-worker-'+str(i) for i in range(15)]
        self.assertEqual(rm24.NODES, old+extra)
        self.assertEqual(len(rm24.NODES), 40)
        self.assertEqual(len(set(rm24.NODE_HOSTS.values())), 40)
        for node in old:
            self.assertEqual(rm24.NODE_HOSTS[node], 'job-'+rm24.CLUSTER+'-'+node)
        self.assertEqual(rm24.NODE_HOSTS['extra-master-0'], 'job-'+rm24.EXTRA_CLUSTER+'-master-0')
        self.assertEqual(rm24.NODE_HOSTS['extra-worker-14'], 'job-'+rm24.EXTRA_CLUSTER+'-worker-14')

    def check_host(self, node, hostname, reject_running_rm=False):
        with mock.patch.object(rm24.socket, 'gethostname', return_value=hostname), \
                mock.patch.object(rm24.os.path, 'ismount', return_value=True), \
                mock.patch.object(rm24.os, 'stat', return_value=types.SimpleNamespace(st_ino=rm24.ROOT_INODE)), \
                mock.patch.object(rm24.os, 'statvfs', return_value=types.SimpleNamespace(f_flag=0)), \
                mock.patch.object(Path, 'read_text', return_value=''), \
                mock.patch.object(rm24.subprocess, 'check_output', return_value='rm (GNU coreutils) 9.5\n'):
            return rm24.node_check(node, reject_running_rm=reject_running_rm)

    def test_correct_real_hostnames_pass_for_both_clusters(self):
        for node, host in rm24.NODE_HOSTS.items():
            with self.subTest(node=node):
                result = self.check_host(node, host)
                self.assertEqual((result['node'], result['host']), (node, host))

    def test_wrong_cluster_or_alias_disguise_is_rejected(self):
        cases = [('master-0', rm24.NODE_HOSTS['extra-master-0']),
                 ('extra-master-0', rm24.NODE_HOSTS['master-0']),
                 ('extra-worker-0', rm24.NODE_HOSTS['extra-worker-1']),
                 ('extra-worker-0', 'job-'+rm24.EXTRA_CLUSTER+'-extra-worker-0'),
                 ('worker-0', rm24.NODE_HOSTS['worker-1'])]
        for node, host in cases:
            with self.subTest(node=node, host=host), self.assertRaisesRegex(ValueError, 'cluster/node differs'):
                self.check_host(node, host)

    def test_unknown_aliases_are_rejected_before_source_checks(self):
        with mock.patch.object(rm24.os.path, 'ismount') as mount:
            for node in ('worker-23', 'extra-worker-15', 'extra-master0', '', None, []):
                with self.subTest(node=node), self.assertRaisesRegex(ValueError, 'unknown node'):
                    rm24.node_check(node)
            mount.assert_not_called()

    def test_rm_scope_detects_absolute_source_from_external_cwd(self):
        for operand in ('/share/project', '/share/project/tree', '/share/project/../project/tree'):
            with self.subTest(operand=operand):
                self.assertTrue(rm24.rm_touches_target(['/usr/bin/rm', '-rf', '--', operand], '/root'))
        self.assertTrue(rm24.rm_touches_target([b'/bin/rm', b'-rf', b'/share/project/x', b''], '/root'))

    def test_rm_scope_detects_ancestor_operands_using_strings_only(self):
        # These are inert strings passed to a pure classifier, never commands.
        with mock.patch.object(Path, 'resolve', side_effect=AssertionError('must not resolve operands')), \
                mock.patch.object(rm24.os.path, 'realpath', side_effect=AssertionError('must not follow links')), \
                mock.patch.object(rm24.os, 'stat', side_effect=AssertionError('must not stat operands')):
            for operand in ('/share', '/', '/share/..'):
                with self.subTest(operand=operand):
                    self.assertTrue(rm24.rm_touches_target(['rm', '-rf', '--', operand], '/root'))

    def test_rm_scope_detects_relative_source_and_cwd_inside_source(self):
        self.assertTrue(rm24.rm_touches_target(['rm', '-rf', 'project/tree'], '/share'))
        self.assertTrue(rm24.rm_touches_target(['rm', '../share/project/tree'], '/root'))
        self.assertTrue(rm24.rm_touches_target(['rm', '--version'], '/share/project/inside'))
        self.assertTrue(rm24.rm_touches_target(['rm', '.'], '/share'))

    def test_rm_scope_ignores_options_unrelated_paths_and_non_rm(self):
        cases = [(['rm', '--version'], '/root'),
                 (['rm', '--interactive=/share/project'], '/root'),
                 (['rm', '--preserve-root', '-rf'], '/root'),
                 (['rm', '-rf', '/share-mixed/project'], '/root'),
                 (['rm', '-rf', '/share/project-old'], '/root'),
                 (['rm', '-rf', '--', '-rf'], '/root'),
                 (['rmdir', '/share/project'], '/root'),
                 ([], '/root')]
        for args, cwd in cases:
            with self.subTest(args=args, cwd=cwd):
                self.assertFalse(rm24.rm_touches_target(args, cwd))

    def test_preflight_rejects_external_cwd_absolute_source_rm(self):
        with mock.patch.object(Path, 'iterdir', return_value=iter([Path('/proc/12345')])), \
                mock.patch.object(Path, 'read_bytes', return_value=b'/usr/bin/rm\0-rf\0/share/project/tree\0'), \
                mock.patch.object(rm24.os, 'readlink', return_value='/root'):
            with self.assertRaisesRegex(ValueError, 'existing rm under target: 12345'):
                self.check_host('extra-worker-0', rm24.NODE_HOSTS['extra-worker-0'], reject_running_rm=True)

    def prepared(self, **changes):
        run = self.base/'control'
        with mock.patch.object(rm24, 'preflight', return_value=[]), contextlib.redirect_stdout(io.StringIO()):
            rm24.prepare(run, 4, seed_tasks=[], supplement_of='private-original')
        cfg = rm24.load(run/'config.json')
        cfg.update(changes)
        rm24.atomic(run/'config.json', cfg)
        return run, cfg

    def test_prepare_freezes_hostname_mapping_and_script_sha(self):
        run, cfg = self.prepared()
        self.assertEqual(cfg['node_hosts'], rm24.NODE_HOSTS)
        self.assertEqual(cfg['nodes'], rm24.NODES)
        self.assertEqual(cfg['sha256'], hashlib.sha256((run/'rm24.py').read_bytes()).hexdigest())
        self.assertEqual((run/'rm24.py').read_bytes(), Path(rm24.__file__).read_bytes())

    def test_worker_rejects_changed_mapping_before_touching_source(self):
        run, cfg = self.prepared(node_hosts=dict(rm24.NODE_HOSTS, **{'extra-worker-0': 'wrong-host'}))
        with mock.patch.object(rm24, 'node_check') as check, mock.patch.object(rm24, 'Root') as root:
            with self.assertRaisesRegex(ValueError, 'hostname registry differs'):
                rm24.worker(run, 'extra-worker-0')
            check.assert_not_called()
            root.assert_not_called()

    def test_worker_keeps_legacy_private_fixture_compatibility(self):
        run, cfg = self.prepared()
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                view = dict(cfg)
                if legacy:
                    view.pop('node_hosts')
                rm24.atomic(run/'config.json', view)
                with mock.patch.object(rm24, 'node_check', side_effect=StopBeforeRoot) as check:
                    with self.assertRaises(StopBeforeRoot):
                        rm24.worker(run, 'extra-worker-0')
                    check.assert_called_once_with('extra-worker-0')

    def test_worker_still_requires_exact_script_sha(self):
        run, cfg = self.prepared(sha256='wrong')
        with mock.patch.object(rm24, 'node_check') as check:
            with self.assertRaisesRegex(ValueError, 'frozen script changed'):
                rm24.worker(run, 'extra-worker-0')
            check.assert_not_called()

    def test_preflight_pool_has_40_slots_and_checks_all_registered_nodes(self):
        def checked(command, **kwargs):
            return types.SimpleNamespace(returncode=0, stdout=json.dumps(dict(node=command[-2])), stderr='')
        with mock.patch.object(rm24.futures, 'ThreadPoolExecutor', RecordingExecutor), \
                mock.patch.object(rm24, 'ssh', side_effect=lambda node, *args: ['private-command', node, 'check']), \
                mock.patch.object(rm24.subprocess, 'run', side_effect=checked):
            result = rm24.preflight(self.base/'never-executed.py')
        self.assertEqual(RecordingExecutor.capacities, [40])
        self.assertEqual([row['node'] for row in result], rm24.NODES)

    def test_controller_fleet_has_40_slots_without_launching_any_process(self):
        run, cfg = self.prepared()
        with mock.patch.object(rm24.futures, 'ThreadPoolExecutor', RecordingExecutor), \
                mock.patch.object(rm24, 'preflight', return_value=[]), contextlib.redirect_stdout(io.StringIO()):
            rm24.controller(run)
        self.assertEqual(RecordingExecutor.capacities, [40])
        self.assertEqual(RecordingExecutor.submitted, rm24.NODES)
        self.assertEqual(rm24.load(run/'status.json')['state'], 'supplement_complete')

    def test_runtime_admission_accepts_extra_alias_with_the_existing_shared_queue(self):
        run, cfg = self.prepared(runtime_join=True)
        source, finished, stop = queue.Queue(), threading.Event(), threading.Event()
        source.put(dict(id='existing-queue-task'))
        actor = dict(pid=123, start='private', boot='private')
        joins = rm24.RuntimeJoins(run, cfg, source, finished, stop, ['master-0'], actor=actor)
        request = dict(version=1, nonce='new-extra', node='extra-worker-14', run=str(run.resolve()),
                       controller=actor, config_sha256=joins.config_sha256, per_node=4)
        rm24.atomic(run/'runtime-requests/new-extra.json', request)
        submitted = []
        joins.poll(lambda item: submitted.append(item))
        self.assertEqual(submitted, [request])
        self.assertTrue(joins.ready(request))
        self.assertEqual(joins.claim()['id'], 'existing-queue-task')
        self.assertEqual(rm24.load(run/'runtime-acks/new-extra.json')['state'], 'accepted')

    def test_cli_node_choices_include_extra_aliases(self):
        argv = ['rm24.py', '_check', '--run', str(self.base/'control'), '--node', 'extra-worker-14']
        with mock.patch.object(sys, 'argv', argv), \
                mock.patch.object(rm24, 'node_check', return_value=dict(node='extra-worker-14')) as check, \
                contextlib.redirect_stdout(io.StringIO()):
            rm24.main()
        check.assert_called_once_with('extra-worker-14', reject_running_rm=True)


if __name__ == '__main__':
    unittest.main()
