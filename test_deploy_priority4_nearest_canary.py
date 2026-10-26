import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import deploy_priority4_nearest_canary as deployment
import deploy_priority4_path_canary as transaction
import migration_priority4_nearest_worker as entry
import test_deploy_priority4_path_canary as original_tests


class SecondCanaryBindingTests(unittest.TestCase):
    def test_context_is_exact_and_restores_first_controller_even_after_failure(self):
        saved = transaction.BUNDLE, transaction.CANDIDATE_MODULE, transaction.SCOPE
        with self.assertRaisesRegex(RuntimeError, 'fixture'):
            with deployment.configured():
                self.assertEqual(transaction.SCOPE, entry.SCOPE)
                self.assertEqual(transaction.AUTHORIZATION, entry.AUTHORIZATION)
                self.assertEqual(transaction.DIRECTORY_STRATEGY, entry.STRATEGY)
                self.assertEqual(transaction.CANDIDATE_ENTRY, entry.ENTRY)
                self.assertEqual(transaction.PREDECESSOR_LOG,
                    transaction.CONTROL / entry.PREVIOUS_NAME / entry.PREVIOUS_CONTROLLER / 'worker.log')
                self.assertEqual(transaction.CONTROLLER_ENTRY, deployment.ENTRY)
                self.assertIn(deployment.ENTRY, transaction.FILES)
                command = transaction.child_command(transaction.CHECKPOINT, 'second', '/usr/bin/python3')
                self.assertEqual(command[2], str(deployment.BUNDLE / entry.ENTRY))
                self.assertNotIn('primary', command)
                self.assertEqual(command[command.index('--threads')+1], '64')
                raise RuntimeError('fixture')
        self.assertEqual((transaction.BUNDLE, transaction.CANDIDATE_MODULE, transaction.SCOPE), saved)

    def test_new_checkpoint_receives_new_policy_but_first_signature_stays_unchanged(self):
        module = SimpleNamespace(validate_bundle=mock.Mock(return_value=({}, {'fixture': 'policy'})),
                                 checkpoint_identity=mock.Mock(return_value='guard'))
        with mock.patch.object(transaction.importlib, 'import_module', return_value=module) as imported:
            transaction.validate_candidate_checkpoint(Path('/fixture/checkpoint'), 'first')
            imported.assert_called_with('migration_priority4_path_worker')
            self.assertEqual(len(module.checkpoint_identity.call_args.args), 6)
            with deployment.configured():
                self.assertEqual(transaction.validate_candidate_checkpoint(Path('/fixture/checkpoint'), 'second'), 'guard')
                imported.assert_called_with('migration_priority4_nearest_worker')
                self.assertEqual(module.checkpoint_identity.call_args.args[-1], {'fixture': 'policy'})
                self.assertEqual(len(module.checkpoint_identity.call_args.args), 7)

    def test_unknown_entry_is_not_imported_or_dispatched(self):
        with mock.patch.object(transaction, 'CANDIDATE_MODULE', 'unknown'), \
                mock.patch.object(transaction.importlib, 'import_module') as imported:
            with self.assertRaises(RuntimeError):
                transaction.validate_candidate_checkpoint(Path('/fixture/checkpoint'), 'op')
            imported.assert_not_called()

    def test_policy_identity_config_strategy_and_log_drift_fail_before_any_mutation(self):
        cfg = dict(job='fixture', supplement_io_bytes_per_second=5000000000)
        target = dict(pid=918041, starttime_ticks='1071970035', argv=['fixture'])
        with deployment.configured():
            policy = dict(scope=entry.SCOPE, directory_strategy=entry.STRATEGY,
                control=str(transaction.CONTROL), node='worker-10', lane='supplement',
                predecessor=target, predecessor_stdout_log=str(deployment.PREDECESSOR_LOG),
                config_sha256=hashlib.sha256(json.dumps(cfg, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
            with mock.patch.object(Path, 'is_symlink', return_value=False), \
                    mock.patch.object(Path, 'exists', return_value=False), \
                    mock.patch.object(Path, 'read_bytes', side_effect=lambda: json.dumps(policy).encode()), \
                    mock.patch.object(transaction.os, 'kill') as kill:
                transaction.verify_rollout_policy(cfg, target)
                for change in (dict(scope='first-scope'), dict(directory_strategy='frontier_path_anchor'),
                               dict(node='worker-9'), dict(lane='primary'), dict(config_sha256='bad'),
                               dict(predecessor=dict(target,pid=693055)), dict(predecessor_stdout_log='/wrong/log')):
                    saved = copy.deepcopy(policy)
                    policy.update(change)
                    with self.assertRaises(RuntimeError):
                        transaction.verify_rollout_policy(cfg, target)
                    policy = saved
                with self.assertRaises(RuntimeError):
                    transaction.verify_rollout_policy(dict(cfg,supplement_io_bytes_per_second=6000000000), target)
                kill.assert_not_called()

    def test_validation_pack_without_authorization_cannot_start_transaction(self):
        with tempfile.TemporaryDirectory(prefix='second-controller-fixture-') as name:
            root = Path(name)
            controller = root / 'controller-validation-fixture'
            controller.mkdir()
            with deployment.configured(), \
                    mock.patch.object(transaction, 'BUNDLE', root), \
                    mock.patch.object(transaction, '__file__', str(controller / 'deploy_priority4_path_canary.py')), \
                    mock.patch.object(transaction, 'freeze_verify'), \
                    mock.patch.object(transaction, 'read_json', side_effect=FileNotFoundError('missing authority')), \
                    mock.patch.object(transaction, 'RPC') as rpc, \
                    mock.patch.object(transaction.protocol, 'roll_one') as roll:
                with self.assertRaises(FileNotFoundError):
                    transaction.run(controller)
                rpc.assert_not_called()
                roll.assert_not_called()

    def test_bad_policy_fails_remote_constructor_before_any_http_opener(self):
        with deployment.configured():
            directory = Path('/fixture/controller')
            cfg = dict(job=transaction.JOB, migration_scope='project_priority4',
                       source='/share/project',destination='/share-mixed/project',exclude=['eai_pwm'])
            authority = dict(operation='second',user_authorization=entry.AUTHORIZATION,
                control=str(transaction.CONTROL),bundle=str(transaction.BUNDLE),node='worker-10',
                lane='supplement',signal_limit=1)
            baseline = {'target': dict(pid=918041,start_ticks='1071970035',argv=['fixture'])}
            def read(path):
                if path==transaction.CONTROL/'config.json':return cfg
                if path==directory/'authorization.json':return authority
                if path==transaction.BUNDLE/'preparation-after.json':return baseline
                raise AssertionError(path)
            with mock.patch.object(transaction, 'freeze_verify'), \
                    mock.patch.object(transaction, 'read_json', side_effect=read), \
                    mock.patch.object(transaction.socket,'gethostname',return_value=transaction.HOST), \
                    mock.patch.object(transaction,'verify_rollout_policy',side_effect=RuntimeError('bad policy')), \
                    mock.patch.object(transaction.urllib.request,'build_opener') as opener:
                with self.assertRaisesRegex(RuntimeError,'bad policy'):
                    transaction.Remote(directory,'second')
                opener.assert_not_called()


class SecondCanaryTransactionTests(original_tests.P4CanaryControlTests):
    """Run the unchanged transaction guards under the new fixed binding too."""
    def setUp(self):
        context = deployment.configured()
        context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)


if __name__ == '__main__':
    unittest.main()
