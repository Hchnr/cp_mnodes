import copy
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import deploy_priority4_path_canary as deploy


class P4CanaryControlTests(unittest.TestCase):
    def test_audit_must_be_fresh_and_cover_all_48_original_copies(self):
        now = time.time()
        audit = dict(all_unchanged=True, signals_sent=0, copy_process_count=48, verified_at=now,
            nodes=[dict(node=n, verified_at=now) for n in ['master-0'] + ['worker-'+str(i) for i in range(11)]])
        deploy.validate_audit(audit, now)
        for changes in (dict(copy_process_count=47), dict(verified_at=now-100), dict(signals_sent=1),
                        dict(nodes=audit['nodes'][:-1]), dict(all_unchanged=False)):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                deploy.validate_audit(dict(audit, **changes), now)

    def test_exit_mm_empty_argv_is_not_an_exit_but_same_start_zombie_is(self):
        expected = dict(pid=123, starttime_ticks='456', argv=['old'])
        for state, done in (('R', False), ('D', False), ('Z', True), ('X', True)):
            with mock.patch.object(deploy.protocol, 'proc_identity',
                                   return_value=dict(pid=123,starttime_ticks='456',state=state,argv=[''])):
                self.assertEqual(deploy.predecessor_done(expected), done)
        with mock.patch.object(deploy.protocol, 'proc_identity',
                               return_value=dict(pid=123,starttime_ticks='457',state='Z',argv=[''])):
            with self.assertRaises(RuntimeError):
                deploy.predecessor_done(expected)

    def test_command_keeps_candidate_lane_threads_and_required_checkpoint(self):
        command = deploy.child_command(Path('/fixture/checkpoint'), 'operation', '/usr/bin/python3')
        self.assertIn('supplement', command)
        self.assertEqual(command[command.index('--threads')+1], '64')
        self.assertEqual(command[command.index('--progress-operation')+1], 'operation')
        self.assertEqual(command[command.index('--cache-max-age')+1], '30.0')
        self.assertNotIn('primary', command)

    def test_only_proven_pre_signal_abort_may_be_regated(self):
        target=dict(pid=1,starttime_ticks='2',argv=['original'])
        old=dict(status='waiting_payload',signals_sent=0,expected=target)
        self.assertTrue(deploy.previous_aborted_before_signal(old,target))
        for changes in (dict(status='idle_verified'),dict(signals_sent=1),dict(helper={'pid':1}),
                        dict(replacement_pid=3),dict(expected=dict(target,pid=3))):
            self.assertFalse(deploy.previous_aborted_before_signal(dict(old,**changes),target))

    def test_only_exact_two_inode_matching_append_logs_are_exempt(self):
        log=Path('/fixture/destination/control/log')
        entries=[dict(fd='1',path=str(log)),dict(fd='2',path=str(log)),
                 dict(fd='3',path=str(log)),dict(fd='4',path='/fixture/source/payload')]
        info=SimpleNamespace(st_mode=0o100600,st_dev=1,st_ino=2)
        with mock.patch.object(deploy.protocol,'payload_fds',return_value=entries), \
                mock.patch.object(Path,'read_text',return_value='flags:\t02002001\n'), \
                mock.patch.object(Path,'lstat',return_value=info), \
                mock.patch.object(deploy.os,'stat',return_value=info):
            result=deploy.live_data_fds(123,('/fixture/source','/fixture/destination'),log)
            self.assertEqual(result,entries[2:])
        with mock.patch.object(deploy.protocol,'payload_fds',return_value=entries), \
                mock.patch.object(Path,'read_text',return_value='flags:\t02000000\n'), \
                mock.patch.object(Path,'lstat',return_value=info), \
                mock.patch.object(deploy.os,'stat',return_value=info):
            self.assertEqual(deploy.live_data_fds(123,('/fixture/source','/fixture/destination'),log),entries)

    def remote(self):
        remote = object.__new__(deploy.Remote)
        remote.target = dict(pid=123,starttime_ticks='456',argv=['old'])
        remote.cfg = dict(source='/fixture/source',destination='/fixture/destination',run_id='fixture')
        remote.node_path = Path('/fixture/node')
        remote.directory = Path('/fixture/controller')
        remote.worker = mock.Mock()
        remote.master = mock.Mock()
        remote.active = mock.Mock()
        remote.protected = mock.Mock()
        remote.save = mock.Mock()
        remote.broker = mock.Mock(return_value=dict(draining_hosts=[deploy.HOST]))
        return remote

    def test_missing_idle_lease_or_two_samples_never_signal(self):
        for change in (dict(status='draining'),dict(lane_leases={'p4-r1':1}),dict(samples=1)):
            remote = self.remote()
            remote.record = mock.Mock(return_value=dict(dict(status='idle_verified',lane_leases={},samples=2),**change))
            with mock.patch.object(deploy.os,'kill') as kill:
                with self.assertRaises(RuntimeError):remote.replace()
                kill.assert_not_called()

    def test_busy_native_snapshot_or_payload_fd_never_signal(self):
        with tempfile.TemporaryDirectory(prefix='p4-canary-gate-fixture-') as name:
            for payloads, idle, draining in (([dict(fd=10)],True,True),([],False,True),([],True,False)):
                remote = self.remote()
                remote.record = mock.Mock(return_value=dict(status='idle_verified',lane_leases={},samples=2,barrier=0))
                remote.broker.return_value = dict(draining_hosts=[deploy.HOST] if draining else [])
                with mock.patch.object(deploy.protocol,'idle_sample',return_value=idle), \
                        mock.patch.object(deploy.protocol,'payload_fds',return_value=payloads), \
                        mock.patch.object(deploy,'read_json',return_value={'pid':123}), \
                        mock.patch.object(deploy,'require_actor',return_value=remote.target), \
                        mock.patch.object(deploy.fcntl,'flock'), \
                        mock.patch('builtins.open',mock.mock_open()), \
                        mock.patch.object(deploy.os,'kill') as kill:
                    with self.assertRaises(RuntimeError):remote.replace()
                    kill.assert_not_called()

    def test_successful_release_waits_for_new_worker_record_not_stale_idle(self):
        remote = self.remote()
        idle = dict(status='idle_verified',idle={'bytes':10},added_by_this_rollout=True)
        ready = dict(idle,status='workers_ready',replacement={'pid':999})
        remote.record = mock.Mock(side_effect=[copy.deepcopy(idle),copy.deepcopy(ready)])
        with mock.patch.object(deploy.time,'sleep') as sleep, \
                mock.patch.object(deploy,'read_json',return_value=dict(pid=999,active={},bytes=10)), \
                mock.patch.object(deploy.protocol,'counter_check'):
            value=remote.release(999)
        self.assertEqual(value['status'],'complete')
        sleep.assert_called_once()
        self.assertEqual(remote.save.call_args.args[0]['replacement']['pid'],999)
        self.assertEqual(remote.broker.call_args.args[0]['draining_hosts'],[])

    def test_failed_pre_replacement_gate_can_release_own_host_barrier(self):
        remote=self.remote()
        remote.record=mock.Mock(return_value=dict(status='idle_verified',added_by_this_rollout=True))
        value=remote.release()
        self.assertEqual(value['status'],'waiting_payload')

    def test_successful_release_also_waits_for_new_heartbeat_visibility(self):
        remote=self.remote()
        remote.record=mock.Mock(return_value=dict(status='workers_ready',replacement={'pid':999},
                                                  idle={'bytes':10},added_by_this_rollout=True))
        with mock.patch.object(deploy.time,'sleep') as sleep, \
                mock.patch.object(deploy,'read_json',side_effect=[dict(pid=123,active={}),dict(pid=999,active={},bytes=10)]), \
                mock.patch.object(deploy.protocol,'counter_check'):
            self.assertEqual(remote.release(999)['status'],'complete')
        sleep.assert_called_once()

    def test_release_rpc_carries_exact_returned_replacement_identity(self):
        rpc=deploy.RPC(control=str(deploy.CONTROL),job=deploy.JOB,node='worker-10',lane='supplement',
            remote_script='/fixture/deploy_priority4_path_canary.py',ssh_prefix='copy-',
            output='/tmp/p4-canary-fixture-output',operation='op')
        with mock.patch.object(deploy.BridgeRPC,'__call__',return_value=dict(ready=True,replacement={'pid':999})):
            rpc('worker-10','replace',lane='supplement')
        self.assertIn('--expected-replacement-pid 999',rpc.command('master-0','release')[-1])


if __name__=='__main__':unittest.main()
