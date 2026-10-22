"""Pure saved-object fixtures for the42-role process collector composition."""
import copy
import unittest

import observe_priority4_dependency_processes as collector
from test_profile_copy_proc_only import evidence, CONTROL, JOB


def inputs():
    rich, argv = evidence()
    active = {'control': CONTROL, 'job': JOB, 'ssh_owner': 'fixture-owner'}
    for node in rich['nodes']:
        if node['host'].endswith('-worker-5'):
            node['identities']['primary'].update(pid=1881537, start_ticks=4105099)
    for node in argv['nodes']:
        if node['node'] == 'worker-5':
            for point in ('first', 'second'):
                node[point]['lanes']['primary'].update(pid=1881537, start_ticks='4105099')
        if node['node'] == 'master-0':
            for point in ('first', 'second'):
                node[point]['roles'] = {name: {'pid': pid, 'start_ticks': ticks,
                    'identity_valid': True, 'state': 'S',
                    'argv': ['python3', CONTROL + '/' + name + '.py', '--control', CONTROL]}
                    for name, (pid, ticks) in collector.MASTER.items()}
    checks = [{'node': node['node'], 'lane': lane, 'pid': value['pid'], 'start_ticks': str(value['start_ticks'])}
              for node in argv['nodes'] for lane, value in node['first']['lanes'].items()]
    authorization = {'control': CONTROL, 'job': JOB,
                     'operation': '7e9c628399b4439cb7af7e40045e9e23',
                     'changed': [['worker-5', 'primary', 1881537, '4105099']],
                     'new_verified_baseline': 'work/' + collector.FILES['rich'],
                     'full_argv_verification': {'path': 'work/' + collector.FILES['argv'], 'errors': [], 'checks': checks}}
    recovery = {'control': CONTROL, 'job': JOB, 'roles': {'master-0:watchdog':
                {'pid': 167021, 'startticks': '500376945',
                 'argv': ['python3', CONTROL + '/watchdog.py', '--control', CONTROL]}}}
    seed = {'pid': 179557, 'starttime_ticks': '500401283',
            'argv': ['python3', CONTROL + '/resume.py', 'seed', '--control', CONTROL]}
    services = {'control': CONTROL, 'nodes': {}}
    for index, name in enumerate(sorted(collector.EXPECTED_NODES)):
        pid, ticks = (288589, '738952359') if name == 'worker-10' else (300000 + index, str(600000 + index))
        services['nodes'][name] = {'role': {'pid': pid, 'starttime_ticks': ticks,
            'control': CONTROL, 'host': 'job-' + JOB + '-' + name,
            'argv': ['python3', CONTROL + '/probe.py', '--control', CONTROL, '--slots', '32']}}
    return dict(active=active, rich=rich, argv=argv, authorization=authorization,
                recovery=recovery, seed=seed, services=services)


def observations(inventory):
    result = {}
    for node, roles in inventory.items():
        result[node] = {}
        for point, start in (('first', 10), ('second', 13)):
            result[node][point] = {'control': CONTROL, 'started_at': start, 'finished_at': start + 1,
                'complete': True, 'errors': [], 'quiescent_snapshot': True, 'live_associated': [],
                'registered': [{**role, 'status': 'pid_absent', 'observation': None,
                                'old_identity_exited': True} for role in copy.deepcopy(roles)]}
    return result


class CollectorCompositionTests(unittest.TestCase):
    def setUp(self):
        self.saved = inputs()

    def build(self):
        return collector.build_inventory(**self.saved)

    def test_exact42_roles_on12_nodes_with_only_authorized_canary(self):
        inventory = self.build()
        self.assertEqual(len(inventory), 12)
        self.assertEqual(sum(map(len, inventory.values())), 42)
        self.assertEqual(len(inventory['master-0']), 9)
        primary = next(x for x in inventory['worker-5'] if x['role'] == 'primary')
        self.assertEqual((primary['pid'], primary['start_ticks']), (1881537, '4105099'))
        self.assertEqual(primary['argv'][-2:], ['--cache-max-age', '30.0'])
        self.assertEqual(next(x for x in inventory['worker-10'] if x['role'] == 'metadata_probe')['pid'], 288589)

    def test_wrong_or_incomplete_authorization_cannot_adopt_new_pid(self):
        self.saved['authorization']['changed'][0][2] = 999999
        with self.assertRaises(ValueError):
            self.build()
        self.saved = inputs()
        self.saved['authorization']['full_argv_verification']['checks'].pop()
        with self.assertRaises(ValueError):
            self.build()

    def test_missing_probe_node_or_wrong_host_or_control_refused(self):
        for key in ('host', 'control'):
            self.saved = inputs()
            self.saved['services']['nodes']['worker-0']['role'][key] = 'different'
            with self.assertRaises(ValueError):
                self.build()
        self.saved = inputs()
        self.saved['services']['nodes'].pop('worker-0')
        with self.assertRaises(ValueError):
            self.build()

    def test_master_seed_and_watchdog_identity_changes_refused(self):
        self.saved['seed']['starttime_ticks'] = '999'
        with self.assertRaises(ValueError):
            self.build()
        self.saved = inputs()
        self.saved['recovery']['roles']['master-0:watchdog']['pid'] = 999999
        with self.assertRaises(ValueError):
            self.build()
        self.saved = inputs()
        master = next(x for x in self.saved['argv']['nodes'] if x['node'] == 'master-0')
        master['second']['roles']['producer']['argv'] += ['--different']
        with self.assertRaises(ValueError):
            self.build()

    def test_duplicate_registered_pid_within_node_refused(self):
        node = next(x for x in self.saved['argv']['nodes'] if x['node'] == 'worker-0')
        self.saved['services']['nodes']['worker-0']['role']['pid'] = node['first']['lanes']['primary']['pid']
        with self.assertRaises(ValueError):
            self.build()

    def test_complete_quiescent_evidence_still_cannot_launch(self):
        inventory = self.build()
        result = collector.summarize_observations(inventory, observations(inventory), [], CONTROL)
        self.assertTrue(result['full_cluster_process_observation_complete'])
        self.assertTrue(result['two_point_process_quiescence_observed'])
        self.assertEqual(result['latest_registered_status_counts'], {'pid_absent': 42})
        self.assertFalse(result['launch_ready'])

    def test_missing_node_wrong_identity_scope_or_time_is_incomplete(self):
        inventory = self.build()
        records = observations(inventory)
        records.pop('worker-0')
        self.assertFalse(collector.summarize_observations(inventory, records, [], CONTROL)['full_cluster_process_observation_complete'])
        for change in ('identity', 'scope', 'time'):
            records = observations(inventory)
            point = records['master-0']['second']
            if change == 'identity':
                point['registered'][0]['pid'] += 1
            elif change == 'scope':
                point['control'] = CONTROL + '-sibling'
            else:
                point['started_at'] = 5
            self.assertFalse(collector.summarize_observations(inventory, records, [], CONTROL)['full_cluster_process_observation_complete'])

    def test_live_roles_and_unknown_associated_processes_block_even_with_true_flag(self):
        inventory = self.build()
        records = observations(inventory)
        for point in ('first', 'second'):
            role = records['master-0'][point]['registered'][0]
            role.update(status='running_exact', old_identity_exited=False,
                        observation={key: role[key] for key in ('pid', 'start_ticks', 'argv')})
            role['observation'].update(state='S', open_fd_count=8)
        result = collector.summarize_observations(inventory, records, [], CONTROL)
        self.assertTrue(result['full_cluster_process_observation_complete'])
        self.assertFalse(result['two_point_process_quiescence_observed'])
        records = observations(inventory)
        records['worker-0']['second']['live_associated'] = [{'pid': 999999, 'start_ticks': '1', 'argv': ['python', CONTROL + '/new.py'], 'state': 'R'}]
        self.assertFalse(collector.summarize_observations(inventory, records, [], CONTROL)['two_point_process_quiescence_observed'])
        records['worker-0']['second']['errors'] = [{'where': 'census', 'error': 'PermissionError'}]
        self.assertFalse(collector.summarize_observations(inventory, records, [], CONTROL)['full_cluster_process_observation_complete'])


if __name__ == '__main__':
    unittest.main()
