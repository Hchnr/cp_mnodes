"""Private temporary trees only; real SSH/start/rm/signal are tripwires."""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cooperative_tail as coop


class CooperativeTailTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cooperative-tail-private-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.runs = self.base/'runs'
        self.runs.mkdir()
        self.old, self.original = self.runs/'tail-boost-private', self.runs/'prepared-v3'
        self.old.mkdir()
        self.original.mkdir()
        self.target = self.base/'private-source'
        self.pipeline = self.target/coop.PIPELINE_REL
        self.pipeline.mkdir(parents=True)
        for name in coop.CHILDREN:
            (self.pipeline/name/'batch'/'job').mkdir(parents=True)
        self.fd_proof = dict(raw_path=str(self.pipeline/'tasks'/'batch'),
                             ino=(self.pipeline/'tasks'/'batch').stat().st_ino)
        self.actor = dict(pid=999991, start='private-control-start', boot='private-control-boot')
        self.original_actor = dict(pid=999992, start='private-original-start', boot='private-control-boot')
        self.boots = {node: 'private-boot-'+node for node in coop.rm24.NODES}
        self.boots[coop.ANCHOR_NODE] = coop.ANCHOR_RM['boot']
        for name in coop.SOURCE_NAMES:
            (self.base/name).write_text('# private nonexecuting source '+name+'\n')
        for run in (self.old, self.original):
            (run/'rm24.py').write_text('# private frozen engine\n')
        for name in coop.HELPERS:
            (self.old/name).write_text('# private frozen helper '+name+'\n')
        self.main_pointer = self.runs/'tail-boost-current.json'
        for obj, key, value in (
            (coop, 'HERE', self.base), (coop, 'POINTER', self.runs/'cooperative-tail-current.json'),
            (coop.tailboost, 'POINTER', self.main_pointer),
            (coop.rm24, 'TARGET', str(self.target)), (coop.rm24, 'ROOT_INODE', self.target.stat().st_ino),
            (coop.rm24, 'RUN', self.original)):
            self.patch(obj, key, new=value)
        self.write(self.main_pointer, dict(run=str(self.old), original=str(self.original)))
        base = dict(target=str(self.target), root_inode=self.target.stat().st_ino,
            sha256=coop.digest(self.original/'rm24.py'), nodes=coop.rm24.NODES[:24])
        self.write(self.original/'config.json', base)
        self.write(self.original/'controller.json', self.original_actor)
        self.write(self.original/'STOP', dict(reason='private-original-self-stop'))
        self.cfg = dict(base, sha256=coop.digest(self.old/'rm24.py'), nodes=coop.rm24.NODES,
            node_hosts=coop.rm24.NODE_HOSTS, per_node=4, runtime_join=True,
            supplement_of=str(self.original), execution_nodes=coop.rm24.NODES[:4],
            tail_scopes=[dict(rel=coop.PIPELINE_REL.rsplit('/', 2)[0], ino=1)],
            parent_policy=dict(limit=2, node_boots=self.boots),
            parent_helpers={name:coop.digest(self.old/name) for name in coop.HELPERS})
        self.write(self.old/'config.json', self.cfg)
        self.write(self.old/'controller.json', self.actor)
        self.status = dict(state='rm_running', updated=coop.time.time(), errors=[],
            runtime_join=dict(accepted=[coop.ANCHOR_NODE], starting=[]),
            parent_scheduler=dict(limit=2, pending=0, active_leases=1, claims=100, releases=99,
                active_flat_groups=0, active_by_boot={coop.ANCHOR_RM['boot']:1}))
        self.write(self.old/'status.json', self.status)
        self.before_original = self.snapshot(self.original)
        self.before_main = self.main_pointer.read_bytes()
        self.inspections, self.prepares, self.starts = [], [], []
        self.row_hook = None
        self.freeze_hook = None
        self.inspect_mock = self.patch(coop, 'inspect_nodes', side_effect=self.inspect)
        self.patch(coop.scale, 'alive', return_value=True)
        self.prepare_mock = self.patch(coop.rm24, 'prepare', side_effect=self.fake_prepare)
        self.start_mock = self.patch(coop.subprocess, 'run', side_effect=self.start)
        self.ready_mock = self.patch(coop.verify_workers, 'wait_workers_ready', side_effect=self.ready)

    def patch(self, obj, name, **kwargs):
        patcher = mock.patch.object(obj, name, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value))

    @staticmethod
    def snapshot(path):
        return {str(p.relative_to(path)):p.read_bytes() for p in path.rglob('*') if p.is_file()}

    def rows(self):
        rows = []
        for node in coop.rm24.NODES:
            row = dict(node=node, boot=self.boots[node],
                check=dict(node=node, host=coop.rm24.NODE_HOSTS[node]),
                helpers=dict(old=[], original=[]), rms=dict(old=[], original=[], unknown=[]))
            if node == coop.ANCHOR_NODE:
                row['helpers']['old'] = [copy.deepcopy(coop.ANCHOR_HELPER)]
                row['rms']['old'] = [dict(coop.ANCHOR_RM, ppid=coop.ANCHOR_HELPER['pid'],
                    argv=['/usr/bin/rm', '-rf', '--one-file-system', '--', self.pipeline.name],
                    cwd=str(self.pipeline.parent), cwd_raw=str(self.pipeline.parent),
                    cwd_ino=self.pipeline.parent.stat().st_ino,
                    dir_fds=[dict(self.fd_proof)],
                    opened=[str(self.pipeline/'tasks'/'busy-batch')])]
            rows.append(row)
        return rows

    def inspect(self, old, original):
        self.assertEqual((Path(old), Path(original)), (self.old, self.original))
        self.inspections.append(True)
        rows = self.rows()
        if self.row_hook:
            self.row_hook(rows)
        return rows

    def fake_prepare(self, run, per_node, **options):
        self.assertEqual(per_node, 4)
        self.assertTrue(Path(run).is_relative_to(self.runs))
        self.prepares.append((run, copy.deepcopy(options)))
        run.mkdir()
        for name in ('rm24.py',)+coop.HELPERS:
            shutil.copyfile(self.base/name, run/name)
        cfg = dict(target=str(self.target), root_inode=self.target.stat().st_ino,
            nodes=coop.rm24.NODES, node_hosts=coop.rm24.NODE_HOSTS, per_node=4,
            sha256=coop.digest(run/'rm24.py'), supplement_of=str(self.original),
            execution_nodes=options['execution_nodes'], runtime_join=True,
            tail_scopes=options['tail_scopes'], blocked_paths=options['blocked_paths'],
            busy_paths=options['busy_paths'], parent_policy=options['parent_policy'],
            parent_helpers={name:coop.digest(run/name) for name in coop.HELPERS})
        self.write(run/'config.json', cfg)
        if self.freeze_hook:
            self.freeze_hook(run)

    def start(self, command, **options):
        # Tripwire: no SSH or unrecognised command can escape this fixture.
        self.assertEqual(command[1:4], ['-S', '-B', '-u'])
        run = Path(command[4]).parent
        self.assertTrue(run.is_relative_to(self.runs))
        self.assertEqual(command[5:], ['start', '--run', str(run)])
        self.assertTrue(options.get('check'))
        self.starts.append(run)
        return mock.Mock(returncode=0)

    def ready(self, run, nodes, timeout=90):
        self.assertEqual(self.starts, [run])
        return dict(state='workers_ready', verified_nodes=4, nodes=list(nodes),
            proofs=[dict(node=node, boot=self.boots[node]) for node in nodes],
            status=dict(state='rm_running', updated=coop.time.time(), errors=[],
                parent_scheduler=dict(limit=2, pending=3, active_leases=4, claims=10, releases=6),
                parent_tuning=dict(limit=2, generation=0, error=None)))

    def prepare(self, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return coop.prepare(**kwargs)

    def apply(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return coop.apply()

    def plan(self):
        pointer = coop.rm24.load(coop.POINTER)
        work = Path(pointer['work'])
        return work, coop.rm24.load(work/'plan.json')

    def untouched(self):
        self.assertEqual(self.main_pointer.read_bytes(), self.before_main)
        self.assertEqual(self.snapshot(self.original), self.before_original)
        self.assertFalse((self.old/'STOP').exists())
        self.assertFalse((self.old/'upgrade40-claim.json').exists())

    def test_prepare_is_readonly_scoped_bounded_and_separate(self):
        result = self.prepare()
        work, plan = self.plan()
        self.assertFalse(result['started'])
        self.assertEqual(plan['execution_nodes'], list(coop.PILOTS))
        self.assertEqual(len(plan['node_boots']), 40)
        self.assertEqual({x['rel'] for x in result['scopes']},
                         {coop.PIPELINE_REL+'/'+name for name in coop.CHILDREN})
        self.assertLessEqual(plan['preview']['tasks'], 96)
        self.assertTrue(plan['preview']['tasks'])
        self.assertNotIn(coop.PIPELINE_REL, [x['parent'] for x in plan['preview']['samples']])
        self.assertEqual(len(self.inspections), 2)
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])
        self.assertTrue((work/'previous-status.json').exists())
        self.assertFalse((self.old/'cooperative-claim.json').exists())
        self.untouched()

    def test_apply_launches_one_bound_queue_without_old_stop_or_pointer_change(self):
        self.prepare()
        result = self.apply()
        self.assertTrue(result['ready'])
        self.assertTrue(result['effective_speedup_not_yet_measured'])
        self.assertEqual(len(self.starts), 1)
        run, options = self.prepares[0]
        self.assertEqual(result['run'], str(run))
        self.assertEqual(options['parent_policy'], dict(limit=2, node_boots=self.boots))
        self.assertTrue(options['runtime_join'])
        self.assertEqual(options['supplement_of'], self.original)
        self.assertEqual(options['execution_nodes'], list(coop.PILOTS))
        self.assertEqual(len(self.inspections), 4)  # all probes precede start
        self.assertTrue((self.old/'cooperative-claim.json').exists())
        self.untouched()

    def test_repeated_apply_and_second_prepared_plan_share_exclusive_anchor_claim(self):
        first = self.prepare()
        self.prepare()
        self.apply()
        with self.assertRaises(FileExistsError):
            self.apply()
        self.assertEqual(len(self.starts), 1)
        with self.assertRaisesRegex(ValueError, 'already has a cooperative claim'):
            self.prepare()
        self.untouched()

    def test_unknown_rm_refuses_without_claim_or_start(self):
        self.prepare()
        self.row_hook = lambda rows:rows[0]['rms']['unknown'].append(dict(coop.ANCHOR_RM))
        with self.assertRaisesRegex(ValueError, 'unowned target rm'):
            self.apply()
        self.assertFalse((self.old/'cooperative-claim.json').exists())
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_changed_or_exited_anchor_and_argv_are_rejected(self):
        self.prepare()
        for change in ('exit', 'pid', 'argv', 'cwd', 'owner'):
            def changed(rows):
                row = next(r for r in rows if r['node'] == coop.ANCHOR_NODE)
                if change == 'exit':
                    row['rms']['old'] = []
                elif change == 'pid':
                    row['rms']['old'][0]['start'] = 'reused-pid'
                elif change == 'argv':
                    row['rms']['old'][0]['argv'][-1] = '..'
                elif change == 'cwd':
                    row['rms']['old'][0]['cwd'] = str(self.target)
                else:
                    row['rms']['old'][0]['ppid'] = 1
            self.row_hook = changed
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.apply()
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_boot_changed_or_shared_pilot_boot_is_rejected(self):
        self.prepare()
        self.row_hook = lambda rows:next(r for r in rows if r['node'] == coop.PILOTS[0]).update(boot='rebooted')
        with self.assertRaisesRegex(ValueError, 'boot identity changed'):
            self.apply()
        self.row_hook = None
        rows = self.rows()
        next(r for r in rows if r['node'] == coop.PILOTS[0])['boot'] = coop.ANCHOR_RM['boot']
        with self.assertRaisesRegex(ValueError, 'separate non-anchor boot'):
            coop.choose_pilots(rows)
        rows = self.rows()
        next(r for r in rows if r['node'] == coop.PILOTS[1])['boot'] = self.boots[coop.PILOTS[0]]
        with self.assertRaisesRegex(ValueError, 'separate non-anchor boot'):
            coop.choose_pilots(rows)
        self.assertEqual(self.starts, [])

    def test_all_three_scopes_missing_refuses_but_one_missing_is_skipped(self):
        self.prepare()
        shutil.rmtree(self.pipeline/'mcap_cache')
        snapshot = coop.scope_snapshot(self.plan()[1]['scope_snapshot'])
        self.assertEqual(len(snapshot['scopes']), 2)
        for name in ('tasks', 'label_work'):
            shutil.rmtree(self.pipeline/name)
        with self.assertRaisesRegex(ValueError, 'all three'):
            self.apply()
        self.assertEqual(self.starts, [])

    def test_unknown_child_symlink_and_replaced_inode_refuse(self):
        self.prepare()
        (self.pipeline/'unexpected').mkdir()
        with self.assertRaisesRegex(ValueError, 'unrecognized'):
            self.apply()
        (self.pipeline/'unexpected').rmdir()
        (self.pipeline/'tasks').rename(self.pipeline/'tasks-old')
        (self.pipeline/'tasks').symlink_to(self.pipeline/'tasks-old', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'unrecognized'):
            self.apply()
        (self.pipeline/'tasks').unlink()
        (self.pipeline/'tasks').mkdir()
        shutil.rmtree(self.pipeline/'tasks-old')
        with self.assertRaisesRegex(ValueError, 'inode changed'):
            self.apply()
        self.assertEqual(self.starts, [])

    def test_parent_inode_pipeline_symlink_and_nested_mount_guards(self):
        self.prepare()
        with mock.patch.object(coop.rm24, 'mount_id', side_effect=[100, 999]):
            with self.assertRaisesRegex(ValueError, 'nested mount'):
                coop.scope_snapshot()
        old = self.pipeline.with_name('old-pipeline')
        self.pipeline.rename(old)
        self.pipeline.symlink_to(old, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'anchored directory'):
            self.apply()
        self.assertEqual(self.starts, [])

    def test_real_probe_template_compiles_and_keeps_positive_cwd_operand_evidence(self):
        self.assertNotEqual(coop.INSPECT, coop.upgrade.INSPECT)
        self.assertIn('cwd=cwd,argv=args', coop.INSPECT)
        self.assertIn('cwd_ino=cwd_ino,dir_fds=dir_fds', coop.INSPECT)
        self.assertIn('raw_path==os.readlink(fd)', coop.INSPECT)
        compile(coop.INSPECT, '<private-readonly-inspect>', 'exec')

    def test_no_pipeline_root_fd_but_anchored_descendant_is_enough(self):
        rows = self.rows()
        snapshot = coop.scope_snapshot()
        proof = coop.verify_tree_anchor(rows, snapshot)
        self.assertEqual(proof['directory_fd']['raw_path'], str(self.pipeline/'tasks'/'batch'))
        self.assertNotEqual(proof['directory_fd']['raw_path'], str(self.pipeline))

    def test_same_named_new_tree_cannot_use_old_descendant_inode(self):
        rows = self.rows()
        displaced = self.pipeline/'tasks'/'batch'
        displaced.rename(self.pipeline/'tasks'/'retired')
        displaced.mkdir()
        with self.assertRaisesRegex(ValueError, 'no live rm directory FD'):
            coop.verify_tree_anchor(rows, coop.scope_snapshot())

    def test_same_named_recreated_pipeline_cannot_bind_original_rm(self):
        rows = self.rows()
        self.pipeline.rename(self.pipeline.with_name('retired-pipeline'))
        self.pipeline.mkdir()
        for name in coop.CHILDREN:
            (self.pipeline/name/'batch').mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, 'no live rm directory FD'):
            coop.verify_tree_anchor(rows, coop.scope_snapshot())

    def test_deleted_fd_and_wrong_cwd_inode_cannot_anchor_tree(self):
        rows = self.rows()
        process = next(row for row in rows if row['node'] == coop.ANCHOR_NODE)['rms']['old'][0]
        snapshot = coop.scope_snapshot()
        process['dir_fds'][0]['raw_path'] += ' (deleted)'
        with self.assertRaisesRegex(ValueError, 'no live rm directory FD'):
            coop.verify_tree_anchor(rows, snapshot)
        process['cwd_ino'] += 1
        with self.assertRaisesRegex(ValueError, 'cwd inode'):
            coop.verify_tree_anchor(rows, snapshot)

    def test_closed_first_fd_does_not_reject_a_later_live_descendant(self):
        rows = self.rows()
        process = next(row for row in rows if row['node'] == coop.ANCHOR_NODE)['rms']['old'][0]
        process['dir_fds'].insert(0, dict(raw_path=str(self.pipeline/'tasks'/'gone'), ino=12345))
        proof = coop.verify_tree_anchor(rows, coop.scope_snapshot())
        self.assertEqual(proof['directory_fd']['ino'], (self.pipeline/'tasks'/'batch').stat().st_ino)

    def test_root_fd_mismatch_is_not_hidden_by_an_earlier_good_descendant(self):
        rows = self.rows()
        process = next(row for row in rows if row['node'] == coop.ANCHOR_NODE)['rms']['old'][0]
        process['dir_fds'].append(dict(raw_path=str(self.pipeline), ino=self.pipeline.stat().st_ino+1))
        with self.assertRaisesRegex(ValueError, 'pipeline-root FD inode differs'):
            coop.verify_tree_anchor(rows, coop.scope_snapshot())

    def test_source_frozen_helper_controller_or_original_stop_change_refuses(self):
        self.prepare()
        for path in (self.base/'parent_scheduler.py', self.old/'parent_tuning.py', self.original/'STOP'):
            content = path.read_bytes()
            path.write_bytes(content+b'changed')
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.apply()
            path.write_bytes(content)
        controller = self.old/'controller.json'
        content = controller.read_bytes()
        self.write(controller, dict(self.actor, start='reused-controller'))
        with self.assertRaisesRegex(ValueError, 'controller binding'):
            self.apply()
        controller.write_bytes(content)
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_stop_errors_phase_pending_and_stale_status_refuse(self):
        self.prepare()
        for key, value in (('state', 'retrying_failed_jobs'), ('errors', ['fatal']), ('updated', 0)):
            bad = dict(self.status, **{key:value})
            self.write(self.old/'status.json', bad)
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'fresh, unique'):
                self.apply()
        self.write(self.old/'status.json', dict(self.status,
            parent_scheduler=dict(self.status['parent_scheduler'], pending=1)))
        with self.assertRaisesRegex(ValueError, 'fresh, unique'):
            self.apply()
        self.write(self.old/'status.json', self.status)
        self.write(self.old/'STOP', dict(private_stop=True))
        with self.assertRaisesRegex(ValueError, 'stopped or failed'):
            self.apply()
        (self.old/'STOP').unlink()  # private fixture only
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_freeze_source_race_and_bad_frozen_manifest_refuse_before_start(self):
        self.prepare()
        self.freeze_hook = lambda run:(run/'parent_scheduler.py').write_text('mutated frozen helper')
        with self.assertRaisesRegex(ValueError, 'frozen parent helper changed'):
            self.apply()
        work, _ = self.plan()
        self.assertTrue((work/'failure.json').exists())
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_source_change_during_freeze_refuses_before_start(self):
        self.prepare()
        self.freeze_hook = lambda run:(self.base/'parent_tuning.py').write_text('different source after copy')
        with self.assertRaisesRegex(ValueError, 'prepared source changed'):
            self.apply()
        self.assertEqual(self.starts, [])
        self.assertTrue((self.plan()[0]/'failure.json').exists())
        self.untouched()

    def test_anchor_exits_during_freeze_refuses_without_any_old_signals(self):
        self.prepare()
        def freeze(run):
            self.row_hook = lambda rows:next(r for r in rows if r['node'] == coop.ANCHOR_NODE)['rms'].update(old=[])
        self.freeze_hook = freeze
        with self.assertRaisesRegex(ValueError, 'anchor helper/rm changed or exited'):
            self.apply()
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_changed_cwd_inode_after_freeze_is_checked_before_start(self):
        self.prepare()
        def freeze(run):
            def changed(rows):
                process = next(r for r in rows if r['node'] == coop.ANCHOR_NODE)['rms']['old'][0]
                process['cwd_ino'] += 1
            self.row_hook = changed
        self.freeze_hook = freeze
        with self.assertRaisesRegex(ValueError, 'cwd inode'):
            self.apply()
        self.assertEqual(self.starts, [])
        self.untouched()

    def test_bad_readiness_proofs_or_timeout_records_failure_without_rerun(self):
        self.prepare()
        self.ready_mock.side_effect = TimeoutError('unconfirmed private readiness')
        with self.assertRaises(TimeoutError):
            self.apply()
        self.assertEqual(len(self.starts), 1)
        self.assertTrue((self.plan()[0]/'failure.json').exists())
        with self.assertRaises(FileExistsError):
            self.apply()
        self.assertEqual(len(self.starts), 1)
        self.untouched()

    def test_wrong_readiness_boot_cannot_claim_success(self):
        self.prepare()
        def wrong(run, nodes, timeout=90):
            result = self.ready(run, nodes, timeout)
            result['proofs'][0]['boot'] = 'rebooted-after-launch'
            return result
        self.ready_mock.side_effect = wrong
        with self.assertRaisesRegex(RuntimeError, 'worker boot'):
            self.apply()
        work, _ = self.plan()
        self.assertFalse((work/'ready.json').exists())
        self.assertTrue((work/'failure.json').exists())
        self.assertEqual(len(self.starts), 1)
        self.untouched()

    def test_inconsistent_scheduler_counters_cannot_claim_success(self):
        self.prepare()
        def wrong(run, nodes, timeout=90):
            result = self.ready(run, nodes, timeout)
            result['status']['parent_scheduler']['releases'] = 1
            return result
        self.ready_mock.side_effect = wrong
        with self.assertRaisesRegex(RuntimeError, 'scheduler/tuner readiness'):
            self.apply()
        self.assertFalse((self.plan()[0]/'ready.json').exists())
        self.assertEqual(len(self.starts), 1)
        self.untouched()

    def test_invalid_timeout_refuses_before_any_claim_or_start(self):
        self.prepare()
        for value in (0, -1, 601, True, '90', float('nan'), float('inf')):
            with self.subTest(timeout=value), self.assertRaisesRegex(ValueError, 'timeout'):
                coop.apply(timeout=value)
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])
        self.assertFalse((self.old/'cooperative-claim.json').exists())
        self.untouched()

    def test_natural_anchor_exit_after_start_does_not_rollback_or_probe_unknown_pilot(self):
        self.prepare()
        def ready(run, nodes, timeout=90):
            result = self.ready(run, nodes, timeout)
            self.write(self.old/'status.json', dict(self.status, state='supplement_complete'))
            self.row_hook = lambda rows:(_ for _ in ()).throw(AssertionError('post-start full40 probe forbidden'))
            return result
        self.ready_mock.side_effect = ready
        result = self.apply()
        self.assertTrue(result['ready'])
        self.assertEqual(len(self.inspections), 4)
        self.untouched()

    def test_terminal_complete_is_not_claimed_as_measured_speedup(self):
        self.prepare()
        def complete(run, nodes, timeout=90):
            return dict(state='terminal_complete', verified_nodes=0, proofs=[],
                status=dict(state='supplement_complete', updated=coop.time.time(), errors=[],
                    parent_scheduler=dict(limit=2, pending=0, active_leases=0, claims=0, releases=0),
                    parent_tuning=dict(limit=2, generation=0, error=None)))
        self.ready_mock.side_effect = complete
        result = self.apply()
        self.assertEqual(result['state'], 'supplement_complete')
        self.assertTrue(result['effective_speedup_not_yet_measured'])
        self.untouched()


if __name__ == '__main__':
    unittest.main()
