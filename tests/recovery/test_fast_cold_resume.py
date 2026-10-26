"""Private temporary fixtures for the P3-only continuation; no production I/O."""
import concurrent.futures
import copy
import json
import os
import pickle
import hashlib
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import deploy_fast_p3_scan as controller
import migration_fast_cold_resume as fast
import migration_linux_open as opening
import migration_overlap_parallel_filter as filtered
import migration_parallel_scan as scan
from migration_io import read_json,write_json
from migration_scan import _stat_fields
from migration_warm_scan_checkpoint import Prefix
from migration_assigned_source_guard import AssignedSourceGuard
import test_migration_overlap_parallel_filter as fixtures


class AnchoredSourceOpenTests(unittest.TestCase):
    def test_native_and_fallback_reject_symlinks_and_escape_and_return_same_directory(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name);(root/'a/b').mkdir(parents=True)
            (root/'alias').symlink_to('a',target_is_directory=True)
            anchor=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
            try:
                for fallback in (False,True):
                    with mock.patch.object(opening,'_openat2_unavailable',fallback):
                        fd=opening.open_directory_beneath(anchor,'a/b')
                        try:self.assertEqual(os.fstat(fd).st_ino,(root/'a/b').stat().st_ino)
                        finally:os.close(fd)
                        with self.assertRaises(OSError):opening.open_directory_beneath(anchor,'alias/b')
                        for rel in ('../outside','/absolute','a//b',None,False,b'a'):
                            with self.assertRaises(ValueError):opening.open_directory_beneath(anchor,rel)
            finally:os.close(anchor)


class DrainGateTests(unittest.TestCase):
    def state(self):
        return dict(stage='p3-r3',recovery_warm=True,next_task_id=7,
                    target_filter=dict(inflight=0,submitted=30,checked=30),
                    task_publication=dict(pending=0,failed=False),priority_inputs=dict(open_groups=0),
                    distributed_feeds=[dict(status='complete') for _ in range(24)])
    def test_all_barriers_required(self):
        value=self.state();self.assertTrue(controller.queue_barrier(value,7))
        for changes in [dict(next_task_id=8),dict(stage='p3-r2'),dict(recovery_warm=False),
                        dict(distributed_feeds=[]),dict(distributed_feeds=[dict(status='waiting')]*24),
                        dict(priority_inputs={'open_groups':1}),dict(task_publication={'pending':1}),
                        dict(target_filter={'inflight':1,'submitted':30,'checked':29})]:
            self.assertFalse(controller.queue_barrier(dict(value,**changes),7))


class PreparedCacheLineageTests(unittest.TestCase):
    def test_third_adoption_follows_frozen_same_pass_chain_and_rejects_cycle_or_digest_change(self):
        with tempfile.TemporaryDirectory(prefix='fast-scan-chain-', dir='/tmp') as name:
            control = Path(name)
            first, second = control/'fast-p3-scan-first', control/'fast-p3-scan-second'
            first.mkdir(); second.mkdir()
            (first/'migration_fast_cold_resume.py').write_text('# frozen fixture\n')
            manifest = {'migration_fast_cold_resume.py': hashlib.sha256(
                (first/'migration_fast_cold_resume.py').read_bytes()).hexdigest()}
            write_json(first/'source-sha256.json', manifest)
            original = dict(pid=111, start_ticks='1', argv=['original'])
            middle = dict(pid=222, start_ticks='2', argv=['/usr/bin/python3', '-u',
                          str(first/'migration_fast_cold_resume.py')])
            scan = dict(source_manifest=str(control/'manifest'), offset=100)
            assigned = dict(assignment_checkpoint=str(control/'assignments'), prefix_floor=5)
            base = dict(control=str(control), missing_manifest=scan['source_manifest'],
                        assignment_checkpoint=assigned['assignment_checkpoint'], next_task_id=5)
            def handover(bundle, old):
                write_json(bundle/'handover.json', dict(base, old_producer=dict(
                    pid=old['pid'], starttime_ticks=old['start_ticks'], argv=old['argv'])))
                write_json(bundle/'cutover.json', dict(same_pass_quiescence_and_flush_verified=True,
                                                     manifest_size=200))
            handover(first, original); handover(second, middle)
            intent = dict(producer=middle, entry_kind='fast-p3-scan', source_bundle=str(first),
                          frozen_active_manifest_sha256=hashlib.sha256(
                              (first/'source-sha256.json').read_bytes()).hexdigest())
            write_json(second/'expected-runtime.json', intent)
            expected = dict(entry_kind='fast-p3-scan', source_bundle=str(second), cache_predecessor=original)
            fast.validate_cache_lineage(control, expected, scan, assigned)
            (first/'migration_fast_cold_resume.py').write_text('# modified\n')
            with self.assertRaisesRegex(ValueError, 'frozen source'):
                fast.validate_cache_lineage(control, expected, scan, assigned)
            (first/'migration_fast_cold_resume.py').write_text('# frozen fixture\n')
            handover(first, middle)
            write_json(first/'expected-runtime.json', intent)
            with self.assertRaisesRegex(ValueError, 'cyclic'):
                fast.validate_cache_lineage(control, expected, scan, assigned)

    def test_second_adoption_requires_the_exact_same_pass_and_assignment_lineage(self):
        with tempfile.TemporaryDirectory(prefix='fast-scan-lineage-',dir='/tmp') as name:
            control=Path(name);prior=control/'fast-p3-scan-prior';prior.mkdir()
            directory=control/'fast-p3-scan-next';directory.mkdir()
            old=dict(pid=111,start_ticks='1',argv=['fixture-old'])
            current=dict(pid=222,start_ticks='2',argv=['fixture-current'])
            expected=dict(entry_kind='fast-p3-scan',producer=current,cache_predecessor=old,source_bundle=str(prior))
            cfg=dict(run_id='fixture-run')
            descriptor=control/'assignment-checkpoint.json';delta=control/'assignment-delta.jsonl';delta.write_text('')
            assignment_cache=control/'assigned.pickle';assignment_cache.write_bytes(b'fixture');assignment_cache.chmod(0o600)
            write_json(descriptor,dict(base_ledger=str(control/'ledger'),prefix_sha256='fixture-prefix',cache=str(assignment_cache)))
            prefix=Prefix();prefix.root_identity=(1,2);cache=control/'prefix.pickle'
            cache.write_bytes(pickle.dumps(prefix.containers(),protocol=5));cache.chmod(0o600)
            scan=dict(status='prepared_prefix_only',control=str(control),run_id='fixture-run',producer=old,
                      source_manifest=str(control/'manifest.jsonl'),offset=100,cache=str(cache),
                      cache_sha256=hashlib.sha256(cache.read_bytes()).hexdigest(),paths=0,directories=0,records=0)
            assignment=dict(status='prepared_assignment_prefix_only',control=str(control),run_id='fixture-run',producer=old,
                            assignment_checkpoint=str(descriptor),prefix_floor=5)
            sp,ap=control/'scan-preparation.json',control/'assignment-preparation.json'
            write_json(sp,scan);write_json(ap,assignment)
            handover=dict(control=str(control),old_producer=dict(pid=111,starttime_ticks='1',argv=old['argv']),
                          missing_manifest=scan['source_manifest'],assignment_checkpoint=str(descriptor),next_task_id=5)
            cutover=dict(same_pass_quiescence_and_flush_verified=True,manifest_size=100)
            write_json(prior/'handover.json',handover);write_json(prior/'cutover.json',cutover)
            with mock.patch.object(fast,'validate_runtime',return_value=(cfg,{},current)), \
                 mock.patch.object(fast,'resource_budget'),mock.patch.object(fast.gc,'freeze'), \
                 mock.patch.object(fast.assignments,'load_checkpoint',return_value=(set(),4,delta,{0,1,2,3,4})) as loader:
                value=fast.Standby(control,directory,expected,sp,ap)
                self.assertEqual(value.floor,5)
                for change in ({'control':'/wrong'},{'missing_manifest':'/wrong'},
                               {'assignment_checkpoint':'/wrong'},{'next_task_id':6},
                               {'old_producer':dict(pid=111,starttime_ticks='9',argv=old['argv'])}):
                    write_json(prior/'handover.json',dict(handover,**change));loader.reset_mock()
                    with self.assertRaises(ValueError):fast.Standby(control,directory,expected,sp,ap)
                    loader.assert_not_called()
                write_json(prior/'handover.json',handover)
                for change in ({'manifest_size':99},{'same_pass_quiescence_and_flush_verified':False}):
                    write_json(prior/'cutover.json',dict(cutover,**change));loader.reset_mock()
                    with self.assertRaises(ValueError):fast.Standby(control,directory,expected,sp,ap)
                    loader.assert_not_called()


class AdoptedWarmScanTests(unittest.TestCase):
    def test_preloaded_adoption_preserves_ids_seed_counters_and_manifest_then_scans_only_remainder(self):
        self.exercise_adoption(False)

    def test_assignment_guard_changes_only_warm_metadata_not_tasks_or_formal_inventory(self):
        self.exercise_adoption(True)

    def exercise_adoption(self, use_guard):
        case=fixtures.ParallelFilterTests('test_identity_assignment_and_stop_guards')
        case.setUp();self.addCleanup(case.doCleanups)
        bodies,warm,partial,ledger,_=case.prepare()
        case.cfg.update(target_probe_endpoints=['http://127.0.0.1:9'],broker_token='fixture-token',
                        distributed_target_threads=4,metadata_client_processes=2)
        write_json(case.control/'config.json',case.cfg)
        prefix=Prefix();prefix.add(dict(rel='',kind='directory',**_stat_fields(case.src.stat())))
        prefix.add(dict(rel='other/orphan',kind='file',**_stat_fields((case.src/'other/orphan').stat())))
        partial.write_text(''.join(json.dumps(e)+'\n' for e in [
            dict(rel='',kind='directory',**_stat_fields(case.src.stat())),dict(prefix.tail[0])]))
        directory=case.control/'fast-p3-scan-fixture';directory.mkdir()
        delta=directory/'delta.jsonl';delta.write_text('')
        result=case.control/'overlap-seed-result.json'
        write_json(result,dict(ok=True,summary={'verified_files':0,'verified_bytes':0},merge={'rows_processed':0}))
        handover=dict(control=str(case.control),stage='p3-r3',
            old_producer=dict(pid=987654321,starttime_ticks='1',argv=['fixture-old']),
            seed_helper=dict(pid=987654322,starttime_ticks='1',argv=['fixture-seed'],result_path=str(result)),
            assigned_tasks_path=str(ledger),assignment_checkpoint='fixture-preloaded-only',
            next_task_id=5,warm_manifest=str(warm),missing_manifest=str(partial))
        write_json(directory/'handover.json',handover)
        state=read_json(case.control/'state.json')
        state.update(target_filter=dict(submitted=20,checked=20,missing=2,peak_inflight=8,inflight=0),
          metadata_batching=dict(submitted_batches=1,submitted_entries=20,full_batches=0,completed_batches=1,batch_seconds=0.,child_cpu_seconds=0.),
          task_batch_policy=dict(max_files=1,max_bytes=1024**3,oversize_singletons=0,new_tasks=0,largest_multi_file_task_bytes=0),
          distributed_target_probe={'counts':{'remote_existing':18},'endpoints':{}})
        write_json(case.control/'state.json',state)
        assigned_prefix = {'other/already','other/assigned'}
        def cached(*args):return assigned_prefix,4,delta,{0,4}
        ctx=types.SimpleNamespace(control=case.control,directory=directory,source=partial,
            offset=partial.stat().st_size,prefix=prefix,completed={},cached_assignments=cached,
            expected=dict(new_source_processes=3,parallel_directory_metadata=True))
        if use_guard:
            ctx.assigned_source_guard = AssignedSourceGuard(assigned_paths=assigned_prefix)
            for rel in assigned_prefix:
                ctx.assigned_source_guard.add(dict(rel=rel, kind='file', **_stat_fields((case.src/rel).stat())))
        class Pool:
            def __init__(self):self.pool=concurrent.futures.ThreadPoolExecutor(max_workers=2)
            def submit(self,entries):
                def probe():
                    return dict(entries=[None if (case.dst/e['rel']).exists() else e for e in entries],
                                probe_delta={'counts':{},'endpoints':{}},seconds=0.,cpu_seconds=0.)
                return self.pool.submit(probe)
        with mock.patch.object(filtered,'validate_mounts'),mock.patch.object(filtered,'process_matches',return_value=False):
            obj=fast.FastColdResume(ctx)
        self.addCleanup(case.close,obj)
        self.assertEqual(obj.next_task_id,5)
        self.assertEqual(obj.seed_process.pid,987654322)
        self.assertEqual(obj.filter_counts['checked'],20)
        self.assertEqual(obj._probe_counts['remote_existing'],18)
        self.assertEqual(obj.combined_manifest,partial)
        self.assertEqual(obj.state['target_probe_counts_scope'],'cumulative-across-producer-handover')
        obj.cfg.update(source=str(case.src),destination=str(case.dst))
        obj.probe_pool=Pool();self.addCleanup(obj.probe_pool.pool.shutdown)
        original=scan.scan_phase
        def tiny(*args,**kwargs):
            kwargs.update(processes=3,threads_per_process=2)
            return original(*args,**kwargs)
        with mock.patch.object(fast,'scan_phase',side_effect=tiny):obj.supplement_missing()
        tasks=[read_json(p) for p in (case.control/'stages/p3-r3/pending').glob('*/*.json')]
        new=[task for task in tasks if task['id']>=5]
        self.assertEqual({e['rel'] for task in new for e in task['entries']},{'other/orphan','other/fast','other/slow'})
        self.assertEqual(len(new),3)
        self.assertGreater(obj.filter_counts['checked'],20)
        rows=[json.loads(line) for line in partial.read_text().splitlines()]
        files=[e['rel'] for e in rows if e.get('kind')=='file']
        expected_files = 4 if use_guard else 6
        self.assertEqual(len(files),expected_files)
        self.assertEqual(len(set(files)),expected_files)
        self.assertTrue(obj.state['warm_missing_scan']['scan_complete'])
        self.assertEqual(obj.state['warm_missing_scan']['files'],expected_files)
        self.assertEqual(obj.state['source_scan_acceleration']['assigned_source_metadata_guard'],use_guard)
        if use_guard:
            self.assertFalse(obj.state['warm_missing_scan']['metadata_inventory_complete'])
            self.assertEqual(obj.state['warm_missing_scan']['process_readers']['skipped_assigned_leaves'],2)
            formal = []
            full = scan.scan_phase(case.src,3,formal.extend,directory/'formal.jsonl',processes=3)
            self.assertEqual(full['files'],6)
            self.assertTrue(assigned_prefix <= {entry['rel'] for entry in formal})
            self.assertNotIn('assignment_metadata_deferred',full)


class AssignedGuardLoaderTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.object(fast.gc, 'freeze')
        patch.start()
        self.addCleanup(patch.stop)

    def test_pending_guard_never_authorizes_readiness_or_touches_a_cache(self):
        standby = object.__new__(fast.Standby)
        standby._guard_pending = True
        standby.control = Path('/fixture')
        standby.expected = {'assigned_guard_cache_max_bytes': 100}
        with mock.patch.object(fast, 'guard_preparation', return_value={
                'status': 'reading_assignment_metadata_only'}), \
                mock.patch.object(fast, 'load_source_guard') as load:
            self.assertFalse(standby.prepare_guard())
            self.assertTrue(standby._guard_pending)
            load.assert_not_called()

    def test_inherited_guard_requires_hash_bound_active_adoption_not_relabelled_owner(self):
        with tempfile.TemporaryDirectory(prefix='inherited-source-guard-', dir='/tmp') as name:
            control = Path(name)
            prior = control / 'fast-p3-scan-active'
            prior.mkdir()
            preparation = control / 'warm-assigned-source-guard-original' / 'preparation.json'
            preparation.parent.mkdir()
            old = dict(pid=111, start_ticks='1', argv=['old'])
            current = dict(pid=222, start_ticks='2', argv=['current'])
            meta = dict(status='prepared_assignment_metadata_only', control=str(control),
                        run_id='fixture', producer=old, assignment_checkpoint='assigned', prefix_floor=5)
            active = dict(producer=old, skip_assigned_source_metadata=True,
                          assigned_source_guard_preparation=str(preparation))
            handover = dict(control=str(control), assignment_checkpoint='assigned', next_task_id=5,
                            old_producer=dict(pid=111, starttime_ticks='1', argv=old['argv']))
            cutover = dict(control=str(control), old_producer=old, next_task_id=5,
                           same_pass_quiescence_and_flush_verified=True)
            state = dict(source_scan_acceleration={'assigned_source_metadata_guard': True}, next_task_id=5)
            expected = dict(skip_assigned_source_metadata=True, producer=current, run_id='fixture',
                entry_kind='fast-p3-scan', source_bundle=str(prior),
                assigned_source_guard_preparation=str(preparation), reuse_assigned_guard_from_active=True,
                inherited_assigned_guard_record_sha256=fast.record_sha256(meta),
                active_assigned_guard_intent_sha256=fast.record_sha256(active))
            def reset():
                write_json(preparation, meta)
                write_json(prior/'expected-runtime.json', active)
                write_json(prior/'handover.json', handover)
                write_json(prior/'cutover.json', cutover)
            reset()
            with mock.patch.object(fast, 'validate_runtime', return_value=({}, state, current)) as runtime:
                self.assertEqual(fast.guard_preparation(control, expected), meta)
                runtime.assert_called_once_with(control, expected)
                # Neither the original record nor its owner is rewritten by validation.
                self.assertEqual(read_json(preparation)['producer'], old)
                for changes in ({'reuse_assigned_guard_from_active': False},
                                {'entry_kind': 'other'}, {'inherited_assigned_guard_record_sha256': 'wrong'},
                                {'active_assigned_guard_intent_sha256': 'wrong'}):
                    with self.subTest(changes=changes), self.assertRaises(ValueError):
                        fast.guard_preparation(control, dict(expected, **changes))
                for target, modified in (
                    (preparation, dict(meta, status='reading_assignment_metadata_only')),
                    (preparation, dict(meta, producer=dict(old, pid=112))),
                    (prior/'expected-runtime.json', dict(active, skip_assigned_source_metadata=False)),
                    (prior/'cutover.json', dict(cutover, same_pass_quiescence_and_flush_verified=False)),
                    (prior/'cutover.json', dict(cutover, next_task_id=6)),
                    (prior/'handover.json', dict(handover, assignment_checkpoint='different'))):
                    reset();write_json(target, modified)
                    with self.subTest(target=target, modified=modified), self.assertRaises(ValueError):
                        fast.guard_preparation(control, expected, require_ready=False)
                reset()
                for modified in (dict(state, next_task_id=6), dict(state, source_scan_acceleration={})):
                    runtime.return_value = ({}, modified, current)
                    with self.assertRaises(ValueError):fast.guard_preparation(control, expected)

    def test_private_cache_owner_assignment_scope_and_primitive_payload_are_all_required(self):
        with tempfile.TemporaryDirectory(prefix='source-guard-load-', dir='/tmp') as name:
            control = Path(name)
            directory = control/'warm-assigned-source-guard-fixture';directory.mkdir()
            descriptor_path = control/'assignment.json'
            descriptor = dict(base_ledger=str(control/'ledger'), prefix_sha256='prefix')
            write_json(descriptor_path, descriptor)
            cache = control/'cache.pickle'
            cache.write_bytes(pickle.dumps({'inode_paths': {10: 'data/a'}, 'excluded': set()}))
            cache.chmod(0o600)
            producer = dict(pid=123, start_ticks='4', argv=['fixture'])
            expected = dict(skip_assigned_source_metadata=True,
                assigned_source_guard_preparation=str(directory/'preparation.json'),
                producer=producer, run_id='fixture')
            meta = dict(status='prepared_assignment_metadata_only', control=str(control), run_id='fixture',
                producer=producer, assignment_checkpoint=str(descriptor_path),
                assignment_descriptor_sha256=fast.assignments.digest_file(descriptor_path),
                prefix_floor=5, tasks=5, base_ledger=descriptor['base_ledger'], prefix_sha256='prefix',
                cache=str(cache), cache_sha256=fast.assignments.digest_file(cache), eligible_inodes=1,
                ambiguous_or_multilink_inodes=0)
            write_json(directory/'preparation.json', meta)
            assigned = {'data/a'}
            guard = fast.load_source_guard(control, expected, descriptor_path, descriptor, assigned, 5)
            self.assertTrue(guard.matches('data/a', 10, True))
            assigned.clear()
            self.assertFalse(guard.matches('data/a', 10, True))
            for change in ({'prefix_floor': 6}, {'run_id': 'other'}, {'control': '/other'},
                           {'producer': dict(producer, pid=124)}, {'prefix_sha256': 'other'}):
                write_json(directory/'preparation.json', dict(meta, **change))
                with self.assertRaises(ValueError):
                    fast.load_source_guard(control, expected, descriptor_path, descriptor, {'data/a'}, 5)
            write_json(directory/'preparation.json', meta)
            cache.chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'owner/mode'):
                fast.load_source_guard(control, expected, descriptor_path, descriptor, {'data/a'}, 5)
            cache.chmod(0o600)
            cache.write_bytes(pickle.dumps(eval))
            write_json(directory/'preparation.json', dict(meta, cache_sha256=fast.assignments.digest_file(cache)))
            with self.assertRaisesRegex(ValueError, 'globals'):
                fast.load_source_guard(control, expected, descriptor_path, descriptor, {'data/a'}, 5)


if __name__=='__main__':unittest.main()
