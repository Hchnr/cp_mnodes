"""Local fixtures only; no cluster contact and no real process signalling."""
import concurrent.futures
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

import migration_frontier_resume as frontier
import migration_missing_discovery as discovery
import test_crash_recovery as fixtures
import deploy_frontier_resume as deployment
import migration_assigned_checkpoint as assigned_checkpoint
from migration_io import read_json, write_json


class ReverseTests(unittest.TestCase):
    def test_reverse_pages_cover_every_complete_record_once_and_drop_crash_tail(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as d:
            path = Path(d) / 'manifest'
            entries = [{'rel': 'datasets/file-' + str(i), 'kind': 'file'} for i in range(200)]
            path.write_bytes(b''.join(json.dumps(e).encode() + b'\n' for e in entries) + b'{"rel":')
            for size in (1, 32, 128, 512, 1024, 8 * 1024 * 1024):
                pages = list(frontier.reverse_chunks(path, size))
                self.assertEqual([e for _, _, records in pages for e in records], entries[::-1])
                self.assertEqual(pages[-1][0], 0)
                for index in range(1, len(pages)):
                    self.assertEqual(pages[index][1], pages[index - 1][0])
                    rest=list(frontier.reverse_chunks(path,size,end_offset=pages[index][1]))
                    self.assertEqual([e for _,_,rs in rest for e in rs],
                        [e for _,_,rs in pages[index:] for e in rs])

    def test_malformed_complete_record_is_not_skipped(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as d:
            path = Path(d) / 'manifest'; path.write_bytes(b'{"rel":\n')
            with self.assertRaises(ValueError): list(frontier.reverse_chunks(path))

    def test_candidates_drop_old_ownership_and_preserve_priority_and_hardlink_rules(self):
        value = frontier.candidate({'kind': 'file', 'rel': 'datasets/a', 'known': {'owned': True}})
        self.assertNotIn('known', value)
        self.assertTrue(value['_refresh_source'])
        for entry in ({'kind': 'directory', 'rel': 'datasets'},
                      {'kind': 'file', 'rel': 'home/hcr/a'},
                      {'kind': 'symlink', 'rel': 'repos/cosmos-framework-pwm/a'},
                      {'kind': 'file', 'rel': 'datasets/link', 'hardlink_to': 'datasets/a'}):
            self.assertIsNone(frontier.candidate(entry))
        with self.assertRaises(ValueError):
            frontier.candidate({'kind': 'file', 'rel': '../escape'})


class AdoptTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CrashProducerTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        old = self.fixture.producer
        fixtures.crash.queue_dirs(old.control, 'p3-r3')
        old.stage = 'p3-r3'
        assigned = {'rel': 'datasets/assigned', 'kind': 'file', 'size': 10}
        old._emit_tasks([assigned], 3, 3)
        old._collect_publications(finish=True)
        old.save(status='scanning', phase=3, round=3, stage='p3-r3', scan_complete=False)
        self.inode = old.live_path.stat().st_ino
        old.db.close(); old._lock.close()
        old.hardlink_pool.shutdown(); old.directory_pool.shutdown()
        self.ledger = old.control / 'ledger.jsonl'
        task = read_json(old.control / 'stages/p3-r3/pending/0/0.json')
        self.ledger.write_text(json.dumps(task) + '\n')
        self.manifest = self.fixture.old / 'prepared.jsonl'
        entries = [assigned, {'rel': 'datasets/existing', 'kind': 'file', 'size': 11},
            {'rel': 'datasets/missing', 'kind': 'file', 'size': 12},
            {'rel': 'datasets/missing', 'kind': 'file', 'size': 12}]
        self.before = b''.join(json.dumps(e).encode() + b'\n' for e in entries)
        self.manifest.write_bytes(self.before)
        warm = old.control / 'empty-warm.jsonl'; warm.write_text('')
        result = old.control / 'overlap-seed-result.json'
        write_json(result, {'ok': True, 'summary': {}, 'merge': {}})
        self.handover = old.control / 'handover.json'
        write_json(self.handover, {'control': str(old.control), 'stage': 'p3-r3',
            'old_producer': {'pid': 987654321, 'starttime_ticks': '1', 'argv': ['old-fixture']},
            'seed_helper': {'pid': 987654322, 'starttime_ticks': '1', 'argv': ['seed-fixture'],
                'result_path': str(result)}, 'assigned_tasks_path': str(self.ledger),
            'next_task_id': 129, 'preserved_enqueued_batches': 1,
            'warm_manifest': str(warm), 'missing_manifest': str(self.manifest),
            'frontier_manifest': str(self.manifest), 'frontier_size': len(self.before)})
        # The actual adoption contract requires CURRENT manifests to remain in
        # control, whereas the read-only frontier belongs to the predecessor.
        current = old.control / 'preserved-current.jsonl'; current.write_text('')
        value = read_json(self.handover); value['missing_manifest'] = str(current)
        write_json(self.handover, value)
        with mock.patch('migration_overlap_parallel_filter.validate_mounts'):
            p = frontier.FrontierResume(old.control, self.handover)
        self.fixture.producer = self.p = p
        self.addCleanup(p._inputs.close)

    def test_adoption_reuses_live_database_ids_progress_and_seed(self):
        self.assertEqual(self.p.live_path.stat().st_ino, self.inode)
        self.assertEqual(self.p.next_task_id, 129)
        self.assertEqual(self.p.enqueued_batches, 1)
        self.assertEqual(self.p.discovered_files, 1)
        self.assertEqual(self.p.seed_process.pid, 987654322)
        with mock.patch.object(frontier.CrashResume, 'start_seed', side_effect=AssertionError('reseed')):
            self.p.start_seed()
        self.assertEqual(self.p.seed_summary, {})

    def test_warm_save_shares_periodic_timers_and_refreshes_filter_snapshot(self):
        self.p.last_scan_save = self.p.last_filter_save = 0
        self.p.filter_counts.update(checked=77, submitted=99)
        self.p.inflight = 22
        before = self.p._save_timing['save']['calls']
        self.p.save()
        self.assertGreater(self.p.last_scan_save, 0)
        self.assertEqual(self.p.last_scan_save, self.p.last_filter_save)
        saved = read_json(self.p.control / 'state.json')
        self.assertEqual(saved['target_filter']['checked'], 77)
        self.assertEqual(saved['target_filter']['inflight'], 22)
        self.assertEqual(saved['producer_save_timing']['save']['calls'], before)
        self.assertEqual(self.p._save_timing['save']['calls'], before + 1)
        # Persisted timing intentionally describes previously completed saves,
        # not mutable references that acquire an invented completion time.
        self.assertEqual(self.p.state['producer_save_timing']['save']['calls'], before)

    def test_periodic_checkpoint_and_task_emit_use_one_save_but_force_is_preserved(self):
        self.p.last_scan_save = self.p.last_filter_save = 0
        with mock.patch.object(self.p, 'save', wraps=self.p.save) as saving:
            self.p._checkpoint()
            self.p._emit_tasks([{'rel':'datasets/shared-save', 'kind':'file', 'size':1}], 3, 3)
            self.p._checkpoint()
            self.assertEqual(saving.call_count, 1)
            self.p._checkpoint(force=True)
            self.assertEqual(saving.call_count, 2)
            self.p.save()
            self.assertEqual(saving.call_count, 3)
        self.p._collect_publications(wait=True)
        self.assertIn('datasets/shared-save', self.p.assigned)
        self.assertEqual(self.p.next_task_id, 130)

    def test_failed_save_never_advances_periodic_timers_or_hides_error(self):
        timers = self.p.last_scan_save, self.p.last_filter_save
        before = dict(self.p._save_timing['save'])
        with mock.patch.object(frontier.CrashResume, 'save', side_effect=OSError('save fixture')):
            with self.assertRaisesRegex(OSError, 'save fixture'):
                self.p.save()
        self.assertEqual((self.p.last_scan_save, self.p.last_filter_save), timers)
        self.assertEqual(self.p._save_timing['save']['failures'], before['failures'] + 1)
        self.p.save()
        saved = read_json(self.p.control / 'state.json')
        self.assertEqual(saved['producer_save_timing']['save']['failures'], before['failures'] + 1)

    def test_formal_save_does_not_change_warm_periodic_timers(self):
        self.p.stage = 'p1-r1'
        self.p.state['recovery_warm'] = False
        self.p.last_scan_save, self.p.last_filter_save = 11, 22
        try:
            self.p.save()
            self.assertEqual((self.p.last_scan_save, self.p.last_filter_save), (11, 22))
        finally:
            self.p.stage = 'p3-r3'
            self.p.state['recovery_warm'] = True

    def test_progress_save_failure_is_measured_and_propagated(self):
        timers = self.p.last_scan_save, self.p.last_filter_save
        failures = {key:self.p._save_timing[key]['failures'] for key in ('save', 'progress')}
        with mock.patch.object(frontier.CrashResume, 'save_progress', side_effect=OSError('progress fixture')):
            with self.assertRaisesRegex(OSError, 'progress fixture'):
                self.p.save()
        self.assertEqual((self.p.last_scan_save, self.p.last_filter_save), timers)
        for key in failures:
            self.assertEqual(self.p._save_timing[key]['failures'], failures[key] + 1)

    def test_assignment_snapshot_adopts_same_database_without_full_ledger_replay(self):
        self.p._inputs.close();self.p._collect_publications(finish=True)
        self.p.db.close();self.p._lock.close()
        self.p.hardlink_pool.shutdown();self.p.directory_pool.shutdown()
        bundle=self.p.control/('checkpoint-'+uuid.uuid4().hex);bundle.mkdir()
        ledger=deployment.SnapshotLedger(bundle/'assigned-tasks.jsonl')
        ledger.add({'id':0,'phase':3,'round':3,'op':'sync',
            'entries':[{'rel':'datasets/assigned','kind':'file','size':10}]})
        descriptor,meta=ledger.prepare_cache(self.p.control,bundle,self.p.cfg)
        cache=Path(meta['cache'])
        self.addCleanup(lambda:cache.parent.rmdir());self.addCleanup(lambda:cache.unlink(missing_ok=True))
        ledger.add({'id':128,'phase':3,'round':3,'op':'sync',
            'entries':[{'rel':'datasets/delta','kind':'file','size':20}]});ledger.close()
        value=read_json(self.handover)
        value.update(assigned_tasks_path=str(ledger.path),assignment_checkpoint=str(descriptor),
            assignment_prefix_sha256=meta['prefix_sha256'],assignment_delta_path=str(bundle/'assignment-delta.jsonl'),
            assignment_delta_sha256=ledger.delta_hash.hexdigest(),preserved_enqueued_batches=2)
        handover=bundle/'handover.json';write_json(handover,value)
        with mock.patch('migration_overlap_parallel_filter.validate_mounts'):
            p=frontier.FrontierResume(self.p.control,handover)
        self.fixture.producer=p;self.addCleanup(p._inputs.close)
        self.assertEqual(p.assigned,{'datasets/assigned','datasets/delta'})
        self.assertEqual(p.adopted_task_ids,{0,128})
        self.assertEqual(p.next_task_id,129)
        self.assertEqual(p.live_path.stat().st_ino,self.inode)

    def test_reverse_frontier_existing_filter_dedupe_and_no_manifest_rewrite(self):
        self.fixture.fake_pool(lambda e: None if e['rel'] == 'datasets/existing'
            else {k: v for k, v in e.items() if k != '_refresh_source'})
        self.p.feed_frontier()
        task = read_json(self.p.control / 'stages/p3-r3/pending/1/129.json')
        self.assertEqual([e['rel'] for e in task['entries']], ['datasets/missing'])
        self.assertEqual(self.p.next_task_id, 130)
        self.assertEqual(self.p.enqueued_batches, 2)
        self.assertEqual(self.p.discovered_files, 2)
        self.assertTrue(read_json(self.p.control / 'state.json')['frontier_feed']['complete'])
        self.assertEqual(self.manifest.read_bytes(), self.before)

    def test_discovery_inputs_use_one_owner_and_ack_only_after_publication(self):
        host = self.p.cfg['copy_hosts'][0]
        root = self.p.control / 'frontier-input' / host
        (root / 'ready').mkdir(parents=True); (root / 'done').mkdir()
        path = root / 'ready/input.json'
        write_json(path, {'control': str(self.p.control), 'run_id': self.p.cfg['run_id'],
            'host': host, 'entries': [{'rel': 'datasets/assigned', 'kind': 'file'},
                {'rel': 'datasets/new', 'kind': 'file'}, {'rel': 'datasets/new', 'kind': 'file'}]})
        write_json(root / 'ready/second.json', {'control':str(self.p.control),
            'run_id':self.p.cfg['run_id'],'host':host,
            'entries':[{'rel':'datasets/new-two','kind':'file'}]})
        self.fixture.fake_pool(lambda e: {k: v for k, v in e.items() if k != '_refresh_source'})
        with mock.patch.object(self.p,'_drain_probes',wraps=self.p._drain_probes) as drain:
            self.wait_inputs(2)
            self.assertEqual(drain.call_count,0)
        self.assertTrue(path.exists()); self.assertFalse((root / 'done/input.json').exists())
        self.assertIn(path, self.p._inputs.receipts.known)
        task = read_json(self.p.control / 'stages/p3-r3/pending/1/129.json')
        self.assertEqual({e['rel'] for e in task['entries']}, {'datasets/new','datasets/new-two'})
        self.assertEqual(self.p.next_task_id, 130)

    def wait_inputs(self, files):
        deadline = time.monotonic() + 5
        while self.p._inputs.files < files:
            self.p.pump_inputs(force=True)
            self.p._harvest_probes()
            if time.monotonic() >= deadline:
                self.fail('input pipeline did not finish')
            time.sleep(.005)

    def input_file(self, name, entries):
        host = self.p.cfg['copy_hosts'][0]
        root = self.p.control / 'frontier-input' / host
        (root / 'ready').mkdir(parents=True, exist_ok=True)
        (root / 'done').mkdir(exist_ok=True)
        path = root / 'ready' / name
        write_json(path, {'control':str(self.p.control),'run_id':self.p.cfg['run_id'],
                         'host':host,'entries':entries})
        return path, root / 'done' / name

    def test_byte_cap_preserves_order_and_large_files_are_singletons(self):
        sizes = [500*1024**2, 600*1024**2, 1024**3, 3*1024**3, 10, 20]
        entries=[{'rel':'datasets/file-'+str(i),'kind':'file','size':size}
                 for i,size in enumerate(sizes)]
        self.p.selected.extend(entries)
        self.p._flush_selected(); self.p._collect_publications(finish=True)
        tasks=[read_json(path) for path in self.p.control.glob('stages/p3-r3/pending/*/*.json')
               if int(path.stem)>=129]
        tasks.sort(key=lambda t:t['id'])
        self.assertEqual([e for t in tasks for e in t['entries']],entries)
        for task in tasks:
            total=sum(e.get('size',0) for e in task['entries'])
            self.assertTrue(total<=1024**3 or len(task['entries'])==1)
        self.assertEqual([len(t['entries']) for t in tasks],[1,1,1,1,2])

    def tune_copy(self, files, **values):
        write_json(self.p.control/'frontier-input-tuning.json',dict(
            control=str(self.p.control),run_id=self.p.cfg['run_id'],
            copy_batch_files=files,**values))
        self.p._inputs.tuning_at=0
        self.p._inputs._tune()

    def test_hot_copy_batch_tuning_changes_only_new_tasks_and_stays_idempotent(self):
        old=[{'rel':'datasets/old-'+str(i),'kind':'file','size':1} for i in range(64)]
        self.p.selected.extend(old);self.p._flush_selected();self.p._collect_publications(wait=True)
        path=self.p.control/'stages/p3-r3/pending/1/129.json';before=path.read_bytes()
        self.tune_copy(128)
        self.assertEqual(self.p.scan_batch_files,128)
        self.assertEqual(self.p._task_batch_policy['effective_from_task_id'],130)
        new=[{'rel':'datasets/new-'+str(i),'kind':'file','size':1} for i in range(320)]
        self.p.selected.extend(new);self.p._flush_selected();self.p._collect_publications(finish=True)
        tasks=[read_json(p) for p in self.p.control.glob('stages/p3-r3/pending/*/*.json')
               if int(p.stem)>=130];tasks.sort(key=lambda t:t['id'])
        self.assertEqual([len(t['entries']) for t in tasks],[128,128,64])
        self.assertEqual([e for t in tasks for e in t['entries']],new)
        self.assertEqual(path.read_bytes(),before)
        revision=self.p._task_batch_policy['revision'];self.tune_copy(128)
        self.assertEqual(self.p._task_batch_policy['revision'],revision)

    def test_copy_batch_tuning_rejects_bad_values_without_partial_limit_changes(self):
        self.tune_copy(128)
        previous=dict(self.p._inputs.limits)
        for bad in (True,0,65,1024,'256'):
            self.tune_copy(bad,max_files=4096)
            self.assertEqual(self.p.scan_batch_files,128)
            self.assertEqual(self.p._inputs.limits,previous)
            self.assertIn('frontier_input_tuning_error',self.p.state)
        self.p.stage='p1-r1';self.tune_copy(256)
        self.assertEqual(self.p.scan_batch_files,128)
        self.assertIn('warm recovery',self.p.state['frontier_input_tuning_error'])
        self.p.stage='p3-r3'

    def test_downshift_splits_already_selected_entries_and_retains_byte_cap(self):
        self.tune_copy(256)
        entries=[{'rel':'datasets/selected-'+str(i),'kind':'file','size':20*1024**2}
                 for i in range(170)]
        self.p.selected.extend(entries);self.tune_copy(64)
        self.p._flush_selected();self.p._collect_publications(finish=True)
        tasks=[read_json(p) for p in self.p.control.glob('stages/p3-r3/pending/*/*.json')
               if int(p.stem)>=129];tasks.sort(key=lambda t:t['id'])
        self.assertEqual([e for t in tasks for e in t['entries']],entries)
        self.assertTrue(all(len(t['entries'])<=64 and sum(e['size'] for e in t['entries'])<=1024**3
                            for t in tasks))

    def test_formal_scan_batch_configuration_is_restored_after_warm_supply(self):
        original=self.p.scan_batch_files;cfg=dict(self.p.cfg)
        self.tune_copy(256)
        self.fixture.fake_pool()
        with mock.patch.object(self.p,'feed_frontier'), \
                mock.patch.object(frontier,'scan_phase',return_value={}) as scan:
            self.p.supplement_missing()
        self.assertEqual(scan.call_args.kwargs['batch_files'],256)
        self.assertEqual(self.p.scan_batch_files,original)
        self.assertEqual(self.p.cfg,cfg)

    def test_duplicate_paths_across_prefetched_groups_have_one_task_owner(self):
        self.p._inputs.limits={'max_files':1,'files_per_host':1}
        entry={'rel':'datasets/shared','kind':'file','size':10}
        self.input_file('a.json',[entry]);self.input_file('b.json',[entry])
        main=threading.get_ident()
        self.fixture.fake_pool(lambda e: {k:v for k,v in e.items() if k!='_refresh_source'})
        original=self.p._emit_tasks
        def emit(*args,**kwargs):
            self.assertEqual(threading.get_ident(),main)
            return original(*args,**kwargs)
        with mock.patch.object(self.p,'_emit_tasks',side_effect=emit):self.wait_inputs(2)
        tasks=[read_json(p) for p in self.p.control.glob('stages/p3-r3/pending/*/*.json')
               if int(p.stem)>=129]
        self.assertEqual(sum(e['rel']=='datasets/shared' for t in tasks for e in t['entries']),1)

    def test_no_ack_until_probe_and_publication_complete(self):
        path,done=self.input_file('gated.json',[{'rel':'datasets/gated','kind':'file','size':10}])
        gate=concurrent.futures.Future();pool=mock.Mock();pool.submit.return_value=gate
        self.p.probe_pool=pool
        deadline=time.monotonic()+3
        while not pool.submit.called:
            self.p.pump_inputs(force=True);time.sleep(.005)
            self.assertLess(time.monotonic(),deadline)
        self.assertTrue(path.exists());self.assertFalse(done.exists())
        entry={'rel':'datasets/gated','kind':'file','size':10}
        gate.set_result({'entries':[entry],'probe_delta':{'counts':{},'endpoints':{}}})
        with mock.patch.object(self.p,'_collect_publications'):
            self.p._harvest_probes()
            self.assertTrue(path.exists());self.assertFalse(done.exists())
        self.wait_inputs(1)
        self.assertFalse(done.exists())
        self.assertIn(path, self.p._inputs.receipts.known)

    def test_deferred_publication_collision_never_acknowledges_the_hint(self):
        from migration_task_publisher import PublicationError
        path,_=self.input_file('collision.json',[{'rel':'datasets/collision','kind':'file','size':10}])
        target=self.p.control/'stages/p3-r3/pending/1/129.json'
        write_json(target,{'old':'must not be replaced'})
        before=target.read_bytes()
        self.assertFalse(self.p._task_publisher.check_on_submit)
        self.fixture.fake_pool(lambda e:{k:v for k,v in e.items() if k!='_refresh_source'})
        try:
            with self.assertRaises(PublicationError):self.wait_inputs(1)
            self.assertTrue(path.exists())
            self.assertNotIn(path,self.p._inputs.receipts.known)
            self.assertEqual(list(self.p._inputs.receipts.groups.glob('*.json')),[])
            self.assertEqual(target.read_bytes(),before)
        finally:
            self.p._collect_publications(finish=True,raise_error=False)
            self.p._task_publisher=None

    def test_slow_ack_does_not_block_next_input_group(self):
        self.p._inputs.limits={'max_files':1,'files_per_host':1}
        a,done=self.input_file('a.json',[{'rel':'datasets/a','kind':'file','size':10}])
        self.input_file('b.json',[{'rel':'datasets/b','kind':'file','size':11}])
        self.fixture.fake_pool(lambda e:{k:v for k,v in e.items() if k!='_refresh_source'})
        ack_gate=threading.Event();started=threading.Event()
        real_ack=self.p._inputs._ack
        def ack(rows, barrier=None):
            if rows[0][0]==a:
                started.set()
                if not ack_gate.wait(3):raise RuntimeError('ack test timeout')
            return real_ack(rows, barrier)
        with mock.patch.object(self.p._inputs,'_ack',side_effect=ack):
            try:
                deadline=time.monotonic()+2
                while 'datasets/b' not in self.p.assigned or not started.is_set():
                    self.p.pump_inputs(force=True);self.p._harvest_probes();time.sleep(.005)
                    self.assertLess(time.monotonic(),deadline)
                self.assertFalse(done.exists())
            finally:ack_gate.set()
            self.wait_inputs(2)

    def test_candidate_directory_enumeration_is_parallel_and_bounded(self):
        hosts=['fixture-'+str(i) for i in range(4)]
        self.p.cfg['copy_hosts']=hosts
        for h in hosts:
            root=self.p.control/'frontier-input'/h
            (root/'ready').mkdir(parents=True);(root/'done').mkdir()
            write_json(root/'ready/a.json',{'control':str(self.p.control),
                'run_id':self.p.cfg['run_id'],'host':h,
                'entries':[{'rel':'datasets/'+h,'kind':'file'}]})
        gate=threading.Barrier(4,timeout=2)
        original=self.p._inputs._scan_ready
        def scan(*args):
            gate.wait()
            return original(*args)
        with mock.patch.object(self.p._inputs,'_scan_ready',side_effect=scan):
            rows=self.p._inputs._load(hosts,{'max_files':3,'files_per_host':1},frozenset())
        self.assertEqual(len(rows),3)

    def test_rename_lane_cannot_starve_candidate_read_executor(self):
        self.p._inputs.close()
        self.p._inputs = inputs = frontier.InputPipeline(self.p, frontier.candidate, group_receipts=False)
        self.addCleanup(inputs.close)
        host = self.p.cfg['copy_hosts'][0]
        root = self.p.control / 'frontier-input' / host
        rows = [(root / 'ready' / (str(i) + '.json'),
                 root / 'done' / (str(i) + '.json'), [], 0) for i in range(32)]
        gate, started = threading.Event(), threading.Event()
        active = peak = 0
        lock = threading.Lock()
        def rename(row):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            started.set()
            if not gate.wait(3):
                raise RuntimeError('rename gate timed out')
            with lock:
                active -= 1
        with mock.patch.object(inputs, '_ack_one', side_effect=rename):
            futures = [inputs.acker.submit(inputs._ack, rows[:16]),
                       inputs.acker.submit(inputs._ack, rows[16:])]
            try:
                self.assertTrue(started.wait(1))
                self.assertEqual(inputs.io.submit(lambda: 'read-is-free').result(timeout=1),
                                 'read-is-free')
            finally:
                gate.set()
            for future in futures:
                future.result(timeout=2)
        self.assertEqual(peak, 1)
        self.assertEqual(inputs.ack_stats[host]['files'], 32)
        self.assertEqual(inputs.ack_stats[host]['groups'], 2)

    def test_ack_lanes_are_parallel_across_hosts_but_serial_within_host(self):
        self.p._inputs.close()
        hosts = self.p.cfg['copy_hosts'] = ['fixture-master', 'fixture-worker']
        self.p._inputs = inputs = frontier.InputPipeline(self.p, frontier.candidate, group_receipts=False)
        self.addCleanup(inputs.close)
        barrier = threading.Barrier(2, timeout=2)
        seen = []
        def rename(row):
            seen.append(row[0].parent.parent.name)
            barrier.wait()
        rows = []
        for host in hosts[:2]:
            root = self.p.control / 'frontier-input' / host
            rows.append((root/'ready/a.json', root/'done/a.json', [], 0))
        with mock.patch.object(inputs, '_ack_one', side_effect=rename):
            inputs._ack(rows)
        self.assertCountEqual(seen, hosts[:2])

    def test_ack_rejects_unknown_host_before_any_rename(self):
        inputs = self.p._inputs
        root = self.p.control / 'frontier-input' / 'unapproved-host'
        with mock.patch.object(inputs, '_ack_one') as rename:
            with self.assertRaises(ValueError):
                inputs._ack([(root/'ready/a.json', root/'done/a.json', [], 0)], 128)
            rename.assert_not_called()

    def test_cursor_keeps_overfetch_and_does_not_reopen_each_batch(self):
        inputs = self.p._inputs
        hosts = self.p.cfg['copy_hosts'] = ['cursor-a', 'cursor-b', 'cursor-c']
        expected = set()
        for host in hosts:
            root = self.p.control/'frontier-input'/host
            (root/'ready').mkdir(parents=True); (root/'done').mkdir()
            for i in range(4):
                path = root/'ready'/(str(i)+'.json'); expected.add(path)
                write_json(path, {'control':str(self.p.control),'run_id':self.p.cfg['run_id'],
                    'host':host,'entries':[{'rel':'datasets/'+host+str(i),'kind':'file'}]})
        seen = set()
        import migration_frontier_inputs as module
        real_scan = module.os.scandir
        opened = []
        def scan(path):
            if Path(path).name == 'ready':
                opened.append(Path(path))
            return real_scan(path)
        with mock.patch.object(module.os, 'scandir', side_effect=scan):
            for i in range(12):
                rows = inputs._load(hosts, {'max_files':1,'files_per_host':2}, frozenset(seen))
                self.assertEqual(len(rows), 1)
                self.assertNotIn(rows[0][0], seen)
                seen.add(rows[0][0])
        self.assertEqual(seen, expected)
        # A host may be re-opened after reaching EOF while other queues drain,
        # but consecutive batches must not restart every host from the front.
        self.assertLess(len(opened), 12 * len(hosts))

    def test_cursor_rechecks_after_eof_and_propagates_directory_errors(self):
        inputs = self.p._inputs
        host = self.p.cfg['copy_hosts'][0]
        a,_ = self.input_file('first.json',[{'rel':'datasets/first','kind':'file'}])
        limits = {'max_files':1,'files_per_host':1}
        self.assertEqual(inputs._load([host],limits,frozenset())[0][0], a)
        self.assertEqual(inputs._load([host],limits,frozenset({a})), [])
        b,_ = self.input_file('later.json',[{'rel':'datasets/later','kind':'file'}])
        inputs.next_scan[host] = 0
        # Fixture files can share a coarse directory timestamp; force the
        # changed-version branch here, and test periodic same-version scans below.
        inputs.ended_versions.pop(host,None)
        self.assertEqual(inputs._load([host],limits,frozenset({a}))[0][0], b)
        inputs.streams[host].close(); inputs.streams[host] = None
        inputs.ended_versions.pop(host,None); inputs.next_scan[host] = 0
        import migration_frontier_inputs as module
        with mock.patch.object(module.os,'scandir',side_effect=PermissionError('fixture denied')):
            with self.assertRaises(PermissionError):
                inputs._load([host], limits, frozenset({a,b}))

    def test_cursor_deduplicates_repeated_directory_entries(self):
        inputs = self.p._inputs
        host = self.p.cfg['copy_hosts'][0]
        root = self.p.control/'frontier-input'/host/'ready'
        a = mock.Mock(path=str(root/'a.json'),name='a.json')
        b = mock.Mock(path=str(root/'b.json'),name='b.json')
        a.name='a.json'; b.name='b.json'
        class Stream:
            def __init__(self):self.items=iter([a,a,b])
            def __next__(self):return next(self.items)
            def close(self):pass
        inputs.streams[host]=Stream()
        self.assertEqual(inputs._scan_ready(host,2,frozenset()),
                         [(host,root/'a.json'),(host,root/'b.json')])

    def test_retired_directory_is_cached_but_periodic_pass_catches_same_timestamp_addition(self):
        inputs = self.p._inputs
        host = self.p.cfg['copy_hosts'][0]
        a,_ = self.input_file('retired.json',[{'rel':'datasets/retired','kind':'file'}])
        limits = {'max_files':2,'files_per_host':2}
        self.assertEqual(len(inputs._load([host],limits,frozenset())),1)
        import migration_frontier_inputs as module
        real_scan = module.os.scandir
        with mock.patch.object(module.os,'scandir',wraps=real_scan) as scan:
            self.assertEqual(inputs._load([host],limits,frozenset({a})),[])
            self.assertEqual(scan.call_count,0)
        b,_ = self.input_file('new.json',[{'rel':'datasets/new','kind':'file'}])
        # Emulate coarse/cache-stale directory timestamps. The periodic full
        # pass must still discover the new immutable hint.
        info = (a.parent).stat(); inputs.ended_versions[host] = (
            info.st_dev,info.st_ino,info.st_mtime_ns,info.st_ctime_ns)
        inputs.next_scan[host] = 0; inputs.last_open[host] = time.monotonic()-31
        self.assertEqual(inputs._load([host],limits,frozenset({a}))[0][0],b)

    def _readopt_input(self, *, lost=False):
        hint,_ = self.input_file('resume.json',[{'rel':'datasets/resume','kind':'file','size':10}])
        original = hint.read_bytes()
        self.fixture.fake_pool(lambda e:{k:v for k,v in e.items() if k!='_refresh_source'})
        self.wait_inputs(1)
        old = self.p
        old._inputs.close(); old._collect_publications(finish=True)
        old.db.close(); old._lock.close(); old.hardlink_pool.shutdown(); old.directory_pool.shutdown()
        tasks = [read_json(path) for path in old.control.glob('stages/p3-r3/pending/*/*.json')]
        if lost:
            tasks = [task for task in tasks if task['id'] != 129]
            (old.control/'stages/p3-r3/pending/1/129.json').unlink()
        ledger = old.control/'readopt-ledger.jsonl'
        ledger.write_text(''.join(json.dumps(task)+'\n' for task in tasks))
        value = read_json(self.handover)
        value.update(assigned_tasks_path=str(ledger),next_task_id=258,preserved_enqueued_batches=len(tasks))
        handover = old.control/'readopt-handover.json'; write_json(handover,value)
        with mock.patch('migration_overlap_parallel_filter.validate_mounts'):
            self.p = p = frontier.FrontierResume(old.control,handover)
        self.fixture.producer = p; self.addCleanup(p._inputs.close)
        self.assertEqual(hint.read_bytes(), original)
        return p,hint

    def test_real_adoption_skips_logically_acknowledged_hint_without_reprobe(self):
        p,hint = self._readopt_input()
        pool = self.fixture.fake_pool()
        for i in range(6):
            p.pump_inputs(force=True); time.sleep(.01)
        self.assertIn(hint, p._inputs.receipts.known)
        self.assertEqual(p._inputs.files, 1)
        pool.submit.assert_not_called()
        self.assertEqual(p.next_task_id, 258)

    def test_real_adoption_reoffers_hint_when_its_published_task_was_lost(self):
        p,hint = self._readopt_input(lost=True)
        self.assertEqual(p._inputs.receipts.replay_groups, 1)
        self.fixture.fake_pool(lambda e:{k:v for k,v in e.items() if k!='_refresh_source'})
        self.wait_inputs(1)
        task = read_json(p.control/'stages/p3-r3/pending/2/258.json')
        self.assertEqual([e['rel'] for e in task['entries']], ['datasets/resume'])
        self.assertTrue(hint.exists())

    def test_small_hint_files_coalesce_into_full_metadata_batches(self):
        entries = [{'rel':'datasets/coalesce-'+str(i),'kind':'file','size':1}
                   for i in range(130)]
        for i, entry in enumerate(entries):
            self.input_file(str(i)+'.json', [entry])
        sizes = []
        pool = self.fixture.fake_pool(lambda e:{k:v for k,v in e.items()
                                                if k!='_refresh_source'})
        submit = pool.submit.side_effect
        def capture(values):
            sizes.append(len(values))
            return submit(values)
        pool.submit.side_effect = capture
        self.p._inputs.limits = {'max_files':256,'files_per_host':256}
        self.wait_inputs(130)
        self.assertEqual(sizes, [64, 64, 2])
        self.assertEqual(self.p._metadata_batching['submitted_entries'], 130)
        self.assertEqual(self.p._metadata_batching['full_batches'], 2)
        self.assertEqual(self.p._metadata_batching['completed_batches'], 3)
        self.assertEqual(self.p.assigned.issuperset(e['rel'] for e in entries), True)

    def test_historical_probes_wait_for_live_input_completion(self):
        self.input_file('priority.json',[{'rel':'datasets/priority','kind':'file','size':10}])
        gate=concurrent.futures.Future();seen=[]
        pool=mock.Mock()
        def submit(entries):
            seen.extend(e['rel'] for e in entries)
            if any(e['rel']=='datasets/priority' for e in entries):return gate
            self.assertTrue(gate.done(),'old inventory must not displace live candidates')
            future=concurrent.futures.Future();future.set_result({'entries':[None]*len(entries),
                'probe_delta':{'counts':{},'endpoints':{}}});return future
        pool.submit.side_effect=submit;self.p.probe_pool=pool
        original=self.p.pump_inputs;calls=[0]
        def pump(*args,**kwargs):
            calls[0]+=1
            if pool.submit.called and not gate.done():
                gate.set_result({'entries':[{'rel':'datasets/priority','kind':'file','size':10}],
                    'probe_delta':{'counts':{},'endpoints':{}}})
            return original(*args,**kwargs)
        with mock.patch.object(self.p,'pump_inputs',side_effect=pump):self.p.feed_frontier()
        self.assertEqual(seen[0],'datasets/priority')
        self.assertEqual(self.p._inputs.files,1)


class DiscoveryTests(unittest.TestCase):
    def test_name_join_publishes_only_missing_hints_and_never_writes_data(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory); src=root/'source'; dst=root/'destination'; control=root/'control'
            src.mkdir(); dst.mkdir(); (control/'frontier-input/node/ready').mkdir(parents=True)
            (src/'present').write_bytes(b'source'); (dst/'present').write_bytes(b'conflict')
            (src/'missing').write_bytes(b'new'); (src/'subdirectory').mkdir()
            cfg={'source':str(src),'destination':str(dst),'run_id':'fixture'}
            result=discovery.scan_directory(cfg,control,'node',{'rel':'','leaves_only':False})
            self.assertEqual(result['missing'],1)
            self.assertEqual(result['files_seen'],2)
            self.assertEqual(result['directories'],[{'rel':'subdirectory','leaves_only':False}])
            payload=read_json(next((control/'frontier-input/node/ready').glob('*.json')))
            self.assertEqual(payload['entries'],[{'rel':'missing','kind':'file','_refresh_source':True}])
            self.assertEqual((dst/'present').read_bytes(),b'conflict')
            self.assertFalse((dst/'missing').exists())

    def test_missing_target_directory_and_priority_exclusion(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root=Path(directory);src=root/'source';dst=root/'destination';control=root/'control'
            (src/'home/hcr').mkdir(parents=True); dst.mkdir()
            (src/'home/loose').write_bytes(b'new')
            (control/'frontier-input/node/ready').mkdir(parents=True)
            result=discovery.scan_directory({'source':str(src),'destination':str(dst),'run_id':'fixture'},
                control,'node',{'rel':'home','leaves_only':False})
            self.assertEqual(result['missing'],1)
            self.assertEqual(result['directories'],[])


class AssignmentCheckpointTests(unittest.TestCase):
    def test_live_catchup_is_bounded_and_never_signals_or_rolls_back_ids(self):
        ledger=mock.Mock(hashes={0:b'a'})
        increments=iter((300,3))
        def claims(*args,**kwargs):
            self.assertTrue(kwargs['missing_only'])
            for _ in range(next(increments)):
                ledger.hashes[max(ledger.hashes)+1]=b'a'
        with mock.patch.object(deployment.base,'collect_pending') as pending, \
                mock.patch.object(deployment.base,'collect_claims',side_effect=claims), \
                mock.patch.object(deployment.os,'kill') as kill:
            self.assertEqual(deployment.catch_up_live(Path('/fixture'),mock.Mock(),ledger),[300,3])
        self.assertEqual(pending.call_count,2);kill.assert_not_called()
    def test_freeze_waits_for_metadata_writer_not_just_stopped_leader(self):
        expected = {'pid':123}
        with mock.patch.object(deployment.base,'require_identity'), \
                mock.patch.object(deployment,'thread_states',side_effect=[['T','D'],['T','T']]), \
                mock.patch.object(deployment.time,'sleep') as sleep:
            self.assertEqual(deployment.wait_frozen_threads(expected), 2)
            sleep.assert_called_once()
        with mock.patch.object(deployment.base,'require_identity'), \
                mock.patch.object(deployment,'thread_states',return_value=['T','D']), \
                mock.patch.object(deployment.time,'sleep'):
            with self.assertRaisesRegex(RuntimeError,'fully frozen'):
                deployment.wait_frozen_threads(expected, seconds=.001)

    def test_checkpoint_requires_exact_canonical_ids_including_behind_maximum(self):
        import sqlite3
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER)')
            db.executemany('INSERT INTO claims VALUES(?,?)',[('p3-r3',0),('p3-r3',9)])
            ledger = mock.Mock(hashes={0:b'a',4:b'b',9:b'c'}, observed_pending=set())
            with self.assertRaisesRegex(RuntimeError,'canonical'):
                deployment.require_assignment_coverage(ledger,db)
            ledger.observed_pending={4}
            self.assertEqual(deployment.require_assignment_coverage(ledger,db),3)
            db.execute("INSERT INTO claims VALUES('p3-r3',10)")
            with self.assertRaisesRegex(RuntimeError,'canonical'):
                deployment.require_assignment_coverage(ledger,db)
        db.close()

    def test_publication_barrier_uses_hot_hook_without_signalling(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            control=Path(directory);bundle=control/'bundle';bundle.mkdir()
            operation='f'*32
            samples=[{'task_publication':{'tuning_operation':'old','pending':0}},
                {'task_publication':{'tuning_operation':operation,'pending':1}},
                {'task_publication':{'tuning_operation':operation,'pending':0}}]
            with mock.patch.object(deployment.uuid,'uuid4',return_value=mock.Mock(hex=operation)), \
                 mock.patch.object(deployment.base,'require_identity'), \
                 mock.patch.object(deployment.base,'guarded_state',side_effect=samples), \
                 mock.patch.object(deployment.time,'sleep'), \
                 mock.patch.object(deployment.os,'kill',side_effect=AssertionError('no signals')):
                current=deployment.request_publication_barrier(control,{'run_id':'fixture'},
                    {'pid':1},{'task_publication':{'workers':8,'limit':64}},bundle,{})
            self.assertEqual(current['task_publication']['pending'],0)
            tuning=read_json(control/'producer-tuning.json')
            self.assertEqual(tuning['publish_workers'],16)
            self.assertEqual(tuning['operation_id'],operation)

    def test_live_prefix_cache_and_frozen_delta_cover_noncontiguous_ids(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            control=Path(directory);bundle=control/('pipelined-producer-'+uuid.uuid4().hex)
            bundle.mkdir();cfg={'run_id':'assigned-fixture-'+uuid.uuid4().hex}
            ledger=deployment.SnapshotLedger(bundle/'assigned-tasks.jsonl')
            def task(i,rel):
                return {'id':i,'phase':3,'round':3,'op':'sync',
                        'entries':[{'rel':rel,'kind':'file','size':10}]}
            ledger.add(task(0,'datasets/old'))
            descriptor,meta=ledger.prepare_cache(control,bundle,cfg)
            cache=Path(meta['cache'])
            self.addCleanup(lambda:cache.parent.rmdir())
            self.addCleanup(lambda:cache.unlink(missing_ok=True))
            ledger.add(task(132,'datasets/new'));ledger.close()
            handover={'assigned_tasks_path':str(ledger.path),'assignment_checkpoint':str(descriptor),
                'assignment_prefix_sha256':meta['prefix_sha256'],
                'assignment_delta_path':str(bundle/'assignment-delta.jsonl'),
                'assignment_delta_sha256':ledger.delta_hash.hexdigest()}
            assigned,maximum,delta,ids=assigned_checkpoint.load_checkpoint(control,cfg,handover)
            self.assertEqual(assigned,{'datasets/old'})
            self.assertEqual(ids, {0})
            self.assertEqual(maximum,0)
            from migration_overlap_parallel_filter import _task_records
            for row in _task_records(delta):
                assigned.update(e['rel'] for e in row['entries']);maximum=max(maximum,row['id'])
            self.assertEqual(assigned,{'datasets/old','datasets/new'})
            self.assertEqual(maximum,132)
            bad=dict(handover,assignment_delta_sha256='0'*64)
            with self.assertRaisesRegex(ValueError,'delta'):
                assigned_checkpoint.load_checkpoint(control,cfg,bad)
            with self.assertRaisesRegex(ValueError,'identity'):
                assigned_checkpoint.load_checkpoint(control,{'run_id':'wrong'},handover)

    def test_checkpoint_cannot_execute_pickle_globals(self):
        import io,pickle
        class Unsafe:
            def __reduce__(self):return (str,('not allowed',))
        with self.assertRaisesRegex(ValueError,'globals'):
            assigned_checkpoint.PrimitiveUnpickler(io.BytesIO(pickle.dumps(Unsafe()))).load()


if __name__ == '__main__':
    unittest.main()
