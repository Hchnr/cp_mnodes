"""Local only: adopt one seed, preserve assignments, parallel target probes."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

import migration_overlap_parallel_filter as filtered
import migration_pipeline as pipeline
from migration_io import read_json, write_json
from migration_run import now, process_entry, queue_dirs
from migration_scan import _stat_fields
from test_migration_overlap_resume import OverlapTests, WORK


class ParallelFilterTests(OverlapTests):
    # The inherited overlap/merge/STOP fixtures continue to exercise the base.
    def prepare(self):
        bodies = {'home/hcr/a': b'priority1', 'repos/cosmos-framework-pwm/r': b'priority2',
                  'other/already': b'already copied', 'other/assigned': b'assigned payload',
                  'other/orphan': b'orphan payload', 'other/fast': b'fast payload',
                  'other/slow': b'slow payload', 'other/present': b'existing payload'}
        for rel, body in bodies.items():
            path = self.src / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        def entry(rel):
            return {'rel': rel, 'kind': 'file', **_stat_fields((self.src / rel).stat())}
        old = self.make()
        old.cfg.update(source=str(self.src), destination=str(self.dst))
        old.state.update(stage='p3-r3', phase=3, round=3, recovery_warm=True)
        old.save()
        queue_dirs(self.control, 'p3-r3')
        (self.control / 'manifests').mkdir(exist_ok=True)
        task0 = {'id': 0, 'phase': 3, 'round': 3, 'op': 'sync', 'entries': [entry('other/already')]}
        task4 = {'id': 4, 'phase': 3, 'round': 3, 'op': 'sync', 'entries': [entry('other/assigned')]}
        write_json(self.control / 'stages/p3-r3/done/0/0.json', {'id': 0, 'finished': now(),
                   'results': [process_entry(old.cfg, task0, task0['entries'][0])]})
        write_json(self.control / 'stages/p3-r3/pending/4/4.json', task4)
        process_entry(old.cfg, task0, entry('other/present'))
        warm = self.control / 'manifests/recovery-warm.jsonl'
        warm.write_text('\n'.join(json.dumps(e) for e in task0['entries'] + task4['entries']) + '\n')
        partial = self.control / 'manifests/recovery-missing.jsonl'
        partial.write_text(json.dumps(entry('other/orphan')) + '\n' + '{"rel":')
        ledger = self.control / 'assigned-tasks.jsonl'
        ledger.write_text(json.dumps(task0) + '\n' + json.dumps({'task': task4}) + '\n')
        original = {p: p.read_bytes() for p in (warm, partial, ledger)}
        old.hardlink_pool.shutdown(wait=True)
        old.directory_pool.shutdown(wait=True)
        old.db.close()
        old._lock.close()
        self.cfg.update(scan_batch_files=1, parallel_scan_threads=4, parallel_target_threads=4,
                        target_filter_inflight=8)
        write_json(self.control / 'config.json', self.cfg)
        return bodies, warm, partial, ledger, original

    def test_live_seed_adoption_out_of_order_probes_full_rounds_and_no_id_reuse(self):
        bodies, warm, partial, ledger, original = self.prepare()
        seed_gate = self.root / 'seed-go'
        result = self.control / 'overlap-seed-result.json'
        candidate = Path('/tmp') / (self.run_id + '-resume-candidate.sqlite')
        code = '''import pathlib,sys,time
sys.path.insert(0,sys.argv[1])
from migration_overlap_resume import seed_child
while not pathlib.Path(sys.argv[5]).exists(): time.sleep(.01)
raise SystemExit(seed_child(*sys.argv[2:5]))
'''
        proc = subprocess.Popen([sys.executable, '-u', '-c', code, str(WORK), str(self.control),
                                 str(candidate), str(result), str(seed_gate)])
        self.addCleanup(lambda: proc.wait(timeout=20))
        helper = {'pid': proc.pid, 'starttime_ticks': '1', 'argv': proc.args, 'result_path': str(result)}
        handover = self.control / 'handover.json'
        write_json(handover, {'control': str(self.control), 'stage': 'p3-r3',
            'old_producer': {'pid': 987654321, 'starttime_ticks': '1', 'argv': ['fixture-old-producer']},
            'seed_helper': helper, 'assigned_tasks_path': str(ledger), 'next_task_id': 5,
            'warm_manifest': str(warm), 'missing_manifest': str(partial)})
        fast_emitted = threading.Event()
        test = self
        main_ident = threading.get_ident()
        class Fixture(filtered.ParallelFilterResume):
            def _probe_missing(inner, entry):
                if entry['rel'] == 'other/slow':
                    if not fast_emitted.wait(10):
                        raise RuntimeError('fast results blocked behind slow probe')
                return super()._probe_missing(entry)
            def _emit_tasks(inner, entries, *args, **kwargs):
                test.assertEqual(threading.get_ident(), main_ident)
                result = super()._emit_tasks(entries, *args, **kwargs)
                if any(e['rel'] == 'other/fast' for e in entries):
                    fast_emitted.set()
                return result
            def drain(inner, stage):
                for path in sorted((inner.control / 'stages' / stage / 'pending').glob('*/*.json')):
                    task = read_json(path)
                    results = [process_entry(inner.cfg, task, e) for e in task['entries']]
                    write_json(inner.control / 'stages' / stage / 'done' / str(task['id'] % 64) / path.name,
                               {'id': task['id'], 'finished': now(), 'results': results})
                    path.unlink()
                if stage == 'p3-r3':
                    test.assertIsNone(proc.poll(), 'original seed must remain live during adopted copy')
                    test.assertEqual((test.dst / 'other/orphan').read_bytes(), bodies['other/orphan'])
                    seed_gate.write_text('warm payload completed\n')
        def matches(identity):
            return identity['pid'] == proc.pid and identity['argv'] == proc.args and proc.poll() is None
        with mock.patch.object(filtered, 'validate_mounts', lambda _cfg: None), \
                mock.patch.object(filtered, 'process_matches', side_effect=matches):
            coordinator = Fixture(self.control, handover)
            self.addCleanup(self.close, coordinator)
            self.assertEqual(coordinator.next_task_id, 5)
            self.assertEqual(coordinator.seed_process.pid, proc.pid)
            coordinator.cfg.update(source=str(self.src), destination=str(self.dst))
            write_json(self.control / 'config.json', coordinator.cfg)
            with mock.patch.object(pipeline, 'validate_mounts', lambda _cfg: None):
                coordinator.run()
        self.assertEqual(coordinator.state['status'], 'complete', coordinator.state)
        self.assertEqual(set(coordinator.pipeline['stages']), {'p%d-r%d' % p for p in pipeline.ORDER})
        self.assertTrue(fast_emitted.is_set())
        self.assertLessEqual(coordinator.state['target_filter']['peak_inflight'], 8)
        self.assertEqual(coordinator.state['target_filter']['missing'], 3)
        issues = read_json(coordinator.adoption / 'manifest-tail-issues.json')['issues']
        self.assertEqual(len(issues), 1)
        warm_tasks = list((self.control / 'stages/p3-r3/done').glob('*/*.json'))
        ids = sorted(int(p.stem) for p in warm_tasks)
        self.assertEqual(ids[:2], [0, 4])
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(i >= 5 for i in ids[2:]))
        for path, before in original.items():
            self.assertEqual(path.read_bytes(), before)
        for rel, body in bodies.items():
            self.assertEqual((self.src / rel).read_bytes(), body)
            self.assertEqual((self.dst / rel).read_bytes(), body)
        self.assertEqual(proc.wait(timeout=20), 0)

    def test_identity_assignment_and_stop_guards(self):
        for identity in ({}, {'pid': 1, 'argv': ['x']}, {'pid': True, 'starttime_ticks': '1', 'argv': ['x']}):
            with self.assertRaises(ValueError):
                filtered._identity(identity, 'fixture')
        bad = self.root / 'bad-ledger'
        bad.write_text(json.dumps({'id': 2, 'entries': [], 'phase': 1, 'round': 3}) + '\n')
        with self.assertRaises(ValueError):
            list(filtered._task_records(bad))

    def test_adoption_rejects_live_old_producer_stop_id_reuse_and_consumed_handover(self):
        import gc
        _bodies, warm, partial, ledger, _original = self.prepare()
        result = self.control / 'overlap-seed-result.json'
        write_json(result, {'ok': True, 'summary': {'verified_files': 0, 'verified_bytes': 0},
                            'merge': {'rows_processed': 0}})
        old = {'pid': 987654321, 'starttime_ticks': '1', 'argv': ['fixture-old-producer']}
        helper = {'pid': 987654322, 'starttime_ticks': '1', 'argv': ['fixture-seed'],
                  'result_path': str(result)}
        payload = {'control': str(self.control), 'stage': 'p3-r3', 'old_producer': old,
                   'seed_helper': helper, 'assigned_tasks_path': str(ledger), 'next_task_id': 5,
                   'warm_manifest': str(warm), 'missing_manifest': str(partial)}
        handover = self.control / 'handover.json'
        write_json(handover, payload)
        live = Path('/tmp') / (self.run_id + '-index.sqlite')
        inode = live.stat().st_ino
        with mock.patch.object(filtered, 'validate_mounts', lambda _cfg: None):
            with mock.patch.object(filtered, 'process_matches', lambda v: v['pid'] == old['pid']):
                with self.assertRaisesRegex(RuntimeError, 'old foreground producer'):
                    filtered.ParallelFilterResume(self.control, handover)
            gc.collect()
            (self.control / 'STOP').write_text('fixture stop\n')
            with self.assertRaisesRegex(RuntimeError, 'STOP'):
                filtered.ParallelFilterResume(self.control, handover)
            gc.collect()
            (self.control / 'STOP').unlink()
            with mock.patch.object(filtered, 'process_matches', lambda _v: False):
                write_json(handover, dict(payload, next_task_id=4))
                with self.assertRaisesRegex(ValueError, 'next_task_id'):
                    filtered.ParallelFilterResume(self.control, handover)
                gc.collect()
                write_json(handover, payload)
                coordinator = filtered.ParallelFilterResume(self.control, handover)
                self.close(coordinator)
                with self.assertRaisesRegex(RuntimeError, 'already been consumed'):
                    filtered.ParallelFilterResume(self.control, handover)
        self.assertEqual(live.stat().st_ino, inode)


if __name__ == '__main__':
    unittest.main()
