"""Local integration: historical receipts rebuild while new payloads copy.

The seed is a real subprocess and the eight formal rounds use the real copy,
SHA, receipt collection, hardlink and metadata helpers on temporary trees.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest import mock

import migration_overlap_resume as overlap
import migration_pipeline as pipeline
from migration_io import read_json, write_json
from migration_run import now, process_entry
from migration_scan import _stat_fields

WORK = Path(__file__).resolve().parent
AUTHORIZED = {'source': '/share/project/eai_pwm', 'destination': '/share-mixed/project/eai_pwm'}
SCHEMA = 'CREATE TABLE files (rel TEXT PRIMARY KEY,source TEXT,destination TEXT,owned INTEGER,sha256 TEXT)'


class OverlapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='overlap-resume-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.src, self.dst, self.old, self.control = (self.root / n for n in ('src', 'dst', 'old', 'new'))
        for path in (self.src, self.dst, self.old, self.control):
            path.mkdir()
        self.run_id = 'overlap-fixture-' + uuid.uuid4().hex
        self.cfg = dict(AUTHORIZED, run_id=self.run_id, resume_from=str(self.old),
                        threads=64, copy_threads_per_node=128, parallel_scan_threads=4,
                        scan_batch_files=2, resume_read_workers=4,
                        predecessor_isolation={'confirmed': True, 'old_control': str(self.old),
                                               'method': 'fixture_all_old_writers_stopped'},
                        require_priority4_plan=False)
        write_json(self.control / 'config.json', self.cfg)
        write_json(self.old / 'config.json', dict(AUTHORIZED, run_id='old-fixture'))
        (self.old / 'STOP').write_text('fixture predecessor stopped\n')
        self.warm = self.control / 'warm-tasks.jsonl'
        self.warm.write_text('')
        self.addCleanup(self.clean_runtime)

    def clean_runtime(self):
        for suffix in ('-index.sqlite', '-resume-candidate.sqlite'):
            for extra in ('', '-wal', '-shm'):
                Path('/tmp/' + self.run_id + suffix + extra).unlink(missing_ok=True)
        Path('/tmp/' + self.run_id + '-coordinator.lock').unlink(missing_ok=True)

    def make(self, cls=None):
        with mock.patch.object(overlap, 'validate_mounts', lambda _cfg: None):
            return (cls or overlap.OverlapResume)(self.control, self.warm)

    def close(self, coordinator):
        coordinator.hardlink_pool.shutdown(wait=True, cancel_futures=True)
        coordinator.directory_pool.shutdown(wait=True, cancel_futures=True)
        try:
            coordinator.db.close()
        except sqlite3.ProgrammingError:
            pass
        coordinator._lock.close()
        if coordinator.seed_process and coordinator.seed_process.poll() is None:
            (self.control / 'STOP').write_text('fixture cleanup\n')
            coordinator.seed_process.wait(timeout=20)

    def test_merge_never_replaces_fresh_rows_and_requires_inode_and_sha_for_ownership(self):
        candidate, live = self.root / 'candidate.sqlite', self.root / 'live.sqlite'
        def row(rel, stamp, inode, digest, owned):
            source = {'source_stamp': stamp}
            destination = {'mode': 0o100600, 'ino': inode, 'mtime_ns': stamp}
            return (rel, json.dumps(source), json.dumps(destination), owned, digest)
        old_rows = [row('fresh-owned', 1, 1, 'a', 1), row('match', 1, 2, 'b', 1),
                    row('different-inode', 1, 3, 'c', 1), row('different-sha', 1, 4, 'd', 1),
                    row('missing', 1, 5, 'e', 1)]
        fresh_rows = [row('fresh-owned', 9, 11, 'z', 1), row('match', 9, 2, 'b', 0),
                      row('different-inode', 9, 30, 'c', 0), row('different-sha', 9, 4, 'changed', 0)]
        for path, rows in ((candidate, old_rows), (live, fresh_rows)):
            with sqlite3.connect(path) as db:
                db.execute(SCHEMA)
                db.executemany('INSERT INTO files VALUES (?,?,?,?,?)', rows)
        result = overlap.merge_history(candidate, live, chunk_size=2)
        self.assertEqual(result, {'rows_processed': 5, 'rows_inserted': 1, 'ownership_upgraded': 1})
        with sqlite3.connect(live) as db:
            rows = {r[0]: r for r in db.execute('SELECT * FROM files')}
        self.assertEqual(rows['fresh-owned'], fresh_rows[0])
        self.assertEqual(rows['match'][:3], fresh_rows[1][:3])
        self.assertEqual(rows['match'][3:], (1, 'b'))
        self.assertEqual(rows['different-inode'], fresh_rows[2])
        self.assertEqual(rows['different-sha'], fresh_rows[3])
        self.assertEqual(rows['missing'], old_rows[-1])

    def test_real_seed_child_overlaps_warm_copy_then_all_eight_rounds_finish(self):
        bodies = {'home/hcr/old.txt': b'old verified payload',
                  'repos/cosmos-framework-pwm/repo.txt': b'repo payload',
                  'other/warm.txt': b'warm uncompleted payload',
                  'other/missing.txt': b'newly discovered missing payload'}
        for rel, data in bodies.items():
            path = self.src / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        canonical = 'home/hcr/old.txt'
        os.link(self.src / canonical, self.src / 'other/alias.txt')
        bodies['other/alias.txt'] = bodies[canonical]
        (self.dst / canonical).parent.mkdir(parents=True)
        shutil.copy2(self.src / canonical, self.dst / canonical)
        proof = {'rel': canonical, 'kind': 'file', 'status': 'copied',
                 'source_identity': _stat_fields((self.src / canonical).stat()),
                 'dst_stat': _stat_fields((self.dst / canonical).stat()),
                 'sha256': hashlib.sha256(bodies[canonical]).hexdigest()}
        write_json(self.old / 'stages/p1-r1/done/0/0.json', {'finished': now(), 'results': [proof]})
        original_receipt = (self.old / 'stages/p1-r1/done/0/0.json').read_bytes()
        self.warm.write_text(json.dumps({'entries': [{'rel': 'other/warm.txt'}]}) + '\n')
        reading, copied = self.root / 'seed-reading', self.root / 'payload-copied'
        test = self
        class Fixture(overlap.OverlapResume):
            def start_seed(inner):
                # Pause the real receipt read, not the whole child startup. The
                # warm copy must finish while the reader is live/in progress.
                code = '''import pathlib,sys,time
sys.path.insert(0,sys.argv[1])
import migration_resume_index_parallel_memory as seed
import migration_overlap_resume as overlap
reading,copied=map(pathlib.Path,sys.argv[2:4])
original=seed._read_receipt
def observed(path):
    value=original(path)
    reading.write_text(str(path))
    deadline=time.monotonic()+20
    while not copied.exists():
        if time.monotonic()>deadline: raise RuntimeError('warm copy never overlapped')
        time.sleep(.01)
    return value
seed._read_receipt=observed
raise SystemExit(overlap.seed_child(*sys.argv[4:]))
'''
                inner.seed_process = subprocess.Popen([sys.executable, '-u', '-c', code, str(WORK),
                    str(reading), str(copied), str(inner.control), str(inner.candidate), str(inner.seed_result)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            def drain(inner, stage):
                if stage == overlap.WARM_STAGE:
                    deadline = time.monotonic() + 20
                    while not reading.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    test.assertTrue(reading.exists(), 'seed did not read a historical receipt')
                    test.assertIsNone(inner.seed_process.poll(), 'seed must remain live during warm copy')
                for path in sorted((inner.control / 'stages' / stage / 'pending').glob('*/*.json')):
                    task = read_json(path)
                    results = [process_entry(inner.cfg, task, entry) for entry in task['entries']]
                    write_json(inner.control / 'stages' / stage / 'done' / str(task['id'] % 64) / path.name,
                               {'id': task['id'], 'finished': now(), 'results': results})
                    path.unlink()
                if stage == overlap.WARM_STAGE:
                    test.assertEqual((test.dst / 'other/warm.txt').read_bytes(), bodies['other/warm.txt'])
                    test.assertIsNone(inner.seed_process.poll())
                    copied.write_text('actual data copied with reader still active\n')
        coordinator = self.make(Fixture)
        self.addCleanup(self.close, coordinator)
        # Constructor validates the real mapping; data engine uses local trees.
        coordinator.cfg.update(source=str(self.src), destination=str(self.dst))
        write_json(self.control / 'config.json', coordinator.cfg)
        with mock.patch.object(pipeline, 'validate_mounts', lambda _cfg: None):
            coordinator.run()
        self.assertEqual(coordinator.state['status'], 'complete', coordinator.state)
        self.assertEqual(set(coordinator.pipeline['stages']), {'p%d-r%d' % p for p in pipeline.ORDER})
        self.assertNotIn(overlap.WARM_STAGE, coordinator.pipeline['stages'])
        self.assertEqual(coordinator.state['resume_index']['verified_files'], 1)
        self.assertTrue(copied.exists())
        self.assertEqual(coordinator.state['warm_summary'].get('copied'), 3)
        self.assertEqual(coordinator.pipeline['stages']['p3-r1']['summary'].get('verified'), 2)
        for rel, body in bodies.items():
            self.assertEqual((self.src / rel).read_bytes(), body)
            self.assertEqual((self.dst / rel).read_bytes(), body)
        self.assertEqual((self.dst / canonical).stat().st_ino, (self.dst / 'other/alias.txt').stat().st_ino)
        self.assertEqual((self.old / 'stages/p1-r1/done/0/0.json').read_bytes(), original_receipt)
        self.assertEqual(set(coordinator.pipeline['directory_jobs']), {'p1-r3', 'p2-r3', 'p3-r2'})

    def test_stop_unsafe_warm_and_reentry_are_refused(self):
        coordinator = self.make()
        self.addCleanup(self.close, coordinator)
        coordinator.cfg.update(source=str(self.src), destination=str(self.dst))
        for rel in ('../escape', '/absolute', 'home/hcr/secret', 'repos/cosmos-framework-pwm/private'):
            with self.assertRaises(ValueError):
                coordinator.fresh_entry({'rel': rel})
        (self.control / 'STOP').write_text('fixture cooperative stop\n')
        with self.assertRaisesRegex(RuntimeError, 'STOP'):
            coordinator.enqueue_warm()
        self.assertFalse(any(self.dst.rglob('*')))
        coordinator._lock.close()
        (self.control / 'STOP').unlink()
        with self.assertRaises((FileExistsError, RuntimeError)):
            self.make()

    def test_seed_child_stop_never_imports_history(self):
        coordinator = self.make()
        self.addCleanup(self.close, coordinator)
        (self.control / 'STOP').write_text('fixture stop before seed read\n')
        status = overlap.seed_child(self.control, coordinator.candidate, coordinator.seed_result)
        self.assertEqual(status, 1)
        self.assertFalse(read_json(coordinator.seed_result)['ok'])
        self.assertEqual(coordinator.db.execute('SELECT COUNT(*) FROM files').fetchone()[0], 0)
        self.assertFalse(coordinator.candidate.exists())


if __name__ == '__main__':
    unittest.main()
