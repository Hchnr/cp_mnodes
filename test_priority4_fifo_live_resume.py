"""Native private-tree fixture of a live threaded FIFO -> process scan resume."""
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from migration_io import read_json
from migration_priority4_frontier import Priority4Prefix, FifoFrontier, observe_source_frames
from migration_priority4_fast_run import unoffered_tail
from migration_scan_boundary import identity
from migration_warm_scan_checkpoint import read_prefix
import migration_priority4_scan as scan


CHILD = r'''
import json, pathlib, sys, time
import migration_priority4_scan as scan
from migration_io import write_json
root=pathlib.Path(sys.argv[1])
assert str(root).startswith('/tmp/p4-fifo-resume-fixture-')
tasks=0
def emit(entries):
 global tasks
 with open(root/'assigned.jsonl','a') as out:
  out.write(json.dumps(entries)+'\n')
 tasks+=1
 write_json(root/'fixture-state.json',dict(next_task_id=tasks))
original=scan._consume
def slow(scanner,entry):
 original(scanner,entry)
 if entry.get('kind')=='file':time.sleep(.002)
scan._consume=slow
write_json(root/'fixture-state.json',dict(next_task_id=0))
print('READY',flush=True)
try:
 scan.scan_phase(root/'source',4,emit,root/'inventory.jsonl',workers=4,queue_size=8,batch_files=3)
except KeyboardInterrupt:
 print('FLUSHED',flush=True)
else:
 raise AssertionError('private fixture completed before requested interrupt')
'''


class FollowedPrefix(Priority4Prefix):
    frontier = None
    def add(self, entry):
        super().add(entry)
        if self.frontier is not None:
            self.frontier.entry(entry)


@unittest.skipUnless(sys.platform == 'linux', 'native /proc observation fixture')
class FifoLiveResumeTests(unittest.TestCase):
    def test_live_fifo_boundary_preserves_queued_and_unentered_work_without_recopy(self):
        with tempfile.TemporaryDirectory(prefix='p4-fifo-resume-fixture-', dir='/tmp') as name:
            root = Path(name)
            source = root / 'source'
            source.mkdir()
            expected = set()
            for i in range(80):
                (source / str(i)).mkdir()
                for j in range(8):
                    rel = str(i) + '/f' + str(j)
                    (source / rel).write_bytes(b'fixture')
                    expected.add(rel)
            process = subprocess.Popen([sys.executable, '-u', '-c', CHILD, str(root)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                start_new_session=True, cwd=Path(__file__).resolve().parent)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(timeout=10))
                    self.assertEqual(process.stdout.readline().strip(), 'READY')
                deadline = time.monotonic() + 15
                manifest = root / 'inventory.jsonl'
                while not manifest.exists() or manifest.stat().st_size < 10000:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.005)
                prefix, digest = FollowedPrefix(), hashlib.sha256()
                with open(manifest, 'rb') as stream:
                    offset = read_prefix(stream, manifest.stat().st_size, prefix, digest)
                    frames = observe_source_frames(identity(process.pid), source)
                    # Only the namespace label differs in this private fixture;
                    # no test is allowed to open the production source root.
                    frames['source_root'] = '/share/project'
                    tasks = read_json(root / 'fixture-state.json')['next_task_id']
                    frontier = FifoFrontier(prefix, frames, completed_tasks=tasks,
                        capture_offset=offset, queue_capacity=8, backpressure=4, poll_every=2)
                    prefix.frontier = frontier
                    while True:
                        state = read_json(root / 'fixture-state.json')
                        size = manifest.stat().st_size
                        frontier.observe_flush(state['next_task_id'], size)
                        offset = read_prefix(stream, size, prefix, digest)
                        proof = frontier.progress(offset, prefix.records)
                        if proof['status'] == 'prepared_fifo_frontier_only':
                            break
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.005)
                    self.assertTrue(frontier.candidates, 'must exercise completed subtree pruning')
                    process.send_signal(signal.SIGINT)  # This fixture child only.
                    output, error = process.communicate(timeout=10)
                    self.assertEqual(process.returncode, 0, error)
                    self.assertEqual(output.strip(), 'FLUSHED')
                    offset = read_prefix(stream, manifest.stat().st_size, prefix, digest)
                    self.assertEqual(offset, manifest.stat().st_size)
                prior = [entry for line in (root / 'assigned.jsonl').read_text().splitlines()
                         for entry in json.loads(line)]
                assigned = {e['rel'] for e in prior}
                emitted = unoffered_tail(prefix, assigned)
                stats = scan.scan_phase(source, 4, emitted.extend, manifest,
                    processes=3, threads_per_process=2, process_batch_size=4, queue_size=8,
                    initial_seeded_paths=prefix.paths, seed_stats=prefix.scanner.stats,
                    hardlink_index=prefix.scanner.hardlinks, append_manifest=True,
                    skip_seeded_nondirectories=True, completed_directories=frontier.candidates,
                    expected_root_identity=prefix.root_identity)
                all_paths = [e['rel'] for e in prior + emitted]
                self.assertEqual(set(all_paths), expected)
                self.assertEqual(len(all_paths), len(expected), 'no old assignment may be re-emitted')
                self.assertEqual(stats['files'], len(expected))
                self.assertGreater(stats['process_readers']['pruned_completed_directories'], 0)
                rows = [json.loads(line) for line in manifest.read_text().splitlines()]
                self.assertEqual(len([r for r in rows if r['kind'] == 'file']), len(expected))
            finally:
                if process.poll() is None:
                    process.kill()  # Exact private fixture child, never job actors.
                process.communicate(timeout=10)


if __name__ == '__main__':
    unittest.main()
