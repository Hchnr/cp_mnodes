"""Linux subprocess fixture; signals target only this test's private processes."""
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


FIXTURE = r'''
import json, multiprocessing, pathlib, sys, threading, time
import migration_parallel_scan as scan
root = pathlib.Path(sys.argv[1])
if not str(root).startswith('/tmp/scan-interrupt-fixture-'):
    raise RuntimeError('private fixture only')
accepted = []
poll_mode = len(sys.argv) > 2 and sys.argv[2] == 'poll'
original = scan._consume
def consume(scanner, entry):
    original(scanner, entry)
    if entry.get('kind') == 'file':
        accepted.append(entry['rel'])
        if len(accepted) == 3:
            print(json.dumps({'ready': True, 'accepted': accepted,
                'children': [p.pid for p in multiprocessing.active_children()]}), flush=True)
            if not poll_mode:
                threading.Event().wait(30)
        if poll_mode:
            time.sleep(.005)
scan._consume = consume
try:
    scan.scan_phase(root/'source', 3, lambda entries: None, root/'inventory.jsonl',
        processes=2, threads_per_process=2, process_batch_size=4, batch_files=512)
except KeyboardInterrupt:
    print(json.dumps({'interrupted': True,
        'children': [p.pid for p in multiprocessing.active_children()]}), flush=True)
else:
    raise RuntimeError('expected a controlled fixture interrupt')
'''


def status(pid):
    try:
        return dict(line.split(':', 1) for line in
                    (Path('/proc') / str(pid) / 'status').read_text().splitlines()
                    if ':' in line)
    except FileNotFoundError:
        return None


@unittest.skipUnless(sys.platform == 'linux', 'Linux /proc and stopped-child fixture')
class GracefulInterruptTests(unittest.TestCase):
    def test_stopped_readers_are_released_only_for_cleanup_and_python_buffer_is_flushed(self):
        with tempfile.TemporaryDirectory(prefix='scan-interrupt-fixture-', dir='/tmp') as name:
            root = Path(name)
            (root / 'source').mkdir()
            for i in range(32):
                (root / 'source' / ('f' + str(i))).write_bytes(b'fixture')
            proc = subprocess.Popen([sys.executable, '-u', '-c', FIXTURE, str(root)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                start_new_session=True, cwd=Path(__file__).resolve().parent)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(timeout=10), 'fixture did not become ready')
                    ready = json.loads(proc.stdout.readline())
                self.assertTrue(ready['ready'])
                self.assertEqual(len(ready['children']), 2)
                for pid in ready['children']:
                    os.kill(pid, signal.SIGSTOP)
                deadline = time.monotonic() + 5
                while not all((status(pid) or {}).get('State', '').strip().startswith('T')
                              for pid in ready['children']):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.01)
                # The source inventory is still in Python's userspace buffer.
                before = (root / 'inventory.jsonl').read_bytes()
                self.assertFalse(before)
                from migration_scan_boundary import observe_boundary, identity, NotQuiescent
                with self.assertRaises(NotQuiescent):
                    # The message is already in Python, but only three of its
                    # four entries have been consumed. Queue emptiness is not
                    # a sufficient handover boundary.
                    observe_boundary(identity(proc.pid),
                        [identity(p) for p in ready['children']], root/'source')
                os.kill(proc.pid, signal.SIGINT)
                pending = set(ready['children'])
                while pending:
                    self.assertLess(time.monotonic(), deadline, 'cleanup did not request reader termination')
                    for pid in list(pending):
                        value = status(pid)
                        if value is None:
                            pending.remove(pid)
                        elif (int(value.get('SigPnd', '0'), 16) |
                              int(value.get('ShdPnd', '0'), 16)) & (1 << (signal.SIGTERM - 1)):
                            # Never resume a reader for more enumeration. First
                            # observe its owner's pending cleanup-only SIGTERM.
                            os.kill(pid, signal.SIGCONT)
                            pending.remove(pid)
                    time.sleep(.01)
                output, error = proc.communicate(timeout=10)
                self.assertEqual(proc.returncode, 0, error)
                stopped = json.loads(output.strip())
                self.assertTrue(stopped['interrupted'])
                self.assertEqual(stopped['children'], [])
                final = (root / 'inventory.jsonl').read_bytes()
                self.assertTrue(final.endswith(b'\n'))
                rows = [json.loads(line) for line in final.splitlines()]
                self.assertTrue(set(ready['accepted']) <= {e['rel'] for e in rows})
                self.assertEqual(len([r for r in rows if r['kind'] == 'file']), 3)
                from migration_warm_scan_checkpoint import Prefix
                from migration_parallel_scan import scan_phase
                prefix = Prefix()
                for entry in rows:
                    prefix.add(entry)
                # These leaves were in the scanner's unoffered batch. Preserve
                # them explicitly, then continue only the remaining inventory.
                emitted = list(prefix.tail)
                result = scan_phase(root/'source', 3, emitted.extend, root/'inventory.jsonl',
                    processes=2, threads_per_process=2, process_batch_size=4,
                    initial_seeded_paths=prefix.paths, seed_stats=prefix.scanner.stats,
                    append_manifest=True, skip_seeded_nondirectories=True,
                    completed_directories=prefix.pruning_candidates({''}, quiescent_and_flushed=True),
                    expected_root_identity=prefix.root_identity)
                self.assertTrue(result['scan_complete'])
                self.assertEqual(result['files'], 32)
                self.assertEqual(len(emitted), 32)
                self.assertEqual(len({e['rel'] for e in emitted}), 32)
            finally:
                if proc.poll() is None:
                    # Only the private start_new_session process group above.
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate(timeout=10)

    def test_idle_poll_and_kernel_queues_allow_a_lossless_directory_pruned_resume(self):
        from migration_scan_boundary import observe_boundary, identity, NotQuiescent
        from migration_unix_queues import unix_queues
        from migration_warm_scan_checkpoint import Prefix
        from migration_parallel_scan import scan_phase
        with tempfile.TemporaryDirectory(prefix='scan-interrupt-fixture-', dir='/tmp') as name:
            root = Path(name)
            source = root/'source'; source.mkdir()
            for i in range(32):
                directory = source/('d'+str(i)); directory.mkdir()
                for j in range(3):
                    (directory/('f'+str(j))).write_bytes(b'fixture')
            proc = subprocess.Popen([sys.executable, '-u', '-c', FIXTURE, str(root), 'poll'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                start_new_session=True, cwd=Path(__file__).resolve().parent)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(timeout=10))
                    ready = json.loads(proc.stdout.readline())
                parent = identity(proc.pid)
                readers = [identity(p) for p in ready['children']]
                for reader in readers:
                    os.kill(reader['pid'], signal.SIGSTOP)
                deadline = time.monotonic()+10
                while True:
                    self.assertLess(time.monotonic(), deadline, 'idle scan boundary was not observed')
                    try:
                        boundary = observe_boundary(parent, readers, source)
                        break
                    except NotQuiescent:
                        time.sleep(.01)
                self.assertFalse(boundary['safe_to_prune'])
                self.assertEqual(boundary['data_processes_signalled_by_observer'], 0)
                with self.assertRaisesRegex(NotQuiescent, 'identity'):
                    observe_boundary(dict(parent, start_ticks='wrong'), readers, source)
                os.kill(proc.pid, signal.SIGSTOP)
                while identity(proc.pid)['state'] != 'T':
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.01)
                q = unix_queues({x[k] for x in boundary['ipc']
                                 for k in ('child_inode','parent_inode')})
                self.assertTrue(all(q[p['child_inode']]['send_bytes'] == 0 and
                                    q[p['parent_inode']]['receive_bytes'] == 0
                                    for p in boundary['ipc']))
                os.kill(proc.pid, signal.SIGINT)
                os.kill(proc.pid, signal.SIGCONT)
                pending = {r['pid'] for r in readers}
                while pending:
                    self.assertLess(time.monotonic(), deadline)
                    for pid in list(pending):
                        v = status(pid)
                        if v is None:
                            pending.remove(pid)
                        elif (int(v.get('SigPnd','0'),16)|int(v.get('ShdPnd','0'),16)) & (1 << (signal.SIGTERM-1)):
                            os.kill(pid, signal.SIGCONT)
                            pending.remove(pid)
                    time.sleep(.01)
                output, error = proc.communicate(timeout=10)
                self.assertEqual(proc.returncode, 0, error)
                self.assertEqual(json.loads(output.strip())['children'], [])
                prefix = Prefix()
                for line in (root/'inventory.jsonl').read_bytes().splitlines():
                    prefix.add(json.loads(line))
                active = {f['rel'] for f in boundary['directory_fds']}
                completed = prefix.pruning_candidates(active, quiescent_and_flushed=True)
                self.assertTrue(completed, 'fixture must actually exercise a completed subtree')
                emitted = list(prefix.tail)
                result = scan_phase(source, 3, emitted.extend, root/'inventory.jsonl',
                    processes=2, threads_per_process=2, process_batch_size=4,
                    initial_seeded_paths=prefix.paths, seed_stats=prefix.scanner.stats,
                    append_manifest=True, skip_seeded_nondirectories=True,
                    completed_directories=completed, expected_root_identity=prefix.root_identity)
                self.assertTrue(result['scan_complete'])
                self.assertGreater(result['process_readers']['pruned_completed_directories'], 0)
                self.assertEqual(result['files'], 96)
                self.assertEqual(len(emitted), 96)
                self.assertEqual(len({e['rel'] for e in emitted}), 96)
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)  # Private fixture session only.
                proc.communicate(timeout=10)


if __name__ == '__main__':
    unittest.main()
