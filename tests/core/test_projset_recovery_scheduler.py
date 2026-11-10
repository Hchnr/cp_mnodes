import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid
import unittest

REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO / 'ops/copy/projset_recovery_copy.py'
SPEC = importlib.util.spec_from_file_location('projset_recovery_copy', SOURCE)
copy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(copy)


class RecoverySchedulerTests(unittest.TestCase):
    @staticmethod
    def clients(count, active_counts=None):
        active_counts = active_counts or {}
        return {
            f'node-{i}': {
                'alive': True,
                'ready': True,
                'active': {str(j): 'copy' for j in range(active_counts.get(i, 0))},
            }
            for i in range(count)
        }

    def test_fewest_active_copy_leases_take_priority(self):
        clients = self.clients(8, {0: 3, 1: 2, 2: 1})
        self.assertEqual(
            copy.fair_copy_order(clients),
            ['node-3', 'node-4', 'node-5', 'node-6', 'node-7',
             'node-2', 'node-1', 'node-0'])

    def test_round_robin_breaks_equal_load_ties(self):
        clients = self.clients(5)
        self.assertEqual(copy.fair_copy_order(clients, cursor=3),
                         ['node-3', 'node-4', 'node-0', 'node-1', 'node-2'])

    def test_unready_workers_are_not_candidates(self):
        clients = self.clients(3)
        clients['node-1']['ready'] = False
        clients['node-2']['alive'] = False
        self.assertEqual(copy.fair_copy_order(clients), ['node-0'])

    def test_small_copy_burst_water_fills_idle_workers_first(self):
        clients = self.clients(16, {0: 3, 1: 3, 2: 3, 3: 3})
        assignments = []
        for cursor in range(9):
            node = next(name for name in copy.fair_copy_order(clients, cursor)
                        if sum(kind == 'copy' for kind in clients[name]['active'].values()) < 3)
            clients[node]['active'][f'new-{cursor}'] = 'copy'
            assignments.append(node)
        self.assertEqual(assignments, [f'node-{i}' for i in range(4, 13)])

    def test_recovery_controller_distributes_copy_batches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / 'run'
            (run / 'recovery' / 'code').mkdir(parents=True)
            (run / 'logs').mkdir()
            source, target = root / 'source', root / 'target'
            (source / 'bucket').mkdir(parents=True)
            target.mkdir()
            for i in range(560):
                (source / 'bucket' / f'f{i:04d}').write_bytes(f'payload-{i}'.encode())

            nodes = {'node-a': {}, 'node-b': {}}
            run_id = 'sched-' + uuid.uuid4().hex
            cfg = {
                'run_id': run_id,
                'source': str(source),
                'target': str(target),
                'threads': 4,
                'nodes': nodes,
                'code': {},
            }
            (run / 'config.json').write_text(json.dumps(cfg))
            zero = {'processed': 0, 'copied': 0, 'copied_bytes': 0,
                    'skipped': 0, 'skipped_bytes': 0, 'payload_written': 0,
                    'hardlink_paths': 0, 'error': 0, 'conflict': 0,
                    'changed': 0, 'unsupported': 0, 'metadata_warnings': 0}
            (run / 'recovery' / 'baseline.json').write_text(json.dumps({n: zero for n in nodes}))
            (run / 'recovery' / 'original-counts.json').write_text(json.dumps({n: zero for n in nodes}))
            for node in nodes:
                folder = run / 'nodes' / node
                folder.mkdir(parents=True)
                (folder / 'receipts.jsonl').touch()
            (run / 'tasks.jsonl').write_text(json.dumps(
                {'id': 1, 'type': 'scan', 'rel': ''}) + '\n')
            (run / 'completed.jsonl').touch()
            (run / 'status.json').write_text(json.dumps({
                'state': 'copying', 'started': 1, 'counts': {},
            }))

            wrapper = run / 'recovery' / 'code' / 'projset_recovery_copy.py'
            wrapper.write_text(
                'import sys\n'
                f'sys.path.insert(0, {str(SOURCE.parent)!r})\n'
                'import projset_recovery_copy as m\n'
                'm.validate = lambda *args: None\n'
                'm.identity = lambda: {}\n'
                'm.main()\n'
            )
            harness = root / 'controller.py'
            harness.write_text(
                'import sys\n'
                f'sys.path.insert(0, {str(SOURCE.parent)!r})\n'
                'import projset_recovery_copy as m\n'
                'from pathlib import Path\n'
                'm.ssh = lambda node, command: ["sh", "-c", command]\n'
                f'm.controller(Path({str(run)!r}))\n'
            )
            env = dict(os.environ, PROJSET_ATTEMPT='test-attempt')
            completed = subprocess.run(
                [sys.executable, '-S', '-B', str(harness)], env=env,
                capture_output=True, text=True, timeout=90)
            logs = '\n'.join(path.read_text() for path in (run / 'logs').glob('*'))
            self.assertEqual(completed.returncode, 0, completed.stderr + logs)

            tasks = [json.loads(line) for line in
                     (run / 'recovery' / 'tasks.test-attempt.jsonl').read_text().splitlines()]
            done = [json.loads(line) for line in
                    (run / 'recovery' / 'completed.test-attempt.jsonl').read_text().splitlines()]
            copy_ids = {row['id'] for row in tasks if row['type'] == 'copy'}
            copy_nodes = {row['node'] for row in done if row['id'] in copy_ids}
            self.assertEqual(len(copy_ids), 5)
            self.assertEqual(copy_nodes, set(nodes))
            self.assertEqual(json.loads((run / 'status.json').read_text())['counts']['copied'], 560)
            self.assertEqual(
                json.loads((run / 'recovery' / 'rebuild-progress.json').read_text())['state'],
                'rebuilding')
            self.assertEqual(
                json.loads((run / 'recovery' / 'rebuild-progress.json').read_text())['stage'],
                'ready')
            for node in nodes:
                try:
                    os.unlink('/tmp/' + run_id + '-' + node + '.lock')
                except FileNotFoundError:
                    pass
            try:
                os.unlink('/tmp/' + run_id + '-controller.lock')
            except FileNotFoundError:
                pass
            for suffix in ('', '-wal', '-shm'):
                try:
                    os.unlink('/var/tmp/' + run_id + '-recovery.sqlite' + suffix)
                except FileNotFoundError:
                    pass


if __name__ == '__main__':
    unittest.main()
