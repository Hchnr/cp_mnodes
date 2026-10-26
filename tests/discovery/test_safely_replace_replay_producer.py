"""Local-only correction guards; no SSH or real process signals."""
import copy
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import safely_replace_replay_producer as correction


def replay_state():
    return {'stage': 'p3-r3', 'run_id': 'test-run', 'recovery_warm': True,
            'status': 'scanning', 'scan_complete': False, 'next_task_id': 419695,
            'task_publication': {'published_since_handover': 0, 'pending': 0,
                                 'next_task_id': 419695, 'failed': False,
                                 'workers': 8, 'limit': 64},
            'target_filter': {'submitted': 30, 'checked': 25, 'missing': 0,
                              'peak_inflight': 8, 'inflight': 5},
            'distributed_target_probe': {
                'counts': {'remote_existing': 25},
                'endpoints': {'http://test:1': {'remote_existing': 25}}}}


class ReplayGuardsTests(unittest.TestCase):
    def test_only_unproductive_replay_accepted(self):
        correction.zero_publication(replay_state(), 419695)
        for key, value in (('stage', 'p1-r1'), ('next_task_id', 419696),
                           ('status', 'copying'), ('scan_complete', True)):
            state = replay_state()
            state[key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                correction.zero_publication(state, 419695)

    def test_any_publication_reservation_failure_or_pending_rejected(self):
        for key, value in (('published_since_handover', 1), ('pending', 1),
                           ('next_task_id', 419696), ('failed', True), ('workers', 16)):
            state = replay_state()
            state['task_publication'][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                correction.zero_publication(state, 419695)

    def test_missing_probe_or_main_thread_missing_rejected(self):
        for target, key in (('target_filter', 'missing'), ('distributed_target_probe', 'remote_missing')):
            state = replay_state()
            if target == 'target_filter':
                state[target][key] = 1
            else:
                state[target]['counts'][key] = 1
            with self.assertRaises(RuntimeError):
                correction.zero_publication(state, 419695)

    def test_baseline_adds_observations_without_resetting_discovery(self):
        original = replay_state()
        original['next_task_id'] = 419674
        original['target_filter'].update(submitted=200, checked=190, missing=170)
        original['distributed_target_probe']['counts']['remote_existing'] = 100
        original['distributed_target_probe']['endpoints']['http://test:1']['remote_existing'] = 100
        original['phases'] = {'3': {'discovered_files': 12345, 'discovered_bytes': 98765}}
        before = copy.deepcopy(original)
        result = correction.cumulative_state(original, replay_state())
        self.assertEqual(original, before)
        self.assertEqual(result['target_filter']['submitted'], 230)
        self.assertEqual(result['target_filter']['checked'], 215)
        self.assertEqual(result['target_filter']['missing'], 170)
        self.assertEqual(result['distributed_target_probe']['counts']['remote_existing'], 125)
        self.assertEqual(result['distributed_target_probe']['endpoints']['http://test:1']['remote_existing'], 125)
        self.assertEqual(result['phases'], before['phases'])
        self.assertEqual(result['next_task_id'], 419674)

    def test_baseline_different_run_rejected(self):
        changed = replay_state()
        changed['run_id'] = 'other'
        with self.assertRaises(RuntimeError):
            correction.cumulative_state(replay_state(), changed)

    def test_invalid_probe_counter_rejected(self):
        for value in (-1, True, '4'):
            changed = replay_state()
            changed['distributed_target_probe']['counts']['remote_existing'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                correction.cumulative_state(replay_state(), changed)


class DescriptorProofTests(unittest.TestCase):
    def test_reading_old_manifest_writing_partial_replay_proven(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / 'old.jsonl'
            old.write_text('old\n')
            active = root / 'partial.jsonl'
            source = root / 'source'
            source.mkdir()
            identity = correction.base.process_identity(os.getpid())
            with open(old, 'rb'), open(active, 'wb'):
                proof = correction.descriptor_proof(identity, old_missing=old,
                                                    active_manifest=active, source=source)
                self.assertEqual(len(proof['old_manifest_reads']), 1)
                self.assertEqual(len(proof['replay_manifest_writes']), 1)
                self.assertEqual(proof['source_fds'], [])
                anchor = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    anchored = correction.descriptor_proof(identity, old_missing=old,
                                                           active_manifest=active, source=source)
                    self.assertEqual(len(anchored['source_root_anchors']), 1)
                    child = source / 'child'
                    child.mkdir()
                    fd = os.open(child, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        with self.assertRaisesRegex(RuntimeError, 'source scan may have started'):
                            correction.descriptor_proof(identity, old_missing=old,
                                                        active_manifest=active, source=source)
                    finally:
                        os.close(fd)
                finally:
                    os.close(anchor)

    def test_no_old_manifest_fd_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old, active = root / 'old', root / 'new'
            old.write_text('')
            with open(active, 'wb'):
                with self.assertRaises(RuntimeError):
                    correction.descriptor_proof(correction.base.process_identity(os.getpid()),
                                                old_missing=old, active_manifest=active,
                                                source=root / 'source')


class TaskFenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.db_path = self.root / 'broker.sqlite'
        with sqlite3.connect(self.db_path) as db:
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER)')
            db.execute('INSERT INTO claims VALUES(?,?)', ('p3-r3', 419694))
        for shard in range(64):
            (self.root / 'stages/p3-r3/pending' / str(shard)).mkdir(parents=True)
        self.reader = patch.object(correction.base, 'claims_reader',
                                   side_effect=lambda _: sqlite3.connect(self.db_path))
        self.reader.start()
        self.addCleanup(self.reader.stop)

    def test_original_old_task_ids_accepted(self):
        proof = correction.task_fence(self.root, {'run_id': 'test'}, 419695)
        self.assertEqual(proof['broker_maximum_id'], 419694)

    def test_any_new_claim_rejected(self):
        with sqlite3.connect(self.db_path) as db:
            db.execute('INSERT INTO claims VALUES(?,?)', ('p3-r3', 419695))
        with self.assertRaisesRegex(RuntimeError, 'new task beyond'):
            correction.task_fence(self.root, {'run_id': 'test'}, 419695)

    def test_any_new_pending_rejected(self):
        task_id = 419695
        (self.root / 'stages/p3-r3/pending' / str(task_id % 64) / (str(task_id) + '.json')).write_text('{}')
        with self.assertRaisesRegex(RuntimeError, 'pending task beyond'):
            correction.task_fence(self.root, {'run_id': 'test'}, 419695)

    def test_new_claim_after_pending_scan_rejected(self):
        original_scandir = os.scandir
        calls = [0]

        def concurrent_claim(path):
            calls[0] += 1
            if calls[0] == 64:
                with sqlite3.connect(self.db_path) as db:
                    db.execute('INSERT INTO claims VALUES(?,?)', ('p3-r3', 419695))
            return original_scandir(path)

        with patch.object(correction.os, 'scandir', side_effect=concurrent_claim):
            with self.assertRaisesRegex(RuntimeError, 'claimed while scanning'):
                correction.task_fence(self.root, {'run_id': 'test'}, 419695)


if __name__ == '__main__':
    unittest.main()
