"""Pure local tests: never SSH, signal real PIDs, or open the live control."""
import contextlib
import hashlib
import json
from pathlib import Path
import signal
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import uuid

import deploy_pipelined_producer as deploy
from migration_io import read_json


def task(task_id, rel=None):
    return {'id': task_id, 'phase': 3, 'round': 3, 'op': 'sync',
            'entries': [{'rel': 'r' + str(task_id) if rel is None else rel,
                         'kind': 'file', 'size': 1}]}


class AssignmentLedgerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = deploy.AssignmentLedger(self.root / 'assigned.jsonl')
        self.addCleanup(self.ledger.close)
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,payload TEXT)')

    def claims(self, ids):
        self.db.executemany('INSERT INTO claims VALUES(?,?,?)',
                            [(deploy.WARM_STAGE, i, json.dumps(task(i))) for i in ids])
        self.db.commit()

    def pending_shards(self):
        root = self.root / 'stages' / deploy.WARM_STAGE / 'pending'
        for shard in range(64):
            (root / str(shard)).mkdir(parents=True)
        return root

    def test_duplicate_suppression_and_immutable_payload_conflict(self):
        self.assertTrue(self.ledger.add(task(100)))
        self.assertFalse(self.ledger.add(task(100)))
        with self.assertRaisesRegex(RuntimeError, 'immutable task payload conflict'):
            self.ledger.add(task(100, 'different'))
        info = self.ledger.close()
        raw = self.ledger.path.read_bytes()
        self.assertEqual(info['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(info['tasks'], 1)
        self.assertEqual(len(raw.splitlines()), 1)
        self.assertEqual(info, self.ledger.close())

    def test_lower_claim_hole_is_recovered_not_only_maximum_id(self):
        self.ledger.add(task(100))
        self.claims((1, 100, 200))
        self.assertEqual(deploy.collect_claims(self.db, self.ledger, missing_only=True), 2)
        self.assertEqual(set(self.ledger.hashes), {1, 100, 200})
        info = self.ledger.close()
        self.assertEqual(info['maximum_id'], 200)
        self.assertEqual([json.loads(line)['id'] for line in
                          self.ledger.path.read_text().splitlines()], [100, 1, 200])

    def test_claim_payload_cannot_disagree_with_row_id(self):
        self.db.execute('INSERT INTO claims VALUES(?,?,?)',
                        (deploy.WARM_STAGE, 1, json.dumps(task(2))))
        with self.assertRaisesRegex(ValueError, 'ID differs'):
            deploy.collect_claims(self.db, self.ledger)

    def test_other_stage_fails_closed(self):
        self.db.execute('INSERT INTO claims VALUES(?,?,?)', ('p1-r1', 1, '{}'))
        with self.assertRaisesRegex(RuntimeError, 'unexpected formal-stage'):
            deploy.collect_claims(self.db, self.ledger)

    def test_pending_disappears_only_after_durable_broker_claim(self):
        root = self.pending_shards()
        path = root / '1' / '1.json'
        path.write_text(json.dumps(task(1)))
        self.claims((1,))

        def vanished(candidate):
            candidate.unlink()
            raise FileNotFoundError()

        self.assertEqual(deploy.collect_pending(self.root, self.db, self.ledger, vanished), 1)
        self.assertEqual(set(self.ledger.hashes), {1})

    def test_vanished_pending_without_claim_fails_closed(self):
        root = self.pending_shards()
        (root / '1' / '1.json').write_text(json.dumps(task(1)))
        with self.assertRaisesRegex(RuntimeError, 'without a durable claim'):
            deploy.collect_pending(self.root, self.db, self.ledger,
                                   lambda _: (_ for _ in ()).throw(FileNotFoundError()))

    def test_missing_one_of_64_shards_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'missing queue shard'):
            deploy.collect_pending(self.root, self.db, self.ledger, read_json)

    def test_wrong_shard_fails_closed(self):
        root = self.pending_shards()
        (root / '2' / '1.json').write_text(json.dumps(task(1)))
        with self.assertRaisesRegex(RuntimeError, 'wrong shard'):
            deploy.collect_pending(self.root, self.db, self.ledger, read_json)

    def test_id_hash_index_contains_no_full_payload(self):
        self.ledger.add(task(1))
        self.assertEqual(set(self.ledger.hashes), {1})
        self.assertIsInstance(self.ledger.hashes[1], bytes)
        self.assertEqual(len(self.ledger.hashes[1]), 32)

    def test_closed_ledger_rejects_new_id(self):
        self.ledger.close()
        with self.assertRaisesRegex(RuntimeError, 'ledger is closed'):
            self.ledger.add(task(2))

    def test_invalid_ids_and_paths_rejected(self):
        for task_id in (True, -1, '1', 1.0):
            with self.subTest(task_id=task_id), self.assertRaises(ValueError):
                deploy.task_bytes(task(task_id))
        for rel in ('', '/abs', '../up', 'a/../b', 'a//b', 'a/', 'nul\0name'):
            with self.subTest(rel=rel), self.assertRaises(ValueError):
                deploy.task_bytes(task(1, rel))


class IdentityTests(unittest.TestCase):
    def test_complete_legacy_role_normalized(self):
        actual = deploy.role_identity({'pid': 123, 'startticks': '456', 'argv': ['python', 'run']})
        self.assertEqual(actual['starttime_ticks'], '456')
        self.assertTrue(deploy.same_identity(dict(actual, state='S'), actual))
        for changed in ({'pid': 124}, {'starttime_ticks': '457'}, {'argv': ['different']}, {'state': 'Z'}):
            self.assertFalse(deploy.same_identity(dict(actual, state='S', **changed)
                             if 'state' not in changed else dict(actual, **changed), actual))

    def test_incomplete_saved_identity_rejected(self):
        for record in ({}, {'pid': True}, {'pid': 1, 'startticks': 'x', 'argv': ['python']},
                       {'pid': 1, 'startticks': '1', 'argv': []}):
            with self.subTest(record=record), self.assertRaises(ValueError):
                deploy.role_identity(record)


class WaitExitedTests(unittest.TestCase):
    def setUp(self):
        self.expected = {'pid': 987654321, 'starttime_ticks': '123',
                         'argv': ['python', 'producer.py']}

    def test_empty_cmdline_transition_waits_for_positive_zombie_evidence(self):
        for empty in ([], ['']):
            with self.subTest(empty=empty):
                observations = [dict(self.expected, state='R', argv=empty),
                                dict(self.expected, state='Z', argv=empty)]
                with patch.object(deploy, 'process_identity', side_effect=observations) as identity, \
                     patch.object(deploy.time, 'monotonic', side_effect=[0, .1, .2]), \
                     patch.object(deploy.time, 'sleep') as sleep, \
                     patch.object(deploy.os, 'kill') as kill:
                    deploy.wait_exited(self.expected, 1)
                    self.assertEqual(identity.call_count, 2)
                    sleep.assert_called_once_with(.1)
                    kill.assert_not_called()

    def test_nonempty_different_argv_still_fails_closed(self):
        changed = dict(self.expected, state='S', argv=['python', 'different.py'])
        with patch.object(deploy, 'process_identity', return_value=changed), \
             patch.object(deploy.time, 'monotonic', side_effect=[0, .1]), \
             patch.object(deploy.time, 'sleep') as sleep, \
             patch.object(deploy.os, 'kill') as kill:
            with self.assertRaisesRegex(RuntimeError, 'unexpectedly changed argv'):
                deploy.wait_exited(self.expected, 1)
            sleep.assert_not_called()
            kill.assert_not_called()

    def test_persistent_empty_cmdline_is_not_mistaken_for_exit(self):
        for empty in ([], ['']):
            with self.subTest(empty=empty):
                live = dict(self.expected, state='R', argv=empty)
                with patch.object(deploy, 'process_identity', return_value=live) as identity, \
                     patch.object(deploy.time, 'monotonic', side_effect=[0, .1, .2, 1.1]), \
                     patch.object(deploy.time, 'sleep') as sleep, \
                     patch.object(deploy.os, 'kill') as kill:
                    with self.assertRaisesRegex(RuntimeError, 'did not exit; no SIGKILL'):
                        deploy.wait_exited(self.expected, 1)
                    self.assertEqual(identity.call_count, 2)
                    self.assertEqual(sleep.call_count, 2)
                    kill.assert_not_called()


class HandoverFailureTests(unittest.TestCase):
    def exercise_failure(self, boundary):
        """Inject failures before/after TERM; every signal is a unittest mock."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle = root / 'bundle'
            bundle.mkdir()
            (root / 'warm.jsonl').write_text('')
            (root / 'missing.jsonl').write_text('')
            old = {'pid': 987654321, 'starttime_ticks': '123',
                   'argv': ['python', 'migration_distributed_target_filter.py']}
            live = dict(old, state='T')
            evidence = {'old_producer': old, 'old_role': {}, 'seed_helper': old,
                        'warm_manifest': str(root / 'warm.jsonl'), 'broker': old, 'training': []}
            cfg = {'run_id': 'test-pipelined-' + uuid.uuid4().hex}
            payload = {'control': str(root), 'mode': 'apply', 'operation': uuid.uuid4().hex,
                       'control_timeout': 30, 'publish_workers': 8, 'publish_limit': 64}
            db = sqlite3.connect(':memory:')
            db.execute('CREATE TABLE claims(stage TEXT,id INTEGER,payload TEXT)')
            pending_calls = [0]
            signals = []

            def pending(*args):
                pending_calls[0] += 1
                if pending_calls[0] == 2 and boundary == 'before_term':
                    raise RuntimeError('injected frozen reconciliation failure')
                return 0

            def exited(*args):
                if boundary == 'after_term':
                    raise RuntimeError('injected old-exit inspection failure')

            def released(*args):
                raise RuntimeError('injected lock-release inspection failure')

            with contextlib.ExitStack() as stack:
                mocks = {
                    'preflight': lambda *a: (evidence, cfg),
                    'immutable_bundle': lambda *a: (bundle, {'migration_pipelined_target_filter.py': 'abc'}),
                    'claims_reader': lambda *a: db,
                    'collect_pending': pending,
                    'assert_protected_identities': lambda *a: None,
                    'require_identity': lambda *a, **kw: live,
                    'process_identity': lambda *a: live,
                    'wait_state': lambda *a: live,
                    'guarded_state': lambda *a: {'next_task_id': 0, 'active_manifest': str(root / 'missing.jsonl')},
                    'broker_control': lambda *a: {'draining_hosts': []},
                    'wait_exited': exited, 'wait_lock_released': released,
                }
                for name, value in mocks.items():
                    stack.enter_context(patch.object(deploy, name, side_effect=value))
                stack.enter_context(patch.object(deploy.subprocess, 'run'))
                launched = stack.enter_context(patch.object(deploy.subprocess, 'Popen'))
                stack.enter_context(patch.object(deploy.os, 'kill', side_effect=lambda p, s: signals.append((p, s))))
                stack.enter_context(patch.object(deploy.sys, 'path', list(deploy.sys.path)))
                with self.assertRaisesRegex(RuntimeError, 'injected'):
                    deploy.remote_operation(payload)
                launched.assert_not_called()
            record = read_json(bundle / 'deployment.json')
            self.assertEqual(signals[0], (old['pid'], signal.SIGSTOP))
            self.assertEqual(signals[-1], (old['pid'], signal.SIGCONT))
            self.assertTrue(all(pid == old['pid'] for pid, _ in signals))
            self.assertFalse(record['worker_broker_training_signalled'])
            self.assertFalse(record['drains_modified'])
            return record, signals

    def test_pre_term_failure_resumes_original_without_termination(self):
        record, signals = self.exercise_failure('before_term')
        self.assertEqual(record['status'], 'aborted_old_preserved')
        self.assertEqual([sig for _, sig in signals], [signal.SIGSTOP, signal.SIGCONT])
        self.assertFalse(record['termination_attempted'])
        self.assertFalse(record['old_producer_terminated'])

    def test_term_attempt_is_not_reported_as_confirmed_exit(self):
        record, signals = self.exercise_failure('after_term')
        self.assertEqual(record['status'], 'needs_inspection')
        self.assertEqual([sig for _, sig in signals], [signal.SIGSTOP, signal.SIGTERM, signal.SIGCONT])
        self.assertTrue(record['termination_attempted'])
        self.assertFalse(record['old_producer_terminated'])

    def test_exit_confirmed_but_lock_uncertain_does_not_launch(self):
        record, _ = self.exercise_failure('after_exit')
        self.assertEqual(record['status'], 'needs_inspection')
        self.assertTrue(record['termination_attempted'])
        self.assertTrue(record['old_producer_terminated'])


if __name__ == '__main__':
    unittest.main()
