"""Scheduling receipts are recoverable hints, not copy-completion proof."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

from migration_input_receipts import InputReceipts, publish_unique


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        self.owner = self.adopt({0, 3}, 10)
        self.store = InputReceipts(self.owner)
        ready = self.control / 'frontier-input/node-a/ready'
        ready.mkdir(parents=True)
        self.path = ready / 'candidate.json'
        self.path.write_bytes(b'{"immutable":"original bytes"}\n')
        self.before = self.path.read_bytes()
        self.rows = [(self.path, ready.parent/'done/candidate.json', [], 7)]

    def adopt(self, ids, floor):
        return SimpleNamespace(control=self.control,
            cfg={'run_id':'receipt-fixture','copy_hosts':['node-a','node-b']},
            adopted_task_ids=set(ids), next_task_id=floor,
            state={'priority_inputs':{'files_consumed':12,'entries_consumed':100}})

    def test_group_is_one_record_original_is_unchanged_and_recovery_counts_once(self):
        record = self.store.commit(self.rows, 11)
        self.assertEqual(self.path.read_bytes(), self.before)
        self.assertFalse(self.rows[0][1].exists())
        self.assertEqual(list(self.store.groups.glob('*.json')), [record])
        restored = InputReceipts(self.adopt({0, 3, 10, 11}, 20))
        self.assertEqual(restored.known, {self.path:7})
        self.assertEqual((restored.files, restored.total_entries), (13, 107))
        self.assertEqual(restored.replay_groups, 0)
        # Another valid generation may acknowledge a replayed hint. It must
        # never inflate unique input counts or become a copy receipt.
        restored.commit(self.rows, 19)
        again = InputReceipts(self.adopt({0, 3, 10, 11}, 30))
        self.assertEqual((again.files, again.total_entries), (13, 107))

    def test_missing_task_behind_higher_id_replays_without_hiding_hint(self):
        self.store.commit(self.rows, 11)
        restored = InputReceipts(self.adopt({0, 3, 11, 99}, 110))
        self.assertNotIn(self.path, restored.known)
        self.assertEqual(restored.replay_groups, 1)
        self.assertTrue(self.path.exists())

    def test_missing_predecessor_basis_task_replays_even_when_new_range_is_complete(self):
        self.store.commit(self.rows, 11)
        restored = InputReceipts(self.adopt({0, 10, 11}, 20))
        self.assertEqual(restored.replay_groups, 1)
        self.assertEqual(restored.known, {})

    def test_no_new_tasks_needs_only_the_original_assignment_basis(self):
        self.store.commit(self.rows, 9)
        restored = InputReceipts(self.adopt({0, 3}, 20))
        self.assertEqual(restored.known, {self.path:7})

    def test_lost_basis_file_replays_hints_instead_of_assuming_success(self):
        self.store.commit(self.rows, 11)
        # Simulate a missing control record in this private fixture only.
        (self.store.bases/(self.store.basis+'.json')).unlink()
        restored = InputReceipts(self.adopt({0, 3, 10, 11}, 20))
        self.assertEqual(restored.replay_groups, 1)

    def test_partial_temporary_publication_is_not_a_group(self):
        (self.store.groups/'.partial.tmp').write_bytes(b'{broken')
        restored = InputReceipts(self.adopt({0, 3}, 20))
        self.assertEqual(restored.known, {})

    def test_checksum_run_binding_and_unsafe_references_fail_visibly(self):
        path = self.store.commit(self.rows, 11)
        value = json.loads(path.read_bytes())
        value['record']['barrier'] = 10
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'checksum'):
            InputReceipts(self.adopt({0, 3, 10, 11}, 20))
        for changes in ({'run_id':'other'}, {'files':[['node-a','../escape.json',7]]},
                        {'files':[['other-node','x.json',7]]},
                        {'files':[['node-a','x.json',True]]}):
            record = copy.deepcopy(value['record']); record.update(changes)
            target = self.store.groups/(uuid.uuid4().hex+'.json')
            publish_unique(target, record)
            with self.assertRaises(ValueError):
                parsed = self.store.read(target, 'group')
                self.store._validate_group(parsed)

    def test_ack_source_must_be_an_exact_candidate_in_this_control(self):
        bad = [(Path('/tmp/node-a/ready/candidate.json'), self.rows[0][1], [], 7)]
        with self.assertRaisesRegex(ValueError, 'escaped'):
            self.store.commit(bad, 11)

    def test_symlink_group_is_rejected_and_existing_record_is_never_replaced(self):
        path = self.store.commit(self.rows, 9)
        before = path.read_bytes()
        with self.assertRaises(FileExistsError):
            publish_unique(path, {'do-not-overwrite':True})
        self.assertEqual(path.read_bytes(), before)
        (self.store.groups/(uuid.uuid4().hex+'.json')).symlink_to(path)
        with self.assertRaises(ValueError):
            InputReceipts(self.adopt({0, 3}, 20))

    def test_fsync_failure_before_publish_keeps_candidate_eligible(self):
        with mock.patch('migration_input_receipts.os.fsync', side_effect=OSError('fixture fsync')):
            with self.assertRaises(OSError):
                self.store.commit(self.rows, 9)
        self.assertEqual(list(self.store.groups.glob('*.json')), [])
        self.assertTrue(self.path.exists())
        restored = InputReceipts(self.adopt({0, 3}, 20))
        self.assertEqual(restored.known, {})

    def test_failure_after_link_is_recoverable_only_with_actual_backing_assignments(self):
        with mock.patch('migration_input_receipts._sync_directory', side_effect=OSError('fixture dir sync')):
            with self.assertRaises(OSError):
                self.store.commit(self.rows, 11)
        self.assertEqual(len(list(self.store.groups.glob('*.json'))),1)
        restored = InputReceipts(self.adopt({0,3,10,11},20))
        self.assertIn(self.path, restored.known)
        missing = InputReceipts(self.adopt({0,3,11},30))
        self.assertNotIn(self.path, missing.known)
        self.assertEqual(missing.replay_groups,1)


if __name__ == '__main__':
    unittest.main()
