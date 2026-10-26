import sqlite3
import unittest

from migration_reaudit_followup import minimal,scopes_from_db,selected_rows


class FollowupTests(unittest.TestCase):
    def test_prefix_interleaving_does_not_duplicate_recursive_ownership(self):
        self.assertEqual(minimal(['a','a-','a/b','a-/b','aa/x']),['a','a-','aa/x'])

    def test_raw_scope_rejected(self):
        with self.assertRaises(ValueError):minimal(['eai_pwm/datasets/raw/x'])

    def test_binary_subtree_selection_includes_non_utf8_and_excludes_siblings(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.execute('CREATE TABLE reviewed(rel BLOB PRIMARY KEY,raw TEXT)')
        paths=[b'a',b'a/x',b'a/\xff',b'aa/x',b'a-/x',b'a0/x']
        db.executemany('INSERT INTO reviewed VALUES(?,?)',[(r,'{}') for r in paths])
        self.assertEqual([r[0] for r in selected_rows(db,'a')],[b'a',b'a/x',b'a/\xff'])

    def test_closed_review_gate_and_enumeration_errors(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.executescript('CREATE TABLE review_inputs(slot);CREATE TABLE reviewed(rel,directory_changed);CREATE TABLE exceptions(rel,operation);')
        with self.assertRaises(ValueError):scopes_from_db(db)
        db.executemany('INSERT INTO review_inputs VALUES(?)',[(i,) for i in range(24)])
        db.execute('INSERT INTO reviewed VALUES(?,1)',(b'a/b',))
        db.executemany('INSERT INTO exceptions VALUES(?,?)',[(b'a','source_directory_changed_during_enumeration'),(b'z','unsupported_source_type')])
        self.assertEqual(scopes_from_db(db),['a'])


if __name__=='__main__':unittest.main()
