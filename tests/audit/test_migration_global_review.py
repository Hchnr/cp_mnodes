import pathlib
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'src/audit'))
import migration_global_review as review


class ReviewGates(unittest.TestCase):
    def test_rejects_old_50_tb_authority(self):
        policy=dict(conditional_copy_authorized=True,threshold_bytes_exclusive=50_000_000_000_000,
            source='/share/project',target='/share-mixed/project',exclusions=review.scan.EXCLUDED)
        with patch.object(review.scan,'read',return_value=policy):
            with self.assertRaises(ValueError):review.policy(pathlib.Path('/control'))
            policy['threshold_bytes_exclusive']=20_000_000_000_000
            self.assertEqual(review.policy(pathlib.Path('/control'))['threshold_bytes_exclusive'],review.THRESHOLD)

    def test_equal_counts_with_foreign_paths_rejected(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.executescript('CREATE TABLE candidates(rel BLOB PRIMARY KEY); CREATE TABLE reviewed(rel BLOB PRIMARY KEY); CREATE TABLE review_inputs(slot INTEGER PRIMARY KEY);')
        db.execute('INSERT INTO candidates VALUES(?)',(b'needed',))
        db.execute('INSERT INTO reviewed VALUES(?)',(b'foreign',))
        db.execute('INSERT INTO review_inputs VALUES(0)')
        manifest=dict(records=1,plans=[dict(records=1)])
        with self.assertRaises(ValueError):review.check_coverage(db,manifest)
        db.execute('DELETE FROM reviewed');db.execute('INSERT INTO reviewed VALUES(?)',(b'needed',))
        review.check_coverage(db,manifest)
        db.execute('DELETE FROM review_inputs')
        with self.assertRaises(ValueError):review.check_coverage(db,manifest)

    def test_prepare_requires_all_frontiers_closed(self):
        with patch.object(review.scan,'check_root'),patch.object(review,'policy'),patch.object(
                review.scan,'read',return_value=dict(enumeration_complete=False,proofs=[])):
            with self.assertRaises(ValueError):review.prepare(pathlib.Path('/control'))


if __name__=='__main__':unittest.main()
