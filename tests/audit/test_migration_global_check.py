"""Completion must require the scanner's post-fsync return footer."""
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]/'src'/'audit'))
import migration_global_check as audit


class CompletionGateTests(unittest.TestCase):
    def test_historical_reference_is_not_a_completion_gate(self):
        row=dict(counts=dict(entries=274_000_000),enumeration_complete=False)
        text=audit.reference_progress(row,445_000_000)
        self.assertIn('61.6%',text)
        self.assertIn('171,000,000',text)
        self.assertIn('非精确完成率',text)
        row['counts']['entries']=500_000_000
        self.assertIn('112.4%',audit.reference_progress(row,445_000_000))
        self.assertFalse(row['enumeration_complete'])

    def test_closed_enumeration_does_not_claim_review_complete(self):
        text=audit.reference_progress(dict(counts=dict(entries=500),enumeration_complete=True),445_000_000)
        self.assertIn('枚举已结束',text)
        self.assertIn('完整性收尾另计',text)
        self.assertNotIn('100%',text)
        for value in (0,-1,True):
            with self.assertRaises(ValueError):audit.reference_progress({},value)

    def view(self, closed, payload=0):
        records=[dict(node='node-'+str(i),lane=0,pid=i,report='/control/'+str(i)) for i in range(48)]
        def read(path):
            if path.name=='launch-results.json':return dict(records=records,errors=[])
            return dict(pid=int(path.parent.name),source='/share/project',target='/share-mixed/project',
                excluded=audit.EXCLUDED,payload_bytes_read=payload,payload_bytes_written=0,
                copy_tasks_published=0,counts={},status='complete',updated=1)
        with patch.object(audit,'read',side_effect=read),patch.object(audit,'returned',return_value=closed):
            return audit.snapshot(pathlib.Path('/control'))

    def test_result_before_footer_not_complete(self):
        self.assertFalse(self.view(False)['enumeration_complete'])

    def test_all_returned_complete(self):
        self.assertTrue(self.view(True)['enumeration_complete'])

    def test_payload_read_rejected(self):
        self.assertFalse(self.view(True,1)['enumeration_complete'])

    def test_raw_partition_rejected(self):
        with self.assertRaises(ValueError):
            audit.validate_partition([dict(rel='',leaves_only=True),dict(rel=audit.EXCLUDED[0],leaves_only=False)])


if __name__=='__main__':unittest.main()
