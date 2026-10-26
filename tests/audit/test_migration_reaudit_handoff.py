import gzip
import json
from pathlib import Path
import tempfile
import unittest

from migration_reaudit_handoff import verify_prefix


class PrefixTests(unittest.TestCase):
    def fixture(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        p=Path(tmp.name)/'shard.gz'
        with gzip.open(p,'wt') as f:
            for i in (2,5,9):f.write(json.dumps(dict(id=i,entry=dict(rel='dir/'+str(i),size=0)))+'\n')
        receipts={i:dict(id=i,rel='dir/'+str(i),status='copied',expected_bytes=0) for i in (2,5)}
        state=dict(active=0,status='stopped',submitted=2,counts=dict(processed=2,copied=2,completed_bytes=0,copied_bytes=0))
        return p,receipts,state

    def test_exact_out_of_order_receipts_pass_and_keep_counts(self):
        p,r,s=self.fixture();r=dict(reversed(list(r.items())))
        v=verify_prefix(p,r,s)
        self.assertEqual(v['prefix_tasks'],2);self.assertEqual(v['counts'],s['counts'])
        self.assertTrue(v['post_copy_revalidation_required'])

    def test_unsubmitted_tail_receipt_cannot_replace_prefix(self):
        p,r,s=self.fixture();del r[5];r[9]=dict(id=9,rel='dir/9',status='copied',expected_bytes=0)
        with self.assertRaisesRegex(ValueError,'identity differs'):verify_prefix(p,r,s)

    def test_live_tasks_refuse_handoff(self):
        p,r,s=self.fixture();s['active']=1
        with self.assertRaisesRegex(ValueError,'not drained'):verify_prefix(p,r,s)

    def test_unrecorded_submission_refused(self):
        p,r,s=self.fixture();s['submitted']=3
        with self.assertRaisesRegex(ValueError,'submitted/processed'):verify_prefix(p,r,s)

    def test_counter_drift_refused(self):
        p,r,s=self.fixture();s['counts']['copied']=1
        with self.assertRaisesRegex(ValueError,'cumulative counters'):verify_prefix(p,r,s)

    def test_zero_prefix_does_not_take_first_task(self):
        p,r,s=self.fixture();s.update(submitted=0,counts=dict(processed=0))
        self.assertEqual(verify_prefix(p,{},s)['prefix_tasks'],0)


if __name__=='__main__':unittest.main()
