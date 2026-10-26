import unittest
from unittest import mock
from migration_source_tuning import SourceLimit


class SourceTuningTests(unittest.TestCase):
    def test_p4_scope_is_explicit_and_does_not_change_the_p3_default(self):
        limit=SourceLimit('/tmp/private-fixture.json','/p4','p4',16,initial=8,scope='priority4-source-reader-limit-v1')
        value=dict(scope='priority4-source-reader-limit-v1',control='/p4',run_id='p4',processes=12)
        with mock.patch.object(limit.cache,'read',return_value=value):self.assertEqual(limit(),12)
        with mock.patch.object(limit.cache,'read',return_value=dict(value,scope='warm-source-reader-limit-v1')):
            self.assertEqual(limit(),12);self.assertIsNotNone(limit.error)
    def test_only_current_control_budget_is_accepted_and_errors_keep_last_limit(self):
        limit=SourceLimit('/tmp/private-fixture.json','/fixture','fixture',64)
        valid=dict(scope='warm-source-reader-limit-v1',control='/fixture',run_id='fixture',processes=32)
        with mock.patch.object(limit.cache,'read',return_value=valid):
            self.assertEqual(limit(),32)
        for change in ({'control':'/other'},{'run_id':'other'},{'scope':'other'},
                       {'processes':True},{'processes':0},{'processes':65}):
            with mock.patch.object(limit.cache,'read',return_value=dict(valid,**change)):
                self.assertEqual(limit(),32);self.assertIsNotNone(limit.error)
        with mock.patch.object(limit.cache,'read',side_effect=OSError('blocked refresher')):
            self.assertEqual(limit(),32)


if __name__=='__main__':unittest.main()
