import unittest
from experiment_source_limit import restoration


class SourceExperimentRestoreTests(unittest.TestCase):
    def test_restores_only_owned_budget_and_preserves_unrelated_changes(self):
        old=dict(control='/fixture',run_id='fixture',scope='warm-source-reader-limit-v1',processes=32)
        current=dict(old,processes=64,operation_id='own',unrelated='keep')
        restored=restoration(current,old,'own','/fixture','fixture')
        self.assertEqual(restored['processes'],32)
        self.assertEqual(restored['unrelated'],'keep')
        self.assertNotIn('operation_id',restored)
        for change in ({'operation_id':'other'},{'processes':48},{'run_id':'other'},
                       {'control':'/other'},{'scope':'other'}):
            self.assertIsNone(restoration(dict(current,**change),old,'own','/fixture','fixture'))
        self.assertEqual(restoration(old,old,'own','/fixture','fixture'),old)

    def test_smaller_trial_budget_is_owned_by_its_exact_target(self):
        old=dict(control='/fixture',run_id='fixture',scope='warm-source-reader-limit-v1',processes=32)
        current=dict(old,processes=48,operation_id='own')
        self.assertIsNone(restoration(current,old,'own','/fixture','fixture'))
        self.assertEqual(restoration(current,old,'own','/fixture','fixture',48),old)
        self.assertIsNone(restoration(dict(current,processes=64),old,'own','/fixture','fixture',48))


if __name__=='__main__':unittest.main()
