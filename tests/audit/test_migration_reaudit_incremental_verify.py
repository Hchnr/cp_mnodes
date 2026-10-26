import unittest
from migration_reaudit_incremental_verify import replay
from test_migration_reaudit_incremental import capture,version


class ReplayTests(unittest.TestCase):
    def event(self,rel,children):
        value=capture(rel,children);value['payload_read']=False
        for row in value['views']:
            row['observation']=dict(rel=row['rel'],kind=row['kind'],source=row['source'],reason='target_absent')
        return dict(event='stable_immediate_directory_capture',**value)

    def test_replays_known_seed_even_below_reused_child(self):
        baseline={'a':version(1),'a/b':version(2),'a/b/c':version(3)}
        events=[self.event('a',[('a/b',version(2))]),self.event('a/b/c',[])]
        result=replay(baseline,['a','a/b/c'],events,'a')
        self.assertEqual(result['captured_directories'],2)

    def test_omitted_required_seed_and_forged_root_fail(self):
        event=self.event('a',[])
        with self.assertRaises(ValueError):replay({'a':version(1),'a/seed':version(2)},['a','a/seed'],[event],'a')
        event['after']=version(999)
        with self.assertRaises(ValueError):replay({'a':version(1)},['a'],[event],'a')

    def test_duplicate_named_child_rejected(self):
        event=self.event('a',[]);event['names']=['x','x']
        with self.assertRaises(ValueError):replay({'a':version(1)},['a'],[event],'a')


if __name__=='__main__':unittest.main()
