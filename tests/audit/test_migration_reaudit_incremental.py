import unittest
from migration_reaudit_incremental import walk


def version(ino):return dict(ino=ino,mode=16877,mtime_ns=1,ctime_ns=1)


def capture(rel,children):
    views=[dict(rel=rel,kind='directory',source=version(1),status='missing')]
    for child,identity in children:views.append(dict(rel=child,kind='directory',source=identity,status='missing'))
    return dict(rel=rel,after=version(1),before=version(1),names=[c.rpartition('/')[2] for c,_ in children],
                children=[c for c,_ in children],views=views)


class IncrementalTests(unittest.TestCase):
    def execute(self,baseline,seeds,values):
        called=[];events=[]
        def visit(rel):called.append(rel);return values[rel]
        result=walk(baseline,seeds,visit,lambda rel:dict(rel=rel,status='source_disappeared'),events.append,lambda *args:None)
        return result,called,events

    def test_unchanged_child_reused_but_explicit_deep_seed_still_checked(self):
        baseline={'a':version(1),'a/b':version(2),'a/b/c':version(3)}
        values={'a':capture('a',[('a/b',version(2))]),'a/b/c':capture('a/b/c',[])}
        result,called,_=self.execute(baseline,['a','a/b/c'],values)
        self.assertEqual(called,['a','a/b/c']);self.assertEqual(result['pending_directories'],0)

    def test_changed_or_unknown_child_traversed(self):
        values={'a':capture('a',[('a/b',version(9)),('a/new',version(10))]),
                'a/b':capture('a/b',[]),'a/new':capture('a/new',[])}
        _,called,_=self.execute({'a':version(1),'a/b':version(2)},['a'],values)
        self.assertEqual(called,['a','a/b','a/new'])

    def test_removed_branch_has_explicit_absence_proof(self):
        result,_,events=self.execute({'a':version(1),'a/gone':version(2)},['a'],{'a':capture('a',[])})
        self.assertEqual(result['pruned_source_subtrees'],1)
        self.assertEqual(events[-1]['observation']['status'],'source_disappeared')

    def test_capture_cannot_omit_a_named_child(self):
        value=capture('a',[]);value['names']=['missing']
        with self.assertRaises(ValueError):self.execute({'a':version(1)},['a'],{'a':value})


if __name__=='__main__':unittest.main()
