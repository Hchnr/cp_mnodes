import pathlib,tempfile,unittest,threading,sys,json,bisect
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).parent))
from migration_receipt_prefetch import receipt_names,prefetch_one
from migration_io import write_json,read_json
class PrefetchTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=pathlib.Path(self.temp.name)
 def receipt(self,rel,value):
  p=self.root/'stages/p3-r1/done'/rel;p.parent.mkdir(parents=True,exist_ok=True);write_json(p,value);return p
 def test_canonical_order_excludes_snapshots_symlinks(self):
  a=self.receipt('0/10.json',{'version':1});b=self.receipt('0/2.json',{'version':2});c=self.receipt('deep/child/1.json',{'version':3})
  write_json(a,{'version':4})
  external=self.root/'outside';external.mkdir();(external/'7.json').write_text('{}')
  (a.parent/'link').symlink_to(external,target_is_directory=True)
  (a.parent/'link.json').symlink_to(external/'7.json')
  names=receipt_names(self.root,workers=4)
  self.assertEqual(names,sorted(map(str,(a,b,c))))
  self.assertEqual(names[bisect.bisect_right(names,str(a))],str(b))
 def test_latest_snapshot_is_read_without_mutation(self):
  a=self.receipt('0/0.json',{'old':True});write_json(a,{'latest':True});before={p:p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
  captured=[]
  import migration_receipt_prefetch as module
  original=module.read_json
  def traced(path):
   value=original(path);captured.append(value);return value
  with patch.object(module,'read_json',side_effect=traced):self.assertEqual(prefetch_one(str(a),threading.Event()),'read')
  self.assertEqual(captured,[{'latest':True}]);self.assertEqual(before,{p:p.read_bytes() for p in self.root.rglob('*') if p.is_file()})
 def test_cancellation_and_nonregular_never_open_reader(self):
  a=self.receipt('0/0.json',{});e=threading.Event();e.set()
  with patch('migration_receipt_prefetch.read_json') as reader:
   self.assertEqual(prefetch_one(str(a),e),'cancelled');self.assertEqual(prefetch_one(str(a.parent),threading.Event()),'nonregular');reader.assert_not_called()
 def test_guard_failure_stops_enumeration_without_writes(self):
  self.receipt('0/0.json',{});before=sorted(str(p) for p in self.root.rglob('*'))
  def guard():raise RuntimeError('stop requested')
  with self.assertRaisesRegex(RuntimeError,'stop requested'):receipt_names(self.root,2,guard)
  self.assertEqual(before,sorted(str(p) for p in self.root.rglob('*')))
if __name__=='__main__':unittest.main()
