import json,os,pathlib,tempfile,unittest
import migration_priority4_scan as p4
import migration_scan as original

class Priority4ScanTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.base=pathlib.Path(self.temp.name).resolve();self.src=self.base/'source';self.src.mkdir();self.out=self.base/'manifest.jsonl';self.emitted=[]
 def run_scan(self,**kwargs):
  result=p4.scan_phase(self.src,4,lambda batch:self.emitted.extend(batch),self.out,workers=4,batch_files=2,queue_size=2,**kwargs)
  rows=[json.loads(line) for line in self.out.read_text().splitlines()];return result,rows
 def put(self,path,value='sample'):
  p=self.src/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(value);return p
 def test_exact_top_level_exclusion_keeps_same_names_elsewhere(self):
  self.put('eai_pwm/private/do_not_list');self.put('home/hcr/own');self.put('repos/cosmos-framework-pwm/own');self.put('other/eai_pwm/keep');self.put('.hidden/keep')
  result,rows=self.run_scan();names={e['rel'] for e in rows};self.assertFalse(any(r=='eai_pwm' or r.startswith('eai_pwm/') for r in names));self.assertTrue({'home/hcr/own','repos/cosmos-framework-pwm/own','other/eai_pwm/keep','.hidden/keep'}<=names);self.assertEqual(result['phase'],4);self.assertEqual(result['files'],4)
 def test_symlink_is_recorded_without_following(self):
  self.put('eai_pwm/private');os.symlink('eai_pwm',self.src/'alias');result,rows=self.run_scan();alias=next(r for r in rows if r['rel']=='alias');self.assertEqual(alias['kind'],'symlink');self.assertEqual(alias['target'],'eai_pwm');self.assertEqual(result['files'],0)
 def test_hardlink_and_empty_directory_are_preserved_as_metadata(self):
  a=self.put('items/a','payload');os.link(a,self.src/'items/b');(self.src/'empty').mkdir();result,rows=self.run_scan();files=[r for r in rows if r.get('kind')=='file'];self.assertEqual(result['hardlinks'],1);self.assertEqual(len(self.emitted),1);self.assertTrue(any(r['rel']=='empty' and r['kind']=='directory' for r in rows));self.assertEqual(a.read_text(),'payload');self.assertEqual(len(files),2)
 def test_original_scope_constants_are_unchanged(self):
  self.assertEqual(original.PHASE_PATHS,{1:'home/hcr',2:'repos/cosmos-framework-pwm',3:''});self.assertEqual(original.PRIORITY_SUBTREES,frozenset(['home/hcr','repos/cosmos-framework-pwm']))
 def test_other_phases_rejected_before_manifest_creation(self):
  for phase in [1,2,3,5]:
   with self.assertRaises(ValueError):p4.scan_phase(self.src,phase,lambda _:None,self.out)
  self.assertFalse(self.out.exists())
 def test_resume_with_eai_covered_tree_rejected(self):
  with self.assertRaisesRegex(ValueError,'covered eai_pwm'):
   self.run_scan(resume_records=iter([{'rel':'eai_pwm/data','kind':'file'}]))
 def test_unsafe_resume_path_is_rejected(self):
  for i,path in enumerate(['../outside','/absolute','a/../b']):
   self.out=self.base/('manifest'+str(i)+'.jsonl')
   with self.assertRaises(ValueError):self.run_scan(resume_records=iter([{'rel':path,'kind':'file'}]))
 def test_stop_does_not_emit_payload(self):
  self.put('keep');control=self.base/'control';control.mkdir();(control/'STOP').touch()
  with self.assertRaises(p4.ScanCancelled):self.run_scan(control=control)
  self.assertEqual(self.emitted,[])
 def test_manifest_inside_source_is_rejected(self):
  self.out=self.src/'unsafe.jsonl'
  with self.assertRaisesRegex(ValueError,'outside source_root'):self.run_scan()
 def test_resume_combined_manifest_does_not_duplicate_or_reemit_old_file(self):
  self.put('old');self.put('new');_,rows=self.run_scan();prior=[r for r in rows if r.get('rel')!='new'];self.out=self.base/'combined.jsonl';self.emitted=[];result,combined=self.run_scan(resume_records=iter(prior));self.assertEqual(sum(r.get('rel')=='old' for r in combined),1);self.assertEqual({r['rel'] for r in self.emitted},{'new'});self.assertEqual(result['files'],2)

if __name__=='__main__':unittest.main()
