import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_reaudit_finish as finish
import migration_reaudit_merge as merge


class MergeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='reaudit-merge-test-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'final').mkdir()
        self.db,_=finish.connect(self.root);self.addCleanup(self.db.close)
        (self.root/'conditional-copy-policy.json').write_text(json.dumps(dict(
            conditional_copy_authorized=True,threshold_bytes_exclusive=50000000000000)))

    def reviewed(self,size=10,kind='file',changed=False,slot=0):
        rel='fixture/a'
        source=dict(size=size,ino=1,mode=33188,uid=0,gid=0,mtime_ns=1,ctime_ns=1,nlink=1)
        observation=dict(rel=rel,kind=kind,source=source,reason='target_absent')
        row=dict(rel=rel,kind=kind,source=source,status='missing',checked_at=100,
                 directory_contents_changed=changed,observation=observation)
        path=self.root/('output-'+str(slot)+'.gz')
        with gzip.open(path,'wt') as out:out.write(json.dumps(row)+'\n')
        counts=dict(processed=1,missing=1)
        counts['missing_'+{'file':'files','directory':'directories','symlink':'symlinks'}[kind]]=1
        if kind=='file':counts['missing_logical_bytes']=size
        if changed:counts['source_directory_changes']=1
        plan=dict(slot=slot,records=1,input_sha256='test-input')
        result=dict(status='complete',counts=counts,input_sha256='test-input',
                    output=str(path),output_sha256=finish.digest(path))
        self.db.execute('INSERT OR IGNORE INTO candidates VALUES(?,?,?,?,?,?,?)',
            (b'fixture/a',b'fixture',kind,json.dumps(observation),1,'fixture','missing'))
        self.db.commit()
        return plan,result

    def test_verified_scope_export_does_not_autostart_copy(self):
        plan,result=self.reviewed()
        self.assertTrue(merge.ingest(self.db,plan,result))
        self.assertFalse(merge.ingest(self.db,plan,result))
        with mock.patch.object(merge,'snapshot',return_value=dict(enumeration_complete=True)):
            out=merge.export(self.root,self.db,dict(records=1,plans=[plan]))
        self.assertEqual(out['missing']['file']['logical_bytes'],10)
        self.assertTrue(out['copy_gates']['below_threshold'])
        self.assertFalse(out['copy_gates']['copy_started'])
        self.assertTrue(out['copy_gates']['final_frontier_and_ownership_audit_pending'])
        with gzip.open(out['outputs']['files']['path'],'rt') as stream:
            row=json.loads(stream.readline())
        self.assertEqual(row['rel'],'fixture/a')

    def test_changed_directory_forces_scoped_followup(self):
        plan,result=self.reviewed(size=4096,kind='directory',changed=True)
        merge.ingest(self.db,plan,result)
        with mock.patch.object(merge,'snapshot',return_value=dict(enumeration_complete=True)):
            out=merge.export(self.root,self.db,dict(records=1,plans=[plan]))
        self.assertEqual(out['copy_gates']['changed_directory_scopes'],1)
        required=json.loads((self.root/'final/followup-required.json').read_text())
        self.assertEqual(required['scopes'],['fixture/a'])

    def test_equal_to_50TB_is_not_below_threshold(self):
        plan,result=self.reviewed(size=50000000000000)
        merge.ingest(self.db,plan,result)
        with mock.patch.object(merge,'snapshot',return_value=dict(enumeration_complete=True)):
            out=merge.export(self.root,self.db,dict(records=1,plans=[plan]))
        self.assertFalse(out['copy_gates']['below_threshold'])

    def test_counter_or_digest_failure_cannot_enter_manifest(self):
        plan,result=self.reviewed();result['output_sha256']='wrong'
        with self.assertRaises(ValueError):merge.ingest(self.db,plan,result)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM reviewed').fetchone()[0],0)

    def test_scope_pruning_does_not_use_unsafe_prefix(self):
        self.assertEqual(merge.minimal_scopes(['a','a/x','aa','aa/x','b/z']),['a','aa','b/z'])


if __name__=='__main__':unittest.main()
