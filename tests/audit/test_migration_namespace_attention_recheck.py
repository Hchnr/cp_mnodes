from pathlib import Path
import tempfile
import unittest

import migration_manifest_copy as base
import migration_namespace_attention_recheck as check


class RecheckTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name)
        self.source=self.root/'source';self.target=self.root/'target';self.source.mkdir();self.target.mkdir()
        (self.source/'dir').mkdir();self.task=dict(id=1,entry=dict(rel='dir',kind='directory',
            identity=base.engine._record((self.source/'dir').stat())))
        self.cfg=dict(source=str(self.source),target=str(self.target))

    def test_existing_directory_is_not_changed_or_called_complete(self):
        (self.target/'dir').mkdir();before=(self.target/'dir').stat()
        row=check.inspect(base,self.cfg,self.task)
        self.assertEqual(row['status'],'namespace_path_present')
        self.assertFalse(row['subtree_completeness_proven'])
        self.assertEqual(before,(self.target/'dir').stat())

    def test_absent_directory_is_not_created(self):
        row=check.inspect(base,self.cfg,self.task)
        self.assertEqual(row['status'],'path_unavailable')
        self.assertFalse((self.target/'dir').exists())

    def test_symlink_directory_not_followed(self):
        outside=self.root/'outside';outside.mkdir();(self.target/'dir').symlink_to(outside)
        row=check.inspect(base,self.cfg,self.task)
        self.assertEqual(row['status'],'namespace_type_or_link_conflict')
        self.assertTrue((self.target/'dir').is_symlink())


if __name__=='__main__':unittest.main()
