import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

import migration_reaudit_copy as copy


class CopyTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='reaudit-copy-test-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.source=self.root/'source';self.target=self.root/'target'
        self.source.mkdir();self.target.mkdir()
        self.cfg=dict(source=str(self.source),target=str(self.target),run_id='fixture')

    def task(self,kind='directory'):
        source=self.source/'a'
        if kind=='directory':source.mkdir(mode=0o750)
        else:source.symlink_to('somewhere/else')
        identity=copy.base.engine._record(source.lstat())
        row=dict(rel='a',kind=kind,source=identity,reason='target_absent')
        if kind=='symlink':row['link_target']='somewhere/else'
        return dict(id=1,entry=copy.namespace_entry(row))

    def test_authorization_required_before_preparation(self):
        with self.assertRaises(FileNotFoundError):copy.authority(self.root)
        self.assertFalse((self.root/'final').exists())

    def test_directory_capacity_is_not_inode_size(self):
        task=self.task()
        self.assertEqual(task['entry']['size'],0)
        self.assertIn('size',task['entry']['identity'])
        result=copy.namespace_one(self.cfg,task)
        self.assertEqual(result['status'],'copied')
        self.assertTrue((self.target/'a').is_dir())
        self.assertEqual(result['bytes'],0)
        self.assertEqual(stat.S_IMODE((self.target/'a').stat().st_mode),0o750)
        resumed=copy.namespace_one(self.cfg,task,result)
        self.assertTrue(resumed['resumed'])

    def test_existing_directory_metadata_is_not_overwritten(self):
        task=self.task();(self.target/'a').mkdir(mode=0o700)
        result=copy.namespace_one(self.cfg,task)
        self.assertEqual(result['status'],'skipped')
        self.assertTrue(result['existing_metadata_untouched'])
        self.assertEqual(stat.S_IMODE((self.target/'a').stat().st_mode),0o700)

    def test_symlink_copy_and_receipt_resume(self):
        task=self.task('symlink')
        result=copy.namespace_one(self.cfg,task)
        self.assertEqual(result['status'],'copied')
        self.assertEqual(os.readlink(self.target/'a'),'somewhere/else')
        self.assertTrue(copy.namespace_one(self.cfg,task,result)['resumed'])

    def test_symlink_conflict_is_not_overwritten(self):
        task=self.task('symlink');(self.target/'a').symlink_to('keep-me')
        result=copy.namespace_one(self.cfg,task)
        self.assertEqual(result['status'],'conflict')
        self.assertEqual(os.readlink(self.target/'a'),'keep-me')

    def test_changed_source_directory_is_not_created(self):
        task=self.task();(self.source/'a/new-child').write_text('new')
        result=copy.namespace_one(self.cfg,task)
        self.assertEqual(result['status'],'changed')
        self.assertFalse((self.target/'a').exists())

    def test_destination_symlink_cannot_redirect_directory_creation(self):
        (self.source/'parent').mkdir();(self.source/'parent/a').mkdir()
        outside=self.root/'outside';outside.mkdir();(self.target/'parent').symlink_to(outside,target_is_directory=True)
        source=self.source/'parent/a';identity=copy.base.engine._record(source.stat())
        entry=copy.namespace_entry(dict(rel='parent/a',kind='directory',source=identity,reason='target_absent'))
        result=copy.namespace_one(self.cfg,dict(id=1,entry=entry))
        self.assertIn(result['status'],('error','conflict'))
        self.assertFalse(list(outside.iterdir()))

    def test_raw_ordinary_files_and_unsigned_links_rejected(self):
        task=self.task()
        identity=task['entry']['identity']
        for row in [dict(rel='eai_pwm/datasets/raw/a',kind='directory',source=identity,reason='target_absent'),
                    dict(rel='a',kind='file',source=identity,reason='target_absent')]:
            with self.assertRaises(ValueError):copy.namespace_entry(row)


if __name__=='__main__':unittest.main()
