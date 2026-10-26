from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os
import stat
import tempfile
import unittest
from unittest import mock

import migration_reaudit_capture as capture


class FakeScan:
    @staticmethod
    def parts(rel):
        if any(x in ('','.','..') for x in rel.split('/')):raise ValueError('unsafe')
    @staticmethod
    def excluded(rel):return rel=='eai_pwm/datasets/raw' or rel.startswith('eai_pwm/datasets/raw/')
    @staticmethod
    def directory(root,rel):return os.open(Path(root)/rel,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    @staticmethod
    def fields(st):return dict(ino=st.st_ino,mode=st.st_mode,mtime_ns=st.st_mtime_ns,ctime_ns=st.st_ctime_ns,size=st.st_size)
    @staticmethod
    def kind(st):return 'directory' if stat.S_ISDIR(st.st_mode) else 'file'


class FakeReview:
    @staticmethod
    def inspect_group(task,source,target,pool):
        result=[]
        for row in task['rows']:
            st=os.stat(Path(source)/row['rel'],follow_symlinks=False)
            result.append(dict(rel=row['rel'],source=row.get('source',FakeScan.fields(st)),
                               kind=row.get('kind',FakeScan.kind(st)),status='missing'))
        return result


class CaptureTests(unittest.TestCase):
    def test_recursive_capture_visits_child_subtrees_instead_of_silently_skipping(self):
        with tempfile.TemporaryDirectory() as directory:
            r=Path(directory);(r/'a/b').mkdir(parents=True);(r/'a/b/f').write_text('data')
            rows,proofs=capture.capture_tree(FakeScan,FakeReview,r,r,'a')
            self.assertEqual(set(rows),{'a','a/b','a/b/f'})
            self.assertEqual({p['rel'] for p in proofs},{'a','a/b'})
            self.assertTrue(all(not p['payload_read'] for p in proofs))

    def test_exclusion_and_entry_limit_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            r=Path(directory);(r/'a').mkdir();(r/'a/f').touch()
            with self.assertRaises(ValueError):capture.capture_directory(FakeScan,FakeReview,r,r,'eai_pwm/datasets/raw')
            with self.assertRaises(ValueError):capture.capture_directory(FakeScan,FakeReview,r,r,'a',limit=0)

    def test_comparison_error_cannot_be_declared_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            r=Path(directory);(r/'a').mkdir()
            with mock.patch.object(FakeReview,'inspect_group',return_value=[dict(status='error')]):
                with self.assertRaises(RuntimeError):capture.capture_directory(FakeScan,FakeReview,r,r,'a',attempts=1)

    def test_replaced_source_directory_path_cannot_validate_an_old_open_fd(self):
        with tempfile.TemporaryDirectory() as directory:
            r=Path(directory);(r/'a').mkdir()
            original=FakeReview.inspect_group
            def replace(task,source,target,pool):
                result=original(task,source,target,pool)
                if task['rows'][0]['rel']=='a':
                    (r/'a').rename(r/'old-a');(r/'a').mkdir()
                return result
            with mock.patch.object(FakeReview,'inspect_group',side_effect=replace):
                with self.assertRaises(RuntimeError):capture.capture_directory(FakeScan,FakeReview,r,r,'a',attempts=1)

    def test_disappeared_child_is_recorded_not_silently_skipped(self):
        class Gone:
            @staticmethod
            def inspect_group(task,*args):return [dict(rel=task['rows'][0]['rel'],status='source_disappeared')]
        with mock.patch.object(capture,'capture_directory',side_effect=FileNotFoundError):
            rows,proofs=capture.capture_tree(FakeScan,Gone,'/fixture','/target','a')
        self.assertEqual(rows['a']['status'],'source_disappeared')
        self.assertEqual(proofs[0]['status'],'source_directory_disappeared')


if __name__=='__main__':unittest.main()
