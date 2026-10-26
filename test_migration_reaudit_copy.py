import json
import gzip
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

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

    def test_explicit_start_is_bound_to_request_and_does_not_claim_full_completion(self):
        final=self.root/'final';final.mkdir()
        request=self.root/'explicit-copy-request.json'
        request.write_text(json.dumps(dict(directive='开始拷贝吧，开大并发尽快完成')))
        outputs={k:dict(sha256=k) for k in ('files','directories','symlinks')}
        report=dict(status='verified_manifest_scope_only',source='/share/project',target='/share-mixed/project',
                    exclusions=['eai_pwm/datasets/raw'],whole_tree_completion_proven=False,
                    missing=dict(file=dict(paths=1,logical_bytes=10)),outputs=outputs)
        path=final/'final-report.json';path.write_text(json.dumps(report))
        auth=dict(kind='explicit-user-verified-scope-copy-v1',threshold_bytes_exclusive=copy.THRESHOLD,
                  final_report_sha256=copy.base.digest(path),unresolved_access_errors=0,
                  verified_path_manifest_only=True,explicit_user_request_sha256=copy.base.digest(request),
                  ordinary_files=report['missing']['file'],outputs=outputs)
        authority=final/'conditional-copy-authorization.json';authority.write_text(json.dumps(auth))
        self.assertEqual(copy.authority(self.root)[2]['status'],'verified_manifest_scope_only')
        request.write_text(json.dumps(dict(directive='not the authorized request')))
        with self.assertRaises(ValueError):copy.authority(self.root)

    def test_conditional_copy_cannot_use_partial_scope_status(self):
        final=self.root/'final';final.mkdir()
        report=final/'final-report.json';report.write_text(json.dumps(dict(status='verified_manifest_scope_only')))
        (final/'conditional-copy-authorization.json').write_text(json.dumps(dict(
            kind='full-reaudit-conditional-copy-v1',threshold_bytes_exclusive=copy.THRESHOLD,
            final_report_sha256=copy.base.digest(report))))
        with self.assertRaises(ValueError):copy.authority(self.root)

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

    def test_namespace_checkpoint_carries_counts_without_recreating_old_directory(self):
        tasks=[]
        for i,name in enumerate(('first','second')):
            path=self.source/name;path.mkdir()
            row=dict(rel=name,kind='directory',source=copy.base.engine._record(path.stat()),reason='target_absent')
            tasks.append(dict(id=i,entry=copy.namespace_entry(row)))
        receipt=copy.namespace_one(self.cfg,tasks[0]);before=(self.target/'first').stat()
        control=self.root/'checkpoint';folder=control/'nodes/node';folder.mkdir(parents=True)
        shard=control/'shard.gz'
        with gzip.open(shard,'wt') as stream:
            for task in tasks:stream.write(json.dumps(task)+'\n')
        (folder/'receipts-old.jsonl').write_text(json.dumps(receipt)+'\n')
        cfg=dict(self.cfg,run_id='namespace-checkpoint-'+self.root.name,threads=1,
                 shards=dict(node=dict(path=str(shard),files=2,bytes=0)))
        (control/'config.json').write_text(json.dumps(cfg))
        checkpoint=dict(status='stopped',active=0,submitted=1,
                        counts=dict(processed=1,copied=1,completed_bytes=0,copied_bytes=0))
        with mock.patch.object(copy.base,'validate_worker'),mock.patch.object(copy.base.signal,'signal'), \
                mock.patch.object(copy.base,'validate_row',copy.validate_namespace_task), \
                mock.patch.object(copy.base,'copy_one',wraps=copy.namespace_one) as worker:
            copy.base.worker(control,'node',checkpoint=checkpoint)
        self.assertEqual([call.args[1]['id'] for call in worker.call_args_list],[1])
        final=copy.base.read_json(folder/'progress.json')
        self.assertEqual(final['counts']['processed'],2)
        self.assertEqual(final['counts']['copied'],2)
        self.assertEqual((self.target/'first').stat().st_ctime_ns,before.st_ctime_ns)
        self.assertTrue((self.target/'second').is_dir())

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
