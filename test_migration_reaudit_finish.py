import collections
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_reaudit_finish as finish
import migration_reaudit_shared as shared


class FinishTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='reaudit-finish-test-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.db,self.cache=finish.connect(self.root)
        self.addCleanup(self.db.close)
        self.report=self.root/'report';self.report.mkdir()
        self.record=dict(instance='fixture',report_directory=str(self.report),
                         log_file=str(self.root/'scan.log'),identity=dict(pid=1234))

    def write_input(self,category,rows):
        p=self.report/(category+'.jsonl')
        p.write_text(''.join(json.dumps(r)+'\n' for r in rows))
        return p

    def result(self,**counts):
        return dict(status='complete',updated=123.5,counts=counts,
            pid=1234,source='/share/project',target='/share-mixed/project',
            excluded=['eai_pwm/datasets/raw'],pending_directories=0,active_directories=0,
            payload_bytes_read=0,payload_bytes_written=0)

    def test_index_unique_missing_and_idempotent_closed_input(self):
        row=dict(rel='a/file',kind='file',source=dict(size=17),reason='target_absent')
        self.write_input('missing',[row])
        result=self.result(missing_files=1,missing_logical_bytes=17)
        self.assertTrue(finish.ingest(self.db,self.record,'missing',result))
        self.assertFalse(finish.ingest(self.db,self.record,'missing',result))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT rows FROM inputs').fetchone()[0],1)

    def test_bad_counters_do_not_commit_candidates(self):
        self.write_input('missing',[dict(rel='a/file',kind='file',source=dict(size=17))])
        with self.assertRaises(ValueError):finish.ingest(self.db,self.record,'missing',self.result(missing_files=2))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0],0)

    def test_partial_closed_input_rejected(self):
        (self.report/'missing.jsonl').write_bytes(b'{"rel":"a/file"}')
        with self.assertRaises(ValueError):finish.ingest(self.db,self.record,'missing',self.result(missing_files=1))

    def test_raw_and_escape_rejected(self):
        for rel in ('eai_pwm/datasets/raw/a','/a','../a','a/../b','a//b','a/./b','a\x00b'):
            with self.subTest(rel=rel),self.assertRaises(ValueError):finish.safe_rel(rel)
        self.assertEqual(finish.safe_rel('other/raw/a'),b'other/raw/a')

    def test_source_exceptions_remain_candidates(self):
        rows=[dict(rel='a/d',operation='source_directory_changed_during_enumeration'),
              dict(rel='a/socket',reason='unsupported_source_type',source=dict(size=0))]
        self.write_input('errors',rows)
        finish.ingest(self.db,self.record,'errors',self.result(changed_source_directories=1,unsupported=1))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM exceptions').fetchone()[0],2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0],2)

    def test_footer_required_for_return_proof(self):
        result=self.result()
        self.assertFalse(shared.returned(self.record,result))
        Path(self.record['log_file']).write_text(json.dumps(result)+'\n')
        self.assertTrue(shared.returned(self.record,result))
        other=dict(result,updated=124)
        self.assertFalse(shared.returned(self.record,other))

    def test_shared_snapshot_keeps_counts_without_ssh(self):
        result=self.result(entries=10,files=7)
        (self.report/'result.json').write_text(json.dumps(result))
        Path(self.record['log_file']).write_text(json.dumps(result)+'\n')
        record=dict(self.record,node='master-0',directory='/tmp/fixture')
        (self.root/'launch-results.json').write_text(json.dumps(dict(nodes=[record],transfers_pending=False)))
        value=shared.snapshot(self.root)
        self.assertEqual(value['counts']['entries'],10)
        self.assertTrue(value['enumeration_complete'])
        self.assertFalse(value['remote_process_states_observed'])

    def test_control_read_pins_and_cleans_its_own_link(self):
        path=self.root/'progress.json';path.write_text('{"value":123}')
        self.assertEqual(shared.read(path),{'value':123})
        self.assertEqual(list((self.root/'.read-pins').iterdir()),[])
        self.assertEqual(path.stat().st_nlink,1)

    def test_control_reader_refuses_dataset_paths(self):
        for path in ['/share/project/eai_pwm/anything.json','/share-mixed/project/eai_pwm/anything.json']:
            with self.assertRaises(ValueError):shared.read(path)

    def test_replacement_during_read_keeps_old_inode_named(self):
        path=self.root/'moving.json';path.write_text('{"epoch":1}')
        original=Path.read_text
        replaced=[False]
        def racing_read(pin,*args,**kwargs):
            if pin.parent.name=='.read-pins' and not replaced[0]:
                replacement=self.root/'new.json';replacement.write_text('{"epoch":2}')
                replacement.replace(path);replaced[0]=True
                self.assertEqual(pin.stat().st_nlink,1)
            return original(pin,*args,**kwargs)
        with mock.patch.object(Path,'read_text',racing_read):
            self.assertEqual(shared.read(path),{'epoch':1})
        self.assertEqual(shared.read(path),{'epoch':2})


if __name__=='__main__':unittest.main()
