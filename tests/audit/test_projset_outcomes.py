import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

REPO=Path(__file__).resolve().parents[2]
SOURCE=REPO/'ops/diagnostics/projset_outcomes.py'
if not SOURCE.is_file():
    SOURCE=Path(__file__).with_name('projset_outcomes.py')
spec=importlib.util.spec_from_file_location('outcomes',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class OutcomeTests(unittest.TestCase):
    def test_incremental_prefix_export_and_torn_catalog_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);run=root/'run';out=root/'out';src=root/'src';dst=root/'dst'
            (run/'nodes/n1').mkdir(parents=True);src.mkdir();dst.mkdir()
            original=(m.SOURCE,m.TARGET);m.SOURCE,m.TARGET=str(src),str(dst)
            try:
                (src/'conflict').write_text('new');(dst/'conflict').write_text('keep!!!')
                (src/'link').symlink_to('new');(dst/'link').symlink_to('old')
                records=[dict(status='copied',kind='file',rel='copied',logical_bytes=99),
                    dict(status='skipped',kind='file',rel='same',logical_bytes=10**12,sha256='proof'),
                    dict(status='conflict',kind='file',rel='conflict',logical_bytes=3,detail='different size'),
                    dict(status='conflict',kind='symlink',rel='link',logical_bytes=3,detail='link mismatch'),
                    dict(status='error',kind='file',rel='missing',logical_bytes=20,detail='Stale file handle')]
                p=run/'nodes/n1/receipts.jsonl';prefix=''.join(json.dumps(r,separators=(',',':'))+'\n' for r in records)
                partial=json.dumps(dict(status='changed',kind='file',rel='later',logical_bytes=5),separators=(',',':'))
                p.write_text(prefix+partial[:20]);(run/'status.json').write_text(json.dumps(dict(counts={},updated=1)))
                m.export(run,out);s=json.loads((out/'summary.json').read_text())
                self.assertEqual(s['statuses']['skipped']['recorded_source_bytes'],10**12)
                self.assertEqual(s['statuses']['conflict']['records'],2)
                self.assertNotIn('copied',s['statuses']);self.assertNotIn('changed',s['statuses'])
                self.assertEqual(json.loads((out/'checkpoint.json').read_text())['offsets']['n1']['offset'],len(prefix))
                reconciled=json.loads((out/'reconciliation/summary.json').read_text())
                self.assertEqual(reconciled['categories'], {'unclassified': 1})
                self.assertEqual(reconciled['attention_records'], 1)
                self.assertIn('reconciliation/REPORT.md', (out/'REPORT.md').read_text())
                import csv
                with (out/'conflict-paths.csv').open(encoding='utf-8-sig') as f:rows=list(csv.DictReader(f))
                self.assertEqual(rows[0]['source_current_bytes'],'3');self.assertEqual(rows[0]['target_current_bytes'],'7')
                self.assertEqual(rows[1]['source_link_target'],'new');self.assertEqual(rows[1]['target_link_target'],'old')
                with (out/'selected-receipts.jsonl').open('ab') as f:f.write(b'{"torn":')
                with p.open('a') as f:f.write(partial[20:]+'\n')
                m.export(run,out);m.export(run,out);s=json.loads((out/'summary.json').read_text())
                self.assertEqual(s['statuses']['skipped']['records'],1)
                self.assertEqual(s['statuses']['changed']['records'],1)
                self.assertEqual(s['new_selected_records'],0)
                changed=json.loads((out/'changed-reconciliation/summary.json').read_text())
                self.assertEqual(changed['attention_records'],1)
                self.assertIn('changed-reconciliation/REPORT.md', (out/'REPORT.md').read_text())
                self.assertEqual((dst/'conflict').read_text(),'keep!!!')
                self.assertFalse(s['automatic_retry'])
            finally:m.SOURCE,m.TARGET=original

    def test_attempt_receipts_with_same_offset_do_not_collide(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);run=root/'run';out=root/'out'
            (run/'nodes/n1/attempt-001').mkdir(parents=True)
            (run/'nodes/n1/attempt-002').mkdir()
            (run/'status.json').write_text(json.dumps(dict(counts={},updated=1)))
            for name in ('receipts.jsonl','attempt-001/receipts.jsonl','attempt-002/receipts.jsonl'):
                (run/'nodes/n1'/name).write_text(json.dumps(dict(status='skipped',kind='file',rel=name,logical_bytes=10),separators=(',',':'))+'\n')
            m.export(run,out);m.export(run,out)
            summary=json.loads((out/'summary.json').read_text())
            self.assertEqual(3,summary['statuses']['skipped']['records'])
            self.assertEqual(30,summary['statuses']['skipped']['recorded_source_bytes'])
            self.assertEqual(0,summary['new_selected_records'])

    def test_reconciliation_failure_does_not_stop_original_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);run=root/'run';out=root/'out';(run/'nodes/n1').mkdir(parents=True)
            (run/'nodes/n1/receipts.jsonl').write_text('')
            (run/'status.json').write_text(json.dumps(dict(counts={},updated=1)))
            with mock.patch.object(m.importlib.util,'spec_from_file_location',side_effect=OSError('fixture')):
                m.export(run,out)
            self.assertTrue((out/'summary.json').exists())
            self.assertTrue((out/'error-paths.csv').exists())
            self.assertEqual(json.loads((out/'reconciliation-health.json').read_text())['state'],'error')
            self.assertEqual(json.loads((out/'changed-reconciliation-health.json').read_text())['state'],'error')


if __name__=='__main__':unittest.main()
