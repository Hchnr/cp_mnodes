import csv
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'src/core'))
import migration_linux_open as native

spec = importlib.util.spec_from_file_location(
    'projset_changed_reconciliation', REPO / 'ops/diagnostics/projset_changed_reconciliation.py')
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class ChangedReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.run = root / 'run'
        self.outcomes = root / 'outcomes'
        self.output = root / 'report'
        self.source, self.target = root / 'source', root / 'target'
        self.batch = self.run / 'recovery' / subject.BATCH
        for folder in (self.batch, self.outcomes, self.source, self.target):
            folder.mkdir(parents=True)
        (self.source / 'a').write_bytes(b'hello')
        (self.target / 'a').write_bytes(b'hello')
        src, dst = (self.source / 'a').stat(), (self.target / 'a').stat()
        source = {key: getattr(src, 'st_' + key) for key in subject.IDENTITY}
        target = {key: getattr(dst, 'st_' + key) for key in subject.IDENTITY}
        checksum = hashlib.sha256(b'hello').hexdigest()
        manifest_raw = json.dumps({'entries': [dict(source, rel='a', kind='file')]}).encode()
        receipt = {'rel': 'a', 'status': 'copied', 'logical_bytes': 5, 'source_identity': source,
                   'dst_stat': target, 'sha256': checksum, 'finished': time.time() - 10}
        receipts_raw = (json.dumps(receipt) + '\n').encode()
        verification = {'summary': {'manifest_sha256': subject.digest(manifest_raw),
                                    'retry_state': 'finished', 'total': 1, 'verified': 1, 'failures': 0},
                        'files': [{'rel': 'a', 'ok': True, 'result': 'same_sha256',
                                   'hash_passes': 2, 'sha256': checksum, 'size': 5}]}
        verification_raw = json.dumps(verification).encode()
        (self.batch / 'manifest.json').write_bytes(manifest_raw)
        (self.batch / 'receipts.jsonl').write_bytes(receipts_raw)
        (self.batch / 'independent-verification.json').write_bytes(verification_raw)
        (self.batch / 'selection.json').write_text(json.dumps({'manifest_sha256': subject.digest(manifest_raw)}))
        (self.batch / 'status.json').write_text(json.dumps({'state': 'finished', 'counts': {'copied': 1}}))
        (self.batch / 'resolution.json').write_text(json.dumps({
            'state': 'verified', 'files': 1, 'manifest_sha256': subject.digest(manifest_raw),
            'receipt_sha256': subject.digest(receipts_raw),
            'independent_verification_sha256': subject.digest(verification_raw)}))
        (self.run / 'status.json').write_text(json.dumps({'counts': {'changed': 1}}))
        self.row = {'relative_path': 'a', 'kind': 'file', 'result_time_beijing':
                    datetime.datetime.fromtimestamp(time.time() - 100, datetime.timezone.utc).isoformat()}
        self.write_rows([self.row])

    def write_rows(self, rows):
        with (self.outcomes / 'changed-paths.csv').open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['relative_path', 'kind', 'result_time_beijing'])
            writer.writeheader()
            writer.writerows(rows)

    def reconcile(self):
        return subject.reconcile(self.run, self.outcomes, self.output, native, str(self.source), str(self.target))

    def test_verified_snapshot_is_associated_without_changing_files(self):
        before = ((self.source / 'a').read_bytes(), (self.target / 'a').read_bytes())
        result = self.reconcile()
        self.assertEqual(result['categories'], {subject.SUCCESS: 1})
        self.assertEqual(result['attention_records'], 0)
        self.assertEqual(before, ((self.source / 'a').read_bytes(), (self.target / 'a').read_bytes()))
        with (self.output / 'pending-changed.csv').open(encoding='utf-8-sig', newline='') as stream:
            self.assertEqual(list(csv.DictReader(stream)), [])

    def test_later_source_change_returns_to_attention(self):
        (self.source / 'a').write_bytes(b'different size')
        result = self.reconcile()
        self.assertEqual(result['categories']['identity_changed_since_verification'], 1)
        self.assertEqual(result['attention_records'], 1)

    def test_later_target_change_returns_to_attention(self):
        (self.target / 'a').write_bytes(b'different size')
        result = self.reconcile()
        self.assertEqual(result['categories']['identity_changed_since_verification'], 1)
        self.assertEqual(result['attention_records'], 1)

    def test_missing_or_tampered_proof_is_not_treated_as_success(self):
        (self.batch / 'independent-verification.json').write_text('{}')
        result = self.reconcile()
        self.assertEqual(result['categories'], {'unconfirmed': 1})
        self.assertEqual(result['attention_records'], 1)
        self.assertEqual(len(result['evidence_warnings']), 1)

    def test_later_historical_change_cannot_use_an_older_retry(self):
        later = dict(self.row, result_time_beijing=datetime.datetime.fromtimestamp(
            time.time() + 3600, datetime.timezone.utc).isoformat())
        self.write_rows([self.row, later])
        result = self.reconcile()
        self.assertEqual(result['categories'], {subject.SUCCESS: 1, 'unconfirmed': 1})
        self.assertEqual(result['attention_unique_paths'], 1)

    def test_event_in_same_recorded_second_as_retry_remains_unconfirmed(self):
        receipt = json.loads((self.batch / 'receipts.jsonl').read_text())
        same_second = datetime.datetime.fromtimestamp(
            int(receipt['finished']), datetime.timezone.utc).isoformat()
        self.write_rows([dict(self.row, result_time_beijing=same_second)])
        result = self.reconcile()
        self.assertEqual(result['categories'], {'unconfirmed': 1})
        self.assertEqual(result['attention_records'], 1)

    def test_missing_current_file_is_attention(self):
        (self.target / 'a').unlink()
        result = self.reconcile()
        self.assertEqual(result['categories'], {'identity_unavailable': 1})
        self.assertEqual(result['attention_records'], 1)


if __name__ == '__main__':
    unittest.main()
