import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    'projset_error_reconciliation', REPO / 'ops/diagnostics/projset_error_reconciliation.py')
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)


class ErrorReconciliationTests(unittest.TestCase):
    def test_scan_retry_proof_binds_historical_receipt_and_verified_children(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / 'run'
            work = run / 'recovery/attention-test'
            worker = work / 'workers/cpu-worker-2'
            worker.mkdir(parents=True)
            original_path = run / 'nodes/cpu-worker-1/attempt-old/receipts.jsonl'
            original_path.parent.mkdir(parents=True)
            error = {'node': 'cpu-worker-1', 'job': '12', 'relative_path': 'dir',
                     'receipt_path': str(original_path), 'receipt_offset': '0'}
            reason = 'RuntimeError: control connection ended during scan'
            original_path.write_text(json.dumps({'status': 'error', 'job': 12, 'rel': 'dir',
                                                 'detail': reason}) + '\n')
            (work / 'manifest.json').write_text(json.dumps({'nodes': ['cpu-worker-2']}))
            key_data = ['error', 'dir', '12', str(original_path), '0']
            key = hashlib.sha256(json.dumps(key_data, ensure_ascii=True).encode()).hexdigest()
            retried = {'key': key, 'category': 'error', 'state': 'done', 'node': 'cpu-worker-2',
                       'rel': 'dir', 'job': '12', 'reason': reason,
                       'receipt_path': str(original_path), 'receipt_offset': '0',
                       'result': {'result': 'scan_verified', 'directories': 1, 'files': 1,
                                  'manual': 0, 'issues': [], 'copied': 0, 'skipped': 1,
                                  'receipts': [{'rel': 'dir/file', 'kind': 'file',
                                                'status': 'skipped', 'sha256': 'a' * 64}]}}
            path = worker / 'receipts.jsonl'
            raw = (json.dumps(retried) + '\n').encode(); path.write_bytes(raw)
            descriptor = {'state': 'verified_scan_retry', 'work': 'recovery/attention-test',
                          'records': [{'error': error, 'retry': {'path': str(path.relative_to(run)),
                                      'offset': 0, 'sha256': hashlib.sha256(raw).hexdigest()}}]}
            descriptor_path = run / 'recovery/scan.json'
            descriptor_path.write_text(json.dumps(descriptor))
            self.assertEqual(subject.verified_scan_batch(run, descriptor_path), {subject.error_key(error)})
            evidence = self.evidence(); evidence['scan_batches'] = {'scan.json': {subject.error_key(error)}}
            row = dict(error, reason=reason)
            self.assertEqual(subject.classify(row, evidence)[0], 'verified_retry_scan')
            retried['result']['receipts'][0]['status'] = 'error'
            raw = (json.dumps(retried) + '\n').encode(); path.write_bytes(raw)
            descriptor['records'][0]['retry']['sha256'] = hashlib.sha256(raw).hexdigest()
            descriptor_path.write_text(json.dumps(descriptor))
            with self.assertRaises(ValueError):
                subject.verified_scan_batch(run, descriptor_path)

    def test_bounded_batch_requires_complete_independent_hashed_proof(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp); recovery = run / 'recovery'; recovery.mkdir()
            identity = dict(ino=1, size=5, mtime_ns=10, ctime_ns=20)
            directory = dict(ino=2, size=4096, mtime_ns=10, ctime_ns=20)
            entry = dict(identity, rel='dir/file', kind='file')
            root = dict(directory, rel='dir', kind='directory')
            row = self.row('dir', '7'); row['receipt_offset'] = '42'
            receipt = dict(rel='dir/file', status='copied', source_identity=identity,
                           dst_stat=identity, sha256='a'*64, logical_bytes=5,
                           finished=1794300000, node='cpu-worker-7')
            proof = dict(rel='dir/file', ok=True, source_identity=identity,
                         target_identity=identity, receipt_finished=receipt['finished'],
                         comparison=dict(result='same_sha256', hash_passes=2,
                                         source_sha256='a'*64, target_sha256='a'*64,
                                         source_size=5, target_size=5))
            descriptor = {'state': 'verified_snapshot'}

            def write(name, value, lines=False):
                raw = (json.dumps(value) + ('\n' if lines else '')).encode()
                path = recovery / name; path.write_bytes(raw)
                descriptor[name] = dict(path=str(path.relative_to(run)),
                                        sha256=hashlib.sha256(raw).hexdigest())

            write('manifest', {'entries': [entry]})
            write('discovery', {'entries': [entry], 'directories': [root],
                                'roots': ['dir'], 'errors': []})
            write('receipts', receipt, lines=True)
            write('error_records', [row])
            verified = dict(state='verified', started=1794300001, finished=1794300002,
                            node='cpu-worker-8', files=[proof],
                            directories=[dict(root, ok=True, source_identity=directory,
                                              children=['file'])],
                            manifest_sha256=descriptor['manifest']['sha256'],
                            summary=dict(files=1, verified_files=1, directories=1,
                                         verified_directories=1, failures=0))
            write('verification', verified)
            path = recovery / 'descriptor.json'
            path.write_text(json.dumps(descriptor))
            keys = subject.bounded_batch(run, path)
            self.assertEqual(keys, {subject.error_key(row)})
            evidence = self.evidence(); evidence['bounded_batches'] = {'batch': keys}
            self.assertEqual(subject.classify(row, evidence)[0], 'verified_retry_snapshot')
            wrong = dict(row, receipt_offset='43')
            self.assertEqual(subject.classify(wrong, evidence)[0], 'unclassified')
            # A missing file cannot be hidden by unchanged success totals.
            verified['files'] = []
            write('verification', verified); path.write_text(json.dumps(descriptor))
            with self.assertRaises(ValueError): subject.bounded_batch(run, path)
            verified['files'] = [proof]; proof['comparison']['target_sha256'] = 'b'*64
            write('verification', verified); path.write_text(json.dumps(descriptor))
            with self.assertRaises(ValueError): subject.bounded_batch(run, path)
            proof['comparison']['target_sha256'] = 'a'*64
            write('verification', verified); path.write_text(json.dumps(descriptor))
            (recovery / 'receipts').write_text('changed')
            with self.assertRaises(ValueError): subject.bounded_batch(run, path)

    @staticmethod
    def row(rel='a', job='1', reason='OSError: [Errno 116] Stale file handle'):
        return {'relative_path': rel, 'job': job, 'reason': reason, 'node': 'cpu-worker-6',
                'receipt_path': '/run/nodes/cpu-worker-6/' + subject.CURRENT_ATTEMPT + '/receipts.jsonl',
                'result_time_beijing': '2026-11-10T12:00:00+08:00'}

    @staticmethod
    def evidence():
        identity = {'ino': 10, 'size': 5, 'mtime_ns': 20, 'ctime_ns': 30}
        return {'first_roots': {('1', 'a')}, 'recent_roots': {('2', 'b')},
                'rescanned': {'3': 'dir'}, 'prior_manifest': {'old': identity},
                'prior_receipts': {}, 'warnings': []}

    def test_exact_job_and_separate_rescan_classification(self):
        evidence = self.evidence()
        self.assertEqual(subject.classify(self.row(), evidence)[0], 'verified_193_file_batch')
        self.assertEqual(subject.classify(self.row(job='99'), evidence)[0], 'unclassified')
        self.assertEqual(subject.classify(self.row('b', '2'), evidence)[0], 'verified_481_file_batch')
        self.assertEqual(subject.classify(self.row('dir', '3',
                         'RuntimeError: control connection ended during scan'), evidence)[0],
                         'rescan_confirmed')
        evidence['recent_roots'].add(('1', 'a'))
        self.assertEqual(subject.classify(self.row(), evidence)[0], 'ambiguous')

    def test_prior_retry_requires_a_later_matching_clean_receipt(self):
        evidence = self.evidence()
        row = self.row('old', '4')
        self.assertEqual(subject.classify(row, evidence)[0], 'retry_pending')
        identity = evidence['prior_manifest']['old']
        receipt = {'status': 'copied', 'source_identity': dict(identity),
                   'dst_stat': {'size': 5}, 'logical_bytes': 5, 'sha256': 'a' * 64,
                   'finished': 1794300000}
        evidence['prior_receipts']['old'] = receipt
        self.assertEqual(subject.classify(row, evidence)[0], 'verified_prior_file')
        receipt['metadata_warnings'] = ['permission']
        self.assertEqual(subject.classify(row, evidence)[0], 'retry_attention')
        receipt.pop('metadata_warnings')
        receipt['finished'] = 1
        self.assertEqual(subject.classify(row, evidence)[0], 'retry_attention')
        receipt['finished'] = 1794300000
        receipt['source_identity']['ino'] = 11
        self.assertEqual(subject.classify(row, evidence)[0], 'retry_attention')

    def test_incomplete_receipt_tail_is_ignored_without_truncation(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'receipts.jsonl'
            raw = b'{"rel":"ok","status":"copied"}\n{"rel":"unfinished"'
            path.write_bytes(raw)
            self.assertEqual(list(subject.read_receipts(path)), ['ok'])
            self.assertEqual(path.read_bytes(), raw)

    def test_contradictory_verification_evidence_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / 'run'
            recent = run / 'recovery/stale-retry-20261110T1724'
            recent.mkdir(parents=True)
            audit = recent / 'audit.json'; target = recent / 'target.json'; verified = recent / 'verified.json'
            audit.write_text(json.dumps({'results': [{'job': 1, 'rel': 'dir', 'matches_original': True}]}))
            target.write_text(json.dumps({'results': [{'rel': 'dir', 'source_kind': 'directory',
                                                        'target_kind': 'directory'}]}))
            verified.write_text(json.dumps({'summary': {'total': 1, 'verified': 1, 'failures': 0},
                                            'files': [{'rel': 'dir/file', 'ok': True}]}))
            manifest = recent / 'manifest.json'; manifest.write_text(json.dumps({'entries': [{'rel': 'dir/file'}]}))
            (recent / 'resolution.json').write_text(json.dumps({
                'state': 'verified', 'source_identity_audit': str(audit), 'root_target_check': str(target),
                'independent_verification': str(verified), 'manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
                'error_roots': 1, 'files': 1,
            }))
            self.assertEqual(subject.evidence_for(run)['recent_roots'], {('1', 'dir')})
            target.write_text(json.dumps({'results': [
                {'rel': 'dir', 'source_kind': 'directory', 'target_kind': 'directory'},
                {'rel': 'extra', 'source_kind': 'directory', 'target_kind': 'missing'},
            ]}))
            evidence = subject.evidence_for(run)
            self.assertEqual(evidence['recent_roots'], set())
            self.assertIn('recent_retry', [warning['evidence'] for warning in evidence['warnings']])

    def test_missing_evidence_keeps_error_in_attention_report(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); run = root / 'run'; outcomes = root / 'outcomes'; out = root / 'report'
            run.mkdir(); outcomes.mkdir()
            (run / 'status.json').write_text(json.dumps({'counts': {'error': 1}, 'updated': 1}))
            row = self.row(); row['recorded_source_bytes'] = '1000'
            fields = list(row)
            with (outcomes / 'error-paths.csv').open('w', encoding='utf-8-sig', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader(); writer.writerow(row)
            summary = subject.reconcile(run, outcomes, out)
            self.assertEqual(summary['categories'], {'unclassified': 1})
            self.assertEqual(summary['attention_records'], 1)
            self.assertEqual(summary['attention_known_TB'], 0.000000001)
            self.assertEqual(len(summary['evidence_warnings']), 4)
            with (out / 'pending-errors.csv').open(encoding='utf-8-sig', newline='') as stream:
                pending = list(csv.DictReader(stream))
            self.assertEqual(pending[0]['relative_path'], 'a')
            self.assertEqual(pending[0]['reconciliation'], 'unclassified')


if __name__ == '__main__':
    unittest.main()
