"""Local synthetic saved-record fixtures; no cluster or migration paths opened."""
import copy
import unittest

from migration_priority4_dependency_records import check_records, ORDER, LATEST


CONTROL = '/fixture/.hcr-copy-fixture-123'
STAMP = '2026-10-21T22:00:00+00:00'


def metadata(label):
    return {'job': label, 'status': 'complete', 'stopped': False,
            'errors': 0, 'source_changed': 0, 'metadata_warnings': 0, 'issues': 0,
            'directories': 3, 'created_checked': 3, 'total_directories': 3,
            'deferred': 0, 'deferred_entries': [],
            'started': STAMP, 'finished': STAMP}


def fixture():
    stages = {}
    for phase, round_no in ORDER:
        stage = 'p%d-r%d' % (phase, round_no)
        stages[stage] = {'phase': phase, 'round': round_no, 'stage': stage,
                         'manifest': CONTROL + '/manifests/' + stage + '.jsonl',
                         'scan': {'phase': phase, 'files': 5, 'symlinks': 1, 'dirs': 3,
                                  'hardlinks': 1, 'copy_entries': 5, 'errors': 0, 'unsupported': 0},
                         'summary': {'copied': 4, 'verified': 1, 'verified_files': 5},
                         'finalize': {'hardlinks': 1}}
    directory_jobs = {s: metadata(s) for s in LATEST}
    directory_jobs['p3-r2'].update(directories=0, created_checked=0, total_directories=0,
                                   deferred=3, deferred_entries=[{'rel': rel} for rel in ('', 'home', 'repos')])
    return {'stop': False, 'metrics': {'control': CONTROL},
            'state': {'run_id': 'hcr-copy-fixture-123', 'status': 'complete',
                      'recovery_warm': False, 'scan_complete': True, 'stage': None,
                      'unresolved': 0, 'finished': STAMP, 'background_metadata': []},
            'pipeline': {'stages': stages, 'directory_jobs': directory_jobs,
                         'shared_ancestors': metadata('shared-ancestors')}}


class DependencyRecordsTest(unittest.TestCase):
    def setUp(self):
        self.value = fixture()

    def result(self):
        result = check_records(self.value, CONTROL)
        self.assertIs(result['launch_ready'], False)
        self.assertIs(result['runtime_mutation_performed'], False)
        return result

    def reject(self, code):
        result = self.result()
        self.assertFalse(result['record_precheck_passed'])
        self.assertIn(code, [x['code'] for x in result['blockers']])

    def test_clean_records_are_only_precheck_not_launch(self):
        before = copy.deepcopy(self.value)
        result = self.result()
        self.assertTrue(result['record_precheck_passed'])
        self.assertEqual(self.value, before)
        self.assertGreater(len(result['remaining_live_checks']), 5)

    def test_missing_formal_cannot_use_warm_p3r3(self):
        stages = self.value['pipeline']['stages']
        stages['p3-r3'] = stages.pop('p3-r2')
        self.reject('formal_stage_set')

    def test_background_placeholder_even_zero_is_rejected(self):
        self.value['pipeline']['stages']['p1-r1']['summary']['_background_collect_pending'] = False
        self.reject('unfinished_or_fatal')

    def test_historical_fatal_cannot_hide_behind_clean_latest(self):
        self.value['pipeline']['stages']['p1-r1']['finalize'] = {'fatal': True, 'errors': 1}
        self.reject('unfinished_or_fatal')

    def test_unfinished_metadata(self):
        self.value['pipeline']['directory_jobs']['p3-r2']['status'] = 'applying_metadata'
        self.reject('metadata_incomplete')

    def test_partial_metadata_count(self):
        self.value['pipeline']['shared_ancestors']['directories'] = 2
        self.reject('metadata_coverage')

    def test_missing_summary_is_not_empty_committed_counter(self):
        del self.value['pipeline']['stages']['p1-r1']['summary']
        self.reject('missing_record')

    def test_zero_payload_sparse_counters_are_supported(self):
        record = self.value['pipeline']['stages']['p1-r1']
        record['scan'].update(files=0, symlinks=0, hardlinks=0, copy_entries=0)
        record['summary'], record['finalize'] = {}, {}
        self.assertTrue(self.result()['record_precheck_passed'])

    def test_receipt_count_mismatch(self):
        self.value['pipeline']['stages']['p2-r3']['summary']['verified_files'] = 4
        self.reject('receipt_coverage')

    def test_latest_alias_count_mismatch(self):
        self.value['pipeline']['stages']['p3-r2']['finalize'] = {}
        self.reject('hardlink_coverage')

    def test_latest_metadata_warning(self):
        self.value['pipeline']['stages']['p2-r3']['summary']['metadata_warnings'] = 1
        self.reject('latest_differences')

    def test_nonfatal_earlier_difference_retained_after_clean_later(self):
        self.value['pipeline']['stages']['p2-r1']['summary'] = {'copied': 4, 'changed': 1, 'verified_files': 4}
        self.assertTrue(self.result()['record_precheck_passed'])

    def test_complete_with_changes_requires_separate_review(self):
        self.value['state']['status'] = 'complete_with_changes'
        self.reject('terminal_not_clean')

    def test_negative_and_boolean_counter_rejected(self):
        for bad in (-1, True):
            self.value['pipeline']['stages']['p1-r1']['scan']['files'] = bad
            self.reject('invalid_counter')

    def test_manifest_escape_and_foreign_control_rejected(self):
        for bad in (CONTROL + '/manifests/../p1-r1.jsonl', '/foreign/manifests/p1-r1.jsonl',
                    CONTROL + '/manifests/p2-r1.jsonl'):
            self.value['pipeline']['stages']['p1-r1']['manifest'] = bad
            self.reject('manifest_scope')

    def test_snapshot_control_mismatch(self):
        self.value['metrics']['control'] = '/foreign'
        self.reject('control_mismatch')

    def test_stop_and_warm_not_promoted_by_terminal_string(self):
        self.value['stop'] = None
        self.value['state']['recovery_warm'] = True
        self.value['state']['scan_complete'] = False
        self.reject('stop_unknown_or_set')
        self.reject('unfinished_scan')

    def test_background_job_and_timestamp_require_completion(self):
        self.value['state']['background_metadata'] = [{'status': 'running'}]
        self.value['pipeline']['shared_ancestors']['finished'] = '2026-10-21T22:00:00'
        self.reject('background_jobs_incomplete')
        self.reject('invalid_timestamp')

    def test_boolean_phase_cannot_impersonate_formal_phase(self):
        self.value['pipeline']['stages']['p1-r1']['phase'] = True
        self.reject('stage_identity')

    def test_directory_and_shared_ancestor_coverage(self):
        self.value['pipeline']['directory_jobs']['p1-r3'].update(directories=2, created_checked=2, total_directories=2)
        self.reject('directory_inventory_coverage')
        self.value['pipeline']['shared_ancestors'].update(directories=2, created_checked=2, total_directories=2)
        self.reject('shared_metadata_coverage')


if __name__ == '__main__':
    unittest.main()
