"""UNDEPLOYED, saved-record precheck for the priorities1-3 dependency of P4.

This checks the actual pipeline record format without opening shared storage,
payloads, SQLite, /proc, sockets or locks. A passing result is only a record
precheck. It cannot authorize dispatch: fresh queue, issue, manifest, metadata
receipt and exact process-exit evidence must still be collected independently.
"""
import argparse
import datetime
from pathlib import Path, PurePosixPath

from migration_io import read_json, write_json


ORDER = ((1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (3, 2), (1, 3), (2, 3))
STAGES = tuple('p%d-r%d' % pair for pair in ORDER)
LATEST = ('p1-r3', 'p2-r3', 'p3-r2')
GOOD = frozenset(('copied', 'skipped', 'verified', 'linked'))
SUMMARY_TOTALS = frozenset(('bytes', 'metadata_warnings', 'verified_files', 'verified_bytes'))
REMAINING = (
    'fresh coherent configuration, source/destination scope and STOP checks',
    'all-stage filesystem queues and broker leases empty; no drain or duplicate owner',
    'complete issue files and differences reviewed, stable repairs verified where required',
    'manifest identity/content and receipt coverage bound to these exact latest records',
    'directory and shared-ancestor receipts, ACL/xattr/time and final inode verification',
    '24 data workers, coordinator/helpers and12 probes actually exited by exact PID/start/argv',
    'fresh training protection and exclusive local idempotent dispatch lock',
    'validated complete P4 engine and its independent scope/database/bootstrap',
)


def check_records(snapshot, expected_control):
    """Return deterministic blockers; never emit launch_ready=True.

    Coordinator summaries/finalizers are sparse Counter dictionaries, including
    empty dictionaries for an empty payload/alias set. Their absence is distinct
    from a legitimate empty result. Historical nonfatal differences may have
    been repaired by later rounds; the latest round must be clean here. A
    complete_with_changes result requires separate difference review/repair and
    is deliberately not promoted by this limited precheck.
    """
    blockers = []

    def block(code, path, detail):
        blockers.append({'code': code, 'path': path, 'detail': detail})

    def mapping(parent, key, path):
        value = parent.get(key)
        if not isinstance(value, dict):
            block('missing_record', path, 'A committed object is required.')
            return None
        return value

    def integer(value, path):
        if type(value) is not int or value < 0:
            block('invalid_counter', path, 'An explicit nonnegative integer is required.')
            return None
        return value

    def clean_tree(value, path):
        if isinstance(value, dict):
            for key, item in value.items():
                sub = path + '.' + str(key)
                if not isinstance(key, str):
                    block('invalid_record_key', sub, 'JSON object keys must be strings.')
                elif key.startswith('_background_') or key == 'fatal' and item is not False:
                    block('unfinished_or_fatal', sub, 'Background placeholders and fatal results are refused.')
                clean_tree(item, sub)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                clean_tree(item, path + '[%d]' % i)

    def timestamp(value, path):
        try:
            if not isinstance(value, str):
                raise ValueError()
            parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.utcoffset() is None:
                raise ValueError()
            return parsed.timestamp()
        except (ValueError, OverflowError):
            block('invalid_timestamp', path, 'An explicit timezone-aware completion timestamp is required.')

    def metadata(value, path, label):
        if value is None:
            return
        clean_tree(value, path)
        if value.get('job') != label or value.get('status') != 'complete' or value.get('stopped') is not False:
            block('metadata_incomplete', path, 'Exact completed, unstopped metadata job required.')
        values = {key: integer(value.get(key), path + '.' + key) for key in
                  ('errors', 'source_changed', 'metadata_warnings', 'issues',
                   'directories', 'created_checked', 'total_directories')}
        if any(values[key] not in (None, 0) for key in
               ('errors', 'source_changed', 'metadata_warnings', 'issues')):
            block('metadata_differences', path, 'Metadata differences require independent review and repair.')
        if values['directories'] != values['total_directories'] or (
                values['created_checked'] is not None and values['total_directories'] is not None
                and values['created_checked'] < values['total_directories']):
            block('metadata_coverage', path, 'Metadata and creation counts do not cover the selected directories.')
        started = timestamp(value.get('started'), path + '.started')
        finished = timestamp(value.get('finished'), path + '.finished')
        if started is not None and finished is not None and finished < started:
            block('invalid_timestamp', path, 'Metadata finish precedes its start.')

    if not isinstance(expected_control, str):
        raise ValueError('an exact absolute priorities1-3 control path is required')
    control = PurePosixPath(expected_control)
    if (not control.is_absolute()
            or str(control) != expected_control or '..' in control.parts
            or not control.name.startswith('.hcr-copy-') or '\x00' in expected_control):
        raise ValueError('an exact absolute priorities1-3 control path is required')
    if not isinstance(snapshot, dict):
        raise TypeError('saved inspection must be an object')
    state = mapping(snapshot, 'state', 'state')
    pipeline = mapping(snapshot, 'pipeline', 'pipeline')
    metrics = mapping(snapshot, 'metrics', 'metrics')
    if snapshot.get('stop') is not False:
        block('stop_unknown_or_set', 'stop', 'STOP must be explicitly false.')
    if metrics is not None and metrics.get('control') != expected_control:
        block('control_mismatch', 'metrics.control', 'Snapshot belongs to another control.')
    if state is not None:
        if state.get('run_id') != control.name[1:]:
            block('control_mismatch', 'state.run_id', 'Run identity does not match the dependency control.')
        if state.get('status') != 'complete':
            block('terminal_not_clean', 'state.status', 'Clean completion or separately verified difference repair required.')
        if state.get('recovery_warm') is not False or state.get('scan_complete') is not True or state.get('stage') is not None:
            block('unfinished_scan', 'state', 'Warm recovery and formal scans must really have finished.')
        if integer(state.get('unresolved'), 'state.unresolved') not in (None, 0):
            block('unresolved_differences', 'state.unresolved', 'Outstanding differences require review/repair.')
        timestamp(state.get('finished'), 'state.finished')
        jobs = state.get('background_metadata')
        if not isinstance(jobs, list) or any(not isinstance(job, dict) or job.get('status') != 'done' for job in jobs):
            block('background_jobs_incomplete', 'state.background_metadata', 'All background jobs require explicit completion.')
    if pipeline is not None:
        stages = mapping(pipeline, 'stages', 'pipeline.stages')
        directories = mapping(pipeline, 'directory_jobs', 'pipeline.directory_jobs')
        shared_relatives = set()
        if stages is not None:
            if set(stages) != set(STAGES):
                block('formal_stage_set', 'pipeline.stages', 'Exactly the eight formal stages are required; warm p3-r3 cannot substitute.')
            for phase, round_no in ORDER:
                stage = 'p%d-r%d' % (phase, round_no)
                path = 'pipeline.stages.' + stage
                record = mapping(stages, stage, path)
                if record is None:
                    continue
                if (type(record.get('phase')) is not int or type(record.get('round')) is not int
                        or (record.get('phase'), record.get('round'), record.get('stage')) != (phase, round_no, stage)):
                    block('stage_identity', path, 'Phase, round and stage identity must agree.')
                manifest = record.get('manifest')
                candidate = PurePosixPath(manifest) if isinstance(manifest, str) else None
                if (candidate is None or str(candidate) != manifest or '\x00' in manifest
                        or '..' in candidate.parts or candidate.parent != control / 'manifests'
                        or not (candidate.name == stage + '.jsonl' or candidate.name.startswith(stage + '-'))
                        or candidate.suffix != '.jsonl'):
                    block('manifest_scope', path + '.manifest', 'An exact stage manifest inside this control is required.')
                scan = mapping(record, 'scan', path + '.scan')
                summary = mapping(record, 'summary', path + '.summary')
                final = mapping(record, 'finalize', path + '.finalize')
                for value, name in ((scan, 'scan'), (summary, 'summary'), (final, 'finalize')):
                    if value is not None:
                        clean_tree(value, path + '.' + name)
                if scan is None or summary is None or final is None:
                    continue
                counters = {key: integer(scan.get(key), path + '.scan.' + key) for key in
                            ('files', 'symlinks', 'dirs', 'hardlinks', 'copy_entries', 'errors', 'unsupported')}
                if type(scan.get('phase')) is not int or scan['phase'] != phase:
                    block('stage_identity', path + '.scan.phase', 'Scan phase does not match the formal stage.')
                if all(counters[key] is not None for key in ('files', 'symlinks', 'hardlinks', 'copy_entries')):
                    if counters['hardlinks'] > counters['files'] or counters['copy_entries'] != counters['files'] + counters['symlinks'] - counters['hardlinks']:
                        block('scan_coverage', path + '.scan', 'Copyable and hardlink inventory counts disagree.')
                for key, value in summary.items():
                    integer(value, path + '.summary.' + key)
                for key in ('errors', 'hardlinks', 'hardlink_bytes', 'directories'):
                    if key in final:
                        integer(final[key], path + '.finalize.' + key)
                if all(type(value) is int and value >= 0 for value in summary.values()):
                    results = sum(value for key, value in summary.items() if key not in SUMMARY_TOTALS)
                    verified = sum(summary.get(key, 0) for key in GOOD)
                    if results != counters['copy_entries'] or verified != summary.get('verified_files', 0):
                        block('receipt_coverage', path + '.summary', 'Receipt and inventory counts do not agree.')
                if stage in LATEST:
                    if (any(counters[key] not in (None, 0) for key in ('errors', 'unsupported'))
                            or any(value for key, value in summary.items() if key not in GOOD | SUMMARY_TOTALS)
                            or summary.get('metadata_warnings', 0) or final.get('errors', 0)):
                        block('latest_differences', path, 'Latest scan, receipts or finalizer still contain differences.')
                    if final.get('hardlinks', 0) != counters['hardlinks']:
                        block('hardlink_coverage', path + '.finalize', 'Latest aliases were not all finalized.')
                    if directories is not None:
                        job = mapping(directories, stage, 'pipeline.directory_jobs.' + stage)
                        metadata(job, 'pipeline.directory_jobs.' + stage,
                                 candidate.stem if candidate is not None else stage)
                        if job is not None:
                            deferred = integer(job.get('deferred'), path + '.directory_job.deferred')
                            entries = job.get('deferred_entries')
                            if (not isinstance(entries, list) or any(not isinstance(entry, dict)
                                    or entry.get('rel') not in ('', 'home', 'repos') for entry in entries)):
                                block('deferred_metadata_coverage', path, 'Exact shared-directory entries are required.')
                            else:
                                relatives = {entry['rel'] for entry in entries}
                                shared_relatives.update(relatives)
                                if len(entries) != len(relatives) or deferred != len(entries):
                                    block('deferred_metadata_coverage', path, 'Shared-directory entry count is inconsistent.')
                            total = job.get('total_directories')
                            if type(total) is int and deferred is not None and total + deferred != counters['dirs']:
                                block('directory_inventory_coverage', path, 'Selected and deferred metadata do not cover inventoried directories.')
        shared = mapping(pipeline, 'shared_ancestors', 'pipeline.shared_ancestors')
        metadata(shared, 'pipeline.shared_ancestors', 'shared-ancestors')
        if shared is not None and shared.get('total_directories') != len(shared_relatives):
            block('shared_metadata_coverage', 'pipeline.shared_ancestors', 'Shared job count does not cover the latest deferred ancestors.')
    return {'version': 1, 'control': expected_control,
            'record_precheck_passed': not blockers, 'launch_ready': False,
            'blockers': blockers, 'required_formal_stages': list(STAGES),
            'latest_stages': list(LATEST), 'remaining_live_checks': list(REMAINING),
            'runtime_mutation_performed': False,
            'limitations': 'Saved records alone cannot prove current queue emptiness, process exits, metadata, payload coverage or launch eligibility.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inspection', type=Path)
    parser.add_argument('--control', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = check_records(read_json(args.inspection), args.control)
    result['inspection'] = str(args.inspection)
    write_json(args.output, result)
    import json
    print(json.dumps({'saved': str(args.output), 'record_precheck_passed': result['record_precheck_passed'],
                      'launch_ready': False, 'blocker_count': len(result['blockers'])}))


if __name__ == '__main__':
    main()
