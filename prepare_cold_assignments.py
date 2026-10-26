"""Prepare an assignment-only adoption cache without changing live task state.

The original producer, broker, seed and copy workers are never signalled. The
broker SQLite connection is read-only. Only a wholly completed, gapless cold
assignment prefix is exported; later assignments remain a cutover-time delta.
"""
import argparse
import gc
import json
from pathlib import Path
import time

from deploy_frontier_resume import SnapshotLedger
import deploy_pipelined_producer as base
from migration_io import read_json, write_json
from migration_warm_scan_checkpoint import validate_runtime, resource_budget


def prepare(control, directory, expected):
    control, directory = Path(control).resolve(), Path(directory).resolve()
    if directory.parent != control or not directory.name.startswith('warm-task-checkpoint-'):
        raise ValueError('assignment preparation needs a private child of its control')
    cfg, state, producer = validate_runtime(control, expected)
    floor = state.get('next_task_id')
    if type(floor) is not int or floor <= 0 or state['task_publication'].get('pending') != 0:
        raise RuntimeError('a published cold assignment prefix is required')
    broker = Path('/tmp') / (cfg['run_id'] + '-broker.sqlite')
    resource_budget(broker.stat().st_size)
    db = base.claims_reader(broker)
    ledger = None
    began, last = time.monotonic(), 0.0
    record = {'status': 'preparing_assignments_only', 'control': str(control),
              'run_id': cfg['run_id'], 'producer': producer, 'prefix_floor': floor,
              'authorizes_handover': False, 'data_processes_signalled': 0}
    was_enabled = gc.isenabled()
    try:
        counts = dict(db.execute('SELECT status,COUNT(*) FROM claims WHERE stage=? AND id<? '
                                 'GROUP BY status', ('p3-r3', floor)))
        if counts != {'done': floor}:
            raise RuntimeError('prefix contains leases, unpublished IDs, or gaps; nothing changed')
        directory.mkdir(mode=0o700)
        ledger = SnapshotLedger(directory / 'assigned-tasks.jsonl')
        gc.disable()  # Private, acyclic checkpoint containers only.
        for index, (task_id, payload) in enumerate(db.execute(
                'SELECT id,payload FROM claims WHERE stage=? AND id<? ORDER BY id', ('p3-r3', floor))):
            if task_id != index:
                raise RuntimeError('cold assignment prefix is not gapless')
            ledger.add(json.loads(payload), expected_id=task_id)
            if time.monotonic() - last >= 5:
                record.update(tasks=len(ledger.hashes), assigned_paths=len(ledger.assigned),
                              ledger_bytes=ledger.bytes, elapsed_seconds=time.monotonic()-began)
                write_json(directory/'preparation.json', record)
                print(json.dumps(record), flush=True)
                last = time.monotonic()
        if len(ledger.hashes) != floor:
            raise RuntimeError('cold assignment prefix changed while preparing')
        descriptor, meta = ledger.prepare_cache(control, directory, cfg)
        info = ledger.close()
        _, _, current = validate_runtime(control, expected)
        if any(current[k] != producer[k] for k in ('pid', 'start_ticks', 'argv')):
            raise RuntimeError('original producer changed during preparation')
        record.update(status='prepared_assignment_prefix_only', tasks=info['tasks'],
                      ledger_bytes=info['bytes'], assignment_checkpoint=str(descriptor),
                      assignment_prefix_sha256=meta['prefix_sha256'], ledger=info,
                      assigned_paths=len(ledger.assigned), elapsed_seconds=time.monotonic()-began)
        write_json(directory/'preparation.json', record)
        print(json.dumps(record), flush=True)
        return record
    except BaseException as exc:
        if directory.exists():
            record.update(status='failed_assignment_preparation_only',
                          error=type(exc).__name__+': '+str(exc))
            write_json(directory/'preparation.json', record)
        raise
    finally:
        if ledger is not None:
            ledger.close()
        db.close()
        if was_enabled:
            gc.enable()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--expected-runtime', required=True)
    args = parser.parse_args()
    prepare(args.control, args.directory, read_json(Path(args.expected_runtime)))
