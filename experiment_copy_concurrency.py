#!/usr/bin/env python3
"""Bounded live concurrency experiment; never signal or restart copy processes.

Retiring threads finish their whole leased batch before ceasing new claims.
The original target is restored in finally, including on HUP/TERM/INT. A
persisted operation marker permits explicit recovery after an uncatchable exit.
"""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import signal
import socket
import time
import uuid

from migration_io import read_json, write_json
from observe_worker_canary import snapshot, compare


FIELDS = ('copy_threads_per_node', 'supplement_threads')
MARKER = 'copy_concurrency_experiment'


def validate_config(control, cfg):
    if (cfg.get('control') != str(control) or
            (cfg.get('source'), cfg.get('destination')) !=
            ('/share/project/eai_pwm', '/share-mixed/project/eai_pwm') or
            cfg.get('threads') != 64 or type(cfg.get('threads')) is not int or
            type(cfg.get('copy_threads_per_node')) is not int or
            cfg['copy_threads_per_node'] not in (96, 128) or
            type(cfg.get('supplement_threads')) is not int or
            cfg['supplement_threads'] != cfg['copy_threads_per_node'] - 64 or
            not isinstance(cfg.get('run_id'), str) or
            control.name != '.' + cfg['run_id'] or
            cfg.get('master_host') != 'job-' + str(cfg.get('job')) + '-master-0' or
            len(set(cfg.get('copy_hosts', []))) != 12):
        raise ValueError('wrong copy scope, identity, or thread partition')
    for key in ('io_bytes_per_second', 'supplement_io_bytes_per_second'):
        if type(cfg.get(key)) is not int or not 0 < cfg[key] <= 10_000_000_000:
            raise ValueError('invalid existing copy I/O budget')


def restore(control, record):
    """Restore only our own partition; preserve unrelated configuration edits."""
    cfg = read_json(control / 'config.json')
    validate_config(control, cfg)
    if (not isinstance(record, dict) or
            any(not isinstance(record.get(key), dict) or set(record[key]) != set(FIELDS)
                for key in ('before', 'target'))):
        raise ValueError('restoration requires exactly the two thread fields')
    for key in ('before', 'target'):
        validate_config(control, dict(cfg, **record[key]))
    if record['before'] == record['target']:
        raise ValueError('restoration must bind a different original partition')
    if (record.get('control') == str(control) and record.get('run_id') == cfg['run_id'] and
            MARKER not in cfg and {key: cfg[key] for key in FIELDS} == record['before']):
        return False  # Publication failed before applying, or already restored.
    if (record.get('control') != str(control) or record.get('run_id') != cfg['run_id'] or
            cfg.get(MARKER) != record.get('marker') or
            not isinstance(record.get('marker'), dict) or
            not record['marker'].get('operation')):
        raise RuntimeError('concurrency marker ownership differs; no configuration overwritten')
    target = record['target']
    if {key: cfg.get(key) for key in FIELDS} != target:
        raise RuntimeError('thread partition changed externally; no configuration overwritten')
    before = record['before']
    restored = dict(cfg, **before)
    restored.pop(MARKER)
    validate_config(control, restored)
    write_json(control / 'config.json', restored)
    return True


def run(control, total, seconds, interval=10):
    control = Path(control).resolve()
    cfg = read_json(control / 'config.json')
    validate_config(control, cfg)
    if socket.gethostname() != cfg['master_host']:
        raise RuntimeError('run only on the real recovery master')
    if (type(total) is not int or total not in (96, 128) or
            total == cfg['copy_threads_per_node'] or not 30 <= seconds <= 300 or
            not 1 <= interval <= 30):
        raise ValueError('require a different 96/128 target and duration within 30..300s')
    if (control / 'STOP').exists() or read_json(control / 'state.json')['status'] not in ('copying', 'scanning'):
        raise RuntimeError('STOP or non-copying state')
    if MARKER in cfg:
        raise RuntimeError('another concurrency experiment needs inspection')
    from migration_run import validate_mounts
    validate_mounts(cfg)
    lock_path = Path('/tmp') / (cfg['run_id'] + '-copy-concurrency.lock')
    with lock_path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        initial = previous = snapshot(control)
        operation = uuid.uuid4().hex
        directory = control / ('copy-concurrency-' + operation)
        directory.mkdir(mode=0o700)
        output = directory / 'experiment.json'
        marker = {'operation': operation, 'started': time.time()}
        record = {'control': str(control), 'run_id': cfg['run_id'], 'marker': marker,
                  'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  'before': {key: cfg[key] for key in FIELDS},
                  'target': {'copy_threads_per_node': total, 'supplement_threads': total - 64},
                  'duration_seconds': seconds, 'status': 'intent', 'initial': initial,
                  'samples': [], 'copy_processes_signalled': 0}
        write_json(output, record)
        if read_json(control / 'config.json') != cfg:
            raise RuntimeError('configuration changed before experiment publication')
        attempted = False
        try:
            # A lost/delayed write may still have succeeded. Set restoration
            # intent before publication, never only after its return value.
            attempted = True
            write_json(control / 'config.json', dict(cfg, **record['target'], **{MARKER: marker}))
            record['status'] = 'running'
            began = time.monotonic()
            while time.monotonic() - began < seconds:
                time.sleep(min(interval, max(0, seconds - (time.monotonic() - began))))
                current = snapshot(control)
                summary = compare(previous, current)
                # Compare against the first snapshot as well as adjacent ones.
                compare(initial, current)
                elapsed = summary['seconds']
                streamed = sum(n['streamed_bytes'] for n in current['nodes'].values()) - sum(
                    n['streamed_bytes'] for n in previous['nodes'].values())
                desired = sum(read_json(control / folder / name)['threads']
                              for folder, name in (key.split('/', 1) for key in current['nodes']))
                row = {'elapsed': time.monotonic() - began, 'seconds': elapsed,
                       'streamed_gib_s': streamed / elapsed / 2**30,
                       'completed_gib_s': summary['GiB_per_second'],
                       'files_s': summary['files_per_second'], 'active': summary['active'],
                       'desired_threads': desired, 'snapshot': current}
                record['samples'].append(row)
                write_json(output, record)
                print(json.dumps({k: v for k, v in row.items() if k != 'snapshot'}), flush=True)
                previous = current
                if (control / 'STOP').exists():
                    raise RuntimeError('STOP appeared; restoring the configuration, not restarting anything')
            record['experiment_completed'] = True
        except BaseException as exc:
            record['experiment_completed'] = False
            record['error'] = type(exc).__name__ + ': ' + str(exc)
            raise
        finally:
            if attempted:
                try:
                    restore(control, record)
                except BaseException as exc:
                    record.update(status='restore_needs_inspection',
                                  restore_error=type(exc).__name__ + ': ' + str(exc))
                    write_json(output, record)
                    raise
                record.update(status='restored', restored=time.time())
                write_json(output, record)
                print(json.dumps({'status': 'restored', 'threads_per_node': cfg['copy_threads_per_node'],
                                  'output': str(output)}), flush=True)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', required=True, type=Path)
    parser.add_argument('--threads-per-node', type=int, choices=(96, 128))
    parser.add_argument('--seconds', type=float, default=180)
    parser.add_argument('--restore-record', type=Path)
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise KeyboardInterrupt('concurrency experiment interrupted by signal ' + str(signum))
    for name in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        signal.signal(name, interrupted)
    if args.restore_record:
        control = args.control.resolve()
        record_path = args.restore_record.resolve()
        if (not record_path.is_relative_to(control) or record_path.name != 'experiment.json' or
                record_path.parent.parent != control or
                not record_path.parent.name.startswith('copy-concurrency-')):
            raise RuntimeError('restore record must be a direct concurrency experiment in this control')
        cfg = read_json(control / 'config.json')
        if socket.gethostname() != cfg['master_host']:
            raise RuntimeError('restore only on the actual master')
        lock_path = Path('/tmp') / (cfg['run_id'] + '-copy-concurrency.lock')
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            restore(control, read_json(record_path))
    else:
        if args.threads_per_node is None:
            parser.error('--threads-per-node is required for an experiment')
        run(args.control, args.threads_per_node, args.seconds)


if __name__ == '__main__':
    main()
