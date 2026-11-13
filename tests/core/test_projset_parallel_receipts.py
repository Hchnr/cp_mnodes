import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'src/core'))


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT/'ops/copy'/(name+'.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


copy = load('projset_recovery_copy')
parallel = load('projset_parallel_receipts')


def write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))


def test_parallel_matches_serial_receipts_with_duplicates_and_torn_tail(tmp_path, monkeypatch):
    run = tmp_path/'run'
    (run/'recovery').mkdir(parents=True)
    original = {'node-a': {'copied': 1, 'processed': 1, 'copied_bytes': 9},
                'node-b': {'copied': 0, 'processed': 0}}
    (run/'recovery'/'original-counts.json').write_text(json.dumps(original))
    write(run/'tasks.jsonl', [dict(id=1, type='scan', rel=''),
                             dict(id=2, type='copy', entries=[dict(rel='a')]),
                             dict(id=3, type='copy', entries=[dict(rel='b')])])
    write(run/'completed.jsonl', [dict(id=3)])
    write(run/'nodes/node-a/receipts.jsonl', [dict(job=2, rel='a', status='copied', logical_bytes=9)])
    write(run/'nodes/node-a/attempt-a/receipts.jsonl', [
        dict(job=2, rel='a', status='copied', logical_bytes=7, _payload_written=6, nlink=2),
        dict(job=3, rel='b', status='skipped', logical_bytes=2)])
    write(run/'nodes/node-b/receipts.jsonl', [])
    write(run/'nodes/node-b/attempt-a/receipts.jsonl', [
        dict(job=2, rel='a', status='error', detail='last receipt'),
        dict(job=3, rel='b', status='copied', logical_bytes=5, metadata_warnings=['x'])])
    with (run/'nodes/node-b/attempt-a/receipts.jsonl').open('a') as stream:
        stream.write('{"partial":')
    monkeypatch.delenv('PROJSET_RECOVERY_GENERATION', raising=False)
    monkeypatch.setenv('PROJSET_FAST_REBUILD', '1')
    monkeypatch.delenv('PROJSET_PARALLEL_RECEIPTS', raising=False)
    serial = copy.rebuild(run, str(tmp_path/'serial.sqlite'))
    expected = serial.execute('select rel, job, payload from prior order by rel').fetchall()
    expected_baseline = json.loads((run/'recovery/baseline.json').read_text())
    serial.close()
    scratch = sqlite3.connect(':memory:')
    scratch.execute('create table prior(rel text primary key,job integer,payload text)')
    monkeypatch.setenv('PROJSET_ATTEMPT', 'parallel-test')
    reports = []
    actual_baseline = parallel.rebuild_receipts(
        run, scratch, {2}, original, lambda *a, **k: reports.append((a, k)),
        lambda node, command: ['/bin/sh', '-c', command])
    assert scratch.execute('select rel, job, payload from prior order by rel').fetchall() == expected
    assert actual_baseline == expected_baseline
    assert reports[0][0] == ('receipts-parallel',)
    folder=run/'recovery/receipt-shards-parallel-test'
    manifest=json.loads((folder/'manifest.json').read_text())
    for node in original:
        result=json.loads((folder/(node+'.result.json')).read_text())
        assert result['done_bytes']==result['total_bytes']==manifest['node_total_bytes'][node]
    assert manifest['total_bytes']==sum(manifest['node_total_bytes'].values())
    scratch.close()
