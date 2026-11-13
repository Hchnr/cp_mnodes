import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'src/core'))


def load(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'ops/copy'/(name+'.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


copy=load('projset_recovery_copy')
parallel=load('projset_parallel_receipts')
prefetch=load('projset_prefetch_receipts')


def write(path, rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))


def test_candidate_frontier_includes_active_and_post_snapshot_ids(tmp_path):
    run=tmp_path/'run';(run/'recovery').mkdir(parents=True)
    write(run/'tasks.jsonl',[{'id':1},{'id':2},{'id':3}])
    path=run/'recovery/tasks.001.jsonl';write(path,[{'id':4},{'id':5},{'id':6}])
    with path.open('a') as stream:stream.write('{"id":7')
    snapshot={'state':'copying','pending':{'copy':0},'discovered_jobs':3,'active':2,
              'workers':{'a':{'active':{'2':'copy','3':'scan'}}}}
    jobs,last,final=prefetch.candidate_jobs(run,snapshot)
    assert jobs=={2,4,5,6}
    assert (last,final)==(3,6)
    snapshot['pending']['copy']=1
    with pytest.raises(ValueError,match='frontier'):
        prefetch.candidate_jobs(run,snapshot)


def test_prefetch_reuse_matches_serial_and_rejects_changed_receipts(tmp_path,monkeypatch):
    run=tmp_path/'run';(run/'recovery').mkdir(parents=True)
    original={'node-a':{'copied':1,'processed':1,'copied_bytes':9},'node-b':{'processed':0}}
    original_path=run/'recovery/original-counts.json';original_path.write_text(json.dumps(original))
    write(run/'tasks.jsonl',[{'id':1,'type':'scan','rel':''},
         {'id':2,'type':'copy','entries':[{'rel':'a'}]},{'id':3,'type':'copy','entries':[{'rel':'b'}]}])
    write(run/'completed.jsonl',[{'id':3}])
    write(run/'nodes/node-a/receipts.jsonl',[{'job':2,'rel':'a','status':'copied','logical_bytes':9}])
    write(run/'nodes/node-a/attempt-a/receipts.jsonl',[{'job':2,'rel':'a','status':'copied','logical_bytes':7},
                                                       {'job':3,'rel':'b','status':'skipped','logical_bytes':2}])
    write(run/'nodes/node-b/receipts.jsonl',[])
    write(run/'nodes/node-b/attempt-a/receipts.jsonl',[{'job':2,'rel':'a','status':'error','detail':'last'},
                                                       {'job':3,'rel':'b','status':'copied','logical_bytes':5}])
    monkeypatch.delenv('PROJSET_RECOVERY_GENERATION',raising=False)
    monkeypatch.setenv('PROJSET_FAST_REBUILD','1')
    monkeypatch.delenv('PROJSET_PARALLEL_RECEIPTS',raising=False)
    serial=copy.rebuild(run,str(tmp_path/'serial.sqlite'))
    expected=serial.execute('select * from prior order by rel').fetchall()
    expected_baseline=json.loads((run/'recovery/baseline.json').read_text());serial.close()
    monkeypatch.setenv('PROJSET_ATTEMPT','prefetch-test')
    monkeypatch.setenv('PROJSET_PREFETCH_MODE','1')
    predb=sqlite3.connect(':memory:');predb.execute('create table prior(rel text primary key,job integer,payload text)')
    baseline=parallel.rebuild_receipts(run,predb,{2,3,99},original,lambda *a,**k:None,
                                       lambda node,command:['/bin/sh','-c',command])
    assert baseline==expected_baseline
    folder=run/'recovery/receipt-shards-prefetch-test'
    handover=run/'recovery/handover';handover.mkdir()
    saved_status=handover/'status.json'
    saved_status.write_text(json.dumps({'counts':{'copied':sum(x.get('copied',0) for x in expected_baseline.values())}}))
    (run/'recovery/legacy-cluster-fence.json').write_text(json.dumps({'handover_dir':str(handover)}))
    pointer={'state':'ready','parent_attempt':'live-test','prefetch_attempt':'prefetch-test','generation':None,
             'candidate_sha256':hashlib.sha256((folder/'pending.json').read_bytes()).hexdigest(),
             'original_sha256':hashlib.sha256(original_path.read_bytes()).hexdigest(),
             'snapshot_sha256':hashlib.sha256(saved_status.read_bytes()).hexdigest()}
    (run/'recovery/prefetched-receipts.json').write_text(json.dumps(pointer))
    monkeypatch.setenv('PROJSET_ATTEMPT','live-test');monkeypatch.delenv('PROJSET_PREFETCH_MODE',raising=False)
    target=sqlite3.connect(':memory:');target.execute('create table prior(rel text primary key,job integer,payload text)')
    reused=parallel.rebuild_receipts(run,target,{2},original,lambda *a,**k:None,
                                     lambda node,command:pytest.fail('must reuse verified shards'))
    assert reused==expected_baseline
    assert target.execute('select * from prior order by rel').fetchall()==expected
    assert json.loads((run/'recovery/prefetch-reuse.json').read_text())['state']=='used'
    with pytest.raises(ValueError,match='cover'):
        parallel.reuse_prefetch(run,target,{999},original)
    before=saved_status.read_bytes()
    saved_status.write_text(json.dumps({'counts':{'copied':10_000}}))
    bad_pointer=dict(pointer,snapshot_sha256=hashlib.sha256(saved_status.read_bytes()).hexdigest())
    with pytest.raises(ValueError,match='below saved snapshot'):
        parallel.reuse_prefetch(run,target,{2},original,bad_pointer)
    saved_status.write_bytes(before)
    with (run/'nodes/node-b/attempt-a/receipts.jsonl').open('a') as stream:
        stream.write(json.dumps({'job':3,'rel':'b','status':'skipped','logical_bytes':1})+'\n')
    changed=sqlite3.connect(':memory:');changed.execute('create table prior(rel text primary key,job integer,payload text)')
    fresh=parallel.rebuild_receipts(run,changed,{2},original,lambda *a,**k:None,
                                    lambda node,command:['/bin/sh','-c',command])
    assert json.loads((run/'recovery/prefetch-reuse.json').read_text())['state']=='rejected'
    serial=copy.rebuild(run,str(tmp_path/'serial-after.sqlite'))
    assert changed.execute('select * from prior order by rel').fetchall()==serial.execute('select * from prior order by rel').fetchall()
    assert fresh==json.loads((run/'recovery/baseline.json').read_text())
    serial.close();predb.close();target.close();changed.close()
