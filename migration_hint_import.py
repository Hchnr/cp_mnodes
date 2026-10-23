#!/usr/bin/env python3
"""Read a fenced predecessor's unacknowledged hints; never copy payload data.

Old acknowledgements affect this optional fast-feed only. Full distributed
source discovery AND the following full scan remain mandatory, so a lost old
assignment or partial copy can never be hidden by an old scheduling receipt.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import os
from pathlib import Path
import socket
import time
import uuid

from migration_io import read_json, write_json
from migration_frontier_resume import candidate
from migration_run import validate_mounts


def run(control):
    cfg=read_json(control/'config.json');host=socket.gethostname()
    if host not in cfg['copy_hosts']:raise ValueError('import host is excluded')
    validate_mounts(cfg)
    old=Path(cfg['resume_from']);old_cfg=read_json(old/'config.json')
    if not (old/'STOP').is_file() or old_cfg['run_id']==cfg['run_id']:
        raise ValueError('predecessor must be a fenced different namespace')
    if any(old_cfg[k]!=cfg[k] for k in ('source','destination')):
        raise ValueError('predecessor hint scope differs')
    lock=open('/tmp/'+cfg['run_id']+'-hint-import.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    plan=read_json(control/'hint-import-plan.json')
    if plan.get('control')!=str(control) or plan.get('run_id')!=cfg['run_id']:
        raise ValueError('hint import plan differs')
    target=control/'frontier-input'/host/'ready'
    progress=target.parent/'import-progress.json'
    counts={'files_read':0,'entries_offered':0,'files_deprioritized':0}
    began=time.monotonic();last=0;batch=[]
    def save(status,error=None):
        write_json(progress,dict(counts,host=host,run_id=cfg['run_id'],pid=os.getpid(),
            status=status,error=error,updated=time.time(),elapsed=time.monotonic()-began))
    def flush():
        if batch:
            write_json(target/(uuid.uuid4().hex+'.json'),dict(control=str(control),
                run_id=cfg['run_id'],host=host,entries=list(batch)))
            batch.clear()
    def read(item):
        old_host,path=item;v=read_json(path)
        if (v.get('control')!=str(old) or v.get('run_id')!=old_cfg['run_id']
                or v.get('host')!=old_host or not isinstance(v.get('entries'),list)
                or len(v['entries'])>512):raise ValueError('old hint identity/entries differ')
        return [e for e in (candidate(v) for v in v['entries']) if e is not None]
    def paths():
        stop_until=0
        for old_host in plan['hosts'][host]:
            if old_host not in old_cfg['copy_hosts']:raise ValueError('invalid old host')
            ignored=frozenset(plan['deprioritized'].get(old_host,[]))
            with os.scandir(old/'frontier-input'/old_host/'ready') as entries:
                for e in entries:
                    if time.monotonic()>=stop_until:
                        if (control/'STOP').exists():raise RuntimeError('STOP requested')
                        stop_until=time.monotonic()+.1
                    if not e.name.endswith('.json'):continue
                    if not e.is_file(follow_symlinks=False):raise ValueError('nonregular old hint')
                    if e.name in ignored:
                        counts['files_deprioritized']+=1;continue
                    yield old_host,Path(e.path)
    try:
        save('importing')
        # executor.map eagerly consumes its iterable; page it to bound memory
        # and preserve STOP responsiveness while the old directory is large.
        from itertools import islice
        stream=iter(paths())
        with ThreadPoolExecutor(max_workers=16) as pool:
            while True:
                page=list(islice(stream,256))
                if not page:break
                for values in pool.map(read,page):
                    counts['files_read']+=1
                    for value in values:
                        batch.append(value);counts['entries_offered']+=1
                        if len(batch)==512:flush()
                flush()
                if time.monotonic()-last>3:save('importing');last=time.monotonic()
        flush();save('complete')
    except BaseException as e:
        save('failed',repr(e));raise
    finally:lock.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True)
    run(p.parse_args().control.resolve())


if __name__=='__main__':main()
