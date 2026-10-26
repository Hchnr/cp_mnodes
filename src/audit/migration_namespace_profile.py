#!/usr/bin/env python3
"""Profile directory-only copies into a NEW dedicated shared control folder.

Never opens payloads, changes existing production targets, or modifies workers.
The optional O_PATH comparison affects only ancestor FDs in this process;
leaf FDs remain O_RDONLY for metadata and fsync.
"""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import threading
import time

import migration_reaudit_copy as entry
from migration_reaudit import atomic


def run(receipts, output, limit=32):
    output=Path(output).absolute()
    if not str(output).startswith('/share-mixed/project/.hcr-readonly-audit-'):
        raise ValueError('dedicated audit folder required')
    if not 1<=limit<=64:raise ValueError('bounded sample required')
    paths=[]
    with open(receipts) as stream:
        for line in stream:
            if not line.endswith('\n'):continue
            row=json.loads(line)
            if row.get('kind')=='directory' and row.get('status') in ('copied','skipped'):
                entry.base.engine._components(row['rel'])
                if row['rel']=='eai_pwm/datasets/raw' or row['rel'].startswith('eai_pwm/datasets/raw/'):
                    raise ValueError('excluded path')
                paths.append(row['rel'])
            if len(paths)>=limit:break
    if len(set(paths))!=limit:raise ValueError('insufficient unique directory sample')
    output.mkdir(exist_ok=False)
    engine=entry.base.engine
    entries=[dict(engine._record(os.stat('/share/project/'+rel,follow_symlinks=False)),
                  rel=rel,kind='directory') for rel in paths]
    original_parent=engine._open_parent;original_meta=engine._metadata_fd
    results=[]
    for mode in ('readonly','path'):
        target=output/mode;target.mkdir()
        cfg=dict(source='/share/project',target=str(target))
        engine._open_parent=original_parent
        entry.base.install_engine(cfg)
        if mode=='path':
            from migration_parent_frontier import FrontierParentOpener
            opener=entry.base.native.open_parent_path
            def strict_parent(root,rel,create=False):return opener(root,rel)
            engine._open_parent=FrontierParentOpener(str(target),strict_parent,
                check=engine._check_cancel,opener=opener,anchor_opener=opener)
        parent=engine._open_parent;stats=collections.defaultdict(lambda:[0,0.,0.]);lock=threading.Lock()
        def timed(label, fn, *args, **kwargs):
            started=time.monotonic()
            try:return fn(*args,**kwargs)
            finally:
                seconds=time.monotonic()-started
                with lock:
                    v=stats[label];v[0]+=1;v[1]+=seconds;v[2]=max(v[2],seconds)
        def parent_call(root,rel,create=False):
            return timed('parent_target' if root==str(target) else 'parent_source',parent,root,rel,create=create)
        def metadata(*args,**kwargs):return timed('metadata_total',original_meta,*args,**kwargs)
        engine._open_parent=parent_call;engine._metadata_fd=metadata
        names=('open','stat','fstat','mkdir','fchmod','fchown','listxattr','setxattr','utime','fsync')
        originals={name:getattr(os,name) for name in names if hasattr(os,name)}
        for name,fn in originals.items():
            def wrap(*args,_name=name,_fn=fn,**kwargs):return timed(_name,_fn,*args,**kwargs)
            setattr(os,name,wrap)
        started=time.monotonic()
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                rows=list(pool.map(lambda item:entry.copy_directory(cfg,item),entries))
        finally:
            for name,fn in originals.items():setattr(os,name,fn)
            engine._open_parent=original_parent;engine._metadata_fd=original_meta
        result=dict(mode=mode,seconds=time.monotonic()-started,
                    statuses=dict(collections.Counter(r['status'] for r in rows)),
                    operations={k:dict(calls=v[0],sum_seconds=v[1],max_seconds=v[2]) for k,v in stats.items()},
                    rows=rows)
        results.append(result)
        atomic(output/(mode+'-result.json'),result)
        print(json.dumps({k:result[k] for k in ('mode','seconds','statuses','operations')}),flush=True)
    atomic(output/'result.json',dict(directory_sample=limit,threads=8,results=results,
        production_target_mutated=False,payload_copied=False,live_workers_modified=False))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--receipts',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--limit',type=int,default=32);a=p.parse_args();run(a.receipts,a.output,a.limit)
