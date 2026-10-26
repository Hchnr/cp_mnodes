#!/usr/bin/env python3
"""Bounded, per-directory-stable metadata tree capture; no file payload opens."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import time


def capture_directory(scan,review,source,target,rel,*,attempts=10,limit=100000):
    scan.parts(rel)
    if not rel or scan.excluded(rel):raise ValueError('invalid namespace capture scope')
    source=Path(source);target=Path(target)
    with ThreadPoolExecutor(max_workers=32) as attributes:
        for attempt in range(attempts):
            fd=scan.directory(source,rel)
            try:
                before=os.fstat(fd)
                with os.scandir(fd) as stream:
                    names=[]
                    for entry in stream:
                        if len(names)>=limit:raise ValueError('directory exceeds bounded capture limit')
                        if not scan.excluded(rel+'/'+entry.name):names.append(entry.name)
                # Capture source versions before comparison. The before/after
                # directory identity and complete nameset close enumeration
                # races, while inspect_group preserves strict no-symlink opens.
                root=dict(rel=rel,kind='directory',source=scan.fields(before))
                views=review.inspect_group(dict(parent=rel.rpartition('/')[0],rows=[root]),source,target,attributes)
                for offset in range(0,len(names),256):
                    # inspect_group already lstat()s every source entry in the
                    # bounded attribute pool. Repeating 18k synchronous stats
                    # here serialized the whole operation and made an otherwise
                    # short stable window impossible for an active directory.
                    # Child directories are captured recursively with their own
                    # fresh before/after identity proof, never skipped.
                    rows=[dict(rel=rel+'/'+name) for name in names[offset:offset+256]]
                    views.extend(review.inspect_group(dict(parent=rel,rows=rows),source,target,attributes))
                after=os.fstat(fd)
                with os.scandir(fd) as stream:
                    current=sorted(e.name for e in stream if not scan.excluded(rel+'/'+e.name))
                # A stable but unlinked directory FD is not proof of the
                # current source pathname. Re-open through the strict path
                # resolver and require the same directory version.
                check=scan.directory(source,rel)
                try:bound=os.fstat(check)
                finally:os.close(check)
                stable=((before.st_ino,before.st_mtime_ns,before.st_ctime_ns)==
                        (after.st_ino,after.st_mtime_ns,after.st_ctime_ns) and current==sorted(names)
                        and (bound.st_ino,bound.st_mtime_ns,bound.st_ctime_ns)==
                            (after.st_ino,after.st_mtime_ns,after.st_ctime_ns)
                        and views and views[0].get('kind')=='directory'
                        and all(views[0].get('source',{}).get(k)==scan.fields(before)[k]
                                for k in ('ino','mode','mtime_ns','ctime_ns'))
                        and len(views)==len(names)+1 and all(v['status'] not in ('error','blocked') for v in views))
                if stable:
                    children=[v['rel'] for v in views[1:] if v.get('kind')=='directory']
                    return dict(rel=rel,before=scan.fields(before),after=scan.fields(after),
                                names=sorted(names),children=children,attempts=attempt+1,views=views,
                                captured_at=time.time(),payload_read=False)
            finally:os.close(fd)
            time.sleep(.1)
    raise RuntimeError('source namespace did not stabilize within bounded attempts: '+rel)


def capture_tree(scan,review,source,target,rel):
    pending=[rel];rows={};proofs=[];visited=set()
    while pending:
        path=pending.pop()
        if path in visited:raise ValueError('duplicate recursive capture ownership')
        visited.add(path)
        try:value=capture_directory(scan,review,source,target,path)
        except FileNotFoundError:
            with ThreadPoolExecutor(max_workers=1) as attributes:
                absent=review.inspect_group(dict(parent=path.rpartition('/')[0],rows=[dict(rel=path)]),
                                            Path(source),Path(target),attributes)[0]
            if absent['status']!='source_disappeared':raise
            rows[path]=absent
            proofs.append(dict(rel=path,status='source_directory_disappeared',captured_at=time.time(),children=[],payload_read=False))
            continue
        for row in value.pop('views'):rows[row['rel']]=row
        pending.extend(value['children']);proofs.append(value)
    return rows,proofs
