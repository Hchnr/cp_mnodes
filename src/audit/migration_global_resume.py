#!/usr/bin/env python3
"""Resume a drained scanner with constant-time pending-directory accounting."""
import argparse
import collections
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import threading
import time

import migration_global_scan as original


class ResumeAudit(original.Audit):
    def __init__(self, source, target, output, baseline, threads=32, stat_threads=64):
        # Reuse only a fully drained, fsynced run. The CLI checks its ownership.
        self.source,self.target,self.output=Path(source),Path(target),Path(output)
        self.exclusions=tuple(baseline['excluded'])
        self.frontier=self.output/'frontier.sqlite'
        self.threads=threads;self.mutex=threading.Lock();self.stop=threading.Event()
        self.counts=collections.Counter(baseline['counts'])
        self.groups=collections.defaultdict(collections.Counter,
            {k:collections.Counter(v) for k,v in baseline['groups'].items()})
        self.started=baseline['started']
        self.pending=baseline['pending_directories']
        self.files={name:open(self.output/(name+'.jsonl'),'a',encoding='utf-8')
            for name in ('missing','conflicts','metadata','errors','source_links')}
        self.attributes=ThreadPoolExecutor(max_workers=stat_threads,thread_name_prefix='audit-stat')

    def run(self):
        db=sqlite3.connect(self.frontier)
        db.execute('PRAGMA journal_mode=DELETE');db.execute('PRAGMA synchronous=FULL')
        if db.execute("SELECT 1 FROM frontier WHERE status='running' LIMIT 1").fetchone():
            raise ValueError('old scanner did not drain running directories')
        pending=self.pending;status='scanning';last=0
        try:
            with ThreadPoolExecutor(max_workers=self.threads,thread_name_prefix='audit-directory') as pool:
                jobs={}
                while True:
                    selected=[] if self.stop.is_set() else db.execute(
                        "SELECT rel,leaves_only FROM frontier WHERE status='pending' LIMIT ?",
                        (max(0,self.threads-len(jobs)),)).fetchall()
                    for rel,leaves in selected:
                        changed=db.execute("UPDATE frontier SET status='running' WHERE rel=? AND status='pending'",(rel,)).rowcount
                        if changed!=1:raise ValueError('frontier ownership changed')
                    db.commit()
                    pending-=len(selected)
                    if pending<0:raise ValueError('negative pending count')
                    for rel,leaves in selected:
                        jobs[pool.submit(self.scan_directory,(rel,bool(leaves)))]=rel
                    if not jobs:
                        if not self.stop.is_set() and pending:
                            raise ValueError('pending count differs from frontier')
                        break
                    done,_=wait(jobs,timeout=.5,return_when=FIRST_COMPLETED)
                    for future in done:
                        rel=jobs.pop(future);children=future.result()
                        before=db.total_changes
                        db.executemany("INSERT OR IGNORE INTO frontier VALUES(?,?,'pending')",
                            [(path,int(leaves)) for path,leaves in children])
                        pending+=db.total_changes-before
                        db.execute("UPDATE frontier SET status='done' WHERE rel=?",(rel,))
                    db.commit()
                    if time.monotonic()-last>=5:
                        self.progress(status,pending,len(jobs));last=time.monotonic()
            status='stopped' if self.stop.is_set() else 'complete'
            if not self.stop.is_set() and db.execute("SELECT 1 FROM frontier WHERE status!='done' LIMIT 1").fetchone():
                raise ValueError('frontier not fully closed')
        except BaseException as exc:
            status='failed';self.error('','audit_controller',exc)
            raise
        finally:
            result=self.progress(status,pending,0)
            result['enumeration_complete']=status=='complete'
            result['coverage_without_observation_errors']=(status=='complete' and not any(
                self.counts.get(k) for k in ('errors','unsupported','changed_source_directories')))
            original.scanner.core.write_json(self.output/'result.json',result)
            self.attributes.shutdown(wait=True)
            for f in self.files.values():f.flush();os.fsync(f.fileno());f.close()
            db.close()
        return result


def previous_returned(pid,start):
    try:fields=Path('/proc',str(pid),'stat').read_text().rsplit(') ',1)[1].split()
    except FileNotFoundError:return True
    return fields[19]!=start or fields[0] in ('Z','X')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--handoff',type=Path,required=True)
    a=p.parse_args();handoff=json.loads(a.handoff.read_text())
    root=Path(__file__).resolve().parents[1]
    if root.parent!=Path('/share-mixed/project') or not root.name.startswith('.hcr-global-check-'):
        raise ValueError('wrong control root')
    folder=a.handoff.resolve().parent
    if not folder.is_relative_to(root/'nodes'):
        raise ValueError('foreign handoff')
    if socket.gethostname()!=handoff['host'] or not previous_returned(handoff['old_pid'],handoff['old_start']):
        raise ValueError('old scanner still active or wrong node')
    plan=json.loads((folder/'plan.json').read_text())
    if (plan['source'],plan['target'],plan['exclusions'])!=('/share/project','/share-mixed/project',['eai_pwm/datasets/raw']):
        raise ValueError('scope differs')
    if plan['host']!=socket.gethostname():raise ValueError('plan host differs')
    report=folder/'report';baseline=handoff['baseline']
    if baseline['status']!='stopped' or baseline['active_directories']!=0 or baseline['pid']!=handoff['old_pid']:
        raise ValueError('not a stopped scanner checkpoint')
    if any(baseline[k] for k in ('payload_bytes_read','payload_bytes_written','copy_tasks_published')):
        raise ValueError('non-readonly checkpoint')
    if json.loads((folder/'scan.log').read_text().splitlines()[-1])!=baseline:
        raise ValueError('post-close footer differs')
    for name,identity in handoff['files'].items():
        path=report/name;s=path.stat()
        if dict(ino=s.st_ino,size=s.st_size,mtime_ns=s.st_mtime_ns)!=identity:
            raise ValueError('checkpoint file changed: '+name)
    original.scanner.COVERED_ROOTS.update(plan['coverage_roots'])
    with (folder/'resume-claim-v1.json').open('x') as f:
        json.dump(dict(pid=os.getpid(),handoff_sha256=hashlib.sha256(a.handoff.read_bytes()).hexdigest()),f)
        f.flush();os.fsync(f.fileno())
    old_result=report/'result.json'
    if json.loads(old_result.read_text())!=baseline:raise ValueError('result/checkpoint differs')
    old_result.rename(report/'stopped-result-v1.json')
    os.nice(10)
    audit=ResumeAudit(plan['source'],plan['target'],report,baseline)
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:audit.stop.set())
    audit.progress('scanning',audit.pending,0)
    original.scanner.core.write_json(folder/'resume-ready-v1.json',dict(pid=os.getpid(),at=time.time()))
    print(json.dumps(audit.run()),flush=True)


if __name__=='__main__':main()
