#!/usr/bin/env python3
"""Persistently receive global scan results, then run read-only revalidation."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import time

import migration_global_check as scan
import migration_global_finish as closure
import migration_global_review as review


def progress(root,status,**values):
    record=dict(status=status,pid=os.getpid(),updated=time.time(),**values)
    scan.atomic(root/'pipeline-progress.json',record)
    print(json.dumps(record),flush=True)


def review_progress(root):
    manifest=scan.read(root/'final/revalidation/manifest.json')
    total=done=processed=missing_bytes=0
    for plan in manifest['plans']:
        if not plan['records']:continue
        total+=1
        folder=root/'final/revalidation/nodes'/('dev-review-'+str(plan['slot']))
        report=folder/'report'
        try:result=scan.read(report/'result.json')
        except FileNotFoundError:
            try:result=scan.read(report/'progress.json')
            except FileNotFoundError:continue
        if result['status']=='failed':raise RuntimeError('review failed: slot '+str(plan['slot']))
        processed+=result['counts'].get('processed',0)
        missing_bytes+=result['counts'].get('missing_logical_bytes',0)
        if result['status']=='complete':
            path=folder/'review.log'
            with path.open('rb') as stream:
                stream.seek(max(0,path.stat().st_size-1024*1024))
                lines=stream.read().splitlines()
            try:footer=json.loads(lines[-1])
            except (ValueError,IndexError):continue
            done+=footer==result
    return dict(total=total,returned=done,processed=processed,expected=manifest['records'],missing_bytes=missing_bytes)


def run(root,interval=30):
    scan.check_root(root);review.policy(root)
    with (root/'pipeline.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        # A stopped/failed controller must be diagnosed before any uncertain
        # preparation or remote launch is retried.
        with (root/'pipeline-claim.json').open('x') as f:
            json.dump(dict(pid=os.getpid(),at=time.time()),f);f.flush();os.fsync(f.fileno())
        try:
            while True:
                current=scan.snapshot(root)
                state=scan.read(root/'final/index-state.json')
                if current['statuses'].get('complete',0)>state['returned_scanners']:
                    closure.index(root)
                progress(root,'waiting_primary',entries=current['counts'].get('entries',0),
                    statuses=current['statuses'],missing_bytes=current['counts'].get('missing_logical_bytes',0))
                if current['enumeration_complete']:
                    state=closure.index(root)
                    if state['status']!='indexed_all_closed_reports':raise ValueError('closed inputs not fully indexed')
                    break
                if current['statuses'].get('failed') or current['statuses'].get('stopped'):
                    raise RuntimeError('primary scanner stopped/failed; inspect its evidence before continuing')
                time.sleep(interval)
            progress(root,'preparing_candidate_review')
            planned=review.prepare(root)
            progress(root,'launching_candidate_review',plan=planned)
            launched=review.launch(root)
            if launched['errors']:raise RuntimeError('uncertain candidate review launch; inspect launch registry')
            while True:
                status=review_progress(root)
                progress(root,'reviewing_candidates',review=status)
                if status['returned']==status['total']:break
                time.sleep(interval)
            progress(root,'merging_candidate_review')
            result=review.merge(root)
            progress(root,'awaiting_namespace_closure',report=str(root/'final/global-reviewed-manifest.json'),
                missing=result['missing'],below_threshold=result['below_threshold'],
                changed_scopes=result['changed_scopes'],unresolved_access_paths=result['unresolved_access_paths'])
            return result
        except BaseException as exc:
            progress(root,'failed',error=repr(exc))
            raise


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    a=p.parse_args();run(a.root)
