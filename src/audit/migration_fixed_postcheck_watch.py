#!/usr/bin/env python3
"""Wait for actual file-worker exits, then run the read-only closed-batch audit.

Never signals/restarts copy actors or edits frozen manifests/config/authority.
"""
import argparse
import fcntl
import os
from pathlib import Path
import socket
import time

from migration_fixed_postcheck import run, terminal
from migration_reaudit import atomic
from migration_reaudit_shared import read


def watch(control):
    control=Path(control).resolve();cfg=read(control/'config.json')
    if cfg['job_kind']!='files':raise ValueError('ordinary-file control required')
    with open('/tmp/cp-mnodes-postcheck-watch-'+control.name+'.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        return watch_locked(control,cfg)


def watch_locked(control,cfg):
    identity=dict(pid=os.getpid(),start=Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()[19],
                  boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    while True:
        rows=[];closed=True
        for node in cfg['shards']:
            state=read(control/'nodes'/node/'progress.json')
            if (state.get('host')!=socket.gethostname()
                    or cfg.get('execution_boot_id')!=identity['boot']):
                atomic(control/'post-copy-watch-state.json',dict(status='remote_exit_proof_required',
                    identity=identity,updated=time.time(),node=node,copy_restarted=False))
                return
            exited=terminal(state)
            returned=exited and state['status'].startswith('complete') and not state.get('active')
            rows.append(dict(node=node,actual_process_terminal=exited,status=state['status'],returned=returned))
            if exited and not returned:
                atomic(control/'post-copy-watch-state.json',dict(status='source_worker_requires_attention',
                    identity=identity,updated=time.time(),nodes=rows,copy_restarted=False))
                return
            closed=closed and returned
        atomic(control/'post-copy-watch-state.json',dict(status='starting_postcheck' if closed else 'waiting_for_worker_exits',
            identity=identity,updated=time.time(),nodes=rows,copy_restarted=False))
        if closed:break
        time.sleep(30)
    result=run(control)
    atomic(control/'post-copy-watch-state.json',dict(status=result['status'],identity=identity,
        updated=time.time(),result=str(control/'post-copy-audit/result.json'),copy_restarted=False,
        whole_tree_completion_proven=False))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True)
    watch(p.parse_args().control)
