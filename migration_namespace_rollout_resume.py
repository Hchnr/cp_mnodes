#!/usr/bin/env python3
"""Resume only a rollout that failed before ANY receiver plan or drain existed.

Preserve the original frozen coordinator and failed-instance records. Staged
or ambiguous handoffs are deliberately not eligible for this resume path.
"""
import argparse
import fcntl
from pathlib import Path
import sys
import time


def check_pristine(rollout,folder):
    plan=rollout.read(folder/'plan.json');control=Path(plan['control']);cfg=rollout.read(control/'config.json')
    state=rollout.read(folder/'progress.json');old=rollout.read(folder/'driver-instance.json')
    spawn=rollout.read(folder/'driver-spawn.json');who=rollout.identity()
    if (state['status']!='inspection_required' or state['completed'] or plan['nodes']!=rollout.NODES
            or any(old[k]!=spawn[k] for k in ('pid','start')) or not rollout.process_terminal(old)
            or any(old[k]!=who[k] for k in ('host','boot'))):
        raise ValueError('not an actually exited pristine preflight failure')
    pilot=rollout.read(control/'handoffs/worker-0/plan.json')
    if (rollout.base.digest(control/'config.json')!=pilot['config_sha256']
            or cfg['job_kind']!='namespace' or cfg['execution_mode']!='local'
            or (who['host'],who['boot'])!=(cfg['execution_host'],cfg['execution_boot_id'])
            or rollout.base.digest(plan['approval'])!=plan['approval_sha256']):
        raise ValueError('original source/control/approval differs')
    rollout.validate_approval(rollout.read(plan['approval']),control,rollout.NODES[0])
    rollout.assert_files_unchanged(Path(plan['files_control']),plan['file_actors'])
    for node in plan['nodes']:
        if (control/'handoffs'/node).exists():
            raise ValueError('existing handoff evidence: inspect instead of pristine resume')
        current=rollout.read(control/'nodes'/node/'progress.json')
        launch=rollout.read(control/'nodes'/node/'launch.json')
        if (current['status']!='copying' or rollout.process_terminal(current)
                or any(current[k]!=launch[k] for k in ('host','pid','start'))):
            raise ValueError('original namespace actor changed')
    return dict(previous_driver=old,previous_progress=state,all_handoff_folders_absent=True,
                file_actors_unchanged=True,original_namespace_actors_live=True)


def resume(control):
    control=Path(control).resolve();bundle=control/'handoff-code-v2'
    sys.path.insert(0,str(bundle));import migration_namespace_rollout as rollout
    folder=control/'namespace-rollout-v2'
    with open(folder/'pristine-resume.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        proof=check_pristine(rollout,folder)
        attempt=folder/('pristine-resume-'+str(time.time_ns())+'.json')
        original=rollout.exclusive
        def record(path,value):
            if Path(path)==folder/'driver-instance.json':
                return original(attempt,dict(identity=value,pristine_proof=proof,created=time.time()))
            return original(path,value)
        rollout.exclusive=record
        try:rollout.run(folder)
        finally:rollout.exclusive=original


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--control',type=Path,required=True)
    resume(p.parse_args().control)
