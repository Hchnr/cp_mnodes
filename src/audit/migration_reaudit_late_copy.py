#!/usr/bin/env python3
"""Prepare a distinct verified delta without modifying the running batch."""
import json
from pathlib import Path
import shutil
import time

from migration_reaudit import atomic
from migration_reaudit_finish import digest
from migration_reaudit_shared import read

THRESHOLD=50000000000000


def prepare(root,folder):
    root=Path(root);folder=Path(folder)
    if not folder.is_relative_to(root/'final'):raise ValueError('shared subordinate scope required')
    source=root/'final/incremental-namespace-closure/verified/report.json';proof=read(source)
    if proof['status']!='namespace_proof_replayed_new_findings_classified' or proof['proof']['pending_directories']:
        raise ValueError('incremental proof incomplete')
    primary=read(root/'final/final-report.json')
    files=proof['nonoverlapping_new_missing']['file']
    if primary['missing']['file']['logical_bytes']+files['logical_bytes']>=THRESHOLD:
        raise ValueError('combined observed missing scope not strictly below 50 TB')
    for kind in ('files','directories','symlinks'):
        item=proof['outputs'][kind]
        if digest(item['path'])!=item['sha256']:raise ValueError('incremental output changed')
    request=root/'explicit-copy-request.json'
    if read(request).get('directive')!='开始拷贝吧，开大并发尽快完成':raise ValueError('explicit start authority missing')
    folder.mkdir(exist_ok=False);(folder/'final').mkdir()
    atomic(folder/'plan.json',read(root/'plan.json'))
    shutil.copyfile(request,folder/'explicit-copy-request.json')
    outputs={k:proof['outputs'][k] for k in ('files','directories','symlinks')}
    report=dict(status='verified_manifest_scope_only',updated=time.time(),source='/share/project',target='/share-mixed/project',
        exclusions=['eai_pwm/datasets/raw'],whole_tree_completion_proven=False,missing=proof['nonoverlapping_new_missing'],
        outputs=outputs,source_proof=str(source),source_proof_sha256=digest(source),
        not_in_primary_frozen_queues=True,inflight_source_changes_deferred=proof['inflight_source_changes'],
        unresolved_access_errors=0,content_hashes_verified=False,data_copy_started=False)
    atomic(folder/'final/final-report.json',report)
    atomic(folder/'final/conditional-copy-authorization.json',dict(kind='explicit-user-verified-scope-copy-v1',
        threshold_bytes_exclusive=THRESHOLD,final_report_sha256=digest(folder/'final/final-report.json'),
        verified_path_manifest_only=True,explicit_user_request_sha256=digest(folder/'explicit-copy-request.json'),
        all_completion_gates_verified=False,unresolved_access_errors=0,ordinary_files=files,outputs=outputs))
    atomic(root/'final/late-copy-1.json',dict(audit_root=str(folder),source_proof=str(source),
        files=files['paths'],bytes=files['logical_bytes'],updated=time.time(),primary_queues_untouched=True))
    return report
