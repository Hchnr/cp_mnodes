#!/usr/bin/env python3
"""Unapplied second P4 canary; fixed binding of the existing transaction.

The CLI must only be staged/applied after the second explicit permission.
Importing/configuring this module neither stages authority nor runs actions.
Native validation packs intentionally do not contain authorization.json.
"""
from contextlib import contextmanager
import deploy_priority4_path_canary as transaction


BUNDLE = transaction.CONTROL / 'p4-copy-nearest-cd98275b19f14511b72dcbbe523da05e'
AUTHORIZATION = '允许，验证后做第二次单进程对照'
SCOPE = 'priority4-nearest-readonly-parent-checkpoint-v1'
STRATEGY = 'nearest_native_readonly'
PREVIOUS = transaction.CONTROL / 'p4-copy-path-0f78c471bb9e4a99afdd2260b4ac4fae'
PREDECESSOR_LOG = PREVIOUS / 'controller-8d15cd7be4964331aa7a5ae164259bfd' / 'worker.log'
ENTRY = 'deploy_priority4_nearest_canary.py'
FILES = (ENTRY, 'deploy_priority4_path_canary.py', 'rollout_cache_age.py',
         'rollout_cache_cluster.py', 'migration_io.py', 'audit_priority4_source.py')


@contextmanager
def configured():
    values = dict(BUNDLE=BUNDLE, AUTHORIZATION=AUTHORIZATION, SCOPE=SCOPE,
        CHECKPOINT=BUNDLE / 'cache-age-rollout' / (transaction.HOST + '-supplement.json'),
        CONTROLLER_ENTRY=ENTRY, CANDIDATE_ENTRY='migration_priority4_nearest_worker.py',
        CANDIDATE_MODULE='migration_priority4_nearest_worker', DIRECTORY_STRATEGY=STRATEGY,
        PREDECESSOR_LOG=PREDECESSOR_LOG, FILES=FILES)
    previous = {key: getattr(transaction, key) for key in values}
    try:
        for key, value in values.items():
            setattr(transaction, key, value)
        yield transaction
    finally:
        for key, value in previous.items():
            setattr(transaction, key, value)


def main():
    with configured():
        transaction.main()


if __name__ == '__main__':
    main()
