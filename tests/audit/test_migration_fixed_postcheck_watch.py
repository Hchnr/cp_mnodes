from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock

import migration_fixed_postcheck_watch as watcher


class WatchTests(unittest.TestCase):
    def execute(self, *, host=None, status='complete', exited=True):
        boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        cfg=dict(job_kind='files',execution_boot_id=boot,shards=dict(node={}))
        state=dict(host=host or socket.gethostname(),status=status,active=0)
        def read(path):return cfg if path.name=='config.json' else state
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(watcher,'read',side_effect=read), \
                mock.patch.object(watcher,'terminal',return_value=exited) as terminal, \
                mock.patch.object(watcher,'run',return_value=dict(status='verified_closed_batch_identities')) as run, \
                mock.patch.object(watcher,'atomic') as atomic:
            watcher.watch(temp)
        return run,terminal,atomic

    def test_only_actual_closed_worker_triggers_audit(self):
        run,terminal,atomic=self.execute()
        run.assert_called_once();terminal.assert_called_once()
        self.assertEqual(atomic.call_args.args[1]['status'],'verified_closed_batch_identities')

    def test_foreign_host_pid_is_not_checked_in_local_proc(self):
        run,terminal,atomic=self.execute(host='foreign-host')
        run.assert_not_called();terminal.assert_not_called()
        self.assertEqual(atomic.call_args.args[1]['status'],'remote_exit_proof_required')

    def test_failed_worker_is_not_restarted_or_audited_as_complete(self):
        run,_,atomic=self.execute(status='failed')
        run.assert_not_called()
        self.assertEqual(atomic.call_args.args[1]['status'],'source_worker_requires_attention')


if __name__=='__main__':unittest.main()
