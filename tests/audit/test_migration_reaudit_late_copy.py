from pathlib import Path
import tempfile
import unittest
from unittest import mock

import migration_reaudit_late_copy as late


class LateTests(unittest.TestCase):
    def test_combined_threshold_checked_before_preparing_new_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);folder=root/'final/late'
            def read(path):
                if path.name=='report.json':return dict(status='namespace_proof_replayed_new_findings_classified',
                    proof=dict(pending_directories=0),nonoverlapping_new_missing=dict(file=dict(paths=1,logical_bytes=10)))
                return dict(missing=dict(file=dict(logical_bytes=late.THRESHOLD-10)))
            with mock.patch.object(late,'read',side_effect=read):
                with self.assertRaises(ValueError):late.prepare(root,folder)
            self.assertFalse(folder.exists())

    def test_scope_cannot_escape_shared_audit_folder(self):
        with self.assertRaises(ValueError):late.prepare(Path('/audit'),Path('/another'))


if __name__=='__main__':unittest.main()
