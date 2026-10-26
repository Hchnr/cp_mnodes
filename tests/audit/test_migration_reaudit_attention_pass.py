import gzip
import json
from pathlib import Path
import tempfile
import unittest

from migration_reaudit_attention_pass import prior_budget,digest,prepare


class PassTests(unittest.TestCase):
    def fixture(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name)
        control=root/'control';control.mkdir();(control/'config.json').write_text('{}')
        folder=root/'previous';folder.mkdir();retry=folder/'retry-v1';retry.mkdir()
        manifest=folder/'tasks.gz'
        with gzip.open(manifest,'wt') as stream:stream.write('{}\n')
        snapshot=folder/'snapshot.json';snapshot.write_text(json.dumps(dict(control=str(control),
            config_sha256=digest(control/'config.json'),manifest=str(manifest),sha256=digest(manifest))))
        plan=dict(original_control=str(control),original_snapshot=str(snapshot),
            original_snapshot_sha256=digest(snapshot),accepted_bytes=123)
        (retry/'plan.json').write_text(json.dumps(plan))
        return control,snapshot,retry,plan

    def test_prior_accepted_byte_plans_bound_to_snapshots(self):
        control,snapshot,_,_=self.fixture();total,proofs=prior_budget(control,[snapshot])
        self.assertEqual(total,123);self.assertEqual(proofs[0]['accepted_bytes'],123)

    def test_tampered_previous_manifest_refused(self):
        control,snapshot,_,_=self.fixture();Path(json.loads(snapshot.read_text())['manifest']).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'proof differs'):prior_budget(control,[snapshot])

    def test_negative_prior_budget_refused(self):
        control,snapshot,retry,plan=self.fixture();plan['accepted_bytes']=-1
        (retry/'plan.json').write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError,'proof differs'):prior_budget(control,[snapshot])

    def test_unbounded_pass_refused_before_directory_creation(self):
        control,snapshot,_,_=self.fixture();cfg=dict(job_kind='files',audit_root=str(control.parent))
        (control/'config.json').write_text(json.dumps(cfg));folder=control.parent/'final/pass'
        with self.assertRaises(ValueError):prepare(control,folder,[snapshot],3,99)
        self.assertFalse(folder.exists())


if __name__=='__main__':unittest.main()
