import copy
import json
import subprocess
import unittest
from unittest import mock

import audit_priority4_source as audit


class PostCanaryAuditTests(unittest.TestCase):
    def fixture(self):
        return {'nodes':[dict(node=n, identities=[dict(pid=i+1,starttime_ticks=str(i+100),argv=['python','fixture'])
                                                 for i in range(4)]) for n in audit.NODES]}

    def test_complete_post_canary_baseline_and_both_start_field_names(self):
        value = self.fixture()
        item = value['nodes'][0]['identities'][0]
        item['start_ticks'] = item.pop('starttime_ticks')
        self.assertEqual(len(audit.validate_expected_audit(value)),12)

    def test_partial_duplicate_or_unbound_baseline_is_not_a_complete_audit(self):
        for mutate in (lambda v:v['nodes'].pop(),
                       lambda v:v['nodes'][0]['identities'].pop(),
                       lambda v:v['nodes'][0]['identities'][0].update(pid=2),
                       lambda v:v['nodes'][0]['identities'][0].update(argv=[]),
                       lambda v:v['nodes'][0]['identities'][0].update(starttime_ticks='unknown')):
            value=copy.deepcopy(self.fixture());mutate(value)
            with self.assertRaises(ValueError):audit.validate_expected_audit(value)

    def test_native_request_preserves_exact_post_canary_identity_and_no_forwarding(self):
        row=self.fixture()['nodes'][-1]
        response=dict(node=row['node'],host='fixture',identities=row['identities'],verified_at=1)
        completed=subprocess.CompletedProcess(['ssh'],0,json.dumps(response),'')
        with mock.patch.object(audit.subprocess,'run',return_value=completed) as run:
            self.assertEqual(audit.inspect_against(row),response)
        self.assertIn('ForwardAgent=no',run.call_args.args[0])
        self.assertIn('ForwardX11=no',run.call_args.args[0])
        self.assertIn('require_identity(item)',run.call_args.kwargs['input'])
        self.assertIn("'start_ticks': '100'",run.call_args.kwargs['input'])
        self.assertEqual(run.call_args.kwargs['timeout'],40)

    def test_native_failure_is_not_reported_as_unchanged(self):
        with mock.patch.object(audit.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['ssh'])):
            with self.assertRaises(subprocess.CalledProcessError):
                audit.inspect_against(self.fixture()['nodes'][0])


if __name__=='__main__':unittest.main()
