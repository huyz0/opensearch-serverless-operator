import unittest
from datetime import datetime, timezone

from diagnose_operator import age, diagnosis


NOW = datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc)


class DiagnosisTests(unittest.TestCase):
    def setUp(self):
        self.instance = {"metadata": {"uid": "instance-1"}, "status": {
            "activeOperation": {"name": "retire", "action": "RetireWriter", "uid": "operation-1", "generation": 1},
        }}
        self.operation = {
            "metadata": {"uid": "operation-1", "generation": 1, "creationTimestamp": "2026-01-01T00:00:00Z"},
            "spec": {"instanceRef": {"uid": "instance-1"}, "writerRetirement": {"podName": "writer-1", "podUID": "pod-1"}},
            "status": {"phase": "Running", "conditions": [{
                "type": "WriterReleaseReady", "status": "False", "reason": "ReceiptUnavailable",
                "lastTransitionTime": "2026-01-01T00:01:00Z", "observedGeneration": 1,
            }]},
        }

    def test_separates_operation_age_from_condition_age(self):
        output = "\n".join(diagnosis(self.instance, self.operation, NOW))
        self.assertIn("operation age: 0h 2m 0s", output)
        self.assertIn("Writer Pod writer-1 (UID pod-1)", output)
        self.assertIn("ReceiptUnavailable, condition state age: 0h 1m 0s", output)
        self.assertIn("S3 read access", output)

    def test_stale_condition_and_replaced_operation_do_not_offer_recovery(self):
        self.operation["status"]["conditions"][0]["observedGeneration"] = 0
        output = "\n".join(diagnosis(self.instance, self.operation, NOW))
        self.assertIn("stale observation", output)
        self.assertNotIn("Next:", output)
        self.operation["metadata"]["uid"] = "operation-2"
        output = "\n".join(diagnosis(self.instance, self.operation, NOW))
        self.assertIn("could not be verified", output)
        self.assertNotIn("ReceiptUnavailable", output)

    def test_provisioning_hold_reports_the_exact_attempt(self):
        self.instance["status"] = {"blockedProvisioningAttempts": [{
            "role": "search", "zone": "a", "name": "search-1", "podUID": "pod-1",
            "reason": "PodGone", "startedAt": "2026-01-01T00:00:00Z",
        }]}
        output = "\n".join(diagnosis(self.instance, None, NOW))
        self.assertIn("search/a Pod search-1 (UID pod-1), PodGone, attempt age: 0h 2m 0s", output)
        self.assertIn("RetryProvisioning", output)
        self.assertEqual(age("", NOW), "unknown")

    def test_named_operation_can_be_diagnosed_before_it_claims_the_instance(self):
        self.instance["status"] = {}
        self.operation["metadata"]["name"] = "stage"
        self.operation["status"]["conditions"][0].update(type="Ready", reason="CandidateUnavailable")
        output = "\n".join(diagnosis(self.instance, self.operation, NOW))
        self.assertIn("no matching claim", output)
        self.assertIn("preview Services", output)
        self.operation["spec"]["instanceRef"]["uid"] = "old-instance"
        output = "\n".join(diagnosis(self.instance, self.operation, NOW))
        self.assertIn("different instance UID", output)
        self.assertNotIn("Next:", output)


if __name__ == "__main__":
    unittest.main()
