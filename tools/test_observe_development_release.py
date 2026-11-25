"""Check that rollout observations reject stale or changed live identities."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from observe_development_release import (
    FLEET_LABEL, INSTANCE_LABEL, ROLE_LABEL,
    manager_identity, require_preserved_workload, workload_identity,
)


NOW = datetime(2026, 11, 25, tzinfo=timezone.utc)
MANAGER = "registry.example/controller@sha256:" + "a" * 64
JAVA = "registry.example/java@sha256:" + "b" * 64


class ReleaseObservationTests(unittest.TestCase):
    def test_ready_manager_must_hold_a_current_lease(self):
        containers = [{"name": "operator", "image": MANAGER}]
        deploy = {
            "metadata": {"generation": 1},
            "spec": {"replicas": 1, "template": {"spec": {"containers": containers}}},
            "status": {"observedGeneration": 1, "readyReplicas": 1, "updatedReplicas": 1},
        }
        pod = {
            "metadata": {"name": "operator-one", "uid": "manager-uid"},
            "spec": {"containers": containers},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{
                    "name": "operator", "ready": True, "imageID": MANAGER,
                    "state": {"running": {"startedAt": (NOW - timedelta(seconds=20)).isoformat()}},
                }],
            },
        }
        lease = {"spec": {"holderIdentity": "operator-one_process", "leaseDurationSeconds": 15,
                          "renewTime": (NOW - timedelta(seconds=14)).isoformat()}}
        self.assertEqual(manager_identity(deploy, [pod], lease, MANAGER, NOW)["uid"], "manager-uid")
        with self.assertRaisesRegex(ValueError, "current leader Lease"):
            manager_identity(deploy, [pod], lease, MANAGER, NOW + timedelta(seconds=1))
        restarted = deepcopy(pod)
        restarted["status"]["containerStatuses"][0]["state"]["running"]["startedAt"] = NOW.isoformat()
        with self.assertRaisesRegex(ValueError, "current leader Lease"):
            manager_identity(deploy, [restarted], lease, MANAGER, NOW)
        lease["spec"]["holderIdentity"] = "operator-two_process"
        with self.assertRaisesRegex(ValueError, "current leader Lease"):
            manager_identity(deploy, [pod], lease, MANAGER, NOW)

    def test_workload_must_have_recent_coverage_and_preserve_route_identity(self):
        fleets = {"search": "initial", "ingest": "initial"}
        instance = {
            "metadata": {"uid": "catalog-uid", "generation": 1},
            "spec": {"image": JAVA},
            "status": {
                "conditions": [{"type": "Available", "status": "True", "observedGeneration": 1,
                                "reason": "ReadyRoleEndpointsObserved"}],
                "servingFleetIntentByRole": fleets,
                "lastEndpointObservation": {"observedGeneration": 1, "observedServingFleets": fleets,
                                            "observedAt": NOW.isoformat(), "role": "search",
                                            "serviceUID": "svc-search"},
            },
        }
        services, pods, slices = {}, [], {}
        for index, role in enumerate(fleets, 1):
            labels = {INSTANCE_LABEL: "catalog-uid", ROLE_LABEL: role, FLEET_LABEL: "initial"}
            owner = [{"kind": "OpenSearchServerless", "uid": "catalog-uid", "controller": True}]
            service_name = f"catalog-{role}"
            services[role] = {"metadata": {"name": service_name, "namespace": "app",
                                           "uid": f"svc-{role}", "ownerReferences": owner},
                              "spec": {"clusterIP": f"10.0.0.{index}", "selector": labels}}
            pods.append({"metadata": {"name": f"catalog-java-{role}", "uid": f"pod-{role}",
                                      "labels": labels, "ownerReferences": owner},
                         "spec": {"containers": [{"name": "opensearch", "image": JAVA}]},
                         "status": {"conditions": [{"type": "Ready", "status": "True"}],
                                    "podIP": f"10.0.1.{index}"}})
            slices[service_name] = [{
                "metadata": {
                    "labels": {"endpointslice.kubernetes.io/managed-by": "endpointslice-controller.k8s.io"},
                    "ownerReferences": [{"kind": "Service", "uid": f"svc-{role}", "controller": True}],
                },
                "endpoints": [{
                    "addresses": [f"10.0.1.{index}"], "conditions": {"ready": True},
                    "targetRef": {"kind": "Pod", "namespace": "app", "name": f"catalog-java-{role}",
                                  "uid": f"pod-{role}"},
                }],
            }]
        workload = workload_identity(instance, pods, services, slices, JAVA, NOW)
        self.assertEqual(workload["roles"]["search"]["readyPodUIDs"], ["pod-search"])
        with self.assertRaisesRegex(ValueError, "verified endpoint coverage"):
            workload_identity(instance, pods, services, slices, JAVA, NOW + timedelta(minutes=2))

        replaced = deepcopy(pods)
        replaced[0]["metadata"]["uid"] = "replacement"
        with self.assertRaisesRegex(ValueError, "outside the verified Pod set"):
            workload_identity(instance, replaced, services, slices, JAVA, NOW)

        moved = deepcopy(slices)
        moved["catalog-search"][0]["endpoints"][0]["addresses"] = ["10.0.1.9"]
        with self.assertRaisesRegex(ValueError, "endpoint IP does not match"):
            workload_identity(instance, pods, services, moved, JAVA, NOW)

        absent = {**slices, "catalog-search": []}
        with self.assertRaisesRegex(ValueError, "endpoints have not converged"):
            workload_identity(instance, pods, services, absent, JAVA, NOW)

        earlier = {"context": "test", "namespace": "app", "workload": workload,
                   "manager": {"uid": "previous", "image": "previous image"}}
        later = {**deepcopy(earlier), "manager": {"uid": "next", "image": "next image"}}
        require_preserved_workload(earlier, later)
        later["workload"]["roles"]["search"]["serviceUID"] = "recreated"
        with self.assertRaisesRegex(ValueError, "changed workload"):
            require_preserved_workload(earlier, later)


if __name__ == "__main__":
    unittest.main()
