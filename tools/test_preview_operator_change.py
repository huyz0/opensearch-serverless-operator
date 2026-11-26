import unittest
from copy import deepcopy
from io import StringIO
from unittest.mock import patch

from preview_operator_change import LABEL_PREFIX, capacity_lines, main, route_lines


def instance_fixture():
    return {
        "metadata": {"name": "catalog", "uid": "owner", "generation": 3},
        "spec": {
            "pools": {"search": {"zones": {"a": {"replicas": 2, "minServing": 1}}}},
            "rollout": {"candidate": {"fleet": "green"}},
        },
        "status": {
            "servingFleetIntentByRole": {"search": "blue", "ingest": "blue"},
            "zoneCapacityObservedGeneration": 3,
            "zoneCapacityObservedAt": "2026-01-01T00:00:00Z",
            "zoneCapacity": [{"role": "search", "zone": "a", "fleet": "blue", "kubernetesReady": 1}],
            "candidateFleet": {"fleet": "green", "generation": 3, "revision": "v2"},
        },
    }


def service(name, role, fleet):
    return {
        "metadata": {
            "name": name,
            "ownerReferences": [{"name": "catalog", "uid": "owner", "kind": "OpenSearchServerless", "controller": True}],
        },
        "spec": {"selector": {
            LABEL_PREFIX + "instance-uid": "owner", LABEL_PREFIX + "role": role, LABEL_PREFIX + "fleet": fleet,
        }},
    }


class PreviewTests(unittest.TestCase):
    def test_capacity_includes_spares_stale_readiness_and_removed_zones(self):
        instance = instance_fixture()
        proposed = deepcopy(instance)
        proposed["spec"]["pools"]["search"]["zones"] = {"b": {"replicas": 2, "warmSpare": 1, "minServing": 2}}
        self.assertEqual(list(capacity_lines(instance, proposed))[-2:], ["search\ta\t2\t0\t0\t1", "search\tb\t0\t3\t2\t-"])
        instance["metadata"]["generation"] += 1
        self.assertEqual(list(capacity_lines(instance, proposed))[-2], "search\ta\t2\t0\t0\t-")

    def test_promotion_uses_live_service_selectors_and_rejects_wrong_owner(self):
        instance = instance_fixture()
        services = {"catalog-search": service("catalog-search", "search", "green"), "catalog-index": service("catalog-index", "ingest", "blue")}
        lines = list(route_lines(instance, services, "promote"))
        self.assertIn("  search catalog-search: green -> green", lines)
        self.assertIn("  ingest catalog-index: blue -> green", lines)
        self.assertIn("  Recorded search intent is blue; inspect the Service and any active operation.", lines)
        services["catalog-index"]["metadata"]["ownerReferences"][0]["uid"] = "previous-instance"
        with self.assertRaisesRegex(ValueError, "unexpected ownership"):
            list(route_lines(instance, services, "promote"))

    def test_rollback_uses_common_retained_target_and_rejects_unsupported_modes(self):
        instance = instance_fixture()
        rollback = {"roles": {"search": {"fleet": "blue"}, "ingest": {"fleet": "blue"}}}
        instance["status"]["rollbackTarget"] = rollback
        services = {"catalog-search": service("catalog-search", "search", "green"), "catalog-index": service("catalog-index", "ingest", "green")}
        lines = list(route_lines(instance, services, "rollback"))
        self.assertIn("  search catalog-search: green -> blue", lines)
        self.assertIn("  ingest catalog-index: green -> blue", lines)
        rollback["roles"]["ingest"]["fleet"] = "other"
        with self.assertRaisesRegex(ValueError, "common retained target"):
            list(route_lines(instance, services, "rollback"))
        rollback["weighted"] = True
        with self.assertRaisesRegex(ValueError, "weighted Gateway rollback"):
            list(route_lines(instance, services, "rollback"))

    def test_cli_previews_using_only_namespaced_gets(self):
        args = ["preview_operator_change.py", "promote", "catalog", "-n", "demo"]
        responses = [instance_fixture(), {"items": [
            service("catalog-search", "search", "blue"), service("catalog-index", "ingest", "blue"),
        ]}]
        with patch("sys.argv", args), patch("sys.stdout", new_callable=StringIO) as output, patch(
            "preview_operator_change.kubectl_get", side_effect=responses
        ) as kubectl_get:
            self.assertEqual(main(), 0)
        self.assertEqual([call.args for call in kubectl_get.call_args_list], [
            ("demo", "opensearchserverless", "catalog"), ("demo", "service", "catalog-search", "catalog-index"),
        ])
        self.assertIn("search catalog-search: blue -> green", output.getvalue())


if __name__ == "__main__":
    unittest.main()
