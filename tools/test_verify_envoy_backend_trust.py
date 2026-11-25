import base64
import unittest

from verify_envoy_backend_trust import endpoint_socket, fingerprints, socket_roots, verify_backend_trust


def pem(value: bytes) -> str:
    return "-----BEGIN CERTIFICATE-----\n" + base64.b64encode(value).decode() + "\n-----END CERTIFICATE-----\n"


def socket(bundle: str) -> dict:
    return {
        "name": "envoy.transport_sockets.tls",
        "typed_config": {"common_tls_context": {"validation_context": {"trusted_ca": {"inline_string": bundle}}}},
    }


def endpoint(match: str) -> dict:
    return {"metadata": {"filter_metadata": {"envoy.transport_socket_match": {"name": match}}}}


class EnvoyBackendTrustTest(unittest.TestCase):
    def test_unused_socket_cannot_hide_an_endpoint_trusting_the_old_root(self) -> None:
        current, old = pem(b"current-root"), pem(b"old-root")
        name = "httproute/test/catalog-search/rule/0"
        cluster = {
            "name": name,
            "transport_socket_matches": [
                {"match": {"name": "current"}, "transport_socket": socket(current)},
                {"match": {"name": "old"}, "transport_socket": socket(old)},
            ],
        }
        locality = {"lb_endpoints": [endpoint("current")]}
        dump = {"configs": [
            {"dynamic_active_clusters": [{"cluster": cluster}]},
            {"dynamic_endpoint_configs": [{"endpoint_config": {"cluster_name": name, "endpoints": [locality]}}]},
        ]}
        expected = fingerprints(current)
        self.assertEqual(verify_backend_trust(dump, "test", ["catalog-search"], expected), {"catalog-search": 1})
        locality["lb_endpoints"].append(endpoint("old"))
        with self.assertRaisesRegex(ValueError, "unexpected CA fingerprints"):
            verify_backend_trust(dump, "test", ["catalog-search"], expected)
        locality["lb_endpoints"][-1] = endpoint("other")
        with self.assertRaisesRegex(ValueError, "does not match a declared TLS socket"):
            verify_backend_trust(dump, "test", ["catalog-search"], expected)
        locality["lb_endpoints"][-1] = endpoint("current")
        cluster["transport_socket_matcher"] = {}
        with self.assertRaisesRegex(ValueError, "unsupported transport socket matcher"):
            verify_backend_trust(dump, "test", ["catalog-search"], expected)

    def test_validation_secret_must_be_unambiguous_and_ads(self) -> None:
        current = pem(b"current-root")
        upstream = socket(current)
        upstream["typed_config"]["common_tls_context"] = {
            "combined_validation_context": {"validation_context_sds_secret_config": {"name": "backend-ca", "sds_config": {"ads": {}}}}
        }
        secret = {"validation_context": {"trusted_ca": {"inline_string": current}}}
        self.assertEqual(socket_roots(upstream, {"backend-ca": [secret]}), fingerprints(current))
        with self.assertRaisesRegex(ValueError, "expected one active validation secret"):
            socket_roots(upstream, {"backend-ca": [secret, secret]})
        cluster = {
            "name": "httproute/test/catalog-index/rule/0",
            "transport_socket": upstream,
            "load_assignment": {"endpoints": [{"lb_endpoints": [{}]}]},
        }
        entry = {"name": "backend-ca", "secret": secret}
        dump = {"configs": [
            {"dynamic_active_clusters": [{"cluster": cluster}]},
            {"dynamic_active_secrets": [entry], "dynamic_warming_secrets": [entry]},
        ]}
        with self.assertRaisesRegex(ValueError, "expected one active validation secret"):
            verify_backend_trust(dump, "test", ["catalog-index"], fingerprints(current))
        upstream["typed_config"]["common_tls_context"]["combined_validation_context"]["validation_context_sds_secret_config"]["sds_config"] = {}
        with self.assertRaisesRegex(ValueError, "only ADS validation secrets"):
            socket_roots(upstream, {"backend-ca": [secret]})

    def test_only_named_gateway_matches_are_supported(self) -> None:
        cluster = {"transport_socket_matches": [{"match": {"name": "active", "values": [True]}}]}
        with self.assertRaisesRegex(ValueError, "expected a named TLS transport socket match"):
            endpoint_socket(cluster, endpoint("active"))
        with self.assertRaisesRegex(ValueError, "expected named TLS socket-selection metadata"):
            endpoint_socket(cluster, endpoint(""))
