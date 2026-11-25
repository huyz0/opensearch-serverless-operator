"""Verify the upstream CA trust selected by active Envoy Gateway endpoints."""

import argparse
import base64
import hashlib
import json
import re
import ssl
import sys
from collections import defaultdict
from pathlib import Path


def fingerprints(pem: str) -> set[str]:
    blocks = re.findall(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", pem, re.S)
    return {hashlib.sha256(ssl.PEM_cert_to_DER_cert(block)).hexdigest() for block in blocks}


def upstream_context(socket: dict, secrets: dict[str, list[dict]]) -> list[dict]:
    if socket.get("name") != "envoy.transport_sockets.tls":
        raise ValueError("endpoint does not select an upstream TLS socket")
    context = socket.get("typed_config", {}).get("common_tls_context", {})
    combined = context.get("combined_validation_context", {})
    sds = combined.get("validation_context_sds_secret_config", context.get("validation_context_sds_secret_config"))
    inline_contexts = [context.get("validation_context", {}), combined.get("default_validation_context", {})]
    if sds is not None:
        if "ads" not in sds.get("sds_config", {}):
            raise ValueError("only ADS validation secrets can be attributed in the active dump")
        name = sds.get("name")
        entries = secrets.get(name, [])
        if len(entries) != 1 or "validation_context" not in entries[0]:
            raise ValueError(f"expected one active validation secret for {name!r}")
        inline_contexts.append(entries[0]["validation_context"])
    return inline_contexts


def socket_roots(socket: dict, secrets: dict[str, list[dict]]) -> set[str]:
    contexts = upstream_context(socket, secrets)
    if any(ctx.get("trust_chain_verification", "VERIFY_TRUST_CHAIN") != "VERIFY_TRUST_CHAIN" for ctx in contexts):
        raise ValueError("backend does not require a trusted certificate chain")
    if any("custom_validator_config" in ctx or "system_root_certs" in ctx for ctx in contexts):
        raise ValueError("backend has a custom validator or additional system trust")
    sources = [ctx["trusted_ca"] for ctx in contexts if "trusted_ca" in ctx]
    if len(sources) != 1:
        raise ValueError("expected exactly one active upstream CA source")
    source = sources[0]
    if "inline_string" in source:
        return fingerprints(source["inline_string"])
    if "inline_bytes" in source:
        return fingerprints(base64.b64decode(source["inline_bytes"], validate=True).decode("ascii"))
    raise ValueError("expected an inline active upstream CA bundle")


def endpoint_socket(cluster: dict, endpoint: dict) -> dict:
    if "transport_socket_matcher" in cluster:
        raise ValueError("unsupported transport socket matcher replaces endpoint metadata selection")
    matches = cluster.get("transport_socket_matches", [])
    if not matches:
        return cluster.get("transport_socket", {})
    metadata = endpoint.get("metadata", {}).get("filter_metadata", {}).get("envoy.transport_socket_match")
    if not isinstance(metadata, dict) or set(metadata) != {"name"} or not isinstance(metadata["name"], str) or not metadata["name"]:
        raise ValueError("expected named TLS socket-selection metadata from Envoy Gateway")
    for match in matches:
        criteria = match["match"]
        if not isinstance(criteria, dict) or set(criteria) != {"name"} or not isinstance(criteria["name"], str) or not criteria["name"]:
            raise ValueError("expected a named TLS transport socket match from Envoy Gateway")
        if criteria["name"] == metadata["name"]:
            return match["transport_socket"]
    raise ValueError("endpoint does not match a declared TLS socket")


def verify_backend_trust(dump: dict, namespace: str, routes: list[str], expected: set[str]) -> dict[str, int]:
    if not routes or not expected:
        raise ValueError("route names and at least one expected CA certificate are required")
    configs = dump["configs"]
    clusters = [item["cluster"] for config in configs for item in config.get("dynamic_active_clusters", [])]
    assignments = {
        item["endpoint_config"]["cluster_name"]: item["endpoint_config"]
        for config in configs
        for item in config.get("dynamic_endpoint_configs", [])
    }
    secrets = defaultdict(list)
    for config in configs:
        for item in config.get("dynamic_active_secrets", []):
            secrets[item["name"]].append(item["secret"])
    for config in configs:
        for item in config.get("dynamic_warming_secrets", []):
            secrets.pop(item["name"], None)
    counts = {}
    for route in routes:
        prefix = f"httproute/{namespace}/{route}/"
        scoped = [cluster for cluster in clusters if cluster["name"].startswith(prefix)]
        if not scoped:
            raise ValueError(f"no active cluster for {namespace}/{route}")
        count = 0
        for cluster in scoped:
            name = cluster["name"]
            assignment = assignments.get(name, cluster.get("load_assignment", {}))
            endpoints = [endpoint for locality in assignment.get("endpoints", []) for endpoint in locality.get("lb_endpoints", [])]
            if not endpoints:
                raise ValueError(f"No active endpoints for {name}. Capture the dump with include_eds.")
            for endpoint in endpoints:
                try:
                    observed = socket_roots(endpoint_socket(cluster, endpoint), secrets)
                except ValueError as error:
                    raise ValueError(f"{name}: {error}") from error
                if observed != expected:
                    raise ValueError(f"{name}: unexpected CA fingerprints {sorted(observed)}")
            count += len(endpoints)
        counts[route] = count
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config_dump", type=Path, help="Envoy /config_dump?include_eds=true JSON")
    parser.add_argument("ca_bundle", type=Path, help="expected CA certificates in a PEM bundle")
    parser.add_argument("--namespace", required=True, help="namespace of the HTTPRoutes")
    parser.add_argument("--route", action="append", required=True, help="HTTPRoute name, repeat for each role")
    args = parser.parse_args()
    try:
        expected = fingerprints(args.ca_bundle.read_text(encoding="ascii"))
        dump = json.loads(args.config_dump.read_text(encoding="utf-8"))
        counts = verify_backend_trust(dump, args.namespace, args.route, expected)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(error, file=sys.stderr)
        return 1
    for route, count in counts.items():
        print(f"{args.namespace}/{route}: {count} endpoints trust {', '.join(sorted(expected))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
