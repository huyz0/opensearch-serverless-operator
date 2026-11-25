"""Exercise an already prepared independent-shard retirement through Envoy."""

import argparse
import base64
from datetime import datetime
import hashlib
from http.client import HTTPException, HTTPSConnection
import json
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote, urlencode, urlsplit
from uuid import uuid4


def require(condition, message):
    if not condition:
        raise ValueError(message)


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def condition(resource, name):
    return next((item["status"] for item in resource.get("status", {}).get("conditions", ())
                 if item["type"] == name), None)


def poll(label, probe, timeout):
    deadline = time.monotonic() + timeout
    while True:
        result = probe()
        if result is not None:
            return result
        if time.monotonic() >= deadline:
            raise ValueError(f"timed out waiting for {label}")
        time.sleep(1)


class TunnelConnection(HTTPSConnection):
    def __init__(self, host, port, context, destination):
        super().__init__(host, port, context=context, timeout=10)
        self.context = context
        self.destination = destination

    def connect(self):
        connection = socket.create_connection(self.destination, self.timeout)
        try:
            self.sock = self.context.wrap_socket(connection, server_hostname=self.host)
        except OSError:
            connection.close()
            raise


def destination(value):
    endpoint = urlsplit(f"//{value}")
    require(endpoint.hostname and endpoint.port and not endpoint.path and not endpoint.username
            and not endpoint.password and not endpoint.query and not endpoint.fragment,
            "tunnel destination must be HOST:PORT")
    return endpoint.hostname, endpoint.port


def http_client(ca, username, password, tunnel=None):
    context = ssl.create_default_context(cafile=ca)
    auth = base64.b64encode(f"{username}:{password}".encode()).decode()

    def request(url, body=None):
        address = urlsplit(url)
        if tunnel:
            connection = TunnelConnection(address.hostname, address.port, context, tunnel)
        else:
            connection = HTTPSConnection(address.hostname, address.port, timeout=10, context=context)
        headers = {"Authorization": f"Basic {auth}", "Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(body).encode()
        path = address.path
        if address.query:
            path += "?" + address.query
        try:
            connection.request("PUT" if body else "GET", path, body=body, headers=headers)
            reply = connection.getresponse()
            require(200 <= reply.status < 300, f"Envoy returned HTTP {reply.status}")
            return json.load(reply)
        finally:
            connection.close()

    return request


def verify_signature(data, public_key, openssl):
    raw = data["evidence.json"]
    signature = base64.b64decode(data["signature"], validate=True)
    require(len(signature) == 64, "invalid Ed25519 signature length")
    with tempfile.TemporaryDirectory() as directory:
        message = Path(directory, "message")
        detached = Path(directory, "signature")
        message.write_bytes(b"opensearch-serverless-writer-withdrawal/v1\n" + raw.encode())
        detached.write_bytes(signature)
        result = subprocess.run(
            [openssl, "pkeyutl", "-verify", "-pubin", "-inkey", public_key,
             "-rawin", "-in", str(message), "-sigfile", str(detached)],
            capture_output=True, timeout=10,
        )
        require(result.returncode == 0, "signed writer evidence did not verify with the supplied public key")
    return json.loads(raw)


def verify_successor(operation, head, writer, receipt):
    journal = operation["status"]["writerRetirement"]
    entries = journal.get("successors", ())
    require(condition(operation, "WriterSuccessorReady") == "True" and len(entries) == 1,
            "completed operation lacks one verified successor")
    entry = entries[0]
    require(entry["inventoryIndex"] == 0 and journal["headInventory"]["heads"] == [head]
            and entry["nodeID"] == receipt["_node"] != writer
            and entry["term"] == receipt["_primary_term"] > head["term"]
            and entry["podUID"] and entry["ephemeralID"],
            "journal does not match the successor that acknowledged the route write")
    return entry


def live_check(args):
    def get(namespace, resource, name, absent_ok=False):
        command = ["kubectl", "--kubeconfig", args.kubeconfig, "--request-timeout=8s", "-n", namespace,
                   "get", resource, name, "-o", "json"]
        if absent_ok:
            command.append("--ignore-not-found")
        result = subprocess.run(command, capture_output=True, text=True, timeout=12)
        require(result.returncode == 0, f"Kubernetes could not read {namespace}/{resource}/{name}")
        return json.loads(result.stdout) if result.stdout else None

    def operation():
        return get(args.namespace, "opensearchserverlessoperations.serverless.opensearch.org", args.operation)

    for route in (args.index_url, args.search_url):
        url = urlsplit(route)
        require(url.scheme == "https" and url.hostname and not url.username and not url.password
                and not url.query and not url.fragment, "supply verified HTTPS route base URLs")
    require(args.timeout > 0, "timeout must be positive")
    require(args.index_url.rstrip("/") != args.search_url.rstrip("/"), "index and search routes must differ")
    password = Path(args.password_file).read_text(encoding="utf-8").rstrip("\r\n")
    require(args.username and ":" not in args.username and password, "Basic Auth credentials are incomplete")
    tunnel = destination(args.envoy_tunnel) if args.envoy_tunnel else None
    request = http_client(args.ca, args.username, password, tunnel)
    first = operation()
    uid, spec = first["metadata"]["uid"], first["spec"]
    target, instance_ref = spec["writerRetirement"], spec["instanceRef"]
    require(spec["action"] == "RetireWriter" and first["status"]["phase"] == "Running",
            "start with a running writer retirement")

    def victim():
        return get(args.namespace, "pod", target["podName"], absent_ok=True)

    original = victim()
    require(original and original["metadata"]["uid"] == target["podUID"]
            and not original["metadata"].get("deletionTimestamp"), "exact original writer must still be running")
    job_namespace, job_name = args.publisher_job.split("/", 1)
    original_job = get(job_namespace, "job", job_name)
    publisher = next(c for c in original_job["spec"]["template"]["spec"]["containers"] if c["name"] == "publisher")
    flags = publisher["args"]
    require(all(flag in flags for flag in ("--writer-withdrawal", f"--namespace={args.namespace}",
                                            f"--operation={args.operation}")), "Job targets a different operation")
    report_ref = next(flag.split("=", 1)[1] for flag in flags if flag.startswith("--writer-report-configmap="))
    report_namespace, report_name = report_ref.split("/", 1)
    require(report_namespace == job_namespace and condition(original_job, "Complete") != "True",
            "start before the staged Job publishes its report")
    print("Waiting for the staged publisher. Capture and submit the Envoy report now.", file=sys.stderr, flush=True)

    def job_complete():
        job = get(job_namespace, "job", job_name)
        require(job["metadata"]["uid"] == original_job["metadata"]["uid"] and condition(job, "Failed") != "True",
                "publisher Job was replaced or failed")
        return job if condition(job, "Complete") == "True" else None

    job = poll("publisher Job completion", job_complete, args.timeout)
    evidence_name = "writer-withdrawal-" + hashlib.sha256(uid.encode()).hexdigest()[:24]
    data = get(args.namespace, "configmap", evidence_name)["data"]
    evidence = verify_signature(data, args.platform_public_key, args.openssl)
    report = json.loads(get(report_namespace, "configmap", report_name)["data"]["report.json"])
    require(report == evidence and evidence["namespace"] == args.namespace
            and evidence["operationUID"] == uid and evidence["instanceUID"] == instance_ref["uid"]
            and evidence["victimPodUID"] == target["podUID"]
            and evidence["authProbeURL"].rstrip("/") == args.index_url.rstrip("/")
            and evidence["servingProxyUIDs"]
            and timestamp(evidence["observedAt"]) > timestamp(job["status"]["startTime"]),
            "signed evidence differs from this Job's Envoy report or retirement")

    def read_operation():
        current = operation()
        require(current["metadata"]["uid"] == uid and current["status"]["phase"] != "Failed",
                "writer retirement was replaced or failed")
        return current

    def instance():
        current = get(args.namespace, "opensearchserverlesses.serverless.opensearch.org", instance_ref["name"])
        require(current["metadata"]["uid"] == instance_ref["uid"], "instance UID changed")
        return current

    def evicted():
        current = read_operation()
        journal = current["status"].get("writerRetirement", {})
        require(current["status"]["phase"] != "Succeeded" and not journal.get("successors"),
                "retirement already verified a successor before the controlled write")
        if not (journal.get("admissionClosed") and journal.get("evictionArmed") and journal.get("victimGone")
                and journal.get("releaseReceipt") and condition(current, "WriterSuccessorReady") == "False"):
            return None
        remaining = victim()
        if remaining and remaining["metadata"]["uid"] == target["podUID"]:
            return None
        claim = instance()["status"].get("activeOperation") or {}
        require(claim.get("uid") == uid and claim.get("action") == "RetireWriter", "retirement claim is missing")
        return journal

    before = poll("operator eviction and release receipt", evicted, args.timeout)
    heads = before["headInventory"]["heads"]
    writer = before["nodeID"]
    require(len(heads) == 1 and heads[0]["index"] == args.index and heads[0]["indexUUID"]
            and not heads[0].get("bucketID"), "fixture must have one inventoried independent head for the test index")
    head = heads[0]
    require(evidence["writer"] == {"node_id": writer, "ephemeral_id": before["nodeEphemeralID"]},
            "signed report did not identify the retired JVM")
    require(all(evidence[key] == before[key] for key in ("serviceUID", "routeUID", "gatewayUID", "gatewayServiceUID")),
            "signed Envoy route identities differ from the withdrawal journal")
    index = quote(args.index, safe="")
    document = "writer-retirement-" + uuid4().hex
    message = "successor-after-retirement"
    result = request(f"{args.index_url.rstrip('/')}/{index}/_doc/{document}?refresh=true", {"message": message})
    require(result["_id"] == document and result["_index"] == args.index and result["_shard"] == head["shard"]
            and result["_primary_term"] > head["term"] and result["_node"] != writer
            and result["_shards"]["successful"] > 0 and result["result"] == "created",
            "index route did not acknowledge the test write against the same shard on a newer writer")

    def search(document_id, expected):
        query = urlencode({"q": f"_id:{document_id}"})
        hits = request(f"{args.search_url.rstrip('/')}/{index}/_search?{query}")["hits"]["hits"]
        if not hits:
            return None
        require(len(hits) == 1 and hits[0]["_id"] == document_id and hits[0]["_index"] == args.index
                and hits[0]["_source"]["message"] == expected, "search route returned the wrong hit or payload")
        return document_id

    found = [poll(f"search hit {doc}", lambda: search(doc, expected), args.timeout)
             for doc, expected in ((args.seed_id, args.seed_message), (document, message))]

    def completed():
        current = read_operation()
        if current["status"]["phase"] == "Succeeded" and not instance()["status"].get("activeOperation"):
            return verify_successor(current, head, writer, result)
        return None

    successor = poll("verified successor and claim release", completed, args.timeout)
    return {"operationUID": uid, "jobUID": job["metadata"]["uid"], "victimPodUID": target["podUID"],
            "signedReportObservedAt": evidence["observedAt"], "retiredTerm": head["term"],
            "evictionArmed": True, "victimGone": True, "successor": successor,
            "write": result, "searchHits": found, "claimReleased": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("kubeconfig", "namespace", "operation", "publisher-job", "platform-public-key", "index-url",
                "search-url", "ca", "username", "password-file", "index", "seed-id", "seed-message"):
        parser.add_argument(f"--{arg}", required=True)
    parser.add_argument("--timeout", type=int, default=150, help="maximum seconds to wait for each condition")
    parser.add_argument("--envoy-tunnel", help="connect both routes via HOST:PORT while verifying the URL hostnames")
    parser.add_argument("--openssl", default="openssl")
    args = parser.parse_args()
    try:
        print(json.dumps(live_check(args), indent=2))
    except (ValueError, KeyError, OSError, HTTPException, StopIteration, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"Writer retirement drill failed: {error}\n")


if __name__ == "__main__":
    main()
