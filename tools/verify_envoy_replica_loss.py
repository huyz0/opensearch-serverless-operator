"""Run authenticated weighted promotion and rollback with proxy loss in the Kind fixture."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time


NS = "operator-envoy-loss"
GATEWAY_NS = "envoy-gateway-system"
SELECTOR = ("gateway.envoyproxy.io/owning-gateway-name=secure-rollout,"
            f"gateway.envoyproxy.io/owning-gateway-namespace={NS}")
PREFIX = "serverless.opensearch.org/"
ROUTES = {"search": "catalog-search", "ingest": "catalog-index"}
HOSTS = {role: f"{prefix}-edge.{NS}.svc.cluster.local" for role, prefix in (("search", "search"), ("ingest", "index"))}


def now():
    return datetime.now(timezone.utc).isoformat()


def run(argv, data=None, timeout=30):
    result = subprocess.run(argv, input=data, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"{Path(argv[0]).name} exited with {result.returncode}")
    return result.stdout


def kube(*args, data=None):
    return run(["kubectl", "--request-timeout=12s", *args], data)


def get(resource, *args, namespace=None):
    namespace = namespace or NS
    for attempt in range(4):
        try:
            return json.loads(kube("-n", namespace, "get", resource, *args, "-o", "json"))
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            transient = isinstance(error, subprocess.TimeoutExpired) or "Unable to connect to the server" in str(error)
            if not transient or attempt == 3:
                raise
            time.sleep(1)


def ready(pod):
    return not pod["metadata"].get("deletionTimestamp") and any(
        c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", [])
    )


def wait_for(check, seconds, description):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if result := check():
            return result
        time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {description}")


class Fixture:
    def __init__(self, directory, resume=False):
        self.directory = directory
        self.run_id = str(time.time_ns())
        self.results = directory / (f"replica-loss-{self.run_id}" if resume else "replica-loss")
        self.results.mkdir(exist_ok=False)
        self.tls = ssl.create_default_context(cafile=str(directory / "edge.crt"))
        password = (directory / "config" / "app-a").read_bytes()
        self.authorization = "Basic " + base64.b64encode(b"fixture-client:" + password).decode("ascii")
        self.public_key = (directory / "weighted-platform-key.txt").read_text().strip()
        (service,) = get("services", "-l", SELECTOR, namespace=GATEWAY_NS)["items"]
        self.service = service["metadata"]
        self.tunnels = {}
        self.trace = []
        self.promotion_fault_trace = []
        self.writes = {"1": "secure warmup"}
        if resume:
            # Keep every acknowledgment from the interrupted promotion in the recovery check.
            for old in sorted(directory.glob("replica-loss*"), key=lambda path: path.stat().st_mtime):
                trace = old / "trace.json"
                journal = old / "events.jsonl"
                if trace.is_file():
                    prior = json.loads(trace.read_text(encoding="utf-8"))
                elif journal.is_file():
                    prior = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines(keepends=True)
                             if line.endswith("\n")]
                else:
                    continue
                prior_writes = [row for row in prior if row["event"] == "traffic" and row["role"] == "ingest"]
                for sequence, row in enumerate(prior_writes, 1):
                    if row["code"] in (200, 201):
                        document = row.get("document", f"{row['operation']}-{sequence}")
                        self.writes[document] = document
                if any(row["event"] == "diagnosis" and row["operation"] == "promote-proxy-loss" for row in prior):
                    self.promotion_fault_trace = [row for row in prior if row.get("operation") == "promote-proxy-loss"]
        self.lock = threading.Lock()

    def record(self, event, **fields):
        with self.lock:
            entry = {"event": event, "at": now(), **fields}
            self.trace.append(entry)
            with (self.results / "events.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(entry) + "\n")

    def proxies(self):
        return {p["metadata"]["uid"]: p["metadata"]["name"] for p in
                get("pods", "-l", SELECTOR, namespace=GATEWAY_NS)["items"] if ready(p)}

    def forward(self, proxies):
        for uid, name in proxies.items():
            if uid in self.tunnels:
                continue
            log = self.results / ("forward-" + uid + ".log")
            with log.open("w", encoding="utf-8") as output:
                child = subprocess.Popen(["kubectl", "-n", GATEWAY_NS, "port-forward", "pod/" + name,
                                          ":19000", ":10443", "--address=127.0.0.1"],
                                         stdout=output, stderr=subprocess.STDOUT)
            try:
                def read_ports():
                    if child.poll() is not None:
                        raise RuntimeError(f"Could not forward the exact proxy {name}")
                    ports = {}
                    for line in log.read_text(encoding="utf-8").splitlines():
                        if line.startswith("Forwarding from 127.0.0.1:"):
                            address, target = line.removeprefix("Forwarding from ").split(" -> ")
                            ports[target] = address
                    return ports if {"19000", "10443"} <= ports.keys() else None

                ports = wait_for(read_ports, 15, f"proxy {name} port forward")
                self.tunnels[uid] = (ports, child)
            except BaseException:
                child.terminate()
                child.wait(timeout=8)
                raise
        return {uid: {"adminAddress": self.tunnels[uid][0]["19000"], "clientAddress": self.tunnels[uid][0]["10443"]}
                for uid in proxies}

    def close(self):
        for _, child in self.tunnels.values():
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=8)
        (self.results / "trace.json").write_text(json.dumps(self.trace, indent=2), encoding="utf-8")

    def publish(self, operation, evidence, weighted=False):
        command = [str(self.directory / "publisher.exe"), "--namespace", NS, "--operation", operation,
                   "--signing-key-file", str(self.directory / "rollout-signing.pem")]
        if weighted:
            command.append("--weighted-route")
        run(command, json.dumps(evidence))

    def approval_pod(self, pod):
        meta = pod["metadata"]
        labels = meta["labels"]
        uid = labels[PREFIX + "instance-uid"]
        role = labels[PREFIX + "role"]
        suffix = hashlib.sha256(uid.encode()).hexdigest()[:20]
        host = f"{pod['spec']['hostname']}.peer-{suffix}.{NS}.svc.cluster.local"
        script = ('exec curl -fsS --max-time 5 --cacert /trust/ca.crt --resolve "$1:9200:$2" '
                  '--user "fixture-client:$(cat /app-a/password)" '
                  '"https://$1:9200/_serverless/stats" -w "\\n" "https://$1:9200/_serverless/ready"')
        observed_at = now()
        stats, health = (json.loads(line) for line in kube("-n", NS, "exec", "tls-probe", "--", "sh", "-c", script,
                                                          "shell", host, pod["status"]["podIP"]).splitlines())
        process = stats.get("process_incarnation")
        if not (process and stats["name"] == meta["name"] and role in stats["roles"] and stats["lease"]["valid_now"]
                and health["ready"] and health["process_incarnation"] == process):
            raise RuntimeError("A Java Pod did not qualify")
        if role == "search" and (health["warmup_result"] != "completed" or health["warmup_timed_out"]):
            raise RuntimeError("Search warm-up did not qualify")
        node = get("node", pod["spec"]["nodeName"])["metadata"]
        zone = node["labels"]["topology.kubernetes.io/zone"]
        if zone != labels[PREFIX + "zone"]:
            raise RuntimeError("Java Pod zone and Node zone differ")
        return (role, labels[PREFIX + "fleet"], labels[PREFIX + "revision"]), observed_at, {
            "uid": meta["uid"], "resourceVersion": meta["resourceVersion"], "nodeName": node["name"],
            "zone": zone, "processIncarnation": process,
        }

    def approve(self, operation, instance):
        pods = [p for p in get("pods", "-l", PREFIX + "instance-uid=" + instance["metadata"]["uid"])["items"] if ready(p)]
        if len(pods) != 6:
            raise RuntimeError("Expected six Ready Java Pods across initial and green")
        cohorts = {}
        with ThreadPoolExecutor(max_workers=6) as pool:
            for key, at, report in pool.map(self.approval_pod, pods):
                cohorts.setdefault(key, []).append((at, report))
        for (role, fleet, revision), cohort in cohorts.items():
            if not operation.get("status", {}).get("weightedRollout") and fleet != operation["spec"]["targetFleet"]:
                continue
            self.publish(operation["metadata"]["name"], {
                "namespace": NS, "instanceUID": instance["metadata"]["uid"],
                "instanceGeneration": instance["metadata"]["generation"], "operationUID": operation["metadata"]["uid"],
                "role": role, "fleet": fleet, "revision": revision, "observedAt": min(at for at, _ in cohort),
                "pods": [report for _, report in cohort], "peerQualified": True, "securityQualified": True,
                "rollbackCompatible": True,
            })

    def reports(self, name, proxies):
        addresses = self.forward(proxies)
        with tempfile.TemporaryDirectory() as directory:
            tunnels = Path(directory) / "tunnels.json"
            tunnels.write_text(json.dumps(addresses), encoding="utf-8")

            def collect(role):
                return json.loads(run([str(self.directory / "observer.exe"), "--namespace", NS, "--operation", name,
                                       "--role", role, "--gateway-namespace", GATEWAY_NS, "--gateway-service", self.service["name"],
                                       "--gateway-service-uid", self.service["uid"], "--route-url", "https://" + HOSTS[role],
                                       "--route-ca-file", str(self.directory / "edge.crt"), "--tunnels-file", str(tunnels),
                                       "--route-auth-secret", "rollout-login", "--platform-key", self.public_key], timeout=42))

            with ThreadPoolExecutor(max_workers=2) as pool:
                return list(pool.map(collect, ROUTES))

    def connection(self, proxy, role):
        host = HOSTS[role]
        address = self.tunnels[proxy][0]["10443"]
        connection = http.client.HTTPSConnection(host, context=self.tls, timeout=3)
        connection._create_connection = lambda _, timeout, source: socket.create_connection(
            ("127.0.0.1", int(address.rsplit(":", 1)[1])), timeout, source
        )
        return connection

    def request(self, connection, role, method, path, body=None):
        connection.request(method, path, body=body, headers={
            "Authorization": self.authorization, "Host": HOSTS[role], "Content-Type": "application/json",
        })
        reply = connection.getresponse()
        return reply.status, reply.read()

    def traffic(self, stop, role, victim, survivor, name):
        proxy = victim
        connection = self.connection(proxy, role)
        sample = 0
        try:
            while not stop.is_set():
                sample += 1
                doc = f"{name}-{self.run_id}-{sample}"
                started = time.monotonic()
                try:
                    if role == "ingest":
                        code, body = self.request(connection, role, "PUT", f"/warm-drill/_doc/{doc}?refresh=true",
                                                  json.dumps({"message": doc}))
                        if code in (200, 201):
                            ack = json.loads(body)
                            if ack.get("_id") != doc or ack.get("_shards", {}).get("successful", 0) < 1:
                                raise RuntimeError(f"Successful index request has no matching acknowledgment for {doc}")
                            self.writes[doc] = doc
                    else:
                        code, _ = self.request(connection, role, "GET", "/warm-drill/_search?q=_id:1")
                except (OSError, http.client.HTTPException):
                    code = "transport_error"
                    connection.close()
                self.record("traffic", operation=name, role=role, proxy=proxy, code=code,
                            durationMs=round((time.monotonic() - started) * 1000, 2), document=doc if role == "ingest" else None)
                if code == "transport_error":
                    proxy = survivor
                    connection = self.connection(proxy, role)
                stop.wait(1.5 if role == "ingest" else 0.4)
        finally:
            connection.close()

    def routes(self):
        return {role: get("httproute", name)["spec"]["rules"][0]["backendRefs"] for role, name in ROUTES.items()}

    def lose_proxy(self, name, operation, victim, proxies, old):
        baseline = self.routes()
        journal = operation["status"]["weightedRollout"]
        if self.proxies() != proxies:
            raise RuntimeError("The Ready proxy cohort changed before fault dispatch")
        dispatched_at = now()
        kube("-n", GATEWAY_NS, "delete", "pod", proxies[victim], "--grace-period=0", "--force", "--wait=false")
        self.record("proxy-deleted", operation=name, proxy=victim, at=dispatched_at)
        print(f"{name}: removed one serving Envoy replica", flush=True)
        wait_for(lambda: victim not in self.proxies(), 15, "the original proxy UID to leave the Ready set")
        for report in old:
            if victim not in report["servingProxyUIDs"]:
                raise RuntimeError("Stale test report did not include the deleted proxy")
            self.publish(name, report, weighted=True)
            self.record("stale-report-published", operation=name, role=report["role"], routeVersion=report["routeVersion"],
                        observedAt=report["inventoryObservedAt"], servingProxyUIDs=report["servingProxyUIDs"])
        hold_started = time.monotonic()
        deadline = hold_started + 28
        observed_hold = False
        while time.monotonic() < deadline:
            instance = get("opensearchserverless", "catalog")
            current = get("opensearchserverlessoperation", name)
            if self.routes() != baseline or instance["status"].get("activeOperation", {}).get("uid") != operation["metadata"]["uid"]:
                raise RuntimeError("Traffic weights or claim changed with stale proxy evidence")
            if current["status"].get("phase") in {"Succeeded", "Failed"} or current["status"].get("weightedRollout") != journal:
                raise RuntimeError("The weighted journal advanced with stale proxy evidence")
            conditions = current["status"].get("conditions", [])
            observed_hold |= any(c["reason"] in {"ProxyEvidenceIncomplete", "WeightedEvidenceUnavailable", "WeightedEvidenceChanged"}
                                 for c in conditions)
            self.record("stale-cohort-hold", operation=name, conditions=conditions, routes=baseline)
            if observed_hold and time.monotonic() - hold_started >= 8:
                break
            time.sleep(1)
        if not observed_hold:
            raise RuntimeError("No controller hold for the stale proxy cohort was observed")
        diagnostic = run([sys.executable, str(Path(__file__).with_name("diagnose_operator.py")), "catalog", "-n", NS])
        self.record("diagnosis", operation=name, text=diagnostic)
        print(f"{name}: the claim and route weights held with stale evidence", flush=True)
        return wait_for(lambda: self.replacement(victim), 65, "a replacement Envoy replica to become Ready")

    def replacement(self, victim):
        proxies = self.proxies()
        return proxies if victim not in proxies and len(proxies) == 2 else None

    def completed_promotion(self):
        operation = get("opensearchserverlessoperation", "promote-proxy-loss")
        status = operation.get("status", {})
        if status.get("phase") != "Succeeded":
            return False
        instance = get("opensearchserverless", "catalog")
        current = instance["status"]
        active = current.get("activeOperation")
        if (not self.promotion_fault_trace or (active and active["name"] != "rollback-proxy-loss")
                or current.get("lastRolloutCommit", {}).get("uid") != operation["metadata"]["uid"]
                or current["servingFleetIntentByRole"] != dict.fromkeys(ROUTES, "green")):
            raise RuntimeError("Completed promotion could not be bound to the saved fault trace and committed fleets")
        completed = next(condition for condition in status["conditions"] if condition["type"] == "Ready" and condition["status"] == "True")
        self.trace.extend(self.promotion_fault_trace)
        self.record("claim-released", operation=operation["metadata"]["name"], fleet="green",
                    at=datetime.fromisoformat(completed["lastTransitionTime"].replace("Z", "+00:00")).isoformat())
        print("promote-proxy-loss: verified both routes committed and the claim was released after the recorded fault", flush=True)
        return True

    def transition(self, action, resume=False):
        name = action.lower() + "-proxy-loss"
        instance = get("opensearchserverless", "catalog")
        current, target = ("initial", "green") if action == "Promote" else ("green", "initial")
        spec = {"action": action, "instanceRef": {"name": "catalog", "uid": instance["metadata"]["uid"]},
                "expectedServingFleetByRole": dict.fromkeys(ROUTES, current), "targetFleet": target}
        if action == "Promote":
            spec["routingMode"] = "WeightedGateway"
        else:
            spec["supersedesOperationUID"] = instance["status"]["rollbackTarget"]["promotionUID"]
        proxies = self.proxies()
        if len(proxies) != 2:
            raise RuntimeError("Expected exactly two serving proxies at the start of the operation")
        self.forward(proxies)
        victim, survivor = sorted(proxies)
        stop = threading.Event()
        with ThreadPoolExecutor(max_workers=2) as pool:
            clients = [pool.submit(self.traffic, stop, role, victim, survivor, name) for role in ROUTES]
            try:
                if resume:
                    operation = get("opensearchserverlessoperation", name)
                    active = instance["status"].get("activeOperation")
                    if operation["spec"] != spec or (active and active["uid"] != operation["metadata"]["uid"]):
                        raise RuntimeError("Interrupted operation or instance claim did not match the fixture")
                else:
                    kube("-n", NS, "create", "-f", "-", data=json.dumps({
                        "apiVersion": "serverless.opensearch.org/v1alpha1", "kind": "OpenSearchServerlessOperation",
                        "metadata": {"name": name}, "spec": spec,
                    }))
                previous_fault = self.promotion_fault_trace if resume and action == "Promote" else []
                self.trace.extend(previous_fault)
                self.advance(name, target, victim, proxies, injected=bool(previous_fault))
            finally:
                stop.set()
                for client in clients:
                    client.result()

    def advance(self, name, target, victim, proxies, injected):
        deadline = time.monotonic() + 330
        next_approval = 0
        while time.monotonic() < deadline:
            try:
                operation = get("opensearchserverlessoperation", name)
                instance = get("opensearchserverless", "catalog")
            except (RuntimeError, subprocess.SubprocessError) as error:
                self.record("observation-pending", operation=name, reason=str(error))
                time.sleep(1)
                continue
            status = operation.get("status", {})
            if status.get("phase") == "Succeeded" and not instance["status"].get("activeOperation"):
                if not injected or instance["status"]["servingFleetIntentByRole"] != dict.fromkeys(ROUTES, target):
                    raise RuntimeError("Transition completed without the fault or correct final fleets")
                self.record("claim-released", operation=name, fleet=target)
                print(f"{name}: both routes now serve {target} and the claim was released", flush=True)
                return
            weighted = status.get("weightedRollout")
            try:
                if time.monotonic() >= next_approval:
                    self.approve(operation, instance)
                    next_approval = time.monotonic() + 10
                if not weighted:
                    time.sleep(2)
                    continue
                reports = None
                if not injected and weighted["roles"]["search"].get("appliedAt"):
                    # An applied step is waiting for evidence, so no report for the new weights has been published yet.
                    reports = self.reports(name, proxies)
                    injected = True
            except (RuntimeError, subprocess.SubprocessError) as error:
                self.record("observation-pending", operation=name, reason=str(error))
                time.sleep(1)
                continue
            if reports is not None:
                proxies = self.lose_proxy(name, operation, victim, proxies, reports)
            try:
                for report in self.reports(name, proxies):
                    self.publish(name, report, weighted=True)
            except (RuntimeError, subprocess.SubprocessError) as error:
                self.record("observation-pending", operation=name, reason=str(error))
                time.sleep(1)
            else:
                # Let reconciliation consume this statement before replacing its resource version.
                time.sleep(4)
        raise RuntimeError(f"{name} did not complete")

    def verify_documents(self):
        proxy = next(iter(self.proxies()))
        self.forward(self.proxies())
        connection = self.connection(proxy, "search")
        try:
            def read_all():
                code, body = self.request(connection, "search", "GET", "/warm-drill/_search?size=1000")
                if code != 200:
                    return False
                documents = {hit["_id"]: hit["_source"]["message"] for hit in json.loads(body)["hits"]["hits"]}
                return all(documents.get(doc) == message for doc, message in self.writes.items())

            wait_for(read_all, 35, "every acknowledged document through restored search")
            self.record("writes-verified", documents=sorted(self.writes))
        finally:
            connection.close()

    def verify_traffic_recovery(self):
        summaries = []
        for fault in (item for item in self.trace if item["event"] == "proxy-deleted"):
            name = fault["operation"]
            released = next(item for item in self.trace if item["operation"] == name and item["event"] == "claim-released")
            timestamp = datetime.fromisoformat(fault["at"])
            summary = {"operation": name, "claimReleaseSeconds": (datetime.fromisoformat(released["at"]) - timestamp).total_seconds(),
                       "roles": {}}
            for role in ROUTES:
                samples = sorted((item for item in self.trace if item["event"] == "traffic"
                                  and item["operation"] == name and item["role"] == role),
                                 key=lambda item: datetime.fromisoformat(item["at"]))
                failures = [item for item in samples if item["code"] not in (200, 201)]
                recovered = next((item for item in samples if datetime.fromisoformat(item["at"]) >= timestamp
                                  and item["proxy"] != fault["proxy"]
                                  and item["code"] in (200, 201)), None)
                lost = any(datetime.fromisoformat(item["at"]) >= timestamp and item["code"] == "transport_error"
                           and item["proxy"] == fault["proxy"] for item in failures)
                if recovered is None or not lost:
                    raise RuntimeError(f"{name} {role} did not observe the lost connection and recover through the survivor")
                summary["roles"][role] = {"requests": len(samples), "failures": len(failures),
                                          "firstSuccessSeconds": (datetime.fromisoformat(recovered["at"]) - timestamp).total_seconds()}
            summaries.append(summary)
        self.record("traffic-summary", operations=summaries)


def main():
    global NS, SELECTOR, HOSTS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--namespace", choices=("operator-envoy-loss", "operator-envoy-loss-replay"), default=NS)
    parser.add_argument("--resume-promotion", action="store_true", help="resume the exact promotion from the saved first trace")
    args = parser.parse_args()
    NS = args.namespace
    SELECTOR = ("gateway.envoyproxy.io/owning-gateway-name=secure-rollout,"
                f"gateway.envoyproxy.io/owning-gateway-namespace={NS}")
    HOSTS = {"search": f"search-edge.{NS}.svc.cluster.local", "ingest": f"index-edge.{NS}.svc.cluster.local"}
    directory = args.fixture.resolve()
    os.environ["KUBECONFIG"] = str(directory / "kubeconfig")
    if kube("config", "current-context").strip() != "kind-oss-secure-install" or kube("get", "nodes", "-o", "name").strip() != "node/oss-secure-install-control-plane":
        raise RuntimeError("Expected the isolated Kind drill cluster")
    fixture = Fixture(directory, resume=args.resume_promotion)
    try:
        if not args.resume_promotion or not fixture.completed_promotion():
            fixture.transition("Promote", resume=args.resume_promotion)
        rollback_exists = args.resume_promotion and bool(kube("-n", NS, "get", "opensearchserverlessoperation", "rollback-proxy-loss",
                                                             "--ignore-not-found", "-o", "name").strip())
        fixture.transition("Rollback", resume=rollback_exists)
        fixture.verify_traffic_recovery()
        fixture.verify_documents()
        print(f"Both transitions released their claims. Search verified {len(fixture.writes)} acknowledged documents.")
    finally:
        fixture.close()


if __name__ == "__main__":
    main()
