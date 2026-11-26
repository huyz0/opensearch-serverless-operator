"""Show instance capacity and stable Service changes without writing to Kubernetes."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROLE_SUFFIXES = {"search": "search", "ingest": "index"}
LABEL_PREFIX = "serverless.opensearch.org/"


def desired_zones(spec):
    return {
        (role, zone): (config["replicas"] + config.get("warmSpare", 0), config["minServing"])
        for role, pool in spec["pools"].items()
        for zone, config in pool["zones"].items()
    }


def capacity_lines(instance, proposed):
    current = desired_zones(instance["spec"])
    desired = desired_zones(proposed["spec"]) if proposed else current
    status = instance.get("status", {})
    recent = status.get("zoneCapacityObservedGeneration") == instance["metadata"]["generation"]
    samples = {
        (entry["role"], entry["zone"]): entry["kubernetesReady"]
        for entry in status.get("zoneCapacity", [])
        if entry["fleet"] == status.get("servingFleetIntentByRole", {}).get(entry["role"])
    } if recent else {}
    timestamp = status.get("zoneCapacityObservedAt") if recent else None
    yield f"Kubernetes Ready Pod sample: {timestamp or 'unavailable for this generation'}"
    yield "ROLE\tZONE\tDESIRED NOW\tDESIRED AFTER\tFLOOR AFTER\tSAMPLED READY"
    for role, zone in sorted(current.keys() | desired.keys()):
        target, floor = desired.get((role, zone), (0, 0))
        ready = samples.get((role, zone), "-") if timestamp else "-"
        yield f"{role}\t{zone}\t{current.get((role, zone), (0, 0))[0]}\t{target}\t{floor}\t{ready}"


def serving_fleet(instance, service, role):
    uid = instance["metadata"]["uid"]
    owned = any(
        owner.get("controller") and owner.get("kind") == "OpenSearchServerless"
        and owner.get("uid") == uid and owner.get("name") == instance["metadata"]["name"]
        for owner in service["metadata"].get("ownerReferences", [])
    )
    selector = service["spec"].get("selector", {})
    fleet = selector.get(LABEL_PREFIX + "fleet")
    expected = {LABEL_PREFIX + "instance-uid": uid, LABEL_PREFIX + "role": role, LABEL_PREFIX + "fleet": fleet}
    if not owned or not fleet or selector != expected:
        raise ValueError(f"Service {service['metadata']['name']} has unexpected ownership or selectors")
    return fleet


def route_lines(instance, services, action):
    status = instance.get("status", {})
    name = instance["metadata"]["name"]
    claim = status.get("activeOperation")
    if claim:
        yield f"Active operation: {claim['name']} ({claim['action']}). Check its status before submitting another operation."
    if action == "promote":
        desired = (instance["spec"].get("rollout") or {}).get("candidate") or {}
        staged = status.get("candidateFleet") or {}
        if not desired.get("fleet") or staged.get("fleet") != desired["fleet"] or staged.get("previewRetiring") or (
            staged.get("generation") != instance["metadata"]["generation"]
        ):
            raise ValueError("wait for the operator to stage the candidate for this instance generation")
        targets = dict.fromkeys(ROLE_SUFFIXES, staged["fleet"])
        timestamp = status.get("zoneCapacityObservedAt") if (
            status.get("zoneCapacityObservedGeneration") == instance["metadata"]["generation"]
        ) else None
        yield f"Candidate revision: {staged['revision']}"
        if timestamp:
            for sample in staged.get("capacity", []):
                desired_pods = sample.get("desiredPods", 0) if sample.get("desiredKnown") else "unknown"
                yield f"Candidate {sample['role']} {sample['zone']}: {sample.get('kubernetesReady', 0)} Ready / {desired_pods} desired"
    else:
        rollback = status.get("rollbackTarget") or {}
        if rollback.get("weighted"):
            raise ValueError("the retained fleet uses weighted Gateway rollback; see the weighted rollout runbook")
        targets = {role: target["fleet"] for role, target in rollback.get("roles", {}).items()}
        if not targets:
            raise ValueError("the operator has no retained rollback target")
        if len(set(targets.values())) != 1:
            raise ValueError("selector rollback requires a common retained target fleet")
        yield f"Rollback retention deadline: {rollback.get('retainUntil', 'unspecified')}"

    yield "Selector rollout preview (search first, then index):"
    for role, suffix in ROLE_SUFFIXES.items():
        service_name = f"{name}-{suffix}"
        if role not in targets or service_name not in services:
            raise ValueError(f"missing {role} target or stable Service {service_name}")
        source = serving_fleet(instance, services[service_name], role)
        yield f"  {role} {service_name}: {source} -> {targets[role]}"
        intent = status.get("servingFleetIntentByRole", {}).get(role)
        if intent and source != intent:
            yield f"  Recorded {role} intent is {intent}; inspect the Service and any active operation."
    yield "The operator rechecks ownership, capacity, JVM readiness and route evidence before changing traffic."


def kubectl_get(namespace, resource, *names):
    completed = subprocess.run(
        ["kubectl", "-n", namespace, "get", resource, *names, "-o", "json", "--request-timeout=15s"],
        stdout=subprocess.PIPE, check=True,
    )
    return json.loads(completed.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("capacity", "promote", "rollback"))
    parser.add_argument("instance")
    parser.add_argument("-n", "--namespace", required=True)
    parser.add_argument("--proposed", type=Path, help="server dry-run instance JSON for a capacity comparison")
    args = parser.parse_args()
    if args.proposed and args.action != "capacity":
        parser.error("--proposed requires the capacity action")

    try:
        instance = kubectl_get(args.namespace, "opensearchserverless", args.instance)
        proposed = json.loads(args.proposed.read_bytes()) if args.proposed else None
        if proposed and any(proposed["metadata"].get(key) != instance["metadata"].get(key) for key in ("name", "namespace", "uid")):
            raise ValueError("the proposed instance must match the live name, namespace and UID")
        lines = list(capacity_lines(instance, proposed))
        if args.action != "capacity":
            names = [f"{args.instance}-{suffix}" for suffix in ROLE_SUFFIXES.values()]
            items = kubectl_get(args.namespace, "service", *names)["items"]
            services = {item["metadata"]["name"]: item for item in items}
            lines.extend(route_lines(instance, services, args.action))
        print("\n".join(lines))
        return 0
    except (KeyError, ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Cannot preview change: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
