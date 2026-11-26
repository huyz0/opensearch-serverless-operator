"""Explain an instance's operation claim and provisioning holds using namespace reads."""

import argparse
from datetime import datetime, timezone
import subprocess
import sys

from preview_operator_change import kubectl_get


NEXT_ACTIONS = {
    "ReceiptUnavailable": "Check surviving index endpoints, Java lease health, and S3 read access. Restore the dependency and let the same operation retry.",
    "ReceiptMissing": "Check the inventoried shard and its owner term. Verify a newer-term write through index and confirm search readback while the operator checks WAL settlement.",
    "HeadsUnaccounted": "Check the uncovered heads in the retirement journal. Use the successor and WAL settlement checks before any further retirement.",
    "WritesPending": "Verify the index route reaches a healthy successor. Check for a newer-term application write and search readback for each remaining shard.",
    "WriterHeadsChanged": "Compare the exact closed JVM's current durable heads with the original inventory. Leave the claim in place while investigating new or changed heads.",
    "RouteNotConverged": "Compare stable Service selectors, EndpointSlices, and authenticated traffic for both roles. Check gateway backend convergence before retrying.",
    "WeightedEvidenceUnavailable": "Check the live HTTPRoutes and serving Envoy replica set. Collect and publish fresh signed observations for this operation and both roles.",
    "ProxyEvidenceIncomplete": "Check manager reads of the frontend Service, EndpointSlices and proxy Pods. Publish fresh signed evidence for the current routes and serving proxies.",
    "WeightedEvidenceChanged": "Refresh signed observations against the current routes and proxy set. The controller rechecks those observations before applying the next step.",
    "WeightedGatewayUnbound": "Ask the operator administrator to pin the frontend Service and apply the Gateway namespace read Role.",
    "CandidateUnavailable": "Check the staged candidate generation, preview Services, and both roles. Wait for pending provisioning and warm-up to settle.",
    "StartupTimeout": "Inspect the exact bound Pod's init containers and readiness. A successful startup on that Pod can clear this hold.",
    "PluginInstallTimeout": "Check the exact Pod's init events, registry access, and plugin policy before retrying provisioning.",
    "PeerDNSPreflightTimeout": "Check the exact Pod's peer DNS init events and the headless Service before retrying provisioning.",
    "PendingTimeout": "Check scheduling events and admitted resources against the role/AZ capacity profile. Fix fit or placement before requesting a retry.",
    "PodGone": "Confirm the original Pod UID is absent, fix the cause, then follow the RetryProvisioning procedure for that exact attempt.",
}


def age(timestamp, now):
    if not timestamp:
        return "unknown"
    try:
        seconds = int((now - datetime.fromisoformat(timestamp.replace("Z", "+00:00"))).total_seconds())
    except (TypeError, ValueError):
        return "unknown"
    if seconds < 0:
        return "unknown (timestamp is in the future)"
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {seconds}s"


def operation_progress(operation, now):
    state = operation.get("status", {})
    target = operation.get("spec", {}).get("writerRetirement")
    if target:
        journal = state.get("writerRetirement", {})
        heads = (journal.get("headInventory") or {}).get("heads")
        yield (f"  Writer Pod {target['podName']} (UID {target['podUID']}), "
               f"absence confirmed: {'yes' if journal.get('victimGone') else 'no'}")
        if heads is not None:
            yield f"  Successor writes verified: {len(journal.get('successors', []))}/{len(heads)} inventoried heads"
    weighted = state.get("weightedRollout") or {}
    for role, route in weighted.get("roles", {}).items():
        target_count = len(route.get("targets", []))
        text = f"  {role} HTTPRoute {route['routeName']}: {route.get('nextStep', 0)}/{target_count} steps verified"
        if route.get("appliedAt"):
            text += f", current weights applied {age(route['appliedAt'], now)} ago"
        yield text


def operation_lines(operation, now):
    identity, state = operation["metadata"], operation.get("status", {})
    phase = state.get("phase") or ("in progress (rollout journal)" if state.get("weightedRollout") or state.get("rolloutRoles") else "pending")
    yield f"Phase: {phase} (operation age: {age(identity.get('creationTimestamp'), now)})"
    yield from operation_progress(operation, now)
    for condition in state.get("conditions", []):
        kind, reason, value = condition["type"], condition["reason"], condition["status"]
        if kind != "Ready" and value == "True":
            continue
        if condition.get("observedGeneration") != identity["generation"]:
            yield f"  {kind}: stale observation ({reason})"
            continue
        since = age(condition.get("lastTransitionTime"), now)
        yield f"  {kind}={value} ({reason}, condition state age: {since})"
        if condition.get("message"):
            yield f"    {condition['message']}"
        if value != "True" and reason in NEXT_ACTIONS:
            yield f"    Next: {NEXT_ACTIONS[reason]}"


def diagnosis(instance, operation, now):
    status = instance.get("status", {})
    for condition in status.get("conditions", []):
        if condition["type"] == "Available":
            if condition.get("observedGeneration") == instance["metadata"].get("generation"):
                yield f"Kubernetes endpoint availability: {condition['status']} ({condition['reason']})"
            else:
                yield "Kubernetes endpoint availability: stale observation"
            break
    claim = status.get("activeOperation")
    identity = operation.get("metadata", {}) if operation else {}
    ref = operation.get("spec", {}).get("instanceRef", {}) if operation else {}
    if not claim:
        yield "Active operation: none recorded"
    if claim:
        yield f"Active operation: {claim['name']} ({claim['action']}, UID {claim['uid']})"
    if operation and identity.get("name") and (not claim or identity["name"] != claim["name"]):
        if ref.get("uid") == instance["metadata"]["uid"]:
            yield f"Requested operation: {identity['name']} (UID {identity['uid']}, no matching claim)"
            yield from operation_lines(operation, now)
        else:
            yield "Requested operation refers to a different instance UID."
    elif claim:
        if (identity.get("uid") == claim["uid"] and identity.get("generation") == claim["generation"]
                and ref.get("uid") == instance["metadata"]["uid"]):
            yield from operation_lines(operation, now)
        else:
            yield "Claimed operation could not be verified. Re-read the instance and exact operation before taking action."
    attempts = status.get("blockedProvisioningAttempts", [])
    current = status.get("provisioningAttempt")
    if current and current.get("phase") == "Blocked":
        attempts = [current, *attempts]
    for attempt in attempts:
        reason = attempt.get("reason", "Unknown")
        started = age(attempt.get("startedAt"), now)
        yield (f"Provisioning hold: {attempt.get('role', '?')}/{attempt.get('zone', '?')} "
               f"Pod {attempt.get('name', '?')} (UID {attempt.get('podUID', '?')}), {reason}, attempt age: {started}")
        yield "  Next: " + NEXT_ACTIONS.get(reason, "Inspect the exact Pod and its scheduler or startup events before requesting a retry.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("instance")
    parser.add_argument("-n", "--namespace", required=True)
    parser.add_argument("--operation", help="inspect a named operation, including a request waiting for a claim")
    args = parser.parse_args()
    try:
        instance = kubectl_get(args.namespace, "opensearchserverless", args.instance)
        claim = instance.get("status", {}).get("activeOperation")
        operation = None
        if args.operation or claim:
            try:
                operation = kubectl_get(args.namespace, "opensearchserverlessoperation", args.operation or claim["name"])
            except subprocess.CalledProcessError:
                if args.operation:
                    raise
        print("\n".join(diagnosis(instance, operation, datetime.now(timezone.utc))))
        return 0
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print(f"Cannot diagnose instance: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
