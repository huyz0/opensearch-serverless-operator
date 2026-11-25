"""Capture or compare live release identity before and after a manager image change."""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess

from verify_development_release import release_image, verify


MANAGER = "opensearch-serverless-operator"
INSTANCE_LABEL = "serverless.opensearch.org/instance"
ROLE_LABEL = "serverless.opensearch.org/role"
FLEET_LABEL = "serverless.opensearch.org/fleet"
ROLE_SERVICES = {"search": "search", "ingest": "index"}


def condition(conditions, name):
    return next((item for item in conditions if item["type"] == name), {})


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def manager_identity(deployment, pods, lease, image, now):
    status = deployment.get("status", {})
    spec = deployment["spec"]
    if (status.get("observedGeneration") != deployment["metadata"]["generation"]
            or spec.get("replicas") != 1 or status.get("readyReplicas") != 1
            or status.get("updatedReplicas") != 1):
        raise ValueError("manager Deployment has not converged to one updated Ready replica")
    template = spec["template"]["spec"]["containers"]
    if not any(c["name"] == "operator" and c["image"] == image for c in template):
        raise ValueError("manager Deployment image does not match the signed package")
    ready = [
        pod for pod in pods
        if not pod["metadata"].get("deletionTimestamp")
        and condition(pod.get("status", {}).get("conditions", []), "Ready").get("status") == "True"
    ]
    if len(ready) != 1:
        raise ValueError("expected exactly one non-terminating Ready manager Pod")
    pod = ready[0]
    if not any(c["name"] == "operator" and c["image"] == image for c in pod["spec"]["containers"]):
        raise ValueError("Ready manager Pod does not have the signed package image")
    container = next((c for c in pod["status"]["containerStatuses"] if c["name"] == "operator"), {})
    started = container.get("state", {}).get("running", {}).get("startedAt")
    if not container.get("imageID") or not container.get("ready") or not started:
        raise ValueError("manager container image ID or readiness is unavailable")
    meta, spec = pod["metadata"], lease["spec"]
    renewed = timestamp(spec["renewTime"])
    duration = timedelta(seconds=spec["leaseDurationSeconds"])
    if (not spec.get("holderIdentity", "").startswith(meta["name"] + "_")
            or renewed < timestamp(started) or renewed > now or now - renewed >= duration):
        raise ValueError("Ready manager Pod does not hold a current leader Lease")
    return {"uid": meta["uid"], "name": meta["name"], "image": image, "imageID": container["imageID"],
            "leaseRenewTime": spec["renewTime"]}


def controlled_by(resource_meta, kind, uid):
    return any(ref.get("controller") is True and ref.get("kind") == kind and ref.get("uid") == uid
               for ref in resource_meta.get("ownerReferences", []))


def verify_ready_endpoints(service, slices, ready_pods):
    meta = service["metadata"]
    matched = set()
    for endpoint_slice in slices:
        slice_meta = endpoint_slice["metadata"]
        if (slice_meta.get("deletionTimestamp")
                or slice_meta.get("labels", {}).get("endpointslice.kubernetes.io/managed-by")
                != "endpointslice-controller.k8s.io"
                or not controlled_by(slice_meta, "Service", meta["uid"])):
            raise ValueError(f"{meta['name']} has an unverified EndpointSlice")
        for endpoint in endpoint_slice["endpoints"]:
            conditions = endpoint.get("conditions", {})
            ready = conditions.get("ready")
            if (not isinstance(ready, bool)
                    or (conditions.get("terminating") and conditions.get("serving") is not False)):
                raise ValueError(f"{meta['name']} has an endpoint with ambiguous readiness")
            if not ready:
                continue
            target = endpoint.get("targetRef", {})
            uid = target.get("uid")
            pod = ready_pods.get(uid)
            if (conditions.get("terminating") or conditions.get("serving") is False
                    or target.get("kind") != "Pod" or target.get("namespace") != meta["namespace"]
                    or pod is None or target.get("name") != pod["metadata"]["name"]):
                raise ValueError(f"{meta['name']} has a Ready endpoint outside the verified Pod set")
            ips = {pod["status"].get("podIP"), *(ip["ip"] for ip in pod["status"].get("podIPs", []))}
            addresses = endpoint.get("addresses", [])
            if not addresses or not set(addresses).issubset(ips):
                raise ValueError(f"{meta['name']} endpoint IP does not match the verified Pod")
            matched.add(uid)
    if matched != ready_pods.keys():
        raise ValueError(f"{meta['name']} endpoints have not converged to the verified Ready Pods")


def workload_identity(instance, pods, services, slices_by_service, java_image, now):
    meta, status = instance["metadata"], instance.get("status", {})
    available = condition(status.get("conditions", []), "Available")
    fleets = status.get("servingFleetIntentByRole", {})
    sample = status.get("lastEndpointObservation", {})
    if (status.get("activeOperation") or set(fleets) != set(ROLE_SERVICES)
            or available.get("status") != "True"
            or available.get("reason") != "ReadyRoleEndpointsObserved"
            or available.get("observedGeneration") != meta["generation"]
            or sample.get("observedGeneration") != meta["generation"]
            or sample.get("observedServingFleets") != fleets
            or not timedelta(0) <= now - timestamp(sample["observedAt"]) < timedelta(minutes=2)):
        raise ValueError("the instance lacks current verified endpoint coverage")
    if instance["spec"]["image"] != java_image:
        raise ValueError("instance Java image does not match the signed package")
    roles = {}
    for role, service in services.items():
        service_meta, spec = service["metadata"], service["spec"]
        selector = {INSTANCE_LABEL: meta["uid"], ROLE_LABEL: role, FLEET_LABEL: fleets[role]}
        if (not controlled_by(service_meta, "OpenSearchServerless", meta["uid"])
                or spec.get("selector") != selector or spec.get("clusterIP") in (None, "", "None")
                or (sample.get("role") == role and sample.get("serviceUID") != service_meta["uid"])):
            raise ValueError(f"{role} Service does not select the recorded serving fleet")
        ready_pods = {
            pod["metadata"]["uid"]: pod for pod in pods
            if all(pod["metadata"].get("labels", {}).get(key) == value for key, value in selector.items())
            and not pod["metadata"].get("deletionTimestamp")
            and controlled_by(pod["metadata"], "OpenSearchServerless", meta["uid"])
            and condition(pod.get("status", {}).get("conditions", []), "Ready").get("status") == "True"
            and condition(pod.get("status", {}).get("conditions", []), "DisruptionTarget").get("status") != "True"
            and any(c["name"] == "opensearch" and c["image"] == java_image for c in pod["spec"]["containers"])
        }
        if not ready_pods:
            raise ValueError(f"{role} has no eligible Ready Pod running the signed package Java image")
        verify_ready_endpoints(service, slices_by_service.get(service_meta["name"], []), ready_pods)
        roles[role] = {
            "readyPodUIDs": sorted(ready_pods), "serviceUID": service_meta["uid"], "clusterIP": spec["clusterIP"],
        }
    return {"instanceUID": meta["uid"], "roles": roles}


def observe(args, inventory):
    namespace = inventory["namespace"]
    prefix = ["kubectl", "--context", args.context, "--namespace", namespace, "--request-timeout=15s"]

    def get(*resource):
        response = subprocess.check_output(prefix + ["get", *resource, "-o", "json"], timeout=20)
        return json.loads(response)

    image = release_image(inventory, "manager")
    deployment = get("deployment", MANAGER)
    manager_pods = get("pods", "-l", f"app.kubernetes.io/name={MANAGER}")["items"]
    lease = get("lease", MANAGER + ".serverless.opensearch.org")
    manager_identity(deployment, manager_pods, lease, image, datetime.now(timezone.utc))

    instance = get("opensearchserverless", args.instance)
    pods = get("pods", "-l", f"{INSTANCE_LABEL}={instance['metadata']['uid']}")["items"]
    services = {role: get("service", f"{args.instance}-{suffix}") for role, suffix in ROLE_SERVICES.items()}
    service_names = ",".join(service["metadata"]["name"] for service in services.values())
    slices = get("endpointslices.discovery.k8s.io", "-l", f"kubernetes.io/service-name in ({service_names})")
    slices_by_service = {}
    for endpoint_slice in slices["items"]:
        service_name = endpoint_slice["metadata"]["labels"]["kubernetes.io/service-name"]
        slices_by_service.setdefault(service_name, []).append(endpoint_slice)
    now = datetime.now(timezone.utc)
    workload = workload_identity(instance, pods, services, slices_by_service, release_image(inventory, "java"), now)
    # Confirm that the same process still holds the Lease after the workload reads.
    current = get("lease", MANAGER + ".serverless.opensearch.org")
    if current["spec"].get("holderIdentity") != lease["spec"].get("holderIdentity"):
        raise ValueError("manager leadership changed during the workload observation")
    manager = manager_identity(deployment, manager_pods, current, image, datetime.now(timezone.utc))
    return {"observedAt": now.isoformat(), "context": args.context, "namespace": namespace,
            "version": inventory["version"], "manager": manager, "workload": workload}


def require_preserved_workload(baseline, current):
    if not isinstance(baseline, dict):
        raise ValueError("baseline must be a release observation object")
    for key in ("context", "namespace", "workload"):
        if baseline.get(key) != current[key]:
            raise ValueError(f"release observation changed {key}")
    if baseline["manager"]["uid"] == current["manager"]["uid"]:
        raise ValueError("manager Pod UID did not change")
    if baseline["manager"]["image"] == current["manager"]["image"]:
        raise ValueError("manager image did not change")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package", type=Path)
    parser.add_argument("--public-key", required=True, type=Path)
    parser.add_argument("--context", required=True, help="explicit kubectl context")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--baseline", type=Path, help="JSON observation for the currently deployed package")
    args = parser.parse_args()
    try:
        inventory, _ = verify(args.package, args.public_key)
        result = observe(args, inventory)
        if args.baseline:
            require_preserved_workload(json.loads(args.baseline.read_bytes()), result)
    except (OSError, subprocess.SubprocessError, TypeError, KeyError, ValueError) as error:
        parser.exit(1, f"release observation failed: {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
