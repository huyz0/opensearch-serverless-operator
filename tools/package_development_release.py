"""Render a digest-pinned secure development release for offline review and install."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from release_sbom import SBOM_FILES, generate_sbom


ROOT = Path(__file__).resolve().parents[1]
MANAGER_IMAGE_NAME = "opensearch-serverless-operator"
DNS_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
DNS_NAME = rf"{DNS_LABEL}(?:\.{DNS_LABEL})*"
IMAGE_COMPONENT = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
IMAGE_REF = rf"(?:{DNS_NAME})(?::[0-9]+)?/(?:{IMAGE_COMPONENT}/)*{IMAGE_COMPONENT}@sha256:[0-9a-f]{{64}}"
VERSION = r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*)?"


def matched(pattern, message):
    def validate(value):
        if not re.fullmatch(pattern, value):
            raise argparse.ArgumentTypeError(message)
        return value

    return validate


def render_manager(config, args):
    release = config / "release"
    release.mkdir()
    repo, digest = args.manager_image.split("@", 1)
    overlay = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "namespace": args.namespace,
        "resources": ["../operator-dev-secure"],
        "images": [{"name": MANAGER_IMAGE_NAME, "newName": repo, "digest": digest}],
        "configMapGenerator": [{
            "name": "opensearch-secure-runtime",
            "literals": [
                f"javaImage={args.java_image}",
                f"clusterDomain={args.cluster_domain}",
            ],
        }],
    }
    (release / "kustomization.yaml").write_text(json.dumps(overlay), encoding="utf-8")
    command = ["kubectl", "kustomize", str(release)]
    return subprocess.run(command, check=True, stdout=subprocess.PIPE).stdout


def render_crds():
    generated = ROOT / "config" / "generated-dev"
    files = sorted(generated.glob("serverless.opensearch.org_*.yaml"))
    required = {
        "serverless.opensearch.org_opensearchserverlesses.yaml",
        "serverless.opensearch.org_opensearchserverlessoperations.yaml",
    }
    if missing := required.difference(path.name for path in files):
        raise ValueError(f"missing generated CRDs: {', '.join(sorted(missing))}")
    documents = (path.read_bytes().replace(b"\r\n", b"\n").rstrip() + b"\n" for path in files)
    return b"---\n".join(documents)


def sign_inventory(inventory, key):
    with tempfile.TemporaryDirectory(prefix="operator-sign-") as scratch:
        source = Path(scratch) / "release.json"
        signature = Path(scratch) / "release.json.sig"
        source.write_bytes(inventory)
        subprocess.run([
            "openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(key),
            "-in", str(source), "-out", str(signature),
        ], check=True)
        return signature.read_bytes()


def package(args):
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError(f"output already exists: {output}")

    with tempfile.TemporaryDirectory(prefix="operator-release-") as scratch:
        config = Path(scratch) / "config"
        for directory in ("dev", "operator-dev", "operator-dev-secure"):
            shutil.copytree(ROOT / "config" / directory, config / directory)
        manifests = {"crds.yaml": render_crds(), "manager.yaml": render_manager(config, args)}
        if args.manager_oci_archive is not None:
            for role, archive, image in (
                ("manager", args.manager_oci_archive, args.manager_image),
                ("java", args.java_oci_archive, args.java_image),
            ):
                filename = SBOM_FILES[role]
                manifests[filename] = generate_sbom(archive, image, args.oci_platform, Path(scratch) / filename)

    inventory = {
        "version": args.version,
        "profile": "secure-development",
        "namespace": args.namespace,
        "clusterDomain": args.cluster_domain,
        "images": {"manager": args.manager_image, "java": args.java_image},
        "sha256": {name: hashlib.sha256(data).hexdigest() for name, data in manifests.items()},
    }
    inventory_bytes = (json.dumps(inventory, indent=2) + "\n").encode("utf-8")
    if args.signing_key is not None:
        manifests["release.json.sig"] = sign_inventory(inventory_bytes, args.signing_key)
    output.mkdir()
    for name, data in manifests.items():
        (output / name).write_bytes(data)
    (output / "release.json").write_bytes(inventory_bytes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    immutable_image = matched(IMAGE_REF, "use registry/repository@sha256:<64 lowercase hex digits>")
    parser.add_argument("--version", required=True, type=matched(VERSION, "use vMAJOR.MINOR.PATCH with an optional prerelease"))
    parser.add_argument("--manager-image", required=True, type=immutable_image)
    parser.add_argument("--java-image", required=True, type=immutable_image)
    parser.add_argument("--namespace", default="operator-development", type=matched(DNS_LABEL, "use a DNS label"))
    parser.add_argument("--cluster-domain", required=True, type=matched(DNS_NAME, "use a DNS name"))
    parser.add_argument("--output", required=True, type=Path, help="new directory under an existing parent")
    parser.add_argument("--signing-key", type=Path, help="private PEM key for a detached release inventory signature")
    parser.add_argument("--manager-oci-archive", type=Path, help="manager image OCI archive for SPDX generation")
    parser.add_argument("--java-oci-archive", type=Path, help="Java image OCI archive for SPDX generation")
    parser.add_argument("--oci-platform", default="linux/amd64", help="OCI platform to scan")
    args = parser.parse_args()
    if (args.manager_oci_archive is None) != (args.java_oci_archive is None):
        parser.error("supply both the manager and Java OCI archives to include SBOMs")
    if len(args.cluster_domain) > 253:
        parser.error("cluster domain must be at most 253 characters")
    try:
        package(args)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"release packaging failed: {error}\n")


if __name__ == "__main__":
    main()
