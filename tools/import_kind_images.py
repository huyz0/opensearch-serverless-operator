"""Stream single-platform local Docker images into a kind node's containerd."""

import argparse
import subprocess
import sys


def canonical_reference(image: str) -> str:
    first, separator, _ = image.partition("/")
    if not separator:
        image = f"docker.io/library/{image}"
    elif "." not in first and ":" not in first and first != "localhost":
        image = f"docker.io/{image}"
    elif first == "index.docker.io":
        image = image.replace("index.docker.io/", "docker.io/", 1)
    if image.startswith("docker.io/") and image.count("/") == 1:
        image = image.replace("docker.io/", "docker.io/library/", 1)
    if ":" not in image.rsplit("/", 1)[-1] and "@" not in image:
        image += ":latest"
    return image


def platform_digest(reference: str, platform: str, *, optional: bool = False) -> str:
    result = subprocess.run(
        ["docker", "image", "inspect", "--platform", platform,
         "--format", "{{.Descriptor.Digest}}", reference],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if optional else None,
        check=not optional,
    )
    return "" if result.returncode else result.stdout.strip()


def export_reference_for_digest(reference: str, repository: str, platform: str, digest: str) -> str:
    inspected = platform_digest(reference, platform, optional=True)
    if inspected == digest:
        return reference
    has_platform_metadata = inspected.startswith("sha256:")

    # Docker may only know the parent OCI index by tag. Look for its exact
    # platform manifest using tags from the requested repository.
    tags = subprocess.check_output(
        ["docker", "image", "ls", "--filter", f"reference={repository}:*",
         "--format", "{{.Repository}}:{{.Tag}}"],
        text=True,
    )
    for tag in tags.splitlines():
        candidate = canonical_reference(tag)
        if candidate.rsplit(":", 1)[0] != repository:
            continue
        inspected = platform_digest(candidate, platform, optional=True)
        if inspected == digest:
            return candidate
        has_platform_metadata |= inspected.startswith("sha256:")
    if not has_platform_metadata:
        # Older Docker daemons or image stores can save a registered digest
        # without reporting a platform descriptor. Verify it after import.
        return reference
    raise RuntimeError(f"No local image in {repository} selects platform manifest {digest} for {platform}")


def imported_manifest(containerd: list[str], reference: str) -> str:
    listing = subprocess.check_output([*containerd, "list", f"name=={reference}"], text=True)
    for row in listing.splitlines():
        fields = row.split()
        if len(fields) >= 3 and fields[0] == reference and fields[2].startswith("sha256:"):
            return fields[2]
    return ""


def import_image(node: str, platform: str, image: str) -> None:
    reference = canonical_reference(image)
    source_reference, _, requested_digest = reference.partition("@")
    has_tag = ":" in source_reference.rsplit("/", 1)[-1]
    repository = source_reference.rsplit(":", 1)[0] if has_tag else source_reference
    export_reference = reference
    if has_tag and requested_digest:
        # A locally built OCI index can expose a platform manifest only through its tag.
        actual_digest = platform_digest(source_reference, platform)
        if actual_digest != requested_digest:
            raise RuntimeError(
                f"{source_reference} selects {actual_digest} for {platform}, expected {requested_digest}"
            )
        export_reference = source_reference
    elif requested_digest:
        export_reference = export_reference_for_digest(reference, repository, platform, requested_digest)

    save_command = ["docker", "image", "save", "--platform", platform, "--", export_reference]
    images_command = [node, "ctr", "-n", "k8s.io", "images"]
    containerd = ["docker", "exec", *images_command]
    # Derive the digest alias from archive content in the same import. Copying a
    # mutable node tag afterward could bind the digest name to the wrong image.
    import_command = [
        "docker", "exec", "-i", *images_command, "import",
        "--platform", platform, "--base-name", repository, "--digests", "-",
    ]
    producer = subprocess.Popen(save_command, stdout=subprocess.PIPE)
    try:
        with producer.stdout as archive:
            result = subprocess.run(import_command, stdin=archive, check=False)
        status = producer.wait()
        if status or result.returncode:
            message = f"Import of {image} failed (Docker export: {status}, containerd import: {result.returncode})"
            raise RuntimeError(message)
    finally:
        if producer.poll() is None:
            producer.terminate()
            producer.wait()

    lookup = export_reference
    manifest = imported_manifest(containerd, lookup)
    if not manifest:
        if requested_digest:
            raise RuntimeError(
                f"Single-platform export did not preserve {lookup} for {platform}. "
                "Import by tag and use the platform manifest digest printed by the importer."
            )
        raise RuntimeError(f"containerd did not expose the imported manifest for {reference}")
    if requested_digest and manifest != requested_digest:
        raise RuntimeError(f"{lookup} resolves to {manifest} on {node}, expected {requested_digest}")

    # Kubernetes uses the repository-qualified digest when a Pod is pinned by digest.
    pinned_reference = f"{repository}@{manifest}"
    if lookup != pinned_reference:
        pinned_manifest = imported_manifest(containerd, pinned_reference)
        if pinned_manifest != manifest:
            raise RuntimeError(
                f"{pinned_reference} resolves to {pinned_manifest or 'no manifest'} on {node}, expected {manifest}"
            )

    # A containerd image record alone does not establish that kubelet can resolve it.
    for local_reference in dict.fromkeys((lookup, pinned_reference)):
        result = subprocess.run(
            ["docker", "exec", node, "crictl", "inspecti", "-q", local_reference],
            stdout=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(f"Kubelet image lookup failed on {node} for {local_reference}")
    print(f"Imported {reference} as {pinned_reference} (verified through CRI)", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", required=True, help="kind node's Docker container name")
    parser.add_argument("--platform", required=True, help="image platform, such as linux/amd64")
    parser.add_argument(
        "images", nargs="+", help="local Docker references by tag, digest, or tag with an exact platform digest"
    )
    args = parser.parse_args()
    try:
        for image in args.images:
            import_image(args.node, args.platform, image)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
