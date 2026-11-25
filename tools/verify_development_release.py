"""Verify a secure development release against a trusted PEM public key."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

from package_development_release import IMAGE_REF
from release_sbom import SBOM_FILES


def verify_signature(inventory, signature, public_key):
    with tempfile.TemporaryDirectory(prefix="operator-verify-") as scratch:
        source = Path(scratch) / "release.json"
        source.write_bytes(inventory)
        result = subprocess.run([
            "openssl", "pkeyutl", "-verify", "-rawin", "-pubin",
            "-inkey", str(public_key), "-in", str(source), "-sigfile", str(signature),
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False)
        if result.returncode != 0:
            raise ValueError("release inventory signature could not be verified")


def verify(package, public_key):
    inventory_bytes = (package / "release.json").read_bytes()
    verify_signature(inventory_bytes, package / "release.json.sig", public_key)
    inventory = json.loads(inventory_bytes)
    if not isinstance(inventory, dict) or inventory.get("profile") != "secure-development":
        raise ValueError("expected a secure-development release inventory")
    hashes = inventory.get("sha256")
    required = {"crds.yaml", "manager.yaml"}
    if not isinstance(hashes, dict) or set(hashes) not in (required, required | set(SBOM_FILES.values())):
        raise ValueError("inventory must contain the manifest checksums and may include both SPDX checksums")
    manifests = {name: (package / name).read_bytes() for name in hashes}
    for name, digest in hashes.items():
        if hashlib.sha256(manifests[name]).hexdigest() != digest:
            raise ValueError(f"checksum mismatch for {name}")
    return inventory, manifests["manager.yaml"]


def release_image(inventory, role):
    images = inventory.get("images")
    image = images.get(role) if isinstance(images, dict) else None
    if not isinstance(image, str) or not re.fullmatch(IMAGE_REF, image):
        raise ValueError(f"inventory must contain a digest-pinned {role} image")
    return image


def manager_image_line(manifest, image):
    expected = f"image: {image}".encode("ascii")
    lines = manifest.splitlines(keepends=True)
    matches = [index for index, line in enumerate(lines) if line.strip() == expected]
    if len(matches) != 1:
        raise ValueError("manager manifest must contain its inventory image on exactly one image line")
    return lines, matches[0]


def require_manager_image_only(previous, candidate):
    old, old_manifest = previous
    new, new_manifest = candidate
    for field in ("namespace", "clusterDomain"):
        value = old.get(field)
        if not isinstance(value, str) or not value or value != new.get(field):
            raise ValueError(f"{field} must match for a manager image-only change")
    if old["sha256"]["crds.yaml"] != new["sha256"]["crds.yaml"]:
        raise ValueError("CRD checksums differ")
    if release_image(old, "java") != release_image(new, "java"):
        raise ValueError("Java image references differ")

    old_image, new_image = release_image(old, "manager"), release_image(new, "manager")
    if old_image == new_image:
        raise ValueError("manager image references are identical")
    old_lines, old_position = manager_image_line(old_manifest, old_image)
    new_lines, new_position = manager_image_line(new_manifest, new_image)
    old_lines[old_position] = old_lines[old_position].replace(old_image.encode("ascii"), new_image.encode("ascii"), 1)
    if old_position != new_position or old_lines != new_lines:
        raise ValueError("manager manifests differ beyond the manager image")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package", type=Path)
    parser.add_argument("--public-key", required=True, type=Path, help="trusted PEM public key obtained separately")
    parser.add_argument("--manager-image-only-from", type=Path, metavar="PACKAGE",
                        help="verify both signed packages with the supplied public key and compare them")
    args = parser.parse_args()
    try:
        candidate = verify(args.package, args.public_key)
        if args.manager_image_only_from is not None:
            previous = verify(args.manager_image_only_from, args.public_key)
            require_manager_image_only(previous, candidate)
    except (OSError, ValueError) as error:
        parser.exit(1, f"release verification failed: {error}\n")
    print(json.dumps(candidate[0], indent=2))


if __name__ == "__main__":
    main()
