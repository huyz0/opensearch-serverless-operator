"""Generate SPDX documents for digest-pinned release images."""

import hashlib
import json
import os
import re
import subprocess
import tarfile


SBOM_FILES = {"manager": "manager.spdx.json", "java": "java.spdx.json"}


def blob_member(members, digest):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError(f"invalid OCI blob digest: {digest}")
    name = "blobs/sha256/" + digest.split(":", 1)[1]
    member = members.get(name)
    if member is None or not member.isfile():
        raise ValueError(f"OCI archive does not contain a regular blob for {digest}")
    return member


def verify_archive_blobs(path, digest):
    with tarfile.open(path) as archive:
        blobs = {}
        for member in archive.getmembers():
            if member.name.startswith("blobs/sha256/"):
                if member.name in blobs:
                    raise ValueError(f"duplicate OCI archive blob: {member.name}")
                blobs[member.name] = member
        with archive.extractfile(blob_member(blobs, digest)) as data:
            raw = data.read()
        if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError(f"OCI image manifest checksum differs from {digest}")
        manifest = json.loads(raw)
        if (not isinstance(manifest, dict) or not isinstance(manifest.get("config"), dict)
                or not isinstance(manifest.get("layers"), list)):
            raise ValueError("the release reference must pin an image platform manifest")
        for descriptor in [manifest["config"], *manifest["layers"]]:
            blob = blob_member(blobs, descriptor["digest"])
            checksum = hashlib.sha256()
            with archive.extractfile(blob) as data:
                for chunk in iter(lambda: data.read(1024 * 1024), b""):
                    checksum.update(chunk)
            if blob.size != descriptor["size"] or "sha256:" + checksum.hexdigest() != descriptor["digest"]:
                raise ValueError(f"OCI archive blob checksum or size differs from {descriptor['digest']}")


def generate_sbom(archive, image, platform, output):
    digest = image.rsplit("@", 1)[-1]
    try:
        verify_archive_blobs(archive, digest)
    except (tarfile.TarError, KeyError, TypeError) as error:
        raise ValueError(f"could not verify the OCI archive for {image}: {error}") from error

    result = subprocess.run([
        "syft", "scan", str(archive.resolve()), "--from", "oci-archive", "--platform", platform,
        "-o", "syft-json", "-o", f"spdx-json={output}",
    ], check=True, stdout=subprocess.PIPE, env={**os.environ, "SYFT_CHECK_FOR_APP_UPDATE": "false"})
    report = json.loads(result.stdout)
    scanned = report.get("source", {}).get("metadata", {}).get("manifestDigest")
    if scanned != digest:
        raise ValueError(f"Syft scanned manifest {scanned!r}, expected {digest}")
    return output.read_bytes()
