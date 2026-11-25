"""Regression tests for OCI image integrity and scanner identity."""

import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from release_sbom import generate_sbom, verify_archive_blobs


def described(blob):
    digest = hashlib.sha256(blob).hexdigest()
    return {"digest": f"sha256:{digest}", "size": len(blob)}


def put(archive, descriptor, content):
    member = tarfile.TarInfo("blobs/" + descriptor["digest"].replace(":", "/", 1))
    member.size = len(content)
    archive.addfile(member, io.BytesIO(content))


class ReleaseSBOMTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.archive = self.root / "image.oci.tar"
        self.spdx = self.root / "image.spdx.json"

    def fixture(self, layer_payload=b"layer"):
        layer, config = described(b"layer"), described(b"{}")
        manifest = json.dumps({"config": config, "layers": [layer]}).encode()
        image = described(manifest)
        with tarfile.open(self.archive, "w") as tar:
            put(tar, image, manifest)
            put(tar, config, b"{}")
            put(tar, layer, layer_payload)
        return image["digest"]

    def test_rejects_tampered_manifest_layer(self):
        digest = self.fixture(b"other")
        with self.assertRaisesRegex(ValueError, "blob checksum or size differs"):
            verify_archive_blobs(self.archive, digest)

    def test_rejects_missing_requested_manifest(self):
        self.fixture()
        with self.assertRaisesRegex(ValueError, "does not contain a regular blob"):
            verify_archive_blobs(self.archive, "sha256:" + "0" * 64)

    def test_returns_spdx_only_for_the_scanned_release_digest(self):
        digest = self.fixture()
        image = "registry.example/release/image@" + digest
        spdx = b'{"spdxVersion":"SPDX-2.3"}'
        self.spdx.write_bytes(spdx)

        def scanner_report(value):
            metadata = {"source": {"metadata": {"manifestDigest": value}}}
            return subprocess.CompletedProcess(["syft"], 0, stdout=json.dumps(metadata).encode())

        with patch("release_sbom.subprocess.run", return_value=scanner_report(digest)):
            self.assertEqual(generate_sbom(self.archive, image, "linux/amd64", self.spdx), spdx)

        with patch("release_sbom.subprocess.run", return_value=scanner_report("sha256:" + "0" * 64)):
            with self.assertRaisesRegex(ValueError, "Syft scanned manifest"):
                generate_sbom(self.archive, image, "linux/amd64", self.spdx)


if __name__ == "__main__":
    unittest.main()
