"""Exercise the secure development package with the repository's real overlays."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
MANAGER = "registry.example/team/controller@sha256:" + "a" * 64
JAVA = "registry.example/team/java@sha256:" + "b" * 64


class DevelopmentReleaseTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name) / "release"

    def package(self, manager=MANAGER, java=JAVA, output=None, extra=()):
        return subprocess.run(
            [
                sys.executable, str(ROOT / "tools" / "package_development_release.py"),
                "--version", "v0.1.0-dev.1", "--manager-image", manager, "--java-image", java,
                "--namespace", "release-test", "--cluster-domain", "test.cluster",
                "--output", str(output or self.output), *extra,
            ],
            check=False, capture_output=True, text=True,
        )

    def test_package_pins_images_and_wires_hashed_runtime_config(self):
        result = self.package()
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.output / "release.json").read_text(encoding="utf-8"))
        self.assertEqual(inventory["version"], "v0.1.0-dev.1")
        self.assertEqual(inventory["images"], {"manager": MANAGER, "java": JAVA})
        self.assertEqual(inventory["namespace"], "release-test")
        self.assertEqual(set(inventory["sha256"]), {"crds.yaml", "manager.yaml"})
        for name, digest in inventory["sha256"].items():
            self.assertEqual(hashlib.sha256((self.output / name).read_bytes()).hexdigest(), digest)

        manifest = (self.output / "manager.yaml").read_text(encoding="utf-8")
        self.assertIn(f"image: {MANAGER}\n", manifest)
        self.assertIn(f"javaImage: {JAVA}\n", manifest)
        self.assertIn("clusterDomain: test.cluster\n", manifest)
        self.assertIn("kind: Namespace\nmetadata:\n  name: release-test\n", manifest)
        self.assertNotIn("operator-development", manifest)
        self.assertNotIn("kind: ClusterRole", manifest)
        config_map = re.search(r"  name: (opensearch-secure-runtime-[a-z0-9]+)\n", manifest)
        self.assertIsNotNone(config_map)
        self.assertEqual(manifest.count(f"name: {config_map[1]}\n"), 3)

        crds = (self.output / "crds.yaml").read_text(encoding="utf-8")
        self.assertIn("name: opensearchserverlesses.serverless.opensearch.org\n", crds)
        self.assertIn("name: opensearchserverlessoperations.serverless.opensearch.org\n", crds)

    def generate_keys(self):
        keys = self.output.parent
        private_key = keys / "release.key"
        public_key = keys / "release.pub"
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", str(private_key)], check=True)
        subprocess.run(["openssl", "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)], check=True)
        return private_key, public_key

    def test_signed_package_rejects_altered_manifests_and_inventory(self):
        private_key, public_key = self.generate_keys()
        result = self.package(extra=("--signing-key", str(private_key)))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.output / "release.json.sig").is_file())
        verify = [
            sys.executable, str(ROOT / "tools" / "verify_development_release.py"),
            str(self.output), "--public-key", str(public_key),
        ]
        verified = subprocess.run(verify, capture_output=True, text=True, check=False)
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.assertEqual(json.loads(verified.stdout)["images"], {"manager": MANAGER, "java": JAVA})

        manifest = self.output / "manager.yaml"
        manifest.write_bytes(manifest.read_bytes() + b"# changed\n")
        changed = subprocess.run(verify, capture_output=True, text=True, check=False)
        self.assertNotEqual(changed.returncode, 0)
        self.assertIn("checksum mismatch for manager.yaml", changed.stderr)

        path = self.output / "release.json"
        inventory = json.loads(path.read_bytes())
        inventory["sha256"]["manager.yaml"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
        path.write_text(json.dumps(inventory), encoding="utf-8")
        rewritten = subprocess.run(verify, capture_output=True, text=True, check=False)
        self.assertNotEqual(rewritten.returncode, 0)
        self.assertIn("release inventory signature could not be verified", rewritten.stderr)

    def test_manager_image_only_check_validates_both_signed_packages(self):
        private_key, public_key = self.generate_keys()
        signing = ("--signing-key", str(private_key))
        new_image = MANAGER.replace("a" * 64, "c" * 64)
        candidate = self.output.with_name("candidate")
        wrong_java = self.output.with_name("wrong-java")
        for options in (
            {},
            {"manager": new_image, "output": candidate},
            {"manager": new_image, "java": JAVA.replace("b" * 64, "d" * 64), "output": wrong_java},
        ):
            result = self.package(extra=signing, **options)
            self.assertEqual(result.returncode, 0, result.stderr)

        def compare(package):
            return subprocess.run([
                sys.executable, str(ROOT / "tools" / "verify_development_release.py"),
                str(package), "--public-key", str(public_key),
                "--manager-image-only-from", str(self.output),
            ], capture_output=True, text=True, check=False)

        result = compare(candidate)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(new_image, result.stdout)

        previous_crds = self.output / "crds.yaml"
        trusted_crds = previous_crds.read_bytes()
        previous_crds.write_bytes(trusted_crds + b"# changed\n")
        result = compare(candidate)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checksum mismatch for crds.yaml", result.stderr)
        previous_crds.write_bytes(trusted_crds)

        result = compare(wrong_java)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Java image references differ", result.stderr)

        manager = candidate / "manager.yaml"
        changed = manager.read_bytes().replace(b"replicas: 1", b"replicas: 2", 1)
        self.assertNotEqual(changed, manager.read_bytes())
        manager.write_bytes(changed)
        release = candidate / "release.json"
        manifest = json.loads(release.read_bytes())
        manifest["sha256"]["manager.yaml"] = hashlib.sha256(changed).hexdigest()
        release.write_text(json.dumps(manifest), encoding="utf-8")
        subprocess.run([
            "openssl", "pkeyutl", "-sign", "-rawin", "-inkey", str(private_key),
            "-in", str(release), "-out", str(candidate / "release.json.sig"),
        ], check=True)
        result = compare(candidate)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("manager manifests differ beyond the manager image", result.stderr)

    def test_refuses_mutable_image_before_creating_output(self):
        result = self.package("registry.example/team/controller:dev")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("use registry/repository@sha256", result.stderr)
        self.assertFalse(self.output.exists())

    def test_refuses_to_overwrite_existing_package(self):
        self.output.mkdir()
        sentinel = self.output / "release.json"
        sentinel.write_text("existing release", encoding="utf-8")
        result = self.package()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "existing release")


if __name__ == "__main__":
    unittest.main()
