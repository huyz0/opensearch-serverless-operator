"""Export and verify the DNS lease overlay against the pinned shell commit."""

import argparse
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "e71d42331a3ec82ec01109a5bb42d6a9a9162a5c"
FILES = ("NodeSettings.java", "NodeLogStart.java", "NodeStartup.java")
PREFIX = "serverless/shell/src/main/java/org/opensearch/serverless/node/"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repo", required=True)
    args = parser.parse_args()
    patch = bytearray()
    originals = {}
    for name in FILES:
        path = PREFIX + name
        baseline = "tools/java-baseline/peer-dns-" + name
        edited = "tools/java-source/" + path
        original = subprocess.check_output(["git", "-C", args.source_repo, "show", f"{REVISION}:{path}"])
        if (ROOT / baseline).read_bytes() != original:
            raise SystemExit(f"baseline differs from pinned commit: {path}")
        originals[path] = original
        diff = subprocess.run(["git", "-c", "core.autocrlf=false", "diff", "--no-index", "--", baseline, edited], cwd=ROOT, capture_output=True)
        if diff.returncode != 1:
            raise SystemExit(f"expected a changed file: {path}: {diff.stderr.decode(errors='replace')}")
        patch.extend(diff.stdout.replace(("a/" + baseline).encode(), ("a/" + path).encode()).replace(("b/" + edited).encode(), ("b/" + path).encode()))
    with tempfile.TemporaryDirectory(prefix="peer-dns-patch-") as temporary:
        temp = pathlib.Path(temporary)
        for path, original in originals.items():
            target = temp / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(original)
        patch_path = temp / "change.patch"
        patch_path.write_bytes(patch)
        subprocess.run(["git", "-c", "core.autocrlf=false", "apply", "--check", str(patch_path)], cwd=temp, check=True)
        subprocess.run(["git", "-c", "core.autocrlf=false", "apply", str(patch_path)], cwd=temp, check=True)
        for path in originals:
            if (temp / path).read_bytes() != (ROOT / "tools/java-source" / path).read_bytes():
                raise SystemExit(f"patched file differs from tested file: {path}")
    target = ROOT / "java/patches/peer-dns-advertisement.patch"
    target.write_bytes(patch)
    print(f"Verified {REVISION} and exported {target}")


if __name__ == "__main__":
    main()
