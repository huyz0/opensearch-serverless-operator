"""Check the web-identity overlay against the pinned Java source."""

import argparse
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "e71d42331a3ec82ec01109a5bb42d6a9a9162a5c"
FILE = "serverless/shell/src/main/java/org/opensearch/serverless/shell/ServerlessBootstrap.java"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repo", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="s3-identity-patch-") as tmp:
        workspace = pathlib.Path(tmp)
        target = workspace / FILE
        target.parent.mkdir(parents=True)
        target.write_bytes(subprocess.check_output(["git", "-C", args.source_repo, "show", f"{REVISION}:{FILE}"]))
        patch = ROOT / "java/patches/s3-web-identity-setting.patch"
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=workspace, check=True)
        subprocess.run(["git", "apply", str(patch)], cwd=workspace, check=True)
        extracted = ROOT / "tools/java-source" / FILE
        if extracted.exists() and target.read_bytes().replace(b"\r\n", b"\n") != extracted.read_bytes().replace(b"\r\n", b"\n"):
            raise SystemExit("Extracted Java source differs from the patched baseline")
    print(f"S3 identity overlay applies cleanly to {REVISION}.")


if __name__ == "__main__":
    main()
