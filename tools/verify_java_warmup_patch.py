"""Check the Java overlay applies to the exact pinned commit without a checkout."""

import argparse
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "e71d42331a3ec82ec01109a5bb42d6a9a9162a5c"
FILES = (
    "serverless/shell/src/main/java/org/opensearch/serverless/node/SearchReadiness.java",
    "serverless/shell/src/main/java/org/opensearch/serverless/node/SearchSettings.java",
    "serverless/shell/src/main/java/org/opensearch/serverless/warm/SearchWarmup.java",
    "serverless/shell/src/main/java/org/opensearch/serverless/rest/ServerlessReadyHandler.java",
    "serverless/testkit/src/test/java/org/opensearch/serverless/testkit/SearchWarmupTests.java",
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repo", required=True, help="Git repository containing the pinned Java commit")
    args = parser.parse_args()
    patch = ROOT / "java/patches/warmup-observation.patch"
    with tempfile.TemporaryDirectory(prefix="warmup-patch-") as tmp:
        workspace = pathlib.Path(tmp)
        for name in FILES:
            data = subprocess.check_output(["git", "-C", args.source_repo, "show", f"{REVISION}:{name}"])
            target = workspace / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=workspace, check=True)
        subprocess.run(["git", "apply", str(patch)], cwd=workspace, check=True)
        extracted = ROOT / "tools/java-source"
        if extracted.exists():
            for name in FILES:
                if (workspace / name).read_bytes().replace(b"\r\n", b"\n") != (extracted / name).read_bytes().replace(b"\r\n", b"\n"):
                    raise SystemExit(f"Extracted source differs from patch result: {name}")
    print(f"Warm-up overlay applies cleanly to {REVISION}.")


if __name__ == "__main__":
    main()
