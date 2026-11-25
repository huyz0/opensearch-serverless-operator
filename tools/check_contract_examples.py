"""Render draft Kubernetes contracts and examples without contacting a cluster."""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "config" / "contracts"
FILES = (
    "instance.draft.yaml",
    "operation.draft.yaml",
    "fixtures/instance.yaml",
    "fixtures/operation-promote.yaml",
)


def main():
    if shutil.which("kubectl") is None:
        print("kubectl is required for offline rendering", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        for index, source in enumerate(FILES):
            shutil.copyfile(CONTRACTS / source, directory / f"resource-{index}.yaml")
        resources = "\n".join(f"- resource-{index}.yaml" for index in range(len(FILES)))
        (directory / "kustomization.yaml").write_text(f"resources:\n{resources}\n", encoding="utf-8")
        result = subprocess.run(["kubectl", "kustomize", str(directory)], capture_output=True, text=True, check=False)
        if result.returncode:
            print(result.stderr, file=sys.stderr)
            return result.returncode
        for expected in ("kind: OpenSearchServerless\n", "kind: OpenSearchServerlessOperation\n", "action: Promote"):
            if expected not in result.stdout:
                print(f"rendered resources are missing {expected.strip()}", file=sys.stderr)
                return 1
    print("Rendered both draft CRDs and their examples offline.")
    print("Schema admission and runtime behavior remain unverified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
