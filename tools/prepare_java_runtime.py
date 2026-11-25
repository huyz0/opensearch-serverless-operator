"""Prepare a disposable, pinned Java checkout for the development image."""

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "e71d42331a3ec82ec01109a5bb42d6a9a9162a5c"
PATCHES = (
    "java/patches/warmup-observation.patch",
    "java/patches/peer-dns-advertisement.patch",
    "java/patches/s3-web-identity-setting.patch",
    "java/patches/local-lease-observation.patch",
    "java/patches/route-process-observation.patch",
    "java/patches/writer-admission-shutdown.patch",
    "java/patches/writer-handoff-report.patch",
    "java/patches/writer-handoff-receipt.patch",
    "java/patches/writer-handoff-readback.patch",
    "java/patches/writer-head-inventory.patch",
    "java/patches/writer-retirement-prepare.patch",
    "java/patches/writer-traffic-withdrawal.patch",
    "java/patches/writer-retirement-routing.patch",
    "java/patches/writer-withdrawal-peer-view.patch",
    "java/patches/writer-prepare-deadline.patch",
    "java/patches/writer-prepared-wake.patch",
    "java/patches/writer-withdrawal-deadline.patch",
    "java/patches/writer-shutdown-defer-claims.patch",
    "java/patches/writer-retirement-release-lease.patch",
    "java/patches/writer-successor-evidence.patch",
    "java/patches/writer-successor-wal-settlement.patch",
    "java/patches/writer-wal-settlement-identity.patch",
    "java/patches/writer-wal-settlement-retry.patch",
    "java/patches/writer-partial-receipt-test.patch",
)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as data:
        for chunk in iter(lambda: data.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repo", required=True, help="Git repository containing the pinned commit")
    parser.add_argument("--output", required=True, help="new checkout directory under this repository's tools directory")
    parser.add_argument("--build", action="store_true", help="build the runtime bundle using the existing Gradle cache")
    parser.add_argument("--stage", action="store_true", help="copy the resulting bundle to the new runtime/staged-bundle directory")
    parser.add_argument("--basic-auth", action="store_true", help="include the pinned shell Basic Auth plugin on the runtime classpath")
    args = parser.parse_args()
    if args.stage and not args.build:
        parser.error("--stage requires --build")
    if args.basic_auth and not args.build:
        parser.error("--basic-auth requires --build")
    staged = ROOT / "runtime/staged-bundle"
    if args.stage and staged.exists():
        parser.error("runtime/staged-bundle already exists; choose a clean workspace")
    output = pathlib.Path(args.output).resolve()
    tools = (ROOT / "tools").resolve()
    if not output.is_relative_to(tools) or output == tools or output.exists():
        parser.error("output must be a new directory inside tools")
    source = pathlib.Path(args.source_repo).resolve()
    actual = subprocess.check_output(["git", "-C", str(source), "rev-parse", f"{REVISION}^{{commit}}"], text=True).strip()
    if actual != REVISION:
        raise SystemExit("pinned commit did not resolve exactly")
    output.mkdir(parents=True)
    process = subprocess.Popen(["git", "-C", str(source), "archive", "--format=tar", REVISION], stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            archive.extractall(path=output, filter="data")
    finally:
        process.stdout.close()
        result = process.wait()
    if result:
        raise SystemExit(f"git archive failed: {result}")
    for name in PATCHES:
        patch = ROOT / name
        subprocess.run(["git", "-c", "core.autocrlf=false", "apply", "--check", str(patch)], cwd=output, check=True)
        subprocess.run(["git", "-c", "core.autocrlf=false", "apply", str(patch)], cwd=output, check=True)
    manifest = {"sourceCommit": REVISION, "patches": {name: sha256(ROOT / name) for name in PATCHES}}
    (output / "operator-source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if args.build:
        gradle = output / ("gradlew.bat" if os.name == "nt" else "gradlew")
        if os.name != "nt":
            gradle.chmod(gradle.stat().st_mode | 0o100)
        cmd = [str(gradle), "--offline", "-Djapicmp.compare.version=3.0.0", "-Doperator.peerTlsSource=" + str(ROOT / "java/peer-tls/src/main/java"), "-I", str(ROOT / "tools/package-java.gradle"), ":serverless:shell:operatorRuntimeBundle"]
        if args.basic_auth:
            cmd.insert(1, "-Doperator.serverlessBasicAuth=true")
        subprocess.run(cmd, cwd=output, check=True)
        lib = output / "serverless/shell/build/operator-runtime/lib"
        files = sorted(lib.glob("*.jar"))
        if not files:
            raise SystemExit("runtime bundle contains no JARs")
        manifest["bundleJars"] = {path.name: sha256(path) for path in files}
        manifest["peerTlsSources"] = {path.relative_to(ROOT).as_posix(): sha256(path) for path in sorted((ROOT / "java/peer-tls/src/main/java").rglob("*.java"))}
        manifest["gradleInit"] = sha256(ROOT / "tools/package-java.gradle")
        if args.basic_auth:
            manifest["serverlessBasicAuth"] = True
        (lib.parent / "operator-build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        if args.stage:
            shutil.copytree(lib.parent, staged)
    print(f"Prepared pinned runtime source in {output}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.CalledProcessError, tarfile.TarError) as exc:
        print(f"Runtime preparation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
