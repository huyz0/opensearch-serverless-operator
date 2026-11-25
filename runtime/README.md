# Java entrypoint

The development entrypoint starts the shell from `/opt/serverless/lib` and passes each container argument to the JVM as one token. The caller must supply required settings including `path.home`, `opensearch.path.conf`, node identity, network addresses, role, zone and store settings. Secure settings use the selected credential mechanism.

The entrypoint follows the JVM flags exercised by the pinned shell process harness. A Linux image, PID 1 shutdown, filesystem permissions, TLS, startup and readiness probes, and workload identity remain to be qualified before this entrypoint is used in production. [Runtime build evidence](../docs/implementation/java-runtime-build.md)

`runtime/Dockerfile` is a development image definition. From the repository
root, run `python tools/prepare_java_runtime.py --source-repo <java-git-repo>
--output tools/java-clean-built-<name> --build --stage`. The command extracts
the pinned commit into a new directory, applies the warm-up, peer DNS, S3
web-identity, local lease, route process observation, writer admission,
writer handoff reporting, durable receipts, authenticated readback,
writer head inventory, retirement preparation, and traffic withdrawal
patches, builds with the peer-TLS
source, and writes a bundle and hash manifest under `runtime/staged-bundle`.
It refuses to overwrite an existing
checkout or staged bundle. Set `JAVA_HOME` and use a populated Gradle cache;
the build runs offline. Pass digest-pinned `JAVA_BASE` and `GO_BASE` build args
to the Docker build. The Dockerfile copies the staged bundle and manifest, and
its Dockerfile-specific ignore file excludes the source checkout from the
context. The manifest records inputs and outputs, but is not a signed
attestation and does not verify the base images or runtime behavior.

The image runs as UID and GID 10001. It expects its home, configuration and
temporary directories to be provided as writable mounts where needed. Build and
run the image on Linux before treating it as qualified.

Use a bundle with the route process overlay for the opt-in development
rollout. It adds `process_incarnation` to the same local stats response
as the lease. Older bundles still provide ordinary lease observations.
Rollout requires the identity when qualifying the target before a switch
and when verifying the production route afterward. Use a compatible
bundle for the rollback fleet as well.

Add `--basic-auth` to the runtime preparation command to include the
pinned shell auth JAR. The entrypoint verifies the mounted keystore
and probe password against the approved bundle checksum before it
copies them into the Java container's private configuration directory.
See [managed Basic Auth startup](../docs/design/security-reload.md#development-basic-auth-at-startup).
