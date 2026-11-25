#!/bin/sh
set -eu

# The operator supplies the complete JVM arguments after validating them.
unset JDK_JAVA_OPTIONS JAVA_TOOL_OPTIONS _JAVA_OPTIONS

mkdir -p /var/lib/serverless/config
if [ -n "${SERVERLESS_STARTUP_BUNDLE_SHA256:-}" ]; then
  /opt/serverless/bin/health-probe \
    -stage-credentials-from /var/run/serverless/startup-credentials \
    -stage-credentials-to /var/lib/serverless/config \
    -startup-bundle-sha256 "$SERVERLESS_STARTUP_BUNDLE_SHA256"
fi

# Each argument is a complete JVM option or -D setting supplied by the operator.
# The image provides the shell and its production runtime dependencies in /opt/serverless/lib.
exec java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -Dio.netty.noUnsafe=true \
  -Dio.netty.noKeySetOptimization=true \
  -Dio.netty.recycler.maxCapacityPerThread=0 \
  -Dopensearch.set.netty.runtime.available.processors=false \
  -cp '/opt/serverless/lib/*' \
  "$@" \
  org.opensearch.serverless.shell.ServerlessBootstrap
