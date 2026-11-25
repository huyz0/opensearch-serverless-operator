# Build with --build-arg JAVA_BASE=<qualified JRE image by digest> from the
# operator repository root after preparing runtime/staged-bundle.
ARG JAVA_BASE
ARG GO_BASE
FROM ${GO_BASE} AS health-probe
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/v1alpha1 ./api/v1alpha1
COPY internal/java ./internal/java
COPY cmd/health-probe ./cmd/health-probe
RUN CGO_ENABLED=0 go build -trimpath -o /health-probe ./cmd/health-probe

FROM ${JAVA_BASE}

COPY runtime/staged-bundle/lib/ /opt/serverless/lib/
COPY runtime/staged-bundle/operator-build-manifest.json /opt/serverless/operator-build-manifest.json
COPY runtime/serverless-entrypoint.sh /opt/serverless/bin/entrypoint
COPY --from=health-probe /health-probe /opt/serverless/bin/health-probe

USER 10001:10001
WORKDIR /opt/serverless
ENTRYPOINT ["/bin/sh", "/opt/serverless/bin/entrypoint"]
