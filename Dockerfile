# The builder supplies the Go toolchain and trusted system roots.
ARG GO_BASE=docker.io/library/golang:1.23.12@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0
FROM ${GO_BASE} AS build-base
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal ./internal
COPY pkg ./pkg

FROM build-base AS observer-build
COPY cmd/rollout-proxy-observer ./cmd/rollout-proxy-observer
RUN go build -mod=readonly -trimpath -o /rollout-proxy-observer ./cmd/rollout-proxy-observer

FROM build-base AS publisher-build
COPY cmd/rollout-evidence-publisher ./cmd/rollout-evidence-publisher
RUN go build -mod=readonly -trimpath -o /rollout-evidence-publisher ./cmd/rollout-evidence-publisher

FROM build-base AS manager-build
COPY cmd/operator ./cmd/operator
RUN go build -mod=readonly -trimpath -o /operator ./cmd/operator

FROM scratch AS runtime
COPY --from=build-base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532

FROM runtime AS rollout-proxy-observer
COPY --from=observer-build /rollout-proxy-observer /rollout-proxy-observer
ENTRYPOINT ["/rollout-proxy-observer"]

FROM runtime AS rollout-evidence-publisher
COPY --from=publisher-build /rollout-evidence-publisher /rollout-evidence-publisher
ENTRYPOINT ["/rollout-evidence-publisher"]

FROM runtime AS manager
COPY --from=manager-build /operator /operator
ENTRYPOINT ["/operator"]
