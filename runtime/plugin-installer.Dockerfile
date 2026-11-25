# Both images must be supplied by digest and qualified by the image builder.
ARG GO_BASE
ARG RUNTIME_BASE
FROM ${GO_BASE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal/plugins ./internal/plugins
COPY cmd/plugin-installer ./cmd/plugin-installer
RUN CGO_ENABLED=0 go build -trimpath -o /plugin-installer ./cmd/plugin-installer

FROM ${RUNTIME_BASE}
COPY --from=build /plugin-installer /plugin-installer
USER 10001:10001
ENTRYPOINT ["/plugin-installer"]
