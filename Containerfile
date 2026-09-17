# Chronos container image, built with podman.
#
#   podman build -t localhost/chronos:dev -f Containerfile .
#
# Dependencies are vendored, so the build stage needs no network access and the
# image is reproducible from a clean clone.

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
# Must satisfy the go directive in go.mod (pinned to 1.26.0). GOTOOLCHAIN=local
# keeps the build offline: without it Go would try to download a toolchain.
FROM docker.io/library/golang:1.26-alpine AS build

WORKDIR /src

# Copy manifests and the vendor tree first so dependency layers cache
# independently of source changes.
COPY go.mod go.sum ./
COPY vendor ./vendor

COPY cmd ./cmd
COPY internal ./internal

# CGO_ENABLED=0 gives a static binary that runs on a distroless/scratch base.
# -trimpath keeps build paths out of the binary; the ldflags strip debug info.
ARG VERSION=dev
RUN --network=none \
    CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=vendor \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/chronos-server ./cmd/chronos-server && \
    CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=vendor \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/chronos-worker ./cmd/chronos-worker

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/chronos-server /usr/local/bin/chronos-server
COPY --from=build /out/chronos-worker /usr/local/bin/chronos-worker

# Run unprivileged. The distroless nonroot user is uid 65532.
USER nonroot:nonroot

EXPOSE 8080

ENV CHRONOS_HTTP_ADDR=":8080" \
    CHRONOS_LOG_FORMAT="json"

ENTRYPOINT ["/usr/local/bin/chronos-server"]
