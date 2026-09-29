# syntax=docker/dockerfile:1

# Build the binary in a full image, then ship it in a minimal one. The runtime
# image needs no toolchain, no libc and no shell, which keeps it small and
# removes a compiler from the attack surface of a service that holds OAuth
# tokens in memory.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies are copied first so that a source-only change reuses the cached
# module layer instead of re-downloading on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# CGO is off because the SQLite driver is pure Go. -trimpath keeps build paths
# out of the binary so stack traces are reproducible.
RUN CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X github.com/VibolSovichea/distributed-drive/internal/api.version=${VERSION}" \
      -o /out/distributed-drive ./cmd/server

# Run vet in the image build. Tests run in the Verify CI job with race detector.
RUN go vet ./...

FROM alpine:3.20

# ca-certificates for outbound TLS to Google; tzdata so RFC 3339 timestamps
# render in the configured zone.
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 drive

COPY --from=build /out/distributed-drive /usr/local/bin/distributed-drive

# The database lives on a volume. A path under /data keeps it out of the
# container's writable layer, so it survives a restart and a rebuild.
RUN mkdir -p /data && chown drive:drive /data
VOLUME ["/data"]

USER drive:drive

ENV DD_HTTP_ADDR=:8080 \
    DD_DATABASE_PATH=/data/distributed-drive.db \
    DD_LOG_FORMAT=json

EXPOSE 8080

# The liveness probe must not depend on the database: a database problem should
# not make the orchestrator restart a process that is otherwise healthy.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/health >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/distributed-drive"]
