# Distributed Drive

A fault-tolerant storage service that splits files into erasure-coded shards and
spreads them across multiple Google Drive accounts, so no single account can
destroy the data.

## Features

- **Multi-stripe erasure coding** — files split into K+M shards per stripe with configurable redundancy
- **Streaming upload/download** — no spooling, works with files larger than memory
- **Automatic recovery** — detects missing shards and rebuilds them onto healthy nodes
- **Integrity verification** — SHA-256 at file, stripe, and shard levels
- **Server-side encryption** — AES-256-GCM or XChaCha20-Poly1305, per-file keys
- **Health monitoring** — node status tracking with automatic degradation detection
- **Abandoned upload cleanup** — sweeps incomplete uploads after configurable timeout

## Quick start

Requires Go 1.26 or newer. No database server, no Docker, no CGO: SQLite is
embedded in the binary through `modernc.org/sqlite`.

```bash
cp .env.example .env      # optional: every value has a working default
make run                  # or: go run ./cmd/server
```

Then, in another shell:

```bash
curl -s localhost:8080/health
# {"status":"ok","version":"dev","time":"..."}

curl -s -X POST localhost:8080/api/pools \
  -H 'Content-Type: application/json' \
  -d '{"name":"primary","dataChunks":4,"parityChunks":2,"chunkSize":8388608}'
# 201 Created, Location: /api/pools/01M...

curl -s localhost:8080/api/pools
```

`make help` lists every target.

## Configuration

All configuration comes from the environment, prefixed `DD_`. Defaults are
chosen so that `make run` works with no setup. See
[`.env.example`](.env.example) for the full list and
[docs/configuration.md](docs/configuration.md) for what each one controls.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness. Never touches the database. |
| `GET` | `/health/ready` | Readiness. Returns 503 when the store is unreachable. |
| `POST` | `/api/pools` | Create a pool. |
| `GET` | `/api/pools` | List pools. |
| `GET` | `/api/pools/{poolID}` | Fetch one pool. |
| `DELETE` | `/api/pools/{poolID}` | Delete an empty pool. |
| `GET` | `/api/pools/{poolID}/capacity` | Aggregate capacity and node shortfall. |
| `POST` | `/api/pools/{poolID}/nodes` | Register a storage node. |
| `GET` | `/api/pools/{poolID}/nodes` | List nodes in a pool. |
| `DELETE` | `/api/pools/{poolID}/nodes/{nodeID}` | Detach a node from a pool. |
| `GET` | `/api/nodes` | List all nodes. |
| `GET` | `/api/nodes/{nodeID}` | Fetch a node. |
| `DELETE` | `/api/nodes/{nodeID}` | Delete a node. |
| `POST` | `/api/nodes/check` | Check all nodes. |
| `GET` | `/api/nodes/{nodeID}/status` | Check one node. |
| `POST` | `/api/pools/{poolID}/files` | Upload a file. |
| `GET` | `/api/pools/{poolID}/files` | List files in a pool. |
| `GET` | `/api/pools/{poolID}/files/{fileID}` | Fetch file metadata. |
| `GET` | `/api/pools/{poolID}/files/{fileID}/download` | Download a file. |
| `POST` | `/api/pools/{poolID}/files/{fileID}/repair` | Repair a file. |
| `POST` | `/api/pools/{poolID}/files/{fileID}/scrub` | Scrub a file. |
| `POST` | `/api/pools/{poolID}/maintenance/repair` | Repair all files in pool. |
| `POST` | `/api/pools/{poolID}/maintenance/scrub` | Scrub all files in pool. |
| `POST` | `/api/pools/{poolID}/maintenance/sweep` | Sweep abandoned uploads. |

Every error uses one envelope:

```json
{
  "error": {
    "code": "bad_request",
    "message": "chunk size must be at least 1048576 bytes, got 1",
    "requestId": "0f9c1a2e-..."
  }
}
```

`code` is stable and safe to branch on. `message` is for humans. `requestId` is
echoed in the `X-Request-ID` response header and in every log line for that
request.

## Architecture

```text
cmd/server            process entry point and exit code only
internal/app          wiring, startup, graceful shutdown
internal/api          HTTP: routing, status codes, JSON, middleware
internal/pool         storage pool rules (identity, validation, capacity)
internal/metadata     domain model and the Store interface
internal/metadata/sqlite   the Store, on SQLite
internal/db           connection, pragmas
internal/db/migrate   embedded, versioned migrations
```

The dependency direction is one-way: `api` knows `pool`, `pool` knows
`metadata`, and only `metadata/sqlite` knows SQL. Adding a PostgreSQL store
means implementing `metadata.Store` and changing one line in `internal/app`.

See [docs/architecture.md](docs/architecture.md) for the reasoning.

## Development

```bash
make check        # gofmt check, go vet, go test — the gate before every commit
make test-race    # the same tests under the race detector
make cover        # coverage summary
make lint         # golangci-lint, if installed
```

Migrations are embedded in the binary and applied on startup when
`DD_MIGRATE_ON_START=true`. They are idempotent, so a restart against an
already-migrated database applies nothing.

## Docker

```bash
make docker-up     # build and run the service
make docker-down   # stop and remove the volume
```

The compose file is a convenience for running the service in a container. There
is no database container: SQLite lives on a volume inside the service container,
which keeps a local `make run` and a containerised run behaviourally identical.

## License

See [LICENSE](LICENSE).
