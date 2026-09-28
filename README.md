# Distributed Drive

A fault-tolerant storage service that splits files into erasure-coded shards and
spreads them across multiple Google Drive accounts, so no single account can
destroy the data.

Built incrementally from [`BUILD-PLAN.md`](BUILD-PLAN.md). The plan is the
source of truth; this README describes what exists today.

## Status

Phases 0 and 1 are complete. The web frontend is intentionally not started yet.

| Phase | Scope | State |
| --- | --- | --- |
| 0 | Project foundation, `/health` | done |
| 1 | Metadata layer (SQLite, models, repositories) | done |
| 2 | Google Drive provider and OAuth | not started |
| 3 | Storage pool manager | partial (pool lifecycle and capacity only) |
| 4+ | Chunking, erasure coding, transfer, recovery, API, UI | not started |

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

Every error uses one envelope, so a client only has to parse one shape:

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
request, so a user-reported failure can be traced to a single entry.

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
