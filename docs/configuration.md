# Configuration

Every setting is read from the environment with the `DD_` prefix. Nothing is
read from a config file, so a container needs only its environment and a
development machine needs only `.env`.

Defaults are chosen so that `make run` works with no setup at all. Copy
`.env.example` to `.env` to change any of them; the server does not read `.env`
itself, so export the values or use your shell, `direnv`, or a container
orchestrator.

## HTTP

| Variable | Default | Meaning |
| --- | --- | --- |
| `DD_HTTP_ADDR` | `:8080` | Listen address. Use `127.0.0.1:8080` to refuse outside connections. |
| `DD_HTTP_READ_TIMEOUT` | `30s` | Limit on reading a whole request. `0` disables it. |
| `DD_HTTP_WRITE_TIMEOUT` | `0` | Left off on purpose: uploads and downloads stream, and a write deadline would cut a large transfer off mid-flight. |
| `DD_HTTP_READ_HEADER_TIMEOUT` | `10s` | Limit on reading request headers. This is the slowloris defence; a small value is safe even with streaming bodies. |
| `DD_HTTP_IDLE_TIMEOUT` | `120s` | How long a keep-alive connection may sit unused. |
| `DD_HTTP_SHUTDOWN_TIMEOUT` | `15s` | How long in-flight requests get to finish after `SIGINT` or `SIGTERM`. |

## Database

| Variable | Default | Meaning |
| --- | --- | --- |
| `DD_DATABASE_PATH` | `./data/distributed-drive.db` | SQLite file. Parent directories are created on startup. |
| `DD_DATABASE_BUSY_TIMEOUT` | `5s` | How long a statement waits for a write lock before failing. |
| `DD_DATABASE_MAX_OPEN_CONNS` | `1` | Left at 1 deliberately: SQLite serialises writers, and a second connection only turns a lock wait into a `database is locked` error. Raise it for read-heavy workloads only. |
| `DD_MIGRATE_ON_START` | `true` | Apply pending migrations during startup. |

## Logging

| Variable | Default | Meaning |
| --- | --- | --- |
| `DD_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |
| `DD_LOG_FORMAT` | `json` | `json` for anything that aggregates logs, `text` for a human at a terminal. |

Logs go to stdout. Durations are rendered as `15s` rather than as a nanosecond
count, so a shutdown log reads correctly at a glance.

## Storage providers

Storage is configured separately from the core service, because a deployment
whose nodes are authorised against another instance needs the pool and file
routes without any provider credentials at all.

The node routes are registered only when at least one provider is enabled.
Otherwise the service still starts and still serves everything else; it simply
has no nodes to report.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DD_STORAGE_TOKEN_KEY` | *(none)* | Encrypts every OAuth refresh token at rest. Required for the Google provider. |
| `DD_STORAGE_TOKEN_DIR` | `./data/tokens` | Where the per-node encrypted token files live. |
| `DD_STORAGE_GOOGLE_CLIENT_ID` | *(none)* | The OAuth client that authorises accounts. |
| `DD_STORAGE_GOOGLE_CLIENT_SECRET` | *(none)* | Its secret. Not actually secret, as below. |
| `DD_STORAGE_GOOGLE_REDIRECT_URI` | *(none)* | Must match a redirect registered in the console. Empty uses the loopback default. |
| `DD_STORAGE_LOCAL_NODE_ROOT` | *(none)* | Parent directory for local-filesystem nodes. Empty disables that provider. |

Three decisions here are worth stating, because each of them is a place where
the convenient choice is the wrong one.

**The token key has no default.** Every other value in this file has one. A
defaulted key would mean every deployment in the world encrypts its Google
credentials under the same value, and the encryption would be theatre. An unset
key disables the Google provider instead of falling back, so the failure is
visible in the configuration rather than discovered when the database is stolen.

**The OAuth client secret is stored in plain configuration.** A web client
secret cannot be kept secret: it ships in the client, and Google's own guidance
is to embed it. What must stay secret is the refresh token, and that is what the
token key protects. Calling the secret a secret in this document would be
misleading.

**A node's local directory is derived from its id, not accepted as input.** The
API has no field for it. A node record cannot name a path, so the node routes
cannot be turned into a remote file browser, and renaming a node does not
silently change where its data lives.

`Config.String()` reports whether a token key is set and never its value, so
the startup log is safe to paste into a bug report.

## Migrations

Migrations are embedded in the binary and identified by filename:

```text
internal/db/migrate/migrations/0001_init.sql
```

The four-digit prefix is the version and the remainder is a label. On startup
the migrator applies every version the database has not recorded, each in its
own transaction, and records it in `schema_migrations`. Applying an already
applied version is a no-op, so restarts are free.

Adding a migration means adding a new file and never editing an applied one: a
database in the field has already run the old file, and changing it would make
the two disagree with no way to detect which is right.

## Invalid configuration

Configuration is validated once, at startup. A bad value fails the process with
a message naming the variable, rather than starting in a state where the first
request fails in a way that points nowhere near the cause.
