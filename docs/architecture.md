# Architecture

## Layers

```text
cmd/server
    │  exit code, signal plumbing, nothing else
    ▼
internal/app
    │  config → logger → database → store → services → HTTP server
    ▼
internal/api            HTTP: routing, status codes, JSON, middleware
    ▼
internal/authflow      OAuth handshake: single-use, expiring, signed state
internal/chunker       file → fixed chunks, and chunks → file
    ▼
internal/pool           pool rules: identity, validation, capacity
    ▼
internal/metadata       domain model + Store interface
    ▼
internal/metadata/sqlite   the one package that knows SQL
```

Dependencies point downwards only. `internal/api` cannot import `internal/db`,
and `internal/pool` cannot import `internal/api`, because nothing in the code
enforces that today but the import graph and the review that maintains it.

## Why the boundaries are where they are

**`api` owns HTTP and nothing else.** Handlers decode a body, call one service
method, and translate the result. The rules a request must satisfy are never
written in a handler, so they cannot be bypassed by adding a second entry point
later.

**`pool` owns the invariants.** A pool identifier is generated here, never
accepted from a client. A pool is validated here, before the store sees it, so
the error names the rule that was broken instead of a SQLite constraint.
Deletion is guarded here: the schema cascades pool to file to chunk, so deleting
a pool that still holds files would erase the metadata of files whose shards
are still on storage nodes. That guard is a service rule, not a schema rule,
because the schema cannot know a cascade is destructive.

**`metadata` is the vocabulary.** Pool, node, file and chunk are defined with no
SQL, no HTTP and no provider concepts. Every later phase — chunking, erasure
coding, placement, recovery — is written against these types, so the storage
engine never has to know that SQLite exists.

**`metadata/sqlite` is the only package with SQL.** One file per aggregate, plain
prepared statements, no query builder. The only translation is result-code to
sentinel error, so callers match on `metadata.ErrNotFound` and never on driver
text.

## Error handling

`metadata` defines three sentinels: `ErrNotFound`, `ErrConflict`, `ErrInvalid`.
The store translates driver errors into them, `pool` wraps them with context,
and `api` maps them to status codes in one switch.

Validation messages reach the client, because a client can only fix a rule it
can read. Everything else is logged in full and replaced with a generic message
before it leaves the process: a driver message can contain a file path,
a connection string or, in a future phase, an OAuth token.

## Time and identifiers

Identifiers are monotonic ULIDs: 26 characters, lexicographically sortable, so
`ORDER BY id` is chronological and a listing needs no extra sort. Two
identifiers minted in the same millisecond still increase.

Timestamps are stored as integer Unix nanoseconds and converted to
`time.Time` at the edge. SQLite has no native time type, and text timestamps
would make ordering and comparison string operations.

## Concurrency

`internal/id` serialises entropy with a mutex. The upstream monotonic entropy
source is stateful and is not safe for concurrent use; the library's locked
variant does not satisfy `io.Reader`, so a plain mutex is the correct fix. This
is documented at the call site because it looks like a gratuitous lock.

The SQLite store runs on one connection by default. Writers serialise anyway,
and a second connection converts a brief lock wait into a hard failure.

## Decisions taken, and what they cost

**Root module rather than `backend/go.mod`.** The plan shows a `backend/`
directory. The module was kept at the repository root because the frontend is
not being built yet, and a nested module in a repository with no other module
adds a directory level and a replace directive for no present benefit. Moving it
later is a `git mv` plus an import path change.

**`modernc.org/sqlite` rather than `mattn/go-sqlite3`.** Pure Go, so there is no
CGO, no cross-compilation toolchain, and `go test` works on any machine that can
build Go. The cost is a slower driver, which does not matter for a metadata
database on a local volume.

**Custom migrator rather than a library.** The requirements are one SQL file per
version, a recorded version, and idempotency, all of which are a hundred lines.
A library would add a dependency and a configuration dialect to express
something the project already controls.

**SQLite runs inside the container, not as a separate service.** The
requirement was that SQLite must not be installed on the host, and a separate
database container was considered and rejected. SQLite is a library, so a
"database service" is either a wrapper we own or a different database wearing
the name; both cost a network hop, a second backup, and a failure mode that
`make run` cannot reproduce. The pure-Go driver satisfies the actual
requirement: there is no `sqlite3` binary, no system package and no C library
on the host or in the image, because the database engine is compiled into the
binary. `docker compose up` and `make run` run the same code against the same
file format. The cost is that the database is not reachable from outside the
container, which for a metadata store is not a cost. If V1 ever needs a second
writer, the plan is Postgres, and the layering above `metadata` is what makes
that a driver swap rather than a rewrite.

**Server-side identifiers and timestamps.** A client cannot choose a pool id.
This costs nothing and removes a class of collision and spoofing bugs.

**Bounded request bodies.** Every current endpoint takes metadata, never file
content, so bodies are capped at 1 MiB. The cap is where the streaming upload
endpoint will replace it.

## Chunks, stripes, and the encryption decision

`chunker` splits a stream into fixed-size chunks with a SHA-256 per chunk, and
joins chunks back in index order while verifying each one. It knows nothing about
HTTP, providers or the store, so both the upload and the download path use the
same rules and a disagreement between them is impossible.

### A file is several stripes, not one

A file is encoded into **stripes** of K data shards plus M parity shards each, and
a file is a *sequence* of them. This is the single most consequential structural
decision in the storage layer, and it is worth recording what it replaced.

The first version had one stripe per file, because the schema keyed chunks on
`(file_id, chunk_index)` with no stripe in the key, so a file could hold at most
K+M shards. That has a hard consequence. With a fixed chunk size, the largest
file a pool can accept is `K * chunk_size`, so a 1 MiB chunk size with K=4 caps
uploads at 4 MiB, and a "distributed drive" that cannot hold a film is not a
drive.

The alternative considered was deriving the chunk size from each file's size,
which removes the ceiling and wastes no space on small files. It was rejected
because the erasure coder's working set is `(K+M) * chunk_size`, so making the
chunk size a function of the file size makes memory consumption a function of
the file size too. A 4 GiB file would want 1 GiB shards and several GiB resident.
The cap would simply move to wherever the host ran out of memory, and it would
move silently, mid-upload.

Multi-stripe keeps the working set at `(K+M) * chunkSize` no matter how large the
file is, and the file size is bounded by disk rather than by arithmetic. Each
stripe is encoded, uploaded and recovered independently, so the cost of a large
file is paid incrementally. This required migration `0002`, which rebuilds the
`chunks` table to key on `(file_id, stripe_index, chunk_index)`; SQLite
materialised the old key as an implicit index that cannot be dropped, so adding
a column was not an option.

The cost of multi-stripe is padding. A file is padded out to a whole number of
stripes, so a 3 MiB file in a pool of 1 MiB shards costs 6 MiB rather than 3.
That is why the default chunk size is 1 MiB rather than 8 MiB: with 8 MiB shards
the same 3 MiB file would cost 48 MiB, because the whole stripe is padded. The
worst case per file is one stripe, so the waste is bounded by `K * chunkSize` and
does not grow with the file. Operators whose workload is large files and few of
them can raise the chunk size toward `MaxChunkSize` and pay more memory per
stripe for fewer objects per file.

`chunker.Plan(total, chunkSize, k)` computes the layout, and the download path
recomputes it from the same three numbers the database recorded. Both must agree
with the splitter, and a property test sweeps the size and K boundaries asserting
exactly that. A layout that disagreed with the splitter would upload cleanly and
download corrupt, which is the failure this package exists to rule out.

An empty file has **no** stripes. It has nothing to encode and nothing to
protect, and inventing a stripe of empty shards would put a degenerate case
through the erasure coder for no gain. This does not conflict with the schema's
`data_chunks > 0`, which constrains K and never the shard count.

Two further properties are deliberate and are pinned by tests:

- **A chunk's `Data` aliases a reused buffer** and is only valid until the next
  `Next` call. That is what lets a file larger than memory be chunked, and it is
  why `Chunk` carries the hash rather than expecting the caller to re-read the
  slice later.
- **A joiner refuses a plan with a gap or a duplicate** rather than assembling
  what it can. A missing chunk is precisely the situation recovery exists for,
  and a silently shortened file is indistinguishable from a successful download
  until it is too late.

### Uploads are not spooled

An earlier design measured the whole request body to a temporary file before
splitting it, because deriving the chunk size from the file size needs the size
up front. With multi-stripe the chunk size is fixed, so the size is not needed
before anything can happen: stripe 0 can be encoded and uploaded while the rest
of the body is still arriving, and the total is accumulated as it streams past.

That removes the spool entirely, and it is worth saying why, because the spool
was chosen deliberately once already. Its cost was real: every upload wrote a
full extra copy of itself to disk, a burst of N concurrent uploads needed N times
the largest upload in free space, and a failed upload had to restart from zero.
The benefit was that it avoided requiring `Content-Length`, which matters because
`Content-Length` is absent from a browser `ReadableStream` body. Removing the
spool does not reintroduce that requirement, because nothing needs the size up
front any more. The requirement vanished with the need for it, rather than being
traded away.

One consequence is worth recording: the `files` row must be written **before** the
body is read, since chunks reference the file, so an upload that never finishes
leaves a row in `uploading` with no content hash. The schema already forbids
promoting such a row to `committed`, and the abandoned-upload sweep is what
removes it.

Encryption is **server-side**: a file is encrypted after chunking and before
shards are placed, so a node stores ciphertext it cannot read. The chunker
therefore deals in plaintext and needs no per-chunk nonce, which keeps this
package independent of the crypto choice. The consequence is that a node is a
blind store rather than an untrusted one: it still learns the shard sizes, and it
can delete or corrupt data, just not read it.

## Provider credentials

No layer above `internal/factory` knows how a provider is authenticated. The
node service asks a `node.Factory` for a client and receives one or an error
distinguishing "this account is not authorised" from "this deployment is
misconfigured", because the two need completely different fixes and the operator
cannot tell them apart from a generic failure.

Three consequences are deliberate:

- **Tokens are encrypted at rest with a key from configuration that has no
  default.** A defaulted key would be the same everywhere and would protect
  nothing, so an unset key disables the provider rather than falling back.
- **One token file per node id, not per account.** Keying by account would let
  two records share a credential, so detaching one would break the other.
- **A local node's directory is derived from its id, never supplied.** The
  registration API has no path field, which is what stops the node routes from
  becoming a remote file browser.

The OAuth client id and secret are treated as public. A web client secret ships
in the client and Google's guidance is to embed it; the secret that must be
protected is the refresh token, and that is what the token key covers.

## Testing

Tests use the real SQLite database rather than a fake store, so each layer's
tests also check that the layers below it agree. The race detector is part of
the gate because the service is concurrent by design: ULID generation, the HTTP
server and the store are all exercised from several goroutines.

Each package that exports an interface has a fake in its own test file. A fake
is used to reach states the real implementation will not produce on demand, such
as a panic inside a handler; it is not used to check ordinary behaviour.
