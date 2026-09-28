# API reference

Base path: none. All responses are JSON. All timestamps are RFC 3339 in UTC.
All durations in responses are integer counts of bytes or seconds.

## Conventions

**Correlation id.** Send `X-Request-ID` and it is echoed back on the response and
attached to every log line for that request. Send nothing and the server
generates one. A supplied id is trusted only if it is at most 64 characters of
`[A-Za-z0-9._-]`; anything else is replaced, because an id that reaches the logs
unfiltered is a log-injection vector.

**Errors.** Every non-2xx response has the same shape:

```json
{"error":{"code":"bad_request","message":"...","requestId":"..."}}
```

`code` is one of `bad_request`, `malformed_json`, `payload_too_large`,
`not_found`, `conflict`, `unavailable`, `method_not_allowed`, `internal_error`.
Branch on `code`. Treat `message` as human-readable and never parse it.

**Limits.** Request bodies are capped at 1 MiB, with a `413` beyond that. No
current endpoint accepts file content.

---

## `GET /health`

Liveness. Answers from memory and never consults the database: a database that
is down is not a reason to restart the process, and restarting would make
recovery harder.

```json
{"status":"ok","version":"dev","time":"2026-03-01T12:00:00Z"}
```

`200` while the process is running.

## `GET /health/ready`

Readiness. Answers whether the process can actually serve, which means the
metadata store is reachable.

```json
{"status":"ok","version":"dev","time":"...","checks":{"database":"ok"}}
```

`503` when the store cannot be reached, with `"status":"unavailable"` and
`"database":"unreachable"`. The underlying driver error is logged, never
returned.

---

## `POST /api/pools`

Creates a pool. The identifier and timestamps are chosen by the server.

Request:

```json
{
  "name": "primary",
  "dataChunks": 4,
  "parityChunks": 2,
  "chunkSize": 8388608
}
```

| Field | Rule |
| --- | --- |
| `name` | Required, 1 to 128 characters. Unique. |
| `dataChunks` | K, at least 1. |
| `parityChunks` | M, at least 0. |
| `chunkSize` | Bytes per shard, 1 MiB to 512 MiB. |

`dataChunks + parityChunks` must not exceed 256, the hard limit of the erasure
coder. An unusable pool is therefore rejected at creation rather than at the
first upload.

Response `201`, with a `Location` header:

```json
{
  "id": "01M3GEPED1QF4CP0054BZVTE82",
  "name": "primary",
  "dataChunks": 4,
  "parityChunks": 2,
  "chunkSize": 8388608,
  "shardsPerStripe": 6,
  "redundancy": "4+2",
  "createdAt": "2026-03-01T12:00:00Z",
  "updatedAt": "2026-03-01T12:00:00Z"
}
```

`400` for a rule violation, `409` if the name is taken.

## `GET /api/pools`

```json
{"pools":[{...}],"count":1}
```

The list is `[]` rather than `null` when empty, so a client can iterate it
without a null check. Pools are ordered newest first, which for ULIDs is the
identifier order.

## `GET /api/pools/{poolID}`

One pool, in the shape `POST` returns.

`400` if the identifier is not a valid ULID, `404` if no such pool exists.

## `DELETE /api/pools/{poolID}`

`204` on success, with no body.

Refuses with `409` while the pool still holds files. Deleting the pool would
cascade to its files and their chunk records, leaving shards that are still on
storage nodes with nothing pointing at them. Empty the pool first.

## `GET /api/pools/{poolID}/capacity`

```json
{
  "poolId": "01M3...",
  "nodes": 6,
  "requiredNodes": 6,
  "missingNodes": 0,
  "knownNodes": 6,
  "totalBytes": 600,
  "usedBytes": 210,
  "availableBytes": 390,
  "logicalCapacity": 40,
  "usable": true
}
```

- `requiredNodes` is K+M: the nodes needed to place one stripe.
- `missingNodes` is `requiredNodes - nodes`. Positive means the pool cannot
  accept an upload yet. Negative is fine; a pool may be over-provisioned.
- `knownNodes` counts nodes whose capacity has been reported. A node whose quota
  is not yet known is excluded from the byte figures rather than assumed empty.
- `logicalCapacity` is the capacity of the **emptiest** node, not the sum: a
  stripe puts a shard on every node, so the pool is only as large as the node
  with the least free space.


## Nodes

These routes exist only when a storage provider is configured. A client that
registers nodes against a service with no provider configured gets a 404, which
is the honest answer: the service genuinely cannot open a client for them.

### `POST /api/nodes`

Registers a storage node and probes it immediately.

```json
{
  "name": "laptop-drive",
  "provider": "google_drive"
}
```

```json
{
  "node": {
    "id": "01HQ000000000000000000001",
    "name": "laptop-drive",
    "provider": "google_drive",
    "status": "auth_error",
    "accountId": "",
    "quotaBytes": 0,
    "usedBytes": 0,
    "lastSeen": "0001-01-01T00:00:00Z",
    "lastError": "no stored credential: no token stored for this node"
  }
}
```

- A node that cannot be opened is still registered, and is reported with
  `201` and a `lastError`. Losing the record would leave an operator with a
  credential they cannot see and no way to attach it to anything.
- A validation failure is `400` and no node is created.
- `provider` is one of `google_drive` or `localfs`.

### `GET /api/nodes`

Lists every node with its capacity figures.

### `GET /api/nodes/{nodeID}`

One node.

### `GET /api/nodes/{nodeID}/status`

The health and capacity figures without the full record.

### `POST /api/nodes/check`

Probes every node, concurrently, bounded by the probe timeout.

```json
{
  "checked": 3,
  "healthy": 2,
  "error": "probe one of three nodes failed: ..."
}
```

A node that fails to answer is `502` for the whole call rather than a partial
`200`, because a health check that cannot tell you a node is down is worse than
one that fails.

## Pool membership

### `GET /api/pools/{poolID}/nodes`

Lists attached nodes, with the pool's capacity figures.

### `POST /api/pools/{poolID}/nodes`

```json
{ "nodeId": "01HQ000000000000000000001" }
```

- A node already attached is `409`.
- A node that does not exist is `404`.
- **A node may be attached while it is offline.** A recovery layout is a pool
  whose nodes are currently down, and refusing to record that makes the
  situation unrepresentable. The pool's `missingNodes` and `usable` figures
  report the consequence instead of pretending the pool is complete.

### `DELETE /api/pools/{poolID}/nodes/{nodeID}`

Detaches a node. Detaching is refused with `409` while the node still holds
chunks, because a chunk with no surviving copy is undetectable data loss. A
node that is not attached is `204`, so the call is idempotent.


## Provider authorisation

These routes exist only when a storage provider is configured.

### `POST /api/nodes/{nodeID}/authorize`

Issues a single-use state for a node and returns the provider's consent URL.

```json
{
  "nodeId": "01HQ000000000000000000001",
  "authorizationUrl": "https://accounts.google.com/o/oauth2/v2/auth?...",
  "expiresInSeconds": 600
}
```

The URL is returned in a body rather than as a redirect, so this route cannot
become an open redirect and the same endpoint works from a script.

### `GET /api/oauth/callback?code=...&state=...`

Where the provider sends the browser back. This is the only route reachable
without authenticating to this service, so nothing in the query string is trusted
on its own:

- The `state` is HMAC-signed with the token key and carries the node id, so a
  callback cannot be aimed at a node it was not issued for, and cannot be
  minted by a client.
- The state is accepted **once** and then discarded, whether or not the provider
  accepted the code. A replayed callback never redeems a second credential.
- The state **expires** after ten minutes, so a link left in browser history is
  not a standing credential.

| Outcome | Status |
| --- | --- |
| Credential stored | `200` |
| No `code` or no `state` | `400` |
| Provider refused (`?error=`) | `400` |
| State unknown, used, or expired | `410` |
| State belongs to a different node | `403` |
| Provider or storage failure | `500` |

`410` is deliberate. A spent or expired link is not a malformed request; a client
that retries it unchanged will keep failing, and the fix is to start a new flow.
