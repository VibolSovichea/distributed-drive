# Fault-Tolerant Distributed Google Drive Storage

## AI Agent Development Plan

> **Purpose:** This document is the execution plan for an AI coding
> agent helping build the project. The agent should implement the system
> incrementally, preserve the architecture, test each layer before
> moving upward, and avoid making large architectural changes without
> explicit approval.

------------------------------------------------------------------------

## 1. Project Objective

Build a storage service that combines multiple Google Drive accounts
into one logical storage pool.

The system should:

-   Connect multiple Google Drive accounts through OAuth.
-   Treat each account as a storage node.
-   Accept files through a separate web interface.
-   Stream and process uploads efficiently.
-   Split files into chunks.
-   Apply erasure coding to create data and parity chunks.
-   Distribute encoded chunks across Google Drive nodes.
-   Maintain metadata describing files, chunks, nodes, hashes, and
    encoding configuration.
-   Detect unavailable or unhealthy nodes.
-   Download files even when some chunks/nodes are unavailable.
-   Reconstruct missing chunks using erasure coding.
-   Restore redundancy through self-healing.
-   Verify reconstructed files using SHA-256.
-   Expose the storage functionality through a REST API.
-   Keep the web interface completely separate from the storage engine.

The initial implementation is a V1 engineering prototype, not a complete
distributed filesystem.

------------------------------------------------------------------------

# 2. Target Architecture

``` text
                         ┌─────────────────────┐
                         │        User         │
                         └──────────┬──────────┘
                                    │
                                  HTTPS
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Web Interface     │
                         │    Next.js / React  │
                         └──────────┬──────────┘
                                    │
                              REST API / HTTPS
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Go Storage Service                          │
│                                                                     │
│  ┌──────────────┐      ┌──────────────────┐                       │
│  │  API Layer   │─────▶│ Storage Pool     │                       │
│  └──────────────┘      │ Manager          │                       │
│                        └────────┬─────────┘                       │
│                                 │                                  │
│       ┌─────────────────────────┼─────────────────────────┐        │
│       ▼                         ▼                         ▼        │
│ ┌──────────────┐      ┌──────────────────┐      ┌──────────────┐ │
│ │ Node Manager │      │ Transfer /       │      │ Metadata     │ │
│ │              │      │ Orchestrator     │      │ Manager      │ │
│ └──────┬───────┘      └────────┬─────────┘      └──────┬───────┘ │
│        │                        │                       │         │
│        │              ┌─────────┼─────────┐             │         │
│        │              ▼         ▼         ▼             │         │
│        │        Chunking    Erasure   Placement         │         │
│        │        Engine      Coding    Engine            │         │
│        │                                                  │         │
│        │        ┌──────────────┐  ┌──────────────┐        │         │
│        └───────▶│ Health /     │  │ Recovery /   │◀───────┘         │
│                 │ Failure      │  │ Self-Healing │                  │
│                 └──────────────┘  └──────────────┘                  │
│                                                                     │
│                 Integrity / Encryption                              │
└───────────────────────┬─────────────────────────────┬───────────────┘
                        │                             │
                        ▼                             ▼
                 ┌─────────────┐              ┌─────────────┐
                 │ SQLite V1   │              │ Google Drive│
                 │ Metadata    │              │ API         │
                 └─────────────┘              └──────┬──────┘
                                                      │
                         ┌────────────────────────────┼───────────────┐
                         ▼                            ▼               ▼
                    Drive A                       Drive B          Drive C...
                    Storage                       Storage          Storage
```

------------------------------------------------------------------------

# 3. Repository Structure

Use a repository structure that keeps the backend and frontend
independent.

``` text
distributed-drive/
│
├── backend/
│   ├── cmd/
│   │   └── server/
│   │       └── main.go
│   │
│   ├── internal/
│   │   ├── api/
│   │   ├── pool/
│   │   ├── node/
│   │   ├── transfer/
│   │   ├── chunk/
│   │   ├── erasure/
│   │   ├── placement/
│   │   ├── recovery/
│   │   ├── health/
│   │   ├── integrity/
│   │   ├── encryption/
│   │   └── metadata/
│   │
│   ├── migrations/
│   ├── tests/
│   ├── go.mod
│   └── go.sum
│
├── frontend/
│   ├── app/
│   ├── components/
│   ├── features/
│   ├── lib/
│   ├── hooks/
│   └── package.json
│
├── docs/
│
├── docker-compose.yml
├── README.md
└── .gitignore
```

The backend is the actual storage engine. The frontend is a client of
the backend and must not contain storage logic.

------------------------------------------------------------------------

# 4. Technology Decisions

## Backend

-   Go
-   REST API
-   SQLite for V1 metadata, embedded in the container via a pure-Go driver so
    nothing is installed on the host.
-   Google Drive API
-   OAuth 2.0
-   Existing Reed-Solomon / erasure-coding library
-   Go `context` for cancellation and request lifetime
-   Streaming `io.Reader` / `io.Writer`
-   Goroutines for controlled concurrency
-   Structured logging
-   Unit and integration tests

## Frontend

-   Next.js
-   React
-   TypeScript
-   REST API client
-   Separate deployment/application

## Later

Potential future replacements/additions:

-   PostgreSQL
-   Additional cloud providers
-   Background workers
-   Distributed metadata
-   Desktop client
-   Virtual filesystem
-   More advanced monitoring

Do not introduce these into V1 unless explicitly requested.

------------------------------------------------------------------------

# 5. Development Rules for the AI Agent

The agent must follow these rules throughout development.

## Rule 1 --- Preserve the Architecture

Do not collapse the system into one large service or one large file.

Each major responsibility should have a clear module.

## Rule 2 --- Implement Incrementally

Complete one subsystem, test it, then move to the next subsystem.

Do not implement the entire application in one pass.

## Rule 3 --- Backend First

Build and verify the Go storage service before building the complete web
interface.

## Rule 4 --- Interface Separation

The frontend communicates through HTTP APIs.

The frontend must never:

-   Talk directly to Google Drive.
-   Perform erasure coding.
-   Determine chunk placement.
-   Reconstruct files.
-   Manage Google OAuth tokens for storage nodes.
-   Access the storage database.

## Rule 5 --- Use Existing Libraries

Do not implement Reed-Solomon mathematics from scratch.

Use an established erasure-coding library and wrap it behind an internal
interface.

## Rule 6 --- Stream Large Files

Do not load an entire large file into memory.

Design upload and download paths around streaming and bounded buffers.

## Rule 7 --- Bounded Concurrency

Do not spawn unlimited goroutines.

Use controlled concurrency for:

-   Google Drive uploads.
-   Google Drive downloads.
-   Chunk processing.
-   Recovery operations.

## Rule 8 --- Test Before Integration

Every major component should have unit tests before being integrated
into the complete pipeline.

## Rule 9 --- Keep Interfaces Explicit

Prefer interfaces such as:

``` go
type StorageNode interface {
    Upload(ctx context.Context, r io.Reader, metadata ObjectMetadata) (RemoteObject, error)
    Download(ctx context.Context, id string) (io.ReadCloser, error)
    Delete(ctx context.Context, id string) error
    Stat(ctx context.Context, id string) (RemoteObject, error)
}
```

The exact interface may evolve, but responsibilities must remain
separated.

## Rule 10 --- Do Not Overengineer V1

Do not introduce:

-   Kubernetes
-   Microservices
-   Kafka
-   Redis
-   distributed databases
-   custom distributed filesystems
-   multi-cloud abstraction

unless a later requirement explicitly requires them.

------------------------------------------------------------------------

# 6. Phase 0 --- Project Foundation

## Objective

Create a working development environment and repository skeleton.

## Tasks

-   Create repository.
-   Initialize Go module.
-   Initialize Next.js application.
-   Establish backend/frontend directories.
-   Add environment configuration.
-   Add `.gitignore`.
-   Add README.
-   Add basic Docker configuration if useful.
-   Create `/health` backend endpoint.
-   Verify frontend can call backend.

## Expected result

``` text
Browser
   │
   ▼
Next.js
   │
   ▼
GET /health
   │
   ▼
Go API
   │
   ▼
200 OK
```

## Agent verification

The agent must run:

``` bash
go test ./...
go vet ./...
go build ./...
```

and the frontend's configured lint/type checks.

Do not proceed if the foundation is broken.

------------------------------------------------------------------------

# 7. Phase 1 --- Metadata Layer

## Objective

Build the database before implementing storage behavior.

## Tables

### pools

``` text
id
name
data_chunks
parity_chunks
chunk_size
created_at
updated_at
```

### nodes

``` text
id
name
provider
account_identifier
status
capacity
used_capacity
last_seen
created_at
updated_at
```

### files

``` text
id
pool_id
name
size
content_hash
chunk_size
data_chunks
parity_chunks
created_at
updated_at
```

### chunks

``` text
id
file_id
chunk_index
node_id
remote_file_id
size
hash
chunk_type
created_at
```

## Tasks

-   Create database connection.
-   Create migrations.
-   Create models.
-   Create repositories.
-   Implement CRUD operations.
-   Add tests.

## Deliverable

The backend can create a pool and persist node/file/chunk metadata.

------------------------------------------------------------------------

# 8. Phase 2 --- Storage Node Abstraction

## Objective

Create a provider-independent abstraction around storage nodes.

Start with Google Drive as the only provider.

``` text
StorageNode
      │
      └── GoogleDriveNode
```

## Tasks

-   Define storage node interface.
-   Implement Google Drive client.
-   Implement OAuth flow.
-   Securely store OAuth credentials/tokens.
-   Register a Drive as a node.
-   Upload object.
-   Download object.
-   Delete object.
-   Get object metadata.
-   Determine available capacity where possible.
-   Add node status tracking.

## Deliverable

The backend can connect to multiple Google Drive accounts and treat them
through one internal interface.

------------------------------------------------------------------------

# 9. Phase 3 --- Storage Pool Manager

## Objective

Build the logical storage pool.

## Tasks

-   Create pool.
-   Add node to pool.
-   Remove node.
-   List nodes.
-   Get node status.
-   Calculate pool capacity.
-   Store K/M configuration.
-   Validate that enough nodes exist for the selected redundancy
    configuration.

## Example

``` text
Pool
├── Drive A
├── Drive B
├── Drive C
├── Drive D
├── Drive E
└── Drive F
```

## Deliverable

The backend can manage a logical pool independently of Google Drive
implementation details.

------------------------------------------------------------------------

# 10. Phase 4 --- Chunking Engine

## Objective

Implement deterministic file chunking and reconstruction.

## Requirements

The chunking engine must support:

``` text
File
  ↓
C1
C2
C3
C4
```

and:

``` text
C1 C2 C3 C4
       ↓
Original file
```

## Tasks

-   Define chunk size.
-   Stream input.
-   Generate chunks without loading the whole file into memory.
-   Assign stable chunk indexes.
-   Calculate chunk hashes.
-   Reassemble chunks in correct order.
-   Handle partial final chunks.
-   Add tests for:
    -   empty file
    -   tiny file
    -   exact chunk boundary
    -   partial final chunk
    -   large file
    -   corrupted chunk

## Deliverable

A standalone tested chunking package.

------------------------------------------------------------------------

# 11. Phase 5 --- Erasure Coding Engine

## Objective

Add redundancy.

Example:

``` text
Data:

C1 C2 C3 C4

        ↓

Erasure Coding

C1 C2 C3 C4 P1 P2
```

With:

``` text
K = 4
M = 2
```

the system should recover the original data when any two encoded chunks
are unavailable.

## Tasks

-   Select established erasure-coding library.
-   Wrap the library behind an internal interface.
-   Encode chunks.
-   Validate shard count.
-   Detect missing shards.
-   Reconstruct missing shards.
-   Verify reconstructed shards.
-   Add tests.

## Required tests

``` text
0 missing → success
1 missing → success
2 missing → success
3 missing → failure
```

Also test combinations of missing data and parity shards.

## Deliverable

A standalone erasure-coding package with deterministic tests.

------------------------------------------------------------------------

# 12. Phase 6 --- Placement Engine

## Objective

Determine where encoded chunks should be stored.

Example:

``` text
C1 → Drive A
C2 → Drive B
C3 → Drive C
C4 → Drive D
P1 → Drive E
P2 → Drive F
```

## Requirements

The placement engine should:

-   Avoid placing all shards on one node.
-   Prefer healthy nodes.
-   Avoid full nodes.
-   Respect node availability.
-   Consider capacity.
-   Produce a deterministic placement plan where practical.

## Important

The placement engine decides **where** chunks go.

It should not perform the actual Google Drive upload.

## Deliverable

A placement plan such as:

``` go
PlacementPlan{
    ChunkIndex: 0,
    NodeID: "drive-a",
}
```

------------------------------------------------------------------------

# 13. Phase 7 --- Upload Pipeline

## Objective

Connect the components into the complete upload flow.

``` text
Client
  │
  ▼
HTTP Upload
  │
  ▼
Upload Orchestrator
  │
  ├── Hash
  ├── Chunk
  ├── Encode
  ├── Placement
  └── Parallel Upload
          │
          ├── Drive A
          ├── Drive B
          ├── Drive C
          ├── Drive D
          ├── Drive E
          └── Drive F
```

## Requirements

-   Stream request body.
-   Avoid loading entire file into RAM.
-   Generate chunks.
-   Encode chunks.
-   Create placement plan.
-   Upload chunks concurrently with bounded concurrency.
-   Persist metadata.
-   Verify uploaded chunks.
-   Handle partial failures.
-   Support cancellation.
-   Clean up incomplete uploads where appropriate.

## Important transaction principle

Do not mark a file as fully committed until the required metadata and
encoded chunks are successfully persisted.

## Deliverable

A file can be uploaded through the API and stored across multiple Google
Drive accounts.

------------------------------------------------------------------------

# 14. Phase 8 --- Download Pipeline

## Objective

Implement normal and degraded downloads.

Normal case:

``` text
Metadata
   ↓
Find chunks
   ↓
Download chunks concurrently
   ↓
Reassemble
   ↓
Hash verification
   ↓
HTTP stream
```

Failure case:

``` text
Metadata
   ↓
Find missing chunk
   ↓
Download surviving shards
   ↓
Erasure reconstruction
   ↓
Reassemble
   ↓
Hash verification
   ↓
HTTP stream
```

## Requirements

-   Stream response to client.
-   Download required chunks concurrently.
-   Detect missing chunks.
-   Reconstruct missing shards.
-   Verify hashes.
-   Verify final SHA-256.
-   Return appropriate errors when recovery is impossible.
-   Never silently return corrupted data.

## Deliverable

A user can download a file even when supported storage nodes are
unavailable.

------------------------------------------------------------------------

# 15. Phase 9 --- Failure Detection

## Objective

Detect unhealthy storage nodes.

## Node states

``` text
HEALTHY
DEGRADED
OFFLINE
FULL
AUTH_ERROR
```

## Tasks

-   Implement health checks.
-   Record `last_seen`.
-   Detect API errors.
-   Detect authentication failures.
-   Detect network failures.
-   Detect capacity exhaustion.
-   Update node status.
-   Expose status through API.

## Deliverable

The system can identify unavailable nodes without requiring manual
status changes.

------------------------------------------------------------------------

# 16. Phase 10 --- Recovery / Self-Healing

## Objective

Restore redundancy after node failure or replacement.

Example:

``` text
Before:

C1 C2 C3 C4 P1 P2
      X
    missing

        ↓

Reconstruct C3

        ↓

Upload C3 to replacement node

        ↓

C1 C2 C3 C4 P1 P2
```

## Tasks

-   Detect degraded files.
-   Determine missing shards.
-   Determine whether reconstruction is possible.
-   Download surviving shards.
-   Reconstruct missing shards.
-   Select destination node.
-   Upload reconstructed shards.
-   Update metadata.
-   Mark file redundancy as restored.

## Important

Recovery must be resumable where practical.

A failed recovery should not corrupt existing healthy shards.

------------------------------------------------------------------------

# 17. Phase 11 --- Integrity

## Objective

Make corruption detectable.

Maintain hashes at multiple levels:

``` text
File hash
Chunk hash
Encoded shard hash
```

## Tasks

-   Calculate SHA-256.
-   Store file hash.
-   Store chunk/shard hashes.
-   Validate downloaded chunks.
-   Validate reconstructed chunks.
-   Validate final file.

## Required invariant

``` text
SHA256(original)
        ==
SHA256(downloaded)
```

If not:

``` text
Download must fail.
```

Do not silently return corrupted data.

------------------------------------------------------------------------

# 18. Phase 12 --- Encryption

## Objective

Add optional client-side encryption.

Pipeline:

``` text
Original
   ↓
Encryption
   ↓
Chunking
   ↓
Erasure Coding
   ↓
Distribution
   ↓
Google Drive
```

## Tasks

-   Define encryption model.
-   Never store plaintext file data on Google Drive when encryption is
    enabled.
-   Define key management clearly.
-   Encrypt/decrypt through streaming APIs where possible.
-   Add tests.

## V1 note

Encryption can remain optional if it threatens the timeline, but the
architecture should not prevent it.

------------------------------------------------------------------------

# 19. Phase 13 --- REST API

The API should expose the storage functionality required by the web
application.

## Pool

``` text
POST   /api/pools
GET    /api/pools
GET    /api/pools/:id
```

## Nodes

``` text
POST   /api/pools/:id/nodes
GET    /api/pools/:id/nodes
DELETE /api/pools/:id/nodes/:nodeId
GET    /api/nodes/:id/status
```

## Files

``` text
POST   /api/pools/:id/files
GET    /api/pools/:id/files
GET    /api/pools/:id/files/:fileId
GET    /api/pools/:id/files/:fileId/download
DELETE /api/pools/:id/files/:fileId
```

## Recovery

``` text
GET  /api/pools/:id/health
GET  /api/pools/:id/recovery
POST /api/pools/:id/recovery/:fileId
```

Exact routes can change during implementation, but the API must remain a
clean boundary between frontend and storage service.

------------------------------------------------------------------------

# 20. Phase 14 --- Web Interface

Only after the backend pipeline works should the agent build the
complete UI.

## Screens

### Dashboard

Show:

-   Pool capacity.
-   Used capacity.
-   Available capacity.
-   Number of nodes.
-   Healthy nodes.
-   Offline nodes.
-   Degraded files.
-   Recovery activity.

### Storage Nodes

Show:

``` text
Drive A   Healthy   15 GB   8 GB
Drive B   Healthy   15 GB   11 GB
Drive C   Offline   15 GB   9 GB
```

### Files

Show:

-   Name
-   Size
-   Status
-   Uploaded time
-   Integrity status
-   Redundancy status

### Upload

Show:

``` text
Uploading...

██████████████░░░░ 70%

Uploaded: 700 MB / 1 GB
Speed: 85 MB/s
```

### Recovery

Show:

``` text
Recovering file...

████████████░░░░░ 72%

Missing shards: 2
Recovered: 1
```

------------------------------------------------------------------------

# 21. Phase 15 --- Performance Engineering

Performance must be measured rather than assumed.

## Measure

### Upload

``` text
Client → Go
Go → Google Drive
End-to-end
```

### Download

``` text
Google Drive → Go
Go → Client
End-to-end
```

## Metrics

Track:

-   Total throughput.
-   Per-node throughput.
-   Upload latency.
-   Download latency.
-   Encoding time.
-   Chunking time.
-   Reconstruction time.
-   CPU usage.
-   Memory usage.
-   Number of concurrent transfers.
-   Retry count.

## Optimization strategy

Start with correctness.

Then optimize:

1.  Streaming.
2.  Parallel Drive transfers.
3.  Connection reuse.
4.  Bounded concurrency.
5.  Buffer sizing.
6.  Chunk size.
7.  Retry behavior.
8.  Backpressure.
9.  Encoding performance.

Do not optimize based on assumptions before benchmarking.

------------------------------------------------------------------------

# 22. Phase 16 --- Testing

Testing must cover the system from isolated units to complete workflows.

## Unit Tests

Test:

-   Chunking.
-   Erasure coding.
-   Placement.
-   Hashing.
-   Metadata.
-   Node states.
-   Recovery decisions.

## Integration Tests

Test:

-   SQLite.
-   Google Drive abstraction.
-   Upload pipeline.
-   Download pipeline.
-   Recovery pipeline.

## End-to-End Tests

### Test 1

``` text
Upload
→ Download
→ SHA256 MATCH
```

### Test 2

``` text
Upload
→ Disable one node
→ Download
→ SHA256 MATCH
```

### Test 3

``` text
Upload
→ Disable two nodes
→ Download
→ SHA256 MATCH
```

### Test 4

``` text
Upload
→ Disable too many nodes
→ Download
→ Correct failure
```

### Test 5

``` text
Upload
→ Node failure
→ Replacement node
→ Recovery
→ Verify redundancy
```

### Test 6

``` text
Upload multiple files
→ Fail node
→ Download multiple files
→ Verify every hash
```

------------------------------------------------------------------------

# 23. Phase 17 --- Failure Simulation

The system needs a way to simulate failures without actually
disconnecting real accounts.

The agent should provide a development/test mechanism such as:

``` bash
pool simulate-failure drive-c
```

The node manager should then treat that node as unavailable.

This allows testing:

``` text
Healthy
   ↓
Drive C OFFLINE
   ↓
Download still works
   ↓
Drive C restored
   ↓
Self-healing
   ↓
Healthy
```

------------------------------------------------------------------------

# 24. Phase 18 --- Deployment

Once the system passes the end-to-end test suite:

## Backend

-   Build Go binary.
-   Create Docker image.
-   Configure environment variables.
-   Configure OAuth credentials securely.
-   Configure database.
-   Configure logging.

## Frontend

-   Build Next.js application.
-   Configure backend API URL.
-   Configure authentication.
-   Deploy independently.

## Production requirements

-   HTTPS.
-   Secure secrets.
-   OAuth token protection.
-   Database backups.
-   Metadata backup.
-   Structured logs.
-   Health endpoints.
-   Error monitoring.

------------------------------------------------------------------------

# 25. AI Agent Execution Protocol

The AI agent should work in small, verifiable tasks.

For every task:

``` text
1. Read relevant existing code.
2. Understand current architecture.
3. Identify the smallest implementation.
4. Implement.
5. Add/update tests.
6. Run tests.
7. Run formatter/linter.
8. Review the diff.
9. Update documentation if behavior changed.
10. Report what changed.
```

The agent must not jump directly to the next phase if the current phase
has failing tests.

------------------------------------------------------------------------

# 26. Prompt Pattern for the AI Agent

Use a task-oriented prompt rather than asking the agent to "build the
whole application."

Example:

``` text
We are building the Fault-Tolerant Distributed Google Drive Storage
project.

Read:
- docs/architecture.md
- docs/development-plan.md
- relevant existing source code

Current phase:
Phase 4 — Chunking Engine

Task:
Implement the chunking package only.

Requirements:
- Stream input using io.Reader.
- Configurable chunk size.
- Stable chunk indexes.
- SHA-256 hash per chunk.
- Do not load the entire file into memory.
- Support partial final chunks.
- Add unit tests.

Constraints:
- Do not modify unrelated packages.
- Do not introduce new architecture.
- Do not implement erasure coding yet.
- Follow existing Go conventions.

Before finishing:
- Run gofmt.
- Run go test ./...
- Run go vet ./...
- Review the git diff.

Return:
1. What was implemented.
2. Files changed.
3. Tests added.
4. Test results.
5. Any architectural decisions that require approval.
```

------------------------------------------------------------------------

# 27. Agent Task Order

The agent should execute tasks in this order:

``` text
01. Repository setup
        ↓
02. Go API skeleton
        ↓
03. SQLite metadata
        ↓
04. Storage node abstraction
        ↓
05. Google Drive OAuth
        ↓
06. Google Drive node implementation
        ↓
07. Storage pool manager
        ↓
08. Chunking engine
        ↓
09. Erasure coding engine
        ↓
10. Placement engine
        ↓
11. Upload pipeline
        ↓
12. Download pipeline
        ↓
13. Integrity verification
        ↓
14. Failure detection
        ↓
15. Recovery / self-healing
        ↓
16. Encryption
        ↓
17. REST API completion
        ↓
18. Web interface
        ↓
19. Performance benchmarks
        ↓
20. End-to-end tests
        ↓
21. Deployment
```

------------------------------------------------------------------------

# 28. Definition of Done

The V1 prototype is complete when the following works:

``` text
                    ┌──────────────┐
                    │   Browser    │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Next.js Web  │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Go Storage   │
                    │   Service    │
                    └──────┬───────┘
                           │
                  ┌────────┼────────┐
                  ▼        ▼        ▼
              Drive A   Drive B   Drive C...
```

The user can:

1.  Create a storage pool.
2.  Connect 3--4 Google Drive accounts.
3.  Configure `K` and `M`.
4.  Upload a file.
5.  See the file in the web interface.
6.  Confirm chunks were distributed across nodes.
7.  Disable a supported number of nodes.
8.  Download the file successfully.
9.  Verify SHA-256 matches.
10. Restore or replace a node.
11. Recover missing chunks.
12. Restore redundancy.
13. Repeat the process with multiple files.

------------------------------------------------------------------------

# 29. What the AI Agent Must Not Do

Do not allow the agent to:

-   Rewrite the architecture without approval.
-   Mix frontend and storage responsibilities.
-   Put Google Drive logic inside Next.js.
-   Implement erasure-coding mathematics from scratch.
-   Load entire large files into memory.
-   Create unbounded goroutines.
-   Ignore failed uploads.
-   Ignore integrity verification.
-   Silently return corrupted files.
-   Add unnecessary infrastructure.
-   Replace SQLite with PostgreSQL during V1 without a requirement. The layering
    keeps this a driver swap: only `internal/metadata/sqlite` speaks SQL.
-   Add additional cloud providers before Google Drive is stable.
-   Build the UI before the core storage pipeline is tested.
-   Mark a task complete without running tests.

------------------------------------------------------------------------

# 30. Final Engineering Principle

The complexity of the distributed storage system should remain inside
the Go storage service.

The web application should see a simple abstraction:

``` text
Create Pool
Add Drive
Upload File
List Files
Download File
Monitor Health
Recover Data
```

Internally, the system performs:

``` text
Upload

Client
  ↓
Stream
  ↓
Chunk
  ↓
Encode
  ↓
Place
  ↓
Parallel Upload
  ↓
Persist Metadata


Download

Metadata
  ↓
Locate Shards
  ↓
Parallel Download
  ↓
Validate
  ↓
Reconstruct Missing Shards
  ↓
Reassemble
  ↓
Verify SHA-256
  ↓
Stream to Client


Recovery

Detect Failure
  ↓
Mark Node Degraded
  ↓
Identify Missing Shards
  ↓
Read Surviving Shards
  ↓
Reconstruct
  ↓
Write to Replacement Node
  ↓
Update Metadata
  ↓
Restore Redundancy
```

**The AI agent's primary job is to implement this architecture one
verified layer at a time---not to invent a new architecture while
coding.**
