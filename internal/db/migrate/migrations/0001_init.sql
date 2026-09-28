-- Initial metadata schema.
--
-- Timestamps are INTEGER unix nanoseconds. They are converted in Go rather
-- than by the driver, so the storage format does not depend on driver
-- configuration and sorts correctly in SQL.
--
-- Sizes are bytes. Capacities and file sizes are 0 when unknown, which is a
-- normal state: the Google Drive API does not report a quota for personal
-- accounts, so capacity has to be supplied out of band.

CREATE TABLE pools (
    id            TEXT    PRIMARY KEY,
    name          TEXT    NOT NULL,
    data_chunks   INTEGER NOT NULL CHECK (data_chunks > 0),
    parity_chunks INTEGER NOT NULL CHECK (parity_chunks >= 0),
    chunk_size    INTEGER NOT NULL CHECK (chunk_size > 0),
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

-- Pool names are user-facing identifiers, so they must be unique.
CREATE UNIQUE INDEX pools_name_unique ON pools (name);

CREATE TABLE nodes (
    id                 TEXT    PRIMARY KEY,
    name               TEXT    NOT NULL,
    provider           TEXT    NOT NULL,
    account_identifier TEXT    NOT NULL DEFAULT '',
    status             TEXT    NOT NULL,
    capacity           INTEGER NOT NULL DEFAULT 0 CHECK (capacity >= 0),
    used_capacity      INTEGER NOT NULL DEFAULT 0 CHECK (used_capacity >= 0),
    last_seen          INTEGER,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    CHECK (status IN ('healthy', 'degraded', 'offline', 'full', 'auth_error'))
);

-- The same account must not be registered twice, or placement could pick it
-- for two shards of one stripe and silently halve the redundancy.
CREATE UNIQUE INDEX nodes_name_unique ON nodes (name);
CREATE UNIQUE INDEX nodes_account_unique ON nodes (provider, account_identifier);

-- Phase 3 adds and removes nodes from a pool, which the nodes table alone
-- cannot express because one node may serve more than one pool.
CREATE TABLE pool_nodes (
    pool_id  TEXT    NOT NULL REFERENCES pools (id) ON DELETE CASCADE,
    node_id  TEXT    NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    added_at INTEGER NOT NULL,
    PRIMARY KEY (pool_id, node_id)
);

CREATE INDEX pool_nodes_node_idx ON pool_nodes (node_id);

CREATE TABLE files (
    id            TEXT    PRIMARY KEY,
    pool_id       TEXT    NOT NULL REFERENCES pools (id) ON DELETE CASCADE,
    name          TEXT    NOT NULL,
    size          INTEGER NOT NULL CHECK (size >= 0),
    -- Empty until the upload pipeline has hashed the whole stream. The CHECK
    -- below makes it impossible to mark a file committed without integrity
    -- metadata, which is the storage engine's core invariant.
    content_hash  TEXT    NOT NULL DEFAULT '',
    chunk_size    INTEGER NOT NULL CHECK (chunk_size > 0),
    data_chunks   INTEGER NOT NULL CHECK (data_chunks > 0),
    parity_chunks INTEGER NOT NULL CHECK (parity_chunks >= 0),
    status        TEXT    NOT NULL DEFAULT 'pending',
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    CHECK (status IN ('pending', 'uploading', 'committed', 'degraded', 'deleting')),
    CHECK (status <> 'committed' OR content_hash <> '')
);

CREATE INDEX files_pool_created_idx ON files (pool_id, created_at DESC);

CREATE TABLE chunks (
    id          TEXT    PRIMARY KEY,
    file_id     TEXT    NOT NULL REFERENCES files (id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    -- NO ACTION, not CASCADE: forgetting a node that still holds chunks would
    -- leave the file silently short of shards. The node must be emptied first.
    node_id     TEXT    NOT NULL REFERENCES nodes (id) ON DELETE NO ACTION,
    remote_file_id TEXT NOT NULL,
    -- True byte length, not the zero-padded length required by the erasure
    -- coder. The final data chunk of a file is short, and its length is needed
    -- to truncate correctly when reassembling the original.
    size           INTEGER NOT NULL CHECK (size >= 0),
    hash           TEXT    NOT NULL,
    chunk_type     TEXT    NOT NULL,
    created_at     INTEGER NOT NULL,
    CHECK (chunk_type IN ('data', 'parity')),
    -- One placement per index. This is what turns a double-placement bug into
    -- a storage-layer error instead of silent redundancy loss.
    UNIQUE (file_id, chunk_index)
);

CREATE INDEX chunks_node_idx ON chunks (node_id);
