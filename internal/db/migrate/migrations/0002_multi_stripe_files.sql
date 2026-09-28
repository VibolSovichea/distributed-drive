-- Allow a file to span more than one erasure-coded stripe.
--
-- The original schema held exactly one stripe per file, because UNIQUE
-- (file_id, chunk_index) with no stripe in the key meant chunk_index counted
-- across the whole file and a file could therefore hold at most K+M shards.
--
-- That has a hard consequence. With a fixed chunk size, the largest file a pool
-- can accept is K * chunk_size, so a 1 MiB chunk size and K=4 caps uploads at
-- 4 MiB. Raising chunk_size to lift the cap makes the encoder's working set
-- (K+M) * chunk_size grow with every file, so the cap just moves to wherever
-- the host runs out of memory.
--
-- Keying on (file_id, stripe_index, chunk_index) removes the ceiling: a file is
-- now ceil(size / (K * chunk_size)) stripes, each encoded and uploaded on its
-- own, and the size of a file is limited by disk rather than by arithmetic.

-- SQLite materialised UNIQUE (file_id, chunk_index) as an implicit
-- sqlite_autoindex, which cannot be dropped. Changing the key therefore means
-- rebuilding the table rather than adding a column to it, so this migration
-- follows the documented twelve-step procedure: create, copy, drop, rename.
--
-- The foreign keys and CHECKs are copied verbatim. A rebuild that quietly
-- relaxed a constraint would be worse than no migration at all, because the
-- invariant it was protecting would be lost without anyone noticing.
CREATE TABLE chunks_rebuilt (
    id          TEXT    PRIMARY KEY,
    file_id     TEXT    NOT NULL REFERENCES files (id) ON DELETE CASCADE,
    -- Which stripe of the file this shard belongs to, counting from zero. Two
    -- shards of the same file at the same index but different stripes are
    -- different shards, which is the entire point of the key.
    stripe_index INTEGER NOT NULL DEFAULT 0 CHECK (stripe_index >= 0),
    -- Index within the stripe: 0..K-1 for data, K..K+M-1 for parity.
    chunk_index  INTEGER NOT NULL CHECK (chunk_index >= 0),
    -- NO ACTION, not CASCADE: forgetting a node that still holds chunks would
    -- leave the file silently short of shards. The node must be emptied first.
    node_id     TEXT    NOT NULL REFERENCES nodes (id) ON DELETE NO ACTION,
    remote_file_id TEXT NOT NULL,
    -- True byte length, not the zero-padded length required by the erasure
    -- coder. The final data shard of the final stripe is short, and its length
    -- is needed to truncate correctly when reassembling the original.
    size           INTEGER NOT NULL CHECK (size >= 0),
    hash           TEXT    NOT NULL,
    chunk_type     TEXT    NOT NULL,
    created_at     INTEGER NOT NULL,
    CHECK (chunk_type IN ('data', 'parity')),
    -- One placement per (stripe, index). This is what turns a double-placement
    -- bug into a storage-layer error instead of silent redundancy loss, and it
    -- is also what guarantees that a stripe's K+M shards are K+M distinct
    -- records, so a missing shard is detectable by counting.
    UNIQUE (file_id, stripe_index, chunk_index)
);

-- Existing rows are all single-stripe, since that is the only thing the old
-- schema could express, so every one of them belongs to stripe zero.
INSERT INTO chunks_rebuilt (
    id, file_id, stripe_index, chunk_index, node_id, remote_file_id,
    size, hash, chunk_type, created_at
)
SELECT
    id, file_id, 0, chunk_index, node_id, remote_file_id,
    size, hash, chunk_type, created_at
FROM chunks;

DROP TABLE chunks;
ALTER TABLE chunks_rebuilt RENAME TO chunks;

-- Renamed from the same table's name, so it has to be recreated: the drop above
-- took the index with it.
CREATE INDEX chunks_node_idx ON chunks (node_id);

-- Reading a file means reading its shards in order, which is now a sort by
-- stripe and then by index. Recovery counts the shards of one stripe, and the
-- stripe-first order serves that too.
CREATE INDEX chunks_file_stripe_idx ON chunks (file_id, stripe_index, chunk_index);
