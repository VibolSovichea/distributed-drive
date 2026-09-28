-- Migration 0003: Add encryption support to pools and files.
--
-- Adds an 'encrypted' boolean column to both pools and files tables.
-- When a pool is created with encryption enabled, all files in that pool
-- are encrypted. The file's encrypted flag is copied from the pool at
-- creation time so that changing the pool's encryption setting doesn't
-- affect existing files.

ALTER TABLE pools ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0 CHECK (encrypted IN (0, 1));

ALTER TABLE files ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0 CHECK (encrypted IN (0, 1));

-- Index for finding encrypted files (useful for key rotation operations).
CREATE INDEX IF NOT EXISTS idx_files_encrypted ON files (encrypted) WHERE encrypted = 1;