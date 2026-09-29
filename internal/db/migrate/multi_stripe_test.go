package migrate

import (
	"database/sql"
	"strconv"
	"testing"
)

type seed struct {
	poolID string
	fileID string
	nodeA  string
	nodeB  string
}

func seedFileAndNodes(t *testing.T, db *sql.DB) seed {
	t.Helper()

	ctx := t.Context()
	now := 1700000000

	if _, err := db.ExecContext(ctx,
		`INSERT INTO pools (id, name, data_chunks, parity_chunks, chunk_size, created_at, updated_at)
		 VALUES ('pool-1', 'pool one', 4, 2, 1048576, ?, ?)`,
		now, now); err != nil {
		t.Fatalf("insert pool: %v", err)
	}

	for _, n := range []struct{ id, name string }{
		{"node-a", "node a"},
		{"node-b", "node b"},
	} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO nodes (id, name, provider, account_identifier, status,
				capacity, used_capacity, created_at, updated_at)
			 VALUES (?, ?, 'localfs', ?, 'healthy', 0, 0, ?, ?)`,
			n.id, n.name, n.id+"-account", now, now); err != nil {
			t.Fatalf("insert node %s: %v", n.id, err)
		}
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO files (id, pool_id, name, size, content_hash, chunk_size,
			data_chunks, parity_chunks, status, created_at, updated_at)
		 VALUES ('file-1', 'pool-1', 'one.bin', 0, '', 1048576, 4, 2, 'pending', ?, ?)`,
		now, now); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	return seed{
		poolID: "pool-1",
		fileID: "file-1",
		nodeA:  "node-a",
		nodeB:  "node-b",
	}
}

func applyOne(t *testing.T, db *sql.DB, version int64) {
	t.Helper()

	all, err := Available()
	if err != nil {
		t.Fatalf("Available: %v", err)
	}
	for _, m := range all {
		if m.Version != version {
			continue
		}
		if _, err := db.ExecContext(t.Context(), m.SQL); err != nil {
			t.Fatalf("apply %s: %v", m, err)
		}
		return
	}
	t.Fatalf("no migration with version %d", version)
}

func TestMultiStripeMigrationRebuildsChunks(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + dir + "/rebuild.db?_pragma=foreign_keys(1)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := t.Context()

	applyOne(t, db, 1)
	s := seedFileAndNodes(t, db)

	rows := []struct {
		id, node string
		index    int
		kind     string
		size     int64
		hash     string
	}{
		{"chunk-0", s.nodeA, 0, "data", 1048576, "aa"},
		{"chunk-1", s.nodeB, 1, "data", 4096, "bb"},
		{"chunk-4", s.nodeA, 4, "parity", 1048576, "cc"},
	}
	for _, r := range rows {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO chunks (id, file_id, chunk_index, node_id,
				remote_file_id, size, hash, chunk_type, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1700000000)`,
			r.id, s.fileID, r.index, r.node, r.id+"-remote", r.size, r.hash, r.kind); err != nil {
			t.Fatalf("insert %s: %v", r.id, err)
		}
	}

	applyOne(t, db, 2)

	got, err := db.QueryContext(ctx,
		`SELECT id, stripe_index, chunk_index, node_id, remote_file_id, size, hash, chunk_type
		 FROM chunks ORDER BY chunk_index`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = got.Close() }()

	var seen int
	for got.Next() {
		var (
			id, node, remote, hash, kind string
			stripe, index                int
			size                         int64
		)
		if err := got.Scan(&id, &stripe, &index, &node, &remote, &size, &hash, &kind); err != nil {
			t.Fatalf("scan: %v", err)
		}

		seen++
		if stripe != 0 {
			t.Errorf("%s: stripe_index = %d, want 0 for a pre-existing row", id, stripe)
		}

		var want *struct {
			id, node string
			index    int
			kind     string
			size     int64
			hash     string
		}
		for _, r := range rows {
			if r.id == id {
				want = &struct {
					id, node string
					index    int
					kind     string
					size     int64
					hash     string
				}{r.id, r.node, r.index, r.kind, r.size, r.hash}
			}
		}
		if want == nil {
			t.Errorf("unexpected row %q survived the rebuild", id)
			continue
		}
		if index != want.index || node != want.node || kind != want.kind {
			t.Errorf("%s: index=%d node=%q kind=%q, want %d %q %q",
				id, index, node, kind, want.index, want.node, want.kind)
		}

		if size != want.size {
			t.Errorf("%s: size = %d, want %d", id, size, want.size)
		}
		if hash != want.hash {
			t.Errorf("%s: hash = %q, want %q", id, hash, want.hash)
		}
		if remote != id+"-remote" {
			t.Errorf("%s: remote_file_id = %q, want %q", id, remote, id+"-remote")
		}
	}
	if err := got.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if seen != len(rows) {
		t.Errorf("%d rows survived the rebuild, want %d", seen, len(rows))
	}
}

func TestChunksAllowRepeatedIndexAcrossStripes(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	if _, err := New(db).Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}
	s := seedFileAndNodes(t, db)

	insert := func(id string, stripe, index int) error {
		_, err := db.ExecContext(ctx,
			`INSERT INTO chunks (id, file_id, stripe_index, chunk_index, node_id,
				remote_file_id, size, hash, chunk_type, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, 1048576, 'hash', 'data', 1700000000)`,
			id, s.fileID, stripe, index, s.nodeA, id+"-remote")
		return err
	}

	if err := insert("a", 0, 0); err != nil {
		t.Fatalf("stripe 0: %v", err)
	}
	if err := insert("b", 1, 0); err != nil {
		t.Fatalf("the same index in a different stripe must be allowed: %v", err)
	}
	if err := insert("c", 0, 0); err == nil {
		t.Error("the same index in the same stripe must still be rejected")
	}
}

func TestChunksStillForbidADuplicateShard(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	if _, err := New(db).Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}
	s := seedFileAndNodes(t, db)

	insert := func(id string, stripe, index int) error {
		_, err := db.ExecContext(ctx,
			`INSERT INTO chunks (id, file_id, stripe_index, chunk_index, node_id,
				remote_file_id, size, hash, chunk_type, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, 1048576, 'hash', 'data', 1700000000)`,
			id, s.fileID, stripe, index, s.nodeA, id+"-remote")
		return err
	}

	if err := insert("first", 2, 3); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := insert("second", 2, 3); err == nil {
		t.Error("a duplicate (file, stripe, index) must be rejected")
	}
}

func TestRebuildPreservedTheForeignKeys(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	if _, err := New(db).Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}
	s := seedFileAndNodes(t, db)

	if _, err := db.ExecContext(ctx,
		`INSERT INTO chunks (id, file_id, stripe_index, chunk_index, node_id,
			remote_file_id, size, hash, chunk_type, created_at)
		 VALUES ('orphan-check', ?, 0, 0, ?, 'remote', 1, 'hash', 'data', 1700000000)`,
		s.fileID, s.nodeA); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO chunks (id, file_id, stripe_index, chunk_index, node_id,
			remote_file_id, size, hash, chunk_type, created_at)
		 VALUES ('bad-fk', 'no-such-file', 0, 0, ?, 'remote', 1, 'hash', 'data', 1700000000)`,
		s.nodeA); err == nil {
		t.Error("the reference to files must still be enforced")
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM files WHERE id = ?`, s.fileID); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	var left int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunks WHERE file_id = ?`,
		s.fileID).Scan(&left); err != nil {
		t.Fatalf("count: %v", err)
	}
	if left != 0 {
		t.Errorf("%d chunks survived the file deletion, want 0", left)
	}
}

func TestRebuildPreservedTheCheckConstraints(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()

	if _, err := New(db).Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}
	s := seedFileAndNodes(t, db)

	insert := func(stripe int, kind string) error {
		_, err := db.ExecContext(ctx,
			`INSERT INTO chunks (id, file_id, stripe_index, chunk_index, node_id,
				remote_file_id, size, hash, chunk_type, created_at)
			 VALUES (?, ?, ?, 0, ?, 'remote', 1, 'hash', ?, 1700000000)`,
			"c"+kind+strconv.Itoa(stripe), s.fileID, stripe, s.nodeA, kind)
		return err
	}

	if err := insert(-1, "data"); err == nil {
		t.Error("a negative stripe index must be rejected")
	}
	if err := insert(0, "mirror"); err == nil {
		t.Error("an unknown chunk type must be rejected")
	}
}
