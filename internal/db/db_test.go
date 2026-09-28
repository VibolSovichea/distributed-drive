package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/config"
)

func testConfig(t *testing.T) config.DatabaseConfig {
	t.Helper()
	return config.DatabaseConfig{
		Path:          filepath.Join(t.TempDir(), "test.db"),
		BusyTimeout:   2 * time.Second,
		MaxOpenConns:  1,
		MigrateOnOpen: true,
	}
}


func openTest(t *testing.T) config.DatabaseConfig {
	t.Helper()

	cfg := testConfig(t)
	sqlDB, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return cfg
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestOpenCreatesTheFile(t *testing.T) {
	cfg := testConfig(t)

	sqlDB, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	mustExec(t, sqlDB, "CREATE TABLE t (id INTEGER)")
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	if !fileExists(cfg.Path) {
		t.Errorf("database file %q was not created", cfg.Path)
	}
}

func TestOpenCreatesParentDirectories(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "c", "test.db")

	sqlDB, err := Open(t.Context(), config.DatabaseConfig{
		Path: path, BusyTimeout: time.Second, MaxOpenConns: 1,
	})
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	defer func() { _ = sqlDB.Close() }()

	if !fileExists(path) {
		t.Errorf("database file %q was not created", path)
	}
}

func TestOpenAppliesPragmas(t *testing.T) {
	openTest(t)
	cfg := testConfig(t)
	sqlDB, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	defer func() { _ = sqlDB.Close() }()

	tests := []struct {
		pragma string
		want   string
	}{
		{pragma: "foreign_keys", want: "1"},
		{pragma: "journal_mode", want: "wal"},
	}

	for _, tt := range tests {
		t.Run(tt.pragma, func(t *testing.T) {
			var got string
			if err := sqlDB.QueryRowContext(t.Context(), "PRAGMA "+tt.pragma).Scan(&got); err != nil {
				t.Fatalf("PRAGMA %s: %v", tt.pragma, err)
			}
			if !strings.EqualFold(got, tt.want) {
				t.Errorf("PRAGMA %s = %q, want %q", tt.pragma, got, tt.want)
			}
		})
	}
}




func TestOpenEnforcesForeignKeys(t *testing.T) {
	cfg := testConfig(t)
	sqlDB, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	defer func() { _ = sqlDB.Close() }()

	mustExec(t, sqlDB, `CREATE TABLE parent (id TEXT PRIMARY KEY)`)
	mustExec(t, sqlDB, `CREATE TABLE child (
		id        TEXT PRIMARY KEY,
		parent_id TEXT NOT NULL REFERENCES parent (id) ON DELETE CASCADE
	)`)
	mustExec(t, sqlDB, `INSERT INTO parent (id) VALUES ('p1')`)
	mustExec(t, sqlDB, `INSERT INTO child (id, parent_id) VALUES ('c1', 'p1')`)

	if _, err := sqlDB.ExecContext(t.Context(),
		`INSERT INTO child (id, parent_id) VALUES ('c2', 'missing')`); err == nil {
		t.Error("inserting a child with an unknown parent succeeded, want a foreign key error")
	}

	mustExec(t, sqlDB, `DELETE FROM parent WHERE id = 'p1'`)

	var remaining int
	if err := sqlDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM child`).Scan(&remaining); err != nil {
		t.Fatalf("count children: %v", err)
	}
	if remaining != 0 {
		t.Errorf("children remaining = %d, want 0 (ON DELETE CASCADE did not run)", remaining)
	}
}

func TestOpenRejectsInvalidConfig(t *testing.T) {
	valid := testConfig(t)

	tests := []struct {
		name    string
		mutate  func(*config.DatabaseConfig)
		wantSub string
	}{
		{
			name:    "empty path",
			mutate:  func(c *config.DatabaseConfig) { c.Path = "" },
			wantSub: "must not be empty",
		},
		{
			name:    "zero busy timeout",
			mutate:  func(c *config.DatabaseConfig) { c.BusyTimeout = 0 },
			wantSub: "busy timeout must be positive",
		},
		{
			name:    "zero max open conns",
			mutate:  func(c *config.DatabaseConfig) { c.MaxOpenConns = 0 },
			wantSub: "at least 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)

			sqlDB, err := Open(t.Context(), cfg)
			if err == nil {
				_ = sqlDB.Close()
				t.Fatalf("Open() error = nil, want an error containing %q", tt.wantSub)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("Open() error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

func TestOpenFailsWhenParentDirectoryCannotBeCreated(t *testing.T) {
	
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocker file: %v", err)
	}

	sqlDB, err := Open(t.Context(), config.DatabaseConfig{
		Path:         filepath.Join(blocker, "test.db"),
		BusyTimeout:  time.Second,
		MaxOpenConns: 1,
	})
	if err == nil {
		_ = sqlDB.Close()
		t.Fatal("Open() error = nil, want an error")
	}
}

func TestOpenIsReusableAfterClose(t *testing.T) {
	cfg := testConfig(t)

	first, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("first Open() error = %v, want nil", err)
	}
	mustExec(t, first, `CREATE TABLE t (id INTEGER)`)
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	second, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("second Open() error = %v, want nil", err)
	}
	defer func() { _ = second.Close() }()

	var name string
	if err := second.QueryRowContext(t.Context(),
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 't'`).Scan(&name); err != nil {
		t.Fatalf("table t did not survive reopen: %v", err)
	}
}

func TestPingFailsAfterClose(t *testing.T) {
	cfg := testConfig(t)
	sqlDB, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("Open() error = %v, want nil", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := sqlDB.PingContext(t.Context()); err == nil {
		t.Error("PingContext() after Close() = nil, want an error")
	}
}

func TestDSNEncodesEveryPragma(t *testing.T) {
	got := dsn(config.DatabaseConfig{
		Path:         filepath.Join("data", "my db.db"),
		BusyTimeout:  1500 * time.Millisecond,
		MaxOpenConns: 1,
	})

	for _, want := range []string{
		"file:data/my%20db.db?",
		"foreign_keys%281%29",
		"journal_mode%28WAL%29",
		"busy_timeout%281500%29",
		"synchronous%28NORMAL%29",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("dsn() = %q, want it to contain %q", got, want)
		}
	}

	
	
	if strings.Contains(got, "file://") {
		t.Errorf("dsn() = %q, want a single slash after the scheme", got)
	}
}



func TestOpenHandlesPathsThatNeedEscaping(t *testing.T) {
	for _, name := range []string{"with space.db", "with#hash.db", "with?question.db", "with%percent.db"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)

			sqlDB, err := Open(t.Context(), config.DatabaseConfig{
				Path: path, BusyTimeout: time.Second, MaxOpenConns: 1,
			})
			if err != nil {
				t.Fatalf("Open(%q) error = %v, want nil", name, err)
			}
			mustExec(t, sqlDB, "CREATE TABLE t (id INTEGER)")
			if err := sqlDB.Close(); err != nil {
				t.Fatalf("Close() error = %v, want nil", err)
			}

			if !fileExists(path) {
				t.Errorf("database file %q was not created at the requested path", path)
			}
		})
	}
}
