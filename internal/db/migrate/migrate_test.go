package migrate

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"strings"
	"testing"

	_ "modernc.org/sqlite" 
)




func testDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/migrate.db?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestUpAppliesEveryEmbeddedMigration(t *testing.T) {
	db := testDB(t)
	m := New(db)

	applied, err := m.Up(t.Context())
	if err != nil {
		t.Fatalf("Up() error = %v, want nil", err)
	}

	available, err := Available()
	if err != nil {
		t.Fatalf("Available() error = %v, want nil", err)
	}
	if applied != len(available) {
		t.Errorf("Up() applied %d migrations, want %d", applied, len(available))
	}

	for _, table := range []string{"pools", "nodes", "pool_nodes", "files", "chunks"} {
		var name string
		err := db.QueryRowContext(t.Context(),
			"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&name)
		if err != nil {
			t.Errorf("table %q is missing after Up(): %v", table, err)
		}
	}
}

func TestUpIsIdempotent(t *testing.T) {
	db := testDB(t)
	m := New(db)

	first, err := m.Up(t.Context())
	if err != nil {
		t.Fatalf("first Up() error = %v, want nil", err)
	}
	if first == 0 {
		t.Fatal("first Up() applied 0 migrations, want at least 1")
	}

	second, err := m.Up(t.Context())
	if err != nil {
		t.Fatalf("second Up() error = %v, want nil", err)
	}
	if second != 0 {
		t.Errorf("second Up() applied %d migrations, want 0", second)
	}

	third, err := m.Up(t.Context())
	if err != nil {
		t.Fatalf("third Up() error = %v, want nil", err)
	}
	if third != 0 {
		t.Errorf("third Up() applied %d migrations, want 0", third)
	}
}

func TestVersionReportsHighestApplied(t *testing.T) {
	db := testDB(t)
	m := New(db)

	got, err := m.Version(t.Context())
	if err != nil {
		t.Fatalf("Version() error = %v, want nil", err)
	}
	if got != 0 {
		t.Errorf("Version() before Up() = %d, want 0", got)
	}

	if _, err := m.Up(t.Context()); err != nil {
		t.Fatalf("Up() error = %v, want nil", err)
	}

	available, err := Available()
	if err != nil {
		t.Fatalf("Available() error = %v, want nil", err)
	}
	want := available[len(available)-1].Version

	got, err = m.Version(t.Context())
	if err != nil {
		t.Fatalf("Version() error = %v, want nil", err)
	}
	if got != want {
		t.Errorf("Version() after Up() = %d, want %d", got, want)
	}
}

func TestUpRecordsNameAndVersion(t *testing.T) {
	db := testDB(t)
	m := New(db)

	if _, err := m.Up(t.Context()); err != nil {
		t.Fatalf("Up() error = %v, want nil", err)
	}

	available, err := Available()
	if err != nil {
		t.Fatalf("Available() error = %v, want nil", err)
	}

	for _, mig := range available {
		var name string
		var appliedAt int64
		err := db.QueryRowContext(t.Context(),
			"SELECT name, applied_at FROM "+TableName+" WHERE version = ?", mig.Version,
		).Scan(&name, &appliedAt)
		if err != nil {
			t.Errorf("version %d is not recorded: %v", mig.Version, err)
			continue
		}
		if name != mig.Name {
			t.Errorf("version %d recorded name = %q, want %q", mig.Version, name, mig.Name)
		}
		if appliedAt <= 0 {
			t.Errorf("version %d recorded applied_at = %d, want a positive timestamp", mig.Version, appliedAt)
		}
	}
}

func TestUpFailsOnInvalidSQLAndLeavesVersionUnchanged(t *testing.T) {
	db := testDB(t)
	m := New(db)

	
	
	if err := m.ensureTable(t.Context()); err != nil {
		t.Fatalf("ensureTable() error = %v, want nil", err)
	}

	broken := Migration{Version: 9999, Name: "broken", SQL: "THIS IS NOT SQL"}
	if err := m.apply(t.Context(), broken); err == nil {
		t.Fatal("apply() of invalid SQL error = nil, want an error")
	}

	var recorded int
	err := db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+TableName+" WHERE version = ?", broken.Version).Scan(&recorded)
	if err != nil {
		t.Fatalf("count recorded: %v", err)
	}
	if recorded != 0 {
		t.Errorf("recorded versions = %d, want 0 (failed migration must roll back)", recorded)
	}
}

func TestUpCanContinueAfterAFailure(t *testing.T) {
	db := testDB(t)
	m := New(db)

	if _, err := m.Up(t.Context()); err != nil {
		t.Fatalf("Up() error = %v, want nil", err)
	}
	versionAfterFirst, err := m.Version(t.Context())
	if err != nil {
		t.Fatalf("Version() error = %v, want nil", err)
	}

	
	
	dup := Migration{Version: 10000, Name: "dup", SQL: "INSERT INTO " + TableName +
		" (version, name, applied_at) VALUES (1, 'dup', 1)"}
	if err := m.apply(t.Context(), dup); err == nil {
		t.Fatal("apply() of duplicate version error = nil, want an error")
	}

	versionAfterFailure, err := m.Version(t.Context())
	if err != nil {
		t.Fatalf("Version() error = %v, want nil", err)
	}
	if versionAfterFailure != versionAfterFirst {
		t.Errorf("Version() = %d after a failed migration, want it unchanged at %d",
			versionAfterFailure, versionAfterFirst)
	}
}

func TestUpRespectsContextCancellation(t *testing.T) {
	db := testDB(t)
	m := New(db)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := m.Up(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Up() with a cancelled context = %v, want context.Canceled", err)
	}
}

func TestAvailableIsOrderedAndComplete(t *testing.T) {
	got, err := Available()
	if err != nil {
		t.Fatalf("Available() error = %v, want nil", err)
	}
	if len(got) == 0 {
		t.Fatal("Available() = empty, want at least one migration")
	}

	for i, mig := range got {
		if mig.Version < 1 {
			t.Errorf("migrations[%d].Version = %d, want >= 1", i, mig.Version)
		}
		if mig.Name == "" {
			t.Errorf("migrations[%d].Name is empty", i)
		}
		if mig.SQL == "" {
			t.Errorf("migrations[%d].SQL is empty", i)
		}
		if i > 0 && got[i-1].Version >= mig.Version {
			t.Errorf("migrations are not ordered: %d before %d", got[i-1].Version, mig.Version)
		}
	}
}

func TestAvailableFindsTheInitialSchema(t *testing.T) {
	got, err := Available()
	if err != nil {
		t.Fatalf("Available() error = %v, want nil", err)
	}

	first := got[0]
	if first.Version != 1 {
		t.Errorf("first migration version = %d, want 1", first.Version)
	}
	if first.Name != "init" {
		t.Errorf("first migration name = %q, want %q", first.Name, "init")
	}
	for _, table := range []string{"pools", "nodes", "pool_nodes", "files", "chunks"} {
		if !strings.Contains(first.SQL, "CREATE TABLE "+table) {
			t.Errorf("first migration does not create %q", table)
		}
	}
}

func TestParseFileName(t *testing.T) {
	tests := []struct {
		in         string
		wantVer    int64
		wantName   string
		wantErrSub string
	}{
		{in: "0001_init.sql", wantVer: 1, wantName: "init"},
		{in: "0042_add_files.sql", wantVer: 42, wantName: "add_files"},
		{in: "12_short_prefix.sql", wantVer: 12, wantName: "short_prefix"},
		{in: "init.sql", wantErrSub: "must be named"},
		{in: "1_.sql", wantErrSub: "must be named"},
		{in: "abc_name.sql", wantErrSub: "non-numeric version"},
		{in: "-1_negative.sql", wantErrSub: "negative version"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			version, name, err := parseFileName(tt.in)

			if tt.wantErrSub != "" {
				if err == nil {
					t.Fatalf("parseFileName(%q) error = nil, want an error containing %q",
						tt.in, tt.wantErrSub)
				}
				if !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("parseFileName(%q) error = %q, want it to contain %q",
						tt.in, err, tt.wantErrSub)
				}
				return
			}

			if err != nil {
				t.Fatalf("parseFileName(%q) error = %v, want nil", tt.in, err)
			}
			if version != tt.wantVer {
				t.Errorf("parseFileName(%q) version = %d, want %d", tt.in, version, tt.wantVer)
			}
			if name != tt.wantName {
				t.Errorf("parseFileName(%q) name = %q, want %q", tt.in, name, tt.wantName)
			}
		})
	}
}

func TestMigrationString(t *testing.T) {
	m := Migration{Version: 7, Name: "add_files"}
	if got, want := m.String(), "0007_add_files"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestErrNoMigrationUnwraps(t *testing.T) {
	inner := fs.ErrNotExist
	err := &ErrNoMigration{Path: "migrations", Err: inner}

	if !errors.Is(err, inner) {
		t.Error("errors.Is() = false, want true")
	}
	if !strings.Contains(err.Error(), "migrations") {
		t.Errorf("Error() = %q, want it to mention the path", err)
	}
}
