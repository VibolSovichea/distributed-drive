package migrate

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*
var migrationsFS embed.FS

const migrationsDir = "migrations"

const TableName = "schema_migrations"

const createMigrationsTable = `
CREATE TABLE IF NOT EXISTS ` + TableName + ` (
    version     INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    applied_at  INTEGER NOT NULL
)`

type Migration struct {
	Version int64
	Name    string
	SQL     string
}

func (m Migration) String() string {
	return fmt.Sprintf("%04d_%s", m.Version, m.Name)
}

type ErrNoMigration struct {
	Path string
	Err  error
}

func (e *ErrNoMigration) Error() string {
	return fmt.Sprintf("migrate: no migration files in %q: %v", e.Path, e.Err)
}

func (e *ErrNoMigration) Unwrap() error { return e.Err }

type Migrator struct {
	db *sql.DB
}

func New(db *sql.DB) *Migrator {
	return &Migrator{db: db}
}

func Available() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return nil, &ErrNoMigration{Path: migrationsDir, Err: err}
	}

	out := make([]Migration, 0, len(entries))
	seen := make(map[int64]string, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version, name, err := parseFileName(entry.Name())
		if err != nil {
			return nil, err
		}
		if previous, dup := seen[version]; dup {
			return nil, fmt.Errorf("migrate: duplicate version %d in %q and %q",
				version, previous, entry.Name())
		}
		seen[version] = entry.Name()

		body, err := fs.ReadFile(migrationsFS, path.Join(migrationsDir, entry.Name()))
		if err != nil {
			return nil, &ErrNoMigration{Path: entry.Name(), Err: err}
		}

		out = append(out, Migration{Version: version, Name: name, SQL: string(body)})
	}

	if len(out) == 0 {
		return nil, &ErrNoMigration{Path: migrationsDir, Err: fmt.Errorf("no *.sql files found")}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func parseFileName(fileName string) (int64, string, error) {
	base := strings.TrimSuffix(fileName, ".sql")

	prefix, name, ok := strings.Cut(base, "_")
	if !ok || name == "" {
		return 0, "", fmt.Errorf("migrate: file %q must be named NNNN_description.sql", fileName)
	}

	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("migrate: file %q has a non-numeric version prefix: %w", fileName, err)
	}
	if version < 0 {
		return 0, "", fmt.Errorf("migrate: file %q has a negative version", fileName)
	}

	return version, name, nil
}

func (m *Migrator) Up(ctx context.Context) (int, error) {
	if err := m.ensureTable(ctx); err != nil {
		return 0, err
	}

	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return 0, err
	}

	available, err := Available()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, mig := range available {
		if applied[mig.Version] {
			continue
		}
		if err := m.apply(ctx, mig); err != nil {
			return count, err
		}
		count++
	}

	return count, nil
}

func (m *Migrator) Version(ctx context.Context) (int64, error) {
	if err := m.ensureTable(ctx); err != nil {
		return 0, err
	}

	var version sql.NullInt64
	err := m.db.QueryRowContext(ctx,
		"SELECT MAX(version) FROM "+TableName,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("migrate: read version: %w", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

func (m *Migrator) ensureTable(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, createMigrationsTable); err != nil {
		return fmt.Errorf("migrate: create %s: %w", TableName, err)
	}
	return nil
}

func (m *Migrator) appliedVersions(ctx context.Context) (map[int64]bool, error) {
	rows, err := m.db.QueryContext(ctx, "SELECT version FROM "+TableName)
	if err != nil {
		return nil, fmt.Errorf("migrate: read applied versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	applied := make(map[int64]bool)
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("migrate: scan applied version: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("migrate: iterate applied versions: %w", err)
	}
	return applied, nil
}

func (m *Migrator) apply(ctx context.Context, mig Migration) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: begin %s: %w", mig, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, mig.SQL); err != nil {
		return fmt.Errorf("migrate: apply %s: %w", mig, err)
	}

	_, err = tx.ExecContext(ctx,
		"INSERT INTO "+TableName+" (version, name, applied_at) VALUES (?, ?, strftime('%s','now'))",
		mig.Version, mig.Name,
	)
	if err != nil {
		return fmt.Errorf("migrate: record %s: %w", mig, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migrate: commit %s: %w", mig, err)
	}
	return nil
}
