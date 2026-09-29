package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/VibolSovichea/distributed-drive/internal/config"

	_ "modernc.org/sqlite"
)

const DriverName = "sqlite"

func Open(ctx context.Context, cfg config.DatabaseConfig) (*sql.DB, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("db: database path must not be empty")
	}
	if cfg.BusyTimeout <= 0 {
		return nil, fmt.Errorf("db: busy timeout must be positive, got %s", cfg.BusyTimeout)
	}
	if cfg.MaxOpenConns < 1 {
		return nil, fmt.Errorf("db: max open connections must be at least 1, got %d", cfg.MaxOpenConns)
	}

	if dir := filepath.Dir(cfg.Path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("db: create directory %q: %w", dir, err)
		}
	}

	sqlDB, err := sql.Open(DriverName, dsn(cfg))
	if err != nil {
		return nil, fmt.Errorf("db: open %q: %w", cfg.Path, err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(0)

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("db: ping %q: %w", cfg.Path, err)
	}

	return sqlDB, nil
}

func dsn(cfg config.DatabaseConfig) string {
	pragmas := []string{
		"foreign_keys(1)",
		"journal_mode(WAL)",
		"busy_timeout(" + strconv.FormatInt(cfg.BusyTimeout.Milliseconds(), 10) + ")",
		"synchronous(NORMAL)",
	}

	q := url.Values{}
	for _, p := range pragmas {
		q.Add("_pragma", p)
	}

	u := url.URL{
		Scheme:   "file",
		Path:     filepath.ToSlash(cfg.Path),
		RawQuery: q.Encode(),
		OmitHost: true,
	}
	return u.String()
}
