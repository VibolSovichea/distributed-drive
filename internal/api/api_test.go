package api

import (
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/config"
	"github.com/VibolSovichea/distributed-drive/internal/db"
	"github.com/VibolSovichea/distributed-drive/internal/db/migrate"
	"github.com/VibolSovichea/distributed-drive/internal/id"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	metastoresqlite "github.com/VibolSovichea/distributed-drive/internal/metadata/sqlite"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()

	srv, _ := newTestServerAndStore(t)
	return srv
}

func newTestServerAndStore(t *testing.T) (*Server, *metastoresqlite.Store) {
	t.Helper()

	handle := openTestDB(t)
	store := metastoresqlite.New(handle)
	t.Cleanup(func() { _ = store.Close() })

	srv, err := New(Config{
		Pools:  pool.NewManager(store, nil),
		Store:  store,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    func() time.Time { return time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	return srv, store
}

func mustCreateFile(t *testing.T, store *metastoresqlite.Store, poolID, name string) metadata.File {
	t.Helper()

	file := metadata.File{
		ID:           id.New(),
		PoolID:       poolID,
		Name:         name,
		Size:         3 << 20,
		ContentHash:  "e3b0c44298fc1c149afbf4c8996fb924",
		ChunkSize:    metadata.MinChunkSize,
		DataChunks:   4,
		ParityChunks: 2,
		Status:       metadata.FileStatusCommitted,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	if err := store.CreateFile(t.Context(), file); err != nil {
		t.Fatalf("CreateFile() error = %v, want nil", err)
	}

	return file
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	handle, err := db.Open(t.Context(), config.DatabaseConfig{
		Path:          filepath.Join(t.TempDir(), "api.db"),
		BusyTimeout:   time.Second,
		MaxOpenConns:  1,
		MigrateOnOpen: true,
	})
	if err != nil {
		t.Fatalf("db.Open() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = handle.Close() })

	if _, err := migrate.New(handle).Up(t.Context()); err != nil {
		t.Fatalf("migrate.Up() error = %v, want nil", err)
	}

	return handle
}

func do(t *testing.T, srv *Server, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, target, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()

	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	if err := json.Unmarshal(rec.Body.Bytes(), dst); err != nil {
		t.Fatalf("decoding %q: %v", rec.Body.String(), err)
	}
}

const validPoolBody = `{"name":"primary","dataChunks":4,"parityChunks":2,"chunkSize":8388608}`

func TestNewRequiresItsDependencies(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("New() with no Pools = nil, want an error")
	}
	if _, err := New(Config{Pools: pool.NewManager(nil, nil)}); err == nil {
		t.Error("New() with no Store = nil, want an error")
	}
}
