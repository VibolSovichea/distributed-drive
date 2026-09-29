package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/config"
	"github.com/VibolSovichea/distributed-drive/internal/db/migrate"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()

	cfg := config.Defaults()
	cfg.HTTP.Addr = "127.0.0.1:0"
	cfg.Database.Path = filepath.Join(t.TempDir(), "app.db")

	return cfg
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewAppliesMigrationsAndServesTheHealthEndpoint(t *testing.T) {
	application, err := New(t.Context(), testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = application.Close() })

	req := httptest.NewRequest(http.MethodPost, "/api/pools",
		strings.NewReader(`{"name":"primary","dataChunks":4,"parityChunks":2,"chunkSize":8388608}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	application.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/pools = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body)
	}

	rec = httptest.NewRecorder()
	application.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health/ready = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body)
	}
}

func TestNewIsIdempotentAcrossRestarts(t *testing.T) {
	cfg := testConfig(t)

	available, err := migrate.Available()
	if err != nil {
		t.Fatalf("migrate.Available() error = %v, want nil", err)
	}
	if len(available) == 0 {
		t.Fatal("there are no embedded migrations, so this test cannot mean anything")
	}

	first, err := New(t.Context(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("first New() error = %v, want nil", err)
	}
	if first.migrations != len(available) {
		t.Errorf("first start applied %d migrations, want all %d",
			first.migrations, len(available))
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	second, err := New(t.Context(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("second New() error = %v, want nil", err)
	}
	defer func() { _ = second.Close() }()

	if second.migrations != 0 {
		t.Errorf("second start applied %d migrations, want 0", second.migrations)
	}
}

func TestNewWithoutMigrateOnOpenRejectsAnUnmigratedDatabase(t *testing.T) {
	cfg := testConfig(t)
	cfg.Database.MigrateOnOpen = false

	application, err := New(t.Context(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = application.Close() })

	rec := httptest.NewRecorder()
	application.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/pools", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("GET /api/pools on an unmigrated database = %d, want %d",
			rec.Code, http.StatusInternalServerError)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	application, err := New(t.Context(), testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	if err := application.Close(); err != nil {
		t.Fatalf("first Close() error = %v, want nil", err)
	}

	if err := application.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
}

func TestRunDrainsOnContextCancellation(t *testing.T) {
	cfg := testConfig(t)

	application, err := New(t.Context(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() { errCh <- application.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil after a clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after the context was cancelled")
	}
}

func TestRunOnAnUnusableAddressFails(t *testing.T) {
	cfg := testConfig(t)

	cfg.HTTP.Addr = "127.0.0.1:1"

	application, err := New(t.Context(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	if err := application.Run(t.Context()); err == nil {
		t.Error("Run() = nil, want a bind error")
	}
}

func TestStoreIsReachableAfterStartup(t *testing.T) {
	application, err := New(t.Context(), testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = application.Close() })

	if err := application.Store().Ping(t.Context()); err != nil {
		t.Errorf("Store().Ping() error = %v, want nil", err)
	}
}

func TestHealthEndpointOverTheWholeStack(t *testing.T) {
	application, err := New(t.Context(), testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = application.Close() })

	rec := httptest.NewRecorder()
	application.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health = %d, want %d", rec.Code, http.StatusOK)
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding %q: %v", rec.Body, err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
}
