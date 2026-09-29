package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestHealthIsAlwaysOK(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/health", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health = %d, want %d", rec.Code, http.StatusOK)
	}

	var body healthBody
	decode(t, rec, &body)

	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
	if body.Time == "" {
		t.Error("time is empty, want a timestamp")
	}
	if body.Version == "" {
		t.Error("version is empty, want a value")
	}

	if len(body.Checks) != 0 {
		t.Errorf("checks = %v, want none for a liveness probe", body.Checks)
	}
}

func TestReadyReportsTheDatabase(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/health/ready", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health/ready = %d, want %d", rec.Code, http.StatusOK)
	}

	var body healthBody
	decode(t, rec, &body)

	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
	if body.Checks["database"] != "ok" {
		t.Errorf("checks[database] = %q, want %q", body.Checks["database"], "ok")
	}
}

type failingPinger struct{ err error }

func (f failingPinger) Ping(_ context.Context) error { return f.err }

func TestReadyFailsWhenTheStoreIsUnreachable(t *testing.T) {
	srv := newTestServer(t)
	srv.store = failingPinger{err: errors.New("disk on fire")}

	rec := do(t, srv, http.MethodGet, "/health/ready", "")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /health/ready = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var body healthBody
	decode(t, rec, &body)

	if body.Status != "unavailable" {
		t.Errorf("status = %q, want %q", body.Status, "unavailable")
	}
	if body.Checks["database"] != "unreachable" {
		t.Errorf("checks[database] = %q, want %q", body.Checks["database"], "unreachable")
	}

	if strings.Contains(rec.Body.String(), "disk on fire") {
		t.Errorf("body %q leaks the internal error", rec.Body.String())
	}
}

func TestUnknownRouteIsA404(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/nope", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope = %d, want %d", rec.Code, http.StatusNotFound)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codeNotFound {
		t.Errorf("code = %q, want %q", body.Error.Code, codeNotFound)
	}
}

func TestWrongMethodIsA405(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodDelete, "/health", "")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /health = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codeMethodNotAllowed {
		t.Errorf("code = %q, want %q", body.Error.Code, codeMethodNotAllowed)
	}
}
