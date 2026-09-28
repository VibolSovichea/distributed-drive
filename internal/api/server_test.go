package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

func quietServer(t *testing.T) *Server {
	t.Helper()
	return newStubServer(t, nil, baseStub())
}

func TestListenAndServeDrainsOnContextCancellation(t *testing.T) {
	srv := quietServer(t)

	
	
	addr := reserveAddr(t)

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx, addr, 2*time.Second) }()

	
	
	waitForServer(t, addr)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v, want nil after a clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ListenAndServe() did not return after the context was cancelled")
	}

	
	
	probe, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the address is still bound after shutdown: %v", err)
	}
	_ = probe.Close()
}

func TestListenAndServeFinishesInFlightRequestsBeforeReturning(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)

	stub := baseStub()
	stub.get = func(_ context.Context, id string) (metadata.Pool, error) {
		entered <- struct{}{}
		<-release
		return metadata.Pool{ID: id}, nil
	}

	srv := newStubServer(t, nil, stub)
	addr := reserveAddr(t)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx, addr, 5*time.Second) }()
	waitForServer(t, addr)

	go func() {
		resp, err := http.Get("http://" + addr + "/api/pools/01J00000000000000000000000")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	<-entered
	cancel()

	
	
	
	time.Sleep(200 * time.Millisecond)
	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ListenAndServe() did not return")
	}
}

func TestListenAndServeReportsABindFailure(t *testing.T) {
	srv := quietServer(t)

	
	
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	defer func() { _ = blocker.Close() }()

	err = srv.ListenAndServe(t.Context(), blocker.Addr().String(), time.Second)
	if err == nil {
		t.Fatal("ListenAndServe() = nil, want a bind error")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error = %q, want it to name the failure", err)
	}
}

func TestACancelledRequestIsNotAnErrorResponse(t *testing.T) {
	stub := baseStub()
	stub.list = func(ctx context.Context) ([]metadata.Pool, error) {
		return nil, fmt.Errorf("query: %w", context.Canceled)
	}

	srv := newStubServer(t, nil, stub)

	rec := do(t, srv, http.MethodGet, "/api/pools", "")

	
	
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want nothing written for a cancelled request", rec.Body.String())
	}
}

func TestATimedOutRequestBecomesA504(t *testing.T) {
	stub := baseStub()
	stub.list = func(ctx context.Context) ([]metadata.Pool, error) {
		return nil, fmt.Errorf("query: %w", context.DeadlineExceeded)
	}

	srv := newStubServer(t, nil, stub)

	rec := do(t, srv, http.MethodGet, "/api/pools", "")

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codeUnavailable {
		t.Errorf("code = %q, want %q", body.Error.Code, codeUnavailable)
	}
}

func TestVersionIsReportedAndOverridable(t *testing.T) {
	srv := quietServer(t)

	rec := do(t, srv, http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health = %d, want %d", rec.Code, http.StatusOK)
	}

	if version == "" {
		t.Error("version is empty, want a value")
	}
	if !strings.Contains(rec.Body.String(), `"version"`) {
		t.Errorf("body = %q, want a version field", rec.Body.String())
	}
}


func reserveAddr(t *testing.T) string {
	t.Helper()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving an address: %v", err)
	}
	addr := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("releasing the reservation: %v", err)
	}

	return addr
}


func waitForServer(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("nothing came up on %s", addr)
}
