package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)

type stubPools struct {
	create func(context.Context, pool.CreateInput) (metadata.Pool, error)
	get    func(context.Context, string) (metadata.Pool, error)
	list   func(context.Context) ([]metadata.Pool, error)
	remove func(context.Context, string) error
	cap    func(context.Context, string) (pool.Capacity, error)
	add    func(context.Context, string, string) (metadata.Node, error)
	detach func(context.Context, string, string) error
	nodes  func(context.Context, string) ([]metadata.Node, error)
}

func (s stubPools) Create(ctx context.Context, in pool.CreateInput) (metadata.Pool, error) {
	return s.create(ctx, in)
}

func (s stubPools) Get(ctx context.Context, id string) (metadata.Pool, error) {
	return s.get(ctx, id)
}

func (s stubPools) List(ctx context.Context) ([]metadata.Pool, error) { return s.list(ctx) }

func (s stubPools) Delete(ctx context.Context, id string) error { return s.remove(ctx, id) }

func (s stubPools) Capacity(ctx context.Context, id string) (pool.Capacity, error) {
	return s.cap(ctx, id)
}

func (s stubPools) AddNode(ctx context.Context, poolID, nodeID string) (metadata.Node, error) {
	return s.add(ctx, poolID, nodeID)
}

func (s stubPools) RemoveNode(ctx context.Context, poolID, nodeID string) error {
	return s.detach(ctx, poolID, nodeID)
}

func (s stubPools) ListNodes(ctx context.Context, poolID string) ([]metadata.Node, error) {
	return s.nodes(ctx, poolID)
}

func newStubServer(t *testing.T, logs *bytes.Buffer, pools pool.Service) *Server {
	t.Helper()

	var logger *slog.Logger
	if logs != nil {
		logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	}

	srv, err := New(Config{
		Pools:  pools,
		Store:  okPinger{},
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}

	return srv
}

type okPinger struct{}

func (okPinger) Ping(context.Context) error { return nil }

func neverCalled(name string) error {
	panic("stubPools." + name + " called but not configured")
}

func baseStub() stubPools {
	return stubPools{
		create: func(context.Context, pool.CreateInput) (metadata.Pool, error) {
			return metadata.Pool{}, neverCalled("Create")
		},
		get: func(context.Context, string) (metadata.Pool, error) {
			return metadata.Pool{}, neverCalled("Get")
		},
		list: func(context.Context) ([]metadata.Pool, error) {
			return nil, neverCalled("List")
		},
		remove: func(context.Context, string) error { return neverCalled("Delete") },
		cap: func(context.Context, string) (pool.Capacity, error) {
			return pool.Capacity{}, neverCalled("Capacity")
		},
		add: func(context.Context, string, string) (metadata.Node, error) {
			return metadata.Node{}, neverCalled("AddNode")
		},
		detach: func(context.Context, string, string) error { return neverCalled("RemoveNode") },
		nodes: func(context.Context, string) ([]metadata.Node, error) {
			return nil, neverCalled("ListNodes")
		},
	}
}

func TestRequestIDIsEchoedBackAndAccepted(t *testing.T) {
	srv := newStubServer(t, nil, baseStub())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(RequestIDHeader, "trace-abc_123")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if got := rec.Header().Get(RequestIDHeader); got != "trace-abc_123" {
		t.Errorf("%s = %q, want the client's id echoed back", RequestIDHeader, got)
	}
}

func TestRequestIDIsReplacedWhenUntrustworthy(t *testing.T) {

	hostile := "abc\r\nlevel=error msg=\"forged\""

	srv := newStubServer(t, nil, baseStub())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(RequestIDHeader, hostile)
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	got := rec.Header().Get(RequestIDHeader)
	if got == hostile {
		t.Fatal("the hostile request id was echoed back")
	}
	if !safeRequestID(got) {
		t.Errorf("replacement id = %q, want a safe one", got)
	}
}

func TestRequestIDIsGeneratedWhenAbsent(t *testing.T) {
	srv := newStubServer(t, nil, baseStub())

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if got := rec.Header().Get(RequestIDHeader); got == "" {
		t.Error("no request id was generated for a request that sent none")
	}
}

func TestSafeRequestID(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{id: "abc", want: true},
		{id: "01J00000000000000000000000", want: true},
		{id: "trace-1.2_3", want: true},
		{id: "", want: false},
		{id: "has space", want: false},
		{id: "semi;colon", want: false},
		{id: strings.Repeat("a", 65), want: false},
		{id: strings.Repeat("a", 64), want: true},
	}

	for _, tt := range tests {
		if got := safeRequestID(tt.id); got != tt.want {
			t.Errorf("safeRequestID(%q) = %t, want %t", tt.id, got, tt.want)
		}
	}
}

func TestPanicInAHandlerBecomesA500(t *testing.T) {
	stub := baseStub()
	stub.get = func(context.Context, string) (metadata.Pool, error) {
		panic("a defect, not a client error")
	}

	var logs bytes.Buffer
	srv := newStubServer(t, &logs, stub)

	rec := do(t, srv, http.MethodGet, "/api/pools/01J00000000000000000000000", "")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codeInternal {
		t.Errorf("code = %q, want %q", body.Error.Code, codeInternal)
	}

	if strings.Contains(rec.Body.String(), "a defect") {
		t.Errorf("body %q leaks the panic value", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "goroutine") {
		t.Errorf("body %q leaks a stack trace", rec.Body.String())
	}

	if !strings.Contains(logs.String(), "a defect, not a client error") {
		t.Errorf("logs = %q, want the panic value recorded", logs.String())
	}
	if !strings.Contains(logs.String(), "stack=") {
		t.Errorf("logs = %q, want a stack trace recorded", logs.String())
	}
}

func TestServerKeepsServingAfterAPanic(t *testing.T) {

	stub := baseStub()
	fail := true
	stub.get = func(_ context.Context, id string) (metadata.Pool, error) {
		if fail {
			fail = false
			panic("boom")
		}
		return metadata.Pool{ID: id, Name: "primary"}, nil
	}

	srv := newStubServer(t, nil, stub)
	handler := srv.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/pools/01J00000000000000000000000", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("first request = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/pools/01J00000000000000000000000", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("second request = %d, want %d: the server must survive", rec.Code, http.StatusOK)
	}
}

func TestAnUnexpectedErrorDoesNotLeakInternals(t *testing.T) {
	stub := baseStub()
	stub.list = func(context.Context) ([]metadata.Pool, error) {
		return nil, errors.New("dial tcp 10.0.0.5:5432: connection refused: password=hunter2")
	}

	srv := newStubServer(t, nil, stub)

	rec := do(t, srv, http.MethodGet, "/api/pools", "")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	for _, leak := range []string{"10.0.0.5", "hunter2", "connection refused"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("body %q leaks %q", rec.Body.String(), leak)
		}
	}
}

func TestResponsesAreNotSniffable(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/health", "")

	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want %q", got, "nosniff")
	}
}

func TestEveryErrorCarriesTheRequestID(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/pools/01J00000000000000000000000", nil)
	req.Header.Set(RequestIDHeader, "trace-xyz")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	var body errorBody
	decode(t, rec, &body)

	if body.Error.RequestID != "trace-xyz" {
		t.Errorf("requestId = %q, want %q", body.Error.RequestID, "trace-xyz")
	}
}

func TestRequestLogsCarryStatusAndID(t *testing.T) {
	var logs bytes.Buffer
	srv := newStubServer(t, &logs, baseStub())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(RequestIDHeader, "trace-1")
	srv.Handler().ServeHTTP(httptest.NewRecorder(), req)

	line := logs.String()
	for _, want := range []string{"msg=\"http request\"", "status=200", "requestId=trace-1", "duration="} {
		if !strings.Contains(line, want) {
			t.Errorf("access log %q is missing %q", line, want)
		}
	}
}

func TestClientErrorsLogAtWarnAndServerErrorsAtError(t *testing.T) {
	var notFoundLogs bytes.Buffer
	srv := newStubServer(t, &notFoundLogs, baseStub())
	do(t, srv, http.MethodGet, "/nope", "")

	if !strings.Contains(notFoundLogs.String(), "level=WARN") {
		t.Errorf("logs = %q, want a 4xx logged at WARN", notFoundLogs.String())
	}

	stub := baseStub()
	stub.list = func(context.Context) ([]metadata.Pool, error) {
		return nil, errors.New("unexpected")
	}
	var serverLogs bytes.Buffer
	srv = newStubServer(t, &serverLogs, stub)
	do(t, srv, http.MethodGet, "/api/pools", "")

	if !strings.Contains(serverLogs.String(), "level=ERROR") {
		t.Errorf("logs = %q, want a 5xx logged at ERROR", serverLogs.String())
	}
}

func TestForwardedForIsOnlyBelievedFromLoopback(t *testing.T) {
	srv := newStubServer(t, nil, baseStub())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "203.0.113.9:5555"
	req.Header.Set("X-Forwarded-For", "10.1.2.3")
	rec := httptest.NewRecorder()

	var seen string
	handler := srv.realIP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.RemoteAddr
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(rec, req)

	if seen != "203.0.113.9:5555" {
		t.Errorf("RemoteAddr = %q, want the direct address to be kept", seen)
	}

	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("X-Forwarded-For", "10.1.2.3, 10.9.9.9")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "10.1.2.3" {
		t.Errorf("RemoteAddr = %q, want the first forwarded address", seen)
	}
}

func TestFirstForwarded(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{header: "10.1.2.3", want: "10.1.2.3"},
		{header: "10.1.2.3, 10.9.9.9", want: "10.1.2.3"},
		{header: " 10.1.2.3 , 10.9.9.9", want: "10.1.2.3"},
		{header: "", want: ""},
	}

	for _, tt := range tests {
		if got := firstForwarded(tt.header); got != tt.want {
			t.Errorf("firstForwarded(%q) = %q, want %q", tt.header, got, tt.want)
		}
	}
}

func TestPublicMessageStripsTheSentinelPrefix(t *testing.T) {

	raw := metadata.ErrInvalid.Error() + ": chunk size must be at least 1048576 bytes, got 1"

	if got := publicMessage(errors.New(raw)); got != "chunk size must be at least 1048576 bytes, got 1" {
		t.Errorf("publicMessage() = %q, want the sentinel prefix removed", got)
	}
	if got := publicMessage(errors.New("no prefix here")); got != "no prefix here" {
		t.Errorf("publicMessage() = %q, want it unchanged", got)
	}
}

func TestCapacityResponseUsesCamelCase(t *testing.T) {

	body, err := json.Marshal(pool.Capacity{
		PoolID: "p1", Nodes: 6, RequiredNodes: 6, MissingNodes: 0,
		KnownNodes: 6, TotalBytes: 600, UsedBytes: 210, AvailableBytes: 390,
		LogicalCapacity: 40, Usable: true,
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v, want nil", err)
	}

	for _, key := range []string{
		`"poolId"`, `"requiredNodes"`, `"missingNodes"`, `"knownNodes"`,
		`"totalBytes"`, `"usedBytes"`, `"availableBytes"`, `"logicalCapacity"`, `"usable"`,
	} {
		if !strings.Contains(string(body), key) {
			t.Errorf("capacity JSON %s is missing %s", body, key)
		}
	}
}
