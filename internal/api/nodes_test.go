package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/node"
	"github.com/VibolSovichea/distributed-drive/internal/pool"
)

type stubNodes struct {
	register func(context.Context, node.RegisterInput) (metadata.Node, error)
	get      func(context.Context, string) (metadata.Node, error)
	list     func(context.Context) ([]metadata.Node, error)
	delete   func(context.Context, string) error
	check    func(context.Context, string) (metadata.Node, error)
	checkAll func(context.Context) ([]metadata.Node, error)
}

func (s stubNodes) Register(ctx context.Context, in node.RegisterInput) (metadata.Node, error) {
	return s.register(ctx, in)
}
func (s stubNodes) Get(ctx context.Context, id string) (metadata.Node, error) {
	return s.get(ctx, id)
}
func (s stubNodes) List(ctx context.Context) ([]metadata.Node, error) { return s.list(ctx) }
func (s stubNodes) Delete(ctx context.Context, id string) error       { return s.delete(ctx, id) }
func (s stubNodes) Check(ctx context.Context, id string) (metadata.Node, error) {
	return s.check(ctx, id)
}
func (s stubNodes) CheckAll(ctx context.Context) ([]metadata.Node, error) {
	return s.checkAll(ctx)
}

func goodNode() metadata.Node {
	seen := time.Date(2026, time.March, 1, 11, 0, 0, 0, time.UTC)
	return metadata.Node{
		ID:                "01HQ000000000000000000001",
		Name:              "drive-1",
		Provider:          metadata.ProviderGoogleDrive,
		AccountIdentifier: "user-1",
		Status:            metadata.NodeStatusHealthy,
		Capacity:          1000,
		UsedCapacity:      400,
		LastSeen:          &seen,
		CreatedAt:         seen,
		UpdatedAt:         seen,
	}
}

func newNodeServer(t *testing.T, nodes stubNodes) *Server {
	t.Helper()

	srv, err := New(Config{
		Pools:  baseStub(),
		Nodes:  nodes,
		Store:  okPinger{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    func() time.Time { return time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return srv
}

func defaultNodeStub() stubNodes {
	return stubNodes{
		register: func(context.Context, node.RegisterInput) (metadata.Node, error) {
			return metadata.Node{}, neverCalled("Register")
		},
		get: func(context.Context, string) (metadata.Node, error) {
			return metadata.Node{}, neverCalled("Get")
		},
		list: func(context.Context) ([]metadata.Node, error) {
			return nil, neverCalled("List")
		},
		check: func(context.Context, string) (metadata.Node, error) {
			return metadata.Node{}, neverCalled("Check")
		},
		checkAll: func(context.Context) ([]metadata.Node, error) {
			return nil, neverCalled("CheckAll")
		},
	}
}

func TestRegisterNodeReturns201(t *testing.T) {
	stub := defaultNodeStub()
	var seen node.RegisterInput
	stub.register = func(_ context.Context, in node.RegisterInput) (metadata.Node, error) {
		seen = in
		return goodNode(), nil
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodPost, "/api/nodes",
		`{"name":"drive-1","provider":"google_drive"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/nodes = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body)
	}
	if seen.Name != "drive-1" || seen.Provider != metadata.ProviderGoogleDrive {
		t.Errorf("the service received %+v", seen)
	}

	var body nodeResponse
	decode(t, rec, &body)
	if body.ID != goodNode().ID {
		t.Errorf("id = %q, want %q", body.ID, goodNode().ID)
	}
	if body.Status != "healthy" {
		t.Errorf("status = %q, want healthy", body.Status)
	}

	if body.Available != 600 {
		t.Errorf("available = %d, want 600", body.Available)
	}
	if body.LastSeen == "" {
		t.Error("lastSeen is empty, want the check timestamp")
	}
}

func TestRegisterNodeReturns201WhenTheAccountIsUnauthorised(t *testing.T) {
	stub := defaultNodeStub()

	broken := goodNode()
	broken.Status = metadata.NodeStatusAuthError
	broken.AccountIdentifier = ""
	broken.Capacity, broken.UsedCapacity, broken.LastSeen = 0, 0, nil

	stub.register = func(context.Context, node.RegisterInput) (metadata.Node, error) {

		return broken, errors.Join(node.ErrUnauthorized, errors.New("no refresh token"))
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodPost, "/api/nodes",
		`{"name":"drive-1","provider":"google_drive"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/nodes = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body)
	}

	var body nodeResponse
	decode(t, rec, &body)
	if body.Status != "auth_error" {
		t.Errorf("status = %q, want auth_error", body.Status)
	}
	if body.Account != "" {
		t.Errorf("account = %q, want empty for a node that was never reached", body.Account)
	}
}

func TestRegisterNodeValidation(t *testing.T) {
	srv := newNodeServer(t, defaultNodeStub())

	cases := []struct {
		name string
		body string
	}{
		{"not json", `{`},
		{"unknown field", `{"name":"n","provider":"google_drive","nope":1}`},
		{"wrong type", `{"name":1,"provider":"google_drive"}`},
		{"trailing json", `{"name":"n","provider":"google_drive"}{}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, srv, http.MethodPost, "/api/nodes", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body)
			}
			assertErrorCode(t, rec, codeUnprocessableJSON)
		})
	}
}

func TestRegisterNodeRejectsAnUnknownProvider(t *testing.T) {

	stub := defaultNodeStub()
	stub.register = func(context.Context, node.RegisterInput) (metadata.Node, error) {

		return metadata.Node{}, fmt.Errorf("%w: unknown provider %q",
			metadata.ErrInvalid, "dropbox")
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodPost, "/api/nodes", `{"name":"n","provider":"dropbox"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body)
	}
	assertErrorCode(t, rec, codeBadRequest)

	if msg := errorMessage(t, rec); msg != `unknown provider "dropbox"` {
		t.Errorf("message = %q, want the rule that was broken", msg)
	}
}

func errorMessage(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()

	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding %q: %v", rec.Body.String(), err)
	}
	return body.Error.Message
}

func TestListNodes(t *testing.T) {
	stub := defaultNodeStub()
	stub.list = func(context.Context) ([]metadata.Node, error) {
		return []metadata.Node{goodNode()}, nil
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodGet, "/api/nodes", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/nodes = %d, want %d", rec.Code, http.StatusOK)
	}

	var body struct {
		Nodes []nodeResponse `json:"nodes"`
	}
	decode(t, rec, &body)
	if len(body.Nodes) != 1 || body.Nodes[0].ID != goodNode().ID {
		t.Errorf("nodes = %+v, want one", body.Nodes)
	}
}

func TestListNodesEmpty(t *testing.T) {
	stub := defaultNodeStub()
	stub.list = func(context.Context) ([]metadata.Node, error) { return nil, nil }

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodGet, "/api/nodes", "")

	if got := rec.Body.String(); got != `{"nodes":[]}` {
		t.Errorf("body = %s, want an empty array", got)
	}
}

func TestGetNodeNotFound(t *testing.T) {
	stub := defaultNodeStub()
	stub.get = func(context.Context, string) (metadata.Node, error) {
		return metadata.Node{}, metadata.ErrNotFound
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodGet, "/api/nodes/nope", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertErrorCode(t, rec, codeNotFound)
}

func TestNodeStatusReturns200EvenWhenTheNodeIsUnhealthy(t *testing.T) {
	stub := defaultNodeStub()
	offline := goodNode()
	offline.Status = metadata.NodeStatusOffline

	stub.check = func(context.Context, string) (metadata.Node, error) {

		return offline, errors.Join(metadata.ErrNotFound, errors.New("unreachable"))
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodGet, "/api/nodes/"+goodNode().ID+"/status", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body)
	}

	var body nodeResponse
	decode(t, rec, &body)
	if body.Status != "offline" {
		t.Errorf("status = %q, want offline", body.Status)
	}
}

func TestCheckAllNodesReturnsPartialResults(t *testing.T) {
	stub := defaultNodeStub()

	bad := goodNode()
	bad.ID = "01HQ000000000000000000002"
	bad.Status = metadata.NodeStatusOffline

	stub.checkAll = func(context.Context) ([]metadata.Node, error) {
		return []metadata.Node{goodNode(), bad},
			errors.Join(errors.New("drive-1: unreachable"))
	}

	srv := newNodeServer(t, stub)
	rec := do(t, srv, http.MethodPost, "/api/nodes/check", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body)
	}

	var body struct {
		Nodes []nodeResponse `json:"nodes"`
	}
	decode(t, rec, &body)
	if len(body.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(body.Nodes))
	}
	if body.Nodes[1].Status != "offline" {
		t.Errorf("second node status = %q, want offline", body.Nodes[1].Status)
	}
}

func TestAttachNodeToPool(t *testing.T) {
	pools := baseStub()
	var gotPool, gotNode string
	pools.add = func(_ context.Context, poolID, nodeID string) (metadata.Node, error) {
		gotPool, gotNode = poolID, nodeID
		return goodNode(), nil
	}
	pools.cap = func(context.Context, string) (pool.Capacity, error) {
		return pool.Capacity{PoolID: "p", Nodes: 1, RequiredNodes: 6, MissingNodes: 5}, nil
	}

	srv, err := New(Config{
		Pools:  pools,
		Nodes:  defaultNodeStub(),
		Store:  okPinger{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rec := do(t, srv, http.MethodPost, "/api/pools/01HQ0000000000000000000AA/nodes",
		`{"nodeId":"01HQ000000000000000000001"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body)
	}
	if gotNode != "01HQ000000000000000000001" || gotPool == "" {
		t.Errorf("the service received pool=%q node=%q", gotPool, gotNode)
	}

	var body struct {
		Node     nodeResponse  `json:"node"`
		Capacity pool.Capacity `json:"capacity"`
	}
	decode(t, rec, &body)
	if body.Node.ID != goodNode().ID {
		t.Errorf("node id = %q", body.Node.ID)
	}
	if body.Capacity.MissingNodes != 5 {
		t.Errorf("missing nodes = %d, want 5", body.Capacity.MissingNodes)
	}
}

func TestDetachNodeHoldingDataIs409(t *testing.T) {
	pools := baseStub()
	pools.detach = func(context.Context, string, string) error {
		return errors.Join(pool.ErrHasData, errors.New("node holds 12 chunk(s)"))
	}

	srv, err := New(Config{
		Pools:  pools,
		Nodes:  defaultNodeStub(),
		Store:  okPinger{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rec := do(t, srv, http.MethodDelete,
		"/api/pools/01HQ0000000000000000000AA/nodes/01HQ000000000000000000001", "")

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body)
	}
	assertErrorCode(t, rec, codeConflict)
}

func TestListPoolNodes(t *testing.T) {
	pools := baseStub()
	pools.nodes = func(context.Context, string) ([]metadata.Node, error) {
		return []metadata.Node{goodNode()}, nil
	}

	srv, err := New(Config{
		Pools:  pools,
		Nodes:  defaultNodeStub(),
		Store:  okPinger{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rec := do(t, srv, http.MethodGet, "/api/pools/01HQ0000000000000000000AA/nodes", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body)
	}

	var body struct {
		Nodes []nodeResponse `json:"nodes"`
	}
	decode(t, rec, &body)
	if len(body.Nodes) != 1 {
		t.Errorf("nodes = %+v, want one", body.Nodes)
	}
}

func TestNodeRoutesAreAbsentWithoutTheService(t *testing.T) {

	srv := newStubServer(t, nil, baseStub())

	for _, target := range []string{"/api/nodes", "/api/pools/01HQ0000000000000000000AA/nodes"} {
		rec := do(t, srv, http.MethodGet, target, "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want %d", target, rec.Code, http.StatusNotFound)
		}
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding %q: %v", rec.Body.String(), err)
	}
	if body.Error.Code != want {
		t.Errorf("error code = %q, want %q", body.Error.Code, want)
	}
	if body.Error.Message == "" {
		t.Error("the error envelope has no message")
	}
}
