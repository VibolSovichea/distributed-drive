package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VibolSovichea/distributed-drive/internal/pool"
)

func TestCreatePoolReturns201AndTheStoredPool(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodPost, "/api/pools", validPoolBody)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/pools = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body)
	}

	var body poolResponse
	decode(t, rec, &body)

	if body.ID == "" {
		t.Error("id is empty, want a server-generated identifier")
	}
	if body.Name != "primary" {
		t.Errorf("name = %q, want %q", body.Name, "primary")
	}
	if body.ShardsPerStripe != 6 {
		t.Errorf("shardsPerStripe = %d, want 6", body.ShardsPerStripe)
	}
	if body.Redundancy != "4+2" {
		t.Errorf("redundancy = %q, want %q", body.Redundancy, "4+2")
	}
	if body.CreatedAt == "" || body.UpdatedAt == "" {
		t.Errorf("timestamps = %q/%q, want both set", body.CreatedAt, body.UpdatedAt)
	}
	if want := "/api/pools/" + body.ID; rec.Header().Get("Location") != want {
		t.Errorf("Location = %q, want %q", rec.Header().Get("Location"), want)
	}
}

func TestCreateThenGetThenListThenDelete(t *testing.T) {
	srv := newTestServer(t)

	created := decodePool(t, do(t, srv, http.MethodPost, "/api/pools", validPoolBody))

	rec := do(t, srv, http.MethodGet, "/api/pools/"+created.ID, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/pools/%s = %d, want %d", created.ID, rec.Code, http.StatusOK)
	}
	got := decodePool(t, rec)
	if got != created {
		t.Errorf("GET returned %+v, want %+v", got, created)
	}

	rec = do(t, srv, http.MethodGet, "/api/pools", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/pools = %d, want %d", rec.Code, http.StatusOK)
	}
	var list listPoolsResponse
	decode(t, rec, &list)
	if list.Count != 1 || len(list.Pools) != 1 {
		t.Fatalf("list = %d pools, want 1", list.Count)
	}
	if list.Pools[0].ID != created.ID {
		t.Errorf("listed %s, want %s", list.Pools[0].ID, created.ID)
	}

	rec = do(t, srv, http.MethodDelete, "/api/pools/"+created.ID, "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE /api/pools/%s = %d, want %d", created.ID, rec.Code, http.StatusNoContent)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("DELETE body = %q, want empty", rec.Body.String())
	}

	rec = do(t, srv, http.MethodGet, "/api/pools/"+created.ID, "")
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET after delete = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestListPoolsIsAnEmptyArrayNotNull(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/api/pools", "")

	if !strings.Contains(rec.Body.String(), `"pools":[]`) {
		t.Errorf("body = %q, want an empty array", rec.Body.String())
	}
}

func TestCreatePoolRejectsABadBody(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode int
		wantErr  string
	}{
		{
			name:     "no name",
			body:     `{"dataChunks":4,"parityChunks":2,"chunkSize":8388608}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "pool name must not be empty",
		},
		{
			name:     "zero data chunks",
			body:     `{"name":"p","dataChunks":0,"parityChunks":2,"chunkSize":8388608}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "data chunks must be at least 1",
		},
		{
			name:     "chunk size too small",
			body:     `{"name":"p","dataChunks":4,"parityChunks":2,"chunkSize":1}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "chunk size must be at least",
		},
		{
			name:     "wrong type",
			body:     `{"name":"p","dataChunks":"four","chunkSize":8388608}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "must be a",
		},
		{
			name:     "unknown field",
			body:     `{"name":"p","dataChunks":4,"chunkSize":8388608,"chunkSizes":9}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "unknown field",
		},
		{
			name:     "not json",
			body:     `not json at all`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid request body",
		},
		{
			name:     "trailing object",
			body:     `{"name":"p","dataChunks":4,"chunkSize":8388608}{"name":"q"}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "single JSON object",
		},
		{
			name:     "empty body",
			body:     `{}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "pool name must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t)

			rec := do(t, srv, http.MethodPost, "/api/pools", tt.body)

			if rec.Code != tt.wantCode {
				t.Fatalf("POST /api/pools = %d, want %d: %s", rec.Code, tt.wantCode, rec.Body)
			}

			var body errorBody
			decode(t, rec, &body)

			if !strings.Contains(body.Error.Message, tt.wantErr) {
				t.Errorf("message = %q, want it to contain %q", body.Error.Message, tt.wantErr)
			}
		})
	}
}

func TestCreatePoolRejectsADuplicateName(t *testing.T) {
	srv := newTestServer(t)

	if rec := do(t, srv, http.MethodPost, "/api/pools", validPoolBody); rec.Code != http.StatusCreated {
		t.Fatalf("first POST = %d, want %d", rec.Code, http.StatusCreated)
	}

	rec := do(t, srv, http.MethodPost, "/api/pools", validPoolBody)

	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate POST = %d, want %d", rec.Code, http.StatusConflict)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codeConflict {
		t.Errorf("code = %q, want %q", body.Error.Code, codeConflict)
	}
}

func TestCreatePoolRejectsAnOversizedBody(t *testing.T) {
	srv := newTestServer(t)

	huge := `{"name":"` + strings.Repeat("x", maxRequestBody) + `"}`
	rec := do(t, srv, http.MethodPost, "/api/pools", huge)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized POST = %d, want %d: %s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codePayloadTooLarge {
		t.Errorf("code = %q, want %q", body.Error.Code, codePayloadTooLarge)
	}
}

func TestGetPoolRejectsAMalformedIdentifier(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/api/pools/not-a-ulid", "")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("GET /api/pools/not-a-ulid = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetMissingPoolIsA404(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/api/pools/01J00000000000000000000000", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing pool = %d, want %d", rec.Code, http.StatusNotFound)
	}

	var body errorBody
	decode(t, rec, &body)

	if body.Error.Code != codeNotFound {
		t.Errorf("code = %q, want %q", body.Error.Code, codeNotFound)
	}

	if body.Error.Message != "the requested resource does not exist" {
		t.Errorf("message = %q, want a generic not-found message", body.Error.Message)
	}
}

func TestDeleteOfAMissingPoolIsA404(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodDelete, "/api/pools/01J00000000000000000000000", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE missing pool = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestDeleteRefusesAPoolThatStillHoldsFiles(t *testing.T) {
	srv, store := newTestServerAndStore(t)

	created := decodePool(t, do(t, srv, http.MethodPost, "/api/pools", validPoolBody))

	mustCreateFile(t, store, created.ID, "report.pdf")

	rec := do(t, srv, http.MethodDelete, "/api/pools/"+created.ID, "")

	if rec.Code != http.StatusConflict {
		t.Fatalf("DELETE non-empty pool = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body)
	}

	var body errorBody
	decode(t, rec, &body)

	if !strings.Contains(body.Error.Message, "still contains files") {
		t.Errorf("message = %q, want it to explain the guard", body.Error.Message)
	}

	if rec := do(t, srv, http.MethodGet, "/api/pools/"+created.ID, ""); rec.Code != http.StatusOK {
		t.Errorf("pool after refused delete = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestPoolCapacity(t *testing.T) {
	srv := newTestServer(t)

	created := decodePool(t, do(t, srv, http.MethodPost, "/api/pools", validPoolBody))

	rec := do(t, srv, http.MethodGet, "/api/pools/"+created.ID+"/capacity", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET capacity = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body)
	}

	var body pool.Capacity
	decode(t, rec, &body)

	if body.PoolID != created.ID {
		t.Errorf("poolId = %q, want %q", body.PoolID, created.ID)
	}

	if body.RequiredNodes != 6 || body.MissingNodes != 6 {
		t.Errorf("nodes = %d required / %d missing, want 6/6", body.RequiredNodes, body.MissingNodes)
	}
	if body.Usable {
		t.Error("usable = true, want false for a pool with no nodes")
	}
}

func TestCapacityOfAMissingPoolIsA404(t *testing.T) {
	srv := newTestServer(t)

	rec := do(t, srv, http.MethodGet, "/api/pools/01J00000000000000000000000/capacity", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET capacity of missing pool = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func decodePool(t *testing.T, rec *httptest.ResponseRecorder) poolResponse {
	t.Helper()

	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body)
	}

	var body poolResponse
	decode(t, rec, &body)
	return body
}
