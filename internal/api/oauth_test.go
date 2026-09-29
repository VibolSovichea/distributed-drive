package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/VibolSovichea/distributed-drive/internal/authflow"
)

type fakeOAuth struct {
	startErr     error
	completeErr  error
	startedFor   string
	completedAs  string
	completedSt  string
	completeCall int
}

func (f *fakeOAuth) Start(_ context.Context, nodeID string) (string, error) {
	if f.startErr != nil {
		return "", f.startErr
	}
	f.startedFor = nodeID
	return "https://provider.example/authorize?state=abc-" + nodeID, nil
}

func (f *fakeOAuth) Complete(_ context.Context, code, state string) error {
	f.completeCall++
	f.completedAs = code
	f.completedSt = state
	return f.completeErr
}

func newOAuthServer(t *testing.T, oauth OAuthService) http.Handler {
	t.Helper()

	srv, err := New(Config{
		Pools:  baseStub(),
		Nodes:  defaultNodeStub(),
		OAuth:  oauth,
		Store:  okPinger{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv.Handler()
}

func doGet(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func doPost(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the response: %v (body %s)", err, rec.Body.String())
	}
	return body
}

func TestAuthorizeStartsAFlow(t *testing.T) {
	t.Parallel()

	oauth := &fakeOAuth{}
	h := newOAuthServer(t, oauth)

	rec := doPost(t, h, "/api/nodes/01HQ000000000000000000001/authorize")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}

	body := decodeBody(t, rec)
	raw, _ := body["authorizationUrl"].(string)
	if raw == "" {
		t.Fatal("the response carried no authorizationUrl")
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("the URL does not parse: %v", err)
	}
	if parsed.Scheme != "https" {
		t.Errorf("the URL scheme is %q, want https", parsed.Scheme)
	}
	if oauth.startedFor != "01HQ000000000000000000001" {
		t.Errorf("started for %q, want the node in the path", oauth.startedFor)
	}
	if body["nodeId"] != "01HQ000000000000000000001" {
		t.Errorf("nodeId = %v, want the node in the path", body["nodeId"])
	}
}

func TestOAuthCallbackCompletesTheFlow(t *testing.T) {
	t.Parallel()

	oauth := &fakeOAuth{}
	h := newOAuthServer(t, oauth)

	rec := doGet(t, h, "/api/oauth/callback?code=the-code&state=the-state")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if oauth.completedAs != "the-code" || oauth.completedSt != "the-state" {
		t.Errorf("completed with %q/%q, want the-code/the-state",
			oauth.completedAs, oauth.completedSt)
	}
}

func TestOAuthCallbackRejectsMissingParameters(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/api/oauth/callback",
		"/api/oauth/callback?code=the-code",
		"/api/oauth/callback?state=the-state",
	} {
		oauth := &fakeOAuth{}
		h := newOAuthServer(t, oauth)

		rec := doGet(t, h, path)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", path, rec.Code)
		}
		if oauth.completeCall != 0 {
			t.Errorf("%s: the flow was completed %d times, want 0", path, oauth.completeCall)
		}
	}
}

func TestOAuthCallbackReportsAProviderRefusal(t *testing.T) {
	t.Parallel()

	oauth := &fakeOAuth{}
	h := newOAuthServer(t, oauth)

	rec := doGet(t, h, "/api/oauth/callback?error=access_denied&state=the-state")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	if oauth.completeCall != 0 {
		t.Error("a refusal must not be treated as a successful completion")
	}

	body := decodeBody(t, rec)
	errBody, _ := body["error"].(map[string]any)
	msg, _ := errBody["message"].(string)
	if msg == "" {
		t.Error("the response carried no message")
	}
}

func TestOAuthCallbackMapsFlowErrorsToStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"used or unknown", authflow.ErrUnknownState, http.StatusGone},
		{"expired", authflow.ErrStateExpired, http.StatusGone},
		{"wrong node", authflow.ErrStateMismatch, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newOAuthServer(t, &fakeOAuth{completeErr: tc.err})

			rec := doGet(t, h, "/api/oauth/callback?code=c&state=s")
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestOAuthCallbackHidesAnUnexpectedFailure(t *testing.T) {
	t.Parallel()

	h := newOAuthServer(t, &fakeOAuth{completeErr: errors.New("token store: disk is full")})

	rec := doGet(t, h, "/api/oauth/callback?code=c&state=s")
	if rec.Code < 500 {
		t.Fatalf("status = %d, want a server error", rec.Code)
	}

	if body := rec.Body.String(); strings.Contains(body, "disk is full") {
		t.Errorf("the internal error leaked to the client: %s", body)
	}
}

func TestProviderErrorIsSanitised(t *testing.T) {
	t.Parallel()

	h := newOAuthServer(t, &fakeOAuth{})

	rec := doGet(t, h, "/api/oauth/callback?error="+url.QueryEscape("a b <script>/etc/passwd"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "<script>") {
		t.Errorf("the provider value was echoed unescaped: %s", rec.Body.String())
	}
}

func TestOAuthRoutesAreAbsentWithoutAService(t *testing.T) {
	t.Parallel()

	srv, err := New(Config{
		Pools:  baseStub(),
		Store:  okPinger{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := srv.Handler()

	for _, path := range []string{
		"/api/oauth/callback?code=c&state=s",
		"/api/nodes/01HQ000000000000000000001/authorize",
	} {
		method := http.MethodGet
		if path == "/api/nodes/01HQ000000000000000000001/authorize" {
			method = http.MethodPost
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))

		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404", path, rec.Code)
		}
	}
}

func TestSanitiseProviderError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want string }{
		{"access_denied", "access_denied"},
		{"a b", "a b"},
		{"<script>", " script "},
		{"", "no reason given"},
		{"%%%", "   "},
	} {
		if got := sanitiseProviderError(tc.in); got != tc.want {
			t.Errorf("sanitise(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	if got := sanitiseProviderError(string(make([]byte, 200))); len(got) > 64 {
		t.Errorf("length = %d, want it capped at 64", len(got))
	}
}
