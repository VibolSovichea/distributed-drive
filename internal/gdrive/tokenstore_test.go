package gdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func testKey() []byte { return KeyFromSecret("a test encryption secret") }

func newTestStore(t *testing.T) *FileTokenStore {
	t.Helper()

	store, err := NewFileTokenStore(filepath.Join(t.TempDir(), "nested", "token.enc"), testKey())
	if err != nil {
		t.Fatalf("NewFileTokenStore: %v", err)
	}
	return store
}

func TestFileTokenStoreRoundTrips(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	want := &oauth2.Token{
		AccessToken:  "access-1",
		RefreshToken: "refresh-1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour).Round(time.Second),
	}

	if err := store.Save(context.Background(), want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.AccessToken != want.AccessToken || got.RefreshToken != want.RefreshToken {
		t.Errorf("loaded token = %+v, want %+v", got, want)
	}
	if !got.Expiry.Equal(want.Expiry) {
		t.Errorf("expiry = %v, want %v", got.Expiry, want.Expiry)
	}
}

func TestFileTokenStoreEncryptsAtRest(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	const secret = "a-very-recognisable-refresh-token-value"

	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken:  "access-1",
		RefreshToken: secret,
		TokenType:    "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	raw, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read the store: %v", err)
	}

	if bytes.Contains(raw, []byte(secret)) {
		t.Error("the refresh token appears in plaintext in the token store")
	}
	if bytes.Contains(raw, []byte("access-1")) {
		t.Error("the access token appears in plaintext in the token store")
	}
}

func TestFileTokenStorePermissions(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken: "a", RefreshToken: "r", TokenType: "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(store.Path())
	if err != nil {
		t.Fatalf("stat the store: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("token file mode = %o, want 600", perm)
	}

	dirInfo, err := os.Stat(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatalf("stat the directory: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0o700 {
		t.Errorf("token directory mode = %o, want 700", perm)
	}
}

func TestFileTokenStoreNoToken(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	_, err := store.Load(context.Background())
	if !errors.Is(err, ErrNoToken) {
		t.Fatalf("error = %v, want ErrNoToken", err)
	}
}

func TestFileTokenStoreWrongKey(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "token.enc")

	first, err := NewFileTokenStore(path, testKey())
	if err != nil {
		t.Fatalf("NewFileTokenStore: %v", err)
	}
	if err := first.Save(context.Background(), &oauth2.Token{
		AccessToken: "a", RefreshToken: "r", TokenType: "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rotated, err := NewFileTokenStore(path, KeyFromSecret("a different secret"))
	if err != nil {
		t.Fatalf("NewFileTokenStore: %v", err)
	}
	if _, err := rotated.Load(context.Background()); err == nil {
		t.Fatal("loading with the wrong key must fail")
	} else if errors.Is(err, ErrNoToken) {
		t.Error("a wrong key must not be reported as a missing token")
	}
}

func TestFileTokenStoreTruncatedFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken: "a", RefreshToken: "r", TokenType: "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := os.WriteFile(store.Path(), []byte("short"), 0o600); err != nil {
		t.Fatalf("truncate the store: %v", err)
	}

	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("a truncated token store must not load")
	}
}

func TestFileTokenStoreForeignFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	if err := os.WriteFile(store.Path(), bytes.Repeat([]byte("x"), 200), 0o600); err != nil {
		t.Fatalf("write the foreign file: %v", err)
	}

	_, err := store.Load(context.Background())
	if err == nil {
		t.Fatal("a foreign file must not load")
	}
	if !strings.Contains(err.Error(), "unrecognised format") {
		t.Errorf("error = %v, want it to name the format problem", err)
	}
}

func TestFileTokenStoreDelete(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken: "a", RefreshToken: "r", TokenType: "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := store.Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrNoToken) {
		t.Errorf("after Delete, Load error = %v, want ErrNoToken", err)
	}

	if err := store.Delete(context.Background()); err != nil {
		t.Errorf("a repeated Delete must succeed: %v", err)
	}
}

func TestFileTokenStoreRejectsEmptyToken(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	if err := store.Save(context.Background(), nil); err == nil {
		t.Error("saving a nil token must fail")
	}
	if err := store.Save(context.Background(), &oauth2.Token{}); err == nil {
		t.Error("saving an empty token must fail")
	}
}

func TestNewFileTokenStoreValidatesKey(t *testing.T) {
	t.Parallel()

	if _, err := NewFileTokenStore(filepath.Join(t.TempDir(), "t"), []byte("too short")); err == nil {
		t.Error("a short key must be rejected")
	}
	if _, err := NewFileTokenStore("", testKey()); err == nil {
		t.Error("an empty path must be rejected")
	}
}

func TestFileTokenStoreSaveIsAtomic(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken: "first", RefreshToken: "r", TokenType: "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken: "second", RefreshToken: "r", TokenType: "Bearer",
	}); err != nil {
		t.Fatalf("Save again: %v", err)
	}

	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.AccessToken != "second" {
		t.Errorf("access token = %q, want the second one", got.AccessToken)
	}

	entries, err := os.ReadDir(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("directory holds %v, want only the token file", names)
	}
}

type stubStore struct {
	mu      sync.Mutex
	token   *oauth2.Token
	saves   int
	saveErr error
	loadErr error
}

func (s *stubStore) Load(context.Context) (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.token, nil
}

func (s *stubStore) Save(_ context.Context, token *oauth2.Token) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saves++
	s.token = token
	return nil
}

func (s *stubStore) Delete(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = nil
	return nil
}

func (s *stubStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

type fakeTokenServer struct {
	*httptest.Server

	refreshed string

	includeRefreshToken bool
}

func newRefreshServer(t *testing.T, accessToken string, includeRefreshToken bool) *fakeTokenServer {
	t.Helper()

	fake := &fakeTokenServer{refreshed: accessToken, includeRefreshToken: includeRefreshToken}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{
			"access_token": fake.refreshed,
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		if fake.includeRefreshToken {
			body["refresh_token"] = "a-brand-new-refresh-token"
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(fake.Close)

	return fake
}

func (f *fakeTokenServer) authConfig() AuthConfig {
	return AuthConfig{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		endpoint: oauth2.Endpoint{
			AuthURL:  f.URL + "/auth",
			TokenURL: f.URL + "/token",
		},
	}
}

func TestTokenSourceReusesValidToken(t *testing.T) {
	t.Parallel()

	store := &stubStore{token: &oauth2.Token{
		AccessToken:  "still-good",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
	}}

	src := NewTokenSource(context.Background(), AuthConfig{
		ClientID: "id", ClientSecret: "secret",
	}, store)

	token, err := src.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token.AccessToken != "still-good" {
		t.Errorf("access token = %q, want the stored one", token.AccessToken)
	}
	if store.saveCount() != 0 {
		t.Error("a valid token must not trigger a refresh or a write")
	}
}

func TestTokenSourceRefreshesNearExpiryAndPreservesRefreshToken(t *testing.T) {
	t.Parallel()

	srv := newRefreshServer(t, "access-2", false)

	store := &stubStore{token: &oauth2.Token{
		AccessToken:  "access-1",
		RefreshToken: "the-only-refresh-token",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(30 * time.Second),
	}}

	src := NewTokenSource(context.Background(), srv.authConfig(), store)

	token, err := src.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token.AccessToken != "access-2" {
		t.Errorf("access token = %q, want the refreshed one", token.AccessToken)
	}

	if token.RefreshToken != "the-only-refresh-token" {
		t.Errorf("refresh token = %q, want the stored one carried forward", token.RefreshToken)
	}
	if store.saveCount() != 1 {
		t.Errorf("saves = %d, want the refreshed token to be persisted once", store.saveCount())
	}
}

func TestTokenSourceWithoutRefreshTokenFails(t *testing.T) {
	t.Parallel()

	store := &stubStore{token: &oauth2.Token{
		AccessToken: "expired",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(-time.Hour),
	}}

	src := NewTokenSource(context.Background(), AuthConfig{
		ClientID: "id", ClientSecret: "secret",
	}, store)

	_, err := src.Token()
	if err == nil {
		t.Fatal("an unrenewable expired token must fail")
	}
	if !errors.Is(err, ErrNoToken) {
		t.Errorf("error = %v, want it to wrap ErrNoToken", err)
	}
}

func TestTokenSourceMissingToken(t *testing.T) {
	t.Parallel()

	store := &stubStore{loadErr: ErrNoToken}
	src := NewTokenSource(context.Background(), AuthConfig{
		ClientID: "id", ClientSecret: "secret",
	}, store)

	if _, err := src.Token(); !errors.Is(err, ErrNoToken) {
		t.Errorf("error = %v, want ErrNoToken", err)
	}
}

func TestTokenSourceTokenWithoutExpiryIsUsed(t *testing.T) {
	t.Parallel()

	store := &stubStore{token: &oauth2.Token{
		AccessToken:  "no-expiry",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
	}}

	src := NewTokenSource(context.Background(), AuthConfig{
		ClientID: "id", ClientSecret: "secret",
	}, store)

	token, err := src.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token.AccessToken != "no-expiry" {
		t.Errorf("access token = %q, want the stored one", token.AccessToken)
	}
}

func TestDueForRefresh(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		token *oauth2.Token
		want  bool
	}{
		{"nil", nil, true},
		{"no expiry", &oauth2.Token{AccessToken: "a"}, false},
		{"far future", &oauth2.Token{Expiry: time.Now().Add(time.Hour)}, false},
		{"expired", &oauth2.Token{Expiry: time.Now().Add(-time.Hour)}, true},
		{"inside the refresh window", &oauth2.Token{Expiry: time.Now().Add(time.Minute)}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dueForRefresh(tc.token); got != tc.want {
				t.Errorf("dueForRefresh = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAuthorizeRequestsOfflineAccess(t *testing.T) {
	t.Parallel()

	url, err := Authorize(AuthConfig{ClientID: "id", ClientSecret: "secret"}, "state-123")
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	for _, want := range []string{
		"access_type=offline",
		"state=state-123",
		"response_type=code",
		"redirect_uri=http%3A%2F%2F127.0.0.1%3A8088%2Foauth%2Fcallback",
		"scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive",
	} {
		if !strings.Contains(url, want) {
			t.Errorf("the authorisation URL is missing %q\ngot %s", want, url)
		}
	}
}

func TestAuthorizeValidatesInput(t *testing.T) {
	t.Parallel()

	if _, err := Authorize(AuthConfig{}, "state"); err == nil {
		t.Error("a missing client id must be rejected")
	}
	if _, err := Authorize(AuthConfig{ClientID: "id"}, "state"); err == nil {
		t.Error("a missing client secret must be rejected")
	}
	if _, err := Authorize(AuthConfig{ClientID: "id", ClientSecret: "s"}, ""); err == nil {
		t.Error("an empty state must be rejected")
	}
}

func TestExchangeRequiresCode(t *testing.T) {
	t.Parallel()

	_, err := Exchange(context.Background(),
		AuthConfig{ClientID: "id", ClientSecret: "secret"}, "  ", "s")
	if err == nil {
		t.Fatal("an empty code must be rejected")
	}
}

func TestNewStateIsRandomAndURLSafe(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		state, err := NewState()
		if err != nil {
			t.Fatalf("NewState: %v", err)
		}
		if state == "" {
			t.Fatal("NewState returned an empty value")
		}
		if seen[state] {
			t.Fatal("NewState repeated a value")
		}
		seen[state] = true

		if strings.ContainsAny(state, "+/=&") {
			t.Fatalf("state %q needs URL escaping", state)
		}
	}
}
