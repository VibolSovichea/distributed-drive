package factory

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/VibolSovichea/distributed-drive/internal/gdrive"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/node"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

const nodeID = "01HQ000000000000000000001"

func googleNode() metadata.Node {
	return metadata.Node{
		ID:       nodeID,
		Name:     "drive-1",
		Provider: metadata.ProviderGoogleDrive,
		Status:   metadata.NodeStatusHealthy,
	}
}

func newFactory(t *testing.T, mutate func(*Options)) *Factory {
	t.Helper()

	dir := t.TempDir()
	opts := Options{
		GoogleClientID:     "client-id",
		GoogleClientSecret: "client-secret",
		TokenKey:           gdrive.KeyFromSecret("a test key"),
		TokenDir:           filepath.Join(dir, "tokens"),
		LocalRoot:          filepath.Join(dir, "nodes"),
	}
	if mutate != nil {
		mutate(&opts)
	}

	f, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

func TestOpenGoogleWithoutACredentialIsUnauthorised(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	_, err := f.Open(context.Background(), googleNode())
	if !errors.Is(err, node.ErrUnauthorized) {
		t.Fatalf("error = %v, want ErrUnauthorized", err)
	}
	if errors.Is(err, node.ErrNoClient) {
		t.Error("an unauthorised account must not also read as a broken configuration")
	}
}

func TestOpenGoogleWithAStoredCredential(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	store, err := f.tokenStore(nodeID)
	if err != nil {
		t.Fatalf("tokenStore: %v", err)
	}
	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken:  "a",
		RefreshToken: "r",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	client, err := f.Open(context.Background(), googleNode())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if client == nil {
		t.Fatal("Open returned no client")
	}
}

func TestOpenGoogleRefusesWithoutAnEncryptionKey(t *testing.T) {
	t.Parallel()

	f := newFactory(t, func(o *Options) { o.TokenKey = nil })

	_, err := f.Open(context.Background(), googleNode())
	if !errors.Is(err, node.ErrNoClient) {
		t.Errorf("error = %v, want ErrNoClient", err)
	}
	if !strings.Contains(err.Error(), "encryption key") {
		t.Errorf("error = %v, want it to name the missing key", err)
	}
}

func TestOpenGoogleRefusesWithoutAnOAuthClient(t *testing.T) {
	t.Parallel()

	f := newFactory(t, func(o *Options) { o.GoogleClientSecret = "" })

	_, err := f.Open(context.Background(), googleNode())
	if !errors.Is(err, node.ErrNoClient) {
		t.Errorf("error = %v, want ErrNoClient", err)
	}
}

func TestOpenGoogleReportsACorruptTokenAsAConfigurationFault(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	store, err := f.tokenStore(nodeID)
	if err != nil {
		t.Fatalf("tokenStore: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(store.(interface{ Path() string }).Path()), 0o700); err != nil {
		t.Fatalf("create the token directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.opts.TokenDir, nodeID+".token"),
		[]byte("not a token"), 0o600); err != nil {
		t.Fatalf("write the bad token: %v", err)
	}

	_, err = f.Open(context.Background(), googleNode())
	if !errors.Is(err, node.ErrNoClient) {
		t.Errorf("error = %v, want ErrNoClient", err)
	}
	if errors.Is(err, node.ErrUnauthorized) {
		t.Error("a corrupt token store must not be reported as an unauthorised account")
	}
}

func TestEachNodeGetsItsOwnTokenFile(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	other := "01HQ000000000000000000002"
	first, err := f.tokenStore(nodeID)
	if err != nil {
		t.Fatalf("tokenStore: %v", err)
	}
	second, err := f.tokenStore(other)
	if err != nil {
		t.Fatalf("tokenStore: %v", err)
	}

	firstPath := first.(interface{ Path() string }).Path()
	secondPath := second.(interface{ Path() string }).Path()

	if firstPath == secondPath {
		t.Errorf("both nodes share the token file %s", firstPath)
	}
}

func TestTokenFileIsNotReadableAsPlaintext(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	store, err := f.tokenStore(nodeID)
	if err != nil {
		t.Fatalf("tokenStore: %v", err)
	}
	if err := store.Save(context.Background(), &oauth2.Token{
		AccessToken:  "a",
		RefreshToken: "the-refresh-token",
		TokenType:    "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	raw, err := os.ReadFile(store.(interface{ Path() string }).Path())
	if err != nil {
		t.Fatalf("read the token: %v", err)
	}
	if strings.Contains(string(raw), "the-refresh-token") {
		t.Error("the refresh token is stored in plaintext")
	}
}

func TestOpenLocalFSCreatesTheNodeDirectory(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	record := metadata.Node{
		ID:       nodeID,
		Name:     "local-1",
		Provider: metadata.ProviderLocalFS,
		Status:   metadata.NodeStatusHealthy,
	}

	client, err := f.Open(context.Background(), record)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := filepath.Join(f.opts.LocalRoot, nodeID)
	if info, err := os.Stat(want); err != nil || !info.IsDir() {
		t.Errorf("the node directory %s was not created: %v", want, err)
	}

	if _, err := client.Upload(context.Background(),
		strings.NewReader("data"), provider.ObjectMetadata{Name: "shard", Size: 4}); err != nil {
		t.Errorf("Upload through the local node: %v", err)
	}
}

func TestOpenLocalFSRefusesWithoutARoot(t *testing.T) {
	t.Parallel()

	f := newFactory(t, func(o *Options) { o.LocalRoot = "" })

	_, err := f.Open(context.Background(), metadata.Node{
		ID: nodeID, Name: "l", Provider: metadata.ProviderLocalFS,
	})
	if !errors.Is(err, node.ErrNoClient) {
		t.Errorf("error = %v, want ErrNoClient", err)
	}
}

func TestOpenRejectsAnUnknownProvider(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	_, err := f.Open(context.Background(), metadata.Node{
		ID: nodeID, Name: "x", Provider: metadata.Provider("dropbox"),
	})
	if !errors.Is(err, node.ErrNoClient) {
		t.Errorf("error = %v, want ErrNoClient", err)
	}
}

func TestNodeIDMustBeAUsablePathSegment(t *testing.T) {
	t.Parallel()

	for _, id := range []string{"", ".", "..", "a/b", `a\b`, "..\\.."} {
		if err := validateIDSegment(id); err == nil {
			t.Errorf("validateIDSegment(%q) accepted it", id)
		}
	}

	if err := validateIDSegment(nodeID); err != nil {
		t.Errorf("validateIDSegment(%q) = %v, want it accepted", nodeID, err)
	}
}

func TestAuthorizeURLRequiresAConfiguredClient(t *testing.T) {
	t.Parallel()

	f := newFactory(t, func(o *Options) { o.GoogleClientID = "" })

	if _, err := f.AuthorizeURL("state"); err == nil {
		t.Error("AuthorizeURL with no client id must fail")
	}
}

func TestAuthorizeURLIsUsable(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	url, err := f.AuthorizeURL("state-abc")
	if err != nil {
		t.Fatalf("AuthorizeURL: %v", err)
	}
	if !strings.Contains(url, "client_id=client-id") {
		t.Errorf("the URL does not carry the client id: %s", url)
	}
	if !strings.Contains(url, "state=state-abc") {
		t.Errorf("the URL does not carry the state: %s", url)
	}
}

func TestStateIsSignedAndCarriesTheNode(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	state, err := f.NewState(nodeID)
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}

	got, err := f.nodeIDFromState(state)
	if err != nil {
		t.Fatalf("nodeIDFromState: %v", err)
	}
	if got != nodeID {
		t.Errorf("node = %q, want %q", got, nodeID)
	}
}

func TestStateCannotBeEditedToNameAnotherNode(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)

	state, err := f.NewState(nodeID)
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}

	raw, err := base64.RawURLEncoding.DecodeString(state)
	if err != nil {
		t.Fatalf("decode the state: %v", err)
	}
	forged := make([]byte, len(raw))
	copy(forged, raw)
	copy(forged[len(statePrefix):], []byte("01HQ000000000000000000002"))

	if _, err := f.nodeIDFromState(base64.RawURLEncoding.EncodeToString(forged)); err == nil {
		t.Fatal("an edited state must not verify")
	}
}

func TestStateFromAnotherKeyDoesNotVerify(t *testing.T) {
	t.Parallel()

	minted := newFactory(t, nil)
	other := newFactory(t, func(o *Options) {
		o.TokenKey = gdrive.KeyFromSecret("a completely different key")
	})

	state, err := minted.NewState(nodeID)
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}

	if _, err := other.nodeIDFromState(state); err == nil {
		t.Error("a state signed with another key must not verify")
	}
}

func TestExchangeRejectsAnUnverifiableStateBeforeAnyNetworkCall(t *testing.T) {
	t.Parallel()

	f := newFactory(t, nil)
	attacker := newFactory(t, func(o *Options) {
		o.TokenKey = gdrive.KeyFromSecret("a completely different key")
	})

	stolen, err := attacker.NewState("01HQ000000000000000000009")
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}

	for _, state := range []string{"forged", "", stolen} {
		if err := f.Exchange(context.Background(), "code", state); err == nil {
			t.Errorf("state %q was accepted", state)
		}
	}

	entries, err := os.ReadDir(f.opts.TokenDir)
	if err == nil {
		for _, entry := range entries {
			t.Errorf("Exchange wrote %s despite an unverifiable state", entry.Name())
		}
	}
}
