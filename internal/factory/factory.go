package factory

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/oauth2"

	"github.com/VibolSovichea/distributed-drive/internal/gdrive"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/node"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
	"github.com/VibolSovichea/distributed-drive/internal/provider/localfs"
)

type Options struct {
	GoogleClientID     string
	GoogleClientSecret string

	GoogleRedirectURI string

	TokenKey []byte

	TokenDir string

	LocalRoot string

	Logger *slog.Logger
}

type Factory struct {
	opts Options

	mu          sync.Mutex
	tokenStores map[string]gdrive.TokenStore
	localRoots  map[string]string

	stateKey []byte
}

var _ node.Factory = (*Factory)(nil)

func New(opts Options) (*Factory, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	return &Factory{
		opts:        opts,
		tokenStores: make(map[string]gdrive.TokenStore),
		localRoots:  make(map[string]string),
		stateKey:    opts.TokenKey,
	}, nil
}

func (f *Factory) Open(ctx context.Context, n metadata.Node) (provider.StorageNode, error) {
	switch n.Provider {
	case metadata.ProviderGoogleDrive:
		return f.openDrive(ctx, n)
	case metadata.ProviderLocalFS:
		return f.openLocalFS(n)
	default:
		return nil, fmt.Errorf("%w: no client for provider %q", node.ErrNoClient, n.Provider)
	}
}

func (f *Factory) NewState(nodeID string) (string, error) {
	if err := validateIDSegment(nodeID); err != nil {
		return "", err
	}
	if len(f.stateKey) == 0 {
		return "", fmt.Errorf("%w: no token encryption key is configured, "+
			"so an authorisation state cannot be signed", node.ErrNoClient)
	}

	body := statePrefix + nodeID
	mac := computeStateMAC(f.stateKey, body)

	return base64.RawURLEncoding.EncodeToString(append([]byte(body), mac[:stateMACLen]...)), nil
}

func (f *Factory) NodeOf(state string) (string, bool) {
	nodeID, err := f.nodeIDFromState(state)
	if err != nil {
		return "", false
	}
	return nodeID, true
}

func (f *Factory) AuthorizeURL(state string) (string, error) {
	return gdrive.Authorize(f.authConfig(), state)
}

func (f *Factory) Exchange(ctx context.Context, code, state string) error {

	nodeID, err := f.nodeIDFromState(state)
	if err != nil {
		return err
	}

	store, err := f.tokenStore(nodeID)
	if err != nil {
		return err
	}

	token, err := gdrive.Exchange(ctx, f.authConfig(), code, state)
	if err != nil {
		return err
	}

	if err := store.Save(ctx, token); err != nil {
		return err
	}

	f.mu.Lock()
	delete(f.tokenStores, nodeID)
	f.mu.Unlock()

	return nil
}

func (f *Factory) nodeIDFromState(state string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(state)
	if err != nil {
		return "", fmt.Errorf("%w: the authorisation state is not readable", node.ErrNoClient)
	}

	if len(raw) <= len(statePrefix)+stateMACLen || string(raw[:len(statePrefix)]) != statePrefix {
		return "", fmt.Errorf("%w: the authorisation state is malformed", node.ErrNoClient)
	}

	nodeID := string(raw[len(statePrefix) : len(raw)-stateMACLen])
	got := raw[len(raw)-stateMACLen:]

	want := computeStateMAC(f.stateKey, statePrefix+nodeID)
	if subtle.ConstantTimeCompare(want[:stateMACLen], got) != 1 {

		return "", fmt.Errorf("%w: the authorisation state does not verify", node.ErrNoClient)
	}

	return nodeID, nil
}

const (
	statePrefix = "v1"
	stateMACLen = 16
)

func computeStateMAC(key []byte, body string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("authflow-state-v1"))
	mac.Write([]byte(body))
	return mac.Sum(nil)
}

func (f *Factory) authConfig() gdrive.AuthConfig {
	return gdrive.AuthConfig{
		ClientID:     f.opts.GoogleClientID,
		ClientSecret: f.opts.GoogleClientSecret,
		RedirectURI:  f.opts.GoogleRedirectURI,
	}
}

func (f *Factory) openDrive(ctx context.Context, n metadata.Node) (provider.StorageNode, error) {
	if f.opts.TokenKey == nil || len(f.opts.TokenKey) == 0 {

		return nil, fmt.Errorf("%w: no token encryption key is configured, "+
			"so a Google credential cannot be stored safely", node.ErrNoClient)
	}
	if !f.googleConfigured() {
		return nil, fmt.Errorf("%w: the Google OAuth client id and secret are not configured",
			node.ErrNoClient)
	}

	store, err := f.tokenStore(n.ID)
	if err != nil {
		return nil, err
	}

	if _, loadErr := store.Load(ctx); loadErr != nil {
		if errors.Is(loadErr, gdrive.ErrNoToken) {

			return nil, fmt.Errorf("%w: %s has no stored credential: %w",
				node.ErrUnauthorized, n.Name, loadErr)
		}

		return nil, fmt.Errorf("%w: read the stored credential: %w", node.ErrNoClient, loadErr)
	}

	source := gdrive.NewTokenSource(ctx, f.authConfig(), store)

	client, err := gdrive.New(gdrive.Options{
		TokenSource: source,
		Account:     n.Name,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: build a Drive client: %w", node.ErrNoClient, err)
	}

	return client, nil
}

func (f *Factory) googleConfigured() bool {
	return strings.TrimSpace(f.opts.GoogleClientID) != "" &&
		strings.TrimSpace(f.opts.GoogleClientSecret) != ""
}

func (f *Factory) tokenStore(nodeID string) (gdrive.TokenStore, error) {
	if strings.TrimSpace(f.opts.TokenDir) == "" {
		return nil, fmt.Errorf("%w: no token directory is configured", node.ErrNoClient)
	}
	if err := validateIDSegment(nodeID); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if store, ok := f.tokenStores[nodeID]; ok {
		return store, nil
	}

	path := filepath.Join(f.opts.TokenDir, nodeID+".token")

	store, err := gdrive.NewFileTokenStore(path, f.opts.TokenKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", node.ErrNoClient, err)
	}

	f.tokenStores[nodeID] = store
	return store, nil
}

func (f *Factory) openLocalFS(n metadata.Node) (provider.StorageNode, error) {
	root, err := f.localRoot(n)
	if err != nil {
		return nil, err
	}

	client, err := localfs.New(localfs.Config{Root: root, AccountID: n.ID})
	if err != nil {
		return nil, fmt.Errorf("%w: open a local node: %w", node.ErrNoClient, err)
	}

	return client, nil
}

func (f *Factory) localRoot(n metadata.Node) (string, error) {
	if strings.TrimSpace(f.opts.LocalRoot) == "" {
		return "", fmt.Errorf("%w: no local node root is configured", node.ErrNoClient)
	}
	if err := validateIDSegment(n.ID); err != nil {
		return "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if root, ok := f.localRoots[n.ID]; ok {
		return root, nil
	}

	root := filepath.Join(f.opts.LocalRoot, n.ID)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("%w: create the node directory: %w", node.ErrNoClient, err)
	}

	f.localRoots[n.ID] = root
	return root, nil
}

func validateIDSegment(id string) error {
	if id == "" {
		return fmt.Errorf("%w: the node id is empty", node.ErrNoClient)
	}
	if strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return fmt.Errorf("%w: node id %q is not a usable path segment", node.ErrNoClient, id)
	}
	return nil
}

func (f *Factory) TokenSourceFor(ctx context.Context, nodeID string) (oauth2.TokenSource, error) {
	store, err := f.tokenStore(nodeID)
	if err != nil {
		return nil, err
	}
	return gdrive.NewTokenSource(ctx, f.authConfig(), store), nil
}
