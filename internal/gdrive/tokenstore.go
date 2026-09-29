package gdrive

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

var ErrNoToken = errors.New("gdrive: no stored token; authorise this account first")

type TokenStore interface {
	Load(ctx context.Context) (*oauth2.Token, error)

	Save(ctx context.Context, token *oauth2.Token) error

	Delete(ctx context.Context) error
}

var tokenMagic = []byte("DDGD1\x00")

const keySize = 32

func KeyFromSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

type FileTokenStore struct {
	path string
	aead cipher.AEAD

	mu sync.Mutex
}

var _ TokenStore = (*FileTokenStore)(nil)

func NewFileTokenStore(path string, key []byte) (*FileTokenStore, error) {
	if path == "" {
		return nil, errors.New("gdrive: the token store needs a path")
	}
	if len(key) != keySize {
		return nil, fmt.Errorf("gdrive: the token store key must be %d bytes, got %d", keySize, len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("gdrive: prepare the token store cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gdrive: prepare the token store AEAD: %w", err)
	}

	return &FileTokenStore{path: path, aead: aead}, nil
}

func (s *FileTokenStore) Path() string { return s.path }

func (s *FileTokenStore) Load(context.Context) (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNoToken
		}
		return nil, fmt.Errorf("gdrive: read the token store: %w", err)
	}

	if len(raw) < len(tokenMagic)+s.aead.NonceSize() {

		return nil, errors.New("gdrive: the token store is truncated or corrupt; authorise this account again")
	}

	if string(raw[:len(tokenMagic)]) != string(tokenMagic) {
		return nil, errors.New("gdrive: the token store has an unrecognised format; it may belong to another tool")
	}

	nonce := raw[len(tokenMagic) : len(tokenMagic)+s.aead.NonceSize()]
	body := raw[len(tokenMagic)+s.aead.NonceSize():]

	plain, err := s.aead.Open(nil, nonce, body, tokenMagic)
	if err != nil {

		return nil, fmt.Errorf("gdrive: decrypt the token store (is the encryption key correct?): %w", err)
	}

	var token oauth2.Token
	if err := json.Unmarshal(plain, &token); err != nil {
		return nil, fmt.Errorf("gdrive: the decrypted token is not valid JSON; authorise this account again: %w", err)
	}

	if token.AccessToken == "" && token.RefreshToken == "" {
		return nil, errors.New("gdrive: the token store holds an empty token; authorise this account again")
	}

	return &token, nil
}

func (s *FileTokenStore) Save(_ context.Context, token *oauth2.Token) error {
	if token == nil {
		return errors.New("gdrive: refusing to save a nil token")
	}
	if token.RefreshToken == "" && token.AccessToken == "" {
		return errors.New("gdrive: refusing to save an empty token")
	}

	plain, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("gdrive: encode the token: %w", err)
	}

	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("gdrive: generate a nonce for the token store: %w", err)
	}

	sealed := s.aead.Seal(nil, nonce, plain, tokenMagic)
	payload := make([]byte, 0, len(tokenMagic)+len(nonce)+len(sealed))
	payload = append(payload, tokenMagic...)
	payload = append(payload, nonce...)
	payload = append(payload, sealed...)

	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("gdrive: create the token directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".token-*")
	if err != nil {
		return fmt.Errorf("gdrive: create the temporary token file: %w", err)
	}
	tmpName := tmp.Name()

	defer func() { _ = os.Remove(tmpName) }()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("gdrive: set the token file permissions: %w", err)
	}

	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("gdrive: write the token: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("gdrive: flush the token: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("gdrive: close the token file: %w", err)
	}

	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("gdrive: replace the token store: %w", err)
	}

	return nil
}

func (s *FileTokenStore) Delete(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.Remove(s.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("gdrive: remove the token store: %w", err)
	}
	return nil
}

const minTokenLifetime = 2 * time.Minute

func NewTokenSource(ctx context.Context, cfg AuthConfig, store TokenStore) oauth2.TokenSource {
	return &refreshingSource{
		ctx:   ctx,
		conf:  cfg.oauth2Config(),
		store: store,
	}
}

type refreshingSource struct {
	ctx   context.Context
	conf  *oauth2.Config
	store TokenStore

	mu sync.Mutex
}

func (s *refreshingSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, err := s.store.Load(s.ctx)
	if err != nil {
		return nil, err
	}

	if !dueForRefresh(stored) {
		return stored, nil
	}

	if stored.RefreshToken == "" {

		return nil, fmt.Errorf("%w: the stored access token has expired and no refresh token is "+
			"available; re-authorise this account", ErrNoToken)
	}

	stale := *stored
	stale.Expiry = time.Now().Add(-time.Hour)

	refreshed, err := s.conf.TokenSource(s.ctx, &stale).Token()
	if err != nil {
		return nil, fmt.Errorf("gdrive: refresh the access token: %w", err)
	}

	if refreshed.RefreshToken == "" {
		refreshed.RefreshToken = stored.RefreshToken
	}

	if err := s.store.Save(s.ctx, refreshed); err != nil {

		return refreshed, fmt.Errorf("gdrive: the token was refreshed but not persisted; "+
			"it will need refreshing again after a restart: %w", err)
	}

	return refreshed, nil
}

func dueForRefresh(token *oauth2.Token) bool {
	if token == nil {
		return true
	}

	if token.Expiry.IsZero() {
		return false
	}
	return !time.Now().Add(minTokenLifetime).Before(token.Expiry)
}
