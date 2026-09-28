










package localfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/provider"
)







const sidecarSuffix = ".meta.json"


type object struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Size        int64  `json:"size"`
	
	
	
	SHA256     string    `json:"sha256"`
	CreatedAt  time.Time `json:"createdAt"`
	ModifiedAt time.Time `json:"modifiedAt"`
}






type Node struct {
	root string

	
	
	quota provider.Quota

	
	
	accountID string

	mu       sync.Mutex
	nowFn    func() time.Time
	written  int64
	identity provider.Identity
}

var _ provider.StorageNode = (*Node)(nil)


type Config struct {
	
	Root string
	
	
	
	Quota int64
	
	
	AccountID string
	
	Now func() time.Time
}


func New(cfg Config) (*Node, error) {
	if strings.TrimSpace(cfg.Root) == "" {
		return nil, errors.New("localfs: root must not be empty")
	}

	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("localfs: resolve root: %w", err)
	}

	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("localfs: create root %s: %w", root, err)
	}

	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	account := cfg.AccountID
	if account == "" {
		account = root
	}

	return &Node{
		root:      root,
		quota:     provider.Quota{Total: cfg.Quota},
		accountID: account,
		nowFn:     now,
		identity: provider.Identity{
			AccountID:   account,
			DisplayName: filepath.Base(root),
		},
	}, nil
}






func (n *Node) Upload(ctx context.Context, r io.Reader, meta provider.ObjectMetadata) (provider.RemoteObject, error) {
	if r == nil {
		return provider.RemoteObject{}, &provider.StatusError{
			Health: provider.HealthDegraded,
			Err:    errors.New("localfs: upload needs a reader"),
		}
	}

	
	
	name := meta.Name
	if name == "" {
		name = fmt.Sprintf("object-%d", n.nowFn().UnixNano())
	}
	if err := validateName(name); err != nil {
		return provider.RemoteObject{}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	finalPath := n.path(name)

	
	
	
	tmp, err := os.CreateTemp(n.root, ".upload-*"+sidecarSuffix)
	if err != nil {
		return provider.RemoteObject{}, n.wrapPathErr("create temp file", err)
	}
	tmpPath := tmp.Name()

	
	defer func() {
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	digest := sha256.New()

	written, err := n.copyWithContext(ctx, tmp, io.TeeReader(r, digest))
	closeErr := tmp.Close()

	switch {
	case err != nil:
		return provider.RemoteObject{}, err
	case closeErr != nil:
		return provider.RemoteObject{}, n.wrapPathErr("close temp file", closeErr)
	}

	if meta.Size > 0 && written != meta.Size {
		
		
		
		return provider.RemoteObject{}, &provider.StatusError{
			Health: provider.HealthDegraded,
			Err: fmt.Errorf("%w: wrote %d bytes, expected %d",
				provider.ErrInvalid, written, meta.Size),
		}
	}

	now := n.nowFn()
	record := object{
		Name:        name,
		Description: meta.Description,
		Size:        written,
		SHA256:      hex.EncodeToString(digest.Sum(nil)),
		CreatedAt:   now,
		ModifiedAt:  now,
	}

	if err := n.writeSidecar(name, record); err != nil {
		return provider.RemoteObject{}, err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return provider.RemoteObject{}, n.wrapPathErr("finalise object", err)
	}
	tmpPath = ""

	n.written += written

	return provider.RemoteObject{
		ID:         name,
		Name:       name,
		Size:       written,
		Checksum:   record.SHA256,
		CreatedAt:  now,
		ModifiedAt: now,
	}, nil
}







func (n *Node) copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	
	
	buf := make([]byte, 256<<10)

	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}

		nr, readErr := src.Read(buf)
		if nr > 0 {
			nw, writeErr := dst.Write(buf[:nr])
			written += int64(nw)
			if writeErr != nil {
				return written, n.wrapPathErr("write object", writeErr)
			}
			
			
			if nw != nr {
				return written, n.wrapPathErr("write object", io.ErrShortWrite)
			}
		}

		if errors.Is(readErr, io.EOF) {
			return written, nil
		}
		if readErr != nil {
			return written, fmt.Errorf("localfs: read source: %w", readErr)
		}
	}
}


func (n *Node) Download(ctx context.Context, id string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateName(id); err != nil {
		return nil, err
	}

	f, err := os.Open(n.path(id))
	if err != nil {
		return nil, n.classifyPathErr("open object", id, err)
	}

	return f, nil
}





func (n *Node) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateName(id); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, err := os.Stat(n.path(id)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return provider.NotFound(id)
		}
		return n.wrapPathErr("stat object", err)
	}

	
	
	
	if err := os.Remove(n.path(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return n.wrapPathErr("delete object", err)
	}

	if err := os.Remove(n.path(id + sidecarSuffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return n.wrapPathErr("delete metadata", err)
	}

	return nil
}


func (n *Node) Stat(ctx context.Context, id string) (provider.RemoteObject, error) {
	if err := ctx.Err(); err != nil {
		return provider.RemoteObject{}, err
	}
	if err := validateName(id); err != nil {
		return provider.RemoteObject{}, err
	}

	record, err := n.readSidecar(id)
	if err != nil {
		
		
		info, statErr := os.Stat(n.path(id))
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return provider.RemoteObject{}, provider.NotFound(id)
			}
			return provider.RemoteObject{}, n.wrapPathErr("stat object", statErr)
		}

		return provider.RemoteObject{
			ID:         id,
			Name:       id,
			Size:       info.Size(),
			CreatedAt:  info.ModTime().UTC(),
			ModifiedAt: info.ModTime().UTC(),
		}, nil
	}

	return provider.RemoteObject{
		ID:         id,
		Name:       record.Name,
		Size:       record.Size,
		Checksum:   record.SHA256,
		CreatedAt:  record.CreatedAt,
		ModifiedAt: record.ModifiedAt,
	}, nil
}


func (n *Node) Quota(context.Context) (provider.Quota, error) {
	return n.quota, nil
}


func (n *Node) Identity(context.Context) (provider.Identity, error) {
	return n.identity, nil
}


func (n *Node) Root() string { return n.root }



func (n *Node) Objects() ([]string, error) {
	entries, err := os.ReadDir(n.root)
	if err != nil {
		return nil, n.wrapPathErr("list root", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		
		if strings.HasSuffix(name, sidecarSuffix) || strings.HasPrefix(name, ".upload-") {
			continue
		}
		names = append(names, name)
	}

	return names, nil
}

func (n *Node) path(name string) string { return filepath.Join(n.root, name) }

func (n *Node) writeSidecar(name string, record object) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("localfs: encode metadata for %q: %w", name, err)
	}

	path := n.path(name + sidecarSuffix)
	
	
	
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return n.wrapPathErr("write metadata", err)
	}

	return nil
}

func (n *Node) readSidecar(name string) (object, error) {
	encoded, err := os.ReadFile(n.path(name + sidecarSuffix))
	if err != nil {
		return object{}, err
	}

	var record object
	if err := json.Unmarshal(encoded, &record); err != nil {
		return object{}, fmt.Errorf("localfs: decode metadata for %q: %w", name, err)
	}

	return record, nil
}



func (n *Node) classifyPathErr(op, id string, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return provider.NotFound(id)
	}
	return n.wrapPathErr(op, err)
}

func (n *Node) wrapPathErr(op string, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return &provider.StatusError{Health: provider.HealthOffline, Err: err}
	}
	if errors.Is(err, os.ErrPermission) {
		return &provider.StatusError{Health: provider.HealthDegraded, Err: err}
	}

	
	
	return &provider.StatusError{Health: provider.HealthOffline, Err: err}
}







func validateName(name string) error {
	switch {
	case name == "":
		return &provider.StatusError{
			Health: provider.HealthDegraded,
			Err:    fmt.Errorf("%w: object name must not be empty", provider.ErrInvalid),
		}
	case name != filepath.Base(name):
		return &provider.StatusError{
			Health: provider.HealthDegraded,
			Err:    fmt.Errorf("%w: object name %q must not contain a path separator", provider.ErrInvalid, name),
		}
	case name == "." || name == "..":
		return &provider.StatusError{
			Health: provider.HealthDegraded,
			Err:    fmt.Errorf("%w: object name %q is not a usable file name", provider.ErrInvalid, name),
		}
	case strings.HasPrefix(name, ".upload-"):
		
		
		return &provider.StatusError{
			Health: provider.HealthDegraded,
			Err:    fmt.Errorf("%w: object name %q is reserved", provider.ErrInvalid, name),
		}
	case strings.ContainsRune(name, 0):
		return &provider.StatusError{
			Health: provider.HealthDegraded,
			Err:    fmt.Errorf("%w: object name contains a null byte", provider.ErrInvalid),
		}
	}

	return nil
}
