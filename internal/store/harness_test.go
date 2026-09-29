package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/config"
	"github.com/VibolSovichea/distributed-drive/internal/db"
	"github.com/VibolSovichea/distributed-drive/internal/db/migrate"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/metadata/sqlite"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

type fakeNode struct {
	mu      sync.Mutex
	objects map[string][]byte
	seq     int

	failWrite bool

	failRead map[string]error

	corrupt map[string][]byte

	uploaded int
	deleted  int
}

func newFakeNode() *fakeNode {
	return &fakeNode{
		objects:  make(map[string][]byte),
		failRead: make(map[string]error),
		corrupt:  make(map[string][]byte),
	}
}

func (f *fakeNode) Upload(_ context.Context, r io.Reader, meta provider.ObjectMetadata) (provider.RemoteObject, error) {
	f.mu.Lock()
	fail := f.failWrite
	f.mu.Unlock()
	if fail {
		return provider.RemoteObject{}, provider.Unavailablef(fmt.Errorf("injected write failure"))
	}

	body, err := io.ReadAll(r)
	if err != nil {
		return provider.RemoteObject{}, err
	}

	if meta.Size > 0 && int64(len(body)) != meta.Size {
		return provider.RemoteObject{}, fmt.Errorf("fake: got %d bytes, promised %d", len(body), meta.Size)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("obj-%d", f.seq)
	f.objects[id] = body
	f.uploaded++

	return provider.RemoteObject{ID: id, Size: int64(len(body))}, nil
}

func (f *fakeNode) Download(_ context.Context, id string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err, ok := f.failRead[id]; ok {
		return nil, err
	}
	body, ok := f.objects[id]
	if !ok {
		return nil, provider.NotFound(id)
	}
	if replacement, ok := f.corrupt[id]; ok {
		body = replacement
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (f *fakeNode) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[id]; !ok {
		return provider.NotFound(id)
	}
	delete(f.objects, id)
	f.deleted++
	return nil
}

func (f *fakeNode) Stat(_ context.Context, id string) (provider.RemoteObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[id]
	if !ok {
		return provider.RemoteObject{}, provider.NotFound(id)
	}
	return provider.RemoteObject{ID: id, Size: int64(len(body))}, nil
}

func (f *fakeNode) Quota(context.Context) (provider.Quota, error) {
	return provider.Quota{Total: 1 << 30}, nil
}

func (f *fakeNode) Identity(context.Context) (provider.Identity, error) {
	return provider.Identity{AccountID: "fake"}, nil
}

func (f *fakeNode) live() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

type fakeResolver struct {
	nodes map[string]*fakeNode
}

func (r fakeResolver) Open(_ context.Context, n metadata.Node) (provider.StorageNode, error) {
	node, ok := r.nodes[n.ID]
	if !ok {
		return nil, fmt.Errorf("no fake node for %s", n.ID)
	}
	return node, nil
}

type harness struct {
	store *Store
	meta  metadata.Store
	nodes map[string]*fakeNode
	pool  metadata.Pool
	now   time.Time
}

type harnessOpts struct {
	k, m    int
	chunk   int64
	nodes   int
	concurr int
}

func newHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()

	cfg := config.DatabaseConfig{
		Path:          filepath.Join(t.TempDir(), "test.db"),
		BusyTimeout:   2 * time.Second,
		MaxOpenConns:  1,
		MigrateOnOpen: true,
	}
	sqlDB, err := db.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("db.Open() = %v", err)
	}
	if _, err := migrate.New(sqlDB).Up(t.Context()); err != nil {
		t.Fatalf("migrate.Up() = %v", err)
	}
	meta := sqlite.New(sqlDB)
	t.Cleanup(func() { _ = meta.Close() })

	ctx := t.Context()
	nodes := make(map[string]*fakeNode, opts.nodes)
	for i := range opts.nodes {
		id := fmt.Sprintf("node-%d", i+1)
		nodes[id] = newFakeNode()

		if err := meta.CreateNode(ctx, metadata.Node{
			ID:                id,
			Name:              id,
			Provider:          metadata.ProviderLocalFS,
			AccountIdentifier: id + "@example.test",
			Status:            metadata.NodeStatusHealthy,
			Capacity:          1 << 30,
		}); err != nil {
			t.Fatalf("CreateNode(%s) = %v", id, err)
		}
	}

	pool := metadata.Pool{
		ID:           "pool-1",
		Name:         "test",
		DataChunks:   opts.k,
		ParityChunks: opts.m,
		ChunkSize:    opts.chunk,
		CreatedAt:    time.Unix(1700000000, 0).UTC(),
	}
	pool.UpdatedAt = pool.CreatedAt
	if err := meta.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool() = %v", err)
	}
	for id := range nodes {
		if err := meta.AddNodeToPool(ctx, pool.ID, id, pool.CreatedAt); err != nil {
			t.Fatalf("AddNodeToPool(%s) = %v", id, err)
		}
	}

	var seq int
	st, err := New(Options{
		Meta:             meta,
		Nodes:            fakeResolver{nodes: nodes},
		Now:              func() time.Time { return time.Unix(1700000000, 0).UTC() },
		NewID:            func() string { seq++; return fmt.Sprintf("id-%d", seq) },
		ShardConcurrency: opts.concurr,
	})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	return &harness{store: st, meta: meta, nodes: nodes, pool: pool, now: time.Unix(1700000000, 0).UTC()}
}

func (h *harness) upload(t *testing.T, name string, body []byte) metadata.File {
	t.Helper()

	file, err := h.store.Upload(t.Context(), UploadInput{
		PoolID: h.pool.ID,
		Name:   name,
		Source: bytes.NewReader(body),
		Size:   int64(len(body)),
	})
	if err != nil {
		t.Fatalf("Upload() = %v", err)
	}
	return file
}

func (h *harness) download(t *testing.T, fileID string) ([]byte, error) {
	t.Helper()

	reader, err := h.store.Download(t.Context(), fileID)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (h *harness) chunks(t *testing.T, fileID string) []metadata.Chunk {
	t.Helper()

	got, err := h.meta.GetFileChunks(t.Context(), fileID)
	if err != nil {
		t.Fatalf("GetFileChunks() = %v", err)
	}
	return got
}

func (h *harness) take(t *testing.T, chunk metadata.Chunk) {
	t.Helper()

	node := h.nodes[chunk.NodeID]
	node.mu.Lock()
	defer node.mu.Unlock()
	delete(node.objects, chunk.RemoteFileID)
}

func (h *harness) scramble(t *testing.T, chunk metadata.Chunk) {
	t.Helper()

	node := h.nodes[chunk.NodeID]
	node.mu.Lock()
	defer node.mu.Unlock()
	body := node.objects[chunk.RemoteFileID]
	wrong := make([]byte, len(body))
	for i := range wrong {
		wrong[i] = body[i] ^ 0xff
	}
	node.corrupt[chunk.RemoteFileID] = wrong
}

func pattern(size int64, seed byte) []byte {
	out := make([]byte, size)
	x := uint32(seed)*2654435761 + 1
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = byte(x >> 24)
	}
	return out
}
