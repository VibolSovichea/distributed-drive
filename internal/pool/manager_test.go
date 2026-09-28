package pool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)




type fakeStore struct {
	pools map[string]metadata.Pool
	nodes map[string][]metadata.Node
	files map[string][]metadata.File
	
	
	allNodes     map[string]metadata.Node
	chunksByNode map[string][]metadata.Chunk
	calls        []string
	putErr       error
	getErr       error
	delErr       error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		pools:        make(map[string]metadata.Pool),
		nodes:        make(map[string][]metadata.Node),
		files:        make(map[string][]metadata.File),
		allNodes:     make(map[string]metadata.Node),
		chunksByNode: make(map[string][]metadata.Chunk),
	}
}

func (f *fakeStore) CreatePool(_ context.Context, pool metadata.Pool) error {
	f.calls = append(f.calls, "CreatePool:"+pool.ID)
	if f.putErr != nil {
		return f.putErr
	}
	if _, exists := f.pools[pool.ID]; exists {
		return metadata.ErrConflict
	}
	f.pools[pool.ID] = pool
	return nil
}

func (f *fakeStore) GetPool(_ context.Context, id string) (metadata.Pool, error) {
	f.calls = append(f.calls, "GetPool:"+id)
	if f.getErr != nil {
		return metadata.Pool{}, f.getErr
	}
	pool, ok := f.pools[id]
	if !ok {
		return metadata.Pool{}, metadata.ErrNotFound
	}
	return pool, nil
}

func (f *fakeStore) ListPools(_ context.Context) ([]metadata.Pool, error) {
	f.calls = append(f.calls, "ListPools")
	pools := make([]metadata.Pool, 0, len(f.pools))
	for _, pool := range f.pools {
		pools = append(pools, pool)
	}
	return pools, nil
}

func (f *fakeStore) DeletePool(_ context.Context, id string) error {
	f.calls = append(f.calls, "DeletePool:"+id)
	if f.delErr != nil {
		return f.delErr
	}
	delete(f.pools, id)
	return nil
}

func (f *fakeStore) ListPoolNodes(_ context.Context, poolID string) ([]metadata.Node, error) {
	f.calls = append(f.calls, "ListPoolNodes:"+poolID)
	return f.nodes[poolID], nil
}

func (f *fakeStore) ListPoolFiles(_ context.Context, poolID string) ([]metadata.File, error) {
	f.calls = append(f.calls, "ListPoolFiles:"+poolID)
	return f.files[poolID], nil
}


func (f *fakeStore) addNode(n metadata.Node) metadata.Node {
	if n.ID == "" {
		n.ID = "01HQ0000000000000000000000"
	}
	if n.Status == "" {
		n.Status = metadata.NodeStatusHealthy
	}
	if !n.Provider.Valid() {
		n.Provider = metadata.ProviderGoogleDrive
	}
	f.allNodes[n.ID] = n
	return n
}

func (f *fakeStore) GetNode(_ context.Context, id string) (metadata.Node, error) {
	f.calls = append(f.calls, "GetNode:"+id)
	node, ok := f.allNodes[id]
	if !ok {
		return metadata.Node{}, fmt.Errorf("%w: node %s", metadata.ErrNotFound, id)
	}
	return node, nil
}

func (f *fakeStore) AddNodeToPool(_ context.Context, poolID, nodeID string, _ time.Time) error {
	f.calls = append(f.calls, "AddNodeToPool:"+poolID+"/"+nodeID)
	for _, n := range f.nodes[poolID] {
		if n.ID == nodeID {
			return fmt.Errorf("%w: node %s", metadata.ErrConflict, nodeID)
		}
	}
	node, ok := f.allNodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: node %s", metadata.ErrNotFound, nodeID)
	}
	f.nodes[poolID] = append(f.nodes[poolID], node)
	return nil
}

func (f *fakeStore) RemoveNodeFromPool(_ context.Context, poolID, nodeID string) error {
	f.calls = append(f.calls, "RemoveNodeFromPool:"+poolID+"/"+nodeID)
	kept := f.nodes[poolID][:0]
	for _, n := range f.nodes[poolID] {
		if n.ID != nodeID {
			kept = append(kept, n)
		}
	}
	f.nodes[poolID] = kept
	return nil
}

func (f *fakeStore) IsNodeInPool(_ context.Context, poolID, nodeID string) (bool, error) {
	f.calls = append(f.calls, "IsNodeInPool:"+poolID+"/"+nodeID)
	for _, n := range f.nodes[poolID] {
		if n.ID == nodeID {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) GetNodeChunks(_ context.Context, nodeID string) ([]metadata.Chunk, error) {
	f.calls = append(f.calls, "GetNodeChunks:"+nodeID)
	return f.chunksByNode[nodeID], nil
}

var fixedNow = time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)

func validInput() CreateInput {
	return CreateInput{Name: "primary", DataChunks: 4, ParityChunks: 2, ChunkSize: 8 << 20}
}

func TestCreateAssignsServerSideIdentity(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, func() time.Time { return fixedNow })

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}

	if created.ID == "" {
		t.Error("Create() returned a pool with no ID, want a generated ULID")
	}
	if !created.CreatedAt.Equal(fixedNow) || !created.UpdatedAt.Equal(fixedNow) {
		t.Errorf("Create() timestamps = %v/%v, want both %v", created.CreatedAt, created.UpdatedAt, fixedNow)
	}
	if got := store.pools[created.ID]; got.Name != "primary" {
		t.Errorf("persisted pool name = %q, want %q", got.Name, "primary")
	}
}

func TestCreateRejectsAnInvalidPoolBeforeTouchingTheStore(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CreateInput)
	}{
		{name: "no name", mutate: func(i *CreateInput) { i.Name = "" }},
		{name: "zero data chunks", mutate: func(i *CreateInput) { i.DataChunks = 0 }},
		{name: "negative parity", mutate: func(i *CreateInput) { i.ParityChunks = -1 }},
		{name: "too many shards", mutate: func(i *CreateInput) { i.DataChunks, i.ParityChunks = 255, 255 }},
		{name: "chunk size too small", mutate: func(i *CreateInput) { i.ChunkSize = 1 }},
		{name: "chunk size too large", mutate: func(i *CreateInput) { i.ChunkSize = 1 << 40 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			svc := NewManager(store, nil)

			input := validInput()
			tt.mutate(&input)

			_, err := svc.Create(t.Context(), input)
			if err == nil {
				t.Fatalf("Create() = nil, want an error")
			}
			if !errors.Is(err, metadata.ErrInvalid) {
				t.Errorf("Create() error = %v, want it to wrap metadata.ErrInvalid", err)
			}
			if len(store.calls) != 0 {
				t.Errorf("Create() called the store (%v), want no store calls for an invalid pool", store.calls)
			}
		})
	}
}

func TestCreatePropagatesAStoreFailure(t *testing.T) {
	store := newFakeStore()
	store.putErr = errors.New("disk on fire")
	svc := NewManager(store, nil)

	_, err := svc.Create(t.Context(), validInput())
	if err == nil {
		t.Fatal("Create() = nil, want the store error")
	}
	if !errors.Is(err, store.putErr) {
		t.Errorf("Create() error = %v, want it to wrap the store error", err)
	}
}

func TestGetRejectsAMalformedIdentifier(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	
	
	_, err := svc.Get(t.Context(), "../../etc/passwd")
	if !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("Get() error = %v, want it to wrap metadata.ErrInvalid", err)
	}
	if len(store.calls) != 0 {
		t.Errorf("Get() called the store (%v), want no store calls", store.calls)
	}
}

func TestGetReportsNotFound(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	id := "01J00000000000000000000000"
	_, err := svc.Get(t.Context(), id)
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("Get() error = %v, want it to wrap metadata.ErrNotFound", err)
	}
}

func TestDeleteRefusesAPoolThatStillHoldsFiles(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}
	store.files[created.ID] = []metadata.File{{ID: "01J00000000000000000000001"}}

	err = svc.Delete(t.Context(), created.ID)
	if !errors.Is(err, ErrNotEmpty) {
		t.Errorf("Delete() error = %v, want it to wrap ErrNotEmpty", err)
	}

	
	if _, ok := store.pools[created.ID]; !ok {
		t.Error("Delete() removed the pool even though it reported ErrNotEmpty")
	}
	for _, call := range store.calls {
		if call == "DeletePool:"+created.ID {
			t.Error("Delete() deleted the pool from the store, want the delete skipped")
		}
	}
}

func TestDeleteRemovesAnEmptyPool(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}

	if err := svc.Delete(t.Context(), created.ID); err != nil {
		t.Fatalf("Delete() error = %v, want nil", err)
	}
	if _, ok := store.pools[created.ID]; ok {
		t.Error("Delete() left the pool in the store, want it removed")
	}
}

func TestDeleteOfAMissingPoolIsNotFound(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	err := svc.Delete(t.Context(), "01J00000000000000000000000")
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("Delete() error = %v, want it to wrap metadata.ErrNotFound", err)
	}
}

func TestCapacityReportsShortfallAndTheEmptyNodeLimit(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}

	
	
	store.nodes[created.ID] = []metadata.Node{
		{ID: "n1", Capacity: 100, UsedCapacity: 50},
		{ID: "n2", Capacity: 100, UsedCapacity: 10},
		{ID: "n3", Capacity: 100, UsedCapacity: 20},
		{ID: "n4", Capacity: 100, UsedCapacity: 30},
		{ID: "n5", Capacity: 100, UsedCapacity: 40},
		{ID: "n6", Capacity: 100, UsedCapacity: 60},
	}

	got, err := svc.Capacity(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Capacity() error = %v, want nil", err)
	}

	if got.Nodes != 6 || got.RequiredNodes != 6 || got.MissingNodes != 0 {
		t.Errorf("node counts = %d/%d/%d, want 6/6/0", got.Nodes, got.RequiredNodes, got.MissingNodes)
	}
	if !got.Usable {
		t.Error("Usable = false, want true for a pool with all its nodes")
	}
	if want := int64(600); got.TotalBytes != want {
		t.Errorf("TotalBytes = %d, want %d", got.TotalBytes, want)
	}
	if want := int64(40); got.LogicalCapacity != want {
		t.Errorf("LogicalCapacity = %d, want %d (the emptiest node)", got.LogicalCapacity, want)
	}
	if want := int64(390); got.AvailableBytes != want {
		t.Errorf("AvailableBytes = %d, want %d", got.AvailableBytes, want)
	}
	if want := int64(210); got.UsedBytes != want {
		t.Errorf("UsedBytes = %d, want %d", got.UsedBytes, want)
	}
}

func TestCapacityOfAnUnderProvisionedPool(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}
	store.nodes[created.ID] = []metadata.Node{
		{ID: "n1", Capacity: 100, UsedCapacity: 0},
		{ID: "n2", Capacity: 100, UsedCapacity: 0},
	}

	got, err := svc.Capacity(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Capacity() error = %v, want nil", err)
	}

	if got.MissingNodes != 4 {
		t.Errorf("MissingNodes = %d, want 4", got.MissingNodes)
	}
	if got.Usable {
		t.Error("Usable = true, want false when the pool is missing nodes")
	}
}

func TestCapacityIgnoresNodesWithUnknownSize(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}
	store.nodes[created.ID] = []metadata.Node{
		{ID: "n1", Capacity: 100, UsedCapacity: 25},
		{ID: "n2", Capacity: 0, UsedCapacity: 0}, 
	}

	got, err := svc.Capacity(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Capacity() error = %v, want nil", err)
	}

	if got.Nodes != 2 {
		t.Errorf("Nodes = %d, want 2", got.Nodes)
	}
	if got.KnownNodes != 1 {
		t.Errorf("KnownNodes = %d, want 1", got.KnownNodes)
	}
	if want := int64(100); got.TotalBytes != want {
		t.Errorf("TotalBytes = %d, want %d: an unknown capacity must not be counted", got.TotalBytes, want)
	}
	
	if want := int64(75); got.LogicalCapacity != want {
		t.Errorf("LogicalCapacity = %d, want %d", got.LogicalCapacity, want)
	}
}

func TestCapacityOfAnEmptyPool(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	created, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create() error = %v, want nil", err)
	}

	got, err := svc.Capacity(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Capacity() error = %v, want nil", err)
	}

	if got.Usable {
		t.Error("Usable = true, want false for a pool with no nodes")
	}
	if got.LogicalCapacity != 0 {
		t.Errorf("LogicalCapacity = %d, want 0", got.LogicalCapacity)
	}
	if got.MissingNodes != 6 {
		t.Errorf("MissingNodes = %d, want 6", got.MissingNodes)
	}
}

func TestCapacityOfAMissingPool(t *testing.T) {
	store := newFakeStore()
	svc := NewManager(store, nil)

	_, err := svc.Capacity(t.Context(), "01J00000000000000000000000")
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("Capacity() error = %v, want it to wrap metadata.ErrNotFound", err)
	}
}

func TestErrorsMentionThePool(t *testing.T) {
	store := newFakeStore()
	store.getErr = fmt.Errorf("wrapped: %w", metadata.ErrNotFound)
	svc := NewManager(store, nil)

	_, err := svc.Get(t.Context(), "01J00000000000000000000000")
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("Get() error = %v, want it to wrap metadata.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "pool: get") {
		t.Errorf("Get() error = %q, want it to name the operation", err)
	}
}
