







package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/id"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)







var ErrNotEmpty = errors.New("pool: still contains files")




type Store interface {
	CreatePool(ctx context.Context, pool metadata.Pool) error
	GetPool(ctx context.Context, id string) (metadata.Pool, error)
	ListPools(ctx context.Context) ([]metadata.Pool, error)
	DeletePool(ctx context.Context, id string) error

	
	ListPoolNodes(ctx context.Context, poolID string) ([]metadata.Node, error)
	ListPoolFiles(ctx context.Context, poolID string) ([]metadata.File, error)

	
	GetNode(ctx context.Context, id string) (metadata.Node, error)
	AddNodeToPool(ctx context.Context, poolID, nodeID string, at time.Time) error
	RemoveNodeFromPool(ctx context.Context, poolID, nodeID string) error
	IsNodeInPool(ctx context.Context, poolID, nodeID string) (bool, error)

	
	GetNodeChunks(ctx context.Context, nodeID string) ([]metadata.Chunk, error)
}


type Service interface {
	
	
	Create(ctx context.Context, input CreateInput) (metadata.Pool, error)
	
	Get(ctx context.Context, poolID string) (metadata.Pool, error)
	
	
	List(ctx context.Context) ([]metadata.Pool, error)
	
	Delete(ctx context.Context, poolID string) error
	
	Capacity(ctx context.Context, poolID string) (Capacity, error)

	
	AddNode(ctx context.Context, poolID, nodeID string) (metadata.Node, error)
	
	RemoveNode(ctx context.Context, poolID, nodeID string) error
	
	ListNodes(ctx context.Context, poolID string) ([]metadata.Node, error)
}

var _ Service = (*Manager)(nil)


type Manager struct {
	store Store
	
	now func() time.Time
}



func NewManager(store Store, now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{store: store, now: now}
}



type CreateInput struct {
	Name         string
	DataChunks   int
	ParityChunks int
	ChunkSize    int64
}


func (m *Manager) Create(ctx context.Context, input CreateInput) (metadata.Pool, error) {
	now := m.now().UTC()

	pool := metadata.Pool{
		ID:           id.New(),
		Name:         input.Name,
		DataChunks:   input.DataChunks,
		ParityChunks: input.ParityChunks,
		ChunkSize:    input.ChunkSize,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	
	
	if err := pool.Validate(); err != nil {
		return metadata.Pool{}, err
	}

	if err := m.store.CreatePool(ctx, pool); err != nil {
		return metadata.Pool{}, fmt.Errorf("pool: create %q: %w", pool.Name, err)
	}

	return pool, nil
}


func (m *Manager) Get(ctx context.Context, poolID string) (metadata.Pool, error) {
	if !id.IsValid(poolID) {
		return metadata.Pool{}, fmt.Errorf("%w: pool id %q is not a valid ULID", metadata.ErrInvalid, poolID)
	}

	pool, err := m.store.GetPool(ctx, poolID)
	if err != nil {
		return metadata.Pool{}, fmt.Errorf("pool: get %s: %w", poolID, err)
	}

	return pool, nil
}





func (m *Manager) List(ctx context.Context) ([]metadata.Pool, error) {
	pools, err := m.store.ListPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("pool: list: %w", err)
	}
	return pools, nil
}


func (m *Manager) Delete(ctx context.Context, poolID string) error {
	if !id.IsValid(poolID) {
		return fmt.Errorf("%w: pool id %q is not a valid ULID", metadata.ErrInvalid, poolID)
	}

	
	
	if _, err := m.store.GetPool(ctx, poolID); err != nil {
		return fmt.Errorf("pool: get %s: %w", poolID, err)
	}

	files, err := m.store.ListPoolFiles(ctx, poolID)
	if err != nil {
		return fmt.Errorf("pool: list files of %s: %w", poolID, err)
	}
	if len(files) > 0 {
		return fmt.Errorf("%w: pool %s holds %d file(s)", ErrNotEmpty, poolID, len(files))
	}

	if err := m.store.DeletePool(ctx, poolID); err != nil {
		return fmt.Errorf("pool: delete %s: %w", poolID, err)
	}

	return nil
}


func (m *Manager) Capacity(ctx context.Context, poolID string) (Capacity, error) {
	if !id.IsValid(poolID) {
		return Capacity{}, fmt.Errorf("%w: pool id %q is not a valid ULID", metadata.ErrInvalid, poolID)
	}

	pool, err := m.store.GetPool(ctx, poolID)
	if err != nil {
		return Capacity{}, fmt.Errorf("pool: get %s: %w", poolID, err)
	}

	nodes, err := m.store.ListPoolNodes(ctx, poolID)
	if err != nil {
		return Capacity{}, fmt.Errorf("pool: list nodes of %s: %w", poolID, err)
	}

	capacity := summarise(nodes)
	capacity.PoolID = pool.ID
	capacity.RequiredNodes = pool.ShardsPerStripe()
	capacity.MissingNodes = capacity.RequiredNodes - len(nodes)
	
	
	capacity.Usable = capacity.MissingNodes <= 0

	return capacity, nil
}






type Capacity struct {
	PoolID string `json:"poolId"`
	
	Nodes int `json:"nodes"`
	
	RequiredNodes int `json:"requiredNodes"`
	
	
	MissingNodes int `json:"missingNodes"`
	
	KnownNodes int `json:"knownNodes"`
	
	TotalBytes int64 `json:"totalBytes"`
	
	UsedBytes int64 `json:"usedBytes"`
	
	
	AvailableBytes int64 `json:"availableBytes"`
	
	
	LogicalCapacity int64 `json:"logicalCapacity"`
	
	Usable bool `json:"usable"`
}

func summarise(nodes []metadata.Node) Capacity {
	capacity := Capacity{Nodes: len(nodes)}

	
	
	
	
	var minAvailable int64
	first := true

	for _, node := range nodes {
		if node.Capacity <= 0 {
			continue
		}

		available := node.Available()
		capacity.KnownNodes++
		capacity.TotalBytes += node.Capacity
		capacity.UsedBytes += node.UsedCapacity
		capacity.AvailableBytes += available

		if first || available < minAvailable {
			minAvailable = available
			first = false
		}
	}

	if !first {
		capacity.LogicalCapacity = minAvailable
	}

	return capacity
}
