package metadata

import (
	"context"
	"time"
)

type Store interface {
	CreatePool(ctx context.Context, pool Pool) error
	GetPool(ctx context.Context, id string) (Pool, error)
	GetPoolByName(ctx context.Context, name string) (Pool, error)
	ListPools(ctx context.Context) ([]Pool, error)
	UpdatePool(ctx context.Context, pool Pool) error
	DeletePool(ctx context.Context, id string) error

	CreateNode(ctx context.Context, node Node) error
	GetNode(ctx context.Context, id string) (Node, error)
	ListNodes(ctx context.Context) ([]Node, error)
	UpdateNode(ctx context.Context, node Node) error
	DeleteNode(ctx context.Context, id string) error

	AddNodeToPool(ctx context.Context, poolID, nodeID string, at time.Time) error
	RemoveNodeFromPool(ctx context.Context, poolID, nodeID string) error
	ListPoolNodes(ctx context.Context, poolID string) ([]Node, error)
	ListPoolsForNode(ctx context.Context, nodeID string) ([]Pool, error)
	IsNodeInPool(ctx context.Context, poolID, nodeID string) (bool, error)

	CreateFile(ctx context.Context, file File) error
	GetFile(ctx context.Context, id string) (File, error)
	ListPoolFiles(ctx context.Context, poolID string) ([]File, error)
	UpdateFile(ctx context.Context, file File) error
	DeleteFile(ctx context.Context, id string) error

	CreateChunks(ctx context.Context, chunks []Chunk) error
	GetFileChunks(ctx context.Context, fileID string) ([]Chunk, error)
	GetNodeChunks(ctx context.Context, nodeID string) ([]Chunk, error)

	ReplaceChunk(ctx context.Context, chunk Chunk) error

	DeleteFileChunks(ctx context.Context, fileID string) error

	CountFileChunks(ctx context.Context, fileID string) (data, parity int, err error)

	WithTx(ctx context.Context, fn func(ctx context.Context, tx Store) error) error

	Ping(ctx context.Context) error

	Close() error
}
