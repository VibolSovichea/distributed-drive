package metadata

import (
	"fmt"
	"time"
)

const (
	MaxShards = 256

	MinChunkSize = 1 << 20

	MaxChunkSize = 512 << 20

	MaxNameLength = 128
)

type Pool struct {
	ID   string
	Name string

	DataChunks int

	ParityChunks int

	ChunkSize int64

	Encrypted bool

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (p Pool) Validate() error {
	switch {
	case p.Name == "":
		return fmt.Errorf("%w: pool name must not be empty", ErrInvalid)
	case len(p.Name) > MaxNameLength:
		return fmt.Errorf("%w: pool name must be at most %d characters, got %d",
			ErrInvalid, MaxNameLength, len(p.Name))
	case p.DataChunks < 1:
		return fmt.Errorf("%w: data chunks must be at least 1, got %d", ErrInvalid, p.DataChunks)
	case p.ParityChunks < 0:
		return fmt.Errorf("%w: parity chunks must not be negative, got %d", ErrInvalid, p.ParityChunks)
	case p.DataChunks+p.ParityChunks > MaxShards:
		return fmt.Errorf("%w: data chunks plus parity chunks must be at most %d, got %d",
			ErrInvalid, MaxShards, p.DataChunks+p.ParityChunks)
	case p.ChunkSize < MinChunkSize:
		return fmt.Errorf("%w: chunk size must be at least %d bytes, got %d",
			ErrInvalid, int64(MinChunkSize), p.ChunkSize)
	case p.ChunkSize > MaxChunkSize:
		return fmt.Errorf("%w: chunk size must be at most %d bytes, got %d",
			ErrInvalid, int64(MaxChunkSize), p.ChunkSize)
	}
	return nil
}

func (p Pool) ShardsPerStripe() int {
	return p.DataChunks + p.ParityChunks
}

type NodeStatus string

const (
	NodeStatusUnknown NodeStatus = ""

	NodeStatusHealthy NodeStatus = "healthy"

	NodeStatusDegraded NodeStatus = "degraded"

	NodeStatusOffline NodeStatus = "offline"

	NodeStatusFull NodeStatus = "full"

	NodeStatusAuthError NodeStatus = "auth_error"
)

var AllNodeStatuses = []NodeStatus{
	NodeStatusHealthy,
	NodeStatusDegraded,
	NodeStatusOffline,
	NodeStatusFull,
	NodeStatusAuthError,
}

func (s NodeStatus) Valid() bool {
	for _, known := range AllNodeStatuses {
		if s == known {
			return true
		}
	}
	return false
}

func (s NodeStatus) Usable() bool {
	return s == NodeStatusHealthy
}

type Provider string

const (
	ProviderGoogleDrive Provider = "google_drive"

	ProviderLocalFS Provider = "localfs"
)

var AllProviders = []Provider{ProviderGoogleDrive, ProviderLocalFS}

func (p Provider) Valid() bool {
	for _, known := range AllProviders {
		if p == known {
			return true
		}
	}
	return false
}

type Node struct {
	ID   string
	Name string

	Provider Provider

	AccountIdentifier string

	Status NodeStatus

	Capacity int64

	UsedCapacity int64

	LastSeen *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (n Node) Available() int64 {
	if n.Capacity <= 0 {
		return 0
	}
	if n.UsedCapacity >= n.Capacity {
		return 0
	}
	return n.Capacity - n.UsedCapacity
}

func (n Node) Validate() error {
	switch {
	case n.ID == "":
		return fmt.Errorf("%w: node id must not be empty", ErrInvalid)
	case n.Name == "":
		return fmt.Errorf("%w: node name must not be empty", ErrInvalid)
	case len(n.Name) > MaxNameLength:
		return fmt.Errorf("%w: node name must be at most %d characters, got %d",
			ErrInvalid, MaxNameLength, len(n.Name))
	case !n.Provider.Valid():
		return fmt.Errorf("%w: unknown provider %q", ErrInvalid, n.Provider)
	case !n.Status.Valid():
		return fmt.Errorf("%w: unknown node status %q", ErrInvalid, n.Status)
	case n.Capacity < 0:
		return fmt.Errorf("%w: capacity must not be negative, got %d", ErrInvalid, n.Capacity)
	case n.UsedCapacity < 0:
		return fmt.Errorf("%w: used capacity must not be negative, got %d", ErrInvalid, n.UsedCapacity)
	case n.Capacity > 0 && n.UsedCapacity > n.Capacity:
		return fmt.Errorf("%w: used capacity %d exceeds capacity %d",
			ErrInvalid, n.UsedCapacity, n.Capacity)
	}
	return nil
}

type FileStatus string

const (
	FileStatusPending FileStatus = "pending"

	FileStatusUploading FileStatus = "uploading"

	FileStatusCommitted FileStatus = "committed"

	FileStatusDegraded FileStatus = "degraded"

	FileStatusDeleting FileStatus = "deleting"
)

var AllFileStatuses = []FileStatus{
	FileStatusPending,
	FileStatusUploading,
	FileStatusCommitted,
	FileStatusDegraded,
	FileStatusDeleting,
}

func (s FileStatus) Valid() bool {
	for _, known := range AllFileStatuses {
		if s == known {
			return true
		}
	}
	return false
}

func (s FileStatus) Complete() bool {
	return s == FileStatusCommitted || s == FileStatusDegraded
}

type ChunkType string

const (
	ChunkTypeData ChunkType = "data"

	ChunkTypeParity ChunkType = "parity"
)

func (t ChunkType) Valid() bool {
	return t == ChunkTypeData || t == ChunkTypeParity
}

type File struct {
	ID     string
	PoolID string
	Name   string

	Size int64

	ContentHash string

	ChunkSize    int64
	DataChunks   int
	ParityChunks int

	Encrypted bool

	Status    FileStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (f File) Validate() error {
	switch {
	case f.ID == "":
		return fmt.Errorf("%w: file id must not be empty", ErrInvalid)
	case f.PoolID == "":
		return fmt.Errorf("%w: file must belong to a pool", ErrInvalid)
	case f.Name == "":
		return fmt.Errorf("%w: file name must not be empty", ErrInvalid)
	case len(f.Name) > MaxNameLength:
		return fmt.Errorf("%w: file name must be at most %d characters, got %d",
			ErrInvalid, MaxNameLength, len(f.Name))
	case f.Size < 0:
		return fmt.Errorf("%w: file size must not be negative, got %d", ErrInvalid, f.Size)
	case f.ChunkSize < MinChunkSize:
		return fmt.Errorf("%w: chunk size must be at least %d bytes, got %d",
			ErrInvalid, int64(MinChunkSize), f.ChunkSize)
	case f.DataChunks < 1:
		return fmt.Errorf("%w: data chunks must be at least 1, got %d", ErrInvalid, f.DataChunks)
	case f.ParityChunks < 0:
		return fmt.Errorf("%w: parity chunks must not be negative, got %d", ErrInvalid, f.ParityChunks)
	case f.DataChunks+f.ParityChunks > MaxShards:
		return fmt.Errorf("%w: data chunks plus parity chunks must be at most %d, got %d",
			ErrInvalid, MaxShards, f.DataChunks+f.ParityChunks)
	case !f.Status.Valid():
		return fmt.Errorf("%w: unknown file status %q", ErrInvalid, f.Status)
	case f.Status == FileStatusCommitted && f.ContentHash == "":
		return fmt.Errorf("%w: a committed file must have a content hash", ErrInvalid)
	}
	return nil
}

func (f File) ShardsPerStripe() int {
	return f.DataChunks + f.ParityChunks
}

func (f File) BytesPerStripe() int64 {
	if f.DataChunks < 1 || f.ChunkSize <= 0 {
		return 0
	}
	return int64(f.DataChunks) * f.ChunkSize
}

func (f File) Stripes() int {
	per := f.BytesPerStripe()
	if per <= 0 || f.Size <= 0 {
		return 0
	}
	return int((f.Size + per - 1) / per)
}

func (f File) DataShardCount() int {
	if f.Size == 0 || f.ChunkSize <= 0 {
		return 0
	}
	return int((f.Size + f.ChunkSize - 1) / f.ChunkSize)
}

func (f File) ParityShardCount() int {
	return f.Stripes() * f.ParityChunks
}

func (f File) TotalShardCount() int {
	return f.DataShardCount() + f.ParityShardCount()
}

func (f File) LastDataShardSize() int64 {
	if f.Size == 0 || f.ChunkSize <= 0 {
		return 0
	}
	if remainder := f.Size % f.ChunkSize; remainder != 0 {
		return remainder
	}
	return f.ChunkSize
}

type Chunk struct {
	ID     string
	FileID string
	NodeID string

	StripeIndex int

	Index     int
	ChunkType ChunkType

	RemoteFileID string

	Size int64

	Hash string

	CreatedAt time.Time
}

func (c Chunk) Validate() error {
	switch {
	case c.ID == "":
		return fmt.Errorf("%w: chunk id must not be empty", ErrInvalid)
	case c.FileID == "":
		return fmt.Errorf("%w: chunk must belong to a file", ErrInvalid)
	case c.NodeID == "":
		return fmt.Errorf("%w: chunk must be placed on a node", ErrInvalid)
	case c.StripeIndex < 0:
		return fmt.Errorf("%w: chunk stripe index must not be negative, got %d",
			ErrInvalid, c.StripeIndex)
	case c.Index < 0:
		return fmt.Errorf("%w: chunk index must not be negative, got %d", ErrInvalid, c.Index)
	case c.RemoteFileID == "":
		return fmt.Errorf("%w: chunk must record the remote object id", ErrInvalid)
	case c.Size < 0:
		return fmt.Errorf("%w: chunk size must not be negative, got %d", ErrInvalid, c.Size)
	case c.Hash == "":
		return fmt.Errorf("%w: chunk must record a hash", ErrInvalid)
	case !c.ChunkType.Valid():
		return fmt.Errorf("%w: unknown chunk type %q", ErrInvalid, c.ChunkType)
	}
	return nil
}
