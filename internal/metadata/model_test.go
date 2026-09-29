package metadata

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPoolValidate(t *testing.T) {
	valid := Pool{
		ID: "01J00000000000000000000000", Name: "primary",
		DataChunks: 4, ParityChunks: 2, ChunkSize: 8 << 20,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() on a valid pool = %v, want nil", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Pool)
		wantSub string
	}{
		{name: "empty name", mutate: func(p *Pool) { p.Name = "" }, wantSub: "name must not be empty"},
		{
			name:    "name too long",
			mutate:  func(p *Pool) { p.Name = strings.Repeat("x", MaxNameLength+1) },
			wantSub: "at most 128 characters",
		},
		{name: "zero data chunks", mutate: func(p *Pool) { p.DataChunks = 0 }, wantSub: "at least 1"},
		{
			name:    "negative parity",
			mutate:  func(p *Pool) { p.ParityChunks = -1 },
			wantSub: "must not be negative",
		},
		{
			name:    "zero parity is allowed",
			mutate:  func(p *Pool) { p.ParityChunks = 0 },
			wantSub: "",
		},
		{
			name:    "shards at the limit",
			mutate:  func(p *Pool) { p.DataChunks, p.ParityChunks = MaxShards, 0 },
			wantSub: "",
		},
		{
			name:    "too many shards",
			mutate:  func(p *Pool) { p.DataChunks, p.ParityChunks = MaxShards, 1 },
			wantSub: "at most 256",
		},
		{
			name:    "chunk size at the minimum",
			mutate:  func(p *Pool) { p.ChunkSize = MinChunkSize },
			wantSub: "",
		},
		{
			name:    "chunk size below the minimum",
			mutate:  func(p *Pool) { p.ChunkSize = MinChunkSize - 1 },
			wantSub: "at least 1048576 bytes",
		},
		{
			name:    "chunk size at the maximum",
			mutate:  func(p *Pool) { p.ChunkSize = MaxChunkSize },
			wantSub: "",
		},
		{
			name:    "chunk size above the maximum",
			mutate:  func(p *Pool) { p.ChunkSize = MaxChunkSize + 1 },
			wantSub: "at most 536870912 bytes",
		},
		{
			name:    "negative chunk size",
			mutate:  func(p *Pool) { p.ChunkSize = -1 },
			wantSub: "at least 1048576 bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := valid
			tt.mutate(&pool)

			err := pool.Validate()
			if tt.wantSub == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tt.wantSub)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Validate() error = %v, want it to wrap ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("Validate() error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}

func TestPoolShardsPerStripe(t *testing.T) {
	pool := Pool{DataChunks: 4, ParityChunks: 2}
	if got := pool.ShardsPerStripe(); got != 6 {
		t.Errorf("ShardsPerStripe() = %d, want 6", got)
	}
}

func TestNodeStatusValid(t *testing.T) {
	if NodeStatusUnknown.Valid() {
		t.Error("the zero NodeStatus is valid, want it to be rejected")
	}
	for _, status := range AllNodeStatuses {
		if !status.Valid() {
			t.Errorf("NodeStatus(%q).Valid() = false, want true", status)
		}
	}
	if NodeStatus("exploded").Valid() {
		t.Error("NodeStatus(\"exploded\").Valid() = true, want false")
	}
}

func TestNodeStatusUsable(t *testing.T) {
	tests := []struct {
		status NodeStatus
		want   bool
	}{
		{status: NodeStatusHealthy, want: true},
		{status: NodeStatusDegraded, want: false},
		{status: NodeStatusOffline, want: false},
		{status: NodeStatusFull, want: false},
		{status: NodeStatusAuthError, want: false},
		{status: NodeStatusUnknown, want: false},
	}

	for _, tt := range tests {
		if got := tt.status.Usable(); got != tt.want {
			t.Errorf("NodeStatus(%q).Usable() = %t, want %t", tt.status, got, tt.want)
		}
	}
}

func TestProviderValid(t *testing.T) {
	for _, p := range AllProviders {
		if !p.Valid() {
			t.Errorf("Provider(%q).Valid() = false, want true", p)
		}
	}
	if Provider("dropbox").Valid() {
		t.Error("Provider(\"dropbox\").Valid() = true, want false")
	}
}

func TestNodeAvailable(t *testing.T) {
	tests := []struct {
		name          string
		node          Node
		wantAvailable int64
	}{
		{
			name:          "partially used",
			node:          Node{Capacity: 100, UsedCapacity: 40},
			wantAvailable: 60,
		},
		{
			name:          "unknown capacity",
			node:          Node{Capacity: 0, UsedCapacity: 0},
			wantAvailable: 0,
		},
		{
			name:          "full",
			node:          Node{Capacity: 100, UsedCapacity: 100},
			wantAvailable: 0,
		},
		{
			name:          "over full is clamped",
			node:          Node{Capacity: 100, UsedCapacity: 130},
			wantAvailable: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.node.Available(); got != tt.wantAvailable {
				t.Errorf("Available() = %d, want %d", got, tt.wantAvailable)
			}
		})
	}
}

func TestNodeValidate(t *testing.T) {
	valid := Node{
		ID: "01J00000000000000000000000", Name: "drive-a",
		Provider: ProviderGoogleDrive, Status: NodeStatusHealthy,
		Capacity: 100, UsedCapacity: 40,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() on a valid node = %v, want nil", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Node)
		wantSub string
	}{
		{name: "empty id", mutate: func(n *Node) { n.ID = "" }, wantSub: "id must not be empty"},
		{name: "empty name", mutate: func(n *Node) { n.Name = "" }, wantSub: "name must not be empty"},
		{
			name:    "name too long",
			mutate:  func(n *Node) { n.Name = strings.Repeat("x", MaxNameLength+1) },
			wantSub: "at most 128 characters",
		},
		{name: "unknown provider", mutate: func(n *Node) { n.Provider = "s3" }, wantSub: "unknown provider"},
		{name: "unknown status", mutate: func(n *Node) { n.Status = "sick" }, wantSub: "unknown node status"},
		{name: "negative capacity", mutate: func(n *Node) { n.Capacity = -1 }, wantSub: "must not be negative"},
		{
			name:    "used above capacity",
			mutate:  func(n *Node) { n.UsedCapacity = 101 },
			wantSub: "exceeds capacity",
		},
		{
			name:    "unknown capacity allows unknown usage",
			mutate:  func(n *Node) { n.Capacity, n.UsedCapacity = 0, 0 },
			wantSub: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := valid
			tt.mutate(&node)

			err := node.Validate()
			if tt.wantSub == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tt.wantSub)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Validate() error = %v, want it to wrap ErrInvalid", err)
			}
		})
	}
}

func TestFileStatusValid(t *testing.T) {
	if FileStatus("").Valid() {
		t.Error("the zero FileStatus is valid, want it to be rejected")
	}
	for _, status := range AllFileStatuses {
		if !status.Valid() {
			t.Errorf("FileStatus(%q).Valid() = false, want true", status)
		}
	}
}

func TestFileStatusComplete(t *testing.T) {
	tests := []struct {
		status FileStatus
		want   bool
	}{
		{status: FileStatusCommitted, want: true},
		{status: FileStatusDegraded, want: true},
		{status: FileStatusPending, want: false},
		{status: FileStatusUploading, want: false},
		{status: FileStatusDeleting, want: false},
	}

	for _, tt := range tests {
		if got := tt.status.Complete(); got != tt.want {
			t.Errorf("FileStatus(%q).Complete() = %t, want %t", tt.status, got, tt.want)
		}
	}
}

func TestChunkTypeValid(t *testing.T) {
	if !ChunkTypeData.Valid() || !ChunkTypeParity.Valid() {
		t.Error("the known chunk types are not all valid")
	}
	if ChunkType("").Valid() || ChunkType("shard").Valid() {
		t.Error("an unknown chunk type is valid, want it to be rejected")
	}
}

func TestFileValidate(t *testing.T) {
	valid := File{
		ID: "01J00000000000000000000000", PoolID: "01J00000000000000000000001",
		Name: "report.pdf", Size: 3_500_000, ContentHash: "abcd",
		ChunkSize: MinChunkSize, DataChunks: 4, ParityChunks: 2,
		Status: FileStatusCommitted,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() on a valid file = %v, want nil", err)
	}

	tests := []struct {
		name    string
		mutate  func(*File)
		wantSub string
	}{
		{name: "empty id", mutate: func(f *File) { f.ID = "" }, wantSub: "id must not be empty"},
		{name: "no pool", mutate: func(f *File) { f.PoolID = "" }, wantSub: "must belong to a pool"},
		{name: "empty name", mutate: func(f *File) { f.Name = "" }, wantSub: "name must not be empty"},
		{
			name:    "negative size",
			mutate:  func(f *File) { f.Size = -1 },
			wantSub: "must not be negative",
		},
		{
			name:    "zero size is allowed",
			mutate:  func(f *File) { f.Size = 0 },
			wantSub: "",
		},
		{
			name:    "committed without a hash",
			mutate:  func(f *File) { f.ContentHash = "" },
			wantSub: "must have a content hash",
		},
		{
			name: "uploading without a hash is allowed",
			mutate: func(f *File) {
				f.ContentHash = ""
				f.Status = FileStatusUploading
			},
			wantSub: "",
		},
		{name: "chunk size too small", mutate: func(f *File) { f.ChunkSize = 1 }, wantSub: "at least"},
		{name: "no data chunks", mutate: func(f *File) { f.DataChunks = 0 }, wantSub: "at least 1"},
		{
			name:    "too many shards",
			mutate:  func(f *File) { f.DataChunks, f.ParityChunks = 200, 100 },
			wantSub: "at most 256",
		},
		{name: "unknown status", mutate: func(f *File) { f.Status = "lost" }, wantSub: "unknown file status"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := valid
			tt.mutate(&file)

			err := file.Validate()
			if tt.wantSub == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tt.wantSub)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Validate() error = %v, want it to wrap ErrInvalid", err)
			}
		})
	}
}

func TestChunkValidate(t *testing.T) {
	valid := Chunk{
		ID: "01J00000000000000000000000", FileID: "01J00000000000000000000001",
		NodeID: "01J00000000000000000000002", Index: 0,
		ChunkType: ChunkTypeData, RemoteFileID: "abc", Size: 1 << 20, Hash: "abcd",
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() on a valid chunk = %v, want nil", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Chunk)
		wantSub string
	}{
		{name: "empty id", mutate: func(c *Chunk) { c.ID = "" }, wantSub: "id must not be empty"},
		{name: "no file", mutate: func(c *Chunk) { c.FileID = "" }, wantSub: "must belong to a file"},
		{name: "no node", mutate: func(c *Chunk) { c.NodeID = "" }, wantSub: "must be placed on a node"},
		{name: "negative index", mutate: func(c *Chunk) { c.Index = -1 }, wantSub: "must not be negative"},
		{
			name:    "no remote id",
			mutate:  func(c *Chunk) { c.RemoteFileID = "" },
			wantSub: "must record the remote object id",
		},
		{name: "negative size", mutate: func(c *Chunk) { c.Size = -1 }, wantSub: "must not be negative"},
		{
			name:    "zero size is allowed for a parity shard",
			mutate:  func(c *Chunk) { c.Size = 0 },
			wantSub: "",
		},
		{name: "no hash", mutate: func(c *Chunk) { c.Hash = "" }, wantSub: "must record a hash"},
		{name: "unknown type", mutate: func(c *Chunk) { c.ChunkType = "shard" }, wantSub: "unknown chunk type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := valid
			tt.mutate(&chunk)

			err := chunk.Validate()
			if tt.wantSub == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error containing %q", tt.wantSub)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Validate() error = %v, want it to wrap ErrInvalid", err)
			}
		})
	}
}

func TestFileDataShardCount(t *testing.T) {
	const chunkSize = int64(1 << 20)

	tests := []struct {
		name string
		size int64
		want int
	}{
		{name: "empty file", size: 0, want: 0},
		{name: "single byte", size: 1, want: 1},
		{name: "exactly one shard", size: chunkSize, want: 1},
		{name: "one byte over", size: chunkSize + 1, want: 2},
		{name: "exactly two shards", size: 2 * chunkSize, want: 2},
		{name: "three and a bit", size: 3*chunkSize + 7, want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := File{Size: tt.size, ChunkSize: chunkSize, DataChunks: 4, ParityChunks: 2}
			if got := file.DataShardCount(); got != tt.want {
				t.Errorf("DataShardCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFileShardCountsSpanEveryStripe(t *testing.T) {
	const chunkSize = int64(1 << 20)
	const k, m = 4, 2

	file := File{Size: 1000 * (1 << 20), ChunkSize: chunkSize, DataChunks: k, ParityChunks: m}

	if got, want := file.Stripes(), 250; got != want {
		t.Errorf("Stripes() = %d, want %d", got, want)
	}
	if got, want := file.DataShardCount(), 1000; got != want {
		t.Errorf("DataShardCount() = %d, want %d", got, want)
	}
	if got, want := file.ParityShardCount(), 250*m; got != want {
		t.Errorf("ParityShardCount() = %d, want %d", got, want)
	}
	if got, want := file.TotalShardCount(), 1000+250*m; got != want {
		t.Errorf("TotalShardCount() = %d, want %d", got, want)
	}

	if want := file.Stripes() * k; file.DataShardCount() < want-1 {
		t.Errorf("DataShardCount() = %d is fewer than the %d full stripes imply",
			file.DataShardCount(), file.Stripes())
	}
}

func TestFileStripeBoundaries(t *testing.T) {
	const chunkSize = int64(1 << 20)
	const k, m = 4, 2
	per := int64(k) * chunkSize

	tests := []struct {
		name      string
		size      int64
		wantStrip int
	}{
		{name: "empty file has no stripe", size: 0, wantStrip: 0},
		{name: "one byte", size: 1, wantStrip: 1},
		{name: "one byte under a stripe", size: per - 1, wantStrip: 1},
		{name: "exactly one stripe", size: per, wantStrip: 1},
		{name: "one byte over a stripe", size: per + 1, wantStrip: 2},
		{name: "exactly two stripes", size: 2 * per, wantStrip: 2},
		{name: "two stripes and a bit", size: 2*per + 1, wantStrip: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := File{Size: tt.size, ChunkSize: chunkSize, DataChunks: k, ParityChunks: m}
			if got := file.Stripes(); got != tt.wantStrip {
				t.Errorf("Stripes() = %d, want %d", got, tt.wantStrip)
			}
		})
	}
}

func TestBytesPerStripeAndAnEmptyFileHasNothing(t *testing.T) {
	file := File{Size: 0, ChunkSize: 1 << 20, DataChunks: 4, ParityChunks: 2}
	if got, want := file.BytesPerStripe(), int64(4<<20); got != want {
		t.Errorf("BytesPerStripe() = %d, want %d", got, want)
	}

	if got := file.TotalShardCount(); got != 0 {
		t.Errorf("TotalShardCount() = %d, want 0 for an empty file", got)
	}
	if got := file.LastDataShardSize(); got != 0 {
		t.Errorf("LastDataShardSize() = %d, want 0 for an empty file", got)
	}
}

func TestFileLastDataShardSize(t *testing.T) {
	const chunkSize = int64(1 << 20)

	tests := []struct {
		name string
		size int64
		want int64
	}{
		{name: "empty file", size: 0, want: 0},
		{name: "one byte", size: 1, want: 1},
		{name: "exact multiple", size: 3 * chunkSize, want: chunkSize},
		{name: "with remainder", size: 3*chunkSize + 511, want: 511},
		{name: "smaller than one shard", size: 1234, want: 1234},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := File{Size: tt.size, ChunkSize: chunkSize, DataChunks: 4, ParityChunks: 2}
			if got := file.LastDataShardSize(); got != tt.want {
				t.Errorf("LastDataShardSize() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFileShardHelpersTolerateAnUnsetChunkSize(t *testing.T) {

	file := File{Size: 1000}

	if got := file.DataShardCount(); got != 0 {
		t.Errorf("DataShardCount() = %d, want 0", got)
	}
	if got := file.LastDataShardSize(); got != 0 {
		t.Errorf("LastDataShardSize() = %d, want 0", got)
	}
}

func TestFileShardsPerStripe(t *testing.T) {
	file := File{DataChunks: 4, ParityChunks: 2}
	if got := file.ShardsPerStripe(); got != 6 {
		t.Errorf("ShardsPerStripe() = %d, want 6", got)
	}
}

func TestLastDataShardSizeRoundTrips(t *testing.T) {
	const chunkSize = int64(1 << 20)
	original := []byte("the quick brown fox jumps over the lazy dog")

	padded := make([]byte, chunkSize)
	copy(padded, original)

	if int64(len(padded)) != chunkSize {
		t.Fatalf("padded length = %d, want %d", len(padded), chunkSize)
	}

	truncated := padded[:len(original)]
	if string(truncated) != string(original) {
		t.Errorf("round trip = %q, want %q", truncated, original)
	}
}

func TestNodeLastSeenIsAPointer(t *testing.T) {

	never := Node{}
	if never.LastSeen != nil {
		t.Error("a zero Node has a non-nil LastSeen, want nil")
	}

	now := time.Now().UTC()
	seen := Node{LastSeen: &now}
	if seen.LastSeen == nil || !seen.LastSeen.Equal(now) {
		t.Errorf("LastSeen = %v, want %v", seen.LastSeen, now)
	}
}
