package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/config"
	"github.com/VibolSovichea/distributed-drive/internal/db"
	"github.com/VibolSovichea/distributed-drive/internal/db/migrate"
	"github.com/VibolSovichea/distributed-drive/internal/id"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

func newStore(t *testing.T) metadata.Store {
	t.Helper()

	cfg := config.DatabaseConfig{
		Path:          filepath.Join(t.TempDir(), "test.db"),
		BusyTimeout:   2 * time.Second,
		MaxOpenConns:  1,
		MigrateOnOpen: true,
	}

	sqlDB, err := db.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("db.Open() error = %v, want nil", err)
	}
	if _, err := migrate.New(sqlDB).Up(t.Context()); err != nil {
		t.Fatalf("migrate.Up() error = %v, want nil", err)
	}

	store := New(sqlDB)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return store
}

var testTime = time.Date(2026, 3, 14, 15, 9, 26, 535897932, time.UTC)

func newPool(t *testing.T, name string) metadata.Pool {
	t.Helper()
	return metadata.Pool{
		ID:           id.New(),
		Name:         name,
		DataChunks:   4,
		ParityChunks: 2,
		ChunkSize:    8 << 20,
		CreatedAt:    testTime,
		UpdatedAt:    testTime,
	}
}

func newNode(t *testing.T, name string) metadata.Node {
	t.Helper()
	return metadata.Node{
		ID:                id.New(),
		Name:              name,
		Provider:          metadata.ProviderGoogleDrive,
		AccountIdentifier: name + "@example.com",
		Status:            metadata.NodeStatusHealthy,
		Capacity:          15 << 30,
		UsedCapacity:      1 << 30,
		CreatedAt:         testTime,
		UpdatedAt:         testTime,
	}
}

func newFile(t *testing.T, poolID, name string) metadata.File {
	t.Helper()
	return metadata.File{
		ID:     id.New(),
		PoolID: poolID,
		Name:   name,

		Size:         3_500_000,
		ContentHash:  "d34db33f0000000000000000000000000000000000000000000000000000000",
		ChunkSize:    1 << 20,
		DataChunks:   4,
		ParityChunks: 2,
		Status:       metadata.FileStatusCommitted,
		CreatedAt:    testTime,
		UpdatedAt:    testTime,
	}
}

func newChunk(t *testing.T, fileID, nodeID string, index int) metadata.Chunk {
	t.Helper()
	return newChunkInStripe(t, fileID, nodeID, 0, index)
}

func newChunkInStripe(t *testing.T, fileID, nodeID string, stripe, index int) metadata.Chunk {
	t.Helper()
	tag := string(rune('a'+stripe)) + string(rune('a'+index))
	return metadata.Chunk{
		ID:           id.New(),
		FileID:       fileID,
		NodeID:       nodeID,
		StripeIndex:  stripe,
		Index:        index,
		ChunkType:    metadata.ChunkTypeData,
		RemoteFileID: "remote-" + fileID[:8] + "-" + tag,
		Size:         1 << 20,
		Hash:         "aaaa" + tag + "00000000000000000000000000000000000000000000000000000000000000",
		CreatedAt:    testTime,
	}
}

func mustCreatePool(t *testing.T, s metadata.Store, name string) metadata.Pool {
	t.Helper()
	pool := newPool(t, name)
	if err := s.CreatePool(t.Context(), pool); err != nil {
		t.Fatalf("CreatePool(%q) error = %v, want nil", name, err)
	}
	return pool
}

func mustCreateNode(t *testing.T, s metadata.Store, name string) metadata.Node {
	t.Helper()
	node := newNode(t, name)
	if err := s.CreateNode(t.Context(), node); err != nil {
		t.Fatalf("CreateNode(%q) error = %v, want nil", name, err)
	}
	return node
}

func mustCreateFile(t *testing.T, s metadata.Store, poolID, name string) metadata.File {
	t.Helper()
	file := newFile(t, poolID, name)
	if err := s.CreateFile(t.Context(), file); err != nil {
		t.Fatalf("CreateFile(%q) error = %v, want nil", name, err)
	}
	return file
}

func TestCreateAndGetPool(t *testing.T) {
	s := newStore(t)
	want := newPool(t, "primary")

	if err := s.CreatePool(t.Context(), want); err != nil {
		t.Fatalf("CreatePool() error = %v, want nil", err)
	}

	got, err := s.GetPool(t.Context(), want.ID)
	if err != nil {
		t.Fatalf("GetPool() error = %v, want nil", err)
	}

	if got.ID != want.ID || got.Name != want.Name {
		t.Errorf("GetPool() = %+v, want id %q name %q", got, want.ID, want.Name)
	}
	if got.DataChunks != want.DataChunks || got.ParityChunks != want.ParityChunks {
		t.Errorf("GetPool() K/M = %d/%d, want %d/%d",
			got.DataChunks, got.ParityChunks, want.DataChunks, want.ParityChunks)
	}
	if got.ChunkSize != want.ChunkSize {
		t.Errorf("GetPool() ChunkSize = %d, want %d", got.ChunkSize, want.ChunkSize)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("GetPool() CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("GetPool() UpdatedAt = %v, want %v", got.UpdatedAt, want.UpdatedAt)
	}
}

func TestGetPoolByName(t *testing.T) {
	s := newStore(t)
	want := mustCreatePool(t, s, "primary")

	got, err := s.GetPoolByName(t.Context(), "primary")
	if err != nil {
		t.Fatalf("GetPoolByName() error = %v, want nil", err)
	}
	if got.ID != want.ID {
		t.Errorf("GetPoolByName() = %q, want %q", got.ID, want.ID)
	}
}

func TestGetMissingPoolReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if _, err := s.GetPool(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("GetPool() error = %v, want metadata.ErrNotFound", err)
	}
	if _, err := s.GetPoolByName(t.Context(), "nope"); !isNotFound(err) {
		t.Errorf("GetPoolByName() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreatePoolWithDuplicateNameReturnsConflict(t *testing.T) {
	s := newStore(t)
	mustCreatePool(t, s, "primary")

	err := s.CreatePool(t.Context(), newPool(t, "primary"))
	if !isConflict(err) {
		t.Fatalf("CreatePool() duplicate error = %v, want metadata.ErrConflict", err)
	}
}

func TestListPoolsIsOrderedAndEmptySafe(t *testing.T) {
	s := newStore(t)

	got, err := s.ListPools(t.Context())
	if err != nil {
		t.Fatalf("ListPools() error = %v, want nil", err)
	}
	if got == nil {
		t.Fatal("ListPools() = nil, want an empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("ListPools() = %d pools, want 0", len(got))
	}

	first := mustCreatePool(t, s, "first")
	second := mustCreatePool(t, s, "second")

	got, err = s.ListPools(t.Context())
	if err != nil {
		t.Fatalf("ListPools() error = %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListPools() = %d pools, want 2", len(got))
	}
	if got[0].ID != first.ID || got[1].ID != second.ID {
		t.Errorf("ListPools() = [%s %s], want [%s %s]", got[0].ID, got[1].ID, first.ID, second.ID)
	}
}

func TestUpdatePool(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	pool.Name = "renamed"
	pool.ChunkSize = 16 << 20
	pool.UpdatedAt = testTime.Add(time.Hour)

	if err := s.UpdatePool(t.Context(), pool); err != nil {
		t.Fatalf("UpdatePool() error = %v, want nil", err)
	}

	got, err := s.GetPool(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("GetPool() error = %v, want nil", err)
	}
	if got.Name != "renamed" || got.ChunkSize != 16<<20 {
		t.Errorf("GetPool() = %+v, want the updated values", got)
	}
	if !got.UpdatedAt.Equal(pool.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, pool.UpdatedAt)
	}
}

func TestUpdatePoolRenameToExistingNameReturnsConflict(t *testing.T) {
	s := newStore(t)
	mustCreatePool(t, s, "taken")
	mine := mustCreatePool(t, s, "mine")

	mine.Name = "taken"
	if err := s.UpdatePool(t.Context(), mine); !isConflict(err) {
		t.Fatalf("UpdatePool() error = %v, want metadata.ErrConflict", err)
	}
}

func TestUpdateMissingPoolReturnsNotFound(t *testing.T) {
	s := newStore(t)

	pool := newPool(t, "ghost")
	pool.ID = id.New()
	if err := s.UpdatePool(t.Context(), pool); !isNotFound(err) {
		t.Errorf("UpdatePool() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestDeletePoolCascadesToFiles(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")
	if err := s.CreateChunks(t.Context(), []metadata.Chunk{newChunk(t, file.ID, node.ID, 0)}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	if err := s.DeletePool(t.Context(), pool.ID); err != nil {
		t.Fatalf("DeletePool() error = %v, want nil", err)
	}

	if _, err := s.GetFile(t.Context(), file.ID); !isNotFound(err) {
		t.Errorf("GetFile() after pool deletion error = %v, want metadata.ErrNotFound", err)
	}

	if err := s.DeleteNode(t.Context(), node.ID); err != nil {
		t.Errorf("DeleteNode() after the pool was removed error = %v, want nil", err)
	}
}

func TestDeleteMissingPoolReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if err := s.DeletePool(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("DeletePool() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreatePoolRejectsInvalidRecord(t *testing.T) {
	s := newStore(t)

	tests := []struct {
		name  string
		mutat func(*metadata.Pool)
	}{
		{name: "no name", mutat: func(p *metadata.Pool) { p.Name = "" }},
		{name: "zero data chunks", mutat: func(p *metadata.Pool) { p.DataChunks = 0 }},
		{name: "negative parity", mutat: func(p *metadata.Pool) { p.ParityChunks = -1 }},
		{name: "too many shards", mutat: func(p *metadata.Pool) { p.DataChunks, p.ParityChunks = 255, 255 }},
		{name: "chunk size too small", mutat: func(p *metadata.Pool) { p.ChunkSize = 1 }},
		{name: "chunk size too large", mutat: func(p *metadata.Pool) { p.ChunkSize = 1 << 40 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newPool(t, "pool-"+tt.name)
			tt.mutat(&pool)
			if err := s.CreatePool(t.Context(), pool); !isInvalid(err) {
				t.Fatalf("CreatePool() error = %v, want metadata.ErrInvalid", err)
			}
		})
	}
}

func TestCreatePoolWithoutIDReturnsInvalid(t *testing.T) {
	s := newStore(t)

	pool := newPool(t, "primary")
	pool.ID = ""
	if err := s.CreatePool(t.Context(), pool); !isInvalid(err) {
		t.Fatalf("CreatePool() error = %v, want metadata.ErrInvalid", err)
	}
}

func TestCreateAndGetNode(t *testing.T) {
	s := newStore(t)
	want := newNode(t, "drive-a")
	want.LastSeen = ptr(testTime)

	if err := s.CreateNode(t.Context(), want); err != nil {
		t.Fatalf("CreateNode() error = %v, want nil", err)
	}

	got, err := s.GetNode(t.Context(), want.ID)
	if err != nil {
		t.Fatalf("GetNode() error = %v, want nil", err)
	}
	if got.Name != want.Name {
		t.Errorf("GetNode() Name = %q, want %q", got.Name, want.Name)
	}
	if got.Provider != metadata.ProviderGoogleDrive {
		t.Errorf("GetNode() Provider = %q, want %q", got.Provider, metadata.ProviderGoogleDrive)
	}
	if got.Status != metadata.NodeStatusHealthy {
		t.Errorf("GetNode() Status = %q, want %q", got.Status, metadata.NodeStatusHealthy)
	}
	if got.Capacity != want.Capacity || got.UsedCapacity != want.UsedCapacity {
		t.Errorf("GetNode() capacity = %d/%d, want %d/%d",
			got.Capacity, got.UsedCapacity, want.Capacity, want.UsedCapacity)
	}
	if got.LastSeen == nil || !got.LastSeen.Equal(testTime) {
		t.Errorf("GetNode() LastSeen = %v, want %v", got.LastSeen, testTime)
	}
}

func TestGetNodeWithNoLastSeenReturnsNil(t *testing.T) {
	s := newStore(t)
	node := newNode(t, "drive-a")

	if err := s.CreateNode(t.Context(), node); err != nil {
		t.Fatalf("CreateNode() error = %v, want nil", err)
	}

	got, err := s.GetNode(t.Context(), node.ID)
	if err != nil {
		t.Fatalf("GetNode() error = %v, want nil", err)
	}
	if got.LastSeen != nil {
		t.Errorf("LastSeen = %v, want nil for a node that was never reached", got.LastSeen)
	}
}

func TestGetMissingNodeReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if _, err := s.GetNode(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("GetNode() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreateNodeWithDuplicateNameReturnsConflict(t *testing.T) {
	s := newStore(t)
	mustCreateNode(t, s, "drive-a")

	if err := s.CreateNode(t.Context(), newNode(t, "drive-a")); !isConflict(err) {
		t.Fatalf("CreateNode() duplicate name error = %v, want metadata.ErrConflict", err)
	}
}

func TestCreateNodeWithDuplicateAccountReturnsConflict(t *testing.T) {
	s := newStore(t)
	first := mustCreateNode(t, s, "drive-a")

	clone := newNode(t, "drive-a-again")
	clone.AccountIdentifier = first.AccountIdentifier

	if err := s.CreateNode(t.Context(), clone); !isConflict(err) {
		t.Fatalf("CreateNode() duplicate account error = %v, want metadata.ErrConflict", err)
	}
}

func TestCreateNodeWithUnknownStatusReturnsInvalid(t *testing.T) {
	s := newStore(t)

	node := newNode(t, "drive-a")
	node.Status = metadata.NodeStatus("exploded")
	if err := s.CreateNode(t.Context(), node); !isInvalid(err) {
		t.Fatalf("CreateNode() error = %v, want metadata.ErrInvalid", err)
	}
}

func TestUpdateNodeStatus(t *testing.T) {
	s := newStore(t)
	node := mustCreateNode(t, s, "drive-a")

	for _, status := range metadata.AllNodeStatuses {
		node.Status = status
		node.UpdatedAt = testTime.Add(time.Duration(len(status)) * time.Minute)

		if err := s.UpdateNode(t.Context(), node); err != nil {
			t.Fatalf("UpdateNode(%q) error = %v, want nil", status, err)
		}

		got, err := s.GetNode(t.Context(), node.ID)
		if err != nil {
			t.Fatalf("GetNode() error = %v, want nil", err)
		}
		if got.Status != status {
			t.Errorf("GetNode() Status = %q, want %q", got.Status, status)
		}
	}
}

func TestDeleteNode(t *testing.T) {
	s := newStore(t)
	node := mustCreateNode(t, s, "drive-a")

	if err := s.DeleteNode(t.Context(), node.ID); err != nil {
		t.Fatalf("DeleteNode() error = %v, want nil", err)
	}
	if _, err := s.GetNode(t.Context(), node.ID); !isNotFound(err) {
		t.Errorf("GetNode() after deletion error = %v, want metadata.ErrNotFound", err)
	}
}

func TestDeleteNodeHoldingChunksReturnsConflict(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")
	if err := s.CreateChunks(t.Context(), []metadata.Chunk{newChunk(t, file.ID, node.ID, 0)}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	if err := s.DeleteNode(t.Context(), node.ID); !isConflict(err) {
		t.Fatalf("DeleteNode() error = %v, want metadata.ErrConflict", err)
	}
	if _, err := s.GetNode(t.Context(), node.ID); err != nil {
		t.Errorf("GetNode() error = %v, want the node to still exist", err)
	}
}

func TestDeleteNodeCascadesPoolMembership(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	node := mustCreateNode(t, s, "drive-a")
	if err := s.AddNodeToPool(t.Context(), pool.ID, node.ID, testTime); err != nil {
		t.Fatalf("AddNodeToPool() error = %v, want nil", err)
	}

	if err := s.DeleteNode(t.Context(), node.ID); err != nil {
		t.Fatalf("DeleteNode() error = %v, want nil", err)
	}

	member, err := s.IsNodeInPool(t.Context(), pool.ID, node.ID)
	if err != nil {
		t.Fatalf("IsNodeInPool() error = %v, want nil", err)
	}
	if member {
		t.Error("IsNodeInPool() = true after the node was deleted, want false")
	}
}

func TestDeleteMissingNodeReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if err := s.DeleteNode(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("DeleteNode() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestListNodesIsOrderedAndEmptySafe(t *testing.T) {
	s := newStore(t)

	got, err := s.ListNodes(t.Context())
	if err != nil {
		t.Fatalf("ListNodes() error = %v, want nil", err)
	}
	if got == nil {
		t.Fatal("ListNodes() = nil, want an empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("ListNodes() = %d nodes, want 0", len(got))
	}

	first := mustCreateNode(t, s, "drive-a")
	second := mustCreateNode(t, s, "drive-b")

	got, err = s.ListNodes(t.Context())
	if err != nil {
		t.Fatalf("ListNodes() error = %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListNodes() = %d nodes, want 2", len(got))
	}
	if got[0].ID != first.ID || got[1].ID != second.ID {
		t.Errorf("ListNodes() = [%s %s], want [%s %s]", got[0].ID, got[1].ID, first.ID, second.ID)
	}
}

func TestPoolMembership(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	nodeA := mustCreateNode(t, s, "drive-a")
	nodeB := mustCreateNode(t, s, "drive-b")

	member, err := s.IsNodeInPool(t.Context(), pool.ID, nodeA.ID)
	if err != nil {
		t.Fatalf("IsNodeInPool() error = %v, want nil", err)
	}
	if member {
		t.Error("IsNodeInPool() = true before adding, want false")
	}

	for _, node := range []metadata.Node{nodeA, nodeB} {
		if err := s.AddNodeToPool(t.Context(), pool.ID, node.ID, testTime); err != nil {
			t.Fatalf("AddNodeToPool(%s) error = %v, want nil", node.Name, err)
		}
		member, err := s.IsNodeInPool(t.Context(), pool.ID, node.ID)
		if err != nil {
			t.Fatalf("IsNodeInPool() error = %v, want nil", err)
		}
		if !member {
			t.Errorf("IsNodeInPool(%s) = false after adding, want true", node.Name)
		}
	}

	nodes, err := s.ListPoolNodes(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolNodes() error = %v, want nil", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListPoolNodes() = %d nodes, want 2", len(nodes))
	}
	if nodes[0].ID != nodeA.ID || nodes[1].ID != nodeB.ID {
		t.Errorf("ListPoolNodes() = [%s %s], want [%s %s]",
			nodes[0].ID, nodes[1].ID, nodeA.ID, nodeB.ID)
	}

	pools, err := s.ListPoolsForNode(t.Context(), nodeA.ID)
	if err != nil {
		t.Fatalf("ListPoolsForNode() error = %v, want nil", err)
	}
	if len(pools) != 1 || pools[0].ID != pool.ID {
		t.Errorf("ListPoolsForNode() = %v, want just the primary pool", pools)
	}

	if err := s.RemoveNodeFromPool(t.Context(), pool.ID, nodeA.ID); err != nil {
		t.Fatalf("RemoveNodeFromPool() error = %v, want nil", err)
	}
	member, err = s.IsNodeInPool(t.Context(), pool.ID, nodeA.ID)
	if err != nil {
		t.Fatalf("IsNodeInPool() error = %v, want nil", err)
	}
	if member {
		t.Error("IsNodeInPool() = true after removal, want false")
	}
}

func TestListPoolNodesOnEmptyPoolReturnsEmptySlice(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	nodes, err := s.ListPoolNodes(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolNodes() error = %v, want nil", err)
	}
	if nodes == nil {
		t.Fatal("ListPoolNodes() = nil, want an empty slice")
	}
	if len(nodes) != 0 {
		t.Errorf("ListPoolNodes() = %d nodes, want 0", len(nodes))
	}
}

func TestAddNodeToPoolTwiceReturnsConflict(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	node := mustCreateNode(t, s, "drive-a")

	if err := s.AddNodeToPool(t.Context(), pool.ID, node.ID, testTime); err != nil {
		t.Fatalf("first AddNodeToPool() error = %v, want nil", err)
	}
	if err := s.AddNodeToPool(t.Context(), pool.ID, node.ID, testTime); !isConflict(err) {
		t.Fatalf("second AddNodeToPool() error = %v, want metadata.ErrConflict", err)
	}
}

func TestAddNodeToPoolWithUnknownPoolOrNodeReturnsNotFound(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	node := mustCreateNode(t, s, "drive-a")

	if err := s.AddNodeToPool(t.Context(), id.New(), node.ID, testTime); !isNotFound(err) {
		t.Errorf("AddNodeToPool() with an unknown pool error = %v, want metadata.ErrNotFound", err)
	}
	if err := s.AddNodeToPool(t.Context(), pool.ID, id.New(), testTime); !isNotFound(err) {
		t.Errorf("AddNodeToPool() with an unknown node error = %v, want metadata.ErrNotFound", err)
	}
}

func TestAddNodeToPoolWithEmptyIDsReturnsInvalid(t *testing.T) {
	s := newStore(t)

	if err := s.AddNodeToPool(t.Context(), "", id.New(), testTime); !isInvalid(err) {
		t.Errorf("AddNodeToPool() with an empty pool id error = %v, want metadata.ErrInvalid", err)
	}
	if err := s.AddNodeToPool(t.Context(), id.New(), "", testTime); !isInvalid(err) {
		t.Errorf("AddNodeToPool() with an empty node id error = %v, want metadata.ErrInvalid", err)
	}
}

func TestRemoveNodeFromPoolTwiceReturnsNotFound(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	node := mustCreateNode(t, s, "drive-a")

	if err := s.AddNodeToPool(t.Context(), pool.ID, node.ID, testTime); err != nil {
		t.Fatalf("AddNodeToPool() error = %v, want nil", err)
	}
	if err := s.RemoveNodeFromPool(t.Context(), pool.ID, node.ID); err != nil {
		t.Fatalf("first RemoveNodeFromPool() error = %v, want nil", err)
	}
	if err := s.RemoveNodeFromPool(t.Context(), pool.ID, node.ID); !isNotFound(err) {
		t.Fatalf("second RemoveNodeFromPool() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestListPoolNodesOnUnknownPoolReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if _, err := s.ListPoolNodes(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("ListPoolNodes() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestOneNodeCanServeSeveralPools(t *testing.T) {
	s := newStore(t)
	first := mustCreatePool(t, s, "first")
	second := mustCreatePool(t, s, "second")
	node := mustCreateNode(t, s, "drive-a")

	for _, pool := range []metadata.Pool{first, second} {
		if err := s.AddNodeToPool(t.Context(), pool.ID, node.ID, testTime); err != nil {
			t.Fatalf("AddNodeToPool(%s) error = %v, want nil", pool.Name, err)
		}
	}

	pools, err := s.ListPoolsForNode(t.Context(), node.ID)
	if err != nil {
		t.Fatalf("ListPoolsForNode() error = %v, want nil", err)
	}
	if len(pools) != 2 {
		t.Fatalf("ListPoolsForNode() = %d pools, want 2", len(pools))
	}
}

func TestCreateAndGetFile(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	want := newFile(t, pool.ID, "report.pdf")

	if err := s.CreateFile(t.Context(), want); err != nil {
		t.Fatalf("CreateFile() error = %v, want nil", err)
	}

	got, err := s.GetFile(t.Context(), want.ID)
	if err != nil {
		t.Fatalf("GetFile() error = %v, want nil", err)
	}
	if got.ID != want.ID || got.PoolID != want.PoolID || got.Name != want.Name {
		t.Errorf("GetFile() = %+v, want the created values", got)
	}
	if got.Size != want.Size {
		t.Errorf("GetFile() Size = %d, want %d", got.Size, want.Size)
	}
	if got.ContentHash != want.ContentHash {
		t.Errorf("GetFile() ContentHash = %q, want %q", got.ContentHash, want.ContentHash)
	}
	if got.Status != metadata.FileStatusCommitted {
		t.Errorf("GetFile() Status = %q, want %q", got.Status, metadata.FileStatusCommitted)
	}
	if got.DataChunks != want.DataChunks || got.ParityChunks != want.ParityChunks {
		t.Errorf("GetFile() K/M = %d/%d, want %d/%d",
			got.DataChunks, got.ParityChunks, want.DataChunks, want.ParityChunks)
	}
}

func TestGetMissingFileReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if _, err := s.GetFile(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("GetFile() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreateFileInUnknownPoolReturnsNotFound(t *testing.T) {
	s := newStore(t)

	file := newFile(t, id.New(), "orphan.pdf")
	if err := s.CreateFile(t.Context(), file); !isNotFound(err) {
		t.Fatalf("CreateFile() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreateFileWithoutIDReturnsInvalid(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	file := newFile(t, pool.ID, "report.pdf")
	file.ID = ""
	if err := s.CreateFile(t.Context(), file); !isInvalid(err) {
		t.Fatalf("CreateFile() error = %v, want metadata.ErrInvalid", err)
	}
}

func TestListPoolFilesIsNewestFirst(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	older := newFile(t, pool.ID, "older.pdf")
	older.CreatedAt = testTime
	newer := newFile(t, pool.ID, "newer.pdf")
	newer.CreatedAt = testTime.Add(time.Hour)

	for _, file := range []metadata.File{older, newer} {
		if err := s.CreateFile(t.Context(), file); err != nil {
			t.Fatalf("CreateFile(%s) error = %v, want nil", file.Name, err)
		}
	}

	files, err := s.ListPoolFiles(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() error = %v, want nil", err)
	}
	if len(files) != 2 {
		t.Fatalf("ListPoolFiles() = %d files, want 2", len(files))
	}
	if files[0].Name != "newer.pdf" {
		t.Errorf("ListPoolFiles()[0] = %q, want %q", files[0].Name, "newer.pdf")
	}
}

func TestListPoolFilesOnEmptyPoolReturnsEmptySlice(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	files, err := s.ListPoolFiles(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() error = %v, want nil", err)
	}
	if files == nil {
		t.Fatal("ListPoolFiles() = nil, want an empty slice")
	}
	if len(files) != 0 {
		t.Errorf("ListPoolFiles() = %d files, want 0", len(files))
	}
}

func TestListPoolFilesDoesNotLeakAcrossPools(t *testing.T) {
	s := newStore(t)
	first := mustCreatePool(t, s, "first")
	second := mustCreatePool(t, s, "second")
	mustCreateFile(t, s, first.ID, "in-first.pdf")
	mustCreateFile(t, s, second.ID, "in-second.pdf")

	files, err := s.ListPoolFiles(t.Context(), first.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() error = %v, want nil", err)
	}
	if len(files) != 1 {
		t.Fatalf("ListPoolFiles() = %d files, want 1", len(files))
	}
	if files[0].Name != "in-first.pdf" {
		t.Errorf("ListPoolFiles() = %q, want %q", files[0].Name, "in-first.pdf")
	}
}

func TestUpdateFileStatus(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	file := newFile(t, pool.ID, "report.pdf")
	file.Status = metadata.FileStatusUploading
	file.ContentHash = ""
	if err := s.CreateFile(t.Context(), file); err != nil {
		t.Fatalf("CreateFile() error = %v, want nil", err)
	}

	file.Status = metadata.FileStatusCommitted
	file.ContentHash = "abcd"
	file.UpdatedAt = testTime.Add(time.Minute)
	if err := s.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() error = %v, want nil", err)
	}

	got, err := s.GetFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFile() error = %v, want nil", err)
	}
	if got.Status != metadata.FileStatusCommitted {
		t.Errorf("GetFile() Status = %q, want %q", got.Status, metadata.FileStatusCommitted)
	}
}

func TestFileCanBeMarkedDegraded(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")

	file.Status = metadata.FileStatusDegraded
	if err := s.UpdateFile(t.Context(), file); err != nil {
		t.Fatalf("UpdateFile() error = %v, want nil", err)
	}

	got, err := s.GetFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFile() error = %v, want nil", err)
	}
	if got.Status != metadata.FileStatusDegraded {
		t.Errorf("Status = %q, want %q", got.Status, metadata.FileStatusDegraded)
	}
}

func TestCommittingAFileWithoutAContentHashIsRejected(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	file := newFile(t, pool.ID, "report.pdf")
	file.Status = metadata.FileStatusUploading
	file.ContentHash = ""
	if err := s.CreateFile(t.Context(), file); err != nil {
		t.Fatalf("CreateFile() error = %v, want nil", err)
	}

	file.Status = metadata.FileStatusCommitted
	if err := s.UpdateFile(t.Context(), file); !isInvalid(err) {
		t.Fatalf("UpdateFile() error = %v, want metadata.ErrInvalid", err)
	}

	got, err := s.GetFile(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFile() error = %v, want nil", err)
	}
	if got.Status == metadata.FileStatusCommitted {
		t.Error("file was marked committed without a content hash")
	}
}

func TestDeleteFileCascadesToChunks(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")
	if err := s.CreateChunks(t.Context(), []metadata.Chunk{newChunk(t, file.ID, node.ID, 0)}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	if err := s.DeleteFile(t.Context(), file.ID); err != nil {
		t.Fatalf("DeleteFile() error = %v, want nil", err)
	}

	if _, err := s.GetFile(t.Context(), file.ID); !isNotFound(err) {
		t.Errorf("GetFile() after deletion error = %v, want metadata.ErrNotFound", err)
	}

	if err := s.DeleteNode(t.Context(), node.ID); err != nil {
		t.Errorf("DeleteNode() after the file was removed error = %v, want nil", err)
	}
}

func TestDeleteMissingFileReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if err := s.DeleteFile(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("DeleteFile() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreateAndGetChunksInIndexOrder(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	chunks := []metadata.Chunk{
		newChunk(t, file.ID, node.ID, 2),
		newChunk(t, file.ID, node.ID, 0),
		newChunk(t, file.ID, node.ID, 1),
	}
	if err := s.CreateChunks(t.Context(), chunks); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	got, err := s.GetFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFileChunks() error = %v, want nil", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetFileChunks() = %d chunks, want 3", len(got))
	}
	for i, chunk := range got {
		if chunk.Index != i {
			t.Errorf("GetFileChunks()[%d].Index = %d, want %d", i, chunk.Index, i)
		}
		if chunk.ChunkType != metadata.ChunkTypeData {
			t.Errorf("GetFileChunks()[%d].ChunkType = %q, want %q",
				i, chunk.ChunkType, metadata.ChunkTypeData)
		}
		if chunk.RemoteFileID == "" {
			t.Errorf("GetFileChunks()[%d].RemoteFileID is empty", i)
		}
	}
}

func TestGetFileChunksOrdersByStripeThenIndex(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "big.bin")
	node := mustCreateNode(t, s, "drive-a")

	chunks := []metadata.Chunk{
		newChunkInStripe(t, file.ID, node.ID, 2, 1),
		newChunkInStripe(t, file.ID, node.ID, 0, 1),
		newChunkInStripe(t, file.ID, node.ID, 1, 0),
		newChunkInStripe(t, file.ID, node.ID, 0, 0),
		newChunkInStripe(t, file.ID, node.ID, 2, 0),
		newChunkInStripe(t, file.ID, node.ID, 1, 1),
	}
	if err := s.CreateChunks(t.Context(), chunks); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	got, err := s.GetFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFileChunks() error = %v, want nil", err)
	}
	if len(got) != len(chunks) {
		t.Fatalf("GetFileChunks() = %d chunks, want %d", len(got), len(chunks))
	}

	type key struct{ stripe, index int }
	want := []key{{0, 0}, {0, 1}, {1, 0}, {1, 1}, {2, 0}, {2, 1}}
	for i, chunk := range got {
		if chunk.StripeIndex != want[i].stripe || chunk.Index != want[i].index {
			t.Errorf("GetFileChunks()[%d] = (stripe %d, index %d), want (stripe %d, index %d)",
				i, chunk.StripeIndex, chunk.Index, want[i].stripe, want[i].index)
		}
	}

	if err := s.CreateChunks(t.Context(), []metadata.Chunk{
		newChunkInStripe(t, file.ID, node.ID, 3, 0),
	}); err != nil {
		t.Errorf("a shard at index 0 of a new stripe must be allowed: %v", err)
	}
}

func TestCreateChunksRejectsADuplicateWithinAStripe(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	chunks := []metadata.Chunk{
		newChunkInStripe(t, file.ID, node.ID, 1, 2),
		newChunkInStripe(t, file.ID, node.ID, 1, 2),
	}
	if err := s.CreateChunks(t.Context(), chunks); !isConflict(err) {
		t.Fatalf("CreateChunks() error = %v, want metadata.ErrConflict", err)
	}
}

func TestGetNodeChunksOrdersByStripe(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "big.bin")
	node := mustCreateNode(t, s, "drive-a")

	if err := s.CreateChunks(t.Context(), []metadata.Chunk{
		newChunkInStripe(t, file.ID, node.ID, 1, 0),
		newChunkInStripe(t, file.ID, node.ID, 0, 1),
		newChunkInStripe(t, file.ID, node.ID, 0, 0),
	}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	got, err := s.GetNodeChunks(t.Context(), node.ID)
	if err != nil {
		t.Fatalf("GetNodeChunks() error = %v, want nil", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetNodeChunks() = %d chunks, want 3", len(got))
	}

	for i, want := range []int{0, 0, 1} {
		if got[i].StripeIndex != want {
			t.Errorf("GetNodeChunks()[%d].StripeIndex = %d, want %d", i, got[i].StripeIndex, want)
		}
	}
}

func TestCreateChunksWithNoChunksIsANoOp(t *testing.T) {
	s := newStore(t)

	if err := s.CreateChunks(t.Context(), nil); err != nil {
		t.Fatalf("CreateChunks(nil) error = %v, want nil", err)
	}
}

func TestCreateChunksWithDuplicateIndexReturnsConflict(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	chunks := []metadata.Chunk{
		newChunk(t, file.ID, node.ID, 0),
		newChunk(t, file.ID, node.ID, 0),
	}
	if err := s.CreateChunks(t.Context(), chunks); !isConflict(err) {
		t.Fatalf("CreateChunks() error = %v, want metadata.ErrConflict", err)
	}
}

func TestCreateChunksIsAtomic(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	chunks := []metadata.Chunk{
		newChunk(t, file.ID, node.ID, 0),
		newChunk(t, file.ID, node.ID, 1),
		newChunk(t, file.ID, node.ID, 0),
	}
	if err := s.CreateChunks(t.Context(), chunks); !isConflict(err) {
		t.Fatalf("CreateChunks() error = %v, want metadata.ErrConflict", err)
	}

	got, err := s.GetFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFileChunks() error = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetFileChunks() = %d chunks, want 0 (the batch must be atomic)", len(got))
	}
}

func TestCreateChunksRejectsUnknownFileOrNode(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	orphanFile := newChunk(t, id.New(), node.ID, 0)
	if err := s.CreateChunks(t.Context(), []metadata.Chunk{orphanFile}); !isNotFound(err) {
		t.Errorf("CreateChunks() with an unknown file error = %v, want metadata.ErrNotFound", err)
	}

	orphanNode := newChunk(t, file.ID, id.New(), 0)
	if err := s.CreateChunks(t.Context(), []metadata.Chunk{orphanNode}); !isNotFound(err) {
		t.Errorf("CreateChunks() with an unknown node error = %v, want metadata.ErrNotFound", err)
	}
}

func TestCreateChunksRejectsInvalidRecord(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	tests := []struct {
		name  string
		mutat func(*metadata.Chunk)
	}{
		{name: "no id", mutat: func(c *metadata.Chunk) { c.ID = "" }},
		{name: "negative index", mutat: func(c *metadata.Chunk) { c.Index = -1 }},
		{name: "no remote id", mutat: func(c *metadata.Chunk) { c.RemoteFileID = "" }},
		{name: "negative size", mutat: func(c *metadata.Chunk) { c.Size = -1 }},
		{name: "no hash", mutat: func(c *metadata.Chunk) { c.Hash = "" }},
		{name: "bad type", mutat: func(c *metadata.Chunk) { c.ChunkType = metadata.ChunkType("shard") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := newChunk(t, file.ID, node.ID, 0)
			tt.mutat(&chunk)
			if err := s.CreateChunks(t.Context(), []metadata.Chunk{chunk}); !isInvalid(err) {
				t.Fatalf("CreateChunks() error = %v, want metadata.ErrInvalid", err)
			}
		})
	}
}

func TestGetNodeChunks(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	nodeA := mustCreateNode(t, s, "drive-a")
	nodeB := mustCreateNode(t, s, "drive-b")

	if err := s.CreateChunks(t.Context(), []metadata.Chunk{
		newChunk(t, file.ID, nodeA.ID, 0),
		newChunk(t, file.ID, nodeA.ID, 1),
		newChunk(t, file.ID, nodeB.ID, 2),
	}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	got, err := s.GetNodeChunks(t.Context(), nodeA.ID)
	if err != nil {
		t.Fatalf("GetNodeChunks() error = %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetNodeChunks() = %d chunks, want 2", len(got))
	}
	for _, chunk := range got {
		if chunk.NodeID != nodeA.ID {
			t.Errorf("GetNodeChunks() returned a chunk for node %q, want only %q", chunk.NodeID, nodeA.ID)
		}
	}

	if _, err := s.GetNodeChunks(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("GetNodeChunks() on an unknown node error = %v, want metadata.ErrNotFound", err)
	}
}

func TestReplaceChunkReassignsNode(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	original := mustCreateNode(t, s, "drive-a")
	replacement := mustCreateNode(t, s, "drive-c")

	chunk := newChunk(t, file.ID, original.ID, 3)
	if err := s.CreateChunks(t.Context(), []metadata.Chunk{chunk}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	chunk.NodeID = replacement.ID
	chunk.RemoteFileID = "recovered-object"
	chunk.Size = 4096
	chunk.Hash = "bbbb"
	if err := s.ReplaceChunk(t.Context(), chunk); err != nil {
		t.Fatalf("ReplaceChunk() error = %v, want nil", err)
	}

	got, err := s.GetFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFileChunks() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("GetFileChunks() = %d chunks, want 1", len(got))
	}
	if got[0].NodeID != replacement.ID {
		t.Errorf("GetFileChunks()[0].NodeID = %q, want %q", got[0].NodeID, replacement.ID)
	}
	if got[0].RemoteFileID != "recovered-object" {
		t.Errorf("GetFileChunks()[0].RemoteFileID = %q, want %q", got[0].RemoteFileID, "recovered-object")
	}
	if got[0].Size != 4096 || got[0].Hash != "bbbb" {
		t.Errorf("GetFileChunks()[0] = size %d hash %q, want size 4096 hash %q",
			got[0].Size, got[0].Hash, "bbbb")
	}
}

func TestReplaceMissingChunkReturnsNotFound(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	chunk := newChunk(t, file.ID, node.ID, 0)
	if err := s.ReplaceChunk(t.Context(), chunk); !isNotFound(err) {
		t.Fatalf("ReplaceChunk() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestDeleteFileChunks(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	if err := s.CreateChunks(t.Context(), []metadata.Chunk{
		newChunk(t, file.ID, node.ID, 0),
		newChunk(t, file.ID, node.ID, 1),
	}); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	if err := s.DeleteFileChunks(t.Context(), file.ID); err != nil {
		t.Fatalf("DeleteFileChunks() error = %v, want nil", err)
	}

	got, err := s.GetFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("GetFileChunks() error = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("GetFileChunks() = %d chunks, want 0", len(got))
	}

	if _, err := s.GetFile(t.Context(), file.ID); err != nil {
		t.Errorf("GetFile() after DeleteFileChunks error = %v, want nil", err)
	}
}

func TestCountFileChunks(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	file := mustCreateFile(t, s, pool.ID, "report.pdf")
	node := mustCreateNode(t, s, "drive-a")

	data, parity, err := s.CountFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("CountFileChunks() error = %v, want nil", err)
	}
	if data != 0 || parity != 0 {
		t.Errorf("CountFileChunks() = %d/%d, want 0/0", data, parity)
	}

	chunks := []metadata.Chunk{
		newChunk(t, file.ID, node.ID, 0),
		newChunk(t, file.ID, node.ID, 1),
		newChunk(t, file.ID, node.ID, 2),
	}
	chunks[2].ChunkType = metadata.ChunkTypeParity
	if err := s.CreateChunks(t.Context(), chunks); err != nil {
		t.Fatalf("CreateChunks() error = %v, want nil", err)
	}

	data, parity, err = s.CountFileChunks(t.Context(), file.ID)
	if err != nil {
		t.Fatalf("CountFileChunks() error = %v, want nil", err)
	}
	if data != 2 || parity != 1 {
		t.Errorf("CountFileChunks() = %d data / %d parity, want 2/1", data, parity)
	}
}

func TestGetFileChunksOnUnknownFileReturnsNotFound(t *testing.T) {
	s := newStore(t)

	if _, err := s.GetFileChunks(t.Context(), id.New()); !isNotFound(err) {
		t.Errorf("GetFileChunks() error = %v, want metadata.ErrNotFound", err)
	}
}

func TestWithTxCommits(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	err := s.WithTx(t.Context(), func(ctx context.Context, tx metadata.Store) error {
		for _, name := range []string{"a", "b", "c"} {
			file := newFile(t, pool.ID, name+".pdf")
			if err := tx.CreateFile(ctx, file); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx() error = %v, want nil", err)
	}

	files, err := s.ListPoolFiles(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() error = %v, want nil", err)
	}
	if len(files) != 3 {
		t.Fatalf("ListPoolFiles() = %d files, want 3", len(files))
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")
	sentinel := errors.New("abort")

	err := s.WithTx(t.Context(), func(ctx context.Context, tx metadata.Store) error {
		file := newFile(t, pool.ID, "doomed.pdf")
		if err := tx.CreateFile(ctx, file); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx() error = %v, want the callback's error", err)
	}

	files, err := s.ListPoolFiles(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() error = %v, want nil", err)
	}
	if len(files) != 0 {
		t.Fatalf("ListPoolFiles() = %d files, want 0 (the transaction must roll back)", len(files))
	}
}

func TestWithTxRollsBackOnPanic(t *testing.T) {
	s := newStore(t)
	pool := mustCreatePool(t, s, "primary")

	func() {
		defer func() {
			if recover() == nil {
				t.Error("WithTx() did not propagate the panic")
			}
		}()
		_ = s.WithTx(t.Context(), func(ctx context.Context, tx metadata.Store) error {
			if err := tx.CreateFile(ctx, newFile(t, pool.ID, "doomed.pdf")); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	files, err := s.ListPoolFiles(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListPoolFiles() error = %v, want nil", err)
	}
	if len(files) != 0 {
		t.Fatalf("ListPoolFiles() = %d files, want 0", len(files))
	}
}

func TestWithTxRejectsNestedTransaction(t *testing.T) {
	s := newStore(t)

	err := s.WithTx(t.Context(), func(ctx context.Context, tx metadata.Store) error {
		return tx.WithTx(ctx, func(context.Context, metadata.Store) error { return nil })
	})
	if err == nil {
		t.Fatal("WithTx() nesting error = nil, want an error")
	}
}

func TestPing(t *testing.T) {
	s := newStore(t)

	if err := s.Ping(t.Context()); err != nil {
		t.Fatalf("Ping() error = %v, want nil", err)
	}
}

func TestPingFailsAfterClose(t *testing.T) {
	s := newStore(t)

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := s.Ping(t.Context()); err == nil {
		t.Error("Ping() after Close() = nil, want an error")
	}
}

func TestStoreHonoursContextCancellation(t *testing.T) {
	s := newStore(t)
	mustCreatePool(t, s, "primary")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := s.ListPools(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("ListPools() with a cancelled context = %v, want context.Canceled", err)
	}
	if err := s.CreatePool(ctx, newPool(t, "another")); !errors.Is(err, context.Canceled) {
		t.Errorf("CreatePool() with a cancelled context = %v, want context.Canceled", err)
	}
}

func ptr[T any](v T) *T { return &v }
