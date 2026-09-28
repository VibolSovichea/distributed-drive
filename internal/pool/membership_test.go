package pool

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)


func newPoolWithNodes(t *testing.T, n int) (*Manager, *fakeStore, metadata.Pool, []metadata.Node) {
	t.Helper()

	store := newFakeStore()
	svc := NewManager(store, func() time.Time { return fixedNow })

	pool, err := svc.Create(t.Context(), validInput())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	nodes := make([]metadata.Node, 0, n)
	for i := 0; i < n; i++ {
		nodes = append(nodes, store.addNode(metadata.Node{
			ID:   fmt.Sprintf("01HQ00000000000000000000%02d", i),
			Name: fmt.Sprintf("node-%d", i),
		}))
	}

	return svc, store, pool, nodes
}

func TestAddNodeAttaches(t *testing.T) {
	svc, _, pool, nodes := newPoolWithNodes(t, 1)

	node, err := svc.AddNode(t.Context(), pool.ID, nodes[0].ID)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if node.ID != nodes[0].ID {
		t.Errorf("returned node id = %q, want %q", node.ID, nodes[0].ID)
	}

	attached, err := svc.ListNodes(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(attached) != 1 || attached[0].ID != nodes[0].ID {
		t.Errorf("attached = %+v, want just %s", attached, nodes[0].ID)
	}
}

func TestAddNodeTwiceIsRejected(t *testing.T) {
	svc, _, pool, nodes := newPoolWithNodes(t, 1)

	if _, err := svc.AddNode(t.Context(), pool.ID, nodes[0].ID); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	
	
	
	_, err := svc.AddNode(t.Context(), pool.ID, nodes[0].ID)
	if !errors.Is(err, ErrAlreadyAttached) {
		t.Errorf("error = %v, want ErrAlreadyAttached", err)
	}
}

func TestAddNodeRejectsBadIdentifiers(t *testing.T) {
	svc, _, pool, nodes := newPoolWithNodes(t, 1)

	if _, err := svc.AddNode(t.Context(), "not-a-ulid", nodes[0].ID); !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("bad pool id: error = %v, want ErrInvalid", err)
	}
	if _, err := svc.AddNode(t.Context(), pool.ID, "not-a-ulid"); !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("bad node id: error = %v, want ErrInvalid", err)
	}
}

func TestAddNodeRejectsUnknownPoolOrNode(t *testing.T) {
	svc, store, pool, nodes := newPoolWithNodes(t, 1)

	unknownPool := fmt.Sprintf("01HQ00000000000000000009%02d", 9)
	if _, err := svc.AddNode(t.Context(), unknownPool, nodes[0].ID); !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("unknown pool: error = %v, want ErrNotFound", err)
	}

	store.pools[pool.ID] = pool
	unknownNode := "01HQ0000000000000000000999"
	if _, err := svc.AddNode(t.Context(), pool.ID, unknownNode); !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("unknown node: error = %v, want ErrNotFound", err)
	}
}

func TestAddNodeAcceptsAnOfflineNode(t *testing.T) {
	svc, store, pool, _ := newPoolWithNodes(t, 0)

	
	
	
	offline := store.addNode(metadata.Node{
		ID: "01HQ0000000000000000000001", Name: "cold", Status: metadata.NodeStatusOffline,
	})

	if _, err := svc.AddNode(t.Context(), pool.ID, offline.ID); err != nil {
		t.Fatalf("AddNode on an offline node: %v", err)
	}
}

func TestRemoveNodeDetaches(t *testing.T) {
	svc, _, pool, nodes := newPoolWithNodes(t, 2)

	for _, n := range nodes {
		if _, err := svc.AddNode(t.Context(), pool.ID, n.ID); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	if err := svc.RemoveNode(t.Context(), pool.ID, nodes[0].ID); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	attached, err := svc.ListNodes(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(attached) != 1 || attached[0].ID != nodes[1].ID {
		t.Errorf("attached = %+v, want only %s", attached, nodes[1].ID)
	}
}

func TestRemoveNodeHoldingDataIsRefused(t *testing.T) {
	svc, store, pool, nodes := newPoolWithNodes(t, 2)

	for _, n := range nodes {
		if _, err := svc.AddNode(t.Context(), pool.ID, n.ID); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	
	
	
	
	store.chunksByNode[nodes[0].ID] = []metadata.Chunk{{ID: "c1", NodeID: nodes[0].ID}}

	err := svc.RemoveNode(t.Context(), pool.ID, nodes[0].ID)
	if !errors.Is(err, ErrHasData) {
		t.Fatalf("error = %v, want ErrHasData", err)
	}

	
	attached, err := svc.ListNodes(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(attached) != 2 {
		t.Errorf("the node was detached despite holding data: %+v", attached)
	}
}

func TestRemoveNodeIsIdempotent(t *testing.T) {
	svc, _, pool, nodes := newPoolWithNodes(t, 1)

	
	
	if err := svc.RemoveNode(t.Context(), pool.ID, nodes[0].ID); err != nil {
		t.Errorf("RemoveNode on a node that was never attached: %v", err)
	}
}

func TestRemoveNodeRejectsBadIdentifiers(t *testing.T) {
	svc, _, _, nodes := newPoolWithNodes(t, 1)

	if err := svc.RemoveNode(t.Context(), "nope", nodes[0].ID); !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
	if err := svc.RemoveNode(t.Context(), "01HQ0000000000000000000001", "nope"); !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
}

func TestListNodesOnAnUnknownPool(t *testing.T) {
	svc, _, _, _ := newPoolWithNodes(t, 0)

	if _, err := svc.ListNodes(t.Context(), "01HQ0000000000000000000099"); !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
	if _, err := svc.ListNodes(t.Context(), "nope"); !errors.Is(err, metadata.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
}

func TestCapacityCountsAttachedNodes(t *testing.T) {
	svc, store, pool, nodes := newPoolWithNodes(t, 3)

	
	before, err := svc.Capacity(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("Capacity: %v", err)
	}
	if before.Nodes != 0 || before.Usable {
		t.Errorf("an empty pool reported %+v", before)
	}

	for i, n := range nodes {
		store.allNodes[n.ID] = withCapacity(n, 1000, 100)
		if _, err := svc.AddNode(t.Context(), pool.ID, n.ID); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		_ = i
	}

	after, err := svc.Capacity(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("Capacity: %v", err)
	}
	if after.Nodes != 3 {
		t.Errorf("nodes = %d, want 3", after.Nodes)
	}
	if after.MissingNodes != 3 {
		t.Errorf("missing nodes = %d, want 3", after.MissingNodes)
	}
	if after.Usable {
		t.Error("three nodes cannot place a six-shard stripe")
	}
	if after.LogicalCapacity != 900 {
		t.Errorf("logical capacity = %d, want 900", after.LogicalCapacity)
	}
}

func TestRemoveNodeThenCapacity(t *testing.T) {
	svc, store, pool, nodes := newPoolWithNodes(t, 6)

	for _, n := range nodes {
		store.allNodes[n.ID] = withCapacity(n, 1000, 0)
		if _, err := svc.AddNode(t.Context(), pool.ID, n.ID); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	full, err := svc.Capacity(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("Capacity: %v", err)
	}
	if !full.Usable {
		t.Fatalf("six nodes with K+M=6 should be usable: %+v", full)
	}

	
	if err := svc.RemoveNode(t.Context(), pool.ID, nodes[0].ID); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	after, err := svc.Capacity(t.Context(), pool.ID)
	if err != nil {
		t.Fatalf("Capacity: %v", err)
	}
	if after.Usable {
		t.Error("the pool must stop being usable below K+M nodes")
	}
}

func withCapacity(n metadata.Node, capacity, used int64) metadata.Node {
	n.Capacity = capacity
	n.UsedCapacity = used
	return n
}
