package placement

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

func node(id string, capacity, used int64) metadata.Node {
	return metadata.Node{
		ID:           id,
		Status:       metadata.NodeStatusHealthy,
		Capacity:     capacity,
		UsedCapacity: used,
	}
}

func nodes(n int) []metadata.Node {
	out := make([]metadata.Node, 0, n)
	for i := range n {
		out = append(out, node(fmt.Sprintf("n%d", i+1), 1000, 0))
	}
	return out
}

func shards(stripe, count int, size int64) []Shard {
	out := make([]Shard, 0, count)
	for i := range count {
		out = append(out, Shard{Stripe: stripe, Index: i, Size: size})
	}
	return out
}

func target(got []Assignment) []string {
	out := make([]string, 0, len(got))
	for _, a := range got {
		out = append(out, a.NodeID)
	}
	return out
}

func TestOneStripeLandsOnDistinctNodes(t *testing.T) {
	t.Parallel()

	got, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: nodes(6)})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("got %d assignments, want 6", len(got))
	}
	if !slices.IsSorted(target(got)) {
		t.Errorf("targets = %v, want the highest ranked nodes in order", target(got))
	}
	if dup := duplicate(target(got)); dup != "" {
		t.Errorf("node %s took two shards of one stripe", dup)
	}
}

func TestTwoShardsOfOneStripeNeverShareANode(t *testing.T) {
	t.Parallel()

	for _, stripeSize := range []int{2, 3, 6, 10, 17} {
		for _, extra := range []int{0, 1, 5} {
			pool := stripeSize + extra
			got, err := Plan(Request{
				Shards: shards(0, stripeSize, 10),
				Nodes:  nodes(pool),
			})
			if err != nil {
				t.Fatalf("stripe %d over %d nodes: %v", stripeSize, pool, err)
			}
			if dup := duplicate(target(got)); dup != "" {
				t.Errorf("stripe %d over %d nodes: node %s took two shards",
					stripeSize, pool, dup)
			}
		}
	}
}

func TestInterleavedStripesStillGetDistinctNodes(t *testing.T) {
	t.Parallel()

	var mixed []Shard
	for stripe := range 5 {
		mixed = append(mixed, shards(stripe, 6, 10)...)
	}
	slices.Reverse(mixed)

	got, err := Plan(Request{Shards: mixed, Nodes: nodes(6)})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(got) != len(mixed) {
		t.Fatalf("got %d assignments, want %d", len(got), len(mixed))
	}

	for stripe := range 5 {
		ids := make([]string, 0, 6)
		for _, a := range got {
			if a.Stripe == stripe {
				ids = append(ids, a.NodeID)
			}
		}
		if len(ids) != 6 {
			t.Errorf("stripe %d got %d assignments, want 6", stripe, len(ids))
		}
		if dup := duplicate(ids); dup != "" {
			t.Errorf("stripe %d: node %s took two shards", stripe, dup)
		}
	}
}

func TestPlanIsIndependentOfInputOrder(t *testing.T) {
	t.Parallel()

	base := append(shards(0, 6, 10), shards(1, 6, 10)...)
	want, err := Plan(Request{Shards: base, Nodes: nodes(9)})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	rng := rand.New(rand.NewSource(7))
	for range 50 {
		shuffled := slices.Clone(base)
		rng.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})

		got, err := Plan(Request{Shards: shuffled, Nodes: nodes(9)})
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if !slices.Equal(target(want), target(got)) {
			t.Fatalf("input order changed the plan:\n got %v\nwant %v", target(got), target(want))
		}
	}
}

func TestRotatesStripesWhenThePoolIsLargerThanAStripe(t *testing.T) {
	t.Parallel()

	got, err := Plan(Request{
		Shards: append(shards(0, 6, 10), shards(1, 6, 10)...),
		Nodes:  nodes(9),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	first := target(got[:6])
	second := target(got[6:])

	if want := []string{"n1", "n2", "n3", "n4", "n5", "n6"}; !slices.Equal(first, want) {
		t.Errorf("stripe 0 = %v, want %v", first, want)
	}
	if want := []string{"n7", "n8", "n9", "n1", "n2", "n3"}; !slices.Equal(second, want) {
		t.Errorf("stripe 1 = %v, want %v", second, want)
	}
}

func TestEveryNodeInAPoolSizedExactlyToAStripeIsUsed(t *testing.T) {
	t.Parallel()

	got, err := Plan(Request{
		Shards: append(shards(0, 6, 10), shards(1, 6, 10)...),
		Nodes:  nodes(6),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got[:6]), target(got[6:])) {
		t.Errorf("stripe 1 = %v, want it to reuse %v", target(got[6:]), target(got[:6]))
	}
}

func TestUnusableNodesAreSkipped(t *testing.T) {
	t.Parallel()

	pool := nodes(9)
	pool[1].Status = metadata.NodeStatusOffline
	pool[2].Status = metadata.NodeStatusFull
	pool[3].Status = metadata.NodeStatusAuthError

	got, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	for _, a := range got {
		if a.NodeID == "n2" || a.NodeID == "n3" || a.NodeID == "n4" {
			t.Errorf("shard %d went to unusable node %s", a.Index, a.NodeID)
		}
	}
}

func TestEveryNodeUnusableIsAnError(t *testing.T) {
	t.Parallel()

	pool := nodes(4)
	for i := range pool {
		pool[i].Status = metadata.NodeStatusOffline
	}

	_, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: pool})
	if !errors.Is(err, ErrNotEnoughNodes) {
		t.Errorf("err = %v, want ErrNotEnoughNodes", err)
	}
}

func TestDegradedAndUncheckedNodesAreUsedOnlyWhenHealthyOnesRunOut(t *testing.T) {
	t.Parallel()

	pool := nodes(6)
	pool[0].Status = metadata.NodeStatusDegraded
	pool[1].Status = metadata.NodeStatusUnknown

	got, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n3", "n4", "n5", "n6", "n1", "n2"}) {
		t.Errorf("targets = %v, want healthy nodes first, then n1 and n2", target(got))
	}
}

func TestHealthyIsPreferredOverDegradedRegardlessOfFreeSpace(t *testing.T) {
	t.Parallel()

	pool := nodes(3)
	pool[0].Status = metadata.NodeStatusDegraded
	pool[0].Capacity = 10_000_000
	pool[0].UsedCapacity = 0
	pool[1].Capacity = 100
	pool[1].UsedCapacity = 90

	got, err := Plan(Request{Shards: shards(0, 2, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if !slices.Equal(target(got), []string{"n3", "n2"}) {
		t.Errorf("targets = %v, want the two healthy nodes ahead of the degraded one", target(got))
	}
}

func TestMoreFreeSpaceIsPreferred(t *testing.T) {
	t.Parallel()

	pool := nodes(3)
	pool[0].Capacity, pool[0].UsedCapacity = 100, 90
	pool[1].Capacity, pool[1].UsedCapacity = 200, 10
	pool[2].Capacity, pool[2].UsedCapacity = 200, 0

	got, err := Plan(Request{Shards: shards(0, 3, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n3", "n2", "n1"}) {
		t.Errorf("targets = %v, want most free space first", target(got))
	}
}

func TestEqualCapacityFallsBackToNodeID(t *testing.T) {
	t.Parallel()

	got, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: nodes(6)})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if want := []string{"n1", "n2", "n3", "n4", "n5", "n6"}; !slices.Equal(target(got), want) {
		t.Errorf("targets = %v, want %v", target(got), want)
	}
}

func TestAFullNodeIsSkippedForANodeWithRoom(t *testing.T) {
	t.Parallel()

	pool := nodes(3)
	pool[0].Capacity, pool[0].UsedCapacity = 100, 100

	got, err := Plan(Request{Shards: shards(0, 2, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n2", "n3"}) {
		t.Errorf("targets = %v, want the full node skipped", target(got))
	}
}

func TestUnknownCapacityIsUsableButFullIsNot(t *testing.T) {
	t.Parallel()

	pool := nodes(2)
	pool[0].Capacity, pool[0].UsedCapacity = 0, 0

	got, err := Plan(Request{Shards: shards(0, 2, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n1", "n2"}) {
		t.Errorf("targets = %v, want the no-quota node used", target(got))
	}

	_, err = Plan(Request{Shards: shards(0, 2, 10), Nodes: pool, StrictCapacity: true})
	if !errors.Is(err, ErrNoNodeTakesShard) {
		t.Errorf("err = %v, want ErrNoNodeTakesShard", err)
	}
}

func TestANodeTooSmallForTheShardIsSkipped(t *testing.T) {
	t.Parallel()

	pool := nodes(4)
	pool[0].Capacity, pool[0].UsedCapacity = 100, 95

	got, err := Plan(Request{Shards: shards(0, 3, 10), Nodes: pool})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n2", "n3", "n4"}) {
		t.Errorf("targets = %v, want the too-small node skipped", target(got))
	}
}

func TestNoNodeHasRoomForTheShard(t *testing.T) {
	t.Parallel()

	pool := nodes(6)
	for i := range pool {
		pool[i].Capacity, pool[i].UsedCapacity = 100, 95
	}

	_, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: pool})
	if !errors.Is(err, ErrNoNodeTakesShard) {
		t.Errorf("err = %v, want ErrNoNodeTakesShard", err)
	}
}

func TestTooFewNodesForAStripe(t *testing.T) {
	t.Parallel()

	_, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: nodes(5)})
	if !errors.Is(err, ErrNotEnoughNodes) {
		t.Errorf("err = %v, want ErrNotEnoughNodes", err)
	}
}

func TestAvoidedNodesAreSkipped(t *testing.T) {
	t.Parallel()

	got, err := Plan(Request{
		Shards: shards(0, 3, 10),
		Nodes:  nodes(5),
		Avoid:  map[string]bool{"n1": true, "n2": true},
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n3", "n4", "n5"}) {
		t.Errorf("targets = %v, want the three unexcluded nodes", target(got))
	}
}

func TestAvoidingEveryNodeIsAnError(t *testing.T) {
	t.Parallel()

	all := map[string]bool{"n1": true}
	for i := 2; i <= 6; i++ {
		all[fmt.Sprintf("n%d", i)] = true
	}

	_, err := Plan(Request{Shards: shards(0, 6, 10), Nodes: nodes(6), Avoid: all})
	if !errors.Is(err, ErrNotEnoughNodes) {
		t.Errorf("err = %v, want ErrNotEnoughNodes", err)
	}
}

func TestRejectsMalformedRequests(t *testing.T) {
	t.Parallel()

	tests := map[string]Request{
		"no shards": {
			Shards: nil,
			Nodes:  nodes(6),
		},
		"no nodes": {
			Shards: shards(0, 6, 10),
			Nodes:  nil,
		},
		"negative stripe": {
			Shards: []Shard{{Stripe: -1, Index: 0, Size: 10}},
			Nodes:  nodes(6),
		},
		"negative index": {
			Shards: []Shard{{Stripe: 0, Index: -1, Size: 10}},
			Nodes:  nodes(6),
		},
		"duplicate index in a stripe": {
			Shards: []Shard{
				{Stripe: 0, Index: 0, Size: 10},
				{Stripe: 0, Index: 0, Size: 10},
			},
			Nodes: nodes(6),
		},
		"stripes of different sizes": {

			Shards: append(shards(0, 6, 10), shards(1, 4, 10)...),
			Nodes:  nodes(6),
		},
	}

	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := Plan(req); err == nil {
				t.Error("Plan accepted a malformed request")
			}
		})
	}
}

func TestEveryAssignmentKeepsItsShardIdentity(t *testing.T) {
	t.Parallel()

	want := append(shards(0, 6, 10), shards(1, 6, 20)...)
	want[9].Size = 33

	got, err := Plan(Request{Shards: want, Nodes: nodes(9)})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	for i, a := range got {

		if a.NodeID == "" {
			t.Errorf("assignment %d has no node", i)
		}
	}

	byKey := map[[2]int]Shard{}
	for _, s := range want {
		byKey[[2]int{s.Stripe, s.Index}] = s
	}
	for _, a := range got {
		orig, ok := byKey[[2]int{a.Stripe, a.Index}]
		if !ok {
			t.Fatalf("assignment for stripe %d index %d was never asked for", a.Stripe, a.Index)
		}
		if a.Size != orig.Size {
			t.Errorf("stripe %d index %d: size %d, want %d", a.Stripe, a.Index, a.Size, orig.Size)
		}
		delete(byKey, [2]int{a.Stripe, a.Index})
	}
	if len(byKey) != 0 {
		t.Errorf("shards left unassigned: %v", byKey)
	}
}

func TestAZeroByteShardStillNeedsItsOwnNode(t *testing.T) {
	t.Parallel()

	got, err := Plan(Request{
		Shards: []Shard{
			{Stripe: 0, Index: 0, Size: 10},
			{Stripe: 0, Index: 1, Size: 0},
		},
		Nodes: nodes(2),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if dup := duplicate(target(got)); dup != "" {
		t.Errorf("node %s took two shards", dup)
	}
}

func TestAZeroSizedShardStillSkipsAFullNode(t *testing.T) {
	t.Parallel()

	pool := nodes(3)
	pool[0].Capacity, pool[0].UsedCapacity = 100, 100

	got, err := Plan(Request{
		Shards: []Shard{
			{Stripe: 0, Index: 0, Size: 0},
			{Stripe: 0, Index: 1, Size: 50},
		},
		Nodes: pool,
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !slices.Equal(target(got), []string{"n2", "n3"}) {
		t.Errorf("targets = %v, want both shards off the full node", target(got))
	}
}

func duplicate(ids []string) string {
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return id
		}
		seen[id] = true
	}
	return ""
}
