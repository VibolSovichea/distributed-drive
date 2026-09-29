package placement

import (
	"errors"
	"fmt"
	"sort"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

var (
	ErrNotEnoughNodes = errors.New("placement: not enough usable nodes for one stripe")

	ErrNoNodeTakesShard = errors.New("placement: no node can accept the shard")

	ErrInvalidRequest = errors.New("placement: invalid request")
)

type Shard struct {
	Stripe int

	Index int

	Size int64
}

type Assignment struct {
	Shard

	NodeID string
}

type Request struct {
	Shards []Shard

	Nodes []metadata.Node

	StrictCapacity bool

	Avoid map[string]bool
}

func Plan(req Request) ([]Assignment, error) {
	if len(req.Shards) == 0 {
		return nil, fmt.Errorf("%w: no shards to place", ErrInvalidRequest)
	}
	if len(req.Nodes) == 0 {
		return nil, fmt.Errorf("%w: the pool has no nodes", ErrNotEnoughNodes)
	}

	ranked := rankNodes(req.Nodes)
	if len(ranked) == 0 {
		return nil, fmt.Errorf("%w: every node is offline, full or unauthenticated",
			ErrNotEnoughNodes)
	}

	groups, err := groupShards(req.Shards)
	if err != nil {
		return nil, err
	}

	perStripe := len(groups[0].shards)
	if len(ranked) < perStripe {
		return nil, fmt.Errorf("%w: a stripe needs %d distinct nodes but only %d are usable",
			ErrNotEnoughNodes, perStripe, len(ranked))
	}

	out := make([]Assignment, 0, len(req.Shards))
	used := make(map[string]int, perStripe)

	for _, group := range groups {

		clear(used)

		offset := 0
		if len(ranked) > perStripe {
			offset = (group.stripe * perStripe) % len(ranked)
		}

		for _, shard := range group.shards {
			node, err := chooseNode(rotate(ranked, offset), used, shard, req)
			if err != nil {
				return nil, err
			}
			used[node.ID]++
			out = append(out, Assignment{Shard: shard, NodeID: node.ID})
		}
	}

	return out, nil
}

type shardGroup struct {
	stripe int
	shards []Shard
}

func groupShards(shards []Shard) ([]shardGroup, error) {
	byStripe := make(map[int][]Shard)
	for _, shard := range shards {
		if shard.Stripe < 0 {
			return nil, fmt.Errorf("%w: negative stripe %d", ErrInvalidRequest, shard.Stripe)
		}
		if shard.Index < 0 {
			return nil, fmt.Errorf("%w: stripe %d has negative index %d",
				ErrInvalidRequest, shard.Stripe, shard.Index)
		}
		byStripe[shard.Stripe] = append(byStripe[shard.Stripe], shard)
	}

	stripes := make([]int, 0, len(byStripe))
	for stripe := range byStripe {
		stripes = append(stripes, stripe)
	}

	sort.Ints(stripes)

	groups := make([]shardGroup, 0, len(stripes))
	for _, stripe := range stripes {
		group := byStripe[stripe]
		sort.Slice(group, func(i, j int) bool { return group[i].Index < group[j].Index })

		for i := 1; i < len(group); i++ {
			if group[i].Index == group[i-1].Index {
				return nil, fmt.Errorf("%w: stripe %d has two shards at index %d",
					ErrInvalidRequest, stripe, group[i].Index)
			}
		}

		groups = append(groups, shardGroup{stripe: stripe, shards: group})
	}

	want := len(groups[0].shards)
	for _, group := range groups[1:] {
		if len(group.shards) != want {
			return nil, fmt.Errorf("%w: stripe %d has %d shards, stripe %d has %d",
				ErrInvalidRequest, groups[0].stripe, want, group.stripe, len(group.shards))
		}
	}

	return groups, nil
}

func rotate(nodes []ranked, offset int) []ranked {
	if offset <= 0 || offset >= len(nodes) {
		return nodes
	}
	out := make([]ranked, 0, len(nodes))
	out = append(out, nodes[offset:]...)
	out = append(out, nodes[:offset]...)
	return out
}

func chooseNode(ranked []ranked, used map[string]int, shard Shard, req Request) (metadata.Node, error) {
	for _, candidate := range ranked {

		if used[candidate.node.ID] > 0 {
			continue
		}
		if req.Avoid[candidate.node.ID] {
			continue
		}
		if !canHold(candidate, shard.Size, req) {
			continue
		}
		return candidate.node, nil
	}

	var wouldFit, excludedOnly int
	for _, candidate := range ranked {
		if used[candidate.node.ID] > 0 || !canHold(candidate, shard.Size, req) {
			continue
		}
		if req.Avoid[candidate.node.ID] {
			excludedOnly++
			continue
		}
		wouldFit++
	}

	switch {
	case wouldFit == 0 && excludedOnly > 0:
		return metadata.Node{}, fmt.Errorf(
			"%w: all %d usable nodes are excluded, and stripe %d index %d still needs one",
			ErrNotEnoughNodes, excludedOnly, shard.Stripe, shard.Index)
	case wouldFit == 0:
		return metadata.Node{}, fmt.Errorf(
			"%w: no node has %d bytes free for stripe %d index %d",
			ErrNoNodeTakesShard, shard.Size, shard.Stripe, shard.Index)
	default:

		return metadata.Node{}, fmt.Errorf(
			"%w: stripe %d needs %d distinct nodes and only %d are usable",
			ErrNotEnoughNodes, shard.Stripe, len(used)+1, len(ranked))
	}
}

func canHold(candidate ranked, size int64, req Request) bool {
	node := candidate.node

	if node.Capacity <= 0 {

		return !req.StrictCapacity
	}

	if node.UsedCapacity >= node.Capacity {
		return false
	}
	if size > 0 && node.UsedCapacity+size > node.Capacity {
		return false
	}
	return true
}

type ranked struct {
	node metadata.Node
	rank int
}

func rankNodes(nodes []metadata.Node) []ranked {
	out := make([]ranked, 0, len(nodes))
	for _, node := range nodes {
		rank, usable := healthRank(node)
		if !usable {
			continue
		}
		out = append(out, ranked{node: node, rank: rank})
	}

	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.rank != b.rank {
			return a.rank < b.rank
		}

		if a.node.Capacity > 0 && b.node.Capacity > 0 {
			freeA := a.node.Capacity - a.node.UsedCapacity
			freeB := b.node.Capacity - b.node.UsedCapacity
			if freeA != freeB {
				return freeA > freeB
			}
		}

		return a.node.ID < b.node.ID
	})

	return out
}

func healthRank(node metadata.Node) (rank int, usable bool) {
	switch node.Status {
	case metadata.NodeStatusHealthy:
		return 0, true
	case metadata.NodeStatusDegraded:

		return 1, true
	case metadata.NodeStatusUnknown:

		return 2, true
	default:

		return 0, false
	}
}
