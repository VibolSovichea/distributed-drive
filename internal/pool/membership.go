package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/VibolSovichea/distributed-drive/internal/id"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)









var ErrHasData = errors.New("pool: the node still holds chunks")


var ErrAlreadyAttached = errors.New("pool: the node is already in this pool")








func (m *Manager) AddNode(ctx context.Context, poolID, nodeID string) (metadata.Node, error) {
	if !id.IsValid(poolID) {
		return metadata.Node{}, fmt.Errorf("%w: pool id %q is not a valid ULID", metadata.ErrInvalid, poolID)
	}
	if !id.IsValid(nodeID) {
		return metadata.Node{}, fmt.Errorf("%w: node id %q is not a valid ULID", metadata.ErrInvalid, nodeID)
	}

	if _, err := m.store.GetPool(ctx, poolID); err != nil {
		return metadata.Node{}, fmt.Errorf("pool: get %s: %w", poolID, err)
	}

	node, err := m.store.GetNode(ctx, nodeID)
	if err != nil {
		return metadata.Node{}, fmt.Errorf("pool: get node %s: %w", nodeID, err)
	}

	attached, err := m.store.IsNodeInPool(ctx, poolID, nodeID)
	if err != nil {
		return metadata.Node{}, fmt.Errorf("pool: check membership of %s in %s: %w", nodeID, poolID, err)
	}
	if attached {
		return node, fmt.Errorf("%w: node %s in pool %s", ErrAlreadyAttached, nodeID, poolID)
	}

	
	
	if err := m.store.AddNodeToPool(ctx, poolID, nodeID, m.now().UTC()); err != nil {
		return metadata.Node{}, fmt.Errorf("pool: attach node %s to %s: %w", nodeID, poolID, err)
	}

	return node, nil
}







func (m *Manager) RemoveNode(ctx context.Context, poolID, nodeID string) error {
	if !id.IsValid(poolID) {
		return fmt.Errorf("%w: pool id %q is not a valid ULID", metadata.ErrInvalid, poolID)
	}
	if !id.IsValid(nodeID) {
		return fmt.Errorf("%w: node id %q is not a valid ULID", metadata.ErrInvalid, nodeID)
	}

	if _, err := m.store.GetPool(ctx, poolID); err != nil {
		return fmt.Errorf("pool: get %s: %w", poolID, err)
	}
	if _, err := m.store.GetNode(ctx, nodeID); err != nil {
		return fmt.Errorf("pool: get node %s: %w", nodeID, err)
	}

	chunks, err := m.store.GetNodeChunks(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("pool: list chunks held by %s: %w", nodeID, err)
	}
	if len(chunks) > 0 {
		return fmt.Errorf("%w: node %s holds %d chunk(s); empty the pool first",
			ErrHasData, nodeID, len(chunks))
	}

	attached, err := m.store.IsNodeInPool(ctx, poolID, nodeID)
	if err != nil {
		return fmt.Errorf("pool: check membership of %s in %s: %w", nodeID, poolID, err)
	}
	if !attached {
		
		
		return nil
	}

	if err := m.store.RemoveNodeFromPool(ctx, poolID, nodeID); err != nil {
		return fmt.Errorf("pool: detach node %s from %s: %w", nodeID, poolID, err)
	}

	return nil
}


func (m *Manager) ListNodes(ctx context.Context, poolID string) ([]metadata.Node, error) {
	if !id.IsValid(poolID) {
		return nil, fmt.Errorf("%w: pool id %q is not a valid ULID", metadata.ErrInvalid, poolID)
	}

	if _, err := m.store.GetPool(ctx, poolID); err != nil {
		return nil, fmt.Errorf("pool: get %s: %w", poolID, err)
	}

	nodes, err := m.store.ListPoolNodes(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("pool: list nodes of %s: %w", poolID, err)
	}

	return nodes, nil
}
