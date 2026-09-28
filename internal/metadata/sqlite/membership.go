package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)




const (
	existPoolSQL = `SELECT 1 FROM pools WHERE id = ?`
	existNodeSQL = `SELECT 1 FROM nodes WHERE id = ?`
	existFileSQL = `SELECT 1 FROM files WHERE id = ?`
)

const (
	insertPoolNodeSQL = `INSERT INTO pool_nodes (pool_id, node_id, added_at)
		VALUES (?, ?, ?)`

	deletePoolNodeSQL = `DELETE FROM pool_nodes WHERE pool_id = ? AND node_id = ?`

	
	
	
	
	touchPoolSQL = `UPDATE pools SET updated_at = ? WHERE id = ?`

	
	
	listPoolNodesSQL = `SELECT n.id, n.name, n.provider, n.account_identifier, n.status,
			n.capacity, n.used_capacity, n.last_seen, n.created_at, n.updated_at
		FROM pool_nodes pn
		JOIN nodes n ON n.id = pn.node_id
		WHERE pn.pool_id = ?
		ORDER BY n.id ASC`

	listPoolsForNodeSQL = `SELECT p.id, p.name, p.data_chunks, p.parity_chunks, p.chunk_size,
		p.encrypted, p.created_at, p.updated_at
		FROM pool_nodes pn
		JOIN pools p ON p.id = pn.pool_id
		WHERE pn.node_id = ?
		ORDER BY p.id ASC`

	isNodeInPoolSQL = `SELECT EXISTS (
		SELECT 1 FROM pool_nodes WHERE pool_id = ? AND node_id = ?
	)`
)

func (s *Store) AddNodeToPool(ctx context.Context, poolID, nodeID string, at time.Time) error {
	if poolID == "" || nodeID == "" {
		return fmt.Errorf("%w: pool id and node id are both required", metadata.ErrInvalid)
	}

	if err := s.requireExists(ctx, "pool", existPoolSQL, poolID); err != nil {
		return err
	}
	if err := s.requireExists(ctx, "node", existNodeSQL, nodeID); err != nil {
		return err
	}

	
	
	
	
	
	
	return s.inTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, insertPoolNodeSQL,
			poolID, nodeID, at.UnixNano()); err != nil {
			return classify("add node to pool", err)
		}

		if _, err := tx.ExecContext(ctx, touchPoolSQL, at.UnixNano(), poolID); err != nil {
			return classify("touch pool", err)
		}
		return nil
	})
}

func (s *Store) RemoveNodeFromPool(ctx context.Context, poolID, nodeID string) error {
	return s.inTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, deletePoolNodeSQL, poolID, nodeID)
		if err != nil {
			return classify("remove node from pool", err)
		}
		if err := requireOneRow(res, "remove node from pool", "pool node", poolID+"/"+nodeID); err != nil {
			return err
		}

		now := time.Now().UTC().UnixNano()
		if _, err := tx.ExecContext(ctx, touchPoolSQL, now, poolID); err != nil {
			return classify("touch pool", err)
		}
		return nil
	})
}

func (s *Store) ListPoolNodes(ctx context.Context, poolID string) ([]metadata.Node, error) {
	if err := s.requireExists(ctx, "pool", existPoolSQL, poolID); err != nil {
		return nil, err
	}

	rows, err := s.query(ctx, "list pool nodes", listPoolNodesSQL, poolID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	nodes := make([]metadata.Node, 0)
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, classify("list pool nodes", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("list pool nodes", err)
	}
	return nodes, nil
}

func (s *Store) ListPoolsForNode(ctx context.Context, nodeID string) ([]metadata.Pool, error) {
	if err := s.requireExists(ctx, "node", existNodeSQL, nodeID); err != nil {
		return nil, err
	}

	rows, err := s.query(ctx, "list pools for node", listPoolsForNodeSQL, nodeID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	pools := make([]metadata.Pool, 0)
	for rows.Next() {
		pool, err := scanPool(rows)
		if err != nil {
			return nil, classify("list pools for node", err)
		}
		pools = append(pools, pool)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("list pools for node", err)
	}
	return pools, nil
}

func (s *Store) IsNodeInPool(ctx context.Context, poolID, nodeID string) (bool, error) {
	var member bool
	if err := s.db.QueryRowContext(ctx, isNodeInPoolSQL, poolID, nodeID).Scan(&member); err != nil {
		return false, classify("check pool membership", err)
	}
	return member, nil
}


func (s *Store) requireExists(ctx context.Context, entity, query, id string) error {
	var found int
	switch err := s.db.QueryRowContext(ctx, query, id).Scan(&found); {
	case errors.Is(err, sql.ErrNoRows):
		return notFound(entity, id)
	case err != nil:
		return classify("look up "+entity, err)
	}
	return nil
}
