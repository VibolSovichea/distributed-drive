package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

const nodeColumns = `id, name, provider, account_identifier, status,
	capacity, used_capacity, last_seen, created_at, updated_at`

const (
	insertNodeSQL = `INSERT INTO nodes (` + nodeColumns + `)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	selectNodeSQL = `SELECT ` + nodeColumns + ` FROM nodes WHERE id = ?`

	listNodesSQL = `SELECT ` + nodeColumns + ` FROM nodes ORDER BY id ASC`

	updateNodeSQL = `UPDATE nodes
		SET name = ?, provider = ?, account_identifier = ?, status = ?,
		    capacity = ?, used_capacity = ?, last_seen = ?, updated_at = ?
		WHERE id = ?`

	deleteNodeSQL = `DELETE FROM nodes WHERE id = ?`
)

func (s *Store) CreateNode(ctx context.Context, node metadata.Node) error {
	if err := node.Validate(); err != nil {
		return err
	}

	_, err := s.exec(ctx, "create node", insertNodeSQL,
		node.ID, node.Name, string(node.Provider), node.AccountIdentifier, string(node.Status),
		node.Capacity, node.UsedCapacity, nullableTime(node.LastSeen),
		node.CreatedAt.UnixNano(), node.UpdatedAt.UnixNano(),
	)
	return err
}

func (s *Store) GetNode(ctx context.Context, id string) (metadata.Node, error) {
	node, err := scanNode(s.db.QueryRowContext(ctx, selectNodeSQL, id))
	if errors.Is(err, sql.ErrNoRows) {
		return metadata.Node{}, notFound("node", id)
	}
	if err != nil {
		return metadata.Node{}, classify("get node", err)
	}
	return node, nil
}

func (s *Store) ListNodes(ctx context.Context) ([]metadata.Node, error) {
	rows, err := s.query(ctx, "list nodes", listNodesSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	nodes := make([]metadata.Node, 0)
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, classify("list nodes", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("list nodes", err)
	}
	return nodes, nil
}

func (s *Store) UpdateNode(ctx context.Context, node metadata.Node) error {
	if err := node.Validate(); err != nil {
		return err
	}

	res, err := s.exec(ctx, "update node", updateNodeSQL,
		node.Name, string(node.Provider), node.AccountIdentifier, string(node.Status),
		node.Capacity, node.UsedCapacity, nullableTime(node.LastSeen),
		node.UpdatedAt.UnixNano(), node.ID,
	)
	if err != nil {
		return err
	}
	return requireOneRow(res, "update node", "node", node.ID)
}






func (s *Store) DeleteNode(ctx context.Context, id string) error {
	var held int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM chunks WHERE node_id = ?`, id).Scan(&held)
	if err != nil {
		return classify("count node chunks", err)
	}
	if held > 0 {
		return &NodeInUseError{NodeID: id, Chunks: held}
	}

	res, err := s.exec(ctx, "delete node", deleteNodeSQL, id)
	if err != nil {
		return err
	}
	return requireOneRow(res, "delete node", "node", id)
}


type NodeInUseError struct {
	NodeID string
	Chunks int
}

func (e *NodeInUseError) Error() string {
	return "sqlite: node " + strconv.Quote(e.NodeID) + " still holds " +
		strconv.Itoa(e.Chunks) + " chunk(s); move or delete those files first"
}

func (e *NodeInUseError) Is(target error) bool {
	return target == metadata.ErrConflict
}

func scanNode(row scanner) (metadata.Node, error) {
	var (
		node      metadata.Node
		provider  string
		status    string
		lastSeen  sql.NullInt64
		createdAt int64
		updatedAt int64
	)

	if err := row.Scan(
		&node.ID, &node.Name, &provider, &node.AccountIdentifier, &status,
		&node.Capacity, &node.UsedCapacity, &lastSeen, &createdAt, &updatedAt,
	); err != nil {
		return metadata.Node{}, err
	}

	node.Provider = metadata.Provider(provider)
	node.Status = metadata.NodeStatus(status)
	node.LastSeen = nullToTime(lastSeen)
	node.CreatedAt = unixNanoToTime(createdAt)
	node.UpdatedAt = unixNanoToTime(updatedAt)
	return node, nil
}

func nullableTime(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: t.UnixNano(), Valid: true}
}

func nullToTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := unixNanoToTime(v.Int64)
	return &t
}

func unixNanoToTime(ns int64) time.Time {
	return time.Unix(0, ns).UTC()
}
