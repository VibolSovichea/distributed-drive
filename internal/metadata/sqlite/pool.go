package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

const poolColumns = `id, name, data_chunks, parity_chunks, chunk_size, encrypted, created_at, updated_at`

const (
	insertPoolSQL = `INSERT INTO pools (` + poolColumns + `)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	selectPoolSQL = `SELECT ` + poolColumns + ` FROM pools WHERE id = ?`

	selectPoolByNameSQL = `SELECT ` + poolColumns + ` FROM pools WHERE name = ?`

	listPoolsSQL = `SELECT ` + poolColumns + ` FROM pools ORDER BY id ASC`

	updatePoolSQL = `UPDATE pools
		SET name = ?, data_chunks = ?, parity_chunks = ?, chunk_size = ?, encrypted = ?, updated_at = ?
		WHERE id = ?`

	deletePoolSQL = `DELETE FROM pools WHERE id = ?`
)

func (s *Store) CreatePool(ctx context.Context, pool metadata.Pool) error {
	if err := pool.Validate(); err != nil {
		return err
	}
	if pool.ID == "" {
		return fmt.Errorf("%w: pool id must be assigned before persisting", metadata.ErrInvalid)
	}

	_, err := s.exec(ctx, "create pool", insertPoolSQL,
		pool.ID, pool.Name, pool.DataChunks, pool.ParityChunks, pool.ChunkSize,
		boolToInt(pool.Encrypted),
		pool.CreatedAt.UnixNano(), pool.UpdatedAt.UnixNano(),
	)
	return err
}

func (s *Store) GetPool(ctx context.Context, id string) (metadata.Pool, error) {
	row := s.db.QueryRowContext(ctx, selectPoolSQL, id)
	pool, err := scanPool(row)
	if errors.Is(err, sql.ErrNoRows) {
		return metadata.Pool{}, notFound("pool", id)
	}
	if err != nil {
		return metadata.Pool{}, classify("get pool", err)
	}
	return pool, nil
}

func (s *Store) GetPoolByName(ctx context.Context, name string) (metadata.Pool, error) {
	row := s.db.QueryRowContext(ctx, selectPoolByNameSQL, name)
	pool, err := scanPool(row)
	if errors.Is(err, sql.ErrNoRows) {
		return metadata.Pool{}, notFound("pool", name)
	}
	if err != nil {
		return metadata.Pool{}, classify("get pool by name", err)
	}
	return pool, nil
}

func (s *Store) ListPools(ctx context.Context) ([]metadata.Pool, error) {
	rows, err := s.query(ctx, "list pools", listPoolsSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	pools := make([]metadata.Pool, 0)
	for rows.Next() {
		pool, err := scanPool(rows)
		if err != nil {
			return nil, classify("list pools", err)
		}
		pools = append(pools, pool)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("list pools", err)
	}
	return pools, nil
}

func (s *Store) UpdatePool(ctx context.Context, pool metadata.Pool) error {
	if err := pool.Validate(); err != nil {
		return err
	}

	res, err := s.exec(ctx, "update pool", updatePoolSQL,
		pool.Name, pool.DataChunks, pool.ParityChunks, pool.ChunkSize,
		boolToInt(pool.Encrypted),
		pool.UpdatedAt.UnixNano(), pool.ID,
	)
	if err != nil {
		return err
	}
	return requireOneRow(res, "update pool", "pool", pool.ID)
}

func (s *Store) DeletePool(ctx context.Context, id string) error {
	res, err := s.exec(ctx, "delete pool", deletePoolSQL, id)
	if err != nil {
		return err
	}
	return requireOneRow(res, "delete pool", "pool", id)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanPool(row scanner) (metadata.Pool, error) {
	var (
		pool      metadata.Pool
		encrypted int
		createdAt int64
		updatedAt int64
	)

	if err := row.Scan(
		&pool.ID, &pool.Name, &pool.DataChunks, &pool.ParityChunks, &pool.ChunkSize,
		&encrypted, &createdAt, &updatedAt,
	); err != nil {
		return metadata.Pool{}, err
	}

	pool.Encrypted = encrypted != 0
	pool.CreatedAt = unixNanoToTime(createdAt)
	pool.UpdatedAt = unixNanoToTime(updatedAt)
	return pool, nil
}

func requireOneRow(res sql.Result, op, entity, id string) error {
	affected, err := res.RowsAffected()
	if err != nil {

		return nil
	}
	if affected == 0 {
		return notFound(entity, id)
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
