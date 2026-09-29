package sqlite

import (
	"context"
	"database/sql"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

const chunkColumns = `id, file_id, stripe_index, chunk_index, node_id, remote_file_id,
	size, hash, chunk_type, created_at`

const (
	insertChunkSQL = `INSERT INTO chunks (` + chunkColumns + `)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	getFileChunksSQL = `SELECT ` + chunkColumns + ` FROM chunks
		WHERE file_id = ?
		ORDER BY stripe_index ASC, chunk_index ASC`

	getNodeChunksSQL = `SELECT ` + chunkColumns + ` FROM chunks
		WHERE node_id = ?
		ORDER BY file_id ASC, stripe_index ASC, chunk_index ASC`

	replaceChunkSQL = `UPDATE chunks
		SET node_id = ?, remote_file_id = ?, size = ?, hash = ?, created_at = ?
		WHERE id = ?`

	deleteFileChunksSQL = `DELETE FROM chunks WHERE file_id = ?`

	countFileChunksSQL = `SELECT chunk_type, COUNT(*) FROM chunks
		WHERE file_id = ?
		GROUP BY chunk_type`
)

func (s *Store) CreateChunks(ctx context.Context, chunks []metadata.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}

	for _, chunk := range chunks {
		if err := chunk.Validate(); err != nil {
			return err
		}
		if err := s.requireExists(ctx, "file", existFileSQL, chunk.FileID); err != nil {
			return err
		}
		if err := s.requireExists(ctx, "node", existNodeSQL, chunk.NodeID); err != nil {
			return err
		}
	}

	return s.inTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, chunk := range chunks {
			_, err := tx.ExecContext(ctx, insertChunkSQL,
				chunk.ID, chunk.FileID, chunk.StripeIndex, chunk.Index, chunk.NodeID,
				chunk.RemoteFileID, chunk.Size, chunk.Hash,
				string(chunk.ChunkType), chunk.CreatedAt.UnixNano(),
			)
			if err != nil {
				return classify("create chunk", err)
			}
		}
		return nil
	})
}

func (s *Store) GetFileChunks(ctx context.Context, fileID string) ([]metadata.Chunk, error) {
	if err := s.requireExists(ctx, "file", existFileSQL, fileID); err != nil {
		return nil, err
	}

	rows, err := s.query(ctx, "get file chunks", getFileChunksSQL, fileID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	chunks := make([]metadata.Chunk, 0)
	for rows.Next() {
		chunk, err := scanChunk(rows)
		if err != nil {
			return nil, classify("get file chunks", err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("get file chunks", err)
	}
	return chunks, nil
}

func (s *Store) GetNodeChunks(ctx context.Context, nodeID string) ([]metadata.Chunk, error) {
	if err := s.requireExists(ctx, "node", existNodeSQL, nodeID); err != nil {
		return nil, err
	}

	rows, err := s.query(ctx, "get node chunks", getNodeChunksSQL, nodeID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	chunks := make([]metadata.Chunk, 0)
	for rows.Next() {
		chunk, err := scanChunk(rows)
		if err != nil {
			return nil, classify("get node chunks", err)
		}
		chunks = append(chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("get node chunks", err)
	}
	return chunks, nil
}

func (s *Store) ReplaceChunk(ctx context.Context, chunk metadata.Chunk) error {
	if err := chunk.Validate(); err != nil {
		return err
	}
	if err := s.requireExists(ctx, "node", existNodeSQL, chunk.NodeID); err != nil {
		return err
	}

	res, err := s.exec(ctx, "replace chunk", replaceChunkSQL,
		chunk.NodeID, chunk.RemoteFileID, chunk.Size, chunk.Hash,
		chunk.CreatedAt.UnixNano(), chunk.ID,
	)
	if err != nil {
		return err
	}
	return requireOneRow(res, "replace chunk", "chunk", chunk.ID)
}

func (s *Store) DeleteFileChunks(ctx context.Context, fileID string) error {
	_, err := s.exec(ctx, "delete file chunks", deleteFileChunksSQL, fileID)
	return err
}

func (s *Store) CountFileChunks(ctx context.Context, fileID string) (int, int, error) {
	rows, err := s.query(ctx, "count file chunks", countFileChunksSQL, fileID)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = rows.Close() }()

	var data, parity int
	for rows.Next() {
		var (
			chunkType string
			count     int
		)
		if err := rows.Scan(&chunkType, &count); err != nil {
			return 0, 0, classify("count file chunks", err)
		}
		switch metadata.ChunkType(chunkType) {
		case metadata.ChunkTypeData:
			data = count
		case metadata.ChunkTypeParity:
			parity = count
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, classify("count file chunks", err)
	}
	return data, parity, nil
}

func scanChunk(row scanner) (metadata.Chunk, error) {
	var (
		chunk     metadata.Chunk
		chunkType string
		createdAt int64
	)

	if err := row.Scan(
		&chunk.ID, &chunk.FileID, &chunk.StripeIndex, &chunk.Index, &chunk.NodeID,
		&chunk.RemoteFileID, &chunk.Size, &chunk.Hash, &chunkType, &createdAt,
	); err != nil {
		return metadata.Chunk{}, err
	}

	chunk.ChunkType = metadata.ChunkType(chunkType)
	chunk.CreatedAt = unixNanoToTime(createdAt)
	return chunk, nil
}
