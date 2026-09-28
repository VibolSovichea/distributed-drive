package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
)

const fileColumns = `id, pool_id, name, size, content_hash,
	chunk_size, data_chunks, parity_chunks, encrypted, status, created_at, updated_at`

const (
	insertFileSQL = `INSERT INTO files (` + fileColumns + `)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	selectFileSQL = `SELECT ` + fileColumns + ` FROM files WHERE id = ?`

	
	listPoolFilesSQL = `SELECT ` + fileColumns + ` FROM files
		WHERE pool_id = ?
		ORDER BY created_at DESC, id DESC`

	updateFileSQL = `UPDATE files
		SET pool_id = ?, name = ?, size = ?, content_hash = ?,
		    chunk_size = ?, data_chunks = ?, parity_chunks = ?, encrypted = ?,
		    status = ?, updated_at = ?
		WHERE id = ?`

	deleteFileSQL = `DELETE FROM files WHERE id = ?`
)

func (s *Store) CreateFile(ctx context.Context, file metadata.File) error {
	if err := file.Validate(); err != nil {
		return err
	}
	if file.ID == "" {
		return errMissingID("file")
	}
	if err := s.requireExists(ctx, "pool", existPoolSQL, file.PoolID); err != nil {
		return err
	}

	_, err := s.exec(ctx, "create file", insertFileSQL,
		file.ID, file.PoolID, file.Name, file.Size, file.ContentHash,
		file.ChunkSize, file.DataChunks, file.ParityChunks, boolToInt(file.Encrypted),
		string(file.Status), file.CreatedAt.UnixNano(), file.UpdatedAt.UnixNano(),
	)
	return err
}

func (s *Store) GetFile(ctx context.Context, id string) (metadata.File, error) {
	file, err := scanFile(s.db.QueryRowContext(ctx, selectFileSQL, id))
	if errors.Is(err, sql.ErrNoRows) {
		return metadata.File{}, notFound("file", id)
	}
	if err != nil {
		return metadata.File{}, classify("get file", err)
	}
	return file, nil
}

func (s *Store) ListPoolFiles(ctx context.Context, poolID string) ([]metadata.File, error) {
	if err := s.requireExists(ctx, "pool", existPoolSQL, poolID); err != nil {
		return nil, err
	}

	rows, err := s.query(ctx, "list pool files", listPoolFilesSQL, poolID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	files := make([]metadata.File, 0)
	for rows.Next() {
		file, err := scanFile(rows)
		if err != nil {
			return nil, classify("list pool files", err)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, classify("list pool files", err)
	}
	return files, nil
}

func (s *Store) UpdateFile(ctx context.Context, file metadata.File) error {
	if err := file.Validate(); err != nil {
		return err
	}
	if err := s.requireExists(ctx, "pool", existPoolSQL, file.PoolID); err != nil {
		return err
	}

	res, err := s.exec(ctx, "update file", updateFileSQL,
		file.PoolID, file.Name, file.Size, file.ContentHash,
		file.ChunkSize, file.DataChunks, file.ParityChunks, boolToInt(file.Encrypted),
		string(file.Status), file.UpdatedAt.UnixNano(), file.ID,
	)
	if err != nil {
		return err
	}
	return requireOneRow(res, "update file", "file", file.ID)
}

func (s *Store) DeleteFile(ctx context.Context, id string) error {
	res, err := s.exec(ctx, "delete file", deleteFileSQL, id)
	if err != nil {
		return err
	}
	return requireOneRow(res, "delete file", "file", id)
}

func scanFile(row scanner) (metadata.File, error) {
	var (
		file      metadata.File
		status    string
		encrypted int
		createdAt int64
		updatedAt int64
	)

	if err := row.Scan(
		&file.ID, &file.PoolID, &file.Name, &file.Size, &file.ContentHash,
		&file.ChunkSize, &file.DataChunks, &file.ParityChunks, &encrypted,
		&status, &createdAt, &updatedAt,
	); err != nil {
		return metadata.File{}, err
	}

	file.Encrypted = encrypted != 0
	file.Status = metadata.FileStatus(status)
	file.CreatedAt = unixNanoToTime(createdAt)
	file.UpdatedAt = unixNanoToTime(updatedAt)
	return file, nil
}
