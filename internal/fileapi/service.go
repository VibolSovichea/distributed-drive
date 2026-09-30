package fileapi

import (
	"context"
	"io"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/store"
)

type Service interface {
	Upload(ctx context.Context, poolID, name string, src io.Reader, size int64) (metadata.File, error)
	GetFile(ctx context.Context, fileID string) (metadata.File, error)
	Download(ctx context.Context, fileID string) (io.ReadCloser, error)
	RepairFile(ctx context.Context, fileID string) (store.RepairResult, error)
	ScrubFile(ctx context.Context, fileID string) (store.ScrubResult, error)
	RepairPool(ctx context.Context, poolID string) ([]store.RepairResult, error)
	ScrubPool(ctx context.Context, poolID string) ([]store.ScrubResult, error)
	SweepAbandoned(ctx context.Context, olderThan time.Duration) (int, error)
	ListFiles(ctx context.Context, poolID string) ([]metadata.File, error)
}
