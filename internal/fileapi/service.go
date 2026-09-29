package fileapi

import (
	"context"
	"io"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/store"
)

const maxUploadSize = 10 << 30

type MetaStore interface {
	GetFile(ctx context.Context, id string) (metadata.File, error)
	ListPoolFiles(ctx context.Context, poolID string) ([]metadata.File, error)
}

type Store interface {
	Upload(ctx context.Context, in store.UploadInput) (metadata.File, error)
	Download(ctx context.Context, fileID string) (io.ReadCloser, error)
	RepairFile(ctx context.Context, fileID string) (store.RepairResult, error)
	ScrubFile(ctx context.Context, fileID string) (store.ScrubResult, error)
	RepairPool(ctx context.Context, poolID string) ([]store.RepairResult, error)
	ScrubPool(ctx context.Context, poolID string) ([]store.ScrubResult, error)
	SweepAbandoned(ctx context.Context, olderThan time.Duration) (int, error)
}

type service struct {
	meta MetaStore
	st   Store
}

func newService(meta MetaStore, st Store) Service {
	return &service{meta: meta, st: st}
}

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

func (s *service) Upload(ctx context.Context, poolID, name string, src io.Reader, size int64) (metadata.File, error) {
	return s.st.Upload(ctx, store.UploadInput{
		PoolID:      poolID,
		Name:        name,
		Source:      src,
		Size:        size,
		ContentType: "application/octet-stream",
	})
}

func (s *service) GetFile(ctx context.Context, fileID string) (metadata.File, error) {
	return s.meta.GetFile(ctx, fileID)
}

func (s *service) Download(ctx context.Context, fileID string) (io.ReadCloser, error) {
	return s.st.Download(ctx, fileID)
}

func (s *service) RepairFile(ctx context.Context, fileID string) (store.RepairResult, error) {
	return s.st.RepairFile(ctx, fileID)
}

func (s *service) ScrubFile(ctx context.Context, fileID string) (store.ScrubResult, error) {
	return s.st.ScrubFile(ctx, fileID)
}

func (s *service) RepairPool(ctx context.Context, poolID string) ([]store.RepairResult, error) {
	return s.st.RepairPool(ctx, poolID)
}

func (s *service) ScrubPool(ctx context.Context, poolID string) ([]store.ScrubResult, error) {
	return s.st.ScrubPool(ctx, poolID)
}

func (s *service) SweepAbandoned(ctx context.Context, olderThan time.Duration) (int, error) {
	return s.st.SweepAbandoned(ctx, olderThan)
}

func (s *service) ListFiles(ctx context.Context, poolID string) ([]metadata.File, error) {
	return s.meta.ListPoolFiles(ctx, poolID)
}
