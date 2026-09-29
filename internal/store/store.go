package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/chunker"
	"github.com/VibolSovichea/distributed-drive/internal/crypto"
	"github.com/VibolSovichea/distributed-drive/internal/ec"
	"github.com/VibolSovichea/distributed-drive/internal/id"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/placement"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

var (
	ErrNoUsableNodes = errors.New("store: the pool cannot hold a stripe")

	ErrUploadTruncated = errors.New("store: the upload ended early")

	ErrFileNotReadable = errors.New("store: the file is not readable in its current state")

	ErrFileCorrupt = errors.New("store: the file does not match its recorded checksum")

	ErrTooFewShards = errors.New("store: too few shards remain to rebuild a stripe")
)

type NodeResolver interface {
	Open(ctx context.Context, node metadata.Node) (provider.StorageNode, error)
}

type Options struct {
	Meta metadata.Store

	Nodes NodeResolver

	Now func() time.Time

	NewID func() string

	ShardConcurrency int

	CipherConfig *crypto.CipherConfig
}

type Store struct {
	meta  metadata.Store
	nodes NodeResolver

	now     func() time.Time
	newID   func() string
	concurr int
	cipher  *crypto.CipherConfig
}

func New(opts Options) (*Store, error) {
	if opts.Meta == nil {
		return nil, errors.New("store: a metadata store is required")
	}
	if opts.Nodes == nil {
		return nil, errors.New("store: a node resolver is required")
	}

	s := &Store{
		meta:    opts.Meta,
		nodes:   opts.Nodes,
		now:     opts.Now,
		newID:   opts.NewID,
		concurr: opts.ShardConcurrency,
		cipher:  opts.CipherConfig,
	}
	if s.now == nil {
		s.now = nowFunc
	}
	if s.newID == nil {
		s.newID = id.New
	}
	if s.concurr < 1 {
		s.concurr = 4
	}
	if s.cipher == nil {
		s.cipher = &crypto.CipherConfig{}
	}

	return s, nil
}

type UploadInput struct {
	PoolID string

	Name string

	ContentType string

	Source io.Reader

	Size int64
}

type uploadProgress struct {
	total int64

	stripes int

	digest hash.Hash
}

func (s *Store) Upload(ctx context.Context, in UploadInput) (metadata.File, error) {
	pool, err := s.meta.GetPool(ctx, in.PoolID)
	if err != nil {
		return metadata.File{}, fmt.Errorf("store: load the pool: %w", err)
	}
	if in.Name == "" {
		return metadata.File{}, fmt.Errorf("store: %w: a file name is required", metadata.ErrInvalid)
	}
	if in.Source == nil {
		return metadata.File{}, fmt.Errorf("store: %w: a source is required", metadata.ErrInvalid)
	}

	params := ec.Params{Data: pool.DataChunks, Parity: pool.ParityChunks}
	codec, err := ec.New(params)
	if err != nil {
		return metadata.File{}, fmt.Errorf("store: the pool's erasure parameters are unusable: %w", err)
	}

	splitter, err := chunker.NewSplitter(pool.ChunkSize)
	if err != nil {
		return metadata.File{}, fmt.Errorf("store: the pool's chunk size is unusable: %w", err)
	}

	file := metadata.File{
		ID:           s.newID(),
		PoolID:       pool.ID,
		Name:         in.Name,
		ChunkSize:    pool.ChunkSize,
		DataChunks:   pool.DataChunks,
		ParityChunks: pool.ParityChunks,
		Status:       metadata.FileStatusUploading,
		CreatedAt:    s.now(),
	}
	file.UpdatedAt = file.CreatedAt
	if err := s.meta.CreateFile(ctx, file); err != nil {
		return metadata.File{}, fmt.Errorf("store: record the file: %w", err)
	}

	progress, err := s.uploadStripes(ctx, in, file, pool, params, codec, splitter)
	if err != nil {

		return metadata.File{}, err
	}

	file.Size = progress.total
	file.ContentHash = hex.EncodeToString(progress.digest.Sum(nil))
	file.Status = metadata.FileStatusCommitted
	file.UpdatedAt = s.now()
	if err := s.meta.UpdateFile(ctx, file); err != nil {
		return metadata.File{}, fmt.Errorf("store: commit the file: %w", err)
	}

	return file, nil
}

func (s *Store) uploadStripes(
	ctx context.Context,
	in UploadInput,
	file metadata.File,
	pool metadata.Pool,
	params ec.Params,
	codec ec.Codec,
	splitter *chunker.Splitter,
) (uploadProgress, error) {
	var progress uploadProgress
	progress.digest = sha256.New()

	width := int(pool.ChunkSize)

	data := make([][]byte, pool.DataChunks)
	for i := range data {
		data[i] = make([]byte, width)
	}
	parity := params.Alloc(width)[params.Data:]

	shards := make([][]byte, 0, params.Shards())
	sizes := make([]int64, 0, params.Shards())

	sess := newSession(s.nodes, s.meta)

	for stripe := 0; ; stripe++ {

		clearAll(data)
		clearAll(parity)

		sizes = sizes[:0]
		filled := 0
		for filled < pool.DataChunks {
			chunk, err := splitter.Next(in.Source)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return progress, fmt.Errorf("store: read stripe %d: %w", stripe, err)
			}

			copy(data[filled], chunk.Data)
			sizes = append(sizes, chunk.Size)
			progress.digest.Write(chunk.Data)
			filled++
		}

		if filled == 0 {

			break
		}

		for range pool.DataChunks - filled {
			sizes = append(sizes, 0)
		}

		for range pool.ParityChunks {
			sizes = append(sizes, pool.ChunkSize)
		}

		shards = append(shards[:0], data...)
		shards = append(shards, parity...)
		if err := codec.Encode(shards); err != nil {
			return progress, fmt.Errorf("store: encode stripe %d: %w", stripe, err)
		}

		if err := s.writeStripe(ctx, sess, in, file, pool, stripe, shards, sizes); err != nil {
			return progress, err
		}
		progress.stripes++

		for i := range pool.DataChunks {
			progress.total += sizes[i]
		}

		file.Size = progress.total
		file.UpdatedAt = s.now()
		if err := s.meta.UpdateFile(ctx, file); err != nil {
			return progress, fmt.Errorf("store: record progress after stripe %d: %w", stripe, err)
		}
	}

	if in.Size > 0 && progress.total != in.Size {
		return progress, fmt.Errorf("%w: got %d bytes, the caller promised %d",
			ErrUploadTruncated, progress.total, in.Size)
	}

	return progress, nil
}

type shardResult struct {
	assignment placement.Assignment
	remote     provider.RemoteObject
	err        error
}

func (s *Store) writeStripe(
	ctx context.Context,
	sess *session,
	in UploadInput,
	file metadata.File,
	pool metadata.Pool,
	stripe int,
	shards [][]byte,
	sizes []int64,
) error {

	planned := make([]placement.Shard, 0, len(shards))
	for i, size := range sizes {
		planned = append(planned, placement.Shard{Stripe: stripe, Index: i, Size: size})
	}

	nodes, err := s.meta.ListPoolNodes(ctx, pool.ID)
	if err != nil {
		return fmt.Errorf("store: list the pool's nodes: %w", err)
	}

	plan, err := placement.Plan(placement.Request{Shards: planned, Nodes: nodes})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNoUsableNodes, err)
	}

	results := make([]shardResult, len(plan))
	runConcurrently(len(plan), s.concurr, func(i int) {
		assignment := plan[i]
		result := shardResult{assignment: assignment}

		var fileKey []byte
		if s.cipher.Enabled() && file.Encrypted {
			key, err := crypto.DeriveKey(s.cipher.MasterKey, file.ID)
			if err != nil {
				result.err = fmt.Errorf("store: derive file key: %w", err)
				results[i] = result
				return
			}
			fileKey = key
		}

		remote, err := s.storeShard(ctx, sess, in, file, pool,
			assignment.NodeID, stripe, assignment.Index, shards[assignment.Index], fileKey)
		if err != nil {
			result.err = err
			results[i] = result
			return
		}

		result.remote = remote
		results[i] = result
	})

	written := make([]shardResult, 0, len(results))
	var firstErr error
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		written = append(written, r)
	}
	if firstErr != nil {
		_ = s.rollback(ctx, sess, written)
		return firstErr
	}

	now := s.now()
	chunks := make([]metadata.Chunk, 0, len(written))
	for _, r := range written {
		shardType := metadata.ChunkTypeParity
		if r.assignment.Index < pool.DataChunks {
			shardType = metadata.ChunkTypeData
		}
		chunks = append(chunks, metadata.Chunk{
			ID:           s.newID(),
			FileID:       file.ID,
			NodeID:       r.assignment.NodeID,
			StripeIndex:  r.assignment.Stripe,
			Index:        r.assignment.Index,
			ChunkType:    shardType,
			RemoteFileID: r.remote.ID,
			Size:         r.assignment.Size,

			Hash:      chunker.Hash(shards[r.assignment.Index]),
			CreatedAt: now,
		})
	}

	if err := s.meta.CreateChunks(ctx, chunks); err != nil {

		_ = s.rollback(ctx, sess, written)
		return fmt.Errorf("store: record stripe %d: %w", stripe, err)
	}

	return nil
}

func (s *Store) rollback(ctx context.Context, sess *session, written []shardResult) error {
	var errs []error
	for _, w := range written {
		if err := sess.delete(ctx, w.assignment.NodeID, w.remote.ID); err != nil {
			errs = append(errs, fmt.Errorf("stripe %d shard %d on node %s: %w",
				w.assignment.Stripe, w.assignment.Index, w.assignment.NodeID, err))
		}
	}
	return errors.Join(errs...)
}

func clearAll(bufs [][]byte) {
	for _, b := range bufs {
		clear(b)
	}
}

func shardName(fileName string, stripe, index int) string {
	return fmt.Sprintf("%s.s%04d.sh%04d", fileName, stripe, index)
}

func shardDescription(fileID, poolID, hash string) string {
	return fmt.Sprintf("distributed-drive file=%s pool=%s sha256=%s", fileID, poolID, hash)
}
