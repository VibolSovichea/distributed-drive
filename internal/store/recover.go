package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VibolSovichea/distributed-drive/internal/chunker"
	"github.com/VibolSovichea/distributed-drive/internal/crypto"
	"github.com/VibolSovichea/distributed-drive/internal/ec"
	"github.com/VibolSovichea/distributed-drive/internal/metadata"
	"github.com/VibolSovichea/distributed-drive/internal/placement"
	"github.com/VibolSovichea/distributed-drive/internal/provider"
)

var ErrUnrepairable = errors.New("store: the file cannot be rebuilt")

type LostShard struct {
	FileID      string
	StripeIndex int
	Index       int
	NodeID      string

	Reason error
}

type RepairResult struct {
	FileID string

	Restored int

	Unrepaired []int

	Healthy bool
}

func (r RepairResult) Repaired() bool { return r.Restored == 0 && len(r.Unrepaired) == 0 }

func (s *Store) DetectLost(ctx context.Context, poolID string) ([]LostShard, error) {
	files, err := s.meta.ListPoolFiles(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("store: list the pool's files: %w", err)
	}

	var lost []LostShard
	for _, file := range files {

		if file.Status == metadata.FileStatusUploading || file.Status == metadata.FileStatusDeleting {
			continue
		}

		chunks, err := s.meta.GetFileChunks(ctx, file.ID)
		if err != nil {
			return nil, fmt.Errorf("store: load file %s's shards: %w", file.ID, err)
		}

		sess := newSession(s.nodes, s.meta)
		for _, chunk := range chunks {

			if chunk.RemoteFileID == "" {
				lost = append(lost, LostShard{
					FileID: file.ID, StripeIndex: chunk.StripeIndex, Index: chunk.Index,
					NodeID: chunk.NodeID, Reason: provider.NotFound(""),
				})
				continue
			}

			if err := statShard(ctx, sess, file, chunk); err != nil {
				lost = append(lost, LostShard{
					FileID: file.ID, StripeIndex: chunk.StripeIndex, Index: chunk.Index,
					NodeID: chunk.NodeID, Reason: err,
				})
			}
		}
	}

	return lost, nil
}

func statShard(ctx context.Context, sess *session, file metadata.File, chunk metadata.Chunk) error {
	handle, err := sess.node(ctx, chunk.NodeID)
	if err != nil {
		return err
	}

	remote, err := handle.Stat(ctx, chunk.RemoteFileID)
	if err != nil {
		return err
	}

	if remote.Size != file.ChunkSize {
		return fmt.Errorf("%w: object %s on node %s is %d bytes, want %d",
			provider.ErrInvalid, remote.ID, chunk.NodeID, remote.Size, file.ChunkSize)
	}
	return nil
}

func (s *Store) RepairFile(ctx context.Context, fileID string) (RepairResult, error) {
	file, err := s.meta.GetFile(ctx, fileID)
	if err != nil {
		return RepairResult{}, fmt.Errorf("store: load the file: %w", err)
	}
	switch file.Status {
	case metadata.FileStatusCommitted, metadata.FileStatusDegraded:
	default:
		return RepairResult{}, fmt.Errorf("%w: it is %s", ErrFileNotReadable, file.Status)
	}

	pool, err := s.meta.GetPool(ctx, file.PoolID)
	if err != nil {
		return RepairResult{}, fmt.Errorf("store: load the pool: %w", err)
	}

	chunks, err := s.meta.GetFileChunks(ctx, fileID)
	if err != nil {
		return RepairResult{}, fmt.Errorf("store: load the file's shards: %w", err)
	}

	stripes, err := indexStripes(file, chunks)
	if err != nil {
		return RepairResult{}, err
	}
	if len(stripes) == 0 {
		return RepairResult{}, fmt.Errorf("%w: file %s has no stripes", metadata.ErrInvalid, fileID)
	}

	params := ec.Params{Data: file.DataChunks, Parity: file.ParityChunks}
	codec, err := ec.New(params)
	if err != nil {
		return RepairResult{}, fmt.Errorf("store: the file's erasure parameters are unusable: %w", err)
	}

	nodes, err := s.meta.ListPoolNodes(ctx, pool.ID)
	if err != nil {
		return RepairResult{}, fmt.Errorf("store: list the pool's nodes: %w", err)
	}

	result := RepairResult{FileID: fileID, Healthy: true}
	sess := newSession(s.nodes, s.meta)

	for stripeIndex, group := range stripes {
		loaded, err := loadStripe(ctx, sess, s.cipher, file, params, codec, group, stripeIndex)
		if err != nil {

			if errors.Is(err, ErrTooFewShards) || errors.Is(err, ErrFileCorrupt) {
				result.Healthy = false
				result.Unrepaired = append(result.Unrepaired, stripeIndex)
				continue
			}
			return result, err
		}

		if loaded.Healthy() {
			continue
		}

		restored, err := s.restoreStripe(ctx, sess, file, pool, stripeIndex, group, loaded, nodes)
		if err != nil {

			s.markStatus(ctx, file.ID, metadata.FileStatusDegraded)
			return result, err
		}
		result.Restored += restored
	}

	if result.Healthy {
		s.markStatus(ctx, file.ID, metadata.FileStatusCommitted)
	} else {
		s.markStatus(ctx, file.ID, metadata.FileStatusDegraded)
	}

	return result, nil
}

type restoredShard struct {
	index  int
	nodeID string
	remote provider.RemoteObject
	chunk  metadata.Chunk
}

func (s *Store) restoreStripe(
	ctx context.Context,
	sess *session,
	file metadata.File,
	pool metadata.Pool,
	stripeIndex int,
	group []metadata.Chunk,
	loaded LoadedStripe,
	nodes []metadata.Node,
) (int, error) {

	avoid := make(map[string]bool, len(group))
	for _, chunk := range group {
		if chunk.NodeID != "" {
			avoid[chunk.NodeID] = true
		}
	}

	wanted := make([]placement.Shard, 0, len(loaded.Rebuilt))
	for _, index := range loaded.Rebuilt {
		wanted = append(wanted, placement.Shard{
			Stripe: stripeIndex,
			Index:  index,
			Size:   group[index].Size,
		})
	}

	plan, err := placement.Plan(placement.Request{Shards: wanted, Nodes: nodes, Avoid: avoid})
	if err != nil {

		return 0, fmt.Errorf(
			"store: pool %s has no spare account for a rebuilt shard of file %s: %w", pool.ID, file.ID, err)
	}

	restored := make([]restoredShard, 0, len(plan))

	var fileKey []byte
	if s.cipher.Enabled() && file.Encrypted {
		key, err := crypto.DeriveKey(s.cipher.MasterKey, file.ID)
		if err != nil {
			return 0, fmt.Errorf("store: derive file key for repair: %w", err)
		}
		fileKey = key
	}

	for _, assignment := range plan {
		index := assignment.Index
		plaintext := loaded.Shards[index]

		remote, err := s.storeShard(ctx, sess, UploadInput{}, file, pool,
			assignment.NodeID, stripeIndex, index, plaintext, fileKey)
		if err != nil {

			s.discardWritten(ctx, sess, restored)
			return 0, err
		}

		row := group[index]
		row.NodeID = assignment.NodeID
		row.RemoteFileID = remote.ID

		row.Hash = chunker.Hash(plaintext)
		row.CreatedAt = s.now()

		restored = append(restored, restoredShard{
			index: index, nodeID: assignment.NodeID, remote: remote, chunk: row,
		})
	}

	for _, p := range restored {
		if err := s.meta.ReplaceChunk(ctx, p.chunk); err != nil {
			return 0, fmt.Errorf("store: repoint stripe %d shard %d: %w", stripeIndex, p.index, err)
		}
	}

	for _, p := range restored {
		old := group[p.index]
		if old.RemoteFileID == "" || (old.NodeID == p.nodeID && old.RemoteFileID == p.remote.ID) {
			continue
		}

		_ = sess.delete(ctx, old.NodeID, old.RemoteFileID)
	}

	return len(restored), nil
}

func (s *Store) discardWritten(ctx context.Context, sess *session, written []restoredShard) {
	for _, p := range written {
		_ = sess.delete(ctx, p.nodeID, p.remote.ID)
	}
}

func (s *Store) markStatus(ctx context.Context, fileID string, status metadata.FileStatus) {
	file, err := s.meta.GetFile(ctx, fileID)
	if err != nil {
		return
	}
	if file.Status == status {
		return
	}
	file.Status = status
	file.UpdatedAt = s.now()
	_ = s.meta.UpdateFile(ctx, file)
}

func (s *Store) RepairPool(ctx context.Context, poolID string) ([]RepairResult, error) {
	files, err := s.meta.ListPoolFiles(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("store: list the pool's files: %w", err)
	}

	results := make([]RepairResult, 0, len(files))
	for _, file := range files {
		switch file.Status {
		case metadata.FileStatusCommitted, metadata.FileStatusDegraded:
		default:

			continue
		}

		result, err := s.RepairFile(ctx, file.ID)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}

	return results, nil
}

func (s *Store) SweepAbandoned(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := s.now().Add(-olderThan)

	pools, err := s.meta.ListPools(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: list pools to sweep: %w", err)
	}

	swept := 0
	for _, pool := range pools {
		files, err := s.meta.ListPoolFiles(ctx, pool.ID)
		if err != nil {
			return swept, fmt.Errorf("store: list pool %s's files: %w", pool.ID, err)
		}

		for _, file := range files {
			if file.Status != metadata.FileStatusUploading || file.UpdatedAt.After(cutoff) {
				continue
			}
			if err := s.sweepFile(ctx, file); err != nil {
				return swept, err
			}
			swept++
		}
	}

	return swept, nil
}

func (s *Store) sweepFile(ctx context.Context, file metadata.File) error {
	chunks, err := s.meta.GetFileChunks(ctx, file.ID)
	if err != nil {
		return fmt.Errorf("store: load file %s's shards: %w", file.ID, err)
	}

	sess := newSession(s.nodes, s.meta)
	for _, chunk := range chunks {
		if chunk.RemoteFileID == "" {
			continue
		}

		_ = sess.delete(ctx, chunk.NodeID, chunk.RemoteFileID)
	}

	if err := s.meta.DeleteFileChunks(ctx, file.ID); err != nil {
		return fmt.Errorf("store: drop file %s's shard rows: %w", file.ID, err)
	}
	if err := s.meta.DeleteFile(ctx, file.ID); err != nil {
		return fmt.Errorf("store: drop file %s: %w", file.ID, err)
	}
	return nil
}

type ScrubResult struct {
	FileID   string
	Verified int
	Corrupt  int
	Missing  int
	Stripes  int
	Healthy  bool
}

func (s *Store) ScrubFile(ctx context.Context, fileID string) (ScrubResult, error) {
	file, err := s.meta.GetFile(ctx, fileID)
	if err != nil {
		return ScrubResult{}, fmt.Errorf("store: load the file: %w", err)
	}

	chunks, err := s.meta.GetFileChunks(ctx, fileID)
	if err != nil {
		return ScrubResult{}, fmt.Errorf("store: load the file's shards: %w", err)
	}

	stripes, err := indexStripes(file, chunks)
	if err != nil {
		return ScrubResult{}, err
	}
	if len(stripes) == 0 {
		return ScrubResult{FileID: fileID, Healthy: true}, nil
	}

	params := ec.Params{Data: file.DataChunks, Parity: file.ParityChunks}
	codec, err := ec.New(params)
	if err != nil {
		return ScrubResult{}, fmt.Errorf("store: the file's erasure parameters are unusable: %w", err)
	}

	sess := newSession(s.nodes, s.meta)
	result := ScrubResult{FileID: fileID, Stripes: len(stripes), Healthy: true}

	for stripeIndex, group := range stripes {
		loaded, err := loadStripe(ctx, sess, s.cipher, file, params, codec, group, stripeIndex)
		if err != nil {
			if errors.Is(err, ErrTooFewShards) {
				result.Missing += params.Data + params.Parity
				result.Healthy = false
				continue
			}
			if errors.Is(err, ErrFileCorrupt) {
				result.Corrupt++
				result.Healthy = false
				continue
			}
			return result, err
		}

		result.Verified += len(loaded.Shards)
		if !loaded.Healthy() {
			result.Corrupt += len(loaded.Rebuilt)
			result.Healthy = false
		}
	}

	return result, nil
}

func (s *Store) ScrubPool(ctx context.Context, poolID string) ([]ScrubResult, error) {
	files, err := s.meta.ListPoolFiles(ctx, poolID)
	if err != nil {
		return nil, fmt.Errorf("store: list the pool's files: %w", err)
	}

	results := make([]ScrubResult, 0, len(files))
	for _, file := range files {
		switch file.Status {
		case metadata.FileStatusCommitted, metadata.FileStatusDegraded:
		default:
			continue
		}

		result, err := s.ScrubFile(ctx, file.ID)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}

	return results, nil
}
